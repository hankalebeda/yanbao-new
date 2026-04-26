"""Residency Controller — long-term autonomous monitoring mode.

After the Escort Team achieves its fix goal (default: 10 consecutive
successful fixes with zero regressions), the system transitions to
Residency Mode — a permanent low-power monitoring state that:

1. Continuously sniffs for new regressions, drift, and blind spots
2. Auto-wakes into active fix mode when triggered
3. Tracks wake/sleep cycles and residency health
4. Manages progressive scan intervals (expanding from 5min → 30min → 2h)
5. Reports residency metrics for Kestra / external monitoring

Inspired by the LiteLLM claude-code-sourcemap persistent session
pattern with heartbeat yields and idle detection.

Usage::

    rc = ResidencyController(state_dir, fix_goal=10)
    while True:
        if rc.should_scan():
            findings = await discovery_agent.scan()
            if findings:
                rc.wake("regression detected")
                # ... enter fix loop ...
                rc.record_fix_success()
            else:
                rc.record_clean_scan()
        await asyncio.sleep(rc.next_scan_interval())
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Scan interval tiers (seconds)
SCAN_TIER_ACTIVE = 60            # 1 minute during active fix
SCAN_TIER_WARM = 300             # 5 minutes after recent fix
SCAN_TIER_IDLE = 1800            # 30 minutes in stable residency
SCAN_TIER_DEEP_SLEEP = 7200     # 2 hours in long-term residency

# Warm → Idle transition after N clean scans
WARM_TO_IDLE_SCANS = 6          # ~30 minutes of clean
IDLE_TO_DEEP_SCANS = 12         # ~6 hours of clean


class ResidencyPhase(str):
    """Residency operating phase."""
    ACTIVE = "active"                # Fixing issues
    WARM = "warm"                    # Recently fixed, frequent checks
    IDLE = "idle"                    # Stable, standard monitoring
    DEEP_SLEEP = "deep_sleep"       # Long-term stable, infrequent checks


@dataclass
class ResidencyState:
    """Persistent state for the Residency Controller."""
    phase: str = ResidencyPhase.ACTIVE
    entered_at: str = ""
    last_scan_at: float = 0.0      # monotonic
    consecutive_clean_scans: int = 0
    total_wake_cycles: int = 0
    total_fixes_in_residency: int = 0
    current_wake_reason: str = ""
    scan_history: List[Dict[str, Any]] = field(default_factory=list)

    # Metrics
    total_scans: int = 0
    total_findings: int = 0
    longest_clean_streak: int = 0
    average_scan_interval: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ResidencyState":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


class ResidencyController:
    """Manages the long-term residency monitoring lifecycle.

    The controller tracks scan results and automatically transitions
    between phases based on system health:

    ACTIVE → WARM (after successful fix)
    WARM → IDLE (after 6 clean scans)
    IDLE → DEEP_SLEEP (after 12 more clean scans)
    Any → ACTIVE (on regression detection)
    """

    def __init__(
        self,
        state_dir: Path,
        fix_goal: int = 10,
    ):
        self._state_dir = state_dir
        self._state_path = state_dir / "residency_state.json"
        self._fix_goal = fix_goal
        self._state = self._load()

    @property
    def phase(self) -> str:
        return self._state.phase

    @property
    def is_active(self) -> bool:
        return self._state.phase == ResidencyPhase.ACTIVE

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "phase": self._state.phase,
            "consecutive_clean": self._state.consecutive_clean_scans,
            "total_wakes": self._state.total_wake_cycles,
            "total_fixes": self._state.total_fixes_in_residency,
            "total_scans": self._state.total_scans,
            "longest_streak": self._state.longest_clean_streak,
        }

    def should_scan(self) -> bool:
        """Check if enough time has passed for the next scan."""
        if self._state.last_scan_at == 0:
            return True
        elapsed = time.monotonic() - self._state.last_scan_at
        return elapsed >= self.next_scan_interval()

    def next_scan_interval(self) -> float:
        """Return the appropriate scan interval based on current phase."""
        intervals = {
            ResidencyPhase.ACTIVE: SCAN_TIER_ACTIVE,
            ResidencyPhase.WARM: SCAN_TIER_WARM,
            ResidencyPhase.IDLE: SCAN_TIER_IDLE,
            ResidencyPhase.DEEP_SLEEP: SCAN_TIER_DEEP_SLEEP,
        }
        return intervals.get(self._state.phase, SCAN_TIER_IDLE)

    def record_clean_scan(self) -> None:
        """Record a scan that found no issues."""
        self._state.total_scans += 1
        self._state.consecutive_clean_scans += 1
        self._state.last_scan_at = time.monotonic()

        # Update longest streak
        if self._state.consecutive_clean_scans > self._state.longest_clean_streak:
            self._state.longest_clean_streak = self._state.consecutive_clean_scans

        # Phase transitions based on clean scan count
        if (self._state.phase == ResidencyPhase.WARM
                and self._state.consecutive_clean_scans >= WARM_TO_IDLE_SCANS):
            self._transition(ResidencyPhase.IDLE)
        elif (self._state.phase == ResidencyPhase.IDLE
                and self._state.consecutive_clean_scans >= WARM_TO_IDLE_SCANS + IDLE_TO_DEEP_SCANS):
            self._transition(ResidencyPhase.DEEP_SLEEP)

        self._append_scan_event("clean")
        self._save()

    def record_finding(self, count: int = 1) -> None:
        """Record a scan that found issues (but hasn't woken yet)."""
        self._state.total_scans += 1
        self._state.total_findings += count
        self._state.last_scan_at = time.monotonic()
        self._append_scan_event("finding", count=count)
        self._save()

    def wake(self, reason: str = "") -> None:
        """Wake into active fix mode."""
        self._state.total_wake_cycles += 1
        self._state.consecutive_clean_scans = 0
        self._state.current_wake_reason = reason
        self._transition(ResidencyPhase.ACTIVE)
        logger.info(
            "[residency] WAKE #%d: %s",
            self._state.total_wake_cycles, reason,
        )
        self._save()

    def record_fix_success(self, count: int = 1) -> None:
        """Record a successful fix in residency."""
        self._state.total_fixes_in_residency += count
        # Transition to WARM after fix
        if self._state.phase == ResidencyPhase.ACTIVE:
            self._transition(ResidencyPhase.WARM)
        self._save()

    def enter_residency(self) -> None:
        """Explicitly enter residency mode (called by SystemControlState)."""
        if self._state.phase == ResidencyPhase.ACTIVE:
            self._transition(ResidencyPhase.WARM)
        now = datetime.now(timezone.utc).isoformat()
        if not self._state.entered_at:
            self._state.entered_at = now
        self._save()
        logger.info("[residency] Entered residency mode")

    def _transition(self, target: str) -> None:
        old = self._state.phase
        self._state.phase = target
        logger.info("[residency] Phase: %s → %s", old, target)

    def _append_scan_event(self, event_type: str, **extra: Any) -> None:
        self._state.scan_history.append({
            "type": event_type,
            "at": datetime.now(timezone.utc).isoformat(),
            "phase": self._state.phase,
            **extra,
        })
        # Keep last 200 events
        if len(self._state.scan_history) > 200:
            self._state.scan_history = self._state.scan_history[-200:]

    def _save(self) -> None:
        import tempfile, os
        self._state_dir.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            dir=str(self._state_dir), suffix=".tmp", prefix=".res_"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._state.to_dict(), f, ensure_ascii=False, indent=2)
            os.replace(tmp, str(self._state_path))
        except Exception:
            try:
                os.unlink(tmp)
            except Exception:
                pass
            raise

    def _load(self) -> ResidencyState:
        if self._state_path.exists():
            try:
                data = json.loads(self._state_path.read_text(encoding="utf-8"))
                return ResidencyState.from_dict(data)
            except Exception:
                logger.warning("Failed to load residency state, starting fresh")
        return ResidencyState()
