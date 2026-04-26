"""Unified Round State Machine — ensures atomic round lifecycle.

Inspired by the LiteLLM issue_mesh control_state protocol and the
claude-code-sourcemap LocalAgentTask lifecycle, this module enforces
the invariant that writeback, verify, and promote form an atomic
transaction within each round.

Prevents split-brain states such as:
  - "code fixed but doc not written back"
  - "doc written back but verify incomplete"
  - "promoted but evidence chain broken"

Each round transitions through a strict FSM::

    PENDING → DISCOVERED → ANALYSED → FIXING → FIXED
        → VERIFYING → VERIFIED → WRITING_BACK → WRITTEN_BACK
        → PROMOTING → PROMOTED → CLOSED

On any failure the round transitions to FAILED and can optionally
roll back to a safe checkpoint.

The ``ControlState`` manages the higher-level system mode transitions::

    RECOVERY_BLOCKED → RECOVERY_REARM → RECOVERY_EXECUTING
        → PROMOTE_READY → RESIDENCY → BACKLOG_OPEN
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Round Phase FSM
# ---------------------------------------------------------------------------

class RoundPhase(str, Enum):
    """Atomic phases within a single orchestration round."""
    PENDING = "pending"
    DISCOVERED = "discovered"
    ANALYSED = "analysed"
    FIXING = "fixing"
    FIXED = "fixed"
    VERIFYING = "verifying"
    VERIFIED = "verified"
    WRITING_BACK = "writing_back"
    WRITTEN_BACK = "written_back"
    PROMOTING = "promoting"
    PROMOTED = "promoted"
    CLOSED = "closed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


# Valid transitions (from → set of allowed targets)
_TRANSITIONS: Dict[RoundPhase, frozenset] = {
    RoundPhase.PENDING: frozenset({RoundPhase.DISCOVERED, RoundPhase.FAILED}),
    RoundPhase.DISCOVERED: frozenset({RoundPhase.ANALYSED, RoundPhase.CLOSED, RoundPhase.FAILED}),
    RoundPhase.ANALYSED: frozenset({RoundPhase.FIXING, RoundPhase.CLOSED, RoundPhase.FAILED}),
    RoundPhase.FIXING: frozenset({RoundPhase.FIXED, RoundPhase.FAILED}),
    RoundPhase.FIXED: frozenset({RoundPhase.VERIFYING, RoundPhase.FAILED}),
    RoundPhase.VERIFYING: frozenset({RoundPhase.VERIFIED, RoundPhase.FAILED}),
    RoundPhase.VERIFIED: frozenset({RoundPhase.WRITING_BACK, RoundPhase.FAILED}),
    RoundPhase.WRITING_BACK: frozenset({RoundPhase.WRITTEN_BACK, RoundPhase.FAILED}),
    RoundPhase.WRITTEN_BACK: frozenset({RoundPhase.PROMOTING, RoundPhase.FAILED}),
    RoundPhase.PROMOTING: frozenset({RoundPhase.PROMOTED, RoundPhase.FAILED}),
    RoundPhase.PROMOTED: frozenset({RoundPhase.CLOSED}),
    RoundPhase.CLOSED: frozenset(),
    RoundPhase.FAILED: frozenset({RoundPhase.ROLLED_BACK, RoundPhase.PENDING}),
    RoundPhase.ROLLED_BACK: frozenset({RoundPhase.PENDING, RoundPhase.CLOSED}),
}


class InvalidTransitionError(Exception):
    """Raised when an illegal state transition is attempted."""
    pass


@dataclass
class RoundState:
    """Tracks atomic state of a single orchestration round.

    Ensures no split-brain: every phase transition is validated
    against the FSM and persisted atomically.
    """
    round_id: str = ""
    phase: str = RoundPhase.PENDING.value
    started_at: str = ""
    phase_entered_at: str = ""
    problem_count: int = 0
    actionable_count: int = 0
    patch_count: int = 0
    verify_passed: Optional[bool] = None
    writeback_receipt_count: int = 0
    promote_approved: Optional[bool] = None
    promote_targets: List[str] = field(default_factory=list)
    evidence_hash: str = ""       # SHA256 of concatenated evidence chain
    error: str = ""
    checkpoint_phase: str = ""    # last successful phase for rollback
    history: List[Dict[str, str]] = field(default_factory=list)

    def transition(self, target: RoundPhase) -> None:
        """Attempt to transition to *target* phase.

        Raises ``InvalidTransitionError`` if the transition is illegal.
        Records the transition in history.
        """
        current = RoundPhase(self.phase)
        allowed = _TRANSITIONS.get(current, frozenset())
        if target not in allowed:
            raise InvalidTransitionError(
                f"Cannot transition from {current.value} to {target.value}. "
                f"Allowed: {sorted(p.value for p in allowed)}"
            )
        # Save checkpoint on every successful forward step
        if target != RoundPhase.FAILED and target != RoundPhase.ROLLED_BACK:
            self.checkpoint_phase = self.phase
        now = datetime.now(timezone.utc).isoformat()
        self.history.append({
            "from": self.phase,
            "to": target.value,
            "at": now,
        })
        self.phase = target.value
        self.phase_entered_at = now

    def update_evidence(self, **kwargs: Any) -> None:
        """Update evidence fields and recompute the evidence chain hash."""
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        self._recompute_hash()

    def _recompute_hash(self) -> None:
        """SHA256 of concatenated evidence fields for integrity checking."""
        parts = [
            self.round_id,
            str(self.problem_count),
            str(self.patch_count),
            str(self.verify_passed),
            str(self.writeback_receipt_count),
            str(self.promote_approved),
            ",".join(self.promote_targets),
        ]
        self.evidence_hash = hashlib.sha256(
            "|".join(parts).encode()
        ).hexdigest()[:16]

    def is_terminal(self) -> bool:
        return self.phase in (
            RoundPhase.CLOSED.value,
            RoundPhase.FAILED.value,
            RoundPhase.ROLLED_BACK.value,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RoundState":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# Control State (system-level mode)
# ---------------------------------------------------------------------------

class ControlState(str, Enum):
    """System-level control state, inspired by issue_mesh protocol."""
    RECOVERY_BLOCKED = "recovery_blocked"
    RECOVERY_REARM = "recovery_rearm"
    RECOVERY_EXECUTING = "recovery_executing"
    PROMOTE_READY = "promote_ready"
    RESIDENCY = "residency"
    BACKLOG_OPEN = "backlog_open"


_CONTROL_TRANSITIONS: Dict[ControlState, frozenset] = {
    ControlState.RECOVERY_BLOCKED: frozenset({ControlState.RECOVERY_REARM}),
    ControlState.RECOVERY_REARM: frozenset({
        ControlState.RECOVERY_EXECUTING,
        ControlState.RECOVERY_BLOCKED,
    }),
    ControlState.RECOVERY_EXECUTING: frozenset({
        ControlState.PROMOTE_READY,
        ControlState.RECOVERY_REARM,     # regression
        ControlState.RECOVERY_BLOCKED,   # hard failure
    }),
    ControlState.PROMOTE_READY: frozenset({
        ControlState.RESIDENCY,
        ControlState.RECOVERY_EXECUTING,  # post-promote regression
    }),
    ControlState.RESIDENCY: frozenset({
        ControlState.RECOVERY_EXECUTING,  # regression in residency → re-fix
        ControlState.BACKLOG_OPEN,
    }),
    ControlState.BACKLOG_OPEN: frozenset({
        ControlState.RESIDENCY,
        ControlState.RECOVERY_EXECUTING,
    }),
}


@dataclass
class SystemControlState:
    """Persistent system-level control state.

    Manages higher-level transitions between recovery, promote, and
    residency modes.
    """
    state: str = ControlState.RECOVERY_REARM.value
    fix_goal: int = 10
    fixes_achieved: int = 0
    consecutive_green: int = 0
    last_transition: str = ""
    residency_entered_at: str = ""
    residency_wake_count: int = 0
    total_fixes_lifetime: int = 0
    history: List[Dict[str, str]] = field(default_factory=list)

    def transition(self, target: ControlState) -> None:
        current = ControlState(self.state)
        allowed = _CONTROL_TRANSITIONS.get(current, frozenset())
        if target not in allowed:
            raise InvalidTransitionError(
                f"Control: cannot transition {current.value} → {target.value}"
            )
        now = datetime.now(timezone.utc).isoformat()
        self.history.append({
            "from": self.state,
            "to": target.value,
            "at": now,
        })
        self.state = target.value
        self.last_transition = now

        if target == ControlState.RESIDENCY and not self.residency_entered_at:
            self.residency_entered_at = now

    def record_fix(self, count: int = 1) -> None:
        self.fixes_achieved += count
        self.total_fixes_lifetime += count

    def record_green_round(self) -> None:
        self.consecutive_green += 1

    def reset_green(self) -> None:
        self.consecutive_green = 0

    def should_promote(self) -> bool:
        """True when fix goal is met and system is ready for promotion."""
        return (
            self.fixes_achieved >= self.fix_goal
            and self.state == ControlState.RECOVERY_EXECUTING.value
        )

    def should_enter_residency(self) -> bool:
        return self.state == ControlState.PROMOTE_READY.value

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SystemControlState":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# Round State Manager (persistence + atomic operations)
# ---------------------------------------------------------------------------

class RoundStateManager:
    """Manages round state persistence and provides atomic operations.

    Ensures:
    1. Every round transition is validated against the FSM
    2. State is persisted atomically (write-tmp + rename)
    3. Evidence chain is always consistent
    4. No two rounds can be active simultaneously (single-writer)
    """

    def __init__(self, state_dir: Path):
        self._state_dir = state_dir
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._active_round: Optional[RoundState] = None
        self._control = self._load_control()

    @property
    def active_round(self) -> Optional[RoundState]:
        return self._active_round

    @property
    def control(self) -> SystemControlState:
        return self._control

    def begin_round(self, round_id: str) -> RoundState:
        """Start a new round. Fails if another round is active."""
        if self._active_round and not self._active_round.is_terminal():
            raise RuntimeError(
                f"Cannot begin round {round_id}: "
                f"round {self._active_round.round_id} is still active "
                f"in phase {self._active_round.phase}"
            )
        now = datetime.now(timezone.utc).isoformat()
        rs = RoundState(
            round_id=round_id,
            phase=RoundPhase.PENDING.value,
            started_at=now,
            phase_entered_at=now,
        )
        self._active_round = rs
        self._persist_round(rs)
        return rs

    def advance(self, target: RoundPhase, **evidence: Any) -> RoundState:
        """Advance current round to *target* and update evidence."""
        if not self._active_round:
            raise RuntimeError("No active round")
        self._active_round.transition(target)
        if evidence:
            self._active_round.update_evidence(**evidence)
        self._persist_round(self._active_round)
        return self._active_round

    def fail_round(self, error: str) -> RoundState:
        """Fail the current round."""
        if not self._active_round:
            raise RuntimeError("No active round")
        self._active_round.error = error
        try:
            self._active_round.transition(RoundPhase.FAILED)
        except InvalidTransitionError:
            # Already in terminal state
            pass
        self._persist_round(self._active_round)
        return self._active_round

    def close_round(self) -> RoundState:
        """Close the current round as successful."""
        if not self._active_round:
            raise RuntimeError("No active round")
        try:
            self._active_round.transition(RoundPhase.CLOSED)
        except InvalidTransitionError:
            # May already be CLOSED
            pass
        self._persist_round(self._active_round)
        return self._active_round

    def advance_control(self, target: ControlState) -> None:
        """Advance system control state."""
        self._control.transition(target)
        self._persist_control()

    # ---- Persistence ----

    def _persist_round(self, rs: RoundState) -> None:
        path = self._state_dir / "active_round.json"
        self._atomic_write(path, rs.to_dict())

    def _persist_control(self) -> None:
        path = self._state_dir / "control_state.json"
        self._atomic_write(path, self._control.to_dict())

    def _load_control(self) -> SystemControlState:
        path = self._state_dir / "control_state.json"
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                return SystemControlState.from_dict(data)
            except Exception:
                logger.warning("Failed to load control state, starting fresh")
        return SystemControlState()

    @staticmethod
    def _atomic_write(path: Path, data: Dict[str, Any]) -> None:
        """Write via temp file + rename for atomicity."""
        import tempfile
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            dir=str(path.parent), suffix=".tmp", prefix=".rsm_"
        )
        try:
            import os
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, str(path))
        except Exception:
            try:
                import os
                os.unlink(tmp)
            except Exception:
                pass
            raise
