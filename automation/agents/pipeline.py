"""PipelineController — overlapping round execution for Escort Team.

Instead of strictly sequential rounds (finish all 6 phases → start next),
the pipeline allows overlapping:

    Round N:   [Discovery] [Analysis] [Fix] [Verify] [Write] [Promote]
    Round N+1:                        [Discovery] [Analysis] [Fix] ...

This doubles throughput when AI providers are the bottleneck, because
Discovery(N+1) runs while Fix(N) is in progress.

Safety: At most 2 concurrent rounds. A new round can only start its
Fix phase after the previous round's Verify phase completes (to prevent
conflicting file edits).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional

logger = logging.getLogger(__name__)

MAX_PIPELINE_DEPTH = 2  # Max concurrent rounds in different phases


class PipelinePhase(str, Enum):
    DISCOVERY = "discovery"
    ANALYSIS = "analysis"
    FIXING = "fixing"
    VERIFICATION = "verification"
    WRITEBACK = "writeback"
    PROMOTION = "promotion"
    COMPLETE = "complete"


@dataclass
class PipelineRound:
    """State of a single round in the pipeline."""
    round_id: str
    phase: PipelinePhase = PipelinePhase.DISCOVERY
    started_at: float = field(default_factory=time.monotonic)
    phase_started_at: float = field(default_factory=time.monotonic)
    # Results from each phase
    discovery_result: Optional[Dict[str, Any]] = None
    analysis_result: Optional[Dict[str, Any]] = None
    fix_result: Optional[Dict[str, Any]] = None
    verify_result: Optional[Dict[str, Any]] = None
    writeback_result: Optional[Dict[str, Any]] = None
    promote_result: Optional[Dict[str, Any]] = None
    # Completion
    success: bool = False
    error: str = ""


class PipelineController:
    """Manages overlapping round execution.

    Usage::

        pipeline = PipelineController(
            on_discovery=coordinator._run_discovery,
            on_analysis=coordinator._run_analysis,
            ...
        )
        await pipeline.run_continuous()
    """

    def __init__(
        self,
        max_depth: int = MAX_PIPELINE_DEPTH,
    ):
        self._max_depth = max_depth
        self._active_rounds: Dict[str, PipelineRound] = {}
        self._write_lock = asyncio.Lock()  # Prevents concurrent write phases
        self._fix_lock = asyncio.Lock()    # Prevents concurrent fix phases

    @property
    def active_count(self) -> int:
        return len(self._active_rounds)

    @property
    def can_start_new_round(self) -> bool:
        """Can we start a new DISCOVERY phase?"""
        if len(self._active_rounds) >= self._max_depth:
            return False
        # Check no other round is in DISCOVERY/ANALYSIS
        early_phases = {PipelinePhase.DISCOVERY, PipelinePhase.ANALYSIS}
        for r in self._active_rounds.values():
            if r.phase in early_phases:
                return False
        return True

    def start_round(self, round_id: str) -> PipelineRound:
        """Register a new round in the pipeline."""
        r = PipelineRound(round_id=round_id)
        self._active_rounds[round_id] = r
        return r

    def advance_phase(self, round_id: str, phase: PipelinePhase) -> None:
        """Advance a round to the next phase."""
        r = self._active_rounds.get(round_id)
        if r:
            r.phase = phase
            r.phase_started_at = time.monotonic()

    def complete_round(self, round_id: str, success: bool = True, error: str = "") -> None:
        """Mark a round as complete and remove from active set."""
        r = self._active_rounds.pop(round_id, None)
        if r:
            r.phase = PipelinePhase.COMPLETE
            r.success = success
            r.error = error
            logger.info(
                "Pipeline round %s complete: success=%s, duration=%.1fs",
                round_id, success, time.monotonic() - r.started_at,
            )

    async def acquire_fix_lock(self) -> None:
        """Acquire exclusive access for fix phase."""
        await self._fix_lock.acquire()

    def release_fix_lock(self) -> None:
        """Release fix phase lock."""
        if self._fix_lock.locked():
            self._fix_lock.release()

    async def acquire_write_lock(self) -> None:
        """Acquire exclusive access for write phase."""
        await self._write_lock.acquire()

    def release_write_lock(self) -> None:
        """Release write phase lock."""
        if self._write_lock.locked():
            self._write_lock.release()

    def get_status(self) -> Dict[str, Any]:
        return {
            "active_rounds": len(self._active_rounds),
            "max_depth": self._max_depth,
            "rounds": {
                rid: {
                    "phase": r.phase.value,
                    "elapsed": round(time.monotonic() - r.started_at, 1),
                }
                for rid, r in self._active_rounds.items()
            },
        }
