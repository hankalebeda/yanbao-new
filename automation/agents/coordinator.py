"""CoordinatorAgent — orchestrates the full autonomous escort team.

Follows the claude-code-sourcemap *Coordinator mode* principle:
the Coordinator never executes business logic itself; it only
dispatches tasks, collects results, manages agent health, and
makes global decisions (mode switching, circuit-breaking, escalation).

State machine::

    BOOTSTRAP → DISCOVERY → ANALYSIS → FIXING → VERIFICATION
        → WRITEBACK → PROMOTION → MONITORING
        ↑                                        │
        └────────── (regression detected) ───────┘
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base_agent import AgentConfig, BaseAgent
from .claim_registry import ClaimRegistry
from .mailbox import Mailbox
from .persistent_retry import RetryCategory, RetryStats, with_retry
from .pipeline import PipelineController
from .worker_pool import WorkerPool, WorkerResult
from .protocol import (
    AgentHealthSnapshot,
    AgentMessage,
    AgentResult,
    AgentRole,
    AgentState,
    CoordinatorMode,
    CoordinatorPhase,
    CoordinatorState,
    MessageType,
    ProblemStatus,
    RoundProgress,
)
from .residency import ResidencyController
from .state_machine import (
    ControlState,
    RoundPhase,
    RoundStateManager,
    InvalidTransitionError,
)
from .team_memory import TeamMemory

logger = logging.getLogger(__name__)

# Thresholds
GREEN_ROUNDS_FOR_MONITOR = 5
GREEN_ROUNDS_FOR_COMPLETED = 5  # v2: auto-complete after N green + all probes clean
FIX_FAILURES_FOR_SAFE_HOLD = 5
SAFE_HOLD_TIMEOUT_SECONDS = 3600     # 1 hour
HEARTBEAT_MISSING_SECONDS = 180      # 3 minutes
HEALTH_CHECK_INTERVAL = 60.0
STALL_THRESHOLD = 5                  # consecutive no-progress rounds before infra_doctor
MAX_AGENT_RESTARTS = 3               # v2: max auto-restarts per agent
AGENT_RESTART_COOLDOWN = 30.0        # v2: seconds between restart attempts
COMPLETED_CHECK_INTERVAL = 21600     # v2: 6 hours in COMPLETED mode
AI_BACKOFF_BASE = 30.0               # v4: base backoff seconds for 429/503
AI_BACKOFF_MAX = 600.0               # v4: max backoff seconds (10 min)
AI_BACKOFF_RESET_AFTER = 1800.0      # v4: reset backoff after 30 min of success


class CoordinatorAgent(BaseAgent):
    """Central orchestrator for the multi-agent escort team.

    The Coordinator:
    1. Dispatches tasks to specialised agents (Discovery → Analysis → Fix → …)
    2. Collects results via Mailbox
    3. Makes global decisions (mode switch, circuit-break, escalation)
    4. Maintains ``CoordinatorState`` (persisted to JSON)
    """

    def __init__(
        self,
        mailbox: Mailbox,
        config: Optional[AgentConfig] = None,
        state_path: Optional[Path] = None,
    ):
        super().__init__(role=AgentRole.COORDINATOR, mailbox=mailbox, config=config)
        # Override generic agent_id to a stable name so workers can address us
        self.agent_id = "coordinator"

        self._state_path = state_path or (
            self.config.repo_root / "runtime" / "agents" / "coordinator_state.json"
        )
        self._coord_state = self._load_state()

        # Agent registry: agent_id → last heartbeat timestamp
        self._agent_heartbeats: Dict[str, float] = {}
        # v2: Agent instances for restart capability
        self._agent_instances: Dict[str, Any] = {}
        # Pending results for current round
        self._round_results: Dict[str, AgentResult] = {}
        # Health-check background task
        self._health_task: Optional[asyncio.Task] = None
        # v4: AI pressure backoff state {role: {"backoff_until": float, "attempts": int}}
        self._ai_pressure_state: Dict[str, Dict[str, Any]] = {}
        # v4: Live round progress for /v1/round-progress
        self._current_round_progress: Optional[RoundProgress] = None
        # v5: Last preflight check results (used by projection for truthful provider_pool)
        self._last_preflight: Dict[str, bool] = {}
        # v5: Timestamp of last preflight check
        self._last_preflight_ts: float = 0.0

        # v8: Unified Round State Machine, Team Memory, Claim Registry, Residency
        agents_dir = self.config.repo_root / "runtime" / "agents"
        self._round_mgr = RoundStateManager(agents_dir / "round_state")
        self._team_memory = TeamMemory(agents_dir)
        self._claim_registry = ClaimRegistry(agents_dir / "claims")
        self._residency = ResidencyController(agents_dir)
        # v8: Persistent retry stats per target role
        self._retry_stats: Dict[str, RetryStats] = {}

        # v10: Worker pool for parallel fix/analysis (Claude Code-style fan-out)
        self._fix_pool: Optional[WorkerPool] = None
        self._analysis_pool: Optional[WorkerPool] = None
        # v10: Pipeline controller for overlapping rounds
        self._pipeline = PipelineController(max_depth=2)

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _load_state(self) -> CoordinatorState:
        if self._state_path.exists():
            try:
                data = json.loads(self._state_path.read_text(encoding="utf-8"))
                return CoordinatorState.from_dict(data)
            except Exception:
                logger.warning("Failed to load coordinator state, starting fresh")
        return CoordinatorState()

    def _save_state(self) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        self._state_path.write_text(
            json.dumps(self._coord_state.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self._write_control_plane_projection()

    # ------------------------------------------------------------------
    # Lifecycle overrides
    # ------------------------------------------------------------------

    async def start(self) -> None:
        # v6: Drain stale messages from previous runs to prevent
        # discovery/analysis rapid-fire from restored mailbox backlog
        stale = await self.mailbox.drain()
        if stale:
            logger.info(
                "[coordinator] Drained %d stale messages from previous session",
                len(stale),
            )

        await super().start()
        self._health_task = asyncio.create_task(
            self._health_check_loop(), name="coordinator-health-check"
        )
        self._coord_state.phase = CoordinatorPhase.BOOTSTRAP.value

        # v9: Reset stale agent registration from prior sessions.
        # Heartbeat window (180s) is much shorter than monitor interval (1800s),
        # so prior-session agents accumulate in agents_registered while only
        # current-session agents are healthy — deflating the health ratio.
        if self._coord_state.agents_registered:
            logger.info(
                "[coordinator] Clearing %d stale agent registrations from prior sessions",
                len(self._coord_state.agents_registered),
            )
            self._coord_state.agents_registered = []
            self._coord_state.agents_healthy = []
        # v9: Reset stale no-progress counter that may have accumulated in
        # prior sessions — prevents immediate SAFE_HOLD after restart.
        if self._coord_state.consecutive_no_progress_rounds > 0:
            logger.info(
                "[coordinator] Resetting stale consecutive_no_progress_rounds=%d",
                self._coord_state.consecutive_no_progress_rounds,
            )
            self._coord_state.consecutive_no_progress_rounds = 0

        # v2: Run preflight checks during bootstrap
        preflight = await self._preflight_checks()
        # v5: Store preflight results for truthful projection
        self._last_preflight = preflight
        self._last_preflight_ts = time.monotonic()
        if preflight.get("all_ok"):
            logger.info("[coordinator] Preflight checks PASSED — all services reachable")
        else:
            failed = [k for k, v in preflight.items() if k != "all_ok" and not v]
            logger.warning(
                "[coordinator] Preflight checks: %d failures: %s",
                len(failed), failed,
            )
            # v5: Auto SAFE_HOLD when helper services are down (not dry-run)
            # v9: Skip SAFE_HOLD entry if system was previously in stable
            # monitor/completed mode (proven healthy) — service unavailability
            # in this case is environment-normal, not a regression.
            failed_svc = [k for k in failed if k.startswith("svc_")]
            dry_run = os.environ.get("DRY_RUN_SERVICES", "").lower() in ("1", "true", "yes")
            was_stable = self._coord_state.mode in (
                CoordinatorMode.MONITOR.value,
                CoordinatorMode.COMPLETED.value,
            ) and self._coord_state.consecutive_green_rounds >= GREEN_ROUNDS_FOR_MONITOR
            if failed_svc and not dry_run and not was_stable:
                logger.warning(
                    "[coordinator] Helper services down (%s) — entering SAFE_HOLD",
                    failed_svc,
                )
                self._coord_state.mode = CoordinatorMode.SAFE_HOLD.value
                self._coord_state.last_safe_hold_problem_count = len(failed_svc)

        self._save_state()
        logger.info("[coordinator] Escort team bootstrap complete")

    async def shutdown(self, reason: str = "requested") -> None:
        # Broadcast shutdown to all agents
        await self.mailbox.send(AgentMessage(
            source=self.agent_id,
            target="*",
            msg_type=MessageType.SHUTDOWN_REQUEST.value,
            payload={"reason": reason},
        ))
        if self._health_task and not self._health_task.done():
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass
        self._save_state()
        await super().shutdown(reason)

    # ------------------------------------------------------------------
    # The Coordinator does NOT use handle_task for its own work.
    # Instead, it runs a custom orchestration loop.
    # ------------------------------------------------------------------

    async def handle_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Not used for Coordinator (it drives its own loop)."""
        return {"info": "coordinator does not receive task_dispatch"}

    async def _run_loop(self) -> None:
        """Override the base run_loop with the Coordinator orchestration cycle."""
        while not self._shutdown_requested:
            try:
                await self._orchestration_round()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("[coordinator] Orchestration round error")
                self._error_count += 1
                self._consecutive_failures += 1
                if self._consecutive_failures >= FIX_FAILURES_FOR_SAFE_HOLD:
                    await self._enter_safe_hold("coordinator internal failures")
                await asyncio.sleep(30)

    # ------------------------------------------------------------------
    # Orchestration Round
    # ------------------------------------------------------------------

    async def _orchestration_round(self) -> None:
        """Execute one full DISCOVERY → … → PROMOTION cycle."""
        mode = CoordinatorMode(self._coord_state.mode)

        if mode == CoordinatorMode.SAFE_HOLD:
            # v3: Exponential backoff: 300s → 600s → 1200s → ... capped at 28800s (8h)
            hold_count = self._coord_state.safe_hold_count
            sleep_s = min(300 * (2 ** hold_count), 28800)
            logger.info(
                "[coordinator] SAFE_HOLD — backoff level %d, sleeping %ds",
                hold_count, sleep_s,
            )
            await asyncio.sleep(sleep_s)

            # v3: Run incremental discovery before deciding to exit
            try:
                discovery_result = await self._dispatch_and_wait(
                    phase=CoordinatorPhase.DISCOVERY,
                    target_role=AgentRole.DISCOVERY.value,
                    round_id=f"safe-hold-check-{uuid.uuid4().hex[:6]}",
                    payload={"round_id": "safe-hold-check", "mode": "incremental"},
                    timeout=120.0,
                )
                problem_count = (discovery_result or {}).get("deduplicated", 0)
            except Exception:
                problem_count = self._coord_state.last_safe_hold_problem_count

            if problem_count >= self._coord_state.last_safe_hold_problem_count and self._coord_state.last_safe_hold_problem_count > 0:
                # Problems not decreasing — stay in SAFE_HOLD, increase backoff
                self._coord_state.safe_hold_count = hold_count + 1
                self._coord_state.last_safe_hold_problem_count = problem_count
                self._save_state()
                self._write_escalation(
                    "safe_hold_extended",
                    f"safe_hold_round_{hold_count}",
                    f"Problems not decreasing ({problem_count}), extending hold",
                )
                return

            # Problems decreased or first check — exit SAFE_HOLD
            # v5: Re-run preflight before exiting to prevent re-entry (doc25 angle #11)
            preflight = await self._preflight_checks()
            self._last_preflight = preflight
            self._last_preflight_ts = time.monotonic()
            if not preflight.get("all_ok"):
                failed = [k for k, v in preflight.items() if k != "all_ok" and not v]
                if any(k.startswith("svc_") for k in failed):
                    # Helper services still down — stay in SAFE_HOLD
                    self._coord_state.safe_hold_count = hold_count + 1
                    self._save_state()
                    logger.warning(
                        "[coordinator] SAFE_HOLD stay: preflight still failing: %s", failed,
                    )
                    return

            logger.info(
                "[coordinator] SAFE_HOLD exit: problems %d → %d",
                self._coord_state.last_safe_hold_problem_count, problem_count,
            )
            self._coord_state.mode = CoordinatorMode.FIX.value
            self._coord_state.consecutive_fix_failures = 0
            self._coord_state.safe_hold_count = 0
            self._coord_state.last_safe_hold_problem_count = 0
            self._save_state()
            return

        # v2: COMPLETED mode — ultra-low frequency monitoring
        if mode == CoordinatorMode.COMPLETED:
            logger.info("[coordinator] COMPLETED mode — checking every %ds", COMPLETED_CHECK_INTERVAL)
            await asyncio.sleep(min(COMPLETED_CHECK_INTERVAL, 300))
            # Run discovery-only to verify still clean
            check_result = await self._dispatch_and_wait(
                phase=CoordinatorPhase.DISCOVERY,
                target_role=AgentRole.DISCOVERY.value,
                round_id=f"completed-check-{uuid.uuid4().hex[:6]}",
                payload={"round_id": f"completed-check", "mode": "full"},
                timeout=120.0,
            )
            problems = (check_result or {}).get("findings", [])
            # Only ACTIVE problems constitute a regression — blocked/review
            # problems are expected external dependencies and do not invalidate
            # the completed state.
            lane_plan = self._plan_execution_lanes(problems)
            active_regression = lane_plan["active_problems"]
            if active_regression:
                logger.info("[coordinator] Regression detected in COMPLETED mode — %d active problems, switching to FIX", len(active_regression))
                self._coord_state.mode = CoordinatorMode.FIX.value
                self._coord_state.consecutive_green_rounds = 0
                self._coord_state.completion_time = ""
                self._save_state()
            return

        round_id = f"round-{uuid.uuid4().hex[:8]}"
        self._coord_state.current_round_id = round_id
        self._coord_state.total_rounds += 1
        self._round_results.clear()

        # v8: Begin atomic round in state machine
        try:
            round_state = self._round_mgr.begin_round(round_id)
        except RuntimeError as exc:
            logger.warning("[coordinator] Cannot begin round: %s", exc)
            await asyncio.sleep(30)
            return

        # v4: Initialize round progress tracker
        self._current_round_progress = RoundProgress(
            round_id=round_id,
            phase=CoordinatorPhase.DISCOVERY.value,
            started_at=datetime.now(timezone.utc).isoformat(),
            phase_started_at=datetime.now(timezone.utc).isoformat(),
            ai_pressure={k: v.get("attempts", 0) for k, v in self._ai_pressure_state.items()},
        )

        interval = 300 if mode == CoordinatorMode.FIX else 1800

        # ---- Phase: DISCOVERY ----
        discovery_result = await self._dispatch_and_wait(
            phase=CoordinatorPhase.DISCOVERY,
            target_role=AgentRole.DISCOVERY.value,
            round_id=round_id,
            payload={"round_id": round_id, "mode": mode.value},
            timeout=300.0,
        )

        problems = (discovery_result or {}).get("findings", [])
        # v8: Advance round FSM to DISCOVERED
        try:
            self._round_mgr.advance(RoundPhase.DISCOVERED, problem_count=len(problems))
        except InvalidTransitionError:
            pass
        lane_plan = self._plan_execution_lanes(problems)
        self._coord_state.execution_lanes = lane_plan["execution_lanes"]
        self._coord_state.blocked_problems = lane_plan["blocked_problems"]
        self._coord_state.last_discovery_time = datetime.now(timezone.utc).isoformat()
        if not problems:
            self._coord_state.execution_lanes = {}
            self._coord_state.blocked_problems = []
            # No problems found — green round
            self._record_green_round(round_id, problem_count=0)
            logger.info(
                "[coordinator] Round %s GREEN (%d consecutive)",
                round_id, self._coord_state.consecutive_green_rounds,
            )
            # v9: Close the round FSM so the next round can begin
            try:
                self._round_mgr.close_round()
            except Exception:
                pass
            # v7: Run a standalone promote round once formal promote is still
            # pending.  Without this, MONITOR-mode green rounds skip the
            # PROMOTE phase entirely (problems=[] → early return), keeping
            # formal_promote.state="not_attempted" forever.
            if (
                self._coord_state.consecutive_green_rounds >= GREEN_ROUNDS_FOR_COMPLETED
                and not self._formal_promote_ready_for_completion()
            ):
                await self._run_standalone_promote_round(round_id)
            # v2: Check for auto-completion (all probes clean + enough green rounds)
            if await self._check_completion():
                return
            if self._coord_state.consecutive_green_rounds >= GREEN_ROUNDS_FOR_MONITOR:
                await self._switch_mode(CoordinatorMode.MONITOR)
            await asyncio.sleep(interval)
            return

        active_problems = lane_plan["active_problems"]
        review_problems = lane_plan["review_problems"]
        blocked_problems = lane_plan["blocked_problems"]
        if not active_problems:
            # No actionable (active) problems — blocked and review_required
            # are both non-actionable, so count as effective green.
            self._record_green_round(round_id, problem_count=len(problems))
            logger.info(
                "[coordinator] Round %s EFFECTIVE GREEN — %d blocked, %d review, 0 active (%d consecutive)",
                round_id, len(blocked_problems), len(review_problems),
                self._coord_state.consecutive_green_rounds,
            )
            # v9: Close the round FSM so the next round can begin
            try:
                self._round_mgr.close_round()
            except Exception:
                pass
            self._save_state()
            # Check completion gates
            if self._coord_state.consecutive_green_rounds >= GREEN_ROUNDS_FOR_COMPLETED:
                if not self._formal_promote_ready_for_completion():
                    await self._run_standalone_promote_round(round_id)
                if await self._check_completion():
                    return
            if self._coord_state.consecutive_green_rounds >= GREEN_ROUNDS_FOR_MONITOR:
                await self._switch_mode(CoordinatorMode.MONITOR)
            # Fast-path: effective green rounds use shorter interval (60s)
            await asyncio.sleep(60)
            return

        # ---- Phase: ANALYSIS ----
        analysis_result = await self._dispatch_and_wait(
            phase=CoordinatorPhase.ANALYSIS,
            target_role=AgentRole.ANALYSIS.value,
            round_id=round_id,
            payload={"round_id": round_id, "problems": active_problems},
            timeout=180.0,
        )
        analyses = (analysis_result or {}).get("findings", [])
        # v8: Advance round FSM to ANALYSED
        try:
            self._round_mgr.advance(RoundPhase.ANALYSED, actionable_count=len(analyses))
        except InvalidTransitionError:
            pass
        actionable = [
            a for a in analyses
            if a.get("triage") == "auto_fix"
            and a.get("problem_id") not in self._coord_state.deferred_problems
            and a.get("current_status", ProblemStatus.ACTIVE.value)
            == ProblemStatus.ACTIVE.value
        ]
        if not actionable:
            logger.info("[coordinator] Round %s — no actionable problems after triage", round_id)
            self._append_round_history(
                round_id,
                "triaged_no_auto_fix",
                problem_count=len(problems),
                blocked_count=len(blocked_problems),
                review_count=len(review_problems),
                active_count=len(active_problems),
            )
            self._save_state()
            await asyncio.sleep(interval)
            return

        # ---- Phase: FIXING (with dynamic fan-out) ----
        # v8: Advance round FSM to FIXING
        try:
            self._round_mgr.advance(RoundPhase.FIXING)
        except InvalidTransitionError:
            pass

        # v10: Use WorkerPool for true parallel fix execution
        patches: List[Dict[str, Any]] = []
        if self._fix_pool is not None:
            # Build per-analysis tasks for the worker pool
            pool_tasks = []
            for analysis in actionable:
                task = {
                    "task_id": analysis.get("problem_id", "unknown"),
                    "round_id": round_id,
                    "analyses": [analysis],
                    "lane_ids": [analysis.get("lane_id", "")],
                    "write_scope": analysis.get("write_scope", []),
                }
                pool_tasks.append(task)
            pool_results = await self._fix_pool.execute_batch(pool_tasks)
            for wr in pool_results:
                if wr.success:
                    patches.extend(wr.result.get("findings", []))
                elif wr.error:
                    logger.warning(
                        "[coordinator] Worker %s failed on %s: %s",
                        wr.worker_id, wr.task_id, wr.error,
                    )
        else:
            # Fallback: original mailbox-based dispatch
            fan_out = self._desired_fan_out(len(actionable), self._coord_state.mode)
            fix_batches = self._build_fix_lane_batches(actionable, round_id)
            for wave_start in range(0, len(fix_batches), max(fan_out, 1)):
                wave = fix_batches[wave_start:wave_start + max(fan_out, 1)]
                if len(wave) == 1:
                    batch = wave[0]
                    fix_result = await self._dispatch_and_wait(
                        phase=CoordinatorPhase.FIXING,
                        target_role=AgentRole.FIX.value,
                        round_id=batch["round_id"],
                        payload={
                            "round_id": batch["round_id"],
                            "analyses": batch["analyses"],
                            "lane_ids": batch["lane_ids"],
                        },
                        timeout=600.0,
                    )
                    patches.extend((fix_result or {}).get("findings", []))
                    continue

                tasks = []
                for batch in wave:
                    tasks.append(self._dispatch_and_wait(
                        phase=CoordinatorPhase.FIXING,
                        target_role=AgentRole.FIX.value,
                        round_id=batch["round_id"],
                        payload={
                            "round_id": batch["round_id"],
                            "analyses": batch["analyses"],
                            "lane_ids": batch["lane_ids"],
                        },
                        timeout=600.0,
                    ))
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                for batch_result in batch_results:
                    if isinstance(batch_result, dict):
                        patches.extend(batch_result.get("findings", []))

        if not patches:
            self._coord_state.consecutive_fix_failures += 1
            self._coord_state.consecutive_green_rounds = 0
            self._coord_state.total_failures += 1
            self._append_round_history(
                round_id,
                "fix_failed",
                problem_count=len(problems),
                blocked_count=len(blocked_problems),
                review_count=len(review_problems),
                active_count=len(active_problems),
            )
            if self._coord_state.consecutive_fix_failures >= FIX_FAILURES_FOR_SAFE_HOLD:
                await self._enter_safe_hold("consecutive fix failures")
            await self._check_stall(had_progress=False)
            self._save_state()
            await asyncio.sleep(interval)
            return

        # ---- Phase: VERIFICATION ----
        # v8: Advance round FSM to FIXED → VERIFYING
        try:
            self._round_mgr.advance(RoundPhase.FIXED, patch_count=len(patches))
            self._round_mgr.advance(RoundPhase.VERIFYING)
        except InvalidTransitionError:
            pass
        verify_result = await self._dispatch_and_wait(
            phase=CoordinatorPhase.VERIFICATION,
            target_role=AgentRole.VERIFY.value,
            round_id=round_id,
            payload={"round_id": round_id, "patches": patches},
            timeout=600.0,
        )

        all_passed = (verify_result or {}).get("all_passed", False)
        if not all_passed:
            # Verification failed — ask FixAgent to rollback
            # v8: Fail round in FSM
            self._round_mgr.fail_round("verification_failed")
            self._coord_state.consecutive_fix_failures += 1
            self._coord_state.consecutive_green_rounds = 0
            self._coord_state.total_failures += 1
            self._append_round_history(
                round_id,
                "verification_failed",
                patch_count=len(patches),
                problem_count=len(problems),
            )
            logger.warning(
                "[coordinator] Round %s verification FAILED — rollback", round_id
            )
            await self._dispatch_fire_and_forget(
                target_role=AgentRole.FIX.value,
                round_id=round_id,
                payload={"round_id": round_id, "action": "rollback", "patches": patches},
            )
            await self._check_stall(had_progress=False)
            self._save_state()
            await asyncio.sleep(interval)
            return

        # ---- Phase: WRITEBACK ----
        # v8: Advance round FSM to VERIFIED → WRITING_BACK
        try:
            self._round_mgr.advance(RoundPhase.VERIFIED, verify_passed=True)
            self._round_mgr.advance(RoundPhase.WRITING_BACK)
        except InvalidTransitionError:
            pass
        wb_result = await self._dispatch_and_wait(
            phase=CoordinatorPhase.WRITEBACK,
            target_role=AgentRole.WRITEBACK.value,
            round_id=round_id,
            payload={"round_id": round_id, "patches": patches, "verify": verify_result},
            timeout=120.0,
        )

        expected_receipts = self._count_real_patchsets(patches)
        if not self._writeback_succeeded(wb_result, expected_receipts):
            # v8: Fail round in FSM
            self._round_mgr.fail_round(f"writeback_failed: {expected_receipts} expected, got {int((wb_result or {}).get('receipt_count') or 0)}")
            self._record_promotion_evidence(
                round_id=round_id,
                verify_result=verify_result,
                writeback_result=wb_result,
                promote_result=None,
            )
            self._coord_state.consecutive_green_rounds = 0
            self._coord_state.total_failures += 1
            self._append_round_history(
                round_id,
                "writeback_failed",
                patch_count=len(patches),
                expected_receipt_count=expected_receipts,
                receipt_count=int((wb_result or {}).get("receipt_count") or 0),
                errors=list((wb_result or {}).get("errors") or []),
                problem_count=len(problems),
            )
            await self._check_stall(had_progress=False)
            self._save_state()
            await asyncio.sleep(interval)
            return

        # ---- Phase: PROMOTION ----
        # v8: Advance round FSM to WRITTEN_BACK → PROMOTING
        try:
            self._round_mgr.advance(
                RoundPhase.WRITTEN_BACK,
                writeback_receipt_count=int((wb_result or {}).get("receipt_count") or 0),
            )
            self._round_mgr.advance(RoundPhase.PROMOTING)
        except InvalidTransitionError:
            pass
        promote_result = await self._dispatch_and_wait(
            phase=CoordinatorPhase.PROMOTION,
            target_role=AgentRole.PROMOTE.value,
            round_id=round_id,
            payload={
                "round_id": round_id,
                "writeback": (wb_result or {}),
                "verify": verify_result,
            },
            timeout=120.0,
        )
        self._record_promotion_evidence(
            round_id=round_id,
            verify_result=verify_result,
            writeback_result=wb_result,
            promote_result=promote_result,
        )
        if not self._promotion_succeeded(promote_result):
            self._coord_state.consecutive_green_rounds = 0
            self._coord_state.total_failures += 1
            self._append_round_history(
                round_id,
                "promotion_blocked",
                patch_count=len(patches),
                receipt_count=int((wb_result or {}).get("receipt_count") or 0),
                promote_reason=str((promote_result or {}).get("reason") or ""),
                targets_promoted=list((promote_result or {}).get("targets_promoted") or []),
                problem_count=len(problems),
            )
            await self._check_stall(had_progress=False)
            self._save_state()
            await asyncio.sleep(interval)
            return

        # Record success
        self._coord_state.consecutive_fix_failures = 0
        self._coord_state.total_fixes += len(patches)
        # v8: Advance round FSM to PROMOTED → CLOSED
        try:
            self._round_mgr.advance(
                RoundPhase.PROMOTED,
                promote_approved=True,
                promote_targets=list((promote_result or {}).get("targets_promoted") or []),
            )
            self._round_mgr.close_round()
        except InvalidTransitionError:
            pass
        # v8: Record in system control state
        self._round_mgr.control.record_fix(count=len(patches))
        self._round_mgr.control.record_green_round()
        # v8: Record in team memory
        self._team_memory.write(
            agent_id=self.agent_id,
            category="fix_result",
            content=f"Round {round_id}: {len(patches)} patches verified and promoted",
            round_id=round_id,
        )
        # v8: Release claims for this round
        self._claim_registry.release_by_round(round_id)
        # v8: Update residency controller
        if self._residency.is_active:
            self._residency.record_fix_success(count=len(patches))
        # v8: Check control state promotion threshold
        if self._round_mgr.control.should_promote():
            try:
                self._round_mgr.advance_control(ControlState.PROMOTE_READY)
            except InvalidTransitionError:
                pass
        completed_lanes = sorted({p.get("lane_id") for p in patches if p.get("lane_id")})
        for lane_id in completed_lanes:
            self._set_lane_status(lane_id, ProblemStatus.COMPLETED.value)
        self._record_green_round(
            round_id,
            patch_count=len(patches),
            problem_count=len(problems),
            lane_ids=completed_lanes,
            blocked_count=len(blocked_problems),
            review_count=len(review_problems),
        )
        await self._check_stall(had_progress=True)

        # v4: Refresh shared artifacts after promotion
        await self._refresh_shared_artifacts()

        self._save_state()

        logger.info(
            "[coordinator] Round %s COMPLETE — green=%d, autonomy=%.1f%%",
            round_id,
            self._coord_state.consecutive_green_rounds,
            self._coord_state.autonomy_index * 100,
        )

        # Post-promote: trigger immediate incremental scan in MONITOR mode
        if CoordinatorMode(self._coord_state.mode) == CoordinatorMode.MONITOR:
            await self._dispatch_fire_and_forget(
                target_role=AgentRole.DISCOVERY.value,
                round_id=round_id,
                payload={"round_id": round_id, "mode": "incremental"},
            )

        await asyncio.sleep(interval)

    # ------------------------------------------------------------------
    # v7: Standalone Promote Round (for MONITOR green-only rounds)
    # ------------------------------------------------------------------

    async def _run_standalone_promote_round(self, round_id: str) -> None:
        """Execute a promote sub-round in a green MONITOR round.

        In a successful MONITOR round there are no patches, so the normal
        WRITEBACK → PROMOTE pipeline is never reached.  This method injects
        a minimal verify+promote cycle so that formal_promote evidence is
        recorded and the completion gate can be cleared.

        Errors are caught and logged; they must not interrupt the normal
        green-round flow.
        """
        try:
            logger.info(
                "[coordinator] Running standalone promote sub-round for %s", round_id,
            )
            # Step 1: Run VerifyAgent with empty patches to get a fresh verify result.
            verify_result = await self._dispatch_and_wait(
                phase=CoordinatorPhase.PROMOTION,
                target_role=AgentRole.VERIFY.value,
                round_id=round_id,
                payload={"round_id": round_id, "patches": []},
                timeout=60.0,
            )
            verify_result = verify_result if isinstance(verify_result, dict) else {}

            # Step 2: Run PromoteAgent with the verify result and an empty writeback.
            promote_result = await self._dispatch_and_wait(
                phase=CoordinatorPhase.PROMOTION,
                target_role=AgentRole.PROMOTE.value,
                round_id=round_id,
                payload={
                    "round_id": round_id,
                    "verify": verify_result,
                    "writeback": {},
                },
                timeout=60.0,
            )

            # Step 3: Record the evidence regardless of outcome so gates can inspect it.
            self._record_promotion_evidence(
                round_id=round_id,
                verify_result=verify_result,
                writeback_result={},
                promote_result=promote_result,
            )
            self._save_state()

            targets = list((promote_result or {}).get("targets_promoted") or [])
            logger.info(
                "[coordinator] Standalone promote sub-round done: approved=%s, targets=%s",
                bool((promote_result or {}).get("approved")), targets,
            )
        except Exception as exc:
            logger.warning(
                "[coordinator] Standalone promote sub-round failed (non-fatal): %s", exc,
            )

    # ------------------------------------------------------------------
    # v4: Shared Artifact Refresh (post-promote)
    # ------------------------------------------------------------------

    async def _refresh_shared_artifacts(self) -> None:
        """Refresh shared artifacts after a successful promotion.

        Runs catalog rebuild and continuous audit in subprocess to keep
        AGENTS.md-mandated shared artifacts aligned:
        - output/junit.xml  (refreshed by pytest, not here)
        - app/governance/catalog_snapshot.json  (rebuilt here)
        - output/blind_spot_audit.json  (refreshed by audit)
        - github/automation/continuous_audit/latest_run.json  (refreshed by audit)
        """
        repo = str(self.config.repo_root)

        # 1. Rebuild feature catalog
        catalog_script = self.config.repo_root / "app" / "governance" / "build_feature_catalog.py"
        if catalog_script.exists():
            try:
                proc = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: subprocess.run(
                        ["python", str(catalog_script)],
                        cwd=repo,
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        timeout=60,
                    ),
                )
                if proc.returncode == 0:
                    logger.info("[coordinator] catalog_snapshot.json refreshed")
                else:
                    logger.warning("[coordinator] catalog rebuild failed: %s", proc.stderr[:200])
            except Exception as exc:
                logger.warning("[coordinator] catalog rebuild error: %s", exc)

        # 2. Run continuous repo audit (shared-artifacts focus)
        audit_script = self.config.repo_root / "scripts" / "continuous_repo_audit.py"
        if audit_script.exists():
            try:
                proc = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: subprocess.run(
                        ["python", str(audit_script), "--mode", "quick"],
                        cwd=repo,
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        timeout=120,
                    ),
                )
                if proc.returncode == 0:
                    logger.info("[coordinator] continuous audit refreshed")
                else:
                    logger.warning("[coordinator] continuous audit failed: %s", proc.stderr[:200])
            except Exception as exc:
                logger.warning("[coordinator] continuous audit error: %s", exc)

    # ------------------------------------------------------------------
    # v4: AI Pressure Backoff
    # ------------------------------------------------------------------

    async def _apply_ai_backoff(self, target_role: str) -> None:
        """Wait if the target role is under AI pressure (429/503).

        Called before dispatching to analysis/fix agents to avoid
        hammering rate-limited AI endpoints.
        """
        state = self._ai_pressure_state.get(target_role)
        if not state:
            return

        backoff_until = state.get("backoff_until", 0.0)
        now = time.monotonic()

        if now >= backoff_until:
            # Backoff period expired — check if we should reset
            if now - state.get("last_pressure_at", 0) > AI_BACKOFF_RESET_AFTER:
                del self._ai_pressure_state[target_role]
            return

        wait = min(backoff_until - now, AI_BACKOFF_MAX)
        logger.info(
            "[coordinator] AI backoff for %s: waiting %.0fs (attempt %d)",
            target_role, wait, state.get("attempts", 0),
        )
        await asyncio.sleep(wait)

    def _record_ai_pressure(self, target_role: str) -> None:
        """Record that target_role encountered AI rate-limit (429/503)."""
        state = self._ai_pressure_state.get(target_role, {"attempts": 0})
        state["attempts"] = state.get("attempts", 0) + 1
        state["last_pressure_at"] = time.monotonic()
        # Exponential backoff: base * 2^(attempts-1), capped
        backoff = min(
            AI_BACKOFF_BASE * (2 ** (state["attempts"] - 1)),
            AI_BACKOFF_MAX,
        )
        state["backoff_until"] = time.monotonic() + backoff
        self._ai_pressure_state[target_role] = state
        logger.warning(
            "[coordinator] AI pressure recorded for %s: attempt=%d, backoff=%.0fs",
            target_role, state["attempts"], backoff,
        )

    # ------------------------------------------------------------------
    # Dispatch helpers
    # ------------------------------------------------------------------

    async def _dispatch_and_wait(
        self,
        phase: CoordinatorPhase,
        target_role: str,
        round_id: str,
        payload: Dict[str, Any],
        timeout: float = 300.0,
    ) -> Optional[Dict[str, Any]]:
        """Send TASK_DISPATCH and wait for TASK_RESULT from the target agent."""
        self._coord_state.phase = phase.value
        self._save_state()

        # v4: Update round progress
        if self._current_round_progress:
            self._current_round_progress.phase = phase.value
            self._current_round_progress.phase_started_at = datetime.now(timezone.utc).isoformat()

        # v4: Apply AI pressure backoff for AI-calling agents
        if target_role in (AgentRole.ANALYSIS.value, AgentRole.FIX.value):
            await self._apply_ai_backoff(target_role)

        await self.mailbox.send(AgentMessage(
            source=self.agent_id,
            target=target_role,
            msg_type=MessageType.TASK_DISPATCH.value,
            payload=payload,
        ))

        # Wait for result
        result_msg = await self.mailbox.receive(
            predicate=lambda m: (
                m.msg_type == MessageType.TASK_RESULT.value
                and m.payload.get("agent_role") == target_role
                and m.payload.get("round_id") == round_id
            ),
            timeout=timeout,
        )

        if result_msg is None:
            logger.warning(
                "[coordinator] Timeout waiting for %s result (round=%s)",
                target_role, round_id,
            )
            return None

        result_data = result_msg.payload
        status = result_data.get("status", "")

        # v4: Detect AI pressure from result errors
        errors = result_data.get("errors", [])
        if errors and target_role in (AgentRole.ANALYSIS.value, AgentRole.FIX.value):
            error_text = " ".join(str(e) for e in errors)
            if "429" in error_text or "503" in error_text or "rate" in error_text.lower():
                self._record_ai_pressure(target_role)

        if status == AgentState.FAILED.value:
            logger.warning(
                "[coordinator] %s reported FAILED: %s",
                target_role, errors,
            )
            return None

        return result_data.get("artifacts", result_data)

    async def _dispatch_fire_and_forget(
        self,
        target_role: str,
        round_id: str,
        payload: Dict[str, Any],
    ) -> None:
        """Send TASK_DISPATCH without waiting for result."""
        await self.mailbox.send(AgentMessage(
            source=self.agent_id,
            target=target_role,
            msg_type=MessageType.TASK_DISPATCH.value,
            payload=payload,
        ))

    # ------------------------------------------------------------------
    # Mode management
    # ------------------------------------------------------------------

    async def _switch_mode(self, new_mode: CoordinatorMode) -> None:
        old = self._coord_state.mode
        self._coord_state.mode = new_mode.value
        # v9: Reset no-progress counter on mode transitions to stable states
        # to prevent stale counters from prior fix sessions triggering
        # immediate SAFE_HOLD or infra_doctor after entering MONITOR/COMPLETED.
        if new_mode in (CoordinatorMode.MONITOR, CoordinatorMode.COMPLETED):
            self._coord_state.consecutive_no_progress_rounds = 0
        self._save_state()
        logger.info("[coordinator] Mode switch: %s → %s", old, new_mode.value)

        # Broadcast to all agents
        await self.mailbox.send(AgentMessage(
            source=self.agent_id,
            target="*",
            msg_type=MessageType.MODE_SWITCH.value,
            payload={"mode": new_mode.value, "previous": old},
        ))

    async def _enter_safe_hold(self, reason: str) -> None:
        logger.warning("[coordinator] Entering SAFE_HOLD: %s", reason)
        # v4: Signal abort to all running agents before mode switch
        await self._abort_all_agents(reason)
        await self._switch_mode(CoordinatorMode.SAFE_HOLD)
        # v3: Record problem count at entry for smart exit comparison
        self._coord_state.last_safe_hold_problem_count = max(
            self._coord_state.last_safe_hold_problem_count, 1
        )
        self._save_state()
        await self.mailbox.send(AgentMessage(
            source=self.agent_id,
            target="coordinator",  # self-note for logging
            msg_type=MessageType.ESCALATION.value,
            payload={"level": "safe_hold", "reason": reason},
        ))
        # v3: Write structured escalation to output file
        self._write_escalation("safe_hold", "coordinator", reason)

    async def _abort_all_agents(self, reason: str) -> None:
        """Signal abort to all registered agent instances (v4 cascade)."""
        for agent_id, agent in self._agent_instances.items():
            if hasattr(agent, "_abort_event"):
                agent._abort_event.set()
                logger.info("[coordinator] Abort signalled to %s: %s", agent_id, reason)

    def _write_escalation(
        self, level: str, problem_id: str, reason: str, suggested_action: str = "",
    ) -> None:
        """Append a structured escalation entry to ``output/escort_escalations.json``.

        Uses atomic write (write to temp + rename) to avoid data loss on
        concurrent or interrupted writes.
        """
        import tempfile as _tmpmod

        path = self.config.repo_root / "output" / "escort_escalations.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            entries = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
        except Exception:
            entries = []
        entries.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "problem_id": problem_id,
            "reason": reason,
            "suggested_action": suggested_action,
            "round_id": self._coord_state.current_round_id,
            "mode": self._coord_state.mode,
        })
        # Keep last 100 entries
        entries = entries[-100:]
        # Atomic write: temp file + rename
        tmp_path = None
        try:
            fd, tmp_path = _tmpmod.mkstemp(
                dir=str(path.parent), suffix=".tmp", prefix=".esc_"
            )
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(entries, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, str(path))
        except Exception as exc:
            logger.warning("[coordinator] Failed to write escalation: %s", exc)
            # Cleanup temp on failure
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Green round tracking
    # ------------------------------------------------------------------

    def _append_round_history(self, round_id: str, result: str, **extra: Any) -> None:
        entry = {
            "round_id": round_id,
            "result": result,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": self._coord_state.mode,
            "phase": self._coord_state.phase,
        }
        # v4: Include round progress snapshot if available
        if self._current_round_progress:
            entry["progress"] = self._current_round_progress.to_dict()
        # v4: Include AI pressure state
        if self._ai_pressure_state:
            entry["ai_pressure"] = {
                k: v.get("attempts", 0)
                for k, v in self._ai_pressure_state.items()
            }
        entry.update({k: v for k, v in extra.items() if v is not None})
        self._coord_state.round_history.append(entry)
        if len(self._coord_state.round_history) > 100:
            self._coord_state.round_history = self._coord_state.round_history[-100:]

    def _record_green_round(self, round_id: str, **extra: Any) -> None:
        self._coord_state.consecutive_green_rounds += 1
        # Sustained green rounds prove system health — clear stale fix failures
        # that accumulated before probe classification was corrected.
        self._coord_state.consecutive_fix_failures = 0
        self._append_round_history(round_id, "green", **extra)
        self._save_state()

    def _plan_execution_lanes(self, problems: List[Dict[str, Any]]) -> Dict[str, Any]:
        execution_lanes: Dict[str, Dict[str, Any]] = {}
        active_problems: List[Dict[str, Any]] = []
        blocked_problems: List[Dict[str, Any]] = []
        review_problems: List[Dict[str, Any]] = []

        for problem in problems:
            lane_id = (
                problem.get("lane_id")
                or problem.get("task_family")
                or problem.get("family")
                or "general"
            )
            status = problem.get("current_status", ProblemStatus.ACTIVE.value)
            lane = execution_lanes.setdefault(
                lane_id,
                {
                    "lane_id": lane_id,
                    "title": self._lane_title(lane_id, problem.get("task_family", "")),
                    "task_family": problem.get("task_family") or problem.get("family", ""),
                    "status": status,
                    "problem_count": 0,
                    "problem_ids": [],
                    "write_scope": [],
                },
            )
            lane["problem_count"] += 1
            lane["problem_ids"].append(problem.get("problem_id"))
            lane["write_scope"] = sorted(
                set(lane["write_scope"]) | set(problem.get("write_scope", []))
            )

            existing_status = lane.get("status", ProblemStatus.ACTIVE.value)
            if existing_status != ProblemStatus.ACTIVE.value:
                if status == ProblemStatus.ACTIVE.value:
                    lane["status"] = status
                elif (
                    existing_status == ProblemStatus.BLOCKED.value
                    and status == ProblemStatus.REVIEW_REQUIRED.value
                ):
                    lane["status"] = status

            if status == ProblemStatus.BLOCKED.value:
                blocked_problems.append(problem)
            elif status == ProblemStatus.REVIEW_REQUIRED.value:
                review_problems.append(problem)
            else:
                active_problems.append(problem)

        return {
            "execution_lanes": execution_lanes,
            "active_problems": active_problems,
            "blocked_problems": blocked_problems,
            "review_problems": review_problems,
        }

    def _build_fix_lane_batches(
        self,
        actionable: List[Dict[str, Any]],
        round_id: str,
    ) -> List[Dict[str, Any]]:
        batches: List[Dict[str, Any]] = []
        ordered = sorted(
            actionable,
            key=lambda item: (
                item.get("lane_id") or item.get("problem_id", ""),
                item.get("problem_id", ""),
            ),
        )

        for analysis in ordered:
            lane_id = analysis.get("lane_id") or analysis.get("problem_id", "general")
            scope = analysis.get("write_scope", [])
            placed = False

            for batch in batches:
                if self._scopes_overlap(batch["write_scope"], scope):
                    continue
                batch["analyses"].append(analysis)
                batch["lane_ids"].append(lane_id)
                batch["write_scope"] = sorted(set(batch["write_scope"]) | set(scope))
                placed = True
                break

            if placed:
                continue

            batches.append(
                {
                    "round_id": f"{round_id}-lane{len(batches)}",
                    "analyses": [analysis],
                    "lane_ids": [lane_id],
                    "write_scope": list(scope),
                }
            )

        return batches or [{"round_id": round_id, "analyses": actionable, "lane_ids": [], "write_scope": []}]

    def _set_lane_status(self, lane_id: str, status: str) -> None:
        lane = self._coord_state.execution_lanes.get(lane_id)
        if not lane:
            return
        lane["status"] = status
        lane["updated_at"] = datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _lane_title(lane_id: str, task_family: str) -> str:
        titles = {
            "gov_registry": "治理注册修复",
            "gov_mapping": "治理映射修复",
            "repo_hygiene": "仓库混合态审查",
            "runtime_blocked": "外部运行依赖",
        }
        return titles.get(lane_id, task_family or lane_id)

    def _lanes_can_run_in_parallel(
        self,
        left_scope: List[str],
        right_scope: List[str],
    ) -> bool:
        return not self._scopes_overlap(left_scope, right_scope)

    def _scopes_overlap(self, left_scope: List[str], right_scope: List[str]) -> bool:
        if not left_scope or not right_scope:
            return False
        for left in left_scope:
            for right in right_scope:
                if self._pattern_overlaps(left, right):
                    return True
        return False

    @staticmethod
    def _pattern_overlaps(left: str, right: str) -> bool:
        if left == right:
            return True

        left_root = left.split("**", 1)[0].rstrip("*").rstrip("/")
        right_root = right.split("**", 1)[0].rstrip("*").rstrip("/")
        if not left_root or not right_root:
            return True

        return (
            left.startswith(right_root)
            or right.startswith(left_root)
            or left_root.startswith(right_root)
            or right_root.startswith(left_root)
        )

    # ------------------------------------------------------------------
    # Agent health management
    # ------------------------------------------------------------------

    async def _health_check_loop(self) -> None:
        """Periodically check agent heartbeats, restart unhealthy agents,
        and re-run preflight checks to detect service recovery."""
        _PREFLIGHT_RECHECK_INTERVAL = float(os.environ.get("PREFLIGHT_RECHECK_INTERVAL", "300"))
        while not self._shutdown_requested:
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)
            try:
                await self._process_heartbeats()
                self._check_agent_health()
                # v5: Periodic preflight re-check for service recovery
                elapsed = time.monotonic() - self._last_preflight_ts
                if elapsed >= _PREFLIGHT_RECHECK_INTERVAL:
                    preflight = await self._preflight_checks()
                    self._last_preflight = preflight
                    self._last_preflight_ts = time.monotonic()
                    self._save_state()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("[coordinator] Health check error")

    async def _process_heartbeats(self) -> None:
        """Drain all pending heartbeat messages and update registry."""
        heartbeats = await self.mailbox.poll_all(
            predicate=lambda m: m.msg_type == MessageType.HEARTBEAT.value
        )
        now = time.monotonic()
        for hb in heartbeats:
            agent_id = hb.payload.get("agent_id", hb.source)
            self._agent_heartbeats[agent_id] = now
            if agent_id not in self._coord_state.agents_registered:
                self._coord_state.agents_registered.append(agent_id)

        # Update healthy list
        self._coord_state.agents_healthy = [
            aid for aid, ts in self._agent_heartbeats.items()
            if now - ts < HEARTBEAT_MISSING_SECONDS
        ]

    def _check_agent_health(self) -> None:
        """Check agent health and attempt auto-restart for missing agents (v2)."""
        now = time.monotonic()
        for aid, ts in list(self._agent_heartbeats.items()):
            if now - ts > HEARTBEAT_MISSING_SECONDS:
                logger.warning(
                    "[coordinator] Agent %s missing heartbeat for %.0fs",
                    aid, now - ts,
                )
                # v2: Attempt auto-restart
                asyncio.create_task(
                    self._try_restart_agent(aid),
                    name=f"restart-{aid}",
                )

    # ------------------------------------------------------------------
    # Message handling (additional control messages)
    # ------------------------------------------------------------------

    async def on_message(self, msg: AgentMessage) -> None:
        if msg.msg_type == MessageType.ESCALATION.value:
            level = msg.payload.get("level", "")
            logger.warning(
                "[coordinator] Escalation from %s: %s",
                msg.source, msg.payload,
            )
            # v2: Handle deferred problems
            if level == "deferred":
                problem_id = msg.payload.get("problem_id", "")
                if problem_id and problem_id not in self._coord_state.deferred_problems:
                    self._coord_state.deferred_problems.append(problem_id)
                    self._save_state()
                    logger.info(
                        "[coordinator] Problem %s deferred — %d total deferred",
                        problem_id, len(self._coord_state.deferred_problems),
                    )
            return

        if msg.msg_type == MessageType.PROMOTE_ROLLBACK.value:
            logger.warning(
                "[coordinator] Promote rollback from %s (green=%d)",
                msg.source, self._coord_state.consecutive_green_rounds,
            )
            # Only reset green when there are actual active problems —
            # rollbacks from standalone promote in blocked-only rounds
            # should not disrupt accumulated green rounds.
            active_lanes = [
                l for l in self._coord_state.execution_lanes.values()
                if l.get("status") == ProblemStatus.ACTIVE.value
            ]
            if active_lanes:
                self._coord_state.consecutive_green_rounds = 0
                await self._switch_mode(CoordinatorMode.FIX)
            else:
                logger.info("[coordinator] Ignoring promote rollback — no active lanes")
            return

    # ------------------------------------------------------------------
    # Control Plane Projection
    # ------------------------------------------------------------------

    def _current_promote_target_state(self) -> tuple[str, str | None]:
        # v7 A4: If in-memory state is already "doc22" (advanced by
        # _record_promotion_evidence after a successful status-note commit),
        # return immediately — no need to read the file.
        mem_mode = getattr(self._coord_state, "promote_target_mode", "infra").strip().lower()
        if mem_mode == "doc22":
            return "doc22", None
        # Fall back to the persisted control-plane JSON so that an existing
        # "doc22" written by a previous session is preserved on restart.
        cp_path = self.config.repo_root / "automation" / "control_plane" / "current_state.json"
        if not cp_path.exists():
            return "infra", "CONTROL_PLANE_STATE_MISSING"
        try:
            payload = json.loads(cp_path.read_text(encoding="utf-8"))
        except Exception:
            return "infra", "CONTROL_PLANE_STATE_INVALID"
        if not isinstance(payload, dict):
            return "infra", "CONTROL_PLANE_STATE_INVALID"
        mode = str(payload.get("promote_target_mode") or "").strip().lower()
        if mode in {"infra", "doc22"}:
            return mode, None
        return "infra", "CONTROL_PLANE_STATE_INVALID"

    def _current_promote_target_mode(self) -> str:
        return self._current_promote_target_state()[0]

    @staticmethod
    def _count_real_patchsets(patches: List[Dict[str, Any]]) -> int:
        count = 0
        for patch_set in patches:
            entries = patch_set.get("patches") if isinstance(patch_set, dict) else None
            if not isinstance(entries, list):
                continue
            if any(
                isinstance(item, dict)
                and str(item.get("path") or "").strip()
                and not str(item.get("path") or "").startswith("__analysis__")
                for item in entries
            ):
                count += 1
        return count

    @staticmethod
    def _writeback_succeeded(
        writeback_result: Dict[str, Any] | None,
        expected_receipts: int,
    ) -> bool:
        if expected_receipts <= 0:
            return False
        payload = writeback_result if isinstance(writeback_result, dict) else {}
        if payload.get("errors"):
            return False
        receipt_count = int(payload.get("receipt_count") or 0)
        return receipt_count >= expected_receipts

    @staticmethod
    def _promotion_succeeded(promote_result: Dict[str, Any] | None) -> bool:
        payload = promote_result if isinstance(promote_result, dict) else {}
        if not bool(payload.get("approved")):
            return False
        targets = payload.get("targets_promoted")
        if not isinstance(targets, list):
            return False
        return any(str(item).strip() for item in targets)

    def _record_promotion_evidence(
        self,
        *,
        round_id: str,
        verify_result: Dict[str, Any] | None,
        writeback_result: Dict[str, Any] | None,
        promote_result: Dict[str, Any] | None,
    ) -> None:
        verify_payload = verify_result if isinstance(verify_result, dict) else {}
        writeback_payload = writeback_result if isinstance(writeback_result, dict) else {}
        promote_payload = promote_result if isinstance(promote_result, dict) else {}

        runtime_gates = verify_payload.get("runtime_gates")
        if not isinstance(runtime_gates, dict):
            runtime_gates = {}
        shared_artifact = runtime_gates.get("shared_artifact_promote")
        if not isinstance(shared_artifact, dict):
            shared_artifact = {}

        runtime_status = str(runtime_gates.get("status") or "unknown").strip().lower() or "unknown"
        artifacts_aligned = bool(verify_payload.get("artifacts_aligned"))
        shared_artifact_allowed = bool(shared_artifact.get("allowed")) if "allowed" in shared_artifact else artifacts_aligned
        writeback_receipt_count = int(writeback_payload.get("receipt_count") or 0)
        audit_run_id = str(verify_payload.get("audit_run_id") or round_id or "").strip()
        verify_all_passed = bool(verify_payload.get("all_passed"))
        public_runtime_status = str(
            verify_payload.get("public_runtime_status")
            or ("READY" if runtime_status == "ready" else "BLOCKED")
        ).strip()
        shadow_ready = bool(audit_run_id) and verify_all_passed and runtime_status == "ready"

        targets_promoted = [
            str(item).strip()
            for item in promote_payload.get("targets_promoted", [])
            if str(item).strip()
        ]
        status_note_committed = "status-note" in targets_promoted or "status-note-local" in targets_promoted
        shared_artifact_recorded = "shared-artifact" in targets_promoted
        current_layer_committed = "current-layer" in targets_promoted
        doc22_committed = "doc22" in targets_promoted

        formal_state = "blocked"
        if doc22_committed:
            formal_state = "doc22_published"
        elif current_layer_committed:
            formal_state = "current_layer_published"
        elif status_note_committed:
            formal_state = "status_note_published"
        elif shadow_ready:
            formal_state = "shadow_validated"

        attempted_at = datetime.now(timezone.utc).isoformat()
        self._coord_state.last_promote_round_id = round_id
        self._coord_state.last_promote_time = attempted_at
        self._coord_state.last_audit_run_id = audit_run_id
        self._coord_state.last_shadow_validation = {
            "audit_run_id": audit_run_id or None,
            "verify_all_passed": verify_all_passed,
            "runtime_status": runtime_status,
            "public_runtime_status": public_runtime_status,
            "artifacts_aligned": artifacts_aligned,
            "shared_artifact_allowed": shared_artifact_allowed,
            "writeback_receipt_count": writeback_receipt_count,
            "ready": shadow_ready,
        }
        self._coord_state.last_formal_promote = {
            "round_id": round_id,
            "attempted_at": attempted_at,
            "tier": int(promote_payload.get("tier") or 0),
            "approved": bool(promote_payload.get("approved")),
            "reason": str(promote_payload.get("reason") or "").strip(),
            "targets_promoted": targets_promoted,
            "status_note_committed": status_note_committed,
            "shared_artifact_recorded": shared_artifact_recorded,
            "current_layer_committed": current_layer_committed,
            "doc22_committed": doc22_committed,
            "ready_for_doc22": shadow_ready and artifacts_aligned and shared_artifact_allowed and writeback_receipt_count > 0,
            "state": formal_state,
        }
        # v7 A4: Once any formal promote milestone is reached, automatically
        # advance promote_target_mode so writeback_service unlocks doc22 writes.
        if status_note_committed and self._coord_state.promote_target_mode != "doc22":
            self._coord_state.promote_target_mode = "doc22"
            logger.info(
                "[coordinator] promote_target_mode advanced to doc22 "
                "after status-note committed in round %s", round_id,
            )

    def _shadow_validation_summary(self) -> Dict[str, Any]:
        summary = dict(self._coord_state.last_shadow_validation or {})
        summary.setdefault("audit_run_id", self._coord_state.last_audit_run_id or None)
        summary.setdefault("verify_all_passed", False)
        summary.setdefault("runtime_status", "unknown")
        summary.setdefault("public_runtime_status", "UNKNOWN")
        summary.setdefault("artifacts_aligned", False)
        summary.setdefault("shared_artifact_allowed", False)
        summary.setdefault("writeback_receipt_count", 0)
        summary.setdefault("ready", False)
        return summary

    def _formal_promote_summary(self) -> Dict[str, Any]:
        summary = dict(self._coord_state.last_formal_promote or {})
        summary.setdefault("round_id", self._coord_state.last_promote_round_id or None)
        summary.setdefault("attempted_at", self._coord_state.last_promote_time or None)
        summary.setdefault("tier", 0)
        summary.setdefault("approved", False)
        summary.setdefault("reason", "")
        summary.setdefault("targets_promoted", [])
        summary.setdefault("status_note_committed", False)
        summary.setdefault("shared_artifact_recorded", False)
        summary.setdefault("current_layer_committed", False)
        summary.setdefault("doc22_committed", False)
        summary.setdefault("ready_for_doc22", False)
        summary.setdefault("state", "not_attempted")
        return summary

    def _completion_evidence(self) -> Dict[str, Any]:
        cs = self._coord_state
        return {
            "green_rounds": cs.consecutive_green_rounds,
            "green_round_goal": GREEN_ROUNDS_FOR_COMPLETED,
            "autonomy_index": cs.autonomy_index,
            "autonomy_goal": 0.85,
            "consecutive_fix_failures": cs.consecutive_fix_failures,
            "deferred_problem_count": len(cs.deferred_problems),
            "blocked_problem_count": len(cs.blocked_problems),
            "completion_blockers": self._completion_blockers(),
            "formal_promote_required": True,
            "formal_promote_ready": self._formal_promote_ready_for_completion(),
            "shadow_validation": self._shadow_validation_summary(),
            "formal_promote": self._formal_promote_summary(),
            "gate_failures": self._completion_gate_failures(),
            "goal_reached": self._completion_goal_reached(),
        }

    def _formal_promote_ready_for_completion(self) -> bool:
        formal = self._formal_promote_summary()
        if not bool(formal.get("approved")):
            return False
        return any(
            bool(formal.get(key))
            for key in ("status_note_committed", "current_layer_committed", "doc22_committed")
        )

    def _completion_gate_failures(self) -> List[str]:
        cs = self._coord_state
        failures: List[str] = []
        if cs.consecutive_green_rounds < GREEN_ROUNDS_FOR_COMPLETED:
            failures.append("green_rounds_below_threshold")
        if (cs.consecutive_fix_failures or 0) != 0:
            failures.append("fix_failures_present")
        if cs.autonomy_index < 0.85:
            failures.append("autonomy_below_threshold")
        if cs.deferred_problems:
            failures.append("deferred_problems_present")
        if self._completion_blockers():
            failures.append("active_or_review_lanes_present")
        # formal_promote is only required when fixes were actually applied;
        # if all lanes are blocked/review (effective-green), there is nothing
        # to promote and the gate can be skipped.
        has_completed_lanes = any(
            l.get("status") == ProblemStatus.COMPLETED.value
            for l in cs.execution_lanes.values()
        )
        if has_completed_lanes and not self._formal_promote_ready_for_completion():
            failures.append("formal_promote_evidence_missing")
        # v5: Require healthy helper services (unless DRY_RUN_SERVICES)
        # v9: Also skip this gate when running in local degraded mode
        # (indicated by formal_promote containing local targets).
        dry_run = os.environ.get("DRY_RUN_SERVICES", "").lower() in ("1", "true", "yes")
        local_promote = any(
            "local" in str(t)
            for t in (self._formal_promote_summary().get("targets_promoted") or [])
        )
        if not dry_run and not local_promote:
            pool = self._build_provider_pool_status()
            if not pool.get("ready"):
                failures.append("helper_services_unhealthy")
        return failures

    def _build_provider_pool_status(self) -> Dict[str, Any]:
        """Build truthful provider_pool status from last preflight results.

        v5: Replaces the hardcoded ``{"ready": True}`` with actual service
        health data.  If no preflight has ever run, reports ``unknown``.
        """
        pf = self._last_preflight
        if not pf:
            return {"ready": False, "status": "unknown", "stage": "no_preflight", "error": "preflight not yet run"}

        svc_checks = {k: v for k, v in pf.items() if k.startswith("svc_")}
        all_svc_ok = all(svc_checks.values()) if svc_checks else False
        failed_svcs = [k.removeprefix("svc_") for k, v in svc_checks.items() if not v]

        if all_svc_ok:
            return {"ready": True, "status": "ok", "stage": "done", "error": None}

        return {
            "ready": False,
            "status": "degraded",
            "stage": "preflight_failures",
            "error": f"services down: {', '.join(failed_svcs)}",
            "failed_services": failed_svcs,
        }

    def _build_runtime_projection(self) -> Dict[str, Any]:
        cs = self._coord_state
        last_summary = cs.round_history[-1] if cs.round_history else {}
        completion_blockers = self._completion_blockers()
        goal_reached = self._completion_goal_reached()
        promote_target_mode, promote_target_reason = self._current_promote_target_state()
        completion_evidence = self._completion_evidence()
        shadow_validation = completion_evidence["shadow_validation"]
        formal_promote = completion_evidence["formal_promote"]

        blocked_reason = None
        if completion_blockers:
            blocked_reason = "; ".join(
                f"{item['lane_id']}:{item['status']}" for item in completion_blockers
            )
        elif cs.blocked_problems:
            blocked_reason = f"{len(cs.blocked_problems)} blocked problems remain external/manual"
        elif completion_evidence["gate_failures"]:
            blocked_reason = "; ".join(completion_evidence["gate_failures"])

        return {
            "_schema": "infra_promote_v1",
            "_note": "Projected by EscortTeam CoordinatorAgent.",
            "promote_target_mode": promote_target_mode,
            "promote_target_mode_reason": promote_target_reason,
            "last_promote_round_id": cs.last_promote_round_id or None,
            "last_promote_at": cs.last_promote_time or None,
            "consecutive_fix_success_count": cs.consecutive_green_rounds,
            "consecutive_verified_problem_fixes": cs.consecutive_green_rounds,
            "consecutive_green_rounds": cs.consecutive_green_rounds,
            "goal_progress_count": cs.consecutive_green_rounds,
            "fix_goal": 10,
            "goal_reached": goal_reached,
            "goal_ever_reached": goal_reached or cs.mode == CoordinatorMode.COMPLETED.value,
            "success_goal_metric": "verified_problem_count",
            "mode": cs.mode,
            "phase": cs.phase,
            "current_round_id": cs.current_round_id,
            "blocked_reason": blocked_reason,
            "provider_pool": self._build_provider_pool_status(),
            "last_audit_run_id": cs.last_audit_run_id or shadow_validation.get("audit_run_id") or None,
            "deferred_problems": cs.deferred_problems,
            "completion_time": cs.completion_time,
            "agent_restart_counts": cs.agent_restart_counts,
            "last_fix_wave_id": cs.current_round_id,
            "total_fixes": cs.total_fixes,
            "total_failures": cs.total_failures,
            "round_history_size": len(cs.round_history),
            "execution_lanes": cs.execution_lanes,
            "blocked_problems": cs.blocked_problems,
            "completion_blockers": completion_blockers,
            "autonomy_index": cs.autonomy_index,
            "shadow_validation": shadow_validation,
            "formal_promote": formal_promote,
            "completion_evidence": completion_evidence,
            "last_round_summary": last_summary if last_summary else None,
            # v5: truthful metadata
            "last_updated_at": datetime.now(timezone.utc).isoformat(),
            "actual_service_health": self._last_preflight,
        }

    def _write_control_plane_projection(self) -> None:
        """Write control_plane/current_state.json + current_status.md.

        Mirrors loop_controller's ``_write_control_plane_projection()`` to
        keep backward compatibility with Kestra and external consumers.
        Uses atomic write (tmp → replace) to avoid partial reads.
        """
        cp_dir = self.config.repo_root / "automation" / "control_plane"
        try:
            cp_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            return

        cs = self._coord_state
        projection = self._build_runtime_projection()

        self._atomic_write(cp_dir / "current_state.json",
                           json.dumps(projection, ensure_ascii=False, indent=2))

        mode_cn = {
            "fix": "修复",
            "monitor": "监控",
            "safe_hold": "安全暂停",
            "completed": "完成",
        }.get(cs.mode, cs.mode)
        goal_reached_cn = "是" if projection["goal_reached"] else "否"
        last_summary = projection.get("last_round_summary") or {}
        last_result = last_summary.get("result", "—") if last_summary else "—"
        active_lane_count = sum(
            1
            for lane in cs.execution_lanes.values()
            if lane.get("status") == ProblemStatus.ACTIVE.value
        )
        review_lane_count = sum(
            1
            for lane in cs.execution_lanes.values()
            if lane.get("status") == ProblemStatus.REVIEW_REQUIRED.value
        )
        shadow_state = "ready" if projection["shadow_validation"].get("ready") else "blocked"
        formal_state = projection["formal_promote"].get("state", "not_attempted")

        status_md = (
            "# 当前自动化基础层状态\n\n"
            "<!-- 本文件由 EscortTeam CoordinatorAgent 自动更新 -->\n"
            "<!-- 禁止手动修改运行时字段 -->\n\n"
            "## 当前执行层\n\n"
            "| 字段 | 值 |\n|------|-----|\n"
            f"| 模式 | {mode_cn} |\n"
            f"| 阶段 | {cs.phase} |\n"
            f"| 当前轮次 | {cs.current_round_id or '—'} |\n"
            f"| 连续成功修复数 | {cs.consecutive_green_rounds} |\n"
            f"| 当前目标进度 | {projection['goal_progress_count']}/{projection['fix_goal']} |\n"
            f"| 目标达成 | {goal_reached_cn} |\n"
            f"| 总修复数 | {cs.total_fixes} |\n"
            f"| 总失败数 | {cs.total_failures} |\n"
            f"| 活跃 lane 数 | {active_lane_count} |\n"
            f"| 待人工审查 lane 数 | {review_lane_count} |\n"
            f"| 显式 blocked 问题数 | {len(cs.blocked_problems)} |\n"
            f"| 最近 round 结果 | {last_result} |\n"
            f"| 最近审计 run_id | {projection['last_audit_run_id'] or '—'} |\n"
            f"| 最近 promote 时间 | {cs.last_promote_time or '—'} |\n"
            f"| Shadow 验证 | {shadow_state} |\n"
            f"| Formal promote | {formal_state} |\n"
            f"| Completion blockers | {projection['blocked_reason'] or '—'} |\n"
            f"| 自治指数 | {cs.autonomy_index:.1%} |\n"
        )

        self._atomic_write(cp_dir / "current_status.md", status_md)

    @staticmethod
    def _atomic_write(path: Path, content: str) -> None:
        """Write content atomically via tmp file + os.replace."""
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(content, encoding="utf-8")
        os.replace(str(tmp), str(path))

    # ------------------------------------------------------------------
    # Dynamic Fan-out
    # ------------------------------------------------------------------

    @staticmethod
    def _desired_fan_out(queue_depth: int, mode: str) -> int:
        """Compute parallel dispatch count based on queue depth and mode.

        Reference: loop_controller ``_desired_workers()``.
        Scales up to 12 concurrent dispatches for large queues.
        """
        if mode == CoordinatorMode.MONITOR.value:
            if queue_depth == 0:
                return 1
            return min(queue_depth, 4)
        # FIX mode — scale towards 12 workers
        max_fan = int(os.environ.get("ESCORT_MAX_FAN_OUT", "12"))
        if queue_depth <= 2:
            return 1
        if queue_depth <= 6:
            return min(3, max_fan)
        if queue_depth <= 12:
            return min(6, max_fan)
        if queue_depth <= 24:
            return min(9, max_fan)
        return min(12, max_fan)

    # ------------------------------------------------------------------
    # Stall Detection
    # ------------------------------------------------------------------

    async def _check_stall(self, had_progress: bool) -> None:
        """Track no-progress rounds and trigger infra_doctor when stalled."""
        if had_progress:
            self._coord_state.consecutive_no_progress_rounds = 0
            return

        self._coord_state.consecutive_no_progress_rounds += 1
        if self._coord_state.consecutive_no_progress_rounds < STALL_THRESHOLD:
            return

        logger.warning(
            "[coordinator] Stall detected: %d consecutive no-progress rounds",
            self._coord_state.consecutive_no_progress_rounds,
        )

        # v2: Use inline infra diagnosis (no external module dependency)
        try:
            report = await self._inline_infra_diagnosis()
            severity = report.get("overall_severity", "info")
            logger.info(
                "[coordinator] Infra diagnosis: severity=%s, failed=%d",
                severity, report.get("failed_count", 0),
            )
            if severity in ("critical", "error"):
                await self._enter_safe_hold(
                    f"stall_detected: infra diagnosis reports {severity}"
                )
        except Exception:
            logger.exception("[coordinator] Inline infra diagnosis failed")

    # ------------------------------------------------------------------
    # External API
    # ------------------------------------------------------------------

    def get_status(self) -> Dict[str, Any]:
        """Return current coordinator status for the API layer."""
        projection = self._build_runtime_projection()
        return {
            "agent_id": self.agent_id,
            "state": self._state.value,
            "coordinator": self._coord_state.to_dict(),
            "autonomy_index": self._coord_state.autonomy_index,
            "agents_registered": self._coord_state.agents_registered,
            "agents_healthy": self._coord_state.agents_healthy,
            "deferred_problems": self._coord_state.deferred_problems,
            "execution_lanes": self._coord_state.execution_lanes,
            "blocked_problems": self._coord_state.blocked_problems,
            "completion_blockers": projection["completion_blockers"],
            "completion_time": self._coord_state.completion_time,
            "last_promote_round_id": projection["last_promote_round_id"],
            "last_audit_run_id": projection["last_audit_run_id"],
            "runtime_projection": projection,
            "completion_evidence": projection["completion_evidence"],
            "mailbox_depth": self.mailbox.depth,
        }

    def get_round_progress(self) -> Dict[str, Any]:
        """Return live round progress for /v1/round-progress."""
        if self._current_round_progress:
            return self._current_round_progress.to_dict()
        return {"round_id": "", "phase": "idle", "problem_count": 0}

    def _completion_blockers(self) -> List[Dict[str, Any]]:
        blockers: List[Dict[str, Any]] = []
        for lane in self._coord_state.execution_lanes.values():
            status = lane.get("status", ProblemStatus.ACTIVE.value)
            # Only ACTIVE lanes block completion — BLOCKED and REVIEW_REQUIRED
            # are non-actionable (external deps / code drift) and should not
            # prevent the system from reaching COMPLETED state.
            if status == ProblemStatus.ACTIVE.value:
                blockers.append(
                    {
                        "lane_id": lane.get("lane_id", "general"),
                        "status": status,
                        "problem_count": lane.get("problem_count", 0),
                    }
                )
        return blockers

    def _completion_goal_reached(self) -> bool:
        return not self._completion_gate_failures()

    # ------------------------------------------------------------------
    # v2: Self-Completion Detection
    # ------------------------------------------------------------------

    async def _check_completion(self) -> bool:
        """Check whether all completion criteria are met.

        Criteria (ALL must be True):
        1. consecutive_green_rounds >= GREEN_ROUNDS_FOR_COMPLETED
        2. zero deferred problems (all fixable problems resolved)
        3. autonomy_index >= 0.85
        4. no active circuit breakers (consecutive_fix_failures == 0)

        Returns True if entering COMPLETED mode.
        """
        cs = self._coord_state
        if not self._completion_goal_reached():
            return False

        logger.info(
            "[coordinator] COMPLETION criteria met: green=%d, ai=%.1f%%, deferred=%d, blocked=%d",
            cs.consecutive_green_rounds, cs.autonomy_index * 100,
            len(cs.deferred_problems),
            len(cs.blocked_problems),
        )

        cs.mode = CoordinatorMode.COMPLETED.value
        cs.completion_time = datetime.now(timezone.utc).isoformat()
        self._save_state()

        await self.mailbox.send(AgentMessage(
            source=self.agent_id,
            target="*",
            msg_type=MessageType.MODE_SWITCH.value,
            payload={"mode": "completed", "reason": "all_completion_criteria_met"},
        ))

        # Write final completion report
        self._write_completion_report()
        return True

    def _write_completion_report(self) -> None:
        """Write a final completion report to output/."""
        cs = self._coord_state
        completion_evidence = self._completion_evidence()
        report = {
            "status": "COMPLETED",
            "completion_time": cs.completion_time,
            "total_rounds": cs.total_rounds,
            "total_fixes": cs.total_fixes,
            "total_failures": cs.total_failures,
            "consecutive_green_rounds": cs.consecutive_green_rounds,
            "autonomy_index": cs.autonomy_index,
            "deferred_problems": cs.deferred_problems,
            "execution_lanes": cs.execution_lanes,
            "blocked_problems": cs.blocked_problems,
            "agents_healthy": cs.agents_healthy,
            "last_promote_round_id": cs.last_promote_round_id or None,
            "last_audit_run_id": cs.last_audit_run_id or None,
            "shadow_validation": completion_evidence["shadow_validation"],
            "formal_promote": completion_evidence["formal_promote"],
            "completion_evidence": completion_evidence,
        }
        report_path = self.config.repo_root / "output" / "escort_team_completion.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        self._atomic_write(report_path, json.dumps(report, ensure_ascii=False, indent=2))
        logger.info("[coordinator] Completion report written to %s", report_path)

    # ------------------------------------------------------------------
    # v2: Preflight Checks
    # ------------------------------------------------------------------

    async def _preflight_checks(self) -> Dict[str, bool]:
        """Run bootstrap preflight checks before entering orchestration.

        Verifies all external services are reachable, governance artifacts
        exist, and the mailbox is operational.

        When ``DRY_RUN_SERVICES=true`` is set, HTTP service checks are
        skipped (all marked True) so local development works without
        running the full helper stack.
        """
        checks: Dict[str, bool] = {}
        urls = self.config.service_urls
        dry_run = os.environ.get("DRY_RUN_SERVICES", "").lower() in ("1", "true", "yes")

        # Service reachability
        for svc, url in urls.items():
            if dry_run:
                checks[f"svc_{svc}"] = True
                continue
            if not url:
                checks[f"svc_{svc}"] = False
                continue
            checks[f"svc_{svc}"] = await self._http_ping(f"{url}/health")

        # Governance artifacts exist
        for artifact_name, rel_path in [
            ("junit_xml", "output/junit.xml"),
            ("catalog", "app/governance/catalog_snapshot.json"),
            ("blind_spot", "output/blind_spot_audit.json"),
        ]:
            full = self.config.repo_root / rel_path
            checks[f"artifact_{artifact_name}"] = full.exists()

        # Mailbox operational
        checks["mailbox"] = self.mailbox.depth >= 0  # basic check

        checks["all_ok"] = all(v for k, v in checks.items() if k != "all_ok")
        return checks

    @staticmethod
    async def _http_ping(url: str, timeout: float = 5.0) -> bool:
        """Quick HTTP GET to check reachability."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.get(url)
                return resp.status_code < 500
        except Exception:
            return False

    # ------------------------------------------------------------------
    # v2: Inline Infrastructure Diagnosis
    # ------------------------------------------------------------------

    async def _inline_infra_diagnosis(self) -> Dict[str, Any]:
        """Run infrastructure diagnosis without external module dependency.

        Checks all service endpoints, disk space, and key file accessibility.
        Returns a diagnosis report with overall severity.
        """
        checks: Dict[str, Any] = {}
        urls = self.config.service_urls

        # Service health
        for svc, url in urls.items():
            if url:
                checks[f"svc_{svc}"] = await self._http_ping(f"{url}/health")

        # Disk space check
        try:
            import shutil
            usage = shutil.disk_usage(str(self.config.repo_root))
            free_gb = usage.free / (1024 ** 3)
            checks["disk_free_gb"] = round(free_gb, 1)
            checks["disk_ok"] = free_gb > 1.0
        except Exception:
            checks["disk_ok"] = True  # assume OK if can't check

        # Key file accessibility
        for name, path in [
            ("coordinator_state", self._state_path),
            ("control_plane", self.config.repo_root / "automation" / "control_plane" / "current_state.json"),
        ]:
            checks[f"file_{name}"] = path.exists()

        # Compute severity
        failed_count = sum(1 for k, v in checks.items() if isinstance(v, bool) and not v)
        if failed_count >= 3:
            severity = "critical"
        elif failed_count >= 1:
            severity = "warning"
        else:
            severity = "info"

        report = {
            "checks": checks,
            "overall_severity": severity,
            "failed_count": failed_count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Persist report
        report_path = self.config.repo_root / "output" / "infra_doctor_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._atomic_write(report_path, json.dumps(report, ensure_ascii=False, indent=2))
        except Exception:
            pass

        return report

    # ------------------------------------------------------------------
    # v2: Agent Auto-Restart (Self-Heal)
    # ------------------------------------------------------------------

    def register_agent_instances(self, agents: Dict[str, Any]) -> None:
        """Register agent instances for potential restart.

        Called by EscortTeam after creating all agents.
        """
        self._agent_instances = dict(agents)
        # Build role-based index for restart lookup when agent_id changes
        self._agent_instances_by_role: Dict[str, Any] = {}
        for aid, agent in agents.items():
            role = getattr(agent, 'role', None)
            if role is not None:
                role_str = role.value if hasattr(role, 'value') else str(role)
                self._agent_instances_by_role[role_str] = agent

    def init_worker_pools(self, mailbox: "Mailbox", config: "AgentConfig") -> None:
        """Initialize worker pools for parallel fix/analysis execution.

        Called by EscortTeam after creation.  Creates ephemeral agent
        factories that spawn independent worker instances — mirroring
        how Claude Code spawns sub-agents for parallel task execution.
        """
        from .fix import FixAgent
        from .analysis import AnalysisAgent

        pool_size = int(os.environ.get("ESCORT_POOL_SIZE", "5"))

        self._fix_pool = WorkerPool(
            agent_factory=lambda: FixAgent(mailbox=mailbox, config=config),
            pool_size=pool_size,
        )
        self._analysis_pool = WorkerPool(
            agent_factory=lambda: AnalysisAgent(mailbox=mailbox, config=config),
            pool_size=pool_size,
        )
        logger.info(
            "[coordinator] Worker pools initialized: pool_size=%d", pool_size,
        )

    async def _try_restart_agent(self, agent_id: str) -> bool:
        """Attempt to restart a failed agent.

        Returns True if restart was successful.
        """
        restart_count = self._coord_state.agent_restart_counts.get(agent_id, 0)
        if restart_count >= MAX_AGENT_RESTARTS:
            logger.warning(
                "[coordinator] Agent %s exceeded max restarts (%d) — permanently down",
                agent_id, MAX_AGENT_RESTARTS,
            )
            return False

        agent = self._agent_instances.get(agent_id)
        if agent is None:
            # Fallback: extract role prefix from agent_id (e.g. "discovery-54ea87" → "discovery")
            role_prefix = agent_id.rsplit("-", 1)[0] if "-" in agent_id else agent_id
            agent = getattr(self, '_agent_instances_by_role', {}).get(role_prefix)
            if agent is not None:
                logger.info(
                    "[coordinator] Resolved agent %s via role '%s'",
                    agent_id, role_prefix,
                )
        if agent is None:
            logger.warning("[coordinator] No instance registered for agent %s", agent_id)
            return False

        try:
            logger.info(
                "[coordinator] Restarting agent %s (attempt %d/%d)",
                agent_id, restart_count + 1, MAX_AGENT_RESTARTS,
            )
            await asyncio.sleep(AGENT_RESTART_COOLDOWN)
            await agent.start()
            self._coord_state.agent_restart_counts[agent_id] = restart_count + 1
            self._agent_heartbeats[agent_id] = time.monotonic()
            self._save_state()
            logger.info("[coordinator] Agent %s restarted successfully", agent_id)
            return True
        except Exception:
            logger.exception("[coordinator] Failed to restart agent %s", agent_id)
            self._coord_state.agent_restart_counts[agent_id] = restart_count + 1
            self._save_state()
            return False
