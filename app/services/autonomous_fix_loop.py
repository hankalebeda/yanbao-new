from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from automation.loop_controller.analyzer import analyze_bundle, detect_drift
from automation.loop_controller.schemas import ProblemSpec

from app.core.config import settings
from app.services.autonomous_fix_state import (
    AutonomousFixState,
    AutonomousFixStateStore,
    LoopMode,
    RoundRecord,
    bundle_fingerprint,
)
from app.services.issue_mesh_gateway import HttpIssueMeshGateway, HttpIssueMeshGatewayConfig, IssueMeshGateway
from app.services.writeback_coordination import WritebackCoordination


_SHARED_ARTIFACT_PROMOTE_KEYS = (
    "artifact_files_present",
    "catalog_fresh",
    "junit_clean",
    "blind_spot_clean",
    "continuous_audit_complete",
    "artifacts_same_round",
)
_TRANSIENT_PROMOTE_HTTP_STATUS_CODES = frozenset({408, 409, 425, 429, 500, 502, 503, 504})
_PROMOTE_RETRY_DELAYS_SECONDS = (0.0, 0.05)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _first_env(*names: str) -> str:
    for name in names:
        raw = os.getenv(name, "")
        if raw.strip():
            return raw.strip()
    return ""


def _env_int(*names: str, default: int, minimum: int = 0) -> int:
    raw = _first_env(*names)
    if not raw:
        return max(minimum, int(default))
    try:
        value = int(raw)
    except ValueError:
        value = int(default)
    return max(minimum, value)


def _issue_mesh_worker_cap() -> int:
    max_workers_cap = _env_int("ISSUE_MESH_MAX_WORKERS_CAP", default=12, minimum=1)
    readonly_workers = _env_int("ISSUE_MESH_READONLY_MAX_WORKERS", default=max_workers_cap, minimum=1)
    return max(1, min(max_workers_cap, readonly_workers))


def _shared_promote_payload(runtime_context: dict[str, Any]) -> dict[str, Any]:
    runtime_gates = runtime_context.get("runtime_gates") or {}
    shared_promote = runtime_gates.get("shared_artifact_promote") or {}
    return shared_promote if isinstance(shared_promote, dict) else {}


def _runtime_gate_status(runtime_context: dict[str, Any]) -> str | None:
    runtime_gates = runtime_context.get("runtime_gates") or {}
    candidate = str(runtime_gates.get("status") or "").strip().lower()
    return candidate or None


def _artifacts_aligned(runtime_context: dict[str, Any], verify: dict[str, Any]) -> bool:
    shared_promote = _shared_promote_payload(runtime_context)
    if any(key in shared_promote for key in _SHARED_ARTIFACT_PROMOTE_KEYS):
        return all(bool(shared_promote.get(key)) for key in _SHARED_ARTIFACT_PROMOTE_KEYS)
    if "allowed" in shared_promote:
        return bool(shared_promote.get("allowed"))
    return bool(verify.get("artifacts_aligned", True))


def _is_transient_promote_failure(reason: str | None) -> bool:
    candidate = str(reason or "").strip()
    if not candidate:
        return False
    prefix = candidate.split(":", 1)[0]
    if prefix not in {"PROMOTE_PREPARE_FAILED", "TRIAGE_FAILED", "PREVIEW_B_FAILED", "COMMIT_B_FAILED"}:
        return False
    status_token = candidate.rsplit(":", 1)[-1]
    return status_token.isdigit() and int(status_token) in _TRANSIENT_PROMOTE_HTTP_STATUS_CODES


def _unique_reasons(*reasons: str) -> list[str]:
    ordered: list[str] = []
    for reason in reasons:
        normalized = str(reason or "").strip()
        if normalized and normalized not in ordered:
            ordered.append(normalized)
    return ordered


def _round_failed_reasons(
    *,
    v_no_new_active: bool,
    v_no_regression: bool,
    v_no_drift: bool,
    verify_all_green: bool,
    v_no_partial_fail: bool,
    v_no_masked_skip: bool,
    v_batch_writeback_complete: bool,
    v_promote_gate_passed: bool,
    v_artifacts_aligned: bool,
    is_noop_fix_round: bool,
    promote_reason: str | None,
    runtime_gate_status: str | None,
) -> list[str]:
    reasons: list[str] = []
    if not v_no_new_active:
        reasons.append("NEW_ACTIVE_PROBLEMS_REMAIN")
    if not v_no_regression:
        reasons.append("REGRESSION_DETECTED")
    if not v_no_drift:
        reasons.append("ARTIFACT_DRIFT_DETECTED")
    if not verify_all_green:
        reasons.append("VERIFY_NOT_GREEN")
    if not v_no_partial_fail:
        reasons.append("PARTIAL_FIX_FAILURE")
    if not v_no_masked_skip:
        reasons.append("MASKED_SKIP_DETECTED")
    if not v_batch_writeback_complete:
        reasons.append("BATCH_WRITEBACK_INCOMPLETE")
    if not v_promote_gate_passed:
        reasons.append(str(promote_reason or "PROMOTE_NOT_READY"))
    if not v_artifacts_aligned:
        reasons.append("SHARED_ARTIFACTS_NOT_ALIGNED")
    if is_noop_fix_round:
        reasons.append("FIX_ROUND_NOOP")
    if runtime_gate_status and runtime_gate_status != "ready":
        reasons.append(f"RUNTIME_GATES_{runtime_gate_status.upper()}")
    return _unique_reasons(*reasons)


def _render_round_note(failed_reasons: list[str]) -> str | None:
    if not failed_reasons:
        return None
    return ";".join(failed_reasons[:3])


class WritebackCoordinator(Protocol):
    def claim(self, *, round_id: str, target_paths: list[str], lease_seconds: int) -> dict[str, Any]:
        ...

    def refresh(self, *, lease: dict[str, Any]) -> dict[str, Any] | None:
        ...

    def release(self, *, lease: dict[str, Any], reason: str) -> None:
        ...


@dataclass(frozen=True)
class AutonomousFixLoopConfig:
    success_round_goal: int = 10
    lease_seconds: int = 120
    max_workers_fix: int = 12
    max_workers_monitor: int = 4
    fix_interval_seconds: int = 300
    monitor_interval_seconds: int = 600
    enabled: bool = False
    auto_start: bool = False

    @classmethod
    def from_env(cls) -> "AutonomousFixLoopConfig":
        worker_cap = _issue_mesh_worker_cap()
        max_workers_fix = min(
            _env_int(
                "AUTONOMY_LOOP_MAX_WORKERS_FIX",
                "AUTONOMOUS_FIX_LOOP_MAX_WORKERS_FIX",
                default=worker_cap,
                minimum=1,
            ),
            worker_cap,
        )
        max_workers_monitor = min(
            _env_int(
                "AUTONOMY_LOOP_MAX_WORKERS_MONITOR",
                "AUTONOMOUS_FIX_LOOP_MAX_WORKERS_MONITOR",
                default=min(4, worker_cap),
                minimum=1,
            ),
            worker_cap,
        )
        return cls(
            success_round_goal=_env_int(
                "AUTONOMY_LOOP_FIX_GOAL",
                "AUTONOMOUS_FIX_LOOP_SUCCESS_ROUND_GOAL",
                default=settings.autonomy_loop_fix_goal,
                minimum=1,
            ),
            lease_seconds=_env_int(
                "AUTONOMOUS_FIX_LOOP_LEASE_SECONDS",
                default=120,
                minimum=30,
            ),
            max_workers_fix=max_workers_fix,
            max_workers_monitor=max_workers_monitor,
            fix_interval_seconds=_env_int(
                "AUTONOMY_LOOP_AUDIT_INTERVAL_SECONDS",
                "AUTONOMOUS_FIX_LOOP_FIX_INTERVAL_SECONDS",
                default=settings.autonomy_loop_audit_interval_seconds,
                minimum=1,
            ),
            monitor_interval_seconds=_env_int(
                "AUTONOMY_LOOP_MONITOR_INTERVAL_SECONDS",
                "AUTONOMOUS_FIX_LOOP_MONITOR_INTERVAL_SECONDS",
                default=settings.autonomy_loop_monitor_interval_seconds,
                minimum=1,
            ),
            enabled=_env_bool(
                "AUTONOMY_LOOP_ENABLED",
                "AUTONOMOUS_FIX_LOOP_ENABLED",
                default=settings.autonomy_loop_enabled,
            ),
            auto_start=_env_bool(
                "AUTONOMY_LOOP_AUTO_START",
                "AUTONOMOUS_FIX_LOOP_AUTO_START",
                default=False,
            ),
        )


def _env_bool(*names: str, default: bool = False) -> bool:
    raw = _first_env(*names)
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


class _WritebackCoordinationAdapter:
    def __init__(self, coordination: WritebackCoordination) -> None:
        self._coordination = coordination

    def claim(self, *, round_id: str, target_paths: list[str], lease_seconds: int) -> dict[str, Any]:
        lease = self._coordination.claim(round_id, target_paths, lease_seconds=lease_seconds)
        return {
            "lease_id": lease.lease_id,
            "round_id": lease.round_id,
            "target_paths": list(lease.target_paths),
            "fencing_token": lease.fencing_token,
            "issued_at": lease.issued_at,
            "lease_until": lease.lease_until,
        }

    def refresh(self, *, lease: dict[str, Any]) -> dict[str, Any] | None:
        refreshed = self._coordination.refresh(lease=lease)
        if refreshed is None:
            return None
        return {
            "lease_id": refreshed.lease_id,
            "round_id": refreshed.round_id,
            "target_paths": list(refreshed.target_paths),
            "fencing_token": refreshed.fencing_token,
            "issued_at": refreshed.issued_at,
            "lease_until": refreshed.lease_until,
        }

    def release(self, *, lease: dict[str, Any], reason: str) -> None:
        self._coordination.release(lease=lease, reason=reason)


class AutonomousFixLoop:
    def __init__(
        self,
        *,
        store: AutonomousFixStateStore,
        gateway: IssueMeshGateway,
        coordinator: WritebackCoordinator | None = None,
        config: AutonomousFixLoopConfig | None = None,
    ) -> None:
        self._store = store
        self._gateway = gateway
        self._coordinator = coordinator
        self._cfg = config or AutonomousFixLoopConfig()

    def run_rounds(self, count: int) -> AutonomousFixState:
        state = self._store.load()
        for _ in range(max(0, count)):
            state = self.run_single_round()
        return state

    def run_single_round(self, *, force_mode: LoopMode | None = None) -> AutonomousFixState:
        state = self._store.load()
        if force_mode is not None:
            state.mode = force_mode
        state.success_round_goal = max(1, self._cfg.success_round_goal)

        round_id = self._next_round_id(state)
        started_at = _utc_now_iso()
        state.phase = "auditing"

        audit_payload = self._gateway.run_audit(
            mode=state.mode.value,
            max_workers=self._desired_workers(state.mode),
        )
        audit_run_id = str(audit_payload.get("audit_run_id") or audit_payload.get("run_id") or "")
        bundle = audit_payload.get("bundle") or {}
        artifact_fingerprints = dict(audit_payload.get("artifact_fingerprints") or {})

        state.last_audit_run_id = audit_run_id
        state.last_bundle_fingerprint = bundle_fingerprint(bundle)

        state.phase = "analyzing"
        problems, new_count, regression_count, skipped_count = analyze_bundle(bundle, state.fixed_problem_ids)
        drift_paths = detect_drift(artifact_fingerprints, state.last_artifact_fingerprints)
        runtime_context = self._gateway.get_runtime_context()
        runtime_gate_status = _runtime_gate_status(runtime_context)
        shared_promote = _shared_promote_payload(runtime_context)

        reentered_fix = False
        if state.mode == LoopMode.MONITOR:
            if new_count > 0 or regression_count > 0 or bool(drift_paths):
                state.mode = LoopMode.FIX
                # Only reset streak on genuinely new problems; preserve on regression/drift re-entry
                if new_count > 0:
                    state.success_round_streak = 0
                reentered_fix = True
            else:
                state.total_rounds += 1
                state.round_seq += 1
                state.last_artifact_fingerprints = artifact_fingerprints
                state.phase = "idle"
                round_record = RoundRecord(
                    round_id=round_id,
                    mode=LoopMode.MONITOR,
                    started_at=started_at,
                    finished_at=_utc_now_iso(),
                    audit_run_id=audit_run_id,
                    problems_found=len(problems),
                    new_problem_count=new_count,
                    regression_count=regression_count,
                    skipped_count=skipped_count,
                    drift_paths=list(drift_paths),
                    verify_all_green=True,
                    promoted=False,
                    reentered_fix=False,
                    no_new_active=True,
                    no_regression=True,
                    no_drift=True,
                    no_partial_fail=True,
                    no_masked_skip=True,
                    batch_writeback_complete=True,
                    artifacts_aligned=True,
                    green_round=True,
                    note="monitor_stable_noop",
                )
                state.round_history.append(round_record)
                return self._store.save(state)

        state.phase = "fixing"
        actionable = [problem for problem in problems if not problem.is_external_blocked]

        lease: dict[str, Any] | None = None
        if actionable and self._coordinator is not None:
            claim = getattr(self._coordinator, "claim", None)
            if callable(claim):
                lease = claim(
                    round_id=round_id,
                    target_paths=self._target_paths_from_problems(actionable),
                    lease_seconds=self._cfg.lease_seconds,
                )

        fix_results: list[dict[str, Any]] = []
        if actionable:
            has_valid_lease = bool(lease) and bool(lease.get("lease_id")) and lease.get("fencing_token") is not None
            if not has_valid_lease:
                fix_results = [
                    {
                        "problem_id": problem.problem_id,
                        "outcome": "failed",
                        "patches_applied": [],
                        "error": "WRITEBACK_LEASE_REQUIRED",
                    }
                    for problem in actionable
                ]
            else:
                fix_results = self._gateway.apply_fixes(
                    problems=actionable,
                    round_id=round_id,
                    audit_run_id=audit_run_id,
                    runtime_context=runtime_context,
                    coordinator=self._coordinator,
                    lease=lease,
                )
                # Ensure patches_applied is always a list for successful results
                for result in fix_results:
                    if str(result.get("outcome", "")).lower() == "success" and "patches_applied" not in result:
                        result["patches_applied"] = []

        changed_files = self._collect_changed_files(fix_results)

        state.phase = "verifying"
        verify = self._gateway.verify_round(round_id=round_id, changed_files=changed_files)
        verify_all_green = bool(verify.get("all_green"))

        # Rollback applied patches when verification fails
        if not verify_all_green and fix_results:
            self._rollback_applied_fixes(fix_results, round_id=round_id)

        successful_fix_results = [
            result for result in fix_results if str(result.get("outcome", "")).lower() == "success"
        ]
        failed_fix_results = [
            result for result in fix_results if str(result.get("outcome", "")).lower() == "failed"
        ]
        skipped_fix_results = [
            result for result in fix_results if str(result.get("outcome", "")).lower() == "skipped"
        ]

        # --- 9-item GreenRoundVerdict evaluation ---
        non_blocked = [p for p in problems if not p.is_external_blocked]
        v_no_new_active = len(non_blocked) == 0 or len(successful_fix_results) >= len(non_blocked)
        v_no_regression = regression_count == 0
        v_no_drift = len(drift_paths) == 0
        v_no_partial_fail = len(failed_fix_results) == 0
        v_no_masked_skip = all(
            bool(r.get("error")) for r in skipped_fix_results
        ) if skipped_fix_results else True
        v_batch_writeback_complete = all(
            bool(r.get("patches_applied")) for r in successful_fix_results
        ) if successful_fix_results else True

        state.phase = "promoting"
        promote_result: dict[str, Any] = {}
        pre_promote_green = all([
            v_no_new_active, v_no_regression, v_no_drift, verify_all_green,
            v_no_partial_fail, v_no_masked_skip, v_batch_writeback_complete,
        ])
        # Evaluate artifacts alignment before promotion attempt for full 9-gate coverage
        v_artifacts_aligned = _artifacts_aligned(runtime_context, verify)
        if pre_promote_green and actionable:
            promote_result = self._promote_with_retry(
                round_id=round_id,
                audit_run_id=audit_run_id,
                runtime_context=runtime_context,
                coordinator=self._coordinator,
                lease_seconds=self._cfg.lease_seconds,
            )
        elif pre_promote_green:
            promote_result = {"promoted": True, "reason": "NO_CHANGES_TO_PROMOTE"}

        promote_reason = str(promote_result.get("reason") or "").strip() or None
        v_promote_gate_passed = bool(promote_result.get("promoted", False))
        if actionable and "allowed" in shared_promote:
            v_promote_gate_passed = v_promote_gate_passed and bool(shared_promote.get("allowed"))
        # v_artifacts_aligned already evaluated before promotion attempt

        # No-op detection: fix mode rounds with no actionable work are not green
        is_noop_fix_round = (
            state.mode == LoopMode.FIX
            and len(actionable) == 0
            and len(fix_results) == 0
        )
        green_round = all([
            v_no_new_active, v_no_regression, v_no_drift, verify_all_green,
            v_no_partial_fail, v_no_masked_skip, v_batch_writeback_complete,
            v_promote_gate_passed, v_artifacts_aligned,
        ]) and not is_noop_fix_round
        verified_problem_count = len(successful_fix_results) if green_round else 0
        failed_reasons = _round_failed_reasons(
            v_no_new_active=v_no_new_active,
            v_no_regression=v_no_regression,
            v_no_drift=v_no_drift,
            verify_all_green=verify_all_green,
            v_no_partial_fail=v_no_partial_fail,
            v_no_masked_skip=v_no_masked_skip,
            v_batch_writeback_complete=v_batch_writeback_complete,
            v_promote_gate_passed=v_promote_gate_passed,
            v_artifacts_aligned=v_artifacts_aligned,
            is_noop_fix_round=is_noop_fix_round,
            promote_reason=promote_reason,
            runtime_gate_status=runtime_gate_status,
        )

        # Always track total fixes and fixed problem IDs regardless of green round
        state.total_fixes += len(successful_fix_results)
        for item in successful_fix_results:
            problem_id = str(item.get("problem_id") or "")
            if problem_id and problem_id not in state.fixed_problem_ids:
                state.fixed_problem_ids.append(problem_id)

        if green_round:
            state.success_round_streak += 1
            state.consecutive_verified_problem_fixes += verified_problem_count
        else:
            if not v_no_regression or not verify_all_green:
                state.success_round_streak = 0
                state.consecutive_verified_problem_fixes = 0
            state.total_failures += 1

        goal_metric = state.success_goal_metric
        goal_count = (
            state.consecutive_verified_problem_fixes
            if goal_metric == "verified_problem_count"
            else state.success_round_streak
        )
        if goal_count >= state.success_round_goal:
            state.mode = LoopMode.MONITOR
            state.goal_ever_reached = True

        if lease and self._coordinator is not None:
            release = getattr(self._coordinator, "release", None)
            if callable(release):
                release(lease=lease, reason="round_finished")

        state.total_rounds += 1
        state.round_seq += 1
        state.last_artifact_fingerprints = artifact_fingerprints
        state.phase = "idle"

        round_record = RoundRecord(
            round_id=round_id,
            mode=LoopMode.MONITOR if state.mode == LoopMode.MONITOR else LoopMode.FIX,
            started_at=started_at,
            finished_at=_utc_now_iso(),
            audit_run_id=audit_run_id,
            problems_found=len(problems),
            new_problem_count=new_count,
            regression_count=regression_count,
            skipped_count=skipped_count,
            drift_paths=list(drift_paths),
            fixes_attempted=len(fix_results),
            fixes_succeeded=len(successful_fix_results),
            verify_all_green=verify_all_green,
            promoted=v_promote_gate_passed,
            reentered_fix=reentered_fix,
            no_new_active=v_no_new_active,
            no_regression=v_no_regression,
            no_drift=v_no_drift,
            no_partial_fail=v_no_partial_fail,
            no_masked_skip=v_no_masked_skip,
            batch_writeback_complete=v_batch_writeback_complete,
            promote_gate_passed=v_promote_gate_passed,
            artifacts_aligned=v_artifacts_aligned,
            green_round=green_round,
            verified_problem_count=verified_problem_count,
            failed_reasons=failed_reasons,
            promote_reason=promote_reason,
            runtime_gate_status=runtime_gate_status,
            note=None if green_round else _render_round_note(failed_reasons),
        )
        state.round_history.append(round_record)
        return self._store.save(state)

    def _promote_with_retry(
        self,
        *,
        round_id: str,
        audit_run_id: str,
        runtime_context: dict[str, Any],
        coordinator: WritebackCoordinator | None = None,
        lease_seconds: int | None = None,
    ) -> dict[str, Any]:
        result = self._gateway.promote_round(
            round_id=round_id,
            audit_run_id=audit_run_id,
            runtime_context=runtime_context,
            coordinator=coordinator,
            lease_seconds=lease_seconds,
        )
        if bool(result.get("promoted")) or not _is_transient_promote_failure(result.get("reason")):
            return result

        for delay_seconds in _PROMOTE_RETRY_DELAYS_SECONDS:
            if delay_seconds > 0:
                time.sleep(delay_seconds)
            result = self._gateway.promote_round(
                round_id=round_id,
                audit_run_id=audit_run_id,
                runtime_context=runtime_context,
                coordinator=coordinator,
                lease_seconds=lease_seconds,
            )
            if bool(result.get("promoted")) or not _is_transient_promote_failure(result.get("reason")):
                return result
        return result

    def _next_round_id(self, state: AutonomousFixState) -> str:
        seq = state.round_seq + 1
        return f"autofix-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{seq:03d}"

    def _desired_workers(self, mode: LoopMode) -> int:
        if mode == LoopMode.MONITOR:
            return self._cfg.max_workers_monitor
        return self._cfg.max_workers_fix

    def _target_paths_from_problems(self, problems: list[ProblemSpec]) -> list[str]:
        targets: set[str] = set()
        for problem in problems:
            for scope in problem.write_scope:
                targets.add(str(scope))
        return sorted(targets)

    def _collect_changed_files(self, fix_results: list[dict[str, Any]]) -> list[str]:
        changed: set[str] = set()
        for result in fix_results:
            for path in result.get("patches_applied") or []:
                if isinstance(path, str) and path:
                    changed.add(path)
        return sorted(changed)

    def _rollback_applied_fixes(self, fix_results: list[dict[str, Any]], *, round_id: str) -> None:
        """Rollback committed patches when verification fails.

        Iterates successful fix results in reverse order and calls
        writeback-A /v1/rollback for each commit_id found.
        """
        rollback_method = getattr(self._gateway, "rollback_commits", None)
        if callable(rollback_method):
            commit_ids = []
            for result in reversed(fix_results):
                if str(result.get("outcome", "")).lower() == "success":
                    for raw in result.get("patches_raw") or []:
                        cid = str(raw.get("commit_id") or "") if isinstance(raw, dict) else ""
                        if cid:
                            commit_ids.append(cid)
            if commit_ids:
                try:
                    rollback_method(commit_ids=commit_ids, round_id=round_id)
                except Exception:
                    pass  # Best-effort rollback; failures logged by gateway


class AutonomousFixLoopService:
    def __init__(self, loop: AutonomousFixLoop, *, config: AutonomousFixLoopConfig) -> None:
        self._loop = loop
        self._cfg = config
        self._runner: threading.Thread | None = None
        self._runner_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    def get_state(self) -> AutonomousFixState:
        return self._loop._store.get()

    def start(self, *, mode: str = LoopMode.FIX.value, success_round_goal: int | None = None, force_new_round: bool = False) -> AutonomousFixState:
        with self._runner_lock:
            if success_round_goal is not None:
                self._loop._cfg = AutonomousFixLoopConfig(
                    success_round_goal=max(1, int(success_round_goal)),
                    lease_seconds=self._loop._cfg.lease_seconds,
                    max_workers_fix=self._loop._cfg.max_workers_fix,
                    max_workers_monitor=self._loop._cfg.max_workers_monitor,
                    fix_interval_seconds=self._loop._cfg.fix_interval_seconds,
                    monitor_interval_seconds=self._loop._cfg.monitor_interval_seconds,
                    enabled=self._loop._cfg.enabled,
                    auto_start=self._loop._cfg.auto_start,
                )
            seeded_mode = LoopMode.MONITOR if str(mode).lower() == LoopMode.MONITOR.value else LoopMode.FIX
            state = self._loop._store.load()
            state.mode = seeded_mode
            state.success_round_goal = self._loop._cfg.success_round_goal
            state.phase = "idle"
            self._loop._store.save(state)
            if not self._running:
                self._stop_event.clear()
                self._wake_event.clear()
                self._runner = threading.Thread(target=self._run_forever, daemon=True, name="autonomous-fix-loop")
                self._running = True
                self._runner.start()
            if force_new_round:
                self._wake_event.set()
            return self.get_state()

    def stop(self, *, reason: str = "manual_stop") -> AutonomousFixState:
        del reason
        self._stop_event.set()
        self._wake_event.set()
        runner = self._runner
        if runner and runner.is_alive():
            runner.join(timeout=30)
        self._running = False
        state = self._loop._store.load()
        state.phase = "idle"
        return self._loop._store.save(state)

    def force_new_round(self, *, mode: str | None = None, success_round_goal: int | None = None) -> AutonomousFixState:
        state = self._loop._store.load()
        if mode is not None:
            state.mode = LoopMode.MONITOR if str(mode).lower() == LoopMode.MONITOR.value else LoopMode.FIX
        if success_round_goal is not None:
            state.success_round_goal = max(1, int(success_round_goal))
        self._loop._store.save(state)
        if self._running:
            self._wake_event.set()
        return self.get_state()

    def await_round(self, *, timeout_seconds: int = 600) -> dict[str, Any]:
        started_total = self.get_state().total_rounds
        deadline = time.monotonic() + max(1, int(timeout_seconds))
        while time.monotonic() < deadline:
            state = self.get_state()
            if state.total_rounds > started_total and state.round_history:
                latest = state.round_history[-1]
                return {
                    "status": "completed",
                    "round_id": latest.round_id,
                    "round_summary": latest.model_dump(),
                    "timed_out": False,
                }
            time.sleep(0.05)
        state = self.get_state()
        latest = state.round_history[-1] if state.round_history else None
        return {
            "status": "timeout",
            "round_id": latest.round_id if latest else None,
            "round_summary": latest.model_dump() if latest else None,
            "timed_out": True,
        }

    def health_payload(self) -> dict[str, Any]:
        state = self.get_state()
        return {
            "status": "ok",
            "running": self._running,
            "mode": state.mode.value,
            "phase": state.phase,
            "success_round_streak": state.success_round_streak,
            "success_round_goal": state.success_round_goal,
            "total_rounds": state.total_rounds,
        }

    def _run_forever(self) -> None:
        try:
            while not self._stop_event.is_set():
                self._loop.run_single_round()
                state = self.get_state()
                interval = self._cfg.monitor_interval_seconds if state.mode == LoopMode.MONITOR else self._cfg.fix_interval_seconds
                self._wake_event.clear()
                if self._wake_event.wait(timeout=max(0.0, float(interval))):
                    self._wake_event.clear()
        finally:
            self._running = False


_DEFAULT_SERVICE_LOCK = threading.Lock()
_DEFAULT_SERVICE: AutonomousFixLoopService | None = None


def _default_repo_root() -> Path:
    raw = _first_env("AUTONOMY_LOOP_REPO_ROOT", "AUTONOMOUS_FIX_LOOP_REPO_ROOT")
    return Path(raw).resolve() if raw.strip() else Path(__file__).resolve().parents[2]


def build_default_autonomous_fix_loop_service() -> AutonomousFixLoopService:
    repo_root = _default_repo_root()
    cfg = AutonomousFixLoopConfig.from_env()
    internal_token = str(
        settings.internal_cron_token
        or _first_env("INTERNAL_CRON_TOKEN", "INTERNAL_TOKEN", "INTERNAL_API_KEY")
        or settings.internal_api_key
    ).strip()
    store = AutonomousFixStateStore(repo_root / "runtime" / "autonomous_fix_loop" / "state.json")
    gateway = HttpIssueMeshGateway(
        HttpIssueMeshGatewayConfig(
            mesh_runner_base_url=os.getenv("MESH_RUNNER_BASE_URL", "http://127.0.0.1:8093"),
            mesh_runner_token=_first_env("MESH_RUNNER_AUTH_TOKEN", "MESH_RUNNER_TOKEN"),
            promote_prep_base_url=os.getenv("PROMOTE_PREP_BASE_URL", "http://127.0.0.1:8094"),
            promote_prep_token=_first_env("PROMOTE_PREP_AUTH_TOKEN", "PROMOTE_PREP_TOKEN"),
            writeback_a_base_url=os.getenv("WRITEBACK_A_BASE_URL", "http://127.0.0.1:8092"),
            writeback_a_token=_first_env("WRITEBACK_A_AUTH_TOKEN", "WRITEBACK_A_TOKEN"),
            writeback_b_base_url=os.getenv("WRITEBACK_B_BASE_URL", ""),
            writeback_b_token=_first_env("WRITEBACK_B_AUTH_TOKEN", "WRITEBACK_B_TOKEN"),
            app_base_url=os.getenv("APP_BASE_URL", "http://127.0.0.1:38001"),
            internal_token=internal_token,
        )
    )
    coordinator = _WritebackCoordinationAdapter(
        WritebackCoordination(repo_root / "runtime" / "writeback_coordination" / "state.json")
    )
    loop = AutonomousFixLoop(store=store, gateway=gateway, coordinator=coordinator, config=cfg)
    return AutonomousFixLoopService(loop, config=cfg)


def get_autonomous_fix_loop_service() -> AutonomousFixLoopService:
    global _DEFAULT_SERVICE
    with _DEFAULT_SERVICE_LOCK:
        if _DEFAULT_SERVICE is None:
            _DEFAULT_SERVICE = build_default_autonomous_fix_loop_service()
        return _DEFAULT_SERVICE


def maybe_auto_start_autonomous_fix_loop() -> AutonomousFixState | None:
    service = get_autonomous_fix_loop_service()
    if not service._cfg.enabled or not service._cfg.auto_start:
        return None
    return service.start()


def shutdown_autonomous_fix_loop_service() -> AutonomousFixState | None:
    global _DEFAULT_SERVICE
    with _DEFAULT_SERVICE_LOCK:
        if _DEFAULT_SERVICE is None:
            return None
        service = _DEFAULT_SERVICE
        _DEFAULT_SERVICE = None
    return service.stop(reason="shutdown")


def reset_autonomous_fix_loop_service_for_tests() -> None:
    global _DEFAULT_SERVICE
    with _DEFAULT_SERVICE_LOCK:
        if _DEFAULT_SERVICE is not None:
            _DEFAULT_SERVICE.stop(reason="test_reset")
        _DEFAULT_SERVICE = None
