"""LoopController — the master control loop for continuous autonomous fix cycles.

Implements the closed loop:
  AUDIT → ANALYZE → FIX → WRITE_BACK → VERIFY → PROMOTE → repeat

Code fixes are written through the guarded writeback path before verification so
pytest and governance checks validate the actual patched workspace. Failed
verification triggers automatic rollback. When ``consecutive_verified_problem_fixes``
reaches ``fix_goal`` (default 10 verified repairs with no round-level fix
errors), the controller switches to *monitor* mode (longer polling interval)
and re-enters fix mode automatically on regression / drift detection.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from automation.loop_controller.analyzer import analyze_bundle, detect_drift
from automation.loop_controller.schemas import (
    FixOutcome,
    FixResult,
    GreenRoundVerdict,
    LoopMode,
    LoopPhase,
    LoopState,
    ProblemSpec,
    RoundSummary,
    VerifyResult,
)
from automation.loop_controller.state import StateStore
from automation.loop_controller.verifier import Verifier

logger = logging.getLogger(__name__)

# Maximum consecutive round failures before extending backoff
_MAX_BACKOFF_FAILURES = 5
_BASE_BACKOFF_SECONDS = 30
_AUDIT_STATUS_POLL_SECONDS = 5.0
_VALID_PROMOTE_TARGET_MODES = frozenset({"infra", "doc22"})
_VALID_SUCCESS_GOAL_METRICS = frozenset({"verified_problem_count", "fix_success_count"})


class AuditFailedError(RuntimeError):
    """Raised when the audit phase fails (network, auth, timeout)."""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
    temp_path.replace(path)


def _round_id_now(seq: int = 1) -> str:
    return f"fix-loop-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{seq:03d}"


def _safe_problem_token(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in str(value or ""))[:80]


def _next_round_id(history: list[RoundSummary]) -> str:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    prefix = f"fix-loop-{today}-"
    seq = 1
    for r in reversed(history):
        if r.round_id.startswith(prefix):
            try:
                seq = int(r.round_id.split("-")[-1]) + 1
            except ValueError:
                pass
            break
    return f"{prefix}{seq:03d}"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LoopControllerConfig:
    repo_root: Path
    mesh_runner_url: str
    mesh_runner_token: str
    promote_prep_url: str
    promote_prep_token: str
    writeback_a_url: str
    writeback_a_token: str
    writeback_b_url: str
    writeback_b_token: str
    auth_token: str
    app_base_url: str = ""
    internal_token: str = ""
    new_api_base_url: str = ""
    new_api_token: str = ""
    fix_goal: int = 10
    audit_interval_seconds: int = 300
    monitor_interval_seconds: int = 600
    max_writeback_retries: int = 3

    @classmethod
    def from_env(cls) -> "LoopControllerConfig":
        repo_root = Path(os.getenv("LOOP_CONTROLLER_REPO_ROOT", "")).resolve() or Path(__file__).resolve().parents[2]
        return cls(
            repo_root=repo_root,
            mesh_runner_url=os.getenv("MESH_RUNNER_BASE_URL", "http://127.0.0.1:8093"),
            mesh_runner_token=os.getenv("MESH_RUNNER_AUTH_TOKEN", ""),
            promote_prep_url=os.getenv("PROMOTE_PREP_BASE_URL", "http://127.0.0.1:8094"),
            promote_prep_token=os.getenv("PROMOTE_PREP_AUTH_TOKEN", ""),
            writeback_a_url=os.getenv("WRITEBACK_A_BASE_URL", "http://127.0.0.1:8092"),
            writeback_a_token=os.getenv("WRITEBACK_A_AUTH_TOKEN", ""),
            writeback_b_url=os.getenv("WRITEBACK_B_BASE_URL", "http://127.0.0.1:8095"),
            writeback_b_token=os.getenv("WRITEBACK_B_AUTH_TOKEN", ""),
            auth_token=os.getenv("LOOP_CONTROLLER_AUTH_TOKEN", ""),
            app_base_url=os.getenv("APP_BASE_URL", "http://127.0.0.1:38001"),
            internal_token=os.getenv("INTERNAL_TOKEN", ""),
            new_api_base_url=os.getenv("NEW_API_BASE_URL", ""),
            new_api_token=os.getenv("NEW_API_TOKEN", ""),
            fix_goal=int(os.getenv("FIX_GOAL_CONSECUTIVE", "10")),
            audit_interval_seconds=int(os.getenv("AUDIT_INTERVAL_SECONDS", "300")),
            monitor_interval_seconds=int(os.getenv("MONITOR_INTERVAL_SECONDS", "600")),
        )


# ---------------------------------------------------------------------------
# Dynamic concurrency
# ---------------------------------------------------------------------------

def _desired_workers(queue_size: int, mode: LoopMode) -> int:
    """Derive readonly workers from backlog size.

    Concurrency three-tier model (doc05):
      - default=12, cap=16
      - MONITOR mode stays conservative, FIX mode scales with queue depth.
    """
    CAP = 16  # doc05 § 并发三层模型 hard cap
    safe_queue = max(0, int(queue_size or 0))
    if mode == LoopMode.MONITOR:
        if safe_queue <= 0:
            return 2
        if safe_queue <= 4:
            return 3
        return 4
    if safe_queue <= 4:
        return 4
    if safe_queue <= 12:
        return 8
    if safe_queue <= 24:
        return 12
    return min(16, CAP)


# ---------------------------------------------------------------------------
# Controller
# ---------------------------------------------------------------------------

class LoopController:
    """Autonomous fix-loop controller with state-machine lifecycle."""

    def __init__(
        self,
        config: LoopControllerConfig,
        store: StateStore | None = None,
        verifier: Verifier | None = None,
    ) -> None:
        self._cfg = config
        self._store = store or StateStore()
        self._verifier = verifier or Verifier(
            repo_root=config.repo_root,
            promote_prep_url=config.promote_prep_url,
            promote_prep_token=config.promote_prep_token,
        )
        self._http = httpx.Client(
            timeout=httpx.Timeout(connect=15.0, read=3660.0, write=30.0, pool=10.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=5),
            trust_env=False,
        )
        self._consecutive_round_failures = 0
        self._consecutive_no_progress_rounds = 0
        self._stall_threshold = 5  # rounds with no progress before triggering infra_doctor
        self._running = False
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._force_event = threading.Event()  # signal to wake loop immediately

    @property
    def running(self) -> bool:
        return self._running

    def _control_plane_state_path(self) -> Path:
        return self._cfg.repo_root / "automation" / "control_plane" / "current_state.json"

    def _control_plane_status_path(self) -> Path:
        return self._cfg.repo_root / "automation" / "control_plane" / "current_status.md"

    def _normalize_success_goal_metric(self, value: str | None) -> str:
        candidate = str(value or "").strip().lower()
        if candidate in _VALID_SUCCESS_GOAL_METRICS:
            return candidate
        return "verified_problem_count"

    def _goal_progress_count(self, state: LoopState) -> int:
        metric = self._normalize_success_goal_metric(state.success_goal_metric)
        if metric == "fix_success_count":
            return state.consecutive_fix_success_count
        return state.consecutive_verified_problem_fixes

    def _goal_reached(self, state: LoopState) -> bool:
        if not state.fix_goal:
            return False
        if self._goal_progress_count(state) < state.fix_goal:
            return False
        # e2e gate: at least one real promote must have succeeded
        if not state.last_promote_round_id:
            logger.warning(
                "goal count satisfied (%d/%d) but no promote round recorded — "
                "blocking completion until a real e2e cycle succeeds",
                self._goal_progress_count(state),
                state.fix_goal,
            )
            return False
        return True

    def _sync_control_plane_config(self, state: LoopState) -> None:
        payload = _read_json_dict(self._control_plane_state_path())

        promote_target_mode = str(payload.get("promote_target_mode") or "infra").strip().lower()
        if promote_target_mode not in _VALID_PROMOTE_TARGET_MODES:
            promote_target_mode = "infra"
        state.promote_target_mode = promote_target_mode
        state.success_goal_metric = self._normalize_success_goal_metric(
            payload.get("success_goal_metric") or state.success_goal_metric
        )

        if state.last_promote_round_id is None:
            last_promote_round_id = str(payload.get("last_promote_round_id") or "").strip()
            state.last_promote_round_id = last_promote_round_id or None
        if state.last_promote_at is None:
            last_promote_at = str(payload.get("last_promote_at") or "").strip()
            state.last_promote_at = last_promote_at or None

    def _last_round_projection(self, state: LoopState) -> dict[str, Any] | None:
        if not state.round_history:
            return None
        latest = state.round_history[-1]
        verify = latest.verify_result
        return {
            "round_id": latest.round_id,
            "mode": latest.mode.value,
            "phase_reached": latest.phase_reached.value,
            "started_at": latest.started_at,
            "finished_at": latest.finished_at,
            "all_success": latest.all_success,
            "error": latest.error,
            "audit_run_id": latest.audit_run_id,
            "problems_found": latest.problems_found,
            "problems_fixed": latest.problems_fixed,
            "problems_failed": latest.problems_failed,
            "problems_skipped": latest.problems_skipped,
            "verify_all_green": bool(verify and verify.all_green),
            "full_pytest_total": verify.full_pytest_total if verify else 0,
            "full_pytest_failed": verify.full_pytest_failed if verify else 0,
        }

    def _control_plane_state_payload(self, state: LoopState) -> dict[str, Any]:
        provider_pool = state.provider_pool if isinstance(state.provider_pool, dict) else {}
        return {
            "_schema": "infra_promote_v1",
            "_note": (
                "Infra-only promote target for Loop Controller phase-1. "
                "State is projected from loop_controller and used as the canonical control-plane snapshot."
            ),
            "promote_target_mode": state.promote_target_mode,
            "last_promote_round_id": state.last_promote_round_id,
            "last_promote_at": state.last_promote_at,
            "consecutive_fix_success_count": state.consecutive_fix_success_count,
            "consecutive_verified_problem_fixes": state.consecutive_verified_problem_fixes,
            "goal_progress_count": self._goal_progress_count(state),
            "fix_goal": state.fix_goal,
            "goal_reached": self._goal_reached(state),
            "goal_ever_reached": state.goal_ever_reached,
            "success_goal_metric": state.success_goal_metric,
            "mode": state.mode.value,
            "phase": state.phase.value,
            "current_round_id": state.current_round_id,
            "blocked_reason": state.blocked_reason,
            "provider_pool": provider_pool,
            "last_audit_run_id": state.last_audit_run_id,
            "last_fix_wave_id": state.last_fix_wave_id,
            "total_fixes": state.total_fixes,
            "total_failures": state.total_failures,
            "round_history_size": len(state.round_history),
            "last_round_summary": self._last_round_projection(state),
        }

    def _render_control_plane_status_markdown(self, state: LoopState) -> str:
        last_round = self._last_round_projection(state) or {}
        goal_progress_count = self._goal_progress_count(state)
        provider_pool = state.provider_pool if isinstance(state.provider_pool, dict) else {}
        provider_status = str(provider_pool.get("status") or ("ready" if provider_pool.get("ready") else "")).strip() or "—"
        latest_round_result = "—"
        if last_round:
            latest_round_result = "success" if last_round.get("all_success") else "failed"
        blocked_reason = str(state.blocked_reason or "").strip() or "—"
        last_promote_round_id = str(state.last_promote_round_id or "").strip() or "—"
        last_promote_at = str(state.last_promote_at or "").strip() or "—"
        current_round_id = str(state.current_round_id or "").strip() or "—"
        allowed_section = (
            "## 允许的 promote 目标（Infra 层）\n\n"
            "阶段一仅推进以下两个文件，不触碰 `docs/core/22_全量功能进度总表_v7_精审.md`：\n\n"
            "- `automation/control_plane/current_state.json`（本 JSON 状态快照）\n"
            "- `automation/control_plane/current_status.md`（本 Markdown 摘要）"
        )
        if state.promote_target_mode != "infra":
            allowed_section = (
                "## 当前 promote 目标\n\n"
                f"当前 control plane 已切换为 `{state.promote_target_mode}`；"
                "本文件仍保留基础层状态摘要与防护边界。"
            )

        return (
            "# 当前自动化基础层状态\n\n"
            f"<!-- 本文件由 loop_controller 自动更新，promote_target_mode={state.promote_target_mode} 时写回此处 -->\n"
            "<!-- 禁止手动修改运行时字段；元数据注释可手动维护 -->\n\n"
            "## 当前执行层\n\n"
            "| 字段 | 值 |\n"
            "|------|-----|\n"
            f"| 模式 | {state.mode.value} |\n"
            f"| 阶段 | {state.phase.value} |\n"
            f"| 当前轮次 | {current_round_id} |\n"
            f"| 连续成功修复数 | {state.consecutive_fix_success_count} |\n"
            f"| 连续已验证问题修复数 | {state.consecutive_verified_problem_fixes} |\n"
            f"| 当前目标进度 | {goal_progress_count}/{state.fix_goal} |\n"
            f"| 目标成功数 | {state.fix_goal} |\n"
            f"| 目标达成 | {'是' if self._goal_reached(state) else '否'} |\n"
            f"| 成功度量指标 | {state.success_goal_metric} |\n"
            f"| 阻塞原因 | {blocked_reason} |\n"
            f"| Provider 状态 | {provider_status} |\n"
            f"| 最近 round 结果 | {latest_round_result} |\n"
            f"| 最近 promote 轮次 | {last_promote_round_id} |\n"
            f"| 最近 promote 时间 | {last_promote_at} |\n\n"
            f"{allowed_section}\n\n"
            "## 供参考：阶段一完成标准\n\n"
            "- `consecutive_verified_problem_fixes` >= `fix_goal`\n"
            "- 连续 verified fixes 均通过 pytest + Green Round Verdict 全部 9 项\n"
            "- 阶段自动切换为 MONITOR 模式\n"
            "- `docs/core/22_全量功能进度总表_v7_精审.md` 不在本阶段写回范围内\n"
        )

    def _write_control_plane_projection(self, state: LoopState) -> None:
        state_payload = self._control_plane_state_payload(state)
        _atomic_write_text(
            self._control_plane_state_path(),
            json.dumps(state_payload, indent=2, ensure_ascii=False),
        )
        _atomic_write_text(
            self._control_plane_status_path(),
            self._render_control_plane_status_markdown(state),
        )

    def _save_state(self, state: LoopState) -> None:
        self._sync_control_plane_config(state)
        self._store.save(state)
        self._write_control_plane_projection(state)

    def _update_state(self, **kwargs: object) -> LoopState:
        state = self._store.load()
        for key, value in kwargs.items():
            if hasattr(state, key):
                setattr(state, key, value)
        self._save_state(state)
        return state

    def _trim_history(self, max_rounds: int = 200) -> None:
        state = self._store.load()
        if len(state.round_history) > max_rounds:
            state.round_history = state.round_history[-max_rounds:]
            self._save_state(state)

    # -- lifecycle -----------------------------------------------------------

    def start(self, mode: LoopMode = LoopMode.FIX, fix_goal: int | None = None) -> LoopState:
        if self._running:
            return self._store.get()
        state = self._store.load()
        state.mode = mode
        state.fix_goal = fix_goal or self._cfg.fix_goal
        self._save_state(state)
        self._running = True
        self._stop_event.clear()
        self._force_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="loop-controller")
        self._thread.start()
        return self._store.get()

    def stop(self, reason: str = "manual") -> LoopState:
        logger.info("stop requested: %s", reason)
        self._stop_event.set()
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=30)
        return self._update_state(phase=LoopPhase.IDLE)

    def get_state(self) -> LoopState:
        return self._store.get()

    def record_round_complete(
        self,
        round_id: str,
        execution_id: str = "",
        status: str = "completed",
        error: str | None = None,
    ) -> None:
        """Record round completion from Kestra (external orchestrator)."""
        state = self._store.load()
        # Find the round in history and annotate it
        for r in reversed(state.round_history):
            if r.round_id == round_id:
                if status == "failed" and error:
                    r.error = error
                    r.all_success = False
                r.finished_at = r.finished_at or _utc_now_iso()
                break
        else:
            # Round not in history — create a stub record
            state.round_history.append(RoundSummary(
                round_id=round_id,
                started_at=_utc_now_iso(),
                finished_at=_utc_now_iso(),
                all_success=status == "completed",
                error=error,
            ))
        logger.info("round-complete recorded: %s status=%s exec=%s", round_id, status, execution_id)
        self._save_state(state)

    def force_new_round(
        self,
        mode: LoopMode = LoopMode.FIX,
        fix_goal: int | None = None,
    ) -> None:
        """Force a new round while the loop is already running.

        Switches mode to FIX (or given mode) and signals the loop thread to
        wake up immediately instead of waiting for the current interval.
        """
        state = self._store.load()
        state.mode = mode
        if fix_goal is not None:
            state.fix_goal = fix_goal
        self._save_state(state)
        # Wake the loop thread if it is sleeping on _stop_event.wait()
        # We use a separate event so the thread re-checks state without stopping.
        self._force_event.set()
        logger.info("force_new_round: mode=%s fix_goal=%s", mode.value, fix_goal)

    def await_round(self, timeout_seconds: int = 600) -> dict[str, object]:
        """Block until a round completes or timeout is reached.

        Returns dict with status, round_id, round_summary, timed_out.
        """
        state = self._store.load()
        baseline_count = len(state.round_history)

        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            state = self._store.load()
            if len(state.round_history) > baseline_count:
                latest = state.round_history[-1]
                return {
                    "status": "round_completed",
                    "round_id": latest.round_id,
                    "round_summary": latest,
                    "timed_out": False,
                }
            time.sleep(min(5.0, max(0, deadline - time.monotonic())))

        # Timeout — return current state
        state = self._store.load()
        return {
            "status": "timeout",
            "round_id": state.current_round_id,
            "round_summary": None,
            "timed_out": True,
        }

    # -- main loop -----------------------------------------------------------

    def _check_provider_readiness(self) -> dict[str, Any]:
        """Check New API provider availability via /v1/models + /v1/responses smoke.

        Three-stage check matching the Kestra health_checks.yml flow:
          1. GET /v1/models — must return 200 with >= 1 model
          2. POST /v1/responses — smoke test with first model id
        Returns dict with keys: ready (bool), status (str), error (str|None),
        stage (str — "models" | "responses" | "done").
        If NEW_API_BASE_URL is not configured, returns ready=False with status 'unconfigured'.
        """
        base_url = str(self._cfg.new_api_base_url or "").rstrip("/")
        if not base_url:
            return {"ready": False, "status": "unconfigured", "stage": "done", "error": "NEW_API_BASE_URL_NOT_SET"}
        headers: dict[str, str] = {}
        if self._cfg.new_api_token:
            headers["Authorization"] = f"Bearer {self._cfg.new_api_token}"

        # Stage 1: models
        try:
            models_resp = self._http.get(
                f"{base_url}/v1/models",
                headers=headers,
                timeout=15.0,
            )
            if models_resp.status_code != 200:
                return {
                    "ready": False,
                    "status": "degraded",
                    "stage": "models",
                    "error": f"NEW_API_MODELS_HTTP_{models_resp.status_code}",
                }
            models_data = models_resp.json().get("data") or []
            if not models_data:
                return {
                    "ready": False,
                    "status": "degraded",
                    "stage": "models",
                    "error": "NEW_API_NO_MODELS",
                }
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            return {"ready": False, "status": "down", "stage": "models",
                    "error": f"NEW_API_UNREACHABLE:{str(exc)[:100]}"}
        except Exception as exc:
            return {"ready": False, "status": "error", "stage": "models",
                    "error": f"NEW_API_CHECK_FAILED:{str(exc)[:100]}"}

        # Stage 2: responses smoke
        first_model_id = models_data[0].get("id", "")
        if first_model_id:
            try:
                smoke_headers = {**headers, "Content-Type": "application/json"}
                smoke_resp = self._http.post(
                    f"{base_url}/v1/responses",
                    headers=smoke_headers,
                    json={"model": first_model_id, "input": "ping", "max_output_tokens": 1},
                    timeout=30.0,
                )
                if smoke_resp.status_code != 200:
                    return {
                        "ready": False,
                        "status": "provider_safe_hold",
                        "stage": "responses",
                        "error": f"NEW_API_RESPONSES_SMOKE_HTTP_{smoke_resp.status_code}",
                    }
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                return {"ready": False, "status": "provider_safe_hold", "stage": "responses",
                        "error": f"NEW_API_RESPONSES_UNREACHABLE:{str(exc)[:100]}"}
            except Exception as exc:
                return {"ready": False, "status": "provider_safe_hold", "stage": "responses",
                        "error": f"NEW_API_RESPONSES_CHECK_FAILED:{str(exc)[:100]}"}

        return {"ready": True, "status": "ok", "stage": "done", "error": None}

    def _trigger_infra_doctor(self, state: LoopState) -> None:
        """Run infra_doctor diagnostic when stall is detected.

        If infra_doctor finds critical issues, enter SAFE_HOLD.
        """
        logger.info("triggering infra_doctor diagnostic...")
        try:
            from automation.diagnostics.infra_doctor import run_full_diagnostic

            report = run_full_diagnostic()
            overall = report.get("overall_status", "unknown")
            criticals = report.get("critical_issues", [])
            warnings = report.get("warnings", [])
            logger.info(
                "infra_doctor: overall=%s, criticals=%d, warnings=%d",
                overall, len(criticals), len(warnings),
            )
            for c in criticals:
                logger.error("infra_doctor critical: %s", c)
            for w in warnings:
                logger.warning("infra_doctor warning: %s", w)

            # Save report to output
            import json as _json

            out_path = self._cfg.repo_root / "output" / "infra_doctor_report.json"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                _json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8",
            )

            if overall == "critical":
                logger.error(
                    "infra_doctor found critical issues — entering SAFE_HOLD: %s",
                    criticals,
                )
                state.mode = LoopMode.SAFE_HOLD
                state.blocked_reason = f"STALL_INFRA_DOCTOR:{','.join(criticals[:3])}"
                self._save_state(state)
            # Reset counter regardless so we don't re-trigger immediately
            self._consecutive_no_progress_rounds = 0
        except Exception as exc:
            logger.exception("infra_doctor failed: %s", exc)
            self._consecutive_no_progress_rounds = 0

    def _run_loop(self) -> None:
        logger.info("loop started")
        try:
            while not self._stop_event.is_set():
                state = self._store.load()
                interval = (
                    self._cfg.monitor_interval_seconds
                    if state.mode in (LoopMode.MONITOR, LoopMode.SAFE_HOLD)
                    else self._cfg.audit_interval_seconds
                )
                # SAFE_HOLD: probe provider readiness before attempting a round
                if state.mode == LoopMode.SAFE_HOLD:
                    provider_status = self._check_provider_readiness()
                    if provider_status.get("ready", True):
                        logger.info("provider recovered — exiting SAFE_HOLD → FIX")
                        state.mode = LoopMode.FIX
                        state.phase = LoopPhase.IDLE
                        state.blocked_reason = None
                        state.provider_pool = provider_status
                        self._save_state(state)
                    else:
                        logger.debug("SAFE_HOLD active: %s", provider_status.get("error"))
                        self._stop_event.wait(timeout=float(interval))
                        continue
                try:
                    _pre_progress = self._goal_progress_count(state)
                    _pre_failures = state.total_failures
                    self._run_one_round(state)
                    self._consecutive_round_failures = 0
                    # Stall detection: check if this round made any progress
                    _post_progress = self._goal_progress_count(state)
                    _post_failures = state.total_failures
                    if _post_progress > _pre_progress:
                        self._consecutive_no_progress_rounds = 0
                    else:
                        self._consecutive_no_progress_rounds += 1
                        logger.warning(
                            "no progress round %d/%d (progress=%d, failures +%d)",
                            self._consecutive_no_progress_rounds,
                            self._stall_threshold,
                            _post_progress,
                            _post_failures - _pre_failures,
                        )
                    if self._consecutive_no_progress_rounds >= self._stall_threshold:
                        logger.error(
                            "STALL DETECTED: %d consecutive rounds with no progress — running infra_doctor",
                            self._consecutive_no_progress_rounds,
                        )
                        self._trigger_infra_doctor(state)
                except AuditFailedError as exc:
                    logger.error("audit failed — skipping round: %s", exc)
                    self._update_state(phase=LoopPhase.IDLE)
                    self._consecutive_round_failures += 1
                except (httpx.TimeoutException, httpx.ConnectError) as exc:
                    # Transient network errors — apply backoff
                    self._consecutive_round_failures += 1
                    backoff = min(
                        _BASE_BACKOFF_SECONDS * (2 ** min(self._consecutive_round_failures, _MAX_BACKOFF_FAILURES)),
                        interval * 2,
                    )
                    logger.warning(
                        "transient error (attempt %d) — backing off %ds: %s",
                        self._consecutive_round_failures, backoff, exc,
                    )
                    self._update_state(phase=LoopPhase.IDLE)
                    if self._stop_event.wait(timeout=backoff):
                        break
                    continue  # skip the normal interval wait below
                except Exception as exc:
                    logger.exception("round failed unexpectedly: %s", exc)
                    self._consecutive_round_failures += 1
                    self._update_state(phase=LoopPhase.IDLE)
                self._trim_history(max_rounds=200)
                # Wait for the interval, but wake early if force_event is set.
                self._force_event.clear()
                remaining = max(0.0, float(interval))
                while remaining > 0:
                    step = min(1.0, remaining)
                    if self._stop_event.wait(timeout=step):
                        break
                    if self._force_event.is_set():
                        self._force_event.clear()
                        remaining = 0.0
                        break
                    remaining -= step
                if self._stop_event.is_set():
                    break
        finally:
            self._running = False
            logger.info("loop exited")

    def _run_one_round(self, state: LoopState) -> None:
        round_id = _next_round_id(state.round_history)
        summary = RoundSummary(round_id=round_id, started_at=_utc_now_iso(), mode=state.mode)
        state.current_round_id = round_id
        self._save_state(state)

        # ① AUDIT
        state.phase = LoopPhase.AUDITING
        summary.phase_reached = LoopPhase.AUDITING
        self._save_state(state)
        audit_run_id, bundle = self._do_audit(state)
        summary.audit_run_id = audit_run_id
        state.last_audit_run_id = audit_run_id

        # ② ANALYZE
        state.phase = LoopPhase.ANALYZING
        summary.phase_reached = LoopPhase.ANALYZING
        self._save_state(state)
        problems, new_count, regression_count, skipped = analyze_bundle(bundle, state.fixed_problems)
        del new_count, regression_count, skipped

        # filter out external_blocked from actionable queue
        actionable = [p for p in problems if not p.is_external_blocked]
        summary.problems_found = len(actionable)

        # drift detection
        _, current_fps = self._verifier.check_artifact_alignment()
        drifted = detect_drift(current_fps, state.last_artifact_fingerprints)
        if drifted:
            logger.warning("artifact drift detected: %s", drifted)

        # decide: need fixing?
        if not actionable and not drifted:
            if state.mode == LoopMode.FIX and self._goal_reached(state):
                # goal reached — switch to monitor
                state.mode = LoopMode.MONITOR
                logger.info(
                    "fix goal reached (%d/%d %s) — switching to MONITOR",
                    self._goal_progress_count(state),
                    state.fix_goal,
                    state.success_goal_metric,
                )
            summary.finished_at = _utc_now_iso()
            summary.all_success = True
            state.phase = LoopPhase.IDLE if state.mode == LoopMode.FIX else LoopPhase.MONITORING
            state.round_history.append(summary)
            state.last_artifact_fingerprints = current_fps
            self._save_state(state)
            return

        # if in monitor mode and problems found → switch back to fix
        if state.mode == LoopMode.MONITOR and (actionable or drifted):
            wake_reasons: list[str] = []
            if actionable:
                wake_reasons.append(f"{len(actionable)}_new_problems")
                regression_count = sum(1 for p in actionable if p.is_regression)
                if regression_count:
                    wake_reasons.append(f"{regression_count}_regressions")
            if drifted:
                wake_reasons.append(f"drift:{','.join(drifted[:5])}")
            wake_reason = "; ".join(wake_reasons)
            logger.info("MONITOR auto-wake → FIX (reason: %s)", wake_reason)
            state.mode = LoopMode.FIX
            if not state.goal_ever_reached:
                state.consecutive_fix_success_count = 0
                state.consecutive_verified_problem_fixes = 0

        state.problems_queue = actionable
        self._save_state(state)
        runtime_context = self._load_runtime_context()

        # ② b) PLANNING — provider readiness gate
        state.phase = LoopPhase.PLANNING
        summary.phase_reached = LoopPhase.PLANNING
        self._save_state(state)
        provider_status = self._check_provider_readiness()
        state.provider_pool = provider_status
        if not provider_status.get("ready", True):
            logger.warning(
                "provider not ready — entering SAFE_HOLD: %s",
                provider_status.get("error"),
            )
            state.mode = LoopMode.SAFE_HOLD
            state.phase = LoopPhase.BLOCKED
            state.blocked_reason = provider_status.get("error") or "PROVIDER_NOT_READY"
            summary.all_success = False
            summary.error = state.blocked_reason
            summary.finished_at = _utc_now_iso()
            state.round_history.append(summary)
            self._save_state(state)
            return
        state.blocked_reason = None
        self._save_state(state)

        # Claim writeback lease BEFORE fix — covers all write_scope paths from problems
        # plus the promote target path (doc22) so that promote commits are also fenced.
        lease_id: str | None = None
        fencing_token: int | None = None
        coord = self._get_writeback_coordination()
        target_paths: set[str] = set()
        for prob in actionable:
            for wp in prob.write_scope:
                if isinstance(wp, str) and wp.strip():
                    target_paths.add(wp.strip())
        # Include promote target so promote commits are covered by the same lease
        promote_doc = "docs/core/22_\u5168\u91cf\u529f\u80fd\u8fdb\u5ea6\u603b\u8868_v7_\u7cbe\u5ba1.md"
        target_paths.add(promote_doc)
        if target_paths:
            if coord is None:
                logger.error(
                    "pre-fix lease claim blocked for %s: writeback coordination unavailable",
                    round_id,
                )
                if not state.goal_ever_reached:
                    state.consecutive_fix_success_count = 0
                    state.consecutive_verified_problem_fixes = 0
                state.total_failures += 1
                summary.all_success = False
                summary.error = "WRITEBACK_LEASE_COORDINATION_UNAVAILABLE"
                summary.finished_at = _utc_now_iso()
                state.phase = LoopPhase.IDLE
                state.problems_queue = []
                state.round_history.append(summary)
                self._save_state(state)
                return
            try:
                lease = coord.claim(round_id, sorted(target_paths), lease_seconds=600)
                lease_id = str(getattr(lease, "lease_id", "") or "").strip()
                fencing_token = int(getattr(lease, "fencing_token", 0) or 0)
            except Exception as exc:
                logger.error("pre-fix lease claim failed for %s: %s", round_id, exc)
                if not state.goal_ever_reached:
                    state.consecutive_fix_success_count = 0
                    state.consecutive_verified_problem_fixes = 0
                state.total_failures += 1
                summary.all_success = False
                summary.error = f"WRITEBACK_LEASE_CLAIM_FAILED:{exc}"
                summary.finished_at = _utc_now_iso()
                state.phase = LoopPhase.IDLE
                state.problems_queue = []
                state.round_history.append(summary)
                self._save_state(state)
                return
            if not lease_id or fencing_token <= 0:
                logger.error(
                    "pre-fix lease claim returned invalid credentials for %s: lease_id=%s fencing=%s",
                    round_id,
                    lease_id,
                    fencing_token,
                )
                if not state.goal_ever_reached:
                    state.consecutive_fix_success_count = 0
                    state.consecutive_verified_problem_fixes = 0
                state.total_failures += 1
                summary.all_success = False
                summary.error = "WRITEBACK_LEASE_INVALID"
                summary.finished_at = _utc_now_iso()
                state.phase = LoopPhase.IDLE
                state.problems_queue = []
                state.round_history.append(summary)
                self._save_state(state)
                return

        # ③ FIX
        state.phase = LoopPhase.FIXING
        summary.phase_reached = LoopPhase.FIXING
        self._save_state(state)
        fix_results = self._do_fix(actionable, round_id, state, runtime_context)

        # ④ WRITE BACK
        state.phase = LoopPhase.WRITING_BACK
        summary.phase_reached = LoopPhase.WRITING_BACK
        self._save_state(state)
        fix_results, applied_commits = self._apply_fix_commits(
            fix_results,
            round_id=round_id,
            runtime_context=runtime_context,
            lease_id=lease_id,
            fencing_token=fencing_token,
        )
        state.last_fix_wave_id = round_id
        summary.fix_results = fix_results
        summary.problems_fixed = sum(1 for r in fix_results if r.outcome == FixOutcome.SUCCESS)
        summary.problems_failed = sum(1 for r in fix_results if r.outcome == FixOutcome.FAILED)
        summary.problems_skipped = sum(
            1 for r in fix_results if r.outcome in (FixOutcome.SKIPPED, FixOutcome.EXTERNAL_BLOCKED)
        )

        # ⑤ VERIFY
        state.phase = LoopPhase.VERIFYING
        summary.phase_reached = LoopPhase.VERIFYING
        self._save_state(state)
        changed_files = self._collect_affected_tests(fix_results)
        verify = self._verifier.run_full_pipeline(changed_files, round_id)
        summary.verify_result = verify

        if not verify.all_green:
            # rollback — reset counter
            logger.warning("verification failed for round %s — resetting counter", round_id)
            self._rollback_fix_commits(applied_commits, round_id=round_id)
            self._release_lease(lease_id, round_id)
            if not state.goal_ever_reached:
                state.consecutive_fix_success_count = 0
                state.consecutive_verified_problem_fixes = 0
            state.total_failures += summary.problems_failed or 1
            summary.all_success = False
            summary.finished_at = _utc_now_iso()
            state.phase = LoopPhase.IDLE
            state.problems_queue = []
            state.round_history.append(summary)
            self._save_state(state)
            return

        # ⑥ PROMOTE
        state.phase = LoopPhase.PROMOTING
        summary.phase_reached = LoopPhase.PROMOTING
        self._save_state(state)
        promote_result = self._do_promote(
            round_id, state, runtime_context,
            lease_id=lease_id, fencing_token=fencing_token,
        )
        if str(promote_result.get("status_note", {}).get("status") or "").lower() == "failed":
            self._release_lease(lease_id, round_id)
            state.total_failures += summary.problems_failed or 1
            # Do NOT credit fixes when promote fails — promote completion
            # is a prerequisite for the success counter.
            state.consecutive_fix_success_count = 0
            state.consecutive_verified_problem_fixes = 0
            summary.all_success = False
            summary.error = str(promote_result["status_note"].get("error") or "STATUS_NOTE_PROMOTE_FAILED")
            summary.finished_at = _utc_now_iso()
            state.phase = LoopPhase.IDLE
            state.problems_queue = []
            state.round_history.append(summary)
            self._save_state(state)
            return
        if str(promote_result.get("current_layer", {}).get("status") or "").lower() == "failed":
            self._release_lease(lease_id, round_id)
            state.total_failures += summary.problems_failed or 1
            # Do NOT credit fixes when promote fails — promote completion
            # is a prerequisite for the success counter.
            state.consecutive_fix_success_count = 0
            state.consecutive_verified_problem_fixes = 0
            summary.all_success = False
            summary.error = str(promote_result["current_layer"].get("error") or "CURRENT_LAYER_PROMOTE_FAILED")
            summary.finished_at = _utc_now_iso()
            state.phase = LoopPhase.IDLE
            state.problems_queue = []
            state.round_history.append(summary)
            self._save_state(state)
            return
        state.last_promote_round_id = round_id
        state.last_promote_at = _utc_now_iso()

        # ⑦ evaluate Green Round Verdict (9-item gate)
        verdict = self._evaluate_green_round(
            summary=summary,
            verify=verify,
            applied_commits=applied_commits,
            promote_result=promote_result,
            drifted=drifted,
            actionable=actionable,
            state=state,
            runtime_context=runtime_context,
        )
        state.total_fixes += summary.problems_fixed
        for r in fix_results:
            if r.outcome == FixOutcome.SUCCESS and r.problem_id not in state.fixed_problems:
                state.fixed_problems.append(r.problem_id)
        if verdict.is_green:
            state.consecutive_fix_success_count += summary.problems_fixed
            state.consecutive_verified_problem_fixes += summary.problems_fixed
            summary.all_success = True
        else:
            failed_checks = [
                name for name, val in [
                    ("no_new_active", verdict.no_new_active),
                    ("no_regression", verdict.no_regression),
                    ("no_drift", verdict.no_drift),
                    ("verify_all_green", verdict.verify_all_green),
                    ("no_partial_fail", verdict.no_partial_fail),
                    ("no_masked_skip", verdict.no_masked_skip),
                    ("batch_writeback_complete", verdict.batch_writeback_complete),
                    ("promote_gate_passed", verdict.promote_gate_passed),
                    ("artifacts_aligned", verdict.artifacts_aligned),
                ] if not val
            ]
            logger.warning(
                "round %s not fully green — failed checks: %s",
                round_id, ", ".join(failed_checks),
            )
            # Only hard-reset counter on actual regression or fix failure.
            # Environment checks (artifacts, catalog, blind_spot) that were
            # already stale before this round should NOT wipe fix progress.
            fix_regression = not verdict.no_regression or not verdict.no_partial_fail
            if fix_regression or (summary.problems_fixed == 0 and not state.goal_ever_reached):
                state.consecutive_fix_success_count = 0
                state.consecutive_verified_problem_fixes = 0
            else:
                # Fixes succeeded but environment checks failed — credit the fixes
                state.consecutive_fix_success_count += summary.problems_fixed
                logger.info(
                    "round %s: %d fixes credited despite env check failures: %s",
                    round_id, summary.problems_fixed, ", ".join(failed_checks),
                )
            state.total_failures += max(1, summary.problems_failed + summary.problems_skipped)
            summary.all_success = False
            summary.error = f"GREEN_VERDICT_FAILED:{','.join(failed_checks)}"

        # check goal — use verified problem fixes as the canonical metric
        if self._goal_reached(state):
            state.goal_ever_reached = True
            logger.info(
                "fix goal reached (%d/%d %s) — switching to MONITOR",
                self._goal_progress_count(state),
                state.fix_goal,
                state.success_goal_metric,
            )
            state.mode = LoopMode.MONITOR
        elif state.goal_ever_reached and verdict.is_green:
            # After initial goal achievement, any single green round
            # returns immediately to MONITOR (no need to re-accumulate).
            logger.info(
                "goal previously achieved — green round triggers MONITOR return",
            )
            state.mode = LoopMode.MONITOR

        summary.finished_at = _utc_now_iso()
        self._release_lease(lease_id, round_id)
        _, new_fps = self._verifier.check_artifact_alignment()
        state.last_artifact_fingerprints = new_fps
        state.phase = (
            LoopPhase.MONITORING
            if state.mode == LoopMode.MONITOR and summary.all_success
            else LoopPhase.IDLE
        )
        state.problems_queue = []
        state.round_history.append(summary)
        self._save_state(state)

    # -- phase implementations -----------------------------------------------

    def _get_writeback_coordination(self) -> Any:
        """Return a WritebackCoordination instance or None if unavailable."""
        try:
            from app.services.writeback_coordination import WritebackCoordination
            state_path = self._cfg.repo_root / "runtime" / "writeback_coordination" / "state.json"
            return WritebackCoordination(state_path)
        except Exception:
            return None

    def _release_lease(self, lease_id: str | None, round_id: str) -> None:
        """Release a writeback lease if it was acquired."""
        if not lease_id:
            return
        coord = self._get_writeback_coordination()
        if coord is not None:
            try:
                coord.release(lease={"lease_id": lease_id}, reason=f"round-{round_id}-complete")
            except Exception as exc:
                logger.warning("lease release failed for %s: %s", lease_id, exc)

    def _evaluate_green_round(
        self,
        *,
        summary: RoundSummary,
        verify: VerifyResult,
        applied_commits: list[dict[str, Any]],
        promote_result: dict[str, Any],
        drifted: list[str],
        actionable: list[ProblemSpec],
        state: LoopState,
        runtime_context: dict[str, Any] | None = None,
    ) -> GreenRoundVerdict:
        """Evaluate the 9-item green round verdict.

        no_new_active: All found problems were addressed (none left unfixed).
        no_regression: None of the found problems are regressions.
        """
        unfixed_count = summary.problems_failed + summary.problems_skipped
        status_note_status = str(promote_result.get("status_note", {}).get("status") or "").lower()
        current_layer_status = str(promote_result.get("current_layer", {}).get("status") or "").lower()
        promote_statuses_ok = status_note_status in ("committed", "skipped") and current_layer_status in ("committed", "skipped")
        runtime_gates = ((runtime_context or {}).get("runtime_gates") or {}) if runtime_context else {}
        runtime_gate_ok = True
        if runtime_context is not None:
            runtime_gate_ok = (
                str(runtime_gates.get("status") or "").lower() == "ready"
                and bool((runtime_gates.get("shared_artifact_promote") or {}).get("allowed"))
            )
        return GreenRoundVerdict(
            no_new_active=unfixed_count == 0,
            no_regression=not any(p.is_regression for p in actionable),
            no_drift=len(drifted) == 0,
            verify_all_green=verify.all_green,
            no_partial_fail=summary.problems_failed == 0 and summary.problems_skipped == 0,
            no_masked_skip=not any(
                r.outcome == FixOutcome.SKIPPED and not r.error
                for r in summary.fix_results
            ),
            batch_writeback_complete=len(applied_commits) > 0 or summary.problems_fixed == 0,
            promote_gate_passed=verify.all_green and promote_statuses_ok and runtime_gate_ok,
            artifacts_aligned=verify.artifacts_aligned,
        )

    def _auth_headers(self, token: str) -> dict[str, str]:
        if not token:
            return {}
        return {"Authorization": f"Bearer {token}"}

    def _internal_headers(self) -> dict[str, str]:
        if not self._cfg.internal_token:
            return {}
        return {"X-Internal-Token": self._cfg.internal_token}

    def _degraded_runtime_context(self, reason: str) -> dict[str, Any]:
        return {
            "runtime_gates": {
                "status": "blocked",
                "runtime_live_recovery": {
                    "allowed": False,
                    "blocking_flags": ["app_audit_context_unavailable"],
                    "runtime_state": "unknown",
                    "runtime_flags": ["app_audit_context_unavailable"],
                },
                "shared_artifact_promote": {
                    "allowed": False,
                    "artifact_files_present": False,
                    "catalog_fresh": False,
                    "junit_clean": False,
                },
                "llm_router": {
                    "primary_status": "unknown",
                    "ready": False,
                },
            },
            "public_runtime_status": "UNKNOWN",
            "context_status": "unavailable",
            "context_error": reason,
        }

    def _load_runtime_context(self) -> dict[str, Any]:
        base_url = str(self._cfg.app_base_url or "").rstrip("/")
        if not base_url:
            return self._degraded_runtime_context("APP_BASE_URL_NOT_CONFIGURED")
        try:
            resp = self._http.get(
                f"{base_url}/api/v1/internal/audit/context",
                headers=self._internal_headers(),
            )
            resp.raise_for_status()
            body = resp.json()
            data = body.get("data") if isinstance(body, dict) else None
            if isinstance(data, dict):
                return data
            return self._degraded_runtime_context("APP_AUDIT_CONTEXT_INVALID")
        except Exception as exc:
            logger.warning("failed to load app audit context: %s", exc)
            return self._degraded_runtime_context(f"APP_AUDIT_CONTEXT_UNAVAILABLE:{exc}")

    def _await_audit_run(self, run_id: str, timeout_seconds: int) -> dict[str, Any]:
        deadline = time.monotonic() + max(1, int(timeout_seconds))
        last_error = ""
        while True:
            try:
                resp = self._http.get(
                    f"{self._cfg.mesh_runner_url}/v1/runs/{run_id}",
                    headers=self._auth_headers(self._cfg.mesh_runner_token),
                    timeout=30.0,
                )
                resp.raise_for_status()
                data = resp.json()
                run_status = str(data.get("status") or "").lower()
                if run_status in ("completed", "finished", "failed"):
                    return data
                last_error = ""
            except (httpx.HTTPError, ValueError) as exc:
                last_error = str(exc)

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if self._stop_event.wait(timeout=min(_AUDIT_STATUS_POLL_SECONDS, remaining)):
                raise AuditFailedError(f"audit run {run_id} interrupted")

        detail = f": {last_error}" if last_error else ""
        raise AuditFailedError(f"audit run {run_id} not completed within {timeout_seconds}s{detail}")

    def _do_audit(self, state: LoopState) -> tuple[str, dict[str, Any]]:
        """Start a mesh_runner audit run and wait for completion."""
        # Readonly issue-mesh runs are always materialized as 12 shards.
        # Keep the canonical floor at 12 so the mesh executor can schedule the
        # full shard set in one wave and avoid partial re-submission churn.
        max_workers = max(12, _desired_workers(len(state.problems_queue), state.mode))

        # --- Build full audit_context (P0 closure) ---
        # Fetch authoritative runtime context from the app API so that the
        # mesh_runner receives a complete snapshot instead of minimal metadata.
        runtime_ctx = self._load_runtime_context()
        audit_context: dict[str, Any] = {
            "source": "loop_controller",
            "round_id": state.current_round_id,
            "requested_workers": max_workers,
            "effective_workers": max_workers,
            "cap_reason": "default_baseline" if max_workers == 12 else "dynamic",
        }
        for key in (
            "runtime_gates",
            "shared_artifacts",
            "runtime_anchors",
            "docs",
            "automation",
        ):
            if key in runtime_ctx:
                audit_context[key] = runtime_ctx[key]

        payload = {
            "max_workers": max_workers,
            "wait_for_completion": False,
            "audit_context": audit_context,
        }
        try:
            resp = self._http.post(
                f"{self._cfg.mesh_runner_url}/v1/runs",
                json=payload,
                headers=self._auth_headers(self._cfg.mesh_runner_token),
                timeout=30.0,
            )
            resp.raise_for_status()
            initial = resp.json()
            run_id = str(initial.get("run_id") or "")
            if not run_id:
                raise AuditFailedError("audit run missing run_id")
            data = initial
            run_status = str(data.get("status") or "").lower()
            if run_status not in ("completed", "finished", "failed"):
                data = self._await_audit_run(run_id, timeout_seconds=3600)
                run_status = str(data.get("status") or "").lower()
            if run_status not in ("completed", "finished", "failed"):
                raise AuditFailedError(f"audit run {run_id} not completed: status={run_status}")
            # fetch bundle
            bundle_resp = self._http.get(
                f"{self._cfg.mesh_runner_url}/v1/runs/{run_id}/bundle",
                headers=self._auth_headers(self._cfg.mesh_runner_token),
                timeout=30.0,
            )
            bundle_resp.raise_for_status()
            bundle = bundle_resp.json().get("bundle", {})
            if run_status == "failed":
                logger.warning(
                    "audit run %s finished with status=failed but produced a bundle; continuing with degraded audit output",
                    run_id,
                )
            return run_id, bundle
        except Exception as exc:
            raise AuditFailedError(f"audit failed: {exc}") from exc

    def _do_fix(
        self,
        problems: list[ProblemSpec],
        round_id: str,
        state: LoopState,
        runtime_context: dict[str, Any],
    ) -> list[FixResult]:
        """Trigger code fix for each problem via promote_prep synthesize-patches."""
        results: list[FixResult] = []
        for prob in problems:
            if self._stop_event.is_set():
                results.append(FixResult(problem_id=prob.problem_id, outcome=FixOutcome.SKIPPED))
                continue
            if prob.is_external_blocked:
                results.append(FixResult(problem_id=prob.problem_id, outcome=FixOutcome.EXTERNAL_BLOCKED))
                continue

            start_t = time.monotonic()
            payload = {
                "source_run_id": state.last_audit_run_id or "",
                "fix_run_id": round_id,
                "max_fix_items": 1,
                "runtime_gates": runtime_context.get("runtime_gates") or {},
                "audit_context": {
                    **runtime_context,
                    "problem_id": prob.problem_id,
                    "issue_key": prob.family,
                    "family": prob.family,
                    "severity": prob.severity.value,
                    "round_id": round_id,
                    "source": "loop_controller",
                },
            }
            last_error = "UNKNOWN_FIX_ERROR"
            max_fix_attempts = 3
            transient_codes = {429, 500, 502, 503, 504}
            for attempt in range(max_fix_attempts):
                try:
                    resp = self._http.post(
                        f"{self._cfg.promote_prep_url}/v1/triage/synthesize-patches",
                        json=payload,
                        headers=self._auth_headers(self._cfg.promote_prep_token),
                        timeout=1800.0,
                    )
                    if resp.status_code != 200:
                        last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                        if resp.status_code in transient_codes and attempt < (max_fix_attempts - 1):
                            backoff_seconds = 2 * (attempt + 1)
                            logger.warning(
                                "fix request transient failure for %s (attempt %d/%d): %s",
                                prob.problem_id,
                                attempt + 1,
                                max_fix_attempts,
                                last_error,
                            )
                            time.sleep(backoff_seconds)
                            continue
                        break

                    body = resp.json()
                    skip_reason = str(body.get("skip_reason") or "").strip()
                    raw_patches = body.get("patches", body.get("patches_applied", []))
                    elapsed = time.monotonic() - start_t
                    if skip_reason == "NO_FIXABLE_FINDINGS":
                        results.append(FixResult(
                            problem_id=prob.problem_id,
                            outcome=FixOutcome.SKIPPED,
                            fix_run_id=round_id,
                            patches_applied=[],
                            patches_raw=raw_patches if isinstance(raw_patches, list) else [],
                            error=skip_reason,
                            duration_seconds=elapsed,
                        ))
                        break
                    valid_patch_objects = [
                        p for p in raw_patches
                        if isinstance(p, dict)
                        and bool(p.get("valid", bool(p.get("target_path") and p.get("patch_text"))))
                        and str(p.get("target_path") or "").strip()
                    ]
                    if not valid_patch_objects:
                        invalid_reason = "NO_VALID_PATCHES"
                        if isinstance(raw_patches, list):
                            for item in raw_patches:
                                if isinstance(item, dict):
                                    candidate_reason = str(item.get("explanation") or "").strip()
                                    if candidate_reason:
                                        invalid_reason = candidate_reason
                                        break
                        results.append(FixResult(
                            problem_id=prob.problem_id,
                            outcome=FixOutcome.FAILED,
                            fix_run_id=round_id,
                            patches_applied=[],
                            patches_raw=raw_patches if isinstance(raw_patches, list) else [],
                            error=invalid_reason,
                            duration_seconds=elapsed,
                        ))
                        break
                    # Extract file paths from patch objects (dicts) or plain strings
                    file_paths = [
                        p["target_path"] if isinstance(p, dict) else p
                        for p in valid_patch_objects
                        if (isinstance(p, dict) and p.get("target_path")) or isinstance(p, str)
                    ]
                    results.append(FixResult(
                        problem_id=prob.problem_id,
                        outcome=FixOutcome.SUCCESS,
                        fix_run_id=round_id,
                        patches_applied=file_paths,
                        patches_raw=raw_patches if isinstance(raw_patches, list) else [],
                        duration_seconds=elapsed,
                    ))
                    break
                except (httpx.TimeoutException, httpx.ConnectError) as exc:
                    last_error = f"FIX_REQUEST_TRANSIENT:{str(exc)[:160]}"
                    if attempt < (max_fix_attempts - 1):
                        backoff_seconds = 2 * (attempt + 1)
                        logger.warning(
                            "fix request transient exception for %s (attempt %d/%d): %s",
                            prob.problem_id,
                            attempt + 1,
                            max_fix_attempts,
                            last_error,
                        )
                        time.sleep(backoff_seconds)
                        continue
                    break
                except Exception as exc:
                    last_error = str(exc)[:200]
                    break
            else:
                last_error = "FIX_REQUEST_MAX_RETRIES_EXCEEDED"

            elapsed = time.monotonic() - start_t
            if not results or results[-1].problem_id != prob.problem_id:
                results.append(FixResult(
                    problem_id=prob.problem_id,
                    outcome=FixOutcome.FAILED,
                    error=last_error,
                    duration_seconds=elapsed,
                ))
        return results

    def _collect_affected_tests(self, fix_results: list[FixResult]) -> list[str]:
        """Collect changed app/tests paths for scoped pytest inference."""
        changed_files: set[str] = set()
        for result in fix_results:
            for path in result.patches_applied:
                if isinstance(path, str) and (path.startswith("app/") or path.startswith("tests/")):
                    changed_files.add(path)
        return sorted(changed_files)

    def _apply_fix_commits(
        self,
        fix_results: list[FixResult],
        *,
        round_id: str,
        runtime_context: dict[str, Any],
        lease_id: str | None = None,
        fencing_token: int | None = None,
    ) -> tuple[list[FixResult], list[dict[str, Any]]]:
        runtime_gates = runtime_context.get("runtime_gates") or {}
        updated_results: list[FixResult] = []
        applied_commits: list[dict[str, Any]] = []
        committed_targets: set[str] = set()

        for result in fix_results:
            result_copy = result.model_copy(deep=True)
            if result_copy.outcome != FixOutcome.SUCCESS:
                updated_results.append(result_copy)
                continue

            patch_lookup = {
                str(raw.get("target_path") or ""): raw
                for raw in result_copy.patches_raw
                if isinstance(raw, dict) and raw.get("target_path")
            }
            committed = False
            failure_reason: str | None = None

            for target_path in result_copy.patches_applied:
                if target_path in committed_targets:
                    failure_reason = "DUPLICATE_TARGET_IN_ROUND"
                    continue

                raw_patch = patch_lookup.get(target_path, {})
                patch_text = str(raw_patch.get("patch_text") or "").strip()
                base_sha256 = str(raw_patch.get("base_sha256") or "").strip()
                if not patch_text:
                    failure_reason = "PATCH_TEXT_MISSING"
                    continue
                if not base_sha256:
                    for _read_attempt in range(2):
                        try:
                            read_resp = self._http.post(
                                f"{self._cfg.writeback_a_url}/v1/read",
                                json={"target_path": target_path},
                                headers=self._auth_headers(self._cfg.writeback_a_token),
                                timeout=120.0,
                            )
                            if read_resp.status_code == 200:
                                base_sha256 = str(read_resp.json().get("sha256") or "").strip()
                                break
                            elif read_resp.status_code == 404:
                                base_sha256 = hashlib.sha256(b"").hexdigest()
                                break
                            else:
                                failure_reason = f"WRITEBACK_READ_FAILED:{read_resp.status_code}"
                        except Exception as exc:
                            failure_reason = f"WRITEBACK_READ_FAILED:{str(exc)[:160]}"
                        if _read_attempt == 0:
                            time.sleep(2)
                if not base_sha256:
                    failure_reason = failure_reason or "PATCH_CONTEXT_INCOMPLETE"
                    continue

                try:
                    preview_resp = self._http.post(
                        f"{self._cfg.writeback_a_url}/v1/preview",
                        json={
                            "target_path": target_path,
                            "base_sha256": base_sha256,
                            "patch_text": patch_text,
                        },
                        headers=self._auth_headers(self._cfg.writeback_a_token),
                        timeout=120.0,
                    )
                    preview_resp.raise_for_status()
                    preview_summary = preview_resp.json()
                except Exception as exc:
                    failure_reason = f"WRITEBACK_PREVIEW_FAILED:{str(exc)[:160]}"
                    continue

                if bool(preview_summary.get("conflict")):
                    failure_reason = "PREVIEW_CONFLICT"
                    continue

                try:
                    triage_resp = self._http.post(
                        f"{self._cfg.promote_prep_url}/v1/triage/writeback",
                        json={
                            "run_id": round_id,
                            "workflow_id": "loop_controller",
                            "layer": "code-fix",
                            "target_path": target_path,
                            "patch_text": patch_text,
                            "base_sha256": base_sha256,
                            "runtime_gates": runtime_gates,
                            "audit_context": {
                                **runtime_context,
                                "round_id": round_id,
                                "problem_id": result_copy.problem_id,
                                "source": "loop_controller",
                            },
                            "preview_summary": preview_summary,
                            "metadata": {
                                "round_id": round_id,
                                "problem_id": result_copy.problem_id,
                            },
                        },
                        headers=self._auth_headers(self._cfg.promote_prep_token),
                        timeout=120.0,
                    )
                    triage_resp.raise_for_status()
                    triage_record = triage_resp.json()
                except Exception as exc:
                    failure_reason = f"WRITEBACK_TRIAGE_FAILED:{str(exc)[:160]}"
                    continue

                if not bool(triage_record.get("auto_commit")):
                    failure_reason = f"TRIAGE_BLOCKED:{triage_record.get('reason')}"
                    continue

                try:
                    commit_json: dict[str, Any] = {
                        "target_path": target_path,
                        "base_sha256": base_sha256,
                        "patch_text": patch_text,
                        "idempotency_key": (
                            f"code-fix:{round_id}:{_safe_problem_token(result_copy.problem_id)}:{target_path}"
                        ),
                        "actor": {"type": "loop_controller", "id": "autofix"},
                        "request_id": (
                            f"req-{round_id}-{_safe_problem_token(result_copy.problem_id)}"
                            f"-{_safe_problem_token(target_path)}"
                        ),
                        "run_id": round_id,
                        "triage_record_id": triage_record.get("triage_record_id"),
                    }
                    if lease_id is not None and fencing_token is not None:
                        commit_json["lease_id"] = lease_id
                        commit_json["fencing_token"] = fencing_token
                    commit_resp = self._http.post(
                        f"{self._cfg.writeback_a_url}/v1/commit",
                        json=commit_json,
                        headers=self._auth_headers(self._cfg.writeback_a_token),
                        timeout=120.0,
                    )
                    commit_resp.raise_for_status()
                    commit_payload = commit_resp.json()
                except Exception as exc:
                    failure_reason = f"WRITEBACK_COMMIT_FAILED:{str(exc)[:160]}"
                    continue

                if str(commit_payload.get("status") or "").lower() == "committed" or bool(
                    commit_payload.get("idempotent_replay")
                ):
                    committed = True
                    committed_targets.add(target_path)
                    applied_commits.append(
                        {
                            "problem_id": result_copy.problem_id,
                            "target_path": target_path,
                            "commit_id": commit_payload.get("commit_id"),
                        }
                    )

            if committed:
                result_copy.outcome = FixOutcome.SUCCESS
                result_copy.error = None
            else:
                if failure_reason and failure_reason.startswith(("TRIAGE_BLOCKED", "DUPLICATE_TARGET")):
                    result_copy.outcome = FixOutcome.SKIPPED
                else:
                    result_copy.outcome = FixOutcome.FAILED
                result_copy.error = failure_reason or "NO_PATCH_COMMITTED"
            updated_results.append(result_copy)

        return updated_results, applied_commits

    def _rollback_fix_commits(self, applied_commits: list[dict[str, Any]], *, round_id: str) -> None:
        for item in reversed(applied_commits):
            commit_id = str(item.get("commit_id") or "").strip()
            if not commit_id:
                continue
            try:
                self._http.post(
                    f"{self._cfg.writeback_a_url}/v1/rollback",
                    json={
                        "commit_id": commit_id,
                        "idempotency_key": f"rollback:{round_id}:{commit_id}",
                        "actor": {"type": "loop_controller", "id": "autofix"},
                        "request_id": f"rollback-{round_id}-{commit_id}",
                        "run_id": round_id,
                    },
                    headers=self._auth_headers(self._cfg.writeback_a_token),
                    timeout=120.0,
                )
            except Exception as exc:
                logger.warning("writeback-A rollback failed for %s: %s", commit_id, exc)

    def _commit_promote_patch(
        self,
        prepare_payload: dict[str, Any],
        *,
        runtime_context: dict[str, Any],
        lease_id: str | None = None,
        fencing_token: int | None = None,
    ) -> dict[str, Any]:
        if bool(prepare_payload.get("skip_commit")):
            return {
                "status": "skipped",
                "skip_reason": prepare_payload.get("skip_reason"),
            }

        triage_resp = self._http.post(
            f"{self._cfg.promote_prep_url}/v1/triage",
            json={
                "run_id": prepare_payload["run_id"],
                "layer": prepare_payload["layer"],
                "target_path": prepare_payload["target_path"],
                "target_anchor": prepare_payload["target_anchor"],
                "patch_text": prepare_payload["patch_text"],
                "base_sha256": prepare_payload["base_sha256"],
                "runtime_gates": runtime_context.get("runtime_gates") or {},
                "audit_context": runtime_context,
                "semantic_fingerprint": prepare_payload.get("semantic_fingerprint"),
            },
            headers=self._auth_headers(self._cfg.promote_prep_token),
            timeout=120.0,
        )
        triage_resp.raise_for_status()
        triage_payload = triage_resp.json()
        if not bool(triage_payload.get("auto_commit")):
            return {
                "status": "skipped",
                "skip_reason": triage_payload.get("reason"),
                "triage": triage_payload,
            }

        # Retry logic for writeback-B preview — handles transient 403 from
        # stale promote_target_mode or short-lived service restarts.
        max_preview_attempts = 3
        preview_payload: dict[str, Any] = {}
        last_preview_error: str = ""
        for attempt in range(max_preview_attempts):
            try:
                preview_resp = self._http.post(
                    f"{self._cfg.writeback_b_url}/v1/preview",
                    json={
                        "target_path": prepare_payload["target_path"],
                        "base_sha256": prepare_payload["base_sha256"],
                        "patch_text": prepare_payload["patch_text"],
                    },
                    headers=self._auth_headers(self._cfg.writeback_b_token),
                    timeout=120.0,
                )
                if preview_resp.status_code == 403:
                    detail = ""
                    try:
                        detail = preview_resp.json().get("detail", "")
                    except Exception:
                        detail = preview_resp.text[:200]
                    last_preview_error = (
                        f"WRITEBACK_B_PREVIEW_403:{detail}"
                        f" (target={prepare_payload['target_path']})"
                    )
                    logger.warning(
                        "writeback-B preview 403 (attempt %d/%d): %s",
                        attempt + 1, max_preview_attempts, last_preview_error,
                    )
                    if attempt < max_preview_attempts - 1:
                        time.sleep(2 * (attempt + 1))
                        continue
                    return {
                        "status": "failed",
                        "error": last_preview_error,
                    }
                preview_resp.raise_for_status()
                preview_payload = preview_resp.json()
                break
            except httpx.HTTPStatusError:
                raise
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                last_preview_error = f"WRITEBACK_B_PREVIEW_TRANSIENT:{exc}"
                logger.warning(
                    "writeback-B preview transient error (attempt %d/%d): %s",
                    attempt + 1, max_preview_attempts, exc,
                )
                if attempt < max_preview_attempts - 1:
                    time.sleep(2 * (attempt + 1))
                    continue
                raise

        if bool(preview_payload.get("conflict")):
            return {
                "status": "skipped",
                "skip_reason": "PREVIEW_CONFLICT",
                "preview": preview_payload,
            }

        commit_json: dict[str, Any] = {
            "target_path": prepare_payload["target_path"],
            "base_sha256": prepare_payload["base_sha256"],
            "patch_text": prepare_payload["patch_text"],
            "idempotency_key": prepare_payload["idempotency_key"],
            "actor": {"type": "loop_controller", "id": "autopromote"},
            "request_id": prepare_payload["request_id"],
            "run_id": prepare_payload["run_id"],
            "triage_record_id": triage_payload.get("triage_record_id"),
        }
        # Propagate lease credentials for promote commits
        if lease_id is not None and fencing_token is not None:
            commit_json["lease_id"] = lease_id
            commit_json["fencing_token"] = fencing_token

        commit_resp = self._http.post(
            f"{self._cfg.writeback_b_url}/v1/commit",
            json=commit_json,
            headers=self._auth_headers(self._cfg.writeback_b_token),
            timeout=120.0,
        )
        commit_resp.raise_for_status()
        return commit_resp.json()

    def _do_promote(
        self,
        round_id: str,
        state: LoopState,
        runtime_context: dict[str, Any],
        *,
        lease_id: str | None = None,
        fencing_token: int | None = None,
    ) -> dict[str, Any]:
        """Trigger status-note/current-layer promote via promote_prep and writeback B."""
        audit_run_id = state.last_audit_run_id or round_id
        payload = {
            "run_id": audit_run_id,
            "runtime_gates": runtime_context.get("runtime_gates") or {},
            "audit_context": {
                **runtime_context,
                "round_id": round_id,
                "source": "loop_controller",
            },
        }
        promote_context = {
            **runtime_context,
            "round_id": round_id,
            "source": "loop_controller",
        }
        result: dict[str, Any] = {
            "status_note": {"status": "skipped", "skip_reason": "NOT_ATTEMPTED"},
            "current_layer": {"status": "skipped", "skip_reason": "NOT_ATTEMPTED"},
        }

        try:
            status_note_resp = self._http.post(
                f"{self._cfg.promote_prep_url}/v1/promote/status-note",
                json=payload,
                headers=self._auth_headers(self._cfg.promote_prep_token),
                timeout=120.0,
            )
            status_note_resp.raise_for_status()
            result["status_note"] = self._commit_promote_patch(
                status_note_resp.json(),
                runtime_context=promote_context,
                lease_id=lease_id,
                fencing_token=fencing_token,
            )
        except Exception as exc:
            result["status_note"] = {"status": "failed", "error": f"STATUS_NOTE_PROMOTE_FAILED:{exc}"}
            return result

        status_note_status = str(result["status_note"].get("status") or "").lower()
        if status_note_status == "failed":
            # _commit_promote_patch returned a hard failure (e.g. exhausted
            # 403 retries) — bail before attempting current_layer.
            return result
        status_note_skip_reason = str(result["status_note"].get("skip_reason") or "").strip()
        if (
            status_note_status == "skipped"
            and status_note_skip_reason
            and status_note_skip_reason not in {"RUN_ID_ALREADY_PRESENT", "SEMANTIC_FINGERPRINT_ALREADY_PRESENT"}
        ):
            result["status_note"] = {
                "status": "failed",
                "error": f"STATUS_NOTE_COMMIT_SKIPPED:{status_note_skip_reason}",
            }
            return result

        if not bool((runtime_context.get("runtime_gates") or {}).get("shared_artifact_promote", {}).get("allowed")):
            result["current_layer"] = {"status": "skipped", "skip_reason": "CURRENT_LAYER_RUNTIME_GATE_BLOCKED"}
            return result

        try:
            current_layer_resp = self._http.post(
                f"{self._cfg.promote_prep_url}/v1/promote/current-layer",
                json={**payload, "enabled": True},
                headers=self._auth_headers(self._cfg.promote_prep_token),
                timeout=120.0,
            )
            current_layer_resp.raise_for_status()
            result["current_layer"] = self._commit_promote_patch(
                current_layer_resp.json(),
                runtime_context=promote_context,
                lease_id=lease_id,
                fencing_token=fencing_token,
            )
        except Exception as exc:
            result["current_layer"] = {"status": "failed", "error": f"CURRENT_LAYER_PROMOTE_FAILED:{exc}"}
            # --- Promote atomicity: rollback status-note if current-layer fails ---
            sn_commit_id = result["status_note"].get("commit_id")
            if sn_commit_id and str(result["status_note"].get("status") or "").lower() == "committed":
                try:
                    self._http.post(
                        f"{self._cfg.writeback_b_url}/v1/rollback",
                        json={
                            "commit_id": sn_commit_id,
                            "idempotency_key": f"promote-atomicity-rb-{sn_commit_id}",
                            "actor": {"type": "loop_controller", "id": "promote_atomicity"},
                            "request_id": f"promote-atomicity-rb-{round_id}",
                            "run_id": audit_run_id,
                            **({"lease_id": lease_id, "fencing_token": fencing_token}
                               if lease_id is not None and fencing_token is not None else {}),
                        },
                        headers=self._auth_headers(self._cfg.writeback_b_token),
                        timeout=120.0,
                    )
                    result["status_note"]["rolled_back_for_atomicity"] = True
                    logger.info("rolled back status-note %s for promote atomicity", sn_commit_id)
                except Exception as rb_exc:
                    logger.warning("failed to rollback status-note %s for atomicity: %s", sn_commit_id, rb_exc)
                    result["status_note"]["atomicity_rollback_failed"] = str(rb_exc)
            return result

        current_layer_skip_reason = str(result["current_layer"].get("skip_reason") or "").strip()
        if (
            str(result["current_layer"].get("status") or "").lower() == "skipped"
            and current_layer_skip_reason
            and current_layer_skip_reason
            not in {"RUN_ID_ALREADY_PRESENT", "CURRENT_LAYER_SEMANTIC_FINGERPRINT_ALREADY_PRESENT"}
        ):
            result["current_layer"] = {
                "status": "failed",
                "error": f"CURRENT_LAYER_COMMIT_SKIPPED:{current_layer_skip_reason}",
            }

        return result

    # -- public analysis endpoint (called by Kestra) -------------------------

    def analyze_from_audit(
        self,
        audit_run_id: str,
        bundle: dict[str, Any],
    ) -> tuple[list[ProblemSpec], int, int, int]:
        state = self._store.load()
        state.last_audit_run_id = audit_run_id
        problems, new_c, reg_c, skip_c = analyze_bundle(bundle, state.fixed_problems)
        actionable = [p for p in problems if not p.is_external_blocked]
        state.problems_queue = actionable
        self._save_state(state)
        return problems, new_c, reg_c, skip_c

    def verify_round(
        self,
        round_id: str,
        fix_results: list[FixResult],
        affected_test_paths: list[str] | None = None,
    ) -> VerifyResult:
        affected = affected_test_paths or self._collect_affected_tests(fix_results)
        return self._verifier.run_full_pipeline(affected, round_id)
