from __future__ import annotations

import contextlib
import json
import os
import threading
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Iterator, Protocol
from uuid import uuid4

from automation.loop_controller.controller import LoopController, LoopControllerConfig
from automation.loop_controller.schemas import LoopMode, LoopState

from app.core.config import settings
from app.services.autonomous_fix_loop import AutonomousFixLoopConfig
from app.services.codex_client import discover_audit_codex_provider_specs

_LEASE_LOCK_TIMEOUT_SECONDS = 2.0
_RUNTIME_SINGLETON: AutonomyLoopRuntime | None = None
_RUNTIME_SINGLETON_LOCK = threading.Lock()
_VALID_SUCCESS_GOAL_METRICS = frozenset({"verified_problem_count", "fix_success_count"})
_VALID_PROMOTE_TARGET_MODES = frozenset({"infra", "doc22"})


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8", newline="") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
            temp_path = Path(handle.name)
        os.replace(temp_path, path)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink(missing_ok=True)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _base_loop_controller_config() -> LoopControllerConfig:
    return LoopControllerConfig.from_env()


def _persisted_loop_state_path(config: LoopControllerConfig) -> Path:
    return config.repo_root / "runtime" / "loop_controller" / "state.json"


def _control_plane_state_path(config: LoopControllerConfig) -> Path:
    return config.repo_root / "automation" / "control_plane" / "current_state.json"


def _control_plane_promote_target_mode(config: LoopControllerConfig) -> str:
    payload = _read_json(_control_plane_state_path(config))
    mode = str(payload.get("promote_target_mode") or "").strip().lower()
    if mode in _VALID_PROMOTE_TARGET_MODES:
        return mode
    return "infra"


def _load_persisted_loop_state(config: LoopControllerConfig) -> LoopState | None:
    payload = _read_json(_persisted_loop_state_path(config))
    if not payload:
        return None
    try:
        return LoopState.model_validate(payload)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning(
            "Failed to parse persisted loop state at %s: %s",
            _persisted_loop_state_path(config),
            exc,
        )
        return None


def _coerce_loop_mode(value: str | LoopMode | None) -> LoopMode:
    if isinstance(value, LoopMode):
        return value
    candidate = str(value or settings.autonomy_loop_mode or LoopMode.FIX.value).strip().lower()
    try:
        return LoopMode(candidate)
    except ValueError:
        return LoopMode.FIX


def _normalize_success_goal_metric(value: str | None) -> str:
    candidate = str(value or "").strip().lower()
    if candidate in _VALID_SUCCESS_GOAL_METRICS:
        return candidate
    return "verified_problem_count"


def _normalize_new_api_base_url(value: str | None) -> str:
    candidate = str(value or "").strip().rstrip("/")
    if candidate.lower().endswith("/v1"):
        return candidate[:-3].rstrip("/")
    return candidate


def _discover_new_api_gateway_credentials() -> tuple[str, str]:
    try:
        providers = discover_audit_codex_provider_specs()
    except Exception:
        return "", ""
    if not providers:
        return "", ""
    provider = providers[0]
    return (
        _normalize_new_api_base_url(provider.base_url),
        str(provider.api_key or "").strip(),
    )


def _goal_progress_count(state: LoopState | None) -> int:
    if state is None:
        return 0
    metric = _normalize_success_goal_metric(state.success_goal_metric)
    if metric == "fix_success_count":
        return state.consecutive_fix_success_count
    return state.consecutive_verified_problem_fixes


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except ValueError:
        return int(default)


def _issue_mesh_worker_cap() -> int:
    max_workers_cap = max(1, _env_int("ISSUE_MESH_MAX_WORKERS_CAP", 12))
    readonly_workers = max(1, _env_int("ISSUE_MESH_READONLY_MAX_WORKERS", max_workers_cap))
    return max(1, min(max_workers_cap, readonly_workers))


def _shadow_autonomous_fix_state_path(config: LoopControllerConfig) -> Path:
    return config.repo_root / "runtime" / "autonomous_fix_loop" / "state.json"


def _formal_last_round_projection(state: LoopState | None) -> dict[str, Any] | None:
    if state is None or not state.round_history:
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


def _shadow_loop_diagnostics(config: LoopControllerConfig | None) -> dict[str, Any]:
    if config is None:
        return {
            "last_round_failed_reasons": [],
            "last_promote_reason": None,
            "last_runtime_gate_status": None,
            "last_round_note": None,
            "shadow_round_history_size": 0,
        }
    payload = _read_json(_shadow_autonomous_fix_state_path(config))
    round_history = payload.get("round_history") if isinstance(payload.get("round_history"), list) else []
    latest = round_history[-1] if round_history and isinstance(round_history[-1], dict) else {}
    failed_reasons = latest.get("failed_reasons") if isinstance(latest.get("failed_reasons"), list) else []
    return {
        "last_round_failed_reasons": [str(item) for item in failed_reasons if str(item or "").strip()],
        "last_promote_reason": str(latest.get("promote_reason") or "").strip() or None,
        "last_runtime_gate_status": str(latest.get("runtime_gate_status") or "").strip() or None,
        "last_round_note": str(latest.get("note") or "").strip() or None,
        "shadow_round_history_size": len(round_history),
    }


def _worker_projection() -> dict[str, int]:
    cfg = AutonomousFixLoopConfig.from_env()
    return {
        "issue_mesh_worker_cap": _issue_mesh_worker_cap(),
        "effective_fix_workers": int(cfg.max_workers_fix),
        "effective_monitor_workers": int(cfg.max_workers_monitor),
    }


def build_autonomy_loop_controller_config() -> LoopControllerConfig:
    base = _base_loop_controller_config()
    control_plane_token = str(base.internal_token or "").strip()
    internal_token = str(
        settings.internal_cron_token
        or control_plane_token
        or settings.internal_api_key
    ).strip()
    new_api_base_url = _normalize_new_api_base_url(
        base.new_api_base_url or settings.codex_api_base_url
    )
    new_api_token = str(base.new_api_token or settings.codex_api_key).strip()
    if not (new_api_base_url and new_api_token):
        discovered_base_url, discovered_token = _discover_new_api_gateway_credentials()
        new_api_base_url = new_api_base_url or discovered_base_url
        new_api_token = new_api_token or discovered_token
    return replace(
        base,
        fix_goal=max(1, int(settings.autonomy_loop_fix_goal)),
        audit_interval_seconds=max(30, int(settings.autonomy_loop_audit_interval_seconds)),
        monitor_interval_seconds=max(60, int(settings.autonomy_loop_monitor_interval_seconds)),
        internal_token=internal_token,
        new_api_base_url=new_api_base_url,
        new_api_token=new_api_token,
    )


class LoopControllerLike(Protocol):
    running: bool

    def start(self, mode: LoopMode = LoopMode.FIX, fix_goal: int | None = None) -> LoopState:
        ...

    def stop(self, reason: str = "manual") -> LoopState:
        ...

    def get_state(self) -> LoopState:
        ...

    def force_new_round(self, mode: LoopMode = LoopMode.FIX, fix_goal: int | None = None) -> None:
        ...


class AutonomyLoopRuntime:
    def __init__(
        self,
        *,
        controller_factory: Callable[[LoopControllerConfig], LoopControllerLike] | None = None,
    ) -> None:
        self._controller_factory = controller_factory or (lambda cfg: LoopController(cfg))
        self._controller: LoopControllerLike | None = None
        self._controller_lock = threading.Lock()
        self._owner_id = f"autonomy-loop-{uuid4()}"
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None

    def _effective_heartbeat_seconds(self) -> int:
        return max(1, int(settings.autonomy_loop_heartbeat_seconds))

    def _effective_lease_seconds(self) -> int:
        heartbeat = self._effective_heartbeat_seconds()
        return max(int(settings.autonomy_loop_lease_seconds), heartbeat * 3)

    def _lease_path(self, config: LoopControllerConfig) -> Path:
        return config.repo_root / "runtime" / "loop_controller" / "runtime_lease.json"

    def _lease_lock_path(self, config: LoopControllerConfig) -> Path:
        return config.repo_root / "runtime" / "loop_controller" / ".runtime_lease.lock"

    @contextlib.contextmanager
    def _lease_file_lock(self, config: LoopControllerConfig) -> Iterator[None]:
        lock_path = self._lease_lock_path(config)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        fd: int | None = None
        while True:
            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.write(fd, f"{os.getpid()}:{_now_utc().isoformat()}".encode("utf-8"))
                break
            except FileExistsError:
                if (time.monotonic() - started) >= _LEASE_LOCK_TIMEOUT_SECONDS:
                    raise RuntimeError("AUTONOMY_LOOP_LEASE_LOCKED")
                time.sleep(0.05)
        try:
            yield
        finally:
            if fd is not None:
                with contextlib.suppress(OSError):
                    os.close(fd)
            with contextlib.suppress(FileNotFoundError):
                lock_path.unlink()

    def _lease_snapshot(self, config: LoopControllerConfig) -> dict[str, Any]:
        return _read_json(self._lease_path(config))

    def _lease_is_stale(self, snapshot: dict[str, Any]) -> bool:
        heartbeat = _parse_iso_datetime(snapshot.get("heartbeat_at"))
        if heartbeat is None:
            return True
        age_seconds = (_now_utc() - heartbeat).total_seconds()
        return age_seconds >= self._effective_lease_seconds()

    def _loop_state_payload(self, state: LoopState | None, *, config: LoopControllerConfig | None = None) -> dict[str, Any]:
        shadow = _shadow_loop_diagnostics(config)
        worker_projection = _worker_projection()
        last_round_summary = _formal_last_round_projection(state)
        if state is None:
            promote_target_mode = _control_plane_promote_target_mode(config) if config is not None else "infra"
            return {
                "mode": None,
                "phase": None,
                "current_round_id": None,
                "consecutive_fix_success_count": 0,
                "consecutive_verified_problem_fixes": 0,
                "fix_goal": int(settings.autonomy_loop_fix_goal),
                "goal_progress_count": 0,
                "goal_reached": False,
                "goal_ever_reached": False,
                "success_goal_metric": "verified_problem_count",
                "total_fixes": 0,
                "total_failures": 0,
                "fixed_problems": [],
                "promote_target_mode": promote_target_mode,
                "blocked_reason": None,
                "provider_pool": {},
                "round_history_size": 0,
                "last_audit_run_id": None,
                "last_fix_wave_id": None,
                "last_promote_round_id": None,
                "last_promote_at": None,
                "last_round_summary": last_round_summary,
                "last_round_failed_reasons": shadow["last_round_failed_reasons"],
                "last_promote_reason": shadow["last_promote_reason"],
                "last_runtime_gate_status": shadow["last_runtime_gate_status"],
                "last_round_note": shadow["last_round_note"],
                "shadow_round_history_size": shadow["shadow_round_history_size"],
                "issue_mesh_worker_cap": worker_projection["issue_mesh_worker_cap"],
                "effective_fix_workers": worker_projection["effective_fix_workers"],
                "effective_monitor_workers": worker_projection["effective_monitor_workers"],
            }
        goal_progress_count = _goal_progress_count(state)
        return {
            "mode": state.mode.value,
            "phase": state.phase.value,
            "current_round_id": state.current_round_id,
            "consecutive_fix_success_count": state.consecutive_fix_success_count,
            "consecutive_verified_problem_fixes": state.consecutive_verified_problem_fixes,
            "fix_goal": state.fix_goal,
            "goal_progress_count": goal_progress_count,
            "goal_reached": bool(state.fix_goal and goal_progress_count >= state.fix_goal and state.last_promote_round_id),
            "goal_ever_reached": state.goal_ever_reached,
            "success_goal_metric": _normalize_success_goal_metric(state.success_goal_metric),
            "total_fixes": state.total_fixes,
            "total_failures": state.total_failures,
            "fixed_problems": list(state.fixed_problems),
            "promote_target_mode": state.promote_target_mode,
            "blocked_reason": state.blocked_reason,
            "provider_pool": state.provider_pool,
            "round_history_size": len(state.round_history),
            "last_audit_run_id": state.last_audit_run_id,
            "last_fix_wave_id": state.last_fix_wave_id,
            "last_promote_round_id": state.last_promote_round_id,
            "last_promote_at": state.last_promote_at,
            "last_round_summary": last_round_summary,
            "last_round_failed_reasons": shadow["last_round_failed_reasons"],
            "last_promote_reason": shadow["last_promote_reason"],
            "last_runtime_gate_status": shadow["last_runtime_gate_status"],
            "last_round_note": shadow["last_round_note"],
            "shadow_round_history_size": shadow["shadow_round_history_size"],
            "issue_mesh_worker_cap": worker_projection["issue_mesh_worker_cap"],
            "effective_fix_workers": worker_projection["effective_fix_workers"],
            "effective_monitor_workers": worker_projection["effective_monitor_workers"],
        }

    def _write_lease_snapshot(
        self,
        config: LoopControllerConfig,
        *,
        state: LoopState | None,
        running: bool,
    ) -> dict[str, Any]:
        payload = {
            "owner_id": self._owner_id,
            "heartbeat_at": _now_utc().isoformat(),
            "lease_seconds": self._effective_lease_seconds(),
            "heartbeat_seconds": self._effective_heartbeat_seconds(),
            "running": running,
            **self._loop_state_payload(state, config=config),
        }
        _atomic_write_json(self._lease_path(config), payload)
        return payload

    def _ensure_controller(self, config: LoopControllerConfig) -> LoopControllerLike:
        with self._controller_lock:
            if self._controller is None:
                self._controller = self._controller_factory(config)
            return self._controller

    def _acquire_lease(self, config: LoopControllerConfig, *, state: LoopState | None) -> tuple[bool, dict[str, Any]]:
        with self._lease_file_lock(config):
            snapshot = self._lease_snapshot(config)
            owner_id = str(snapshot.get("owner_id") or "").strip()
            if owner_id and owner_id != self._owner_id and not self._lease_is_stale(snapshot):
                return False, snapshot
            return True, self._write_lease_snapshot(config, state=state, running=True)

    def _refresh_lease(self) -> None:
        config = build_autonomy_loop_controller_config()
        controller = self._controller
        state = controller.get_state() if controller is not None else None
        with self._lease_file_lock(config):
            snapshot = self._lease_snapshot(config)
            if str(snapshot.get("owner_id") or "").strip() not in {"", self._owner_id} and not self._lease_is_stale(snapshot):
                return
            self._write_lease_snapshot(config, state=state, running=bool(controller and controller.running))

    def _release_lease(self) -> None:
        config = build_autonomy_loop_controller_config()
        with self._lease_file_lock(config):
            snapshot = self._lease_snapshot(config)
            if str(snapshot.get("owner_id") or "").strip() != self._owner_id:
                return
            self._lease_path(config).unlink(missing_ok=True)

    def _heartbeat_loop(self) -> None:
        interval = self._effective_heartbeat_seconds()
        while not self._heartbeat_stop.wait(interval):
            controller = self._controller
            if controller is None or not controller.running:
                break
            try:
                self._refresh_lease()
            except Exception:
                continue

    def _start_heartbeat(self) -> None:
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            return
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name="autonomy-loop-heartbeat",
        )
        self._heartbeat_thread.start()

    def _stop_heartbeat(self) -> None:
        self._heartbeat_stop.set()
        thread = self._heartbeat_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)
        self._heartbeat_thread = None

    def status(self) -> dict[str, Any]:
        config = build_autonomy_loop_controller_config()
        controller = self._controller
        state = controller.get_state() if controller is not None else _load_persisted_loop_state(config)
        loop_payload = self._loop_state_payload(state, config=config)
        lease_snapshot = self._lease_snapshot(config)
        lease_owner = str(lease_snapshot.get("owner_id") or "").strip()
        lease_status = "absent"
        if lease_owner:
            if self._lease_is_stale(lease_snapshot):
                lease_status = "stale"
            elif lease_owner == self._owner_id:
                lease_status = "owned_by_self"
            else:
                lease_status = "owned_by_other"
        return {
            "enabled": bool(settings.autonomy_loop_enabled),
            "initialized": controller is not None,
            "running": bool(controller is not None and controller.running),
            "configured_mode": _coerce_loop_mode(settings.autonomy_loop_mode).value,
            "configured_fix_goal": int(settings.autonomy_loop_fix_goal),
            "configured_audit_interval_seconds": int(settings.autonomy_loop_audit_interval_seconds),
            "configured_monitor_interval_seconds": int(settings.autonomy_loop_monitor_interval_seconds),
            **loop_payload,
            "lease": {
                "path": str(self._lease_path(config)),
                "status": lease_status,
                "owner_id": lease_owner or None,
                "heartbeat_at": lease_snapshot.get("heartbeat_at"),
                "stale": self._lease_is_stale(lease_snapshot) if lease_owner else False,
                "ttl_seconds": self._effective_lease_seconds(),
                "owned_by_current_process": lease_owner == self._owner_id,
            },
        }

    def start(
        self,
        *,
        mode: str | LoopMode | None = None,
        fix_goal: int | None = None,
        force_new_round: bool = False,
        reason: str = "manual_start",
    ) -> dict[str, Any]:
        config = build_autonomy_loop_controller_config()
        controller = self._ensure_controller(config)
        loop_mode = _coerce_loop_mode(mode)
        desired_fix_goal = max(1, int(fix_goal or settings.autonomy_loop_fix_goal))
        if controller.running:
            if force_new_round:
                controller.force_new_round(mode=loop_mode, fix_goal=desired_fix_goal)
                payload = self.status()
                payload["status"] = "force_requested"
                payload["reason"] = reason
                return payload
            payload = self.status()
            payload["status"] = "already_running"
            payload["reason"] = reason
            return payload
        acquired, lease_snapshot = self._acquire_lease(config, state=controller.get_state())
        if not acquired:
            payload = self.status()
            payload["status"] = "lease_conflict"
            payload["reason"] = reason
            payload["lease_conflict"] = lease_snapshot
            return payload
        try:
            controller.start(mode=loop_mode, fix_goal=desired_fix_goal)
            self._refresh_lease()
            self._start_heartbeat()
        except Exception:
            self._stop_heartbeat()
            self._release_lease()
            raise
        payload = self.status()
        payload["status"] = "started"
        payload["reason"] = reason
        return payload

    def ensure_started(self, *, reason: str = "app_startup") -> dict[str, Any]:
        if not settings.autonomy_loop_enabled:
            payload = self.status()
            payload["status"] = "disabled"
            payload["reason"] = reason
            return payload
        return self.start(
            mode=settings.autonomy_loop_mode,
            fix_goal=int(settings.autonomy_loop_fix_goal),
            reason=reason,
        )

    def stop(self, *, reason: str = "manual_stop") -> dict[str, Any]:
        controller = self._controller
        if controller is None or not controller.running:
            self._stop_heartbeat()
            self._release_lease()
            payload = self.status()
            payload["status"] = "already_stopped"
            payload["reason"] = reason
            return payload
        controller.stop(reason=reason)
        self._stop_heartbeat()
        self._release_lease()
        payload = self.status()
        payload["status"] = "stopped"
        payload["reason"] = reason
        return payload

    def force_new_round(
        self,
        *,
        mode: str | LoopMode | None = None,
        fix_goal: int | None = None,
        reason: str = "force_round_api",
    ) -> dict[str, Any]:
        """Signal the running loop controller to start a new round immediately."""
        controller = self._controller
        if controller is None or not controller.running:
            return {**self.status(), "status": "not_running", "reason": reason}
        loop_mode = _coerce_loop_mode(mode)
        desired_fix_goal = max(1, int(fix_goal or settings.autonomy_loop_fix_goal))
        controller.force_new_round(mode=loop_mode, fix_goal=desired_fix_goal)
        payload = self.status()
        payload["status"] = "force_requested"
        payload["reason"] = reason
        return payload

    def await_round(self, timeout_seconds: int = 600) -> dict[str, Any]:
        """Block until the loop controller completes a round or timeout.

        Delegates to the underlying LoopController.await_round(). If no
        controller is running, returns immediately with status=not_running.
        """
        controller = self._controller
        if controller is None or not controller.running:
            return {"status": "not_running", "round_id": None, "timed_out": True}
        result = controller.await_round(timeout_seconds=timeout_seconds)
        # Normalise: LoopController returns plain dicts (may contain RoundSummary)
        round_summary = result.get("round_summary")
        return {
            "status": result.get("status", "unknown"),
            "round_id": result.get("round_id"),
            "timed_out": bool(result.get("timed_out")),
            "round_summary": round_summary.model_dump() if hasattr(round_summary, "model_dump") else round_summary,
        }

    def shutdown(self, *, reason: str = "app_shutdown") -> dict[str, Any]:
        return self.stop(reason=reason)


def get_autonomy_loop_runtime() -> AutonomyLoopRuntime:
    global _RUNTIME_SINGLETON
    with _RUNTIME_SINGLETON_LOCK:
        if _RUNTIME_SINGLETON is None:
            _RUNTIME_SINGLETON = AutonomyLoopRuntime()
        return _RUNTIME_SINGLETON
