"""FastAPI application for the Loop Controller service (port 8096)."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request

from automation.loop_controller.controller import LoopController, LoopControllerConfig
from automation.loop_controller.schemas import (
    AnalyzeRequest,
    AnalyzeResponse,
    AwaitRoundResponse,
    FixResult,
    LoopMode,
    RoundCompleteRequest,
    StartLoopRequest,
    StartLoopResponse,
    StateResponse,
    StopLoopRequest,
    VerifyRequest,
    VerifyResponse,
)


def _require_auth(token: str, request: Request) -> None:
    if not token:
        return
    if (request.headers.get("Authorization") or "").strip() != f"Bearer {token}":
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")


def create_app(
    config: LoopControllerConfig | None = None,
    controller: LoopController | None = None,
) -> FastAPI:
    cfg = config or LoopControllerConfig.from_env()
    ctrl = controller or LoopController(cfg)

    async def _startup_token_probe() -> None:
        """Validate internal token against app at startup to fail-fast on drift."""
        import httpx
        import logging

        _log = logging.getLogger("loop_controller.startup")
        app_url = str(cfg.app_base_url or "").rstrip("/")
        token = str(cfg.internal_token or "").strip()
        if not app_url or not token:
            _log.warning("STARTUP_TOKEN_PROBE_SKIPPED: app_base_url or internal_token missing")
            return
        probe_url = f"{app_url}/api/v1/internal/runtime/gates"
        try:
            async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
                resp = await client.get(probe_url, headers={"X-Internal-Token": token})
            if resp.status_code == 401:
                _log.error(
                    "STARTUP_TOKEN_PROBE_FAILED: %s returned 401 — internal token may be drifted",
                    probe_url,
                )
            elif resp.status_code != 200:
                _log.warning(
                    "STARTUP_TOKEN_PROBE_DEGRADED: %s returned %d",
                    probe_url,
                    resp.status_code,
                )
            else:
                _log.info("STARTUP_TOKEN_PROBE_OK: internal token validated against app")
        except Exception as exc:
            _log.warning("STARTUP_TOKEN_PROBE_UNREACHABLE: %s — %s", probe_url, exc)

    @asynccontextmanager
    async def _lifespan(_: FastAPI):
        await _startup_token_probe()
        yield

    app = FastAPI(title="Loop Controller", version="0.1.0", lifespan=_lifespan)

    def _goal_progress_count(state) -> int:
        metric = str(state.success_goal_metric or "verified_problem_count").strip().lower()
        if metric == "fix_success_count":
            return state.consecutive_fix_success_count
        return state.consecutive_verified_problem_fixes

    def _goal_reached(state) -> bool:
        return bool(
            state.fix_goal
            and _goal_progress_count(state) >= state.fix_goal
            and state.last_promote_round_id
        )

    # -- health --------------------------------------------------------------

    @app.get("/health")
    def health() -> dict[str, object]:
        state = ctrl.get_state()
        goal_progress_count = _goal_progress_count(state)
        return {
            "status": "ok",
            "running": ctrl.running,
            "mode": state.mode.value,
            "phase": state.phase.value,
            "consecutive_fix_success_count": state.consecutive_fix_success_count,
            "consecutive_verified_problem_fixes": state.consecutive_verified_problem_fixes,
            "fix_goal": state.fix_goal,
            "success_goal_metric": state.success_goal_metric,
            "goal_progress_count": goal_progress_count,
            "goal_reached": _goal_reached(state),
        }

    # -- state ---------------------------------------------------------------

    @app.get("/v1/state", response_model=StateResponse)
    def get_state(request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        state = ctrl.get_state()
        goal_progress_count = _goal_progress_count(state)
        return {
            "mode": state.mode,
            "phase": state.phase,
            "consecutive_fix_success_count": state.consecutive_fix_success_count,
            "consecutive_verified_problem_fixes": state.consecutive_verified_problem_fixes,
            "fix_goal": state.fix_goal,
            "goal_progress_count": goal_progress_count,
            "goal_reached": _goal_reached(state),
            "total_fixes": state.total_fixes,
            "total_failures": state.total_failures,
            "current_round_id": state.current_round_id,
            "last_promote_round_id": state.last_promote_round_id,
            "last_promote_at": state.last_promote_at,
            "problems_queue_size": len(state.problems_queue),
            "fixed_count": len(state.fixed_problems),
            "round_history_size": len(state.round_history),
            "running": ctrl.running,
            "blocked_reason": state.blocked_reason,
            "provider_pool": state.provider_pool,
            "promote_target_mode": state.promote_target_mode,
            "success_goal_metric": state.success_goal_metric,
        }

    # -- start / stop --------------------------------------------------------

    @app.post("/v1/start", response_model=StartLoopResponse)
    def start_loop(payload: StartLoopRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        if ctrl.running:
            state = ctrl.get_state()
            if payload.force_new_round:
                ctrl.force_new_round(mode=payload.mode, fix_goal=payload.fix_goal)
                return {
                    "status": "started",
                    "mode": state.mode.value,
                    "fix_goal": state.fix_goal,
                    "current_round_id": state.current_round_id,
                }
            raise HTTPException(status_code=409, detail="ALREADY_RUNNING")
        state = ctrl.start(mode=payload.mode, fix_goal=payload.fix_goal)
        return {
            "status": "started",
            "mode": state.mode.value,
            "fix_goal": state.fix_goal,
            "current_round_id": state.current_round_id,
        }

    @app.post("/v1/stop")
    def stop_loop(payload: StopLoopRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        state = ctrl.stop(reason=payload.reason)
        return {"status": "stopped", "reason": payload.reason, "phase": state.phase.value}

    # -- analysis (callable by Kestra) ---------------------------------------

    @app.post("/v1/analyze", response_model=AnalyzeResponse)
    def analyze(payload: AnalyzeRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        problems, new_c, reg_c, skip_c = ctrl.analyze_from_audit(
            audit_run_id=payload.audit_run_id,
            bundle=payload.bundle,
        )
        return {
            "problems": [p.model_dump() for p in problems],
            "new_count": new_c,
            "regression_count": reg_c,
            "skipped_count": skip_c,
        }

    # -- verify (callable by Kestra) -----------------------------------------

    @app.post("/v1/verify", response_model=VerifyResponse)
    def verify(payload: VerifyRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        results = [FixResult.model_validate(r) if isinstance(r, dict) else r for r in payload.fix_results]
        vr = ctrl.verify_round(
            round_id=payload.round_id,
            fix_results=results,
            affected_test_paths=payload.affected_test_paths or None,
        )
        return {"result": vr.model_dump(), "should_rollback": not vr.all_green}

    # -- round complete (callable by Kestra) ----------------------------------

    @app.post("/v1/round-complete")
    def round_complete(payload: RoundCompleteRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        ctrl.record_round_complete(
            round_id=payload.round_id,
            execution_id=payload.execution_id,
            status=payload.status,
            error=payload.error,
        )
        return {
            "status": "recorded",
            "round_id": payload.round_id,
            "execution_id": payload.execution_id,
        }

    # -- await round (long-poll for Kestra) ----------------------------------

    @app.get("/v1/await-round", response_model=AwaitRoundResponse)
    def await_round(request: Request, timeout: int = 600) -> dict[str, object]:
        """Long-poll until the current round finishes or timeout (seconds)."""
        _require_auth(cfg.auth_token, request)
        timeout = min(max(timeout, 10), 1800)  # clamp 10s–30min
        result = ctrl.await_round(timeout_seconds=timeout)
        return result

    # -- round history -------------------------------------------------------

    @app.get("/v1/rounds")
    def list_rounds(request: Request, last: int = 20) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        state = ctrl.get_state()
        rounds = state.round_history[-last:] if state.round_history else []
        return {"rounds": [r.model_dump() for r in rounds], "total": len(state.round_history)}

    return app


app = create_app()
