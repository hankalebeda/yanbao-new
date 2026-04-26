"""HTTP Shell — FastAPI bridge for the Escort Team agent engine.

Exposes the same 9 HTTP endpoints as ``loop_controller/app.py`` so that
Kestra, external orchestrators, and monitoring tools can drive the team
without any API changes.

Usage::

    uvicorn automation.agents.http_shell:app --port 8097

Architecture (inspired by claude-code-sourcemap DirectConnect)::

    HTTP request → FastAPI endpoint
        → EscortTeam (asyncio engine)
            → Coordinator → Workers → …
        ← response

All long-running work happens inside asyncio tasks; HTTP handlers
bridge the gap with ``asyncio.Event`` for round-await and
``asyncio.create_task`` for background start.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from automation.loop_controller.schemas import (
    AnalyzeRequest,
    AwaitRoundResponse,
    RoundCompleteRequest,
    StartLoopRequest,
    StartLoopResponse,
    StateResponse,
    StopLoopRequest,
    VerifyRequest,
    VerifyResponse,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Auth helper  (constant-time comparison to prevent timing side-channel)
# ---------------------------------------------------------------------------

def _require_auth(token: str, request: Request) -> None:
    if not token:
        return
    provided = (request.headers.get("Authorization") or "").strip()
    expected = f"Bearer {token}"
    if not hmac.compare_digest(provided.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_app(
    *,
    auth_token: str | None = None,
    repo_root: Path | None = None,
) -> FastAPI:
    """Create the HTTP Shell FastAPI application.

    Parameters
    ----------
    auth_token : str, optional
        Bearer token for authenticated endpoints.  Falls back to
        ``ESCORT_TEAM_TOKEN`` → ``INTERNAL_TOKEN`` env vars.
    repo_root : Path, optional
        Repository root.  Falls back to ``LOOP_CONTROLLER_REPO_ROOT``
        env var or current directory.
    """
    from automation.agents import EscortTeam, Mailbox, create_team

    token = auth_token or os.environ.get("ESCORT_TEAM_TOKEN") or os.environ.get("INTERNAL_TOKEN", "")
    root = repo_root or Path(os.environ.get("LOOP_CONTROLLER_REPO_ROOT", ".")).resolve()

    app = FastAPI(title="Escort Team HTTP Shell", version="0.2.0")

    # Shared mutable state -------------------------------------------------
    _team: dict[str, EscortTeam | None] = {"instance": None}
    _run_task: dict[str, asyncio.Task | None] = {"task": None}
    _round_event = asyncio.Event()

    def _get_team() -> EscortTeam:
        t = _team["instance"]
        if t is None:
            raise HTTPException(status_code=409, detail="TEAM_NOT_STARTED")
        return t

    def _read_promote_target_mode() -> str:
        state_path = root / "automation" / "control_plane" / "current_state.json"
        if not state_path.exists():
            return "infra"
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            return "infra"
        if not isinstance(payload, dict):
            return "infra"
        mode = str(payload.get("promote_target_mode") or "").strip().lower()
        if mode in {"infra", "doc22"}:
            return mode
        return "infra"

    def _formal_promote_ready(formal: Any) -> bool:
        if not isinstance(formal, dict):
            return False
        if not bool(formal.get("approved")):
            return False
        return any(
            bool(formal.get(key))
            for key in ("status_note_committed", "current_layer_committed", "doc22_committed")
        )

    def _runtime_projection(status: dict[str, Any]) -> dict[str, Any]:
        coord_status = status.get("coordinator", {})
        projection = coord_status.get("runtime_projection")
        if isinstance(projection, dict) and projection:
            return dict(projection)

        coord = coord_status.get("coordinator", coord_status)
        completion_blockers = coord_status.get("completion_blockers", [])
        completion_evidence = coord_status.get("completion_evidence") or {}
        formal_promote = completion_evidence.get("formal_promote", {})
        goal_reached = (
            not completion_blockers
            and not coord.get("deferred_problems", [])
            and coord.get("mode") == "completed"
            and _formal_promote_ready(formal_promote)
        )
        return {
            "mode": coord.get("mode", "fix"),
            "phase": coord.get("phase", "bootstrap"),
            "consecutive_fix_success_count": coord.get("consecutive_green_rounds", 0),
            "consecutive_verified_problem_fixes": coord.get("consecutive_green_rounds", 0),
            "consecutive_green_rounds": coord.get("consecutive_green_rounds", 0),
            "fix_goal": 10,
            "goal_progress_count": coord.get("consecutive_green_rounds", 0),
            "goal_reached": goal_reached,
            "total_fixes": coord.get("total_fixes", 0),
            "total_failures": coord.get("total_failures", 0),
            "current_round_id": coord.get("current_round_id"),
            "last_promote_round_id": coord_status.get("last_promote_round_id"),
            "last_promote_at": coord.get("last_promote_time"),
            "problems_queue_size": 0,
            "fixed_count": coord.get("total_fixes", 0),
            "round_history_size": len(coord.get("round_history", [])),
            "blocked_reason": "; ".join(
                f"{item.get('lane_id')}:{item.get('status')}" for item in completion_blockers
            ) or None,
            "provider_pool": coord_status.get("provider_pool", {"ready": False, "status": "unknown"}),
            "promote_target_mode": _read_promote_target_mode(),
            "success_goal_metric": "verified_problem_count",
            "execution_lanes": coord.get("execution_lanes", {}),
            "blocked_problems": coord.get("blocked_problems", []),
            "completion_blockers": completion_blockers,
            "completion_time": coord.get("completion_time", ""),
            "last_audit_run_id": coord_status.get("last_audit_run_id"),
            "shadow_validation": completion_evidence.get("shadow_validation", {}),
            "formal_promote": formal_promote,
            "completion_evidence": completion_evidence,
            "autonomy_index": coord_status.get("autonomy_index", 0.0),
        }

    # -- health (no auth) --------------------------------------------------

    @app.get("/health")
    def health() -> dict[str, Any]:
        t = _team["instance"]
        if t is None:
            return {"status": "idle", "running": False}
        status = t.get_status()
        coord_status = status.get("coordinator", {})
        coord = coord_status.get("coordinator", coord_status)
        return {
            "status": "ok",
            "running": True,
            "mode": coord.get("mode", "unknown"),
            "phase": coord.get("phase", "unknown"),
            "agents_healthy": len(coord_status.get("agents_healthy", [])),
            "agents_registered": len(coord_status.get("agents_registered", [])),
        }

    # -- state -------------------------------------------------------------

    @app.get("/v1/state")
    def get_state(request: Request) -> dict[str, Any]:
        _require_auth(token, request)
        t = _get_team()
        status = t.get_status()
        projection = _runtime_projection(status)
        projection["running"] = True
        return projection

    # -- start / stop ------------------------------------------------------

    @app.post("/v1/start")
    async def start_team(payload: StartLoopRequest, request: Request) -> dict[str, Any]:
        _require_auth(token, request)

        if _team["instance"] is not None:
            if payload.force_new_round:
                # Signal the coordinator to force a new round
                return {"status": "forced", "mode": payload.mode.value, "fix_goal": payload.fix_goal}
            raise HTTPException(status_code=409, detail="ALREADY_RUNNING")

        backing_dir = root / "runtime" / "agents"
        backing_dir.mkdir(parents=True, exist_ok=True)

        team = create_team(repo_root=root, backing_dir=backing_dir)
        _team["instance"] = team

        async def _run() -> None:
            try:
                await team.start()
            except Exception:
                logger.exception("Escort team crashed")
            finally:
                _team["instance"] = None
                _run_task["task"] = None

        _run_task["task"] = asyncio.create_task(_run(), name="escort-team-main")

        return {
            "status": "started",
            "mode": payload.mode.value,
            "fix_goal": payload.fix_goal,
            "current_round_id": None,
        }

    @app.post("/v1/stop")
    async def stop_team(payload: StopLoopRequest, request: Request) -> dict[str, Any]:
        _require_auth(token, request)
        t = _get_team()
        await t.shutdown(reason=payload.reason)

        task = _run_task.get("task")
        if task and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        _team["instance"] = None
        _run_task["task"] = None
        return {"status": "stopped", "reason": payload.reason, "phase": "idle"}

    # -- analyze (manual trigger) ------------------------------------------

    @app.post("/v1/analyze")
    async def analyze(payload: AnalyzeRequest, request: Request) -> dict[str, Any]:
        _require_auth(token, request)
        t = _get_team()
        # v5: Dispatch to discovery and WAIT for real result via mailbox
        from automation.agents.protocol import AgentMessage, AgentRole, MessageType
        round_id = payload.audit_run_id or f"http-analyze-{int(time.time())}"
        await t._mailbox.send(AgentMessage(
            source="http_shell",
            target=AgentRole.DISCOVERY.value,
            msg_type=MessageType.TASK_DISPATCH.value,
            payload={"round_id": round_id, "mode": "full"},
        ))
        # Wait for the coordinator to complete discovery and send result
        try:
            result_msg = await t._mailbox.receive(
                predicate=lambda m: (
                    m.msg_type == MessageType.TASK_RESULT.value
                    and m.source in (AgentRole.DISCOVERY.value, "coordinator")
                    and m.payload.get("phase") == "discovery"
                ),
                timeout=120.0,
            )
            if result_msg and result_msg.payload:
                findings = result_msg.payload.get("findings", [])
                return {
                    "problems": findings,
                    "new_count": len(findings),
                    "regression_count": 0,
                    "skipped_count": 0,
                }
        except Exception:
            logger.warning("[http_shell] /v1/analyze: timeout waiting for discovery result")
        return {
            "problems": [],
            "new_count": 0,
            "regression_count": 0,
            "skipped_count": 0,
            "_note": "discovery timed out or no results available",
        }

    # -- verify (manual trigger) -------------------------------------------

    @app.post("/v1/verify")
    async def verify(payload: VerifyRequest, request: Request) -> dict[str, Any]:
        _require_auth(token, request)
        t = _get_team()
        # v5: Dispatch to verify agent and wait for real result
        from automation.agents.protocol import AgentMessage, AgentRole, MessageType
        await t._mailbox.send(AgentMessage(
            source="http_shell",
            target=AgentRole.VERIFY.value,
            msg_type=MessageType.TASK_DISPATCH.value,
            payload={
                "round_id": getattr(payload, "round_id", f"http-verify-{int(time.time())}"),
                "patches": [],
                "analysis": {},
            },
        ))
        try:
            result_msg = await t._mailbox.receive(
                predicate=lambda m: (
                    m.msg_type == MessageType.TASK_RESULT.value
                    and m.source in (AgentRole.VERIFY.value, "coordinator")
                    and m.payload.get("phase") == "verification"
                ),
                timeout=600.0,
            )
            if result_msg and result_msg.payload:
                vr = result_msg.payload
                all_green = vr.get("all_passed", False)
                return {
                    "result": {
                        "scoped_pytest_passed": vr.get("scoped_pytest_passed", False),
                        "full_pytest_passed": vr.get("full_pytest_passed", False),
                        "full_pytest_total": vr.get("full_pytest_total", 0),
                        "full_pytest_failed": vr.get("full_pytest_failed", 0),
                        "blind_spot_clean": vr.get("blind_spot_clean", False),
                        "catalog_improved": vr.get("catalog_improved", False),
                        "artifacts_aligned": vr.get("artifacts_aligned", False),
                        "all_green": all_green,
                        "details": vr.get("details", {}),
                    },
                    "should_rollback": not all_green,
                }
        except Exception:
            logger.warning("[http_shell] /v1/verify: timeout waiting for verify result")
        return {
            "result": {
                "scoped_pytest_passed": False,
                "full_pytest_passed": False,
                "full_pytest_total": 0,
                "full_pytest_failed": 0,
                "blind_spot_clean": False,
                "catalog_improved": False,
                "artifacts_aligned": False,
                "all_green": False,
                "details": {},
                "_note": "verification timed out or not available",
            },
            "should_rollback": True,
        }

    # -- round complete (external notification) ----------------------------

    @app.post("/v1/round-complete")
    def round_complete(payload: RoundCompleteRequest, request: Request) -> dict[str, Any]:
        _require_auth(token, request)
        # v5: Only set the event; let await_round clear it after waking up
        _round_event.set()
        return {
            "status": "recorded",
            "round_id": payload.round_id,
            "execution_id": payload.execution_id,
        }

    # -- await round (long-poll) -------------------------------------------

    @app.get("/v1/await-round")
    async def await_round(request: Request, timeout: int = 600) -> dict[str, Any]:
        _require_auth(token, request)
        timeout = min(max(timeout, 10), 1800)
        try:
            await asyncio.wait_for(_round_event.wait(), timeout=timeout)
            _round_event.clear()  # v5: clear after wake, not at set time
            t = _team["instance"]
            if t:
                status = t.get_status()
                coord = status.get("coordinator", {})
                history = coord.get("round_history", [])
                last = history[-1] if history else None
                return {
                    "status": "completed",
                    "round_id": coord.get("current_round_id"),
                    "round_summary": last,
                    "timed_out": False,
                }
            return {"status": "completed", "round_id": None, "round_summary": None, "timed_out": False}
        except asyncio.TimeoutError:
            return {"status": "timeout", "round_id": None, "round_summary": None, "timed_out": True}

    # -- round history -----------------------------------------------------

    @app.get("/v1/rounds")
    def list_rounds(request: Request, last: int = 20) -> dict[str, Any]:
        _require_auth(token, request)
        t = _get_team()
        status = t.get_status()
        coord = status.get("coordinator", {})
        history = coord.get("round_history", [])
        rounds = history[-last:] if history else []
        return {"rounds": rounds, "total": len(history)}

    # -- v2: completion status ---------------------------------------------

    @app.get("/v1/completion-status")
    def completion_status(request: Request) -> dict[str, Any]:
        """Return whether the Escort Team has reached self-completion."""
        _require_auth(token, request)
        t = _get_team()
        status = t.get_status()
        coord_status = status.get("coordinator", {})
        coord = coord_status.get("coordinator", coord_status)
        projection = _runtime_projection(status)
        completion_evidence = projection.get("completion_evidence")
        if not isinstance(completion_evidence, dict) or not completion_evidence:
            completion_evidence = coord_status.get("completion_evidence", {})

        goal_reached = bool(projection.get("goal_reached", False))
        is_completed = coord.get("mode") == "completed" and goal_reached
        return {
            "completed": is_completed,
            "completion_time": projection.get("completion_time", coord.get("completion_time", "")),
            "mode": projection.get("mode", coord.get("mode", "unknown")),
            "goal_reached": goal_reached,
            "autonomy_index": completion_evidence.get("autonomy_index", coord_status.get("autonomy_index", 0.0)),
            "consecutive_green_rounds": projection.get("consecutive_green_rounds", coord.get("consecutive_green_rounds", 0)),
            "deferred_problems": projection.get("deferred_problems", coord.get("deferred_problems", [])),
            "execution_lanes": projection.get("execution_lanes", coord_status.get("execution_lanes", {})),
            "blocked_problems": projection.get("blocked_problems", coord_status.get("blocked_problems", [])),
            "completion_blockers": projection.get("completion_blockers", coord_status.get("completion_blockers", [])),
            "total_fixes": projection.get("total_fixes", coord.get("total_fixes", 0)),
            "total_rounds": coord.get("total_rounds", 0),
            "last_audit_run_id": projection.get("last_audit_run_id"),
            "last_promote_round_id": projection.get("last_promote_round_id"),
            "shadow_validation": projection.get("shadow_validation", completion_evidence.get("shadow_validation", {})),
            "formal_promote": projection.get("formal_promote", completion_evidence.get("formal_promote", {})),
        }

    # -- v4: round progress ------------------------------------------------

    @app.get("/v1/round-progress")
    def round_progress(request: Request) -> dict[str, Any]:
        """Return live progress of the current orchestration round."""
        _require_auth(token, request)
        t = _get_team()
        coord = t.coordinator
        if coord and hasattr(coord, "get_round_progress"):
            return coord.get_round_progress()
        return {"round_id": "", "phase": "idle", "problem_count": 0}

    # -- startup probe (v4: migrated to lifespan) ----------------------------

    @asynccontextmanager
    async def _lifespan(app_instance: FastAPI):
        """Lifespan context manager — replaces deprecated on_event("startup")."""
        # --- startup ---
        try:
            import httpx
            app_url = os.environ.get("APP_BASE_URL", "").rstrip("/")
            int_token = os.environ.get("INTERNAL_TOKEN", "").strip()
            if app_url and int_token:
                probe_url = f"{app_url}/api/v1/internal/runtime/gates"
                try:
                    async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
                        resp = await client.get(probe_url, headers={"X-Internal-Token": int_token})
                    if resp.status_code == 401:
                        logger.error("STARTUP_PROBE_FAILED: token drifted at %s", probe_url)
                    elif resp.status_code != 200:
                        logger.warning("STARTUP_PROBE_DEGRADED: %s returned %d", probe_url, resp.status_code)
                    else:
                        logger.info("STARTUP_PROBE_OK: internal token validated")
                except Exception as exc:
                    logger.warning("STARTUP_PROBE_UNREACHABLE: %s — %s", probe_url, exc)
            else:
                logger.info("STARTUP_PROBE_SKIPPED: APP_BASE_URL or INTERNAL_TOKEN not set")
        except ImportError:
            logger.info("STARTUP_PROBE_SKIPPED: httpx not installed")
        yield
        # --- shutdown ---
        logger.info("HTTP Shell shutting down")

    app.router.lifespan_context = _lifespan

    return app


app = create_app()
