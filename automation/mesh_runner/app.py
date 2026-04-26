from __future__ import annotations

from fastapi import FastAPI, HTTPException, Request

from automation.mesh_runner.runner import MeshRunnerConfig, MeshRunnerService
from automation.mesh_runner.schemas import RunAcceptedResponse, StartRunRequest


def _require_auth(token: str, request: Request) -> None:
    if not token:
        return
    if (request.headers.get("Authorization") or "").strip() != f"Bearer {token}":
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")


def create_app(
    config: MeshRunnerConfig | None = None,
    service: MeshRunnerService | None = None,
) -> FastAPI:
    cfg = config or MeshRunnerConfig.from_env()
    svc = service or MeshRunnerService(cfg)
    app = FastAPI(title="Issue Mesh Runner", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, object]:
        return {
            "status": "ok",
            "canonical_provider": cfg.canonical_provider,
            "readonly_max_workers": cfg.readonly_max_workers,
        }

    @app.post("/v1/runs", response_model=RunAcceptedResponse)
    def start_run(payload: StartRunRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        try:
            return svc.start_run(
                run_id=payload.run_id,
                run_label=payload.run_label,
                benchmark_label=payload.benchmark_label,
                max_workers=payload.max_workers,
                provider_allowlist=payload.provider_allowlist,
                audit_scope=payload.audit_scope,
                shard_strategy=payload.shard_strategy,
                control_state_snapshot=payload.control_state_snapshot,
                audit_context=payload.audit_context,
                wait_for_completion=payload.wait_for_completion,
                wait_timeout_seconds=payload.wait_timeout_seconds,
            )
        except ValueError as exc:
            detail = str(exc).strip() or "INVALID_RUN_REQUEST"
            status_code = 409 if detail == "RUN_ID_ALREADY_EXISTS" else 400
            raise HTTPException(status_code=status_code, detail=detail) from exc

    @app.get("/v1/runs/{run_id}", response_model=RunAcceptedResponse)
    def get_run(
        run_id: str,
        request: Request,
        wait_for_completion: bool = False,
        wait_timeout_seconds: int = 0,
    ) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        try:
            return svc.get_run(
                run_id,
                wait_for_completion=wait_for_completion,
                wait_timeout_seconds=wait_timeout_seconds,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="RUN_NOT_FOUND") from exc

    @app.get("/v1/runs/{run_id}/bundle")
    def get_bundle(run_id: str, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        try:
            return {"run_id": run_id, "bundle": svc.get_bundle(run_id)}
        except FileNotFoundError as exc:
            raise HTTPException(status_code=409, detail="RUN_NOT_COMPLETED") from exc

    return app


app = create_app()
