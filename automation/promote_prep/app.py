from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from automation.promote_prep.service import PromotePrepConfig, PromotePrepService

RUN_ID_PATTERN = r"^issue-mesh-\d{8}-\d{3}$"


def _require_auth(token: str, request: Request) -> None:
    if not token:
        return
    if (request.headers.get("Authorization") or "").strip() != f"Bearer {token}":
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")


class ShadowWriteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(pattern=RUN_ID_PATTERN)
    summary_markdown: str
    findings_bundle: dict
    candidate_writeback_markdown: str | None = None


class IntentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(pattern=RUN_ID_PATTERN)
    summary_markdown: str
    findings_bundle: dict
    logical_target: str = Field(default="issue_mesh_shadow")
    candidate_writeback_markdown: str | None = None


class PromotePrepareRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(pattern=RUN_ID_PATTERN)
    runtime_gates: dict
    audit_context: dict


class CurrentLayerPrepareRequest(PromotePrepareRequest):
    enabled: bool = False


class TriageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(pattern=RUN_ID_PATTERN)
    layer: str
    target_path: str
    target_anchor: str
    patch_text: str
    base_sha256: str
    runtime_gates: dict
    audit_context: dict
    semantic_fingerprint: str | None = None


class GenericWritebackTriageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str | None = None
    workflow_id: str | None = None
    layer: str = "writeback"
    target_path: str
    patch_text: str
    base_sha256: str
    runtime_gates: dict = Field(default_factory=dict)
    audit_context: dict = Field(default_factory=dict)
    preview_summary: dict | None = None
    metadata: dict = Field(default_factory=dict)


class RollbackAcceptanceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(pattern=RUN_ID_PATTERN)
    layer: str
    target_path: str
    expected_base_sha256: str
    expected_shadow_snapshot: dict


class SynthesizePatchesRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_run_id: str = Field(pattern=RUN_ID_PATTERN)
    fix_run_id: str
    max_fix_items: int = Field(default=10, ge=1, le=50)
    runtime_gates: dict = Field(default_factory=dict)
    audit_context: dict = Field(default_factory=dict)


class ScopedPytestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fix_run_id: str
    changed_files: list[str]
    timeout_seconds: int = Field(default=120, ge=10, le=600)


def create_app(
    config: PromotePrepConfig | None = None,
    service: PromotePrepService | None = None,
) -> FastAPI:
    cfg = config or PromotePrepConfig.from_env()
    svc = service or PromotePrepService(cfg)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        svc.start()
        try:
            yield
        finally:
            svc.stop()

    app = FastAPI(title="Issue Mesh Shadow Writer", version="0.2.0", lifespan=lifespan)

    @app.get("/health")
    def health() -> dict[str, object]:
        return {
            "status": "ok",
            "shadow_root": str(cfg.shadow_root),
            "runtime_root": str(cfg.runtime_root),
            "redis_enabled": bool(cfg.redis_url),
            "metrics": svc.get_metrics(),
        }

    @app.post("/v1/shadow")
    def write_shadow(payload: ShadowWriteRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        return svc.write_shadow_sync(
            run_id=payload.run_id,
            summary_markdown=payload.summary_markdown,
            findings_bundle=payload.findings_bundle,
            candidate_writeback_markdown=payload.candidate_writeback_markdown,
        )

    @app.post("/v1/intents")
    def submit_intent(payload: IntentRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        return svc.submit_intent(
            run_id=payload.run_id,
            summary_markdown=payload.summary_markdown,
            findings_bundle=payload.findings_bundle,
            logical_target=payload.logical_target,
            candidate_writeback_markdown=payload.candidate_writeback_markdown,
        )

    @app.get("/v1/intents/{intent_id}")
    def get_intent(
        intent_id: str,
        request: Request,
        wait_for_completion: bool = False,
        wait_timeout_seconds: int = 0,
    ) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        try:
            return svc.get_intent(
                intent_id,
                wait_for_completion=wait_for_completion,
                wait_timeout_seconds=wait_timeout_seconds,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="INTENT_NOT_FOUND") from exc

    @app.post("/v1/promote/status-note")
    def prepare_status_note(payload: PromotePrepareRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        try:
            return svc.prepare_status_note_promote(
                run_id=payload.run_id,
                runtime_gates=payload.runtime_gates,
                audit_context=payload.audit_context,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=409, detail=str(exc) or "SHADOW_BUNDLE_NOT_FOUND") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc) or "STATUS_NOTE_PREPARE_REJECTED") from exc

    @app.post("/v1/promote/current-layer")
    def prepare_current_layer(payload: CurrentLayerPrepareRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        try:
            return svc.prepare_current_layer_promote(
                run_id=payload.run_id,
                enabled=payload.enabled,
                runtime_gates=payload.runtime_gates,
                audit_context=payload.audit_context,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=409, detail=str(exc) or "SHADOW_BUNDLE_NOT_FOUND") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc) or "CURRENT_LAYER_PREPARE_REJECTED") from exc

    @app.post("/v1/triage")
    def triage_promote(payload: TriageRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        try:
            return svc.triage_promote(
                run_id=payload.run_id,
                layer=payload.layer,
                target_path=payload.target_path,
                target_anchor=payload.target_anchor,
                patch_text=payload.patch_text,
                base_sha256=payload.base_sha256,
                runtime_gates=payload.runtime_gates,
                audit_context=payload.audit_context,
                semantic_fingerprint=payload.semantic_fingerprint,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=409, detail=str(exc) or "TRIAGE_INPUT_NOT_FOUND") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc) or "TRIAGE_REJECTED") from exc

    @app.post("/v1/triage/writeback")
    def triage_writeback(payload: GenericWritebackTriageRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        try:
            return svc.triage_writeback(
                run_id=payload.run_id,
                workflow_id=payload.workflow_id,
                layer=payload.layer,
                target_path=payload.target_path,
                patch_text=payload.patch_text,
                base_sha256=payload.base_sha256,
                runtime_gates=payload.runtime_gates,
                audit_context=payload.audit_context,
                preview_summary=payload.preview_summary,
                metadata=payload.metadata,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc) or "TRIAGE_REJECTED") from exc

    @app.post("/v1/promote/rollback-acceptance")
    def verify_rollback_acceptance(payload: RollbackAcceptanceRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        try:
            return svc.verify_rollback_acceptance(
                run_id=payload.run_id,
                layer=payload.layer,
                target_path=payload.target_path,
                expected_base_sha256=payload.expected_base_sha256,
                expected_shadow_snapshot=payload.expected_shadow_snapshot,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=409, detail=str(exc) or "ROLLBACK_ACCEPTANCE_NOT_FOUND") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc) or "ROLLBACK_ACCEPTANCE_FAILED") from exc

    @app.post("/v1/triage/synthesize-patches")
    def synthesize_patches(payload: SynthesizePatchesRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        try:
            return svc.synthesize_code_fix_patches(
                source_run_id=payload.source_run_id,
                fix_run_id=payload.fix_run_id,
                max_fix_items=payload.max_fix_items,
                runtime_gates=payload.runtime_gates,
                audit_context=payload.audit_context,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=409, detail=str(exc) or "SOURCE_BUNDLE_NOT_FOUND") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc) or "SYNTHESIS_FAILED") from exc

    @app.post("/v1/triage/scoped-pytest")
    def scoped_pytest(payload: ScopedPytestRequest, request: Request) -> dict[str, object]:
        _require_auth(cfg.auth_token, request)
        return svc.run_scoped_pytest(
            fix_run_id=payload.fix_run_id,
            changed_files=payload.changed_files,
            timeout_seconds=payload.timeout_seconds,
        )

    return app


app = create_app()
