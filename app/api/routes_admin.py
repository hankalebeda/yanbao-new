from __future__ import annotations

import json
import logging
from datetime import date as date_type, datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.db import get_db
from app.core.error_codes import http_status_for
from app.core.request_context import require_request_id
from app.core.response import envelope
from app.core.security import get_current_user_optional
from app.models import (
    AdminOperation,
    AuditLog,
    Base,
    BillingOrder,
    PaymentWebhookEvent,
    Report,
    SimPosition,
    User,
)
from app.services.admin_audit import (
    create_admin_operation,
    create_audit_log,
    create_rejected_admin_artifacts,
)
from app.services.cookie_session_ssot import (
    CookieSessionNotFoundError,
    get_cookie_session_health,
    upsert_cookie_session,
)
from app.services.membership import (
    TIER_RANK,
    _extend_tier,
    _normalize_tier,
    get_payment_capability,
    grant_membership_order_entitlement,
    handle_webhook,
    is_paid_tier,
    probe_provider_order_status,
)
from app.services.notification import emit_operational_alert
from app.services.report_generation_ssot import (
    ReportGenerationServiceError,
    ensure_non_report_usage_collected_if_needed,
    generate_report_ssot,
)
from app.services.report_admin import (
    ReportPatchValidationError,
    patch_report_admin_fields,
    record_report_patch_rejection,
    report_admin_summary,
    validate_report_patch_payload,
)
from app.services.scheduler_ops_ssot import list_scheduler_runs
from app.services.settlement_ssot import (
    SettlementServiceError,
    _sync_admin_operation_from_settlement_task,
    get_settlement_task_status,
    get_settlement_pipeline_status,
    submit_settlement_task,
)
from app.services.ssot_read_model import get_public_pool_snapshot_ssot, get_runtime_anchor_dates_ssot
from app.services.stock_pool import PoolColdStartError, PoolRefreshConflict, get_public_pool_view, refresh_stock_pool

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])
logger = logging.getLogger(__name__)


class PoolRefreshRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trade_date: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    force_rebuild: bool = False


class SettlementRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trade_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    window_days: int
    target_scope: str
    target_report_id: str | None = None
    target_stock_code: str | None = Field(default=None, pattern=r"^\d{6}\.(SH|SZ|BJ)$")
    force: bool = False


class CookieSessionUpsertRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    login_source: str = Field(..., pattern=r"^(weibo|douyin|xueqiu|kuaishou)$")
    cookie_string: str = Field(..., min_length=1)


async def require_admin(request: Request):
    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    if (user.role or "").lower() not in {"admin", "super_admin"}:
        raise HTTPException(status_code=403, detail="FORBIDDEN")
    return user


def _gate_context_for_request(request: Request, *, gate: str) -> dict | None:
    path = request.url.path or ""
    path_params = request.path_params or {}
    if gate == "exact_admin" and "/admin/billing/orders/" in path and path.endswith("/reconcile"):
        return {
            "action_type": "RECONCILE_ORDER",
            "target_table": "billing_order",
            "target_pk": str(path_params.get("order_id") or "unknown"),
            "status_reason": "role_requires_exact_admin",
        }
    if gate == "super_admin" and path.endswith("/force-regenerate"):
        return {
            "action_type": "FORCE_REGENERATE",
            "target_table": "report",
            "target_pk": str(path_params.get("report_id") or "unknown"),
            "status_reason": "role_requires_super_admin",
        }
    return None


def _emit_admin_status_alert(
    *,
    action_type: str,
    target_table: str,
    target_pk: str,
    request_id: str,
    status: str,
    status_reason: str,
    error_code: str | None = None,
) -> None:
    if status not in {"FAILED", "REJECTED"}:
        return
    alert_type = "FORCE_REGENERATE_BLOCKED" if action_type == "FORCE_REGENERATE" and status == "REJECTED" else "ADMIN_OP_FAILED"
    try:
        emit_operational_alert(
            alert_type=alert_type,
            fr_id="FR-12",
            message=f"{action_type} {status.lower()} for {target_table}:{target_pk}",
            extra={
                "action_type": action_type,
                "target_table": target_table,
                "target_pk": target_pk,
                "request_id": request_id,
                "status": status,
                "status_reason": status_reason,
                "error_code": error_code,
            },
        )
    except Exception:
        logger.exception(
            "admin_status_alert_dispatch_failed action_type=%s target_table=%s target_pk=%s status=%s",
            action_type,
            target_table,
            target_pk,
            status,
        )


def _record_gate_rejection(
    db: Session,
    *,
    actor_user_id: str,
    request_id: str,
    action_type: str,
    target_table: str,
    target_pk: str,
    status_reason: str,
) -> None:
    existing = _find_existing_admin_operation(
        db,
        action_type=action_type,
        target_pk=target_pk,
        request_id=request_id,
    )
    if existing is not None:
        return
    create_rejected_admin_artifacts(
        db,
        actor_user_id=actor_user_id,
        action_type=action_type,
        target_table=target_table,
        target_pk=target_pk,
        request_id=request_id,
        status_reason=status_reason,
        error_code="FORBIDDEN",
        failure_category=status_reason,
    )
    db.commit()
    _emit_admin_status_alert(
        action_type=action_type,
        target_table=target_table,
        target_pk=target_pk,
        request_id=request_id,
        status="REJECTED",
        status_reason=status_reason,
        error_code="FORBIDDEN",
    )


def _record_admin_precheck_rejection(
    db: Session,
    *,
    actor_user_id: str,
    action_type: str,
    target_table: str,
    target_pk: str,
    request_id: str,
    status_reason: str,
    error_code: str,
    reason_code: str | None = None,
    before_snapshot: dict | None = None,
    after_snapshot: dict | None = None,
    audit_action_type: str | None = None,
) -> None:
    existing = _find_existing_admin_operation(
        db,
        action_type=action_type,
        target_pk=target_pk,
        request_id=request_id,
    )
    if existing is not None:
        return
    create_rejected_admin_artifacts(
        db,
        actor_user_id=actor_user_id,
        action_type=action_type,
        target_table=target_table,
        target_pk=target_pk,
        request_id=request_id,
        status_reason=status_reason,
        error_code=error_code,
        reason_code=reason_code,
        before_snapshot=before_snapshot,
        after_snapshot=after_snapshot,
        audit_action_type=audit_action_type,
    )
    db.commit()
    _emit_admin_status_alert(
        action_type=action_type,
        target_table=target_table,
        target_pk=target_pk,
        request_id=request_id,
        status="REJECTED",
        status_reason=status_reason,
        error_code=error_code,
    )


async def require_exact_admin(request: Request, db: Session = Depends(get_db)):
    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    if (user.role or "").lower() != "admin":
        gate_context = _gate_context_for_request(request, gate="exact_admin")
        if gate_context is not None:
            _record_gate_rejection(
                db,
                actor_user_id=str(user.user_id),
                request_id=_request_id(),
                action_type=gate_context["action_type"],
                target_table=gate_context["target_table"],
                target_pk=gate_context["target_pk"],
                status_reason=gate_context["status_reason"],
            )
        raise HTTPException(status_code=403, detail="FORBIDDEN")
    return user


def _request_id() -> str:
    return require_request_id()


def _find_existing_admin_operation(
    db: Session,
    *,
    action_type: str,
    target_pk: str,
    request_id: str,
) -> AdminOperation | None:
    return (
        db.execute(
            select(AdminOperation)
            .where(
                AdminOperation.action_type == action_type,
                AdminOperation.target_pk == target_pk,
                AdminOperation.request_id == request_id,
            )
            .order_by(AdminOperation.created_at.desc(), AdminOperation.operation_id.desc())
        )
        .scalars()
        .first()
    )


def _ensure_allowed_query_params(request: Request, allowed: set[str]) -> None:
    if set(request.query_params.keys()) - allowed:
        raise HTTPException(status_code=422, detail="INVALID_PAYLOAD")


def _parse_sort_value(sort: str | None, *, allowed: set[str], default: str) -> tuple[str, bool]:
    raw = (sort or default).strip()
    descending = raw.startswith("-")
    field = raw[1:] if descending else raw
    if field not in allowed:
        raise HTTPException(status_code=422, detail="INVALID_PAYLOAD")
    return field, descending


def _pipeline_stage_status_for_ui(pipeline_status: str | None) -> str:
    normalized = str(pipeline_status or "").upper()
    if normalized == "ACCEPTED":
        return "PENDING"
    if normalized == "RUNNING":
        return "RUNNING"
    if normalized == "COMPLETED":
        return "SUCCESS"
    if normalized == "DEGRADED":
        return "PARTIAL_SUCCESS"
    if normalized == "FAILED":
        return "FAILED"
    return normalized or "NOT_RUN"


def _record_billing_reconcile_outcome(
    db: Session,
    *,
    actor_user_id: str,
    target_pk: str,
    request_id: str,
    reason_code: str,
    before_snapshot: dict,
    after_snapshot: dict,
    status: str,
    status_reason: str | None = None,
) -> AdminOperation:
    operation = create_admin_operation(
        db,
        action_type="RECONCILE_ORDER",
        actor_user_id=actor_user_id,
        target_table="billing_order",
        target_pk=target_pk,
        request_id=request_id,
        status=status,
        reason_code=reason_code,
        before_snapshot=before_snapshot,
        after_snapshot=after_snapshot,
        failure_category=status_reason if status != "COMPLETED" else None,
        status_reason=status_reason,
    )
    create_audit_log(
        db,
        actor_user_id=actor_user_id,
        action_type="RECONCILE_ORDER",
        target_table="billing_order",
        target_pk=target_pk,
        request_id=request_id,
        operation_id=operation.operation_id,
        reason_code=reason_code,
        before_snapshot=before_snapshot,
        after_snapshot=after_snapshot,
        failure_category=status_reason if status != "COMPLETED" else None,
    )
    db.commit()
    if status != "COMPLETED" and status_reason:
        _emit_admin_status_alert(
            action_type="RECONCILE_ORDER",
            target_table="billing_order",
            target_pk=target_pk,
            request_id=request_id,
            status=status,
            status_reason=status_reason,
            error_code=str(after_snapshot.get("error_code") or "") or None,
        )
    return operation


def _resolved_membership_tier(current_tier: str | None, expected_tier: str | None) -> str:
    current = _normalize_tier(current_tier)
    expected = _normalize_tier(expected_tier)
    if TIER_RANK.get(expected, 0) >= TIER_RANK.get(current, 0):
        return expected
    return current


def _parse_iso_datetime(value: str | None) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _completed_reconcile_requires_repair(order: BillingOrder, user: User, operation: AdminOperation | None) -> bool:
    if operation is None:
        return False
    after_snapshot = getattr(operation, "after_snapshot", None) or {}
    expected_tier = str(after_snapshot.get("user_tier") or order.granted_tier or order.expected_tier or "")
    if expected_tier not in {"Pro", "Enterprise"}:
        return False
    current_order_status = str(getattr(order, "status", "") or "").strip().upper()
    expected_order_status = str(after_snapshot.get("order_status") or "PAID").strip().upper()
    expected_granted_tier = str(after_snapshot.get("order_granted_tier") or expected_tier or "")
    current_granted_tier = str(getattr(order, "granted_tier", "") or "")
    if expected_order_status == "PAID":
        if current_order_status != "PAID":
            return True
        if expected_granted_tier in {"Pro", "Enterprise"} and current_granted_tier != expected_granted_tier:
            return True
    membership_status = str(after_snapshot.get("membership_status") or "").strip().lower()
    current_tier = str(getattr(user, "tier", "") or "")
    current_expiry = getattr(user, "tier_expires_at", None)
    if membership_status == "unknown":
        if current_tier not in {"Free", expected_tier}:
            return False
        return current_tier != expected_tier or current_expiry is not None
    expected_expiry = _parse_iso_datetime(after_snapshot.get("user_tier_expires_at"))
    if expected_expiry is None:
        return False
    if current_tier not in {"Free", expected_tier}:
        return False
    if current_tier == "Free":
        return True
    if current_expiry is None:
        return True
    if current_expiry.tzinfo is None:
        current_expiry = current_expiry.replace(tzinfo=timezone.utc)
    else:
        current_expiry = current_expiry.astimezone(timezone.utc)
    return current_expiry != expected_expiry


@router.post("/pool/refresh")
async def admin_pool_refresh(
    payload: PoolRefreshRequest,
    request: Request,
    _: object = Depends(require_admin),
    db: Session = Depends(get_db),
):
    request_id = _request_id()
    user = await get_current_user_optional(request)
    actor_user_id = getattr(user, "user_id", "system")
    try:
        data = refresh_stock_pool(
            db,
            trade_date=payload.trade_date,
            force_rebuild=payload.force_rebuild,
            request_id=request_id,
        )
        operation = create_admin_operation(
            db,
            action_type="POOL_REFRESH",
            actor_user_id=actor_user_id,
            target_table="stock_pool_snapshot",
            target_pk=payload.trade_date or "latest",
            request_id=request_id,
            status="COMPLETED",
            before_snapshot=None,
            after_snapshot={"force_rebuild": payload.force_rebuild, "trade_date": payload.trade_date},
        )
        create_audit_log(
            db,
            actor_user_id=actor_user_id,
            action_type="POOL_REFRESH",
            target_table="stock_pool_snapshot",
            target_pk=payload.trade_date or "latest",
            request_id=request_id,
            operation_id=operation.operation_id,
        )
        db.commit()
        return envelope(data=data)
    except PoolRefreshConflict as exc:
        return JSONResponse(
            status_code=409,
            content={"success": False, "error_code": "CONCURRENT_CONFLICT", "message": str(exc)},
        )
    except PoolColdStartError as exc:
        raise HTTPException(status_code=500, detail="COLD_START_ERROR") from exc


@router.get("/scheduler/status")
async def admin_scheduler_status(
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    _: object = Depends(require_admin),
    db: Session = Depends(get_db),
):
    _ensure_allowed_query_params(request, {"page", "page_size"})
    return envelope(data=list_scheduler_runs(db, page=page, page_size=page_size))


@router.post("/cookie-session", status_code=201)
async def admin_upsert_cookie_session(
    payload: CookieSessionUpsertRequest,
    request: Request,
    user=Depends(require_admin),
    db: Session = Depends(get_db),
):
    result = upsert_cookie_session(
        db,
        login_source=payload.login_source,
        cookie_string=payload.cookie_string,
    )
    operation = create_admin_operation(
        db,
        action_type="UPSERT_COOKIE_SESSION",
        actor_user_id=user.user_id,
        target_table="cookie_session",
        target_pk=result["cookie_session_id"],
        request_id=_request_id(),
        status="COMPLETED",
        before_snapshot=result["before_snapshot"],
        after_snapshot=result["after_snapshot"],
    )
    create_audit_log(
        db,
        actor_user_id=user.user_id,
        action_type="UPSERT_COOKIE_SESSION",
        target_table="cookie_session",
        target_pk=result["cookie_session_id"],
        request_id=_request_id(),
        operation_id=operation.operation_id,
        before_snapshot=result["before_snapshot"],
        after_snapshot=result["after_snapshot"],
    )
    db.commit()
    return envelope(data=result["result"])


@router.get("/cookie-session/health")
async def admin_cookie_session_health(
    request: Request,
    login_source: str = Query(..., pattern=r"^(weibo|douyin|xueqiu|kuaishou)$"),
    session_id: str | None = Query(None),
    _: object = Depends(require_admin),
    db: Session = Depends(get_db),
):
    _ensure_allowed_query_params(request, {"login_source", "session_id"})
    try:
        data = get_cookie_session_health(db, login_source=login_source, session_id=session_id)
    except CookieSessionNotFoundError as exc:
        raise HTTPException(status_code=404, detail="NOT_FOUND") from exc
    return envelope(data=data)


@router.post("/settlement/run", status_code=202)
async def admin_settlement_run(
    payload: SettlementRunRequest,
    request: Request,
    user=Depends(require_admin),
    db: Session = Depends(get_db),
):
    request_id = _request_id()
    try:
        data = submit_settlement_task(
            db,
            trade_date=payload.trade_date,
            window_days=payload.window_days,
            target_scope=payload.target_scope,
            target_report_id=payload.target_report_id,
            target_stock_code=payload.target_stock_code,
            force=payload.force,
            request_id=request_id,
            requested_by_user_id=user.user_id,
            # Honor SETTLEMENT_INLINE_EXECUTION in test/runtime envs.
            run_inline=None,
        )
        task_status_snapshot = get_settlement_task_status(db, task_id=data["task_id"])
        after_snap = {
                "task_id": data["task_id"],
                "trade_date": payload.trade_date,
                "window_days": payload.window_days,
                "target_scope": payload.target_scope,
                "target_report_id": payload.target_report_id,
                "target_stock_code": payload.target_stock_code,
                "force": payload.force,
                "task_submit_status": data["status"],
                "task_status_snapshot": task_status_snapshot["status"],
        }
        create_admin_operation(
            db,
            action_type="RUN_SETTLEMENT",
            actor_user_id=user.user_id,
            target_table="settlement_task",
            target_pk=data["task_id"],
            request_id=request_id,
            status="PENDING",
            before_snapshot=None,
            after_snapshot=after_snap,
            started_at=datetime.now(timezone.utc),
        )
        db.commit()
        task_status_snapshot = get_settlement_task_status(db, task_id=data["task_id"])
        _sync_admin_operation_from_settlement_task(db, task_status_snapshot)
        db.commit()
        return envelope(data=data)
    except SettlementServiceError as exc:
        db.rollback()
        raise HTTPException(status_code=exc.status_code, detail=exc.error_code) from exc


@router.patch("/reports/{report_id}")
async def admin_patch_report(
    report_id: str,
    request: Request,
    user=Depends(require_admin),
    db: Session = Depends(get_db),
):
    request_id = _request_id()
    report = db.get(Report, report_id)
    if not report:
        raise HTTPException(status_code=404, detail="NOT_FOUND")

    try:
        raw_payload = await request.json()
    except json.JSONDecodeError as exc:
        _record_admin_precheck_rejection(
            db,
            actor_user_id=user.user_id,
            action_type="PATCH_REPORT",
            target_table="report",
            target_pk=report_id,
            request_id=request_id,
            status_reason="payload_must_be_object",
            error_code="INVALID_PAYLOAD",
            before_snapshot=report_admin_summary(report),
            after_snapshot={"attempted_payload": None},
        )
        raise HTTPException(status_code=422, detail="INVALID_PAYLOAD") from exc

    try:
        payload = validate_report_patch_payload(raw_payload)
    except ReportPatchValidationError as exc:
        record_report_patch_rejection(
            db,
            actor_user_id=user.user_id,
            report=report,
            request_id=request_id,
            payload=raw_payload if isinstance(raw_payload, dict) else None,
            status_reason=str(exc),
        )
        db.commit()
        _emit_admin_status_alert(
            action_type="PATCH_REPORT",
            target_table="report",
            target_pk=report_id,
            request_id=request_id,
            status="REJECTED",
            status_reason=str(exc),
            error_code="INVALID_PAYLOAD",
        )
        raise HTTPException(status_code=422, detail="INVALID_PAYLOAD") from exc

    data = patch_report_admin_fields(
        db,
        actor_user_id=user.user_id,
        report=report,
        payload=payload,
        request_id=request_id,
    )
    db.commit()
    return envelope(data=data)


# ──── require_super_admin ──────────────────────────────
async def require_super_admin(request: Request, db: Session = Depends(get_db)):
    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    if (user.role or "").lower() != "super_admin":
        gate_context = _gate_context_for_request(request, gate="super_admin")
        if gate_context is not None:
            _record_gate_rejection(
                db,
                actor_user_id=str(user.user_id),
                request_id=_request_id(),
                action_type=gate_context["action_type"],
                target_table=gate_context["target_table"],
                target_pk=gate_context["target_pk"],
                status_reason=gate_context["status_reason"],
            )
        raise HTTPException(status_code=403, detail="FORBIDDEN")
    return user


# ──── GET /admin/overview ──────────────────────────────
@router.get("/overview")
async def admin_overview(
    request: Request,
    _: object = Depends(require_admin),
    db: Session = Depends(get_db),
):
    _ensure_allowed_query_params(request, set())

    # Capital tiers from config.CAPITAL_TIERS (SSOT driven, not hardcoded)
    try:
        tier_keys = tuple(json.loads(settings.capital_tiers).keys())
    except (json.JSONDecodeError, TypeError, ValueError):
        _fallback_tiers = {"10k": {}, "100k": {}, "500k": {}}
        tier_keys = tuple(_fallback_tiers.keys())

    pool_snapshot = get_public_pool_snapshot_ssot(db)
    pool_view = pool_snapshot["pool_view"]
    pool_size = int(pool_snapshot["pool_size"] or 0)

    anchor_dates = get_runtime_anchor_dates_ssot(db)
    # 管理后台的“今日”固定指向当前运行日，避免和公开看板/首页口径继续漂移
    runtime_trade_date = anchor_dates.get("runtime_trade_date")
    if runtime_trade_date:
        today = date_type.fromisoformat(runtime_trade_date)
    else:
        from app.services.trade_calendar import latest_trade_date_str

        today = date_type.fromisoformat(latest_trade_date_str())
    today_reports = db.scalar(
        select(func.count()).select_from(Report.__table__).where(
            Report.trade_date == today,
            Report.is_deleted == False,  # noqa: E712
        )
    ) or 0

    # today_buy_signals — BF-21 fix: query report table per SSOT 04§9:
    # trade_date=当日 AND recommendation='BUY' AND confidence>=0.65 AND is_deleted=false
    today_buy_signals = db.scalar(
        select(func.count()).select_from(Report.__table__).where(
            Report.trade_date == today,
            Report.recommendation == "BUY",
            Report.confidence >= 0.65,
            Report.is_deleted == False,  # noqa: E712
        )
    ) or 0

    # pending_review
    pending_review = db.scalar(
        select(func.count()).select_from(Report.__table__).where(
            Report.review_flag == "PENDING_REVIEW",
            Report.is_deleted == False,  # noqa: E712
        )
    ) or 0

    # active_positions per tier
    active_positions = {}
    for tier_key in tier_keys:
        cnt = db.scalar(
            select(func.count()).select_from(SimPosition.__table__).where(
                SimPosition.capital_tier == tier_key,
                SimPosition.position_status == "OPEN",
            )
        ) or 0
        active_positions[tier_key] = cnt

    # scheduler_last_run
    sr_t = Base.metadata.tables["scheduler_task_run"]
    last_run_row = db.execute(
        select(sr_t.c.started_at, sr_t.c.status)
        .order_by(sr_t.c.started_at.desc())
        .limit(1)
    ).first()
    scheduler_last_run = str(last_run_row[0]) if last_run_row else None
    scheduler_last_run_status = last_run_row[1] if last_run_row else None

    # ── Pipeline progress (FR-02 DAG stages for latest trade date) ──
    pipeline_stages = {}
    for task_name in ("fr01_stock_pool", "fr04_data_collect", "fr05_market_state",
                      "fr05_non_report_truth_materialize", "fr06_report_gen", "fr07_settlement", "fr08_sim_trade", "fr13_event_notify"):
        if task_name == "fr07_settlement":
            settlement_pipeline = get_settlement_pipeline_status(
                db,
                trade_date=today.isoformat(),
                target_scope="all",
            )
            pipeline_status = settlement_pipeline.get("pipeline_status")
            if pipeline_status and pipeline_status != "NOT_RUN":
                pipeline_stages[task_name] = {
                    "status": _pipeline_stage_status_for_ui(pipeline_status),
                    "pipeline_status": pipeline_status,
                    "started_at": str(settlement_pipeline.get("started_at")) if settlement_pipeline.get("started_at") else None,
                    "completed_at": str(settlement_pipeline.get("finished_at")) if settlement_pipeline.get("finished_at") else None,
                    "error": settlement_pipeline.get("status_reason"),
                }
                continue
        stage_row = db.execute(
            select(sr_t.c.status, sr_t.c.started_at, sr_t.c.finished_at, sr_t.c.error_message)
            .where(sr_t.c.task_name == task_name, sr_t.c.trade_date == today)
            .order_by(sr_t.c.started_at.desc())
            .limit(1)
        ).first()
        if stage_row:
            pipeline_stages[task_name] = {
                "status": stage_row[0],
                "started_at": str(stage_row[1]) if stage_row[1] else None,
                "completed_at": str(stage_row[2]) if stage_row[2] else None,
                "error": stage_row[3],
            }
        else:
            pipeline_stages[task_name] = {"status": "NOT_RUN", "started_at": None, "completed_at": None, "error": None}

    # ── Report generation progress ──
    report_gen_total = db.scalar(
        select(func.count()).select_from(Report.__table__).where(
            Report.trade_date == today, Report.is_deleted == False,  # noqa: E712
        )
    ) or 0
    report_gen_by_strategy = {}
    for st in ("A", "B", "C"):
        cnt = db.scalar(
            select(func.count()).select_from(Report.__table__).where(
                Report.trade_date == today, Report.is_deleted == False,  # noqa: E712
                Report.strategy_type == st,
            )
        ) or 0
        report_gen_by_strategy[st] = cnt

    # ── Data freshness ──
    kline_t = Base.metadata.tables["kline_daily"]
    latest_kline_date = db.scalar(
        select(func.max(kline_t.c.trade_date))
    )
    market_state_t = Base.metadata.tables["market_state_cache"]
    latest_ms_date = db.scalar(
        select(func.max(market_state_t.c.trade_date))
    )

    # ── LLM health check ──
    llm_health = {"status": "unknown", "provider": settings.llm_backend or "default"}
    try:
        from app.services.llm_router import get_primary_status
        status = get_primary_status()
        llm_health = {
            "status": status,
            "provider": settings.llm_backend or "codex_api",
            "reason": None if status == "ok" else f"llm_primary_status={status}",
        }
    except Exception as e:
        llm_health = {"status": "error", "provider": settings.llm_backend or "default", "reason": str(e)}

    # ── Database statistics ──
    db_tables = Base.metadata.tables.keys()
    db_stats = {
        "total_tables": len(db_tables),
        "app_user_count": db.scalar(select(func.count()).select_from(User.__table__)) or 0,
        "report_count": db.scalar(select(func.count()).select_from(Report.__table__)) or 0,
    }

    return envelope(data={
        "pool_size": pool_size,
        "today_reports": today_reports,
        "today_buy_signals": today_buy_signals,
        "pending_review": pending_review,
        "active_positions": active_positions,
        "scheduler_last_run": scheduler_last_run,
        "scheduler_last_run_status": scheduler_last_run_status,
        "latest_trade_date": today.isoformat(),
        "pipeline_stages": pipeline_stages,
        "report_generation": {
            "total": report_gen_total,
            "pool_size": pool_size,
            "progress_pct": round(report_gen_total / max(pool_size, 1) * 100, 1),
            "by_strategy": report_gen_by_strategy,
        },
        "source_dates": anchor_dates,
        "data_freshness": {
            "latest_kline_date": str(latest_kline_date) if latest_kline_date else None,
            "latest_market_state_date": str(latest_ms_date) if latest_ms_date else None,
        },
        "llm_health": llm_health,
        "db_statistics": db_stats,
    })


# ──── POST /admin/reports/{id}/force-regenerate ────────
class ForceRegenerateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    force_regenerate: bool = Field(...)
    reason_code: str = Field(..., min_length=1)


@router.post("/reports/{report_id}/force-regenerate")
async def admin_force_regenerate(
    report_id: str,
    payload: ForceRegenerateRequest,
    request: Request,
    user=Depends(require_super_admin),
    db: Session = Depends(get_db),
):
    if payload.force_regenerate is not True:
        _record_admin_precheck_rejection(
            db,
            actor_user_id=user.user_id,
            action_type="FORCE_REGENERATE",
            target_table="report",
            target_pk=report_id,
            request_id=_request_id(),
            status_reason="force_regenerate_literal_true_required",
            error_code="INVALID_PAYLOAD",
            reason_code=payload.reason_code,
            before_snapshot=None,
            after_snapshot={
                "old_report_id": report_id,
                "new_report_id": None,
                "requested_force_regenerate": payload.force_regenerate,
            },
        )
        raise HTTPException(status_code=422, detail="INVALID_PAYLOAD")

    request_id = _request_id()
    existing_op = _find_existing_admin_operation(
        db,
        action_type="FORCE_REGENERATE",
        target_pk=report_id,
        request_id=request_id,
    )
    if existing_op:
        existing_after = existing_op.after_snapshot or {}
        if existing_op.status == "COMPLETED":
            return envelope(data={
                "old_report_id": report_id,
                "new_report_id": existing_after.get("new_report_id"),
                "status": "COMPLETED",
                "reason_code": existing_op.reason_code,
            })
        if existing_op.status == "REJECTED" and existing_op.status_reason == "REPORT_ALREADY_REFERENCED_BY_SIM":
            raise HTTPException(status_code=409, detail="REPORT_ALREADY_REFERENCED_BY_SIM")
        if existing_op.status_reason == "DEPENDENCY_NOT_READY":
            raise HTTPException(status_code=503, detail="DEPENDENCY_NOT_READY")
        raise HTTPException(status_code=409, detail="IDEMPOTENCY_CONFLICT")

    report = db.get(Report, report_id)
    if not report:
        raise HTTPException(status_code=404, detail="NOT_FOUND")

    before_snap = {
        "report_id": report.report_id,
        "is_deleted": report.is_deleted,
        "force_regenerate": payload.force_regenerate,
    }

    def _record_force_regenerate_outcome(*, status: str, status_reason: str, after_snapshot: dict) -> None:
        operation = create_admin_operation(
            db,
            action_type="FORCE_REGENERATE",
            actor_user_id=user.user_id,
            target_table="report",
            target_pk=report_id,
            request_id=request_id,
            status=status,
            reason_code=payload.reason_code,
            before_snapshot=before_snap,
            after_snapshot=after_snapshot,
            status_reason=status_reason,
        )
        create_audit_log(
            db,
            actor_user_id=user.user_id,
            action_type="FORCE_REGENERATE",
            target_table="report",
            target_pk=report_id,
            request_id=request_id,
            operation_id=operation.operation_id,
            reason_code=payload.reason_code,
            before_snapshot=before_snap,
            after_snapshot=after_snapshot,
            failure_category=status_reason if status != "COMPLETED" else None,
        )
        db.commit()
        if status != "COMPLETED":
            _emit_admin_status_alert(
                action_type="FORCE_REGENERATE",
                target_table="report",
                target_pk=report_id,
                request_id=request_id,
                status=status,
                status_reason=status_reason,
                error_code=str(after_snapshot.get("error_code") or "") or None,
            )

    regen_key = f"regen:{report.report_id}:{uuid4()}"

    # Cascade guard: check sim_position / settlement_result references
    has_sim = db.scalar(
        select(func.count()).select_from(SimPosition.__table__).where(
            SimPosition.report_id == report_id,
        )
    )
    if has_sim:
        _record_force_regenerate_outcome(
            status="REJECTED",
            status_reason="REPORT_ALREADY_REFERENCED_BY_SIM",
            after_snapshot={
                "old_report_id": report.report_id,
                "new_report_id": None,
                "status": "REJECTED",
                "requested_force_regenerate": payload.force_regenerate,
                "error_code": "REPORT_ALREADY_REFERENCED_BY_SIM",
                "fallback_clone_disabled": True,
            },
        )
        raise HTTPException(
            status_code=409,
            detail="REPORT_ALREADY_REFERENCED_BY_SIM",
        )

    sr_t = Base.metadata.tables["settlement_result"]
    has_settle = db.scalar(
        select(func.count()).select_from(sr_t).where(
            sr_t.c.report_id == report_id,
        )
    )
    if has_settle:
        _record_force_regenerate_outcome(
            status="REJECTED",
            status_reason="REPORT_ALREADY_REFERENCED_BY_SIM",
            after_snapshot={
                "old_report_id": report.report_id,
                "new_report_id": None,
                "status": "REJECTED",
                "requested_force_regenerate": payload.force_regenerate,
                "error_code": "REPORT_ALREADY_REFERENCED_BY_SIM",
                "fallback_clone_disabled": True,
            },
        )
        raise HTTPException(
            status_code=409,
            detail="REPORT_ALREADY_REFERENCED_BY_SIM",
        )

    try:
        await ensure_non_report_usage_collected_if_needed(
            db,
            stock_code=report.stock_code,
            trade_date=str(report.trade_date) if report.trade_date else None,
        )
        generated = generate_report_ssot(
            db,
            stock_code=report.stock_code,
            trade_date=str(report.trade_date) if report.trade_date else None,
            idempotency_key=regen_key,
            request_id=request_id,
            skip_pool_check=True,
            force_same_day_rebuild=True,
        )
        new_report = db.get(Report, generated["report_id"])
        if not new_report:
            raise ReportGenerationServiceError(503, "DEPENDENCY_NOT_READY")
    except ReportGenerationServiceError as exc:
        db.rollback()
        if exc.error_code == "DEPENDENCY_NOT_READY":
            _record_force_regenerate_outcome(
                status="FAILED",
                status_reason="DEPENDENCY_NOT_READY",
                after_snapshot={
                    "old_report_id": report.report_id,
                    "new_report_id": None,
                    "status": "FAILED",
                    "requested_force_regenerate": payload.force_regenerate,
                    "error_code": "DEPENDENCY_NOT_READY",
                    "fallback_clone_disabled": True,
                },
            )
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=exc.status_code,
            content=envelope(
                code=exc.status_code, message=exc.error_code,
                error=exc.error_code, error_code=exc.error_code,
            ),
        )

    # Soft-delete old report after new report exists to satisfy self-FK.
    report.is_deleted = True
    report.deleted_at = datetime.now(timezone.utc)
    report.superseded_by_report_id = new_report.report_id

    after_snap = {
        "old_report_id": report.report_id,
        "new_report_id": new_report.report_id,
        "is_deleted": True,
        "regenerate_mode": "rebuild",
    }

    _record_force_regenerate_outcome(
        status="COMPLETED",
        status_reason="COMPLETED",
        after_snapshot=after_snap,
    )
    return envelope(data={
        "old_report_id": report_id,
        "new_report_id": new_report.report_id,
        "status": "COMPLETED",
        "reason_code": payload.reason_code,
    })


# ──── POST /admin/billing/orders/{order_id}/reconcile ──
class BillingReconcileRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    provider: str = Field(...)
    order_id: str = Field(...)
    expected_tier: str = Field(..., pattern=r"^(Pro|Enterprise)$")
    reason_code: str = Field(..., min_length=1)


@router.post("/billing/orders/{order_id}/reconcile")
async def admin_billing_reconcile(
    order_id: str,
    payload: BillingReconcileRequest,
    request: Request,
    user=Depends(require_exact_admin),
    db: Session = Depends(get_db),
):
    request_id = _request_id()
    order = db.get(BillingOrder, order_id)
    if not order:
        raise HTTPException(status_code=404, detail="NOT_FOUND")

    before_snap = {
        "order_status": order.status,
        "order_expected_tier": order.expected_tier,
        "order_granted_tier": order.granted_tier,
    }

    # P1-12: 校验 body order_id 与 path 一致
    if payload.order_id != order_id:
        _record_admin_precheck_rejection(
            db,
            actor_user_id=user.user_id,
            action_type="RECONCILE_ORDER",
            target_table="billing_order",
            target_pk=order_id,
            request_id=request_id,
            status_reason="path_body_order_mismatch",
            error_code="INVALID_PAYLOAD",
            reason_code=payload.reason_code,
            before_snapshot=before_snap,
            after_snapshot={
                "body_order_id": payload.order_id,
                "provider": payload.provider,
                "expected_tier": payload.expected_tier,
            },
        )
        raise HTTPException(status_code=422, detail="INVALID_PAYLOAD")
    # P1-12: 校验 provider 与订单一致
    if hasattr(order, "provider") and order.provider and payload.provider != order.provider:
        _record_admin_precheck_rejection(
            db,
            actor_user_id=user.user_id,
            action_type="RECONCILE_ORDER",
            target_table="billing_order",
            target_pk=order_id,
            request_id=request_id,
            status_reason="provider_mismatch",
            error_code="INVALID_PAYLOAD",
            reason_code=payload.reason_code,
            before_snapshot=before_snap,
            after_snapshot={
                "body_order_id": payload.order_id,
                "provider": payload.provider,
                "expected_tier": payload.expected_tier,
            },
        )
        raise HTTPException(status_code=422, detail="INVALID_PAYLOAD")
    if payload.expected_tier != order.expected_tier:
        _record_admin_precheck_rejection(
            db,
            actor_user_id=user.user_id,
            action_type="RECONCILE_ORDER",
            target_table="billing_order",
            target_pk=order_id,
            request_id=request_id,
            status_reason="expected_tier_mismatch",
            error_code="INVALID_PAYLOAD",
            reason_code=payload.reason_code,
            before_snapshot=before_snap,
            after_snapshot={
                "body_order_id": payload.order_id,
                "provider": payload.provider,
                "expected_tier": payload.expected_tier,
            },
        )
        raise HTTPException(status_code=422, detail="INVALID_PAYLOAD")

    existing_request_op = _find_existing_admin_operation(
        db,
        action_type="RECONCILE_ORDER",
        target_pk=order_id,
        request_id=request_id,
    )
    if existing_request_op and existing_request_op.status in {"FAILED", "REJECTED"}:
        after_snapshot = getattr(existing_request_op, "after_snapshot", None) or {}
        error_code = str(after_snapshot.get("error_code") or existing_request_op.status_reason or "CONCURRENT_CONFLICT")
        replay_status = 503 if error_code == "UPSTREAM_TIMEOUT" else http_status_for(error_code)
        raise HTTPException(status_code=replay_status, detail=error_code)

    target_user = db.get(User, order.user_id) if order.user_id else None
    if not target_user:
        raise HTTPException(status_code=404, detail="NOT_FOUND")

    # Idempotency: only short-circuit when the current order/user state is still healthy.
    existing_op = db.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "RECONCILE_ORDER",
            AdminOperation.target_pk == order_id,
            AdminOperation.status == "COMPLETED",
        )
        .order_by(AdminOperation.created_at.desc(), AdminOperation.operation_id.desc())
    ).scalars().first()
    repair_required = _completed_reconcile_requires_repair(order, target_user, existing_op)
    if existing_op and not repair_required:
        return envelope(data={
            "operation_id": existing_op.operation_id,
            "reconciled": True,
            "order": {
                "order_id": order.order_id,
                "status": order.status,
                "expected_tier": order.expected_tier,
            },
        })

    before_snap = {
        "order_status": order.status,
        "order_expected_tier": order.expected_tier,
        "order_granted_tier": order.granted_tier,
        "user_tier": target_user.tier,
        "user_tier_expires_at": target_user.tier_expires_at.isoformat() if getattr(target_user, "tier_expires_at", None) else None,
    }
    has_successful_webhook = bool(
        db.scalar(
            select(func.count()).select_from(PaymentWebhookEvent.__table__).where(
                PaymentWebhookEvent.order_id == order_id,
                PaymentWebhookEvent.provider == payload.provider,
                PaymentWebhookEvent.event_type == "PAYMENT_SUCCEEDED",
                PaymentWebhookEvent.processing_succeeded.is_(True),
            )
        )
    )
    has_provider_truth = has_successful_webhook
    provider_truth_source = "payment_webhook_event" if has_successful_webhook else None
    provider_probe_payload = None
    probe_matches_order = False
    if not has_provider_truth:
        provider_probe_payload = probe_provider_order_status(db, order.order_id)
        probe_status = str((provider_probe_payload or {}).get("status") or "").upper()
        probe_provider = str((provider_probe_payload or {}).get("provider") or order.provider or "")
        probe_tier = str((provider_probe_payload or {}).get("tier_id") or order.expected_tier or "")
        try:
            probe_amount = float((provider_probe_payload or {}).get("paid_amount") or order.amount_cny)
        except (TypeError, ValueError):
            probe_amount = None
        probe_matches_order = (
            probe_status == "PAID"
            and probe_provider == str(order.provider or "")
            and probe_tier == str(order.expected_tier or "")
            and probe_amount is not None
            and float(order.amount_cny) == probe_amount
        )
        if probe_matches_order:
            probe_event_id = str(provider_probe_payload.get("event_id") or f"admin_probe:{order.order_id}")
            if getattr(order, "provider_order_id", None) in {None, ""}:
                order.provider_order_id = str(provider_probe_payload.get("provider_order_id") or probe_event_id)
            if db.get(PaymentWebhookEvent, probe_event_id) is None:
                now = datetime.now(timezone.utc)
                db.add(
                    PaymentWebhookEvent(
                        event_id=probe_event_id,
                        order_id=order.order_id,
                        provider=str(provider_probe_payload.get("provider") or order.provider),
                        event_type="PAYMENT_SUCCEEDED",
                        payload_json={
                            "source": "admin_reconcile_probe",
                            "provider_status": provider_probe_payload,
                            "provider_order_id": provider_probe_payload.get("provider_order_id"),
                        },
                        request_id=request_id,
                        processing_succeeded=True,
                        duplicate_count=0,
                        received_at=now,
                        processed_at=now,
                    )
                )
            has_provider_truth = True
            provider_truth_source = "provider_status_probe"
    if not has_provider_truth:
        payment_capability = get_payment_capability()
        after_snap = {
            "order_status": order.status,
            "order_expected_tier": order.expected_tier,
            "order_granted_tier": order.granted_tier,
            "user_tier": target_user.tier,
            "user_tier_expires_at": target_user.tier_expires_at.isoformat() if getattr(target_user, "tier_expires_at", None) else None,
            "reconciled": False,
            "error_code": "UPSTREAM_TIMEOUT",
            "provider_truth_state": "missing",
            "provider_truth_source": None,
            "provider_status": str(payment_capability.get("provider_status") or "provider-not-configured"),
            "provider_probe_status": str((provider_probe_payload or {}).get("status") or "") or None,
            "provider_probe_matches_order": bool(provider_probe_payload) and probe_matches_order,
            "provider_modes": sorted(
                {
                    str(item.get("mode"))
                    for item in (payment_capability.get("providers") or [])
                    if isinstance(item, dict) and item.get("mode")
                }
            ),
        }
        _record_billing_reconcile_outcome(
            db,
            actor_user_id=user.user_id,
            target_pk=order_id,
            request_id=request_id,
            reason_code=payload.reason_code,
            before_snapshot=before_snap,
            after_snapshot=after_snap,
            status="FAILED",
            status_reason="reconcile_probe_missing",
        )
        raise HTTPException(status_code=503, detail="UPSTREAM_TIMEOUT")

    if repair_required and existing_op is not None:
        snapshot = getattr(existing_op, "after_snapshot", None) or {}
        repaired_tier = str(snapshot.get("user_tier") or order.granted_tier or order.expected_tier)
        repaired_membership_status = str(snapshot.get("membership_status") or "").strip().lower()
        repaired_expiry = _parse_iso_datetime(snapshot.get("user_tier_expires_at"))
        if repaired_membership_status == "unknown" and repaired_tier in {"Pro", "Enterprise"}:
            target_user.tier = repaired_tier
            target_user.tier_expires_at = None
            order.status = "PAID"
            order.granted_tier = str(snapshot.get("order_granted_tier") or repaired_tier)
            if hasattr(order, "paid_at") and not order.paid_at:
                order.paid_at = datetime.now(timezone.utc)
            after_snap = {
                "order_status": order.status,
                "order_expected_tier": order.expected_tier,
                "order_granted_tier": order.granted_tier,
                "user_tier": target_user.tier,
                "user_tier_expires_at": None,
                "reconciled": True,
                "provider_truth_state": "confirmed",
                "provider_truth_source": str(snapshot.get("provider_truth_source") or "payment_webhook_event"),
                "membership_status": "unknown",
                "membership_status_reason": str(snapshot.get("membership_status_reason") or "expiry_unconfirmed"),
                "repair_source": "completed_admin_snapshot",
            }
            operation = _record_billing_reconcile_outcome(
                db,
                actor_user_id=user.user_id,
                target_pk=order_id,
                request_id=request_id,
                reason_code=payload.reason_code,
                before_snapshot=before_snap,
                after_snapshot=after_snap,
                status="COMPLETED",
            )
            return envelope(data={
                "operation_id": operation.operation_id,
                "reconciled": True,
                "order": {
                    "order_id": order.order_id,
                    "status": order.status,
                    "expected_tier": order.expected_tier,
                },
            })
        if repaired_tier in {"Pro", "Enterprise"} and repaired_expiry is not None:
            target_user.tier = repaired_tier
            target_user.tier_expires_at = repaired_expiry
            order.status = "PAID"
            order.granted_tier = str(snapshot.get("order_granted_tier") or repaired_tier)
            if hasattr(order, "paid_at") and not order.paid_at:
                order.paid_at = datetime.now(timezone.utc)
            after_snap = {
                "order_status": order.status,
                "order_expected_tier": order.expected_tier,
                "order_granted_tier": order.granted_tier,
                "user_tier": target_user.tier,
                "user_tier_expires_at": repaired_expiry.isoformat(),
                "reconciled": True,
                "provider_truth_state": "confirmed",
                "provider_truth_source": provider_truth_source or "payment_webhook_event",
                "repair_source": "completed_admin_snapshot",
            }
            operation = _record_billing_reconcile_outcome(
                db,
                actor_user_id=user.user_id,
                target_pk=order_id,
                request_id=request_id,
                reason_code=payload.reason_code,
                before_snapshot=before_snap,
                after_snapshot=after_snap,
                status="COMPLETED",
            )
            return envelope(data={
                "operation_id": operation.operation_id,
                "reconciled": True,
                "order": {
                    "order_id": order.order_id,
                    "status": order.status,
                    "expected_tier": order.expected_tier,
                },
            })

    if is_paid_tier(getattr(target_user, "tier", None)) and getattr(target_user, "tier_expires_at", None) is None:
        granted_tier = _resolved_membership_tier(target_user.tier, order.expected_tier)
        target_user.tier = granted_tier
        order.status = "PAID"
        order.granted_tier = granted_tier
        order.status_reason = None
        if hasattr(order, "paid_at") and not order.paid_at:
            order.paid_at = datetime.now(timezone.utc)
        after_snap = {
            "order_status": order.status,
            "order_expected_tier": order.expected_tier,
            "order_granted_tier": granted_tier,
            "user_tier": granted_tier,
            "user_tier_expires_at": None,
            "reconciled": True,
            "provider_truth_state": "confirmed",
            "provider_truth_source": provider_truth_source or "payment_webhook_event",
            "membership_status": "unknown",
            "membership_status_reason": "expiry_unconfirmed",
        }
        operation = _record_billing_reconcile_outcome(
            db,
            actor_user_id=user.user_id,
            target_pk=order_id,
            request_id=request_id,
            reason_code=payload.reason_code,
            before_snapshot=before_snap,
            after_snapshot=after_snap,
            status="COMPLETED",
        )
        return envelope(data={
            "operation_id": operation.operation_id,
            "reconciled": True,
            "order": {
                "order_id": order.order_id,
                "status": order.status,
                "expected_tier": order.expected_tier,
            },
        })

    grant_already_applied = bool(
        order.granted_tier
        and target_user.tier == order.granted_tier
        and getattr(target_user, "tier_expires_at", None)
    )
    if grant_already_applied:
        order.status = "PAID"
        if hasattr(order, "paid_at") and not order.paid_at:
            order.paid_at = datetime.now(timezone.utc)
        after_snap = {
            "order_status": order.status,
            "order_expected_tier": order.expected_tier,
            "order_granted_tier": order.granted_tier,
            "user_tier": target_user.tier,
            "user_tier_expires_at": target_user.tier_expires_at.isoformat() if getattr(target_user, "tier_expires_at", None) else None,
            "reconciled": True,
            "provider_truth_state": "confirmed",
            "provider_truth_source": provider_truth_source or "payment_webhook_event",
        }
        operation = _record_billing_reconcile_outcome(
            db,
            actor_user_id=user.user_id,
            target_pk=order_id,
            request_id=request_id,
            reason_code=payload.reason_code,
            before_snapshot=before_snap,
            after_snapshot=after_snap,
            status="COMPLETED",
        )
        return envelope(data={
            "operation_id": operation.operation_id,
            "reconciled": True,
            "order": {
                "order_id": order.order_id,
                "status": order.status,
                "expected_tier": order.expected_tier,
            },
        })

    granted_tier, expires_at = grant_membership_order_entitlement(target_user, order)
    order.status = "PAID"
    order.granted_tier = granted_tier
    order.status_reason = None
    if hasattr(order, "paid_at") and not order.paid_at:
        order.paid_at = datetime.now(timezone.utc)

    after_snap = {
        "order_status": "PAID",
        "order_expected_tier": order.expected_tier,
        "order_granted_tier": granted_tier,
        "user_tier": granted_tier,
        "user_tier_expires_at": expires_at.isoformat() if expires_at else None,
        "reconciled": True,
        "provider_truth_state": "confirmed",
        "provider_truth_source": provider_truth_source or "payment_webhook_event",
    }
    operation = _record_billing_reconcile_outcome(
        db,
        actor_user_id=user.user_id,
        target_pk=order_id,
        request_id=request_id,
        reason_code=payload.reason_code,
        before_snapshot=before_snap,
        after_snapshot=after_snap,
        status="COMPLETED",
    )
    return envelope(data={
        "operation_id": operation.operation_id,
        "reconciled": True,
        "order": {
            "order_id": order.order_id,
            "status": order.status,
            "expected_tier": order.expected_tier,
        },
    })


# ---------------------------------------------------------------------------
# GET /admin/users — 用户列表（分页）
# ---------------------------------------------------------------------------
@router.get("/users")
async def admin_users(
    request: Request,
    tier: str | None = Query(None, pattern=r"^(Free|Pro|Enterprise)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    sort: str | None = Query(None),
    db: Session = Depends(get_db),
    _: object = Depends(require_admin),
):
    _ensure_allowed_query_params(request, {"tier", "page", "page_size", "sort"})
    sort_field, descending = _parse_sort_value(sort, allowed={"created_at", "last_login_at"}, default="-created_at")
    where_clauses = []
    if tier:
        where_clauses.append(func.coalesce(User.tier, "Free") == tier)
    order_col = getattr(User, sort_field)
    order_by = order_col.desc() if descending else order_col.asc()
    total = db.scalar(select(func.count()).select_from(User).where(*where_clauses)) or 0
    rows = (
        db.execute(
            select(User)
            .where(*where_clauses)
            .order_by(order_by, User.user_id.asc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        .scalars()
        .all()
    )
    items = []
    for u in rows:
        items.append({
            "user_id": u.user_id,
            "email": u.email or "",
            "role": u.role or "",
            "tier": u.tier or "Free",
            "membership_expires_at": u.tier_expires_at.isoformat() if getattr(u, "tier_expires_at", None) else None,
            "email_verified": bool(getattr(u, "email_verified", False)),
            "last_login_at": u.last_login_at.isoformat() if getattr(u, "last_login_at", None) else None,
            "created_at": u.created_at.isoformat() if getattr(u, "created_at", None) else None,
        })
    return envelope(data={
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    })


# ---------------------------------------------------------------------------
# GET /admin/reports — 研报列表（管理员，支持按 review_flag 筛选）P1-17
# ---------------------------------------------------------------------------
@router.get("/reports")
async def admin_reports(
    request: Request,
    review_flag: str | None = Query(None, pattern=r"^(NONE|PENDING_REVIEW|APPROVED|REJECTED)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    sort: str | None = Query(None),
    db: Session = Depends(get_db),
    _: object = Depends(require_admin),
):
    _ensure_allowed_query_params(request, {"review_flag", "page", "page_size", "sort"})
    sort_field, descending = _parse_sort_value(sort, allowed={"trade_date", "confidence", "review_flag"}, default="-trade_date")
    where_clauses = [Report.is_deleted == False]  # noqa: E712
    if review_flag:
        where_clauses.append(Report.review_flag == review_flag)
    order_col = getattr(Report, sort_field)
    order_by = order_col.desc() if descending else order_col.asc()
    total = db.scalar(
        select(func.count()).select_from(Report.__table__).where(*where_clauses)
    ) or 0
    rows = db.execute(
        select(Report).where(*where_clauses)
        .order_by(order_by, Report.report_id.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    ).scalars().all()
    items = [
        {
            "report_id": r.report_id,
            "stock_code": r.stock_code,
            "trade_date": str(r.trade_date) if r.trade_date else None,
            "recommendation": r.recommendation,
            "confidence": r.confidence,
            "strategy_type": r.strategy_type,
            "quality_flag": r.quality_flag,
            "review_flag": r.review_flag,
            "published": bool(r.published),
            "publish_status": r.publish_status,
            "negative_feedback_count": int(r.negative_feedback_count or 0),
            "status_reason": r.status_reason,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]
    return envelope(data={"items": items, "total": total, "page": page, "page_size": page_size})


# ---------------------------------------------------------------------------
# POST /admin/dag/retrigger — 重触发 DAG 节点（管理员手动修复用）
# ---------------------------------------------------------------------------
ALLOWED_DAG_TASKS = {
    "fr01_stock_pool", "fr04_data_collect", "fr05_market_state",
    "fr05_non_report_truth_materialize",
    "fr06_report_gen", "fr07_settlement", "fr08_sim_trade", "fr13_event_notify",
}


# ---------------------------------------------------------------------------
# PATCH /admin/users/{user_id} — 修改用户角色/等级/验证状态  FR12-ADMIN-07
# ---------------------------------------------------------------------------
class UserPatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: str | None = Field(default=None, pattern=r"^(user|admin|super_admin)$")
    tier: str | None = Field(default=None, pattern=r"^(Free|Pro|Enterprise)$")
    email_verified: bool | None = None
    reason_code: str = Field(default="admin_manual_edit", min_length=1)


@router.patch("/users/{user_id}")
async def admin_patch_user(
    user_id: str,
    payload: UserPatchRequest,
    request: Request,
    actor=Depends(require_admin),
    db: Session = Depends(get_db),
):
    _ensure_allowed_query_params(request, set())
    request_id = _request_id()
    target = db.get(User, user_id)
    if not target:
        raise HTTPException(status_code=404, detail="NOT_FOUND")

    actor_role = getattr(actor, "role", "")
    target_role = getattr(target, "role", "")
    is_super = actor_role == "super_admin"

    # super_admin role cannot be assigned via PATCH API
    if payload.role == "super_admin":
        _record_admin_precheck_rejection(
            db,
            actor_user_id=actor.user_id,
            action_type="PATCH_USER",
            target_table="app_user",
            target_pk=user_id,
            request_id=request_id,
            status_reason="invalid_role",
            error_code="INVALID_PAYLOAD",
        )
        raise HTTPException(status_code=422, detail="INVALID_PAYLOAD")

    # Non-super_admin cannot modify super_admin targets
    if target_role == "super_admin" and not is_super:
        _record_admin_precheck_rejection(
            db,
            actor_user_id=actor.user_id,
            action_type="PATCH_USER",
            target_table="app_user",
            target_pk=user_id,
            request_id=request_id,
            status_reason="protected_super_admin_target",
            error_code="FORBIDDEN",
        )
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    # Non-super_admin cannot change role of admin targets
    if target_role == "admin" and payload.role is not None and payload.role != target_role and not is_super:
        _record_admin_precheck_rejection(
            db,
            actor_user_id=actor.user_id,
            action_type="PATCH_USER",
            target_table="app_user",
            target_pk=user_id,
            request_id=request_id,
            status_reason="role_change_requires_super_admin",
            error_code="FORBIDDEN",
        )
        raise HTTPException(status_code=403, detail="FORBIDDEN")

    before_snap = {
        "role": target.role,
        "tier": target.tier,
        "email_verified": bool(getattr(target, "email_verified", False)),
    }

    changed = False
    if payload.role is not None and payload.role != target.role:
        target.role = payload.role
        changed = True
    if payload.tier is not None and payload.tier != (target.tier or "Free"):
        target.tier = payload.tier
        changed = True
    if payload.email_verified is not None and payload.email_verified != bool(getattr(target, "email_verified", False)):
        target.email_verified = payload.email_verified
        changed = True

    if not changed:
        return envelope(data={"user_id": user_id, "changed": False})

    after_snap = {
        "role": target.role,
        "tier": target.tier,
        "email_verified": bool(getattr(target, "email_verified", False)),
    }
    request_id = _request_id()
    create_admin_operation(
        db,
        action_type="PATCH_USER",
        actor_user_id=actor.user_id,
        target_table="app_user",
        target_pk=user_id,
        request_id=request_id,
        status="COMPLETED",
        reason_code=payload.reason_code,
        before_snapshot=before_snap,
        after_snapshot=after_snap,
    )
    create_audit_log(
        db,
        actor_user_id=actor.user_id,
        action_type="PATCH_USER",
        target_table="app_user",
        target_pk=user_id,
        request_id=request_id,
        reason_code=payload.reason_code,
        before_snapshot=before_snap,
        after_snapshot=after_snap,
    )
    db.commit()
    return envelope(data={
        "user_id": user_id,
        "changed": True,
        "role": target.role,
        "tier": target.tier,
        "email_verified": bool(getattr(target, "email_verified", False)),
    })


class DagRetriggerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_name: str = Field(..., min_length=1)
    trade_date: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    reason_code: str = Field(default="admin_manual_retrigger", min_length=1)


@router.post("/dag/retrigger", status_code=410)
async def admin_dag_retrigger(
    payload: DagRetriggerRequest,
    request: Request,
    user=Depends(require_admin),
    db: Session = Depends(get_db),
):
    del payload, request, user, db
    raise HTTPException(status_code=410, detail="ROUTE_RETIRED")


# ---------------------------------------------------------------------------
# FR03-COOKIE: Cookie 会话管理 CRUD 端点
# ---------------------------------------------------------------------------

@router.get("/cookie-sessions")
async def admin_cookie_session_list(
    user=Depends(require_admin),
    db: Session = Depends(get_db),
):
    """列出所有 Cookie 会话（FR03-COOKIE-01）。"""
    del user
    table = Base.metadata.tables["cookie_session"]
    rows = db.execute(table.select().order_by(table.c.updated_at.desc())).mappings().all()
    def _iso(v):
        return v.isoformat() if v is not None else None

    items = [
        {
            "cookie_session_id": str(r["cookie_session_id"]),
            "provider": r["provider"],
            "account_key": r.get("account_key"),
            "status": r["status"],
            "expires_at": _iso(r.get("expires_at")),
            "last_probe_at": _iso(r.get("last_probe_at")),
            "cookie_present": bool(r.get("cookie_blob")),
        }
        for r in rows
    ]
    return envelope(data={"total": len(items), "items": items})


@router.post("/cookie-sessions")
async def admin_cookie_session_create(
    payload: CookieSessionUpsertRequest,
    request: Request,
    user=Depends(require_admin),
    db: Session = Depends(get_db),
):
    """创建/更新 Cookie 会话（FR03-COOKIE-02）。"""
    result = upsert_cookie_session(
        db,
        login_source=payload.login_source,
        cookie_string=payload.cookie_string,
    )
    db.commit()
    create_audit_log(
        db,
        actor_user_id=str(getattr(user, "user_id", "admin")),
        action_type="cookie_session_upsert",
        target_table="cookie_session",
        target_pk=str(result.get("cookie_session_id", "")),
        request_id=require_request_id(),
    )
    db.commit()
    return envelope(data=result)


@router.get("/cookie-sessions/{session_id}")
async def admin_cookie_session_detail(
    session_id: str,
    user=Depends(require_admin),
    db: Session = Depends(get_db),
):
    """获取单个 Cookie 会话详情（FR03-COOKIE-03）。"""
    del user
    from app.services.cookie_session_ssot import _cookie_snapshot
    table = Base.metadata.tables["cookie_session"]
    row = db.execute(
        table.select().where(table.c.cookie_session_id == session_id)
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="COOKIE_SESSION_NOT_FOUND")
    snapshot = _cookie_snapshot(row)
    snapshot["cookie_session_id"] = session_id
    return envelope(data=snapshot)


@router.delete("/cookie-sessions/{session_id}")
async def admin_cookie_session_delete(
    session_id: str,
    request: Request,
    user=Depends(require_admin),
    db: Session = Depends(get_db),
):
    """删除 Cookie 会话（FR03-COOKIE-04）。"""
    table = Base.metadata.tables["cookie_session"]
    result = db.execute(table.delete().where(table.c.cookie_session_id == session_id))
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="COOKIE_SESSION_NOT_FOUND")
    create_audit_log(
        db,
        actor_user_id=str(getattr(user, "user_id", "admin")),
        action_type="cookie_session_delete",
        target_table="cookie_session",
        target_pk=session_id,
        request_id=require_request_id(),
    )
    db.commit()
    return envelope(data={"deleted": session_id})
