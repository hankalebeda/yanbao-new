from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import uuid4

from app.core.request_context import resolve_record_request_id
from app.models import AdminOperation, AuditLog
from app.services.notification import dispatch_nfr13_alert
from sqlalchemy import text

logger = logging.getLogger(__name__)

_ADMIN_OPERATION_ACTION_TYPES = (
    "PATCH_USER",
    "PATCH_REPORT",
    "FORCE_REGENERATE",
    "RECONCILE_ORDER",
    "RUN_SETTLEMENT",
    "UPSERT_COOKIE_SESSION",
    "POOL_REFRESH",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_admin_operation_action_type_support(db) -> None:
    if db.bind.dialect.name != "sqlite":
        return

    create_sql = db.execute(
        text("SELECT sql FROM sqlite_master WHERE type='table' AND name='admin_operation'")
    ).scalar()
    if not create_sql:
        return
    create_sql = str(create_sql)
    if "POOL_REFRESH" in create_sql:
        return
    if "ck_admin_operation_action_type_enum" not in create_sql:
        return

    index_rows = db.execute(
        text(
            """
            SELECT name, sql
            FROM sqlite_master
            WHERE type = 'index'
              AND tbl_name = 'admin_operation'
              AND sql IS NOT NULL
            ORDER BY name ASC
            """
        )
    ).mappings().all()
    column_rows = db.execute(text("PRAGMA table_info(admin_operation)")).mappings().all()
    column_names = [str(row.get("name") or "") for row in column_rows if str(row.get("name") or "")]
    if not column_names:
        return

    old_constraint = (
        "CONSTRAINT ck_admin_operation_action_type_enum CHECK "
        "(action_type IN ('PATCH_USER', 'PATCH_REPORT', 'FORCE_REGENERATE', 'RECONCILE_ORDER', 'RUN_SETTLEMENT', 'UPSERT_COOKIE_SESSION'))"
    )
    new_constraint = (
        "CONSTRAINT ck_admin_operation_action_type_enum CHECK "
        "(action_type IN ('PATCH_USER', 'PATCH_REPORT', 'FORCE_REGENERATE', 'RECONCILE_ORDER', 'RUN_SETTLEMENT', 'UPSERT_COOKIE_SESSION', 'POOL_REFRESH'))"
    )
    temp_name = f"admin_operation_repair_{uuid4().hex[:8]}"
    quoted_cols = ", ".join(f'"{name}"' for name in column_names)
    repaired_sql = create_sql.replace('CREATE TABLE admin_operation', f'CREATE TABLE "{temp_name}"', 1)
    repaired_sql = repaired_sql.replace('CREATE TABLE "admin_operation"', f'CREATE TABLE "{temp_name}"', 1)
    repaired_sql = repaired_sql.replace(old_constraint, new_constraint, 1)

    db.commit()
    db.execute(text("PRAGMA foreign_keys=OFF"))
    db.execute(text(repaired_sql))
    db.execute(text(f'INSERT INTO "{temp_name}" ({quoted_cols}) SELECT {quoted_cols} FROM admin_operation'))
    db.execute(text("DROP TABLE admin_operation"))
    db.execute(text(f'ALTER TABLE "{temp_name}" RENAME TO admin_operation'))
    for row in index_rows:
        sql = str(row.get("sql") or "").strip()
        if sql:
            db.execute(text(sql))
    db.execute(text("PRAGMA foreign_keys=ON"))
    db.commit()


def _admin_operation_alert_type(*, action_type: str, status: str) -> str:
    if status == "REJECTED" and action_type == "FORCE_REGENERATE":
        return "FORCE_REGENERATE_BLOCKED"
    return "ADMIN_OP_FAILED"


def _emit_admin_operation_alert(
    *,
    action_type: str,
    actor_user_id: str,
    target_table: str,
    target_pk: str,
    request_id: str,
    status: str,
    reason_code: str | None,
    failure_category: str | None,
    status_reason: str | None,
    after_snapshot: dict | None,
) -> None:
    if status not in {"FAILED", "REJECTED"}:
        return

    alert_type = _admin_operation_alert_type(action_type=action_type, status=status)
    extra = {
        "action_type": action_type,
        "admin_operation_status": status,
        "actor_user_id": str(actor_user_id),
        "target_table": target_table,
        "target_pk": target_pk,
        "request_id": request_id,
    }
    if reason_code:
        extra["reason_code"] = reason_code
    if failure_category:
        extra["failure_category"] = failure_category
    if status_reason:
        extra["status_reason"] = status_reason
    if isinstance(after_snapshot, dict):
        error_code = after_snapshot.get("error_code")
        if error_code:
            extra["error_code"] = str(error_code)

    try:
        dispatch_nfr13_alert(
            alert_type=alert_type,
            fr_id="FR-12",
            message=f"admin operation {action_type} {status.lower()} target={target_table}:{target_pk}",
            extra=extra,
        )
    except Exception:
        logger.exception(
            "admin_operation_alert_dispatch_failed action_type=%s target=%s:%s request_id=%s",
            action_type,
            target_table,
            target_pk,
            request_id,
        )


def create_admin_operation(
    db,
    *,
    action_type: str,
    actor_user_id: str,
    target_table: str,
    target_pk: str,
    request_id: str | None,
    status: str,
    before_snapshot: dict | None = None,
    after_snapshot: dict | None = None,
    reason_code: str | None = None,
    failure_category: str | None = None,
    status_reason: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
):
    _ensure_admin_operation_action_type_support(db)
    resolved_request_id = resolve_record_request_id(request_id)
    operation = AdminOperation(
        operation_id=str(uuid4()),
        action_type=action_type,
        actor_user_id=actor_user_id,
        target_table=target_table,
        target_pk=target_pk,
        status=status,
        reason_code=reason_code,
        failure_category=failure_category,
        request_id=resolved_request_id,
        before_snapshot=before_snapshot,
        after_snapshot=after_snapshot,
        status_reason=status_reason,
        started_at=started_at,
        finished_at=finished_at,
        created_at=utc_now(),
    )
    db.add(operation)
    db.flush()
    _emit_admin_operation_alert(
        action_type=action_type,
        actor_user_id=actor_user_id,
        target_table=target_table,
        target_pk=target_pk,
        request_id=resolved_request_id,
        status=status,
        reason_code=reason_code,
        failure_category=failure_category,
        status_reason=status_reason,
        after_snapshot=after_snapshot,
    )
    return operation


def create_audit_log(
    db,
    *,
    actor_user_id: str,
    action_type: str,
    target_table: str,
    target_pk: str,
    request_id: str | None,
    operation_id: str | None = None,
    reason_code: str | None = None,
    failure_category: str | None = None,
    before_snapshot: dict | None = None,
    after_snapshot: dict | None = None,
):
    resolved_request_id = resolve_record_request_id(request_id)
    audit = AuditLog(
        audit_log_id=str(uuid4()),
        operation_id=operation_id,
        actor_user_id=actor_user_id,
        action_type=action_type,
        target_table=target_table,
        target_pk=target_pk,
        request_id=resolved_request_id,
        reason_code=reason_code,
        failure_category=failure_category,
        before_snapshot=before_snapshot,
        after_snapshot=after_snapshot,
        created_at=utc_now(),
    )
    db.add(audit)
    db.flush()
    return audit


def create_rejected_admin_artifacts(
    db,
    *,
    actor_user_id: str,
    action_type: str,
    target_table: str,
    target_pk: str,
    request_id: str | None,
    status_reason: str,
    error_code: str,
    reason_code: str | None = None,
    failure_category: str | None = None,
    before_snapshot: dict | None = None,
    after_snapshot: dict | None = None,
    audit_action_type: str | None = None,
):
    resolved_failure = failure_category or status_reason
    merged_after_snapshot = {
        "status": "REJECTED",
        "error_code": error_code,
        "status_reason": status_reason,
    }
    if after_snapshot:
        merged_after_snapshot.update(after_snapshot)

    operation = create_admin_operation(
        db,
        action_type=action_type,
        actor_user_id=actor_user_id,
        target_table=target_table,
        target_pk=target_pk,
        request_id=request_id,
        status="REJECTED",
        reason_code=reason_code,
        before_snapshot=before_snapshot,
        after_snapshot=merged_after_snapshot,
        failure_category=resolved_failure,
        status_reason=status_reason,
        started_at=utc_now(),
        finished_at=utc_now(),
    )
    create_audit_log(
        db,
        actor_user_id=actor_user_id,
        action_type=audit_action_type or action_type,
        target_table=target_table,
        target_pk=target_pk,
        request_id=request_id,
        operation_id=operation.operation_id,
        reason_code=reason_code,
        failure_category=resolved_failure,
        before_snapshot=before_snapshot,
        after_snapshot=merged_after_snapshot,
    )
    return operation
