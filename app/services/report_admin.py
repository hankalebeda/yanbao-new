from __future__ import annotations

from datetime import date, datetime

from sqlalchemy.orm import Session

from app.models import Report
from app.services.admin_audit import create_admin_operation, create_audit_log, utc_now

ALLOWED_REVIEW_FLAGS = {"NONE", "PENDING_REVIEW", "APPROVED", "REJECTED"}
ALLOWED_PATCH_FIELDS = {"review_flag", "published"}
FORBIDDEN_CONTENT_FIELDS = {"conclusion_text", "citations", "instruction_card", "reasoning_chain"}


class ReportPatchValidationError(ValueError):
    pass


def _iso_date(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def report_admin_summary(report: Report) -> dict:
    return {
        "report_id": report.report_id,
        "stock_code": report.stock_code,
        "trade_date": _iso_date(report.trade_date),
        "recommendation": report.recommendation,
        "confidence": float(report.confidence) if report.confidence is not None else None,
        "strategy_type": report.strategy_type,
        "quality_flag": report.quality_flag,
        "published": bool(report.published),
        "publish_status": report.publish_status,
        "review_flag": report.review_flag,
        "negative_feedback_count": int(report.negative_feedback_count or 0),
        "status_reason": report.status_reason,
    }


def validate_report_patch_payload(payload: object) -> dict:
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ReportPatchValidationError("payload_must_be_object")
    unknown_keys = set(payload) - ALLOWED_PATCH_FIELDS
    if unknown_keys:
        raise ReportPatchValidationError("unknown_fields")

    review_flag = payload.get("review_flag")
    if review_flag is not None and review_flag not in ALLOWED_REVIEW_FLAGS:
        raise ReportPatchValidationError("invalid_review_flag")

    published = payload.get("published")
    if published is not None and not isinstance(published, bool):
        raise ReportPatchValidationError("invalid_published")

    return payload


def record_report_patch_rejection(
    db: Session,
    *,
    actor_user_id: str,
    report: Report,
    request_id: str,
    payload: dict | None,
    status_reason: str,
) -> None:
    before_snapshot = report_admin_summary(report)
    operation = create_admin_operation(
        db,
        action_type="PATCH_REPORT",
        actor_user_id=actor_user_id,
        target_table="report",
        target_pk=report.report_id,
        request_id=request_id,
        status="REJECTED",
        before_snapshot=before_snapshot,
        after_snapshot=before_snapshot,
        status_reason=status_reason,
        started_at=utc_now(),
        finished_at=utc_now(),
    )
    create_audit_log(
        db,
        actor_user_id=actor_user_id,
        action_type="PUBLISH_WRITE_REJECTED",
        target_table="report",
        target_pk=report.report_id,
        request_id=request_id,
        operation_id=operation.operation_id,
        before_snapshot=before_snapshot,
        after_snapshot={"attempted_payload": payload or {}},
    )
    db.flush()


def patch_report_admin_fields(
    db: Session,
    *,
    actor_user_id: str,
    report: Report,
    payload: dict,
    request_id: str,
) -> dict:
    before_snapshot = report_admin_summary(report)
    now = utc_now()

    if "review_flag" in payload:
        report.review_flag = payload["review_flag"]
    if "published" in payload:
        new_published = payload["published"]
        old_published = bool(before_snapshot["published"])
        report.published = new_published
        if new_published:
            report.publish_status = "PUBLISHED"
            if report.published_at is None:
                report.published_at = now
        elif old_published:
            report.publish_status = "UNPUBLISHED"

    db.flush()
    after_snapshot = report_admin_summary(report)

    # 同值 PATCH：before == after，可返回 200 但不得重复写审计（P1-18 / NFR-19）
    if before_snapshot == after_snapshot:
        return after_snapshot

    operation = create_admin_operation(
        db,
        action_type="PATCH_REPORT",
        actor_user_id=actor_user_id,
        target_table="report",
        target_pk=report.report_id,
        request_id=request_id,
        status="EXECUTING",
        before_snapshot=before_snapshot,
        started_at=now,
    )
    operation.status = "COMPLETED"
    operation.after_snapshot = after_snapshot
    operation.finished_at = utc_now()

    if before_snapshot["published"] is False and after_snapshot["published"] is True:
        audit_action = "PUBLISH"
    elif before_snapshot["published"] is True and after_snapshot["published"] is False:
        audit_action = "UNPUBLISH"
    else:
        audit_action = "PATCH_REPORT"

    create_audit_log(
        db,
        actor_user_id=actor_user_id,
        action_type=audit_action,
        target_table="report",
        target_pk=report.report_id,
        request_id=request_id,
        operation_id=operation.operation_id,
        before_snapshot=before_snapshot,
        after_snapshot=after_snapshot,
    )
    db.flush()
    return after_snapshot
