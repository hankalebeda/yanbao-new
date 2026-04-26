from __future__ import annotations

import logging
from contextlib import nullcontext
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models import Base, Report
from app.services.notification import attempt_business_event_delivery

logger = logging.getLogger(__name__)

_feedback_lock_guard = Lock()
_feedback_locks: dict[str, Lock] = {}


class FeedbackServiceError(Exception):
    def __init__(self, status_code: int, error_code: str):
        super().__init__(error_code)
        self.status_code = status_code
        self.error_code = error_code


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime | None) -> datetime:
    current = value or _utc_now()
    if current.tzinfo is None:
        return current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def _feedback_lock(user_id: str, current_time: datetime):
    lock_key = f"{user_id}:{current_time.date().isoformat()}"
    with _feedback_lock_guard:
        return _feedback_locks.setdefault(lock_key, Lock())


def _begin_immediate_if_sqlite(db: Session) -> None:
    bind = db.get_bind()
    if bind is not None and bind.dialect.name == "sqlite":
        db.connection().exec_driver_sql("BEGIN IMMEDIATE")


def _day_window(current_time: datetime) -> tuple[datetime, datetime]:
    start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def _report_not_available(report: Report | None) -> bool:
    return report is None or not bool(report.published) or bool(report.is_deleted)


def _review_projection_key(report: Report) -> str:
    trade_date = str(report.trade_date) if report.trade_date else "unknown"
    return f"REPORT_PENDING_REVIEW:{report.stock_code}:{trade_date}"


def _enqueue_report_pending_review_event(
    db: Session,
    *,
    report: Report,
    negative_count: int,
    current_time: datetime,
) -> str:
    cursor_table = Base.metadata.tables["event_projection_cursor"]
    business_event_table = Base.metadata.tables["business_event"]
    outbox_table = Base.metadata.tables["outbox_event"]
    projection_key = _review_projection_key(report)

    cursor_row = db.execute(
        select(cursor_table).where(
            cursor_table.c.event_type == "REPORT_PENDING_REVIEW",
            cursor_table.c.event_projection_key == projection_key,
        )
    ).mappings().first()

    projection_cursor_id = cursor_row["projection_cursor_id"] if cursor_row else str(uuid4())
    business_event_id = str(uuid4())
    payload = {
        "report_id": report.report_id,
        "stock_code": report.stock_code,
        "trade_date": str(report.trade_date) if report.trade_date else None,
        "negative_count": negative_count,
        "review_flag": "PENDING_REVIEW",
        "triggered_at": current_time.isoformat(),
    }

    if cursor_row:
        db.execute(
            cursor_table.update()
            .where(cursor_table.c.projection_cursor_id == projection_cursor_id)
            .values(
                last_business_event_id=business_event_id,
                updated_at=current_time,
            )
        )
    else:
        db.execute(
            cursor_table.insert().values(
                projection_cursor_id=projection_cursor_id,
                event_type="REPORT_PENDING_REVIEW",
                event_projection_key=projection_key,
                last_business_event_id=business_event_id,
                last_sent_at=None,
                dedup_until=None,
                last_state_value=None,
                recovered_at=None,
                created_at=current_time,
                updated_at=current_time,
            )
        )

    db.execute(
        business_event_table.insert().values(
            business_event_id=business_event_id,
            event_type="REPORT_PENDING_REVIEW",
            projection_cursor_id=projection_cursor_id,
            event_projection_key=projection_key,
            event_status="ENQUEUED",
            source_table="report",
            source_pk=report.report_id,
            stock_code=report.stock_code,
            trade_date=date.fromisoformat(str(report.trade_date)) if report.trade_date else None,
            capital_tier=None,
            payload_json=payload,
            dedup_until=None,
            status_reason=None,
            created_at=current_time,
            enqueued_at=current_time,
        )
    )
    db.execute(
        outbox_table.insert().values(
            outbox_event_id=str(uuid4()),
            business_event_id=business_event_id,
            dispatch_status="PENDING",
            claim_token=None,
            claimed_at=None,
            claimed_by=None,
            dispatch_attempt_count=0,
            next_retry_at=None,
            payload_json=payload,
            status_reason=None,
            created_at=current_time,
            updated_at=current_time,
            dispatched_at=None,
        )
    )
    return business_event_id


def _mark_outbox_dispatch_failed(db: Session, business_event_id: str, *, current_time: datetime, status_reason: str) -> None:
    outbox_table = Base.metadata.tables["outbox_event"]
    db.execute(
        outbox_table.update()
        .where(outbox_table.c.business_event_id == business_event_id)
        .values(
            dispatch_status="DISPATCH_FAILED",
            dispatch_attempt_count=outbox_table.c.dispatch_attempt_count + 1,
            status_reason=status_reason,
            updated_at=current_time,
        )
    )
    db.commit()


def _dispatch_review_notification(db: Session, business_event_id: str, *, current_time: datetime) -> None:
    cursor_table = Base.metadata.tables["event_projection_cursor"]
    business_event_table = Base.metadata.tables["business_event"]
    outbox_table = Base.metadata.tables["outbox_event"]
    notification_table = Base.metadata.tables["notification"]

    event_row = db.execute(
        select(
            business_event_table.c.business_event_id,
            business_event_table.c.event_type,
            business_event_table.c.projection_cursor_id,
            business_event_table.c.stock_code,
            business_event_table.c.trade_date,
            business_event_table.c.payload_json,
            outbox_table.c.outbox_event_id,
            outbox_table.c.dispatch_attempt_count,
        )
        .join(outbox_table, outbox_table.c.business_event_id == business_event_table.c.business_event_id)
        .where(business_event_table.c.business_event_id == business_event_id)
    ).mappings().first()
    if event_row is None:
        return

    claim_token = str(uuid4())
    claimed = db.execute(
        outbox_table.update()
        .where(
            outbox_table.c.outbox_event_id == event_row["outbox_event_id"],
            outbox_table.c.dispatch_status == "PENDING",
        )
        .values(
            dispatch_status="DISPATCHING",
            claim_token=claim_token,
            claimed_at=current_time,
            claimed_by="feedback_ssot",
            updated_at=current_time,
            status_reason=None,
        )
    )
    if not claimed.rowcount:
        return

    existing_notification = db.execute(
        select(
            notification_table.c.notification_id,
            notification_table.c.status,
            notification_table.c.status_reason,
            notification_table.c.sent_at,
        ).where(
            notification_table.c.business_event_id == business_event_id,
            notification_table.c.channel == "webhook",
            notification_table.c.recipient_key == "admin_global",
        )
        .order_by(notification_table.c.created_at.desc(), notification_table.c.notification_id.desc())
    ).mappings().first()
    if existing_notification and existing_notification["status"] in {"sent", "skipped"}:
        db.execute(
            outbox_table.update()
            .where(
                outbox_table.c.outbox_event_id == event_row["outbox_event_id"],
                outbox_table.c.claim_token == claim_token,
            )
            .values(
                dispatch_status="DISPATCHED",
                dispatch_attempt_count=event_row["dispatch_attempt_count"] + 1,
                updated_at=current_time,
                dispatched_at=current_time,
                status_reason=existing_notification["status_reason"],
            )
        )
        if existing_notification["status"] == "sent":
            db.execute(
                cursor_table.update()
                .where(cursor_table.c.projection_cursor_id == event_row["projection_cursor_id"])
                .values(
                    last_sent_at=existing_notification["sent_at"] or current_time,
                    updated_at=current_time,
                )
            )
        db.commit()
        return

    payload = event_row["payload_json"] or {}
    notification_status, status_reason = attempt_business_event_delivery(
        event_type=event_row["event_type"],
        payload=payload,
        recipient_scope="admin",
    )
    summary = (
        f"report pending review stock={event_row['stock_code']} "
        f"trade_date={event_row['trade_date']} negative_count={payload.get('negative_count')}"
    )

    sent_at = current_time if notification_status == "sent" else None
    if existing_notification:
        db.execute(
            notification_table.update()
            .where(notification_table.c.notification_id == existing_notification["notification_id"])
            .values(
                triggered_at=current_time,
                status=notification_status,
                payload_summary=summary,
                status_reason=status_reason,
                sent_at=sent_at,
            )
        )
    else:
        db.execute(
            notification_table.insert().values(
                notification_id=str(uuid4()),
                business_event_id=business_event_id,
                event_type=event_row["event_type"],
                channel="webhook",
                recipient_scope="admin",
                recipient_key="admin_global",
                recipient_user_id=None,
                triggered_at=current_time,
                status=notification_status,
                payload_summary=summary,
                status_reason=status_reason,
                sent_at=sent_at,
                created_at=current_time,
            )
        )
    outbox_values = {
        "dispatch_attempt_count": event_row["dispatch_attempt_count"] + 1,
        "status_reason": status_reason,
        "updated_at": current_time,
    }
    if notification_status == "failed":
        outbox_values.update(
            dispatch_status="DISPATCH_FAILED",
            next_retry_at=current_time + timedelta(seconds=max(1, int(getattr(settings, "outbox_dispatch_claim_timeout_seconds", 5)))),
        )
    else:
        outbox_values.update(
            dispatch_status="DISPATCHED",
            dispatched_at=current_time,
            next_retry_at=None,
        )

    db.execute(
        outbox_table.update()
        .where(
            outbox_table.c.outbox_event_id == event_row["outbox_event_id"],
            outbox_table.c.claim_token == claim_token,
        )
        .values(**outbox_values)
    )
    if notification_status == "sent":
        db.execute(
            cursor_table.update()
            .where(cursor_table.c.projection_cursor_id == event_row["projection_cursor_id"])
            .values(last_sent_at=current_time, updated_at=current_time)
        )
    db.commit()


def submit_report_feedback(
    db: Session,
    *,
    path_report_id: str,
    report_id: str,
    user_id: str,
    feedback_type: str,
    now: datetime | None = None,
) -> dict:
    current_time = _ensure_utc(now)
    if path_report_id != report_id:
        raise FeedbackServiceError(422, "INVALID_PAYLOAD")
    if feedback_type not in {"positive", "negative"}:
        raise FeedbackServiceError(422, "INVALID_PAYLOAD")

    feedback_table = Base.metadata.tables["report_feedback"]
    business_event_id: str | None = None
    lock_context = _feedback_lock(user_id, current_time) if feedback_type == "negative" else nullcontext()

    with lock_context:
        try:
            _begin_immediate_if_sqlite(db)

            report = db.get(Report, report_id)
            if _report_not_available(report):
                raise FeedbackServiceError(404, "REPORT_NOT_AVAILABLE")

            review_flag = report.review_flag or "NONE"
            negative_count = int(report.negative_feedback_count or 0)

            if feedback_type == "negative":
                existing_negative = db.execute(
                    select(feedback_table.c.feedback_id).where(
                        feedback_table.c.report_id == report_id,
                        feedback_table.c.user_id == user_id,
                        feedback_table.c.feedback_type == "negative",
                    )
                ).mappings().first()
                if existing_negative:
                    db.rollback()
                    return {
                        "feedback_id": existing_negative["feedback_id"],
                        "report_id": report_id,
                        "feedback_type": "negative",
                        "negative_count": negative_count,
                        "review_flag": review_flag,
                        "is_duplicate_negative": True,
                        "review_event_enqueued": False,
                    }

                day_start, day_end = _day_window(current_time)
                today_negative_count = int(
                    db.execute(
                        select(func.count()).select_from(feedback_table).where(
                            feedback_table.c.user_id == user_id,
                            feedback_table.c.feedback_type == "negative",
                            feedback_table.c.created_at >= day_start,
                            feedback_table.c.created_at < day_end,
                        )
                    ).scalar_one()
                )
                if today_negative_count >= int(settings.feedback_negative_daily_limit):
                    raise FeedbackServiceError(429, "RATE_LIMITED")

            feedback_id = str(uuid4())
            db.execute(
                feedback_table.insert().values(
                    feedback_id=feedback_id,
                    report_id=report_id,
                    user_id=user_id,
                    feedback_type=feedback_type,
                    created_at=current_time,
                )
            )

            is_duplicate_negative = False
            review_event_enqueued = False
            if feedback_type == "negative":
                negative_count += 1
                report.negative_feedback_count = negative_count
                if review_flag == "NONE" and negative_count >= 3:
                    report.review_flag = "PENDING_REVIEW"
                    review_flag = "PENDING_REVIEW"
                    business_event_id = _enqueue_report_pending_review_event(
                        db,
                        report=report,
                        negative_count=negative_count,
                        current_time=current_time,
                    )
                    review_event_enqueued = True
                else:
                    review_flag = report.review_flag or "NONE"

            db.flush()
            db.commit()
        except FeedbackServiceError:
            db.rollback()
            raise
        except OperationalError as exc:
            db.rollback()
            raise FeedbackServiceError(503, "UPSTREAM_TIMEOUT") from exc

    if business_event_id:
        try:
            _dispatch_review_notification(db, business_event_id, current_time=current_time)
        except OperationalError:
            db.rollback()
            _mark_outbox_dispatch_failed(
                db,
                business_event_id,
                current_time=current_time,
                status_reason="outbox_dispatch_timeout",
            )
        except Exception as exc:
            logger.exception("review_notification_dispatch_failed business_event_id=%s err=%s", business_event_id, exc)
            db.rollback()
            _mark_outbox_dispatch_failed(
                db,
                business_event_id,
                current_time=current_time,
                status_reason="outbox_dispatch_failed",
            )

    return {
        "feedback_id": feedback_id,
        "report_id": report_id,
        "feedback_type": feedback_type,
        "negative_count": negative_count,
        "review_flag": review_flag,
        "is_duplicate_negative": is_duplicate_negative,
        "review_event_enqueued": review_event_enqueued,
    }
