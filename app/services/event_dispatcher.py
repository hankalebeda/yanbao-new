"""
FR-13 业务事件推送 — 事件分发器
Handles: POSITION_CLOSED, DELISTED_LIQUIDATED, BUY_SIGNAL_DAILY,
         DRAWDOWN_ALERT, REPORT_PENDING_REVIEW
"""
from __future__ import annotations

import logging
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Base, User
from app.services.membership import user_has_truth_backed_paid_membership
from app.services.notification import attempt_business_event_delivery

logger = logging.getLogger(__name__)

_EVENT_TYPES = frozenset({
    "POSITION_CLOSED",
    "BUY_SIGNAL_DAILY",
    "DRAWDOWN_ALERT",
    "REPORT_PENDING_REVIEW",
})

DRAWDOWN_SUPPRESS_HOURS = 4
BUY_SIGNAL_MAX_PER_DAY = 5
POSITION_CLOSED_TERMINAL_STATES = frozenset({
    "TAKE_PROFIT",
    "STOP_LOSS",
    "TIMEOUT",
    "DELISTED_LIQUIDATED",
})
_PAID_NOTIFICATION_TIERS = frozenset({"Pro", "Enterprise"})


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso_date(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()[:10]
    return str(value)[:10]


def _json_safe(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat") and not isinstance(value, (str, bytes)):
        try:
            return value.isoformat()
        except TypeError:
            pass
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return value


def _user_notification_targets(db: Session, *, current: datetime) -> list[dict[str, object | None]]:
    user_table = Base.metadata.tables.get("app_user")
    if user_table is None:
        user_table = User.__table__
    rows = db.execute(
        select(
            user_table.c.user_id,
            user_table.c.email,
            user_table.c.tier,
            user_table.c.tier_expires_at,
            user_table.c.email_verified,
        )
        .where(user_table.c.tier.in_(tuple(_PAID_NOTIFICATION_TIERS)))
        .order_by(user_table.c.created_at.asc(), user_table.c.user_id.asc())
    ).mappings().all()
    targets: list[dict[str, str | None]] = []
    for row in rows:
        expires_at = _ensure_utc(row.get("tier_expires_at"))
        user_id = str(row["user_id"])
        block_reason = None
        recipient_email = (row.get("email") or "").strip() or None
        if expires_at is None:
            block_reason = "expiry_unconfirmed"
        elif expires_at <= current:
            block_reason = "membership_inactive"
        elif not recipient_email:
            block_reason = "user_email_missing"
        elif not bool(row.get("email_verified")):
            block_reason = "user_email_unverified"
        membership_truth_confirmed = False
        if block_reason is None:
            membership_truth_confirmed = user_has_truth_backed_paid_membership(
                db,
                user_id=user_id,
                tier=row.get("tier"),
            )
        targets.append(
            {
                "channel": "email",
                "recipient_scope": "user",
                "recipient_key": f"user:{user_id}",
                "recipient_user_id": user_id,
                "recipient_address": recipient_email,
                "delivery_block_reason": block_reason,
                "membership_truth_confirmed": membership_truth_confirmed,
            }
        )
    return targets


def _dispatch_targets_for_event(
    db: Session,
    *,
    event_type: str,
    current: datetime,
) -> list[dict[str, object | None]]:
    if event_type in ("DRAWDOWN_ALERT", "REPORT_PENDING_REVIEW"):
        return [
            {
                "channel": "webhook",
                "recipient_scope": "admin",
                "recipient_key": "admin_global",
                "recipient_user_id": None,
            }
        ]
    if event_type in ("BUY_SIGNAL_DAILY", "POSITION_CLOSED"):
        return _user_notification_targets(db, current=current)
    return [
        {
            "channel": "webhook",
            "recipient_scope": "admin",
            "recipient_key": "admin_global",
            "recipient_user_id": None,
        }
    ]


def _insert_or_reuse_notification(
    db: Session,
    *,
    notification_table,
    business_event_id: str,
    event_type: str,
    channel: str,
    recipient_scope: str,
    recipient_key: str,
    recipient_user_id: str | None,
    triggered_at: datetime,
    status: str,
    payload_summary: str,
    status_reason: str | None,
):
    existing = db.execute(
        select(
            notification_table.c.notification_id,
            notification_table.c.status,
            notification_table.c.status_reason,
            notification_table.c.sent_at,
        )
        .where(notification_table.c.business_event_id == business_event_id)
        .where(notification_table.c.channel == channel)
        .where(notification_table.c.recipient_key == recipient_key)
        .order_by(notification_table.c.created_at.desc(), notification_table.c.notification_id.desc())
    ).mappings().first()
    if existing is not None and existing["status"] in {"sent", "skipped"}:
        return dict(existing)

    sent_at = triggered_at if status == "sent" else None
    if existing is not None:
        db.execute(
            notification_table.update()
            .where(notification_table.c.notification_id == existing["notification_id"])
            .values(
                triggered_at=triggered_at,
                status=status,
                payload_summary=payload_summary[:500],
                status_reason=status_reason,
                sent_at=sent_at,
            )
        )
        return {
            "notification_id": existing["notification_id"],
            "status": status,
            "status_reason": status_reason,
            "sent_at": sent_at,
        }

    notification_id = str(uuid4())
    db.execute(
        notification_table.insert().values(
            notification_id=notification_id,
            business_event_id=business_event_id,
            event_type=event_type,
            channel=channel,
            recipient_scope=recipient_scope,
            recipient_key=recipient_key,
            recipient_user_id=recipient_user_id,
            triggered_at=triggered_at,
            status=status,
            payload_summary=payload_summary[:500],
            status_reason=status_reason,
            sent_at=sent_at,
            created_at=triggered_at,
        )
    )
    return {
        "notification_id": notification_id,
        "status": status,
        "status_reason": status_reason,
        "sent_at": sent_at,
    }


def _projection_key(event_type: str, *, stock_code: str | None = None,
                     trade_date: str | None = None, capital_tier: str | None = None,
                     account_id: str | None = None) -> str:
    parts = [event_type]
    if stock_code:
        parts.append(stock_code)
    if trade_date:
        parts.append(trade_date)
    if capital_tier:
        parts.append(capital_tier)
    if account_id:
        parts.append(account_id)
    return ":".join(parts)


def _record_dedup_skipped_event(
    db: Session,
    *,
    business_event_table,
    cursor_table,
    projection_cursor_id: str,
    business_event_id: str,
    event_type: str,
    projection_key: str,
    source_table: str,
    source_pk: str,
    stock_code: str | None,
    trade_date,
    capital_tier: str | None,
    payload: dict | None,
    current: datetime,
    status_reason: str,
    dedup_until=None,
) -> None:
    db.execute(
        business_event_table.insert().values(
            business_event_id=business_event_id,
            event_type=event_type,
            projection_cursor_id=projection_cursor_id,
            event_projection_key=projection_key,
            event_status="DEDUP_SKIPPED",
            source_table=source_table,
            source_pk=source_pk,
            stock_code=stock_code,
            trade_date=trade_date,
            capital_tier=capital_tier,
            payload_json=payload or {},
            dedup_until=dedup_until,
            status_reason=status_reason,
            created_at=current,
            enqueued_at=None,
        )
    )
    db.execute(
        cursor_table.update()
        .where(cursor_table.c.projection_cursor_id == projection_cursor_id)
        .values(
            last_business_event_id=business_event_id,
            updated_at=current,
        )
    )


def enqueue_event(
    db: Session,
    *,
    event_type: str,
    source_table: str,
    source_pk: str,
    stock_code: str | None = None,
    trade_date=None,
    capital_tier: str | None = None,
    payload: dict | None = None,
    now: datetime | None = None,
    account_id: str | None = None,
) -> str | None:
    """Enqueue a business event + outbox entry. Returns business_event_id or None if deduped/suppressed."""
    if event_type not in _EVENT_TYPES:
        raise ValueError(f"Unknown event_type: {event_type}")

    current = now or _utc_now()
    td_str = trade_date.isoformat() if hasattr(trade_date, "isoformat") else str(trade_date) if trade_date else None

    cursor_table = Base.metadata.tables["event_projection_cursor"]
    business_event_table = Base.metadata.tables["business_event"]
    outbox_table = Base.metadata.tables["outbox_event"]

    safe_payload = _json_safe(payload or {})

    pkey = _projection_key(event_type, stock_code=stock_code, trade_date=td_str,
                           capital_tier=capital_tier, account_id=account_id)

    # Check dedup / suppression
    cursor_row = db.execute(
        select(cursor_table).where(
            cursor_table.c.event_type == event_type,
            cursor_table.c.event_projection_key == pkey,
        )
    ).mappings().first()

    if cursor_row:
        if event_type == "DRAWDOWN_ALERT":
            last_sent = cursor_row.get("last_sent_at")
            dedup_until = cursor_row.get("dedup_until")
            # Normalize naive datetimes from SQLite to UTC-aware
            if dedup_until and dedup_until.tzinfo is None:
                dedup_until = dedup_until.replace(tzinfo=timezone.utc)
            if last_sent and last_sent.tzinfo is None:
                last_sent = last_sent.replace(tzinfo=timezone.utc)
            if dedup_until and current < dedup_until:
                _record_dedup_skipped_event(
                    db,
                    business_event_table=business_event_table,
                    cursor_table=cursor_table,
                    projection_cursor_id=cursor_row["projection_cursor_id"],
                    business_event_id=str(uuid4()),
                    event_type=event_type,
                    projection_key=pkey,
                    source_table=source_table,
                    source_pk=source_pk,
                    stock_code=stock_code,
                    trade_date=trade_date,
                    capital_tier=capital_tier,
                    payload=safe_payload,
                    current=current,
                    status_reason="dedup_suppressed_within_window",
                    dedup_until=dedup_until,
                )
                return None  # suppressed
            if last_sent and (current - last_sent) < timedelta(hours=DRAWDOWN_SUPPRESS_HOURS):
                _record_dedup_skipped_event(
                    db,
                    business_event_table=business_event_table,
                    cursor_table=cursor_table,
                    projection_cursor_id=cursor_row["projection_cursor_id"],
                    business_event_id=str(uuid4()),
                    event_type=event_type,
                    projection_key=pkey,
                    source_table=source_table,
                    source_pk=source_pk,
                    stock_code=stock_code,
                    trade_date=trade_date,
                    capital_tier=capital_tier,
                    payload=safe_payload,
                    current=current,
                    status_reason="dedup_suppressed_recent_dispatch",
                    dedup_until=dedup_until,
                )
                return None  # suppressed
        elif event_type in ("POSITION_CLOSED", "BUY_SIGNAL_DAILY"):
            # Idempotent: same projection key already exists → skip
            existing = db.execute(
                select(business_event_table.c.business_event_id).where(
                    business_event_table.c.event_projection_key == pkey,
                    business_event_table.c.event_type == event_type,
                )
            ).first()
            if existing:
                _record_dedup_skipped_event(
                    db,
                    business_event_table=business_event_table,
                    cursor_table=cursor_table,
                    projection_cursor_id=cursor_row["projection_cursor_id"],
                    business_event_id=str(uuid4()),
                    event_type=event_type,
                    projection_key=pkey,
                    source_table=source_table,
                    source_pk=source_pk,
                    stock_code=stock_code,
                    trade_date=trade_date,
                    capital_tier=capital_tier,
                    payload=safe_payload,
                    current=current,
                    status_reason="dedup_projection_key_exists",
                )
                return None  # deduped

    # Create cursor if not exists, update if exists
    projection_cursor_id = cursor_row["projection_cursor_id"] if cursor_row else str(uuid4())
    business_event_id = str(uuid4())

    if cursor_row:
        db.execute(
            cursor_table.update()
            .where(cursor_table.c.projection_cursor_id == projection_cursor_id)
            .values(
                last_business_event_id=business_event_id,
                dedup_until=(current + timedelta(hours=DRAWDOWN_SUPPRESS_HOURS)) if event_type == "DRAWDOWN_ALERT" else cursor_row.get("dedup_until"),
                updated_at=current,
            )
        )
    else:
        dedup_until_val = None
        if event_type == "DRAWDOWN_ALERT":
            dedup_until_val = current + timedelta(hours=DRAWDOWN_SUPPRESS_HOURS)
        db.execute(
            cursor_table.insert().values(
                projection_cursor_id=projection_cursor_id,
                event_type=event_type,
                event_projection_key=pkey,
                last_business_event_id=business_event_id,
                last_sent_at=None,
                dedup_until=dedup_until_val,
                last_state_value=None,
                recovered_at=None,
                created_at=current,
                updated_at=current,
            )
        )

    db.execute(
        business_event_table.insert().values(
            business_event_id=business_event_id,
            event_type=event_type,
            projection_cursor_id=projection_cursor_id,
            event_projection_key=pkey,
            event_status="ENQUEUED",
            source_table=source_table,
            source_pk=source_pk,
            stock_code=stock_code,
            trade_date=trade_date,
            capital_tier=capital_tier,
            payload_json=safe_payload,
            dedup_until=None,
            status_reason=None,
            created_at=current,
            enqueued_at=current,
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
            payload_json=safe_payload,
            status_reason=None,
            created_at=current,
            updated_at=current,
            dispatched_at=None,
        )
    )

    return business_event_id


def dispatch_pending_events(
    db: Session,
    *,
    now: datetime | None = None,
    claimed_by: str = "event_dispatcher",
) -> list[str]:
    """Dispatch all PENDING outbox events. Returns list of dispatched business_event_ids."""
    current = now or _utc_now()
    outbox_table = Base.metadata.tables["outbox_event"]
    business_event_table = Base.metadata.tables["business_event"]
    notification_table = Base.metadata.tables["notification"]
    cursor_table = Base.metadata.tables["event_projection_cursor"]
    from app.core.config import settings as _settings
    from app.services import notification as notification_service

    claim_timeout = timedelta(seconds=max(1, int(getattr(_settings, "outbox_dispatch_claim_timeout_seconds", 5))))
    stale_claim_cutoff = current - claim_timeout
    stale_requeued = db.execute(
        outbox_table.update()
        .where(outbox_table.c.dispatch_status == "DISPATCHING")
        .where(outbox_table.c.claimed_at.isnot(None))
        .where(outbox_table.c.claimed_at < stale_claim_cutoff)
        .values(
            dispatch_status="PENDING",
            claim_token=None,
            claimed_at=None,
            claimed_by=None,
            status_reason="outbox_claim_timeout",
            updated_at=current,
        )
    )

    pending_rows = db.execute(
        select(
            outbox_table.c.outbox_event_id,
            outbox_table.c.business_event_id,
            outbox_table.c.payload_json,
            outbox_table.c.dispatch_attempt_count,
            business_event_table.c.event_type,
            business_event_table.c.stock_code,
            business_event_table.c.trade_date,
            business_event_table.c.capital_tier,
            business_event_table.c.projection_cursor_id,
        )
        .join(business_event_table, business_event_table.c.business_event_id == outbox_table.c.business_event_id)
        .where(outbox_table.c.dispatch_status.in_(("PENDING", "DISPATCH_FAILED")))
        .where(
            (outbox_table.c.next_retry_at.is_(None))
            | (outbox_table.c.next_retry_at <= current)
        )
        .order_by(outbox_table.c.created_at.asc())
    ).mappings().all()

    dispatched = []
    for row in pending_rows:
        claim_token = str(uuid4())
        claimed = db.execute(
            outbox_table.update()
            .where(
                outbox_table.c.outbox_event_id == row["outbox_event_id"],
                    outbox_table.c.dispatch_status.in_(("PENDING", "DISPATCH_FAILED")),
            )
            .values(
                dispatch_status="DISPATCHING",
                claim_token=claim_token,
                claimed_at=current,
                claimed_by=claimed_by,
                updated_at=current,
                status_reason=None,
            )
        )
        if not claimed.rowcount:
            continue

        event_type = row["event_type"]
        payload = row["payload_json"] or {}
        targets = _dispatch_targets_for_event(db, event_type=event_type, current=current)
        summary = f"{event_type} stock={row['stock_code']} trade_date={row['trade_date']}"

        try:
            if not targets:
                target_results = [
                    _insert_or_reuse_notification(
                        db,
                        notification_table=notification_table,
                        business_event_id=row["business_event_id"],
                        event_type=event_type,
                        channel="email",
                        recipient_scope="user",
                        recipient_key="audience:none",
                        recipient_user_id=None,
                        triggered_at=current,
                        status="skipped",
                        payload_summary=summary,
                        status_reason="no_active_paid_user_recipients",
                    )
                ]
            else:
                target_results = []
                for target in targets:
                    block_reason = target.get("delivery_block_reason")
                    if block_reason:
                        notification_status, status_reason = "skipped", str(block_reason)
                    elif (
                        str(target.get("recipient_scope") or "") == "user"
                        and not bool(target.get("membership_truth_confirmed"))
                    ):
                        if not _settings.user_email_enabled:
                            notification_status, status_reason = "skipped", "user_email_disabled"
                        elif not notification_service._user_email_transport_configured():
                            notification_status, status_reason = "skipped", "user_email_transport_not_implemented"
                        else:
                            notification_status, status_reason = "skipped", "membership_truth_unverified"
                    elif event_type in _EVENT_TYPES:
                        notification_status, status_reason = attempt_business_event_delivery(
                            event_type=event_type,
                            payload=payload,
                            recipient_scope=str(target["recipient_scope"]),
                            recipient_address=target.get("recipient_address"),
                        )
                    else:
                        notification_status, status_reason = "skipped", f"unknown_event_type_{event_type}"
                    result = _insert_or_reuse_notification(
                        db,
                        notification_table=notification_table,
                        business_event_id=row["business_event_id"],
                        event_type=event_type,
                        channel=str(target["channel"]),
                        recipient_scope=str(target["recipient_scope"]),
                        recipient_key=str(target["recipient_key"]),
                        recipient_user_id=target["recipient_user_id"],
                        triggered_at=current,
                        status=notification_status,
                        payload_summary=summary,
                        status_reason=status_reason,
                    )
                    target_results.append(result)

            any_failed = any(item["status"] == "failed" for item in target_results)
            any_sent = any(item.get("sent_at") is not None for item in target_results)
            failed_status_reason = next(
                (
                    item.get("status_reason")
                    for item in target_results
                    if item.get("status") == "failed" and item.get("status_reason")
                ),
                None,
            )
            skipped_status_reason = next(
                (
                    item.get("status_reason")
                    for item in target_results
                    if item.get("status") != "failed" and item.get("status_reason")
                ),
                None,
            )
            status_reason = failed_status_reason or skipped_status_reason

            outbox_values = {
                "dispatch_attempt_count": row["dispatch_attempt_count"] + 1,
                "updated_at": current,
                "status_reason": status_reason,
            }
            if any_failed:
                outbox_values.update(
                    dispatch_status="DISPATCH_FAILED",
                    next_retry_at=current + claim_timeout,
                )
            else:
                outbox_values.update(
                    dispatch_status="DISPATCHED",
                    dispatched_at=current,
                    next_retry_at=None,
                )

            db.execute(
                outbox_table.update()
                .where(
                    outbox_table.c.outbox_event_id == row["outbox_event_id"],
                    outbox_table.c.claim_token == claim_token,
                )
                .values(**outbox_values)
            )

            if row.get("projection_cursor_id") and any_sent:
                db.execute(
                    cursor_table.update()
                    .where(cursor_table.c.projection_cursor_id == row["projection_cursor_id"])
                    .values(last_sent_at=current, updated_at=current)
                )

            if not any_failed:
                dispatched.append(row["business_event_id"])
        except Exception as exc:
            logger.exception("event_dispatch_failed business_event_id=%s err=%s", row["business_event_id"], exc)
            retry_at = current + claim_timeout
            db.execute(
                outbox_table.update()
                .where(
                    outbox_table.c.outbox_event_id == row["outbox_event_id"],
                    outbox_table.c.claim_token == claim_token,
                )
                .values(
                    dispatch_status="DISPATCH_FAILED",
                    dispatch_attempt_count=row["dispatch_attempt_count"] + 1,
                    next_retry_at=retry_at,
                    updated_at=current,
                    status_reason=str(exc)[:200],
                )
            )

    if pending_rows or int(stale_requeued.rowcount or 0) > 0:
        db.commit()
    return dispatched


def enqueue_position_closed_event(
    db: Session, *, position_id: str, stock_code: str, trade_date,
    capital_tier: str, position_status: str, now: datetime | None = None,
) -> str | None:
    payload_row = db.execute(
        select(
            Base.metadata.tables["sim_position"].c.position_id,
            Base.metadata.tables["sim_position"].c.stock_code,
            Base.metadata.tables["sim_position"].c.position_status,
            Base.metadata.tables["sim_position"].c.capital_tier,
            Base.metadata.tables["sim_position"].c.actual_entry_price,
            Base.metadata.tables["sim_position"].c.exit_price,
            Base.metadata.tables["sim_position"].c.net_return_pct,
            Base.metadata.tables["sim_position"].c.holding_days,
            Base.metadata.tables["sim_position"].c.signal_date,
            Base.metadata.tables["sim_position"].c.exit_date,
            Base.metadata.tables["report"].c.stock_name_snapshot,
        )
        .select_from(
            Base.metadata.tables["sim_position"].outerjoin(
                Base.metadata.tables["report"],
                Base.metadata.tables["report"].c.report_id == Base.metadata.tables["sim_position"].c.report_id,
            )
        )
        .where(Base.metadata.tables["sim_position"].c.position_id == position_id)
    ).mappings().first()
    resolved_status = str((payload_row or {}).get("position_status") or position_status)
    if resolved_status not in POSITION_CLOSED_TERMINAL_STATES:
        raise ValueError(f"invalid_position_closed_status:{resolved_status}")
    payload = {
        "stock_code": stock_code,
        "stock_name": (payload_row or {}).get("stock_name_snapshot") or stock_code,
        "position_status": resolved_status,
        "capital_tier": str((payload_row or {}).get("capital_tier") or capital_tier),
        "actual_entry_price": (payload_row or {}).get("actual_entry_price"),
        "exit_price": (payload_row or {}).get("exit_price"),
        "net_return_pct": (payload_row or {}).get("net_return_pct"),
        "holding_days": (payload_row or {}).get("holding_days"),
        "signal_date": _iso_date((payload_row or {}).get("signal_date")),
        "close_date": _iso_date((payload_row or {}).get("exit_date") or trade_date),
    }
    return enqueue_event(
        db, event_type="POSITION_CLOSED", source_table="sim_position",
        source_pk=position_id, stock_code=stock_code, trade_date=trade_date,
        capital_tier=capital_tier, payload=payload, now=now,
    )


def enqueue_buy_signal_events(
    db: Session, *, signals: list[dict], trade_date, now: datetime | None = None,
) -> list[str]:
    """Enqueue one BUY_SIGNAL_DAILY aggregate event for the trade date."""
    if not signals:
        return []
    sorted_signals = sorted(signals, key=lambda s: -(s.get("confidence") or 0))
    top_signals = [
        {
            "stock_code": sig.get("stock_code"),
            "stock_name": sig.get("stock_name") or sig.get("stock_code"),
            "confidence": sig.get("confidence"),
            "strategy_type": sig.get("strategy_type"),
        }
        for sig in sorted_signals[:BUY_SIGNAL_MAX_PER_DAY]
    ]
    payload = {
        "trade_date": _iso_date(trade_date),
        "signal_count": len(sorted_signals),
        "signals": top_signals,
    }
    event_id = enqueue_event(
        db,
        event_type="BUY_SIGNAL_DAILY",
        source_table="report",
        source_pk=f"BUY_SIGNAL_DAILY:{payload['trade_date']}",
        trade_date=trade_date,
        payload=payload,
        now=now,
    )
    return [event_id] if event_id else []


def enqueue_drawdown_alert(
    db: Session, *, account_id: str, drawdown_pct: float,
    capital_tier: str | None = None, drawdown_state: str | None = None, now: datetime | None = None,
) -> str | None:
    current = now or _utc_now()
    trade_date = current.date()
    payload = {
        "drawdown_pct": drawdown_pct,
        "capital_tier": capital_tier,
        "drawdown_state": drawdown_state,
        "triggered_at": current.isoformat(),
    }
    return enqueue_event(
        db, event_type="DRAWDOWN_ALERT", source_table="sim_account",
        source_pk=account_id,
        trade_date=trade_date,
        capital_tier=capital_tier,
        payload=payload,
        now=current,
    )
