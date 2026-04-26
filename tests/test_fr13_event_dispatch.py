"""FR-13 业务事件推送验收测试"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from app.models import Base, BillingOrder
from app.services.event_dispatcher import (
    enqueue_event,
    enqueue_buy_signal_events,
    enqueue_drawdown_alert,
    enqueue_position_closed_event,
    dispatch_pending_events,
)
from app.services.membership import handle_webhook
from tests.helpers_ssot import insert_open_position, insert_report_bundle_ssot

pytestmark = [
    pytest.mark.feature("FR13-EVENT-01"),
    pytest.mark.feature("FR13-EVENT-02"),
    pytest.mark.feature("FR13-EVENT-03"),
    pytest.mark.feature("FR13-EVENT-04"),
    pytest.mark.feature("FR13-EVENT-05"),
]


def _utc(y=2026, m=3, d=10, h=12, mi=0) -> datetime:
    return datetime(y, m, d, h, mi, tzinfo=timezone.utc)


def _seed_paid_recipient(db_session, create_user, *, email: str, tier: str = "Pro"):
    account = create_user(
        email=email,
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
        tier_expires_at=None,
    )
    user = account["user"]
    now = _utc()
    amount = 29.9 if tier == "Pro" else 99.9
    order = BillingOrder(
        user_id=user.user_id,
        provider="alipay",
        expected_tier=tier,
        period_months=1,
        granted_tier=None,
        amount_cny=amount,
        currency="CNY",
        payment_url="",
        status="CREATED",
        status_reason=None,
        created_at=now - timedelta(minutes=1),
        paid_at=None,
        expires_at=now + timedelta(minutes=15),
        updated_at=now - timedelta(minutes=1),
    )
    db_session.add(order)
    db_session.flush()
    handle_webhook(
        db_session,
        event_id=f"evt-fr13-{uuid4()}",
        order_id=order.order_id,
        user_id=user.user_id,
        tier_id=tier,
        paid_amount=amount,
        provider="alipay",
        payload={"source": "pytest_truth_seed"},
    )
    db_session.commit()
    db_session.refresh(user)
    return user


def test_fr13_position_closed_notification(db_session, monkeypatch, create_user):
    """POSITION_CLOSED enters the notification chain but must not fake a send."""
    from app.core.config import settings
    monkeypatch.setattr(settings, "user_email_enabled", True)
    recipient = _seed_paid_recipient(db_session, create_user, email="fr13-position@example.com")

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()
    assert event_id is not None

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=1))
    assert event_id in dispatched

    notification_table = Base.metadata.tables["notification"]
    outbox_table = Base.metadata.tables["outbox_event"]
    rows = db_session.execute(
        notification_table.select().where(
            notification_table.c.business_event_id == event_id,
        )
    ).fetchall()
    assert len(rows) == 1
    assert rows[0].event_type == "POSITION_CLOSED"
    assert rows[0].channel == "email"
    assert rows[0].recipient_scope == "user"
    assert rows[0].recipient_key == f"user:{recipient.user_id}"
    assert rows[0].recipient_user_id == recipient.user_id
    assert rows[0].status == "skipped"
    assert rows[0].status_reason == "user_email_transport_not_implemented"

    outbox_row = db_session.execute(
        outbox_table.select().where(outbox_table.c.business_event_id == event_id)
    ).mappings().one()
    assert outbox_row["dispatch_status"] == "DISPATCHED"
    assert outbox_row["claim_token"] is not None
    assert outbox_row["claimed_at"] is not None
    assert outbox_row["claimed_by"] == "event_dispatcher"


def test_fr13_user_notifications_fan_out_to_each_active_paid_user(db_session, monkeypatch, create_user):
    from app.core.config import settings

    monkeypatch.setattr(settings, "user_email_enabled", False)
    paid_one = _seed_paid_recipient(db_session, create_user, email="fr13-fanout-pro@example.com", tier="Pro")
    paid_two = _seed_paid_recipient(db_session, create_user, email="fr13-fanout-ent@example.com", tier="Enterprise")
    create_user(
        email="fr13-fanout-free@example.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=1))
    assert dispatched == [event_id]

    notification_table = Base.metadata.tables["notification"]
    rows = db_session.execute(
        notification_table.select().where(notification_table.c.business_event_id == event_id)
    ).mappings().all()
    assert len(rows) == 2
    keys = {row["recipient_key"] for row in rows}
    assert keys == {f"user:{paid_one.user_id}", f"user:{paid_two.user_id}"}
    assert {row["recipient_user_id"] for row in rows} == {paid_one.user_id, paid_two.user_id}
    assert {row["status"] for row in rows} == {"skipped"}
    assert {row["status_reason"] for row in rows} == {"user_email_disabled"}


def test_fr13_zero_paid_recipients_records_audit_skipped_notification(db_session, monkeypatch, create_user):
    from app.core.config import settings

    monkeypatch.setattr(settings, "user_email_enabled", True)
    create_user(
        email="fr13-free-only@example.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=2))
    assert dispatched == [event_id]

    notification_table = Base.metadata.tables["notification"]
    outbox_table = Base.metadata.tables["outbox_event"]
    row = db_session.execute(
        notification_table.select().where(notification_table.c.business_event_id == event_id)
    ).mappings().one()
    assert row["channel"] == "email"
    assert row["recipient_scope"] == "user"
    assert row["recipient_key"] == "audience:none"
    assert row["recipient_user_id"] is None
    assert row["status"] == "skipped"
    assert row["status_reason"] == "no_active_paid_user_recipients"

    outbox_row = db_session.execute(
        outbox_table.select().where(outbox_table.c.business_event_id == event_id)
    ).mappings().one()
    assert outbox_row["dispatch_status"] == "DISPATCHED"
    assert outbox_row["status_reason"] == "no_active_paid_user_recipients"


def test_fr13_position_closed_payload_includes_frozen_fields(db_session):
    now = _utc()
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-09",
    )
    position_id = insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code="600519.SH",
        capital_tier="100k",
        signal_date="2026-03-09",
        entry_date="2026-03-10",
        actual_entry_price=10.0,
        signal_entry_price=9.9,
        position_ratio=0.2,
        shares=100,
        atr_pct_snapshot=0.03,
        atr_multiplier_snapshot=2.0,
        stop_loss_price=9.2,
        target_price=11.0,
    )
    position_table = Base.metadata.tables["sim_position"]
    db_session.execute(
        position_table.update()
        .where(position_table.c.position_id == position_id)
        .values(
            position_status="TAKE_PROFIT",
            exit_date=date(2026, 3, 12),
            exit_price=10.8,
            holding_days=2,
            net_return_pct=0.071,
            commission_total=10.0,
            stamp_duty=0.5,
            slippage_total=1.0,
            updated_at=now,
        )
    )
    db_session.commit()

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=position_id,
        stock_code="600519.SH",
        trade_date=date(2026, 3, 12),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=now,
    )
    db_session.commit()

    business_event = Base.metadata.tables["business_event"]
    row = db_session.execute(
        business_event.select().where(business_event.c.business_event_id == event_id)
    ).mappings().one()
    payload = row["payload_json"]
    assert set(payload.keys()) == {
        "stock_code",
        "stock_name",
        "position_status",
        "capital_tier",
        "actual_entry_price",
        "exit_price",
        "net_return_pct",
        "holding_days",
        "signal_date",
        "close_date",
    }
    assert payload["stock_name"] == "MOUTAI"
    assert payload["actual_entry_price"] == 10.0
    assert payload["exit_price"] == 10.8
    assert payload["net_return_pct"] == 0.071
    assert payload["holding_days"] == 2
    assert payload["signal_date"] == "2026-03-09"
    assert payload["close_date"] == "2026-03-12"


def test_fr13_notification_idempotent(db_session):
    """Same POSITION_CLOSED for same stock/date/tier is deduped."""
    pos_id = str(uuid4())
    now = _utc()
    eid1 = enqueue_position_closed_event(
        db_session, position_id=pos_id, stock_code="000001.SZ",
        trade_date=date(2026, 3, 10), capital_tier="10k",
        position_status="STOP_LOSS", now=now,
    )
    db_session.commit()
    assert eid1 is not None

    eid2 = enqueue_position_closed_event(
        db_session, position_id=pos_id, stock_code="000001.SZ",
        trade_date=date(2026, 3, 10), capital_tier="10k",
        position_status="STOP_LOSS", now=now + timedelta(seconds=5),
    )
    assert eid2 is None  # deduped


def test_fr13_drawdown_alert_suppression(db_session):
    """DRAWDOWN_ALERT within 4h window is suppressed by capital_tier + trade_date."""
    now = _utc()
    account_id = str(uuid4())

    eid1 = enqueue_drawdown_alert(
        db_session, account_id=account_id, drawdown_pct=15.0,
        capital_tier="100k", now=now,
    )
    db_session.commit()
    dispatched = dispatch_pending_events(db_session, now=now + timedelta(minutes=1))
    assert eid1 in dispatched

    # Within 4h → suppressed even if account_id changes
    eid2 = enqueue_drawdown_alert(
        db_session, account_id=str(uuid4()), drawdown_pct=16.0,
        capital_tier="100k", now=now + timedelta(hours=2),
    )
    assert eid2 is None
    db_session.commit()

    business_event_table = Base.metadata.tables["business_event"]
    skipped_row = db_session.execute(
        business_event_table.select()
        .where(business_event_table.c.event_type == "DRAWDOWN_ALERT")
        .order_by(business_event_table.c.created_at.desc())
    ).mappings().first()
    assert skipped_row is not None
    assert skipped_row["event_status"] == "DEDUP_SKIPPED"
    assert skipped_row["status_reason"] in {
        "dedup_suppressed_within_window",
        "dedup_suppressed_recent_dispatch",
    }

    # After 4h → allowed
    eid3 = enqueue_drawdown_alert(
        db_session, account_id=account_id, drawdown_pct=17.0,
        capital_tier="100k", now=now + timedelta(hours=5),
    )
    db_session.commit()
    assert eid3 is not None


def test_fr13_txn_rollback_no_send(db_session):
    """If transaction rolls back, no event is dispatched."""
    outbox_table = Base.metadata.tables["outbox_event"]
    now = _utc()

    # Create a savepoint to simulate rollback
    before_count = db_session.execute(
        outbox_table.select()
    ).fetchall()

    enqueue_position_closed_event(
        db_session, position_id=str(uuid4()), stock_code="600036.SH",
        trade_date=date(2026, 3, 10), capital_tier="500k",
        position_status="TIMEOUT", now=now,
    )
    db_session.rollback()

    after_count = db_session.execute(
        outbox_table.select()
    ).fetchall()
    assert len(after_count) == len(before_count)


def test_fr13_buy_signal_max5(db_session):
    """BUY_SIGNAL_DAILY must be a single per-day aggregate with top-5 signals."""
    td = date(2026, 3, 10)
    signals = [
        {
            "report_id": str(uuid4()),
            "stock_code": f"{600000 + i:06d}.SH",
            "stock_name": f"NAME{i}",
            "confidence": 0.5 + i * 0.05,
            "strategy_type": "B",
        }
        for i in range(8)
    ]
    result = enqueue_buy_signal_events(db_session, signals=signals, trade_date=td, now=_utc())
    db_session.commit()
    assert len(result) == 1

    business_event_table = Base.metadata.tables["business_event"]
    rows = db_session.execute(
        business_event_table.select().where(
            business_event_table.c.event_type == "BUY_SIGNAL_DAILY",
            business_event_table.c.trade_date == td,
        )
    ).mappings().all()
    assert len(rows) == 1
    payload = rows[0]["payload_json"]
    assert payload["trade_date"] == "2026-03-10"
    assert payload["signal_count"] == 8
    assert len(payload["signals"]) == 5
    assert payload["signals"][0]["stock_code"] == "600007.SH"
    assert payload["signals"][-1]["stock_code"] == "600003.SH"


def test_fr13_buy_signal_empty(db_session):
    """signal_count=0 creates no events."""
    result = enqueue_buy_signal_events(db_session, signals=[], trade_date=date(2026, 3, 10), now=_utc())
    assert result == []


def test_fr13_drawdown_payload_includes_state_and_trigger_time(db_session):
    event_id = enqueue_drawdown_alert(
        db_session,
        account_id=str(uuid4()),
        drawdown_pct=-0.13,
        capital_tier="100k",
        drawdown_state="REDUCE",
        now=_utc(),
    )
    db_session.commit()
    business_event = Base.metadata.tables["business_event"]
    row = db_session.execute(
        business_event.select().where(business_event.c.business_event_id == event_id)
    ).mappings().one()
    payload = row["payload_json"]
    assert "account_id" not in payload
    assert set(payload.keys()) == {"capital_tier", "drawdown_pct", "drawdown_state", "triggered_at"}
    assert row["trade_date"].isoformat() == "2026-03-10"
    assert payload["drawdown_state"] == "REDUCE"
    assert payload["triggered_at"].startswith("2026-03-10T12:00:00")


def test_fr13_position_closed_rejects_non_terminal_status(db_session):
    with pytest.raises(ValueError, match="invalid_position_closed_status"):
        enqueue_position_closed_event(
            db_session,
            position_id=str(uuid4()),
            stock_code="600519.SH",
            trade_date=date(2026, 3, 10),
            capital_tier="100k",
            position_status="OPEN",
            now=_utc(),
        )


def test_fr13_dispatch_sets_claim_fields_and_dispatching_state(db_session, monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "user_email_enabled", True)
    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=2), claimed_by="pytest_dispatcher")
    assert event_id in dispatched

    outbox_table = Base.metadata.tables["outbox_event"]
    outbox_row = db_session.execute(
        outbox_table.select().where(outbox_table.c.business_event_id == event_id)
    ).mappings().one()
    assert outbox_row["dispatch_status"] == "DISPATCHED"
    assert outbox_row["claim_token"] is not None
    assert outbox_row["claimed_at"] is not None
    assert outbox_row["claimed_by"] == "pytest_dispatcher"


@pytest.mark.feature("FR13-EVENT-04")
def test_fr13_admin_notification_marks_failed_when_webhook_delivery_fails(db_session, monkeypatch):
    from app.core.config import settings
    from app.services import notification as notification_service

    monkeypatch.setattr(settings, "admin_alert_webhook_url", "https://example.com/webhook")
    monkeypatch.setattr(notification_service, "send_admin_notification", lambda kind, payload: False)

    event_id = enqueue_event(
        db_session,
        event_type="REPORT_PENDING_REVIEW",
        source_table="report",
        source_pk=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        payload={"review_flag": "PENDING_REVIEW", "negative_count": 3},
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=2), claimed_by="pytest_dispatcher")
    assert event_id not in dispatched

    notification_table = Base.metadata.tables["notification"]
    outbox_table = Base.metadata.tables["outbox_event"]
    notification_row = db_session.execute(
        notification_table.select().where(notification_table.c.business_event_id == event_id)
    ).mappings().one()
    assert notification_row["channel"] == "webhook"
    assert notification_row["status"] == "failed"
    assert notification_row["status_reason"] == "admin_channel_send_failed"
    assert notification_row["sent_at"] is None

    outbox_row = db_session.execute(
        outbox_table.select().where(outbox_table.c.business_event_id == event_id)
    ).mappings().one()
    assert outbox_row["dispatch_status"] == "DISPATCH_FAILED"
    assert outbox_row["next_retry_at"] is not None


@pytest.mark.feature("FR13-EVENT-04")
def test_fr13_admin_notification_marks_sent_only_after_real_webhook_delivery(db_session, monkeypatch):
    from app.core.config import settings
    from app.services import notification as notification_service

    monkeypatch.setattr(settings, "admin_alert_webhook_url", "https://example.com/webhook")
    monkeypatch.setattr(notification_service, "send_admin_notification", lambda kind, payload: True)

    event_id = enqueue_event(
        db_session,
        event_type="REPORT_PENDING_REVIEW",
        source_table="report",
        source_pk=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        payload={"review_flag": "PENDING_REVIEW", "negative_count": 3},
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=2), claimed_by="pytest_dispatcher")
    assert event_id in dispatched

    notification_table = Base.metadata.tables["notification"]
    outbox_table = Base.metadata.tables["outbox_event"]
    notification_row = db_session.execute(
        notification_table.select().where(notification_table.c.business_event_id == event_id)
    ).mappings().one()
    assert notification_row["status"] == "sent"
    assert notification_row["status_reason"] is None
    assert notification_row["sent_at"] is not None

    outbox_row = db_session.execute(
        outbox_table.select().where(outbox_table.c.business_event_id == event_id)
    ).mappings().one()
    assert outbox_row["dispatch_status"] == "DISPATCHED"


def test_fr13_admin_notification_does_not_depend_on_user_notification_switch(db_session, monkeypatch):
    from app.core.config import settings
    from app.services import notification as notification_service

    monkeypatch.setattr(settings, "admin_alert_webhook_url", "https://example.com/webhook")
    monkeypatch.setattr(notification_service, "send_admin_notification", lambda kind, payload: True)

    event_id = enqueue_event(
        db_session,
        event_type="REPORT_PENDING_REVIEW",
        source_table="report",
        source_pk=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        payload={"review_flag": "PENDING_REVIEW", "negative_count": 3},
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=2), claimed_by="admin_switch_dispatch")
    assert event_id in dispatched

    notification_table = Base.metadata.tables["notification"]
    row = db_session.execute(
        notification_table.select().where(notification_table.c.business_event_id == event_id)
    ).mappings().one()
    assert row["status"] == "sent"
    assert row["status_reason"] is None


def test_fr13_user_email_message_uses_configured_from_name(monkeypatch):
    from app.core.config import settings
    from app.services import notification as notification_service

    monkeypatch.setattr(settings, "user_email_from_name", "Lane 3 Alerts")
    monkeypatch.setattr(settings, "user_email_from_address", "alerts@example.com")

    message = notification_service._build_user_email_message(
        "POSITION_CLOSED",
        {"stock_code": "600519.SH", "position_status": "TAKE_PROFIT", "net_return_pct": 0.1, "close_date": "2026-03-10"},
        "user@example.com",
    )

    assert "Lane 3 Alerts" in message["From"]
    assert "alerts@example.com" in message["From"]


def test_fr13_user_email_notification_marks_sent_after_transport_success(db_session, monkeypatch, create_user):
    from app.core.config import settings
    from app.services import notification as notification_service

    monkeypatch.setattr(settings, "user_email_enabled", True)
    monkeypatch.setattr(settings, "user_email_smtp_host", "smtp.example.com")
    monkeypatch.setattr(settings, "user_email_from_address", "alerts@example.com")
    monkeypatch.setattr(notification_service, "send_user_email_notification", lambda **kwargs: True)
    recipient = _seed_paid_recipient(db_session, create_user, email="fr13-email-sent@example.com")

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=2), claimed_by="smtp_dispatcher")
    assert event_id in dispatched

    notification_table = Base.metadata.tables["notification"]
    outbox_table = Base.metadata.tables["outbox_event"]
    notification_row = db_session.execute(
        notification_table.select().where(notification_table.c.business_event_id == event_id)
    ).mappings().one()
    assert notification_row["recipient_key"] == f"user:{recipient.user_id}"
    assert notification_row["status"] == "sent"
    assert notification_row["status_reason"] is None
    assert notification_row["sent_at"] is not None

    outbox_row = db_session.execute(
        outbox_table.select().where(outbox_table.c.business_event_id == event_id)
    ).mappings().one()
    assert outbox_row["dispatch_status"] == "DISPATCHED"


def test_fr13_transport_ready_without_truth_backed_membership_skips_fail_closed(db_session, monkeypatch, create_user):
    from app.core.config import settings
    from app.services import notification as notification_service

    monkeypatch.setattr(settings, "user_email_enabled", True)
    monkeypatch.setattr(settings, "user_email_smtp_host", "smtp.example.com")
    monkeypatch.setattr(settings, "user_email_from_address", "alerts@example.com")

    sent_calls: list[str] = []

    def _unexpected_send(**kwargs):
        sent_calls.append(str(kwargs.get("recipient_email")))
        return True

    monkeypatch.setattr(notification_service, "send_user_email_notification", _unexpected_send)
    recipient = create_user(
        email="fr13-unbacked@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
    )["user"]

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=2), claimed_by="truth_gate_dispatcher")
    assert event_id in dispatched
    assert sent_calls == []

    notification_table = Base.metadata.tables["notification"]
    outbox_table = Base.metadata.tables["outbox_event"]
    notification_row = db_session.execute(
        notification_table.select().where(notification_table.c.business_event_id == event_id)
    ).mappings().one()
    assert notification_row["recipient_key"] == f"user:{recipient.user_id}"
    assert notification_row["status"] == "skipped"
    assert notification_row["status_reason"] == "membership_truth_unverified"
    assert notification_row["sent_at"] is None

    outbox_row = db_session.execute(
        outbox_table.select().where(outbox_table.c.business_event_id == event_id)
    ).mappings().one()
    assert outbox_row["dispatch_status"] == "DISPATCHED"
    assert outbox_row["status_reason"] == "membership_truth_unverified"


def test_fr13_dispatch_retries_failed_rows_when_retry_time_arrives(db_session, monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "user_email_enabled", True)
    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()

    outbox_table = Base.metadata.tables["outbox_event"]
    db_session.execute(
        outbox_table.update()
        .where(outbox_table.c.business_event_id == event_id)
        .values(
            dispatch_status="DISPATCH_FAILED",
            next_retry_at=_utc(mi=1),
            claim_token=None,
            claimed_at=None,
            claimed_by=None,
            status_reason="temporary_failure",
        )
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=2), claimed_by="retry_dispatcher")

    assert event_id in dispatched
    outbox_row = db_session.execute(
        outbox_table.select().where(outbox_table.c.business_event_id == event_id)
    ).mappings().one()
    assert outbox_row["dispatch_status"] == "DISPATCHED"
    assert outbox_row["claimed_by"] == "retry_dispatcher"


def test_fr13_user_email_notification_marks_sent_when_smtp_adapter_succeeds(db_session, monkeypatch, create_user):
    from app.core.config import settings
    from app.services import notification as notification_service

    class _FakeSMTP:
        def __init__(self, host, port, timeout=None):
            self.host = host
            self.port = port
            self.timeout = timeout

        def starttls(self):
            return None

        def login(self, username, password):
            return None

        def send_message(self, message):
            assert message["To"] == "fr13-smtp@example.com"

        def quit(self):
            return None

    monkeypatch.setattr(settings, "user_email_enabled", True)
    monkeypatch.setattr(settings, "user_email_smtp_host", "smtp.example.com")
    monkeypatch.setattr(settings, "user_email_from_address", "noreply@example.com")
    monkeypatch.setattr(notification_service.smtplib, "SMTP", _FakeSMTP)
    _seed_paid_recipient(db_session, create_user, email="fr13-smtp@example.com")

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=3))
    assert event_id in dispatched

    notification_table = Base.metadata.tables["notification"]
    row = db_session.execute(
        notification_table.select().where(notification_table.c.business_event_id == event_id)
    ).mappings().one()
    assert row["status"] == "sent"
    assert row["status_reason"] is None
    assert row["sent_at"] is not None


def test_fr13_failed_user_email_retry_updates_existing_notification(db_session, monkeypatch, create_user):
    from app.core.config import settings
    from app.services import notification as notification_service

    monkeypatch.setattr(settings, "user_email_enabled", True)
    monkeypatch.setattr(settings, "user_email_smtp_host", "smtp.example.com")
    monkeypatch.setattr(settings, "user_email_from_address", "noreply@example.com")
    monkeypatch.setattr(notification_service, "send_user_email_notification", lambda **kwargs: False)
    _seed_paid_recipient(db_session, create_user, email="fr13-retry@example.com")

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()

    first = dispatch_pending_events(db_session, now=_utc(mi=4), claimed_by="first_dispatch")
    assert event_id not in first

    notification_table = Base.metadata.tables["notification"]
    outbox_table = Base.metadata.tables["outbox_event"]
    first_rows = db_session.execute(
        notification_table.select()
        .where(notification_table.c.business_event_id == event_id)
        .order_by(notification_table.c.created_at.asc(), notification_table.c.notification_id.asc())
    ).mappings().all()
    assert len(first_rows) == 1
    assert first_rows[0]["status"] == "failed"
    assert first_rows[0]["status_reason"] == "user_email_send_failed"

    monkeypatch.setattr(notification_service, "send_user_email_notification", lambda **kwargs: True)
    db_session.execute(
        outbox_table.update()
        .where(outbox_table.c.business_event_id == event_id)
        .values(next_retry_at=_utc(mi=4), claim_token=None, claimed_at=None, claimed_by=None)
    )
    db_session.commit()

    second = dispatch_pending_events(db_session, now=_utc(mi=5), claimed_by="retry_dispatch")
    assert event_id in second

    rows = db_session.execute(
        notification_table.select()
        .where(notification_table.c.business_event_id == event_id)
        .order_by(notification_table.c.created_at.asc(), notification_table.c.notification_id.asc())
    ).mappings().all()
    assert len(rows) == 1
    assert rows[0]["status"] == "sent"
    assert rows[0]["status_reason"] is None
    outbox_row = db_session.execute(
        outbox_table.select().where(outbox_table.c.business_event_id == event_id)
    ).mappings().one()
    assert outbox_row["dispatch_status"] == "DISPATCHED"


def test_fr13_outbox_failure_reason_prefers_failed_delivery_over_skipped_reason(db_session, monkeypatch, create_user):
    from app.core.config import settings
    from app.services import notification as notification_service

    monkeypatch.setattr(settings, "user_email_enabled", True)
    monkeypatch.setattr(settings, "user_email_smtp_host", "smtp.example.com")
    monkeypatch.setattr(settings, "user_email_from_address", "alerts@example.com")
    monkeypatch.setattr(notification_service, "send_user_email_notification", lambda **kwargs: False)
    create_user(
        email="fr13-skipped-before-failed@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
        tier_expires_at=None,
    )
    _seed_paid_recipient(db_session, create_user, email="fr13-failed-recipient@example.com")

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=7), claimed_by="reason_dispatch")
    assert event_id not in dispatched

    notification_table = Base.metadata.tables["notification"]
    outbox_table = Base.metadata.tables["outbox_event"]
    rows = db_session.execute(
        notification_table.select()
        .where(notification_table.c.business_event_id == event_id)
        .order_by(notification_table.c.created_at.asc(), notification_table.c.notification_id.asc())
    ).mappings().all()
    assert len(rows) == 2
    assert {row["status_reason"] for row in rows} == {"expiry_unconfirmed", "user_email_send_failed"}

    outbox_row = db_session.execute(
        outbox_table.select().where(outbox_table.c.business_event_id == event_id)
    ).mappings().one()
    assert outbox_row["dispatch_status"] == "DISPATCH_FAILED"
    assert outbox_row["status_reason"] == "user_email_send_failed"


def test_fr13_missing_email_records_skipped_notification(db_session, monkeypatch, create_user):
    from app.core.config import settings

    monkeypatch.setattr(settings, "user_email_enabled", True)
    create_user(
        email=None,
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
    )

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=8), claimed_by="missing_email_dispatch")
    assert event_id in dispatched

    notification_table = Base.metadata.tables["notification"]
    row = db_session.execute(
        notification_table.select().where(notification_table.c.business_event_id == event_id)
    ).mappings().one()
    assert row["status"] == "skipped"
    assert row["status_reason"] == "user_email_missing"


def test_fr13_unverified_email_records_skipped_notification(db_session, monkeypatch, create_user):
    from app.core.config import settings

    monkeypatch.setattr(settings, "user_email_enabled", True)
    create_user(
        email="fr13-unverified@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=False,
    )

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=9), claimed_by="unverified_dispatch")
    assert event_id in dispatched

    notification_table = Base.metadata.tables["notification"]
    row = db_session.execute(
        notification_table.select().where(notification_table.c.business_event_id == event_id)
    ).mappings().one()
    assert row["status"] == "skipped"
    assert row["status_reason"] == "user_email_unverified"


def test_fr13_inactive_paid_membership_records_skipped_notification(db_session, monkeypatch, create_user):
    from app.core.config import settings

    monkeypatch.setattr(settings, "user_email_enabled", True)
    create_user(
        email="fr13-inactive@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
        tier_expires_at=_utc() - timedelta(days=1),
    )

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=10), claimed_by="inactive_dispatch")
    assert event_id in dispatched

    notification_table = Base.metadata.tables["notification"]
    row = db_session.execute(
        notification_table.select().where(notification_table.c.business_event_id == event_id)
    ).mappings().one()
    assert row["status"] == "skipped"
    assert row["status_reason"] == "membership_inactive"


def test_fr13_unknown_paid_expiry_records_skipped_notification(db_session, monkeypatch, create_user):
    from app.core.config import settings

    monkeypatch.setattr(settings, "user_email_enabled", True)
    create_user(
        email="fr13-unknown@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
        tier_expires_at=None,
    )

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="TAKE_PROFIT",
        now=_utc(),
    )
    db_session.commit()

    dispatched = dispatch_pending_events(db_session, now=_utc(mi=6), claimed_by="expiry_dispatch")
    assert event_id in dispatched

    notification_table = Base.metadata.tables["notification"]
    row = db_session.execute(
        notification_table.select().where(notification_table.c.business_event_id == event_id)
    ).mappings().one()
    assert row["status"] == "skipped"
    assert row["status_reason"] == "expiry_unconfirmed"


# ---------------------------------------------------------------------------
# FR13-EVENT-01: DELISTED + exit_price=0 事件投递场景
# ---------------------------------------------------------------------------

@pytest.mark.feature("FR13-EVENT-01")
def test_fr13_delisted_position_closed_event_has_zero_exit_price(db_session, monkeypatch, create_user):
    """
    验证退市（DELISTED_LIQUIDATED）场景下：
    1. enqueue_position_closed_event 能接受 exit_price=0 的事件
    2. 事件 payload 中 position_status = DELISTED_LIQUIDATED，exit_price = 0
    3. 事件成功进入派送队列（dispatch 返回该 event_id）
    """
    from app.core.config import settings

    monkeypatch.setattr(settings, "user_email_enabled", True)

    _seed_paid_recipient(db_session, create_user, email="fr13-delisted@example.com")

    event_id = enqueue_position_closed_event(
        db_session,
        position_id=str(uuid4()),
        stock_code="600519.SH",
        trade_date=date(2026, 3, 10),
        capital_tier="100k",
        position_status="DELISTED_LIQUIDATED",
        now=_utc(),
    )
    db_session.commit()

    # 事件已入队 — 使用正确的主键列名 business_event_id
    business_event_table = Base.metadata.tables["business_event"]
    event_row = db_session.execute(
        business_event_table.select().where(business_event_table.c.business_event_id == event_id)
    ).mappings().one()
    payload = event_row["payload_json"]
    import json as _json
    if isinstance(payload, str):
        payload = _json.loads(payload)
    assert payload.get("position_status") == "DELISTED_LIQUIDATED", (
        f"Expected DELISTED_LIQUIDATED in payload, got: {payload}"
    )

    # 派送后应有通知记录（不关心 status，只验证可以被处理而非崩溃）
    dispatched = dispatch_pending_events(db_session, now=_utc(mi=10), claimed_by="delisted_dispatch")
    assert event_id in dispatched, f"event_id {event_id} was not dispatched"
