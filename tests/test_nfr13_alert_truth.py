from sqlalchemy import func, select

from app.core.config import settings
from app.models import Base
from app.services.cookie_session_ssot import execute_cookie_probe, upsert_cookie_session
from app.services.multisource_ingest import ingest_market_data
from app.services.notification import emit_operational_alert
from tests.helpers_ssot import utc_now


def test_cookie_failure_alert_uses_nfr13_operational_alert_without_drawdown_pollution(
    db_session,
    monkeypatch,
):
    captured: list[dict] = []
    upsert_cookie_session(
        db_session,
        login_source="weibo",
        cookie_string="SUB=secret-cookie",
    )
    db_session.commit()
    monkeypatch.setattr(
        "app.services.notification.emit_operational_alert",
        lambda **kwargs: (captured.append(kwargs) or ("sent", None, "webhook")),
    )
    monkeypatch.setattr(
        "app.services.cookie_session_ssot._do_http_probe",
        lambda provider, cookie_blob: ("failed", 401, 120, "probe_http_401"),
    )

    execute_cookie_probe(db_session, login_source="weibo")
    execute_cookie_probe(db_session, login_source="weibo")

    assert len(captured) == 1
    assert captured[0]["alert_type"] == "COOKIE_CONSECUTIVE_FAILURE"
    assert captured[0]["fr_id"] == "FR-03"
    business_event_t = Base.metadata.tables["business_event"]
    assert db_session.scalar(select(func.count()).select_from(business_event_t)) == 0


def test_circuit_breaker_open_and_close_use_nfr13_operational_alerts(db_session, monkeypatch):
    captured: list[dict] = []
    monkeypatch.setattr("app.services.multisource_ingest.HOTSPOT_SOURCE_PRIORITY", ("weibo",))
    monkeypatch.setattr(settings, "source_fail_open_threshold", 1, raising=False)
    monkeypatch.setattr(settings, "source_circuit_cooldown_seconds", 0, raising=False)
    monkeypatch.setattr(
        "app.services.notification.emit_operational_alert",
        lambda **kwargs: (captured.append(kwargs) or ("sent", None, "webhook")),
    )

    def _raise(*args, **kwargs):
        raise RuntimeError("source_down")

    ingest_market_data(
        db_session,
        trade_date="2026-03-24",
        fetch_hotspot_by_source=_raise,
        now=utc_now(),
    )
    ingest_market_data(
        db_session,
        trade_date="2026-03-24",
        fetch_hotspot_by_source=lambda source_name, trade_date: [
            {
                "rank": 1,
                "topic_title": "Recovered topic",
                "source_url": "https://example.com/topic",
                "fetch_time": utc_now(),
                "news_event_type": "policy",
                "hotspot_tags": ["policy"],
                "stock_codes": [],
            }
        ],
        now=utc_now(),
    )

    assert [item["alert_type"] for item in captured] == [
        "CIRCUIT_BREAKER_OPEN",
        "CIRCUIT_BREAKER_CLOSE",
    ]
    assert {item["fr_id"] for item in captured} == {"FR-04"}
    business_event_t = Base.metadata.tables["business_event"]
    assert db_session.scalar(select(func.count()).select_from(business_event_t)) == 0


def test_operational_alert_falls_back_to_email_truthfully(monkeypatch):
    monkeypatch.setattr(settings, "admin_alert_webhook_url", "", raising=False)
    monkeypatch.setattr(settings, "alert_webhook_enabled", False, raising=False)
    monkeypatch.setattr(settings, "alert_email", "ops@example.com", raising=False)
    monkeypatch.setattr("app.services.notification._admin_email_transport_configured", lambda: True)
    monkeypatch.setattr("app.services.notification.send_admin_email_alert", lambda **kwargs: True)

    status, reason, channel = emit_operational_alert(
        alert_type="COOKIE_CONSECUTIVE_FAILURE",
        fr_id="FR-03",
        message="cookie probe failed twice",
    )

    assert status == "sent"
    assert reason == "admin_email_fallback"
    assert channel == "email"


def test_operational_alert_preserves_webhook_failure_when_email_transport_unavailable(monkeypatch):
    monkeypatch.setattr(settings, "admin_alert_webhook_url", "https://alerts.example/webhook", raising=False)
    monkeypatch.setattr(settings, "alert_webhook_enabled", False, raising=False)
    monkeypatch.setattr(settings, "alert_email", "ops@example.com", raising=False)
    monkeypatch.setattr("app.services.notification.send_admin_notification", lambda *args, **kwargs: False)
    monkeypatch.setattr("app.services.notification._admin_email_transport_configured", lambda: False)

    status, reason, channel = emit_operational_alert(
        alert_type="COOKIE_CONSECUTIVE_FAILURE",
        fr_id="FR-03",
        message="cookie probe failed twice",
    )

    assert status == "failed"
    assert reason == "admin_webhook_send_failed"
    assert channel == "webhook"
