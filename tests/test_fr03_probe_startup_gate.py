from __future__ import annotations

import pytest
from datetime import datetime, timedelta, timezone

pytestmark = pytest.mark.feature("FR-03")

from uuid import uuid4

from fastapi.testclient import TestClient

from app.models import Base


def _insert_cookie_session(db_session, *, provider: str) -> str:
    session_id = str(uuid4())
    now = datetime.now(timezone.utc)
    db_session.execute(
        Base.metadata.tables["cookie_session"].insert().values(
            cookie_session_id=session_id,
            provider=provider,
            account_key=f"{provider}-account",
            status="ACTIVE",
            cookie_blob=f"{provider}=secret",
            expires_at=now + timedelta(hours=2),
            created_at=now,
            updated_at=now,
        )
    )
    db_session.commit()
    return session_id


def test_fr03_probe_skipped_records_mutex_busy_state(db_session, monkeypatch):
    from app.services import cookie_session_ssot as cookie_mod

    session_id = _insert_cookie_session(db_session, provider="xueqiu")
    acquired = cookie_mod._probe_lock.acquire(blocking=False)
    assert acquired is True
    monkeypatch.setattr(
        cookie_mod,
        "_do_http_probe",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("mutex_busy path must not probe upstream")),
    )
    try:
        result = cookie_mod.execute_cookie_probe(db_session, login_source="xueqiu")
    finally:
        cookie_mod._probe_lock.release()

    assert result == {"outcome": "skipped", "reason": "mutex_busy"}

    probe_row = db_session.execute(
        Base.metadata.tables["cookie_probe_log"]
        .select()
        .where(Base.metadata.tables["cookie_probe_log"].c.cookie_session_id == session_id)
        .order_by(Base.metadata.tables["cookie_probe_log"].c.probed_at.desc())
    ).mappings().first()
    assert probe_row is not None
    assert probe_row["probe_outcome"] == "skipped"
    assert probe_row["status_reason"] == "mutex_busy"

    session_row = db_session.execute(
        Base.metadata.tables["cookie_session"]
        .select()
        .where(Base.metadata.tables["cookie_session"].c.cookie_session_id == session_id)
    ).mappings().one()
    assert session_row["status"] == "SKIPPED"
    assert session_row["status_reason"] == "mutex_busy"
    assert session_row["last_probe_at"] is not None


def test_fr03_startup_degraded_health_does_not_crash_with_scheduler_enabled(isolated_app, monkeypatch):
    import app.main as app_main
    from app.core.config import settings

    start_hits: list[str] = []
    stop_hits: list[str] = []

    monkeypatch.setattr(settings, "enable_scheduler", True)
    monkeypatch.setattr(app_main, "start_scheduler", lambda: start_hits.append("start"))
    monkeypatch.setattr(app_main, "stop_scheduler", lambda: stop_hits.append("stop"))

    with TestClient(isolated_app["app"], base_url="http://localhost") as client:
        response = client.get("/health")

        assert start_hits == ["start"]
        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["data"]["scheduler_status"] == "degraded"
        assert body["data"]["status"] == "degraded"

    assert stop_hits == ["stop"]
