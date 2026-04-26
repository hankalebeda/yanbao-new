from sqlalchemy import func, select

from app.models import Base
from app.services.cookie_session_ssot import upsert_cookie_session


def test_internal_cookie_refresh_does_not_create_legacy_row_without_session(client, db_session, internal_headers):
    table = Base.metadata.tables["cookie_session"]

    response = client.post(
        "/api/v1/internal/cookie/refresh",
        params={"platform": "weibo"},
        headers=internal_headers("internal-cookie-token"),
    )

    assert response.status_code == 200
    body = response.json()["data"]
    assert body["status"] == "SKIPPED"
    assert body["status_reason"] == "no_session"
    assert db_session.scalar(select(func.count()).select_from(table).where(table.c.provider == "weibo")) == 0


def test_internal_cookie_refresh_uses_cookie_session_ssot_probe_flow(
    client,
    db_session,
    internal_headers,
    monkeypatch,
):
    upsert_cookie_session(
        db_session,
        login_source="weibo",
        cookie_string="SUB=secret-cookie",
    )
    db_session.commit()
    monkeypatch.setattr(
        "app.services.cookie_session_ssot._do_http_probe",
        lambda provider, cookie_blob: ("failed", 401, 120, "probe_http_401"),
    )

    response = client.post(
        "/api/v1/internal/cookie/refresh",
        params={"platform": "weibo"},
        headers=internal_headers("internal-cookie-token"),
    )

    assert response.status_code == 200
    body = response.json()["data"]
    assert body["status"] == "REFRESH_FAILED"
    assert body["status_reason"] == "probe_http_401"
    assert body["last_probe_at"] is not None
    assert body["last_refresh_at"] is not None

    table = Base.metadata.tables["cookie_session"]
    row = db_session.execute(
        table.select().where(table.c.provider == "weibo").order_by(table.c.updated_at.desc())
    ).mappings().first()
    assert row is not None
    assert row["status"] == "REFRESH_FAILED"
    assert row["status_reason"] == "probe_http_401"
    assert row["last_probe_at"] is not None
