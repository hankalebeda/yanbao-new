from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from app.models import Base
from app.services.cookie_session_ssot import record_cookie_probe

pytestmark = [
    pytest.mark.feature("FR03-COOKIE-01"),
    pytest.mark.feature("FR03-COOKIE-02"),
    pytest.mark.feature("FR03-COOKIE-03"),
    pytest.mark.feature("FR03-COOKIE-04"),
]


def _auth_headers(client, create_user, *, role: str = "admin"):
    admin = create_user(
        email=f"{role}-fr03@example.com",
        password="Password123",
        role=role,
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": admin["user"].email, "password": admin["password"]},
    )
    assert login.status_code == 200
    return {"Authorization": f"Bearer {login.json()['data']['access_token']}"}


def _latest_cookie_session_row(db_session, provider: str):
    table = Base.metadata.tables["cookie_session"]
    return db_session.execute(
        table.select().where(table.c.provider == provider).order_by(table.c.updated_at.desc())
    ).first()


def test_fr03_health_check(client, db_session, create_user):
    headers = _auth_headers(client, create_user)
    response = client.post(
        "/api/v1/admin/cookie-session",
        headers=headers | {"X-Request-ID": "req-fr03-health-upsert"},
        json={"login_source": "xueqiu", "cookie_string": "xq_a_token=secret-cookie"},
    )

    assert response.status_code == 201
    session_row = _latest_cookie_session_row(db_session, "xueqiu")
    assert session_row is not None

    probe_time = datetime.now(timezone.utc)
    record_cookie_probe(
        db_session,
        cookie_session_id=session_row.cookie_session_id,
        outcome="success",
        http_status=200,
        latency_ms=82,
        status_reason=None,
        now=probe_time,
    )
    db_session.commit()

    health_response = client.get(
        "/api/v1/admin/cookie-session/health",
        headers=headers | {"X-Request-ID": "req-fr03-health-read"},
        params={"login_source": "xueqiu", "session_id": session_row.cookie_session_id},
    )

    assert health_response.status_code == 200
    body = health_response.json()
    assert body["success"] is True
    assert body["request_id"] == "req-fr03-health-read"
    assert body["data"]["status"] == "ok"
    assert body["data"]["status_reason"] is None
    assert datetime.fromisoformat(body["data"]["last_refresh_at"])


def test_fr03_expires_at_format(client, db_session, create_user):
    headers = _auth_headers(client, create_user)
    cookie_string = "SUB=plain-secret; SUBP=another-secret"

    response = client.post(
        "/api/v1/admin/cookie-session",
        headers=headers | {"X-Request-ID": "req-fr03-expires"},
        json={"login_source": "weibo", "cookie_string": cookie_string},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["success"] is True
    assert body["request_id"] == "req-fr03-expires"
    assert body["data"]["status"] == "saved"

    expires_at = datetime.fromisoformat(body["data"]["expires_at"])
    assert expires_at.tzinfo is not None
    ttl = expires_at - datetime.now(expires_at.tzinfo)
    assert timedelta(hours=23) <= ttl <= timedelta(hours=24, minutes=2)
    assert cookie_string not in response.text

    admin_operation = Base.metadata.tables["admin_operation"]
    audit_log = Base.metadata.tables["audit_log"]
    operation_row = db_session.execute(
        admin_operation.select().where(admin_operation.c.request_id == "req-fr03-expires")
    ).first()
    audit_row = db_session.execute(audit_log.select().where(audit_log.c.request_id == "req-fr03-expires")).first()
    assert operation_row is not None
    assert audit_row is not None
    assert cookie_string not in str(operation_row.after_snapshot)
    assert cookie_string not in str(audit_row.after_snapshot)


def test_fr03_douyin_ttl_24h(client, create_user):
    headers = _auth_headers(client, create_user)
    response = client.post(
        "/api/v1/admin/cookie-session",
        headers=headers | {"X-Request-ID": "req-fr03-douyin-ttl"},
        json={"login_source": "douyin", "cookie_string": "ttwid=secret"},
    )

    assert response.status_code == 201
    expires_at = datetime.fromisoformat(response.json()["data"]["expires_at"])
    ttl = expires_at - datetime.now(expires_at.tzinfo)
    assert timedelta(hours=23) <= ttl <= timedelta(hours=24, minutes=2)


def test_fr03_kuaishou_ttl_24h(client, create_user):
    headers = _auth_headers(client, create_user)
    response = client.post(
        "/api/v1/admin/cookie-session",
        headers=headers | {"X-Request-ID": "req-fr03-kuaishou-ttl"},
        json={"login_source": "kuaishou", "cookie_string": "kpn=secret"},
    )

    assert response.status_code == 201
    expires_at = datetime.fromisoformat(response.json()["data"]["expires_at"])
    ttl = expires_at - datetime.now(expires_at.tzinfo)
    assert timedelta(hours=23) <= ttl <= timedelta(hours=24, minutes=2)


def test_fr03_status_reason_on_fail(client, db_session, create_user):
    headers = _auth_headers(client, create_user)
    upsert_response = client.post(
        "/api/v1/admin/cookie-session",
        headers=headers,
        json={"login_source": "douyin", "cookie_string": "ttwid=secret"},
    )
    assert upsert_response.status_code == 201

    session_row = _latest_cookie_session_row(db_session, "douyin")
    assert session_row is not None

    record_cookie_probe(
        db_session,
        cookie_session_id=session_row.cookie_session_id,
        outcome="failed",
        http_status=401,
        latency_ms=120,
        status_reason="probe_http_401",
        now=datetime.now(timezone.utc),
    )
    db_session.commit()

    response = client.get(
        "/api/v1/admin/cookie-session/health",
        headers=headers,
        params={"login_source": "douyin", "session_id": session_row.cookie_session_id},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["status"] == "fail"
    assert body["data"]["status_reason"] == "probe_http_401"


def test_fr03_cookie_session_requires_admin_auth(client):
    response = client.post(
        "/api/v1/admin/cookie-session",
        json={"login_source": "xueqiu", "cookie_string": "xq_a_token=secret-cookie"},
    )
    assert response.status_code == 401
    assert response.json()["error_code"] == "UNAUTHORIZED"


def test_fr03_cookie_session_health_requires_admin_auth(client):
    response = client.get(
        "/api/v1/admin/cookie-session/health",
        params={"login_source": "xueqiu"},
    )
    assert response.status_code == 401
    assert response.json()["error_code"] == "UNAUTHORIZED"


def test_fr03_cookie_session_forbids_normal_user(client, create_user):
    headers = _auth_headers(client, create_user, role="user")
    response = client.post(
        "/api/v1/admin/cookie-session",
        headers=headers,
        json={"login_source": "xueqiu", "cookie_string": "xq_a_token=secret-cookie"},
    )
    assert response.status_code == 403
    assert response.json()["error_code"] == "FORBIDDEN"


def test_fr03_cookie_session_health_forbids_normal_user(client, create_user):
    headers = _auth_headers(client, create_user, role="user")
    response = client.get(
        "/api/v1/admin/cookie-session/health",
        headers=headers,
        params={"login_source": "xueqiu"},
    )
    assert response.status_code == 403
    assert response.json()["error_code"] == "FORBIDDEN"


# ──────────────────────────────────────────────────────────────
# FR03-COOKIE-03 EXPIRING/EXPIRED 状态可达
# ──────────────────────────────────────────────────────────────

def test_fr03_expiring_state_reachable(db_session):
    """expires_at 接近（<=30分钟）→ ACTIVE 迁移到 EXPIRING。"""
    from app.services.cookie_session_ssot import _transition_expiring_sessions

    table = Base.metadata.tables["cookie_session"]
    now = datetime.now(timezone.utc)
    session_id = str(__import__("uuid").uuid4())
    db_session.execute(
        table.insert().values(
            cookie_session_id=session_id,
            provider="weibo",
            account_key="test_account",
            status="ACTIVE",
            cookie_blob="test",
            expires_at=now + timedelta(minutes=10),  # 10分钟后过期
            created_at=now,
            updated_at=now,
        )
    )
    db_session.commit()

    _transition_expiring_sessions(db_session)

    row = db_session.execute(
        table.select().where(table.c.cookie_session_id == session_id)
    ).mappings().one()
    assert row["status"] == "EXPIRING"


def test_fr03_expired_state_reachable(db_session):
    """expires_at 已过期 → ACTIVE 迁移到 EXPIRED。"""
    from app.services.cookie_session_ssot import _transition_expiring_sessions

    table = Base.metadata.tables["cookie_session"]
    now = datetime.now(timezone.utc)
    session_id = str(__import__("uuid").uuid4())
    db_session.execute(
        table.insert().values(
            cookie_session_id=session_id,
            provider="xueqiu",
            account_key="test_account",
            status="ACTIVE",
            cookie_blob="test",
            expires_at=now - timedelta(hours=1),  # 已过期1小时
            created_at=now,
            updated_at=now,
        )
    )
    db_session.commit()

    _transition_expiring_sessions(db_session)

    row = db_session.execute(
        table.select().where(table.c.cookie_session_id == session_id)
    ).mappings().one()
    assert row["status"] == "EXPIRED"
