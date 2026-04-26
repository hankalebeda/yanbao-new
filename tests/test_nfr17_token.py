"""NFR-17 token security regression tests."""

import pytest
from datetime import datetime, timedelta, timezone

pytestmark = pytest.mark.feature("NFR-17-TOKEN-ROTATION")

from app.models import AccessTokenLease


def test_nfr17_refresh_rotation_once_only(client, create_user):
    """refresh token 轮转后旧 token 不可复用。"""
    create_user(email="nfr17@example.com", password="Nfr17Pass!")
    login_resp = client.post(
        "/auth/login",
        json={"email": "nfr17@example.com", "password": "Nfr17Pass!"},
    )
    assert login_resp.status_code == 200
    tokens = login_resp.json()["data"]
    old_refresh = tokens["refresh_token"]

    r1 = client.post("/auth/refresh", json={"refresh_token": old_refresh})
    assert r1.status_code == 200
    new_tokens = r1.json()["data"]
    new_refresh = new_tokens["refresh_token"]
    assert new_refresh != old_refresh

    r2 = client.post("/auth/refresh", json={"refresh_token": old_refresh})
    assert r2.status_code in (401, 400)


def test_nfr17_access_claims_complete(client, create_user):
    """access_token 的 JWT claims 应包含完整用户字段。"""
    from app.core.security import decode_token

    create_user(email="nfr17claims@example.com", password="ClaimsPass1!", role="admin", tier="Pro")
    login_resp = client.post(
        "/auth/login",
        json={"email": "nfr17claims@example.com", "password": "ClaimsPass1!"},
    )
    assert login_resp.status_code == 200
    access_token = login_resp.json()["data"]["access_token"]

    claims = decode_token(access_token)
    assert "sub" in claims
    assert "exp" in claims
    assert claims["sub"]


def test_nfr17_expired_access_lease_blocks_still_valid_jwt(client, create_user, db_session):
    """JWT 未过期但 access lease 已过期时，服务端必须 fail-close。"""
    from app.core.security import decode_token

    account = create_user(
        email="lease-expired@example.com",
        password="Password123",
        tier="Pro",
        role="admin",
        email_verified=True,
    )
    login_resp = client.post(
        "/auth/login",
        json={"email": account["user"].email, "password": account["password"]},
    )
    assert login_resp.status_code == 200
    access_token = login_resp.json()["data"]["access_token"]
    claims = decode_token(access_token)
    lease = db_session.get(AccessTokenLease, claims["jti"])
    assert lease is not None
    lease.expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    db_session.commit()

    me_resp = client.get("/auth/me", headers={"Authorization": f"Bearer {access_token}"})
    assert me_resp.status_code == 401
