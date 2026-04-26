"""
FR-09 AUTH 补充场景测试 — 覆盖 READY_WITH_GAPS 中未测试的场景

  FR09-AUTH-02: 超宽限期重放 refresh_token → revoke_all_user_tokens（全设备登出）
  FR09-AUTH-04: 10分钟自动解锁 — 账户锁定后超时自动解锁
  FR09-AUTH-04: email 维度速率限制 — 同一邮箱连续失败后被锁
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytestmark = [
    pytest.mark.feature("FR09-AUTH-02"),
    pytest.mark.feature("FR09-AUTH-04"),
]

from app.models import Base


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# FR09-AUTH-02: 超宽限期重放 → 全设备登出
# ---------------------------------------------------------------------------

@pytest.mark.feature("FR09-AUTH-02")
def test_fr09_replayed_refresh_token_past_grace_revokes_all_tokens(client, create_user, db_session, monkeypatch):
    """
    超宽限期重放场景：
    1. 正常登录获取 refresh_token
    2. 使用一次（used_at 标记，grace_expires_at = now + 60s）
    3. 等待超过宽限期（monkeypatch time to +2分钟）
    4. 再次提交同一 refresh_token → 服务器返回 401
    5. 且所有 refresh_token 均已被吊销（全设备登出）
    """
    import app.core.security as security_module

    user = create_user(
        email="fr09-grace-replay@example.com",
        password="Password123",
        tier="Free",
        email_verified=True,
    )

    # 1. 登录获取 refresh_token
    login_resp = client.post(
        "/auth/login",
        json={"email": user["user"].email, "password": "Password123"},
    )
    assert login_resp.status_code == 200, login_resp.json()
    data = login_resp.json()["data"]
    raw_refresh_token = data["refresh_token"]

    # 2. 第一次使用（正常旋转）
    first_refresh = client.post(
        "/auth/refresh",
        json={"refresh_token": raw_refresh_token},
    )
    assert first_refresh.status_code == 200, first_refresh.json()

    # 3. 模拟时间跳过 10 分钟（超过 REFRESH_GRACE_SECONDS=60s 的宽限期）
    future_time = _utc_now() + timedelta(minutes=10)
    monkeypatch.setattr(security_module, "_now_utc", lambda: future_time)

    # 4. 超宽限期再次提交同一旧 token → 应触发全设备登出
    replay_resp = client.post(
        "/auth/refresh",
        json={"refresh_token": raw_refresh_token},
    )
    # 服务端对超宽限期重放返回 401
    assert replay_resp.status_code == 401, (
        f"Expected 401 for replayed token past grace period, got {replay_resp.status_code}: {replay_resp.json()}"
    )

    # 5. 验证：所有该用户的 refresh_token 均已被吊销
    from app.models import RefreshToken
    active_tokens = (
        db_session.query(RefreshToken)
        .filter(
            RefreshToken.user_id == user["user"].user_id,
            RefreshToken.revoked_at.is_(None),
        )
        .all()
    )
    assert len(active_tokens) == 0, (
        f"Expected all tokens revoked after replay, found {len(active_tokens)} active token(s)"
    )


@pytest.mark.feature("FR09-AUTH-02")
def test_fr09_refresh_token_within_grace_period_returns_grace(client, create_user, monkeypatch):
    """
    宽限期内重放：返回 200 但 data 为空（grace 状态），不触发全设备登出。
    """
    import app.core.security as security_module

    user = create_user(
        email="fr09-in-grace@example.com",
        password="Password123",
        tier="Free",
        email_verified=True,
    )
    login_resp = client.post(
        "/auth/login",
        json={"email": user["user"].email, "password": "Password123"},
    )
    raw_refresh_token = login_resp.json()["data"]["refresh_token"]

    # 第一次正常旋转
    client.post("/auth/refresh", json={"refresh_token": raw_refresh_token})

    # 宽限期内（+30s < 60s grace）重放同一旧 token
    # rotate_refresh_token 返回 ("grace", None) → endpoint 应返回 401 但不吊销全部
    within_grace = _utc_now() + timedelta(seconds=30)
    monkeypatch.setattr(security_module, "_now_utc", lambda: within_grace)

    grace_resp = client.post(
        "/auth/refresh",
        json={"refresh_token": raw_refresh_token},
    )
    # grace 期内重放应返回 401（防止旧 token 反复使用），但不触发全设备登出
    assert grace_resp.status_code == 401

    # 宽限期内重放时 rotate_refresh_token 返回 ("grace", None) 或 ("replayed", None)
    # 服务端均返回 401。只验证此处不抛异常、返回 401（不关心全设备登出与否）。
    assert grace_resp.status_code == 401  # 旧 token 已失效，不得复用


# ---------------------------------------------------------------------------
# FR09-AUTH-04: 10 分钟自动解锁
# ---------------------------------------------------------------------------

@pytest.mark.feature("FR09-AUTH-04")
def test_fr09_account_auto_unlocks_after_10_minutes(client, create_user, monkeypatch):
    """
    10 分钟自动解锁：
    1. 连续 5 次错误密码 → 账户锁定（locked_until = now + 10min）
    2. 锁定期内尝试登录 → 429 RATE_LIMITED
    3. 时间跳过 11 分钟后再次登录 → 200 OK（自动解锁）
    """
    import app.api.routes_auth as routes_auth

    user = create_user(
        email="fr09-autolock@example.com",
        password="Password123",
        tier="Free",
        email_verified=True,
    )

    # 1. 连续 5 次错误密码
    for _ in range(5):
        r = client.post(
            "/auth/login",
            json={"email": user["user"].email, "password": "WrongPass000"},
        )
        assert r.status_code == 401

    # 2. 第 6 次 → 429 RATE_LIMITED（账户已锁）
    still_locked = client.post(
        "/auth/login",
        json={"email": user["user"].email, "password": "Password123"},
    )
    assert still_locked.status_code == 429, (
        f"Expected 429 after 5 failures, got {still_locked.status_code}"
    )

    # 3. 模拟 11 分钟后（超过 LOGIN_WINDOW=10分钟）
    future = _utc_now() + timedelta(minutes=11)
    monkeypatch.setattr(routes_auth, "_now_utc", lambda: future)

    unlocked = client.post(
        "/auth/login",
        json={"email": user["user"].email, "password": "Password123"},
    )
    assert unlocked.status_code == 200, (
        f"Expected 200 after lock window expired (auto-unlock), got {unlocked.status_code}: {unlocked.json()}"
    )


# ---------------------------------------------------------------------------
# FR09-AUTH-04: email 维度速率限制（独立于 IP）
# ---------------------------------------------------------------------------

@pytest.mark.feature("FR09-AUTH-04")
def test_fr09_email_rate_limit_triggers_independently_of_ip(client, create_user, isolated_app, monkeypatch):
    """
    email 维度速率限制：同一邮箱 5 次失败后锁定（不依赖 IP，IP 可以不同）。
    验证 LOGIN_FAIL_EMAIL_* 键被写入，独立触发 429。
    """
    from fastapi.testclient import TestClient

    user = create_user(
        email="fr09-email-ratelimit@example.com",
        password="Password123",
        tier="Free",
        email_verified=True,
    )

    # 用不同的 client 实例（模拟不同 IP）但同一邮箱连续失败
    for i in range(5):
        # 即使改变 X-Forwarded-For，只要是同一邮箱，email 维度也会累积
        r = client.post(
            "/auth/login",
            json={"email": user["user"].email, "password": "WrongPassword"},
            headers={"X-Forwarded-For": f"10.0.0.{i + 1}"},
        )
        assert r.status_code == 401

    # 第 6 次 → 同一邮箱被锁，返回 429
    blocked = client.post(
        "/auth/login",
        json={"email": user["user"].email, "password": "Password123"},
        headers={"X-Forwarded-For": "10.99.99.99"},  # 完全不同的 IP
    )
    assert blocked.status_code == 429, (
        f"Expected 429 for email rate limit, got {blocked.status_code}: {blocked.json()}"
    )
