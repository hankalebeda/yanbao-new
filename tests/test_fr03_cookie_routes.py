"""FR03-COOKIE: Cookie 会话管理 CRUD 端点测试。"""
from __future__ import annotations

import pytest


def _login_admin(client, create_user):
    admin = create_user(email="cookie-admin@test.com", role="admin")
    resp = client.post("/auth/login", json={"email": admin["user"].email, "password": admin["password"]})
    assert resp.status_code == 200
    return {"Authorization": f"Bearer {resp.json()['data']['access_token']}"}


@pytest.mark.feature("FR03-COOKIE-01")
def test_cookie_session_list_empty(client, create_user):
    """管理员可查看空的 cookie 会话列表。"""
    headers = _login_admin(client, create_user)
    resp = client.get("/api/v1/admin/cookie-sessions", headers=headers)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["total"] == 0
    assert data["items"] == []


@pytest.mark.feature("FR03-COOKIE-02")
def test_cookie_session_create_and_list(client, create_user):
    """管理员可创建 cookie 会话并在列表中看到。"""
    headers = _login_admin(client, create_user)

    # Create
    resp = client.post(
        "/api/v1/admin/cookie-sessions",
        json={"login_source": "weibo", "cookie_string": "test_cookie=abc123"},
        headers=headers,
    )
    assert resp.status_code == 200
    session_id = resp.json()["data"]["cookie_session_id"]
    assert session_id

    # List
    resp = client.get("/api/v1/admin/cookie-sessions", headers=headers)
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["total"] >= 1
    assert any(item["cookie_session_id"] == session_id for item in data["items"])


@pytest.mark.feature("FR03-COOKIE-03")
def test_cookie_session_detail(client, create_user):
    """管理员可查看单个 cookie 会话详情。"""
    headers = _login_admin(client, create_user)

    # Create first
    resp = client.post(
        "/api/v1/admin/cookie-sessions",
        json={"login_source": "xueqiu", "cookie_string": "xq_cookie=xyz"},
        headers=headers,
    )
    session_id = resp.json()["data"]["cookie_session_id"]

    # Detail
    resp = client.get(f"/api/v1/admin/cookie-sessions/{session_id}", headers=headers)
    assert resp.status_code == 200
    detail = resp.json()["data"]
    assert detail["provider"] == "xueqiu"
    assert detail["status"] == "ACTIVE"
    assert detail["cookie_present"] is True


@pytest.mark.feature("FR03-COOKIE-03")
def test_cookie_session_detail_not_found(client, create_user):
    """不存在的 session_id 返回 404。"""
    headers = _login_admin(client, create_user)
    resp = client.get("/api/v1/admin/cookie-sessions/nonexistent-id", headers=headers)
    assert resp.status_code == 404


@pytest.mark.feature("FR03-COOKIE-04")
def test_cookie_session_delete(client, create_user):
    """管理员可删除 cookie 会话。"""
    headers = _login_admin(client, create_user)

    # Create
    resp = client.post(
        "/api/v1/admin/cookie-sessions",
        json={"login_source": "douyin", "cookie_string": "dy_cookie=hello"},
        headers=headers,
    )
    session_id = resp.json()["data"]["cookie_session_id"]

    # Delete
    resp = client.delete(f"/api/v1/admin/cookie-sessions/{session_id}", headers=headers)
    assert resp.status_code == 200

    # Verify gone
    resp = client.get(f"/api/v1/admin/cookie-sessions/{session_id}", headers=headers)
    assert resp.status_code == 404


@pytest.mark.feature("FR03-COOKIE-04")
def test_cookie_session_delete_not_found(client, create_user):
    """删除不存在的 session_id 返回 404。"""
    headers = _login_admin(client, create_user)
    resp = client.delete("/api/v1/admin/cookie-sessions/nonexistent-id", headers=headers)
    assert resp.status_code == 404


def test_cookie_session_requires_admin(client, create_user):
    """非管理员用户不能访问 cookie 会话端点。"""
    user = create_user(email="regular@test.com", role="user")
    resp = client.post("/auth/login", json={"email": user["user"].email, "password": user["password"]})
    assert resp.status_code == 200
    headers = {"Authorization": f"Bearer {resp.json()['data']['access_token']}"}
    resp = client.get("/api/v1/admin/cookie-sessions", headers=headers)
    assert resp.status_code == 403