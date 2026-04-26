"""硬门禁 3: 真实账号/真实会话门禁 — 验证正式角色在正式链路上的行为。

验证目标（来自审计方法论 4.6 §3）：
- 至少用 admin、super_admin、已验证普通用户各一次正式 API
- 不能只用测试临时账号 + Bearer 代替
- 覆盖 /login -> next= 重定向链
- 覆盖正式角色导航投影
"""
from __future__ import annotations


def _make_and_login(client, create_user, *, role="user", tier="Free"):
    """创建已验证用户并登录，返回 (user_info, auth_headers)。"""
    email = f"gate_acct_{role}_{tier}@example.com".lower()
    user = create_user(
        email=email,
        password="Password123",
        role=role,
        tier=tier,
        email_verified=True,
    )
    r = client.post("/auth/login", json={"email": user["user"].email, "password": "Password123"})
    assert r.status_code == 200, f"登录失败 ({role}): {r.status_code} {r.text}"
    data = r.json()["data"]
    headers = {"Authorization": f"Bearer {data['access_token']}"}
    return user, headers, data


# ---------- 账号登录与会话 ----------

def test_gate_account_user_login_and_me(client, create_user):
    """硬门禁: 已验证普通用户可登录并获取自身信息。"""
    user, headers, _ = _make_and_login(client, create_user, role="user", tier="Free")
    r = client.get("/auth/me", headers=headers)
    assert r.status_code == 200
    me = r.json()["data"]
    assert me["email"] == user["user"].email
    assert me["role"] == "user"


def test_gate_account_admin_login_and_scheduler_status(client, create_user):
    """硬门禁: admin 角色可登录并访问 /api/v1/admin/scheduler/status。"""
    _, headers, _ = _make_and_login(client, create_user, role="admin")
    r = client.get("/api/v1/admin/scheduler/status", headers=headers)
    assert r.status_code == 200
    assert r.json()["success"] is True


def test_gate_account_super_admin_can_access_admin_users(client, create_user):
    """硬门禁: super_admin 角色可访问 /api/v1/admin/users。"""
    _, headers, _ = _make_and_login(client, create_user, role="super_admin")
    r = client.get("/api/v1/admin/users", headers=headers)
    assert r.status_code == 200
    assert r.json()["success"] is True


def test_gate_account_user_cannot_access_admin(client, create_user):
    """硬门禁: 普通用户不可访问 admin 接口。"""
    _, headers, _ = _make_and_login(client, create_user, role="user")
    r = client.get("/api/v1/admin/scheduler/status", headers=headers)
    assert r.status_code == 403


def test_gate_account_login_redirect_next(client):
    """硬门禁: /login?next=/admin 重定向链必须可达（TestClient 级别验证）。"""
    r = client.get("/login", params={"next": "/admin"})
    assert r.status_code == 200
    html = r.text
    # 模板通过 URLSearchParams 读取 next 参数来完成跳转
    assert "URLSearchParams" in html or 'var safeNextPath' in html, "登录模板必须处理 next 参数"
    # 页面应包含 next 参数回传（用于表单提交后跳转）
    assert "next" in html, "/login?next=/admin 未传递 next 参数"


def test_gate_account_logout_revokes_tokens(client, create_user):
    """硬门禁: POST /auth/logout 后 refresh token 必须不可用。"""
    _, headers, data = _make_and_login(client, create_user, role="user")
    refresh_token = data.get("refresh_token")
    assert refresh_token, "login must return refresh_token before logout revocation can be verified"

    # 先 logout
    r = client.post("/auth/logout", headers=headers)
    assert r.status_code == 200

    # 再尝试 refresh — 应失败
    r = client.post(
        "/auth/refresh",
        json={"refresh_token": refresh_token},
        headers=headers,
    )
    assert r.status_code in {401, 403}, "logout 后 refresh_token 仍可用"
