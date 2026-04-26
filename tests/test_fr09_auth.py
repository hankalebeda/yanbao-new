from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from http.cookies import SimpleCookie

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from app.core.db import build_engine
from app.core.security import decode_token
from app.models import Base, BillingOrder, User
from app.services.membership import (
    audit_membership_provider_truth,
    build_webhook_signature,
    classify_paid_null_expiry_user,
    reconcile_pending_orders,
    subscription_status,
)


def _auth_headers(client, create_user):
    account = create_user(
        email="billing-user@example.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": account["user"].email, "password": account["password"]},
    )
    assert login.status_code == 200
    return account, {"Authorization": f"Bearer {login.json()['data']['access_token']}"}


class _AuthPageParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements: list[dict] = []
        self._stack: list[dict] = []
        self.text_chunks: list[str] = []

    def handle_starttag(self, tag, attrs):
        element = {"tag": tag, "attrs": dict(attrs)}
        self.elements.append(element)
        self._stack.append(element)

    def handle_endtag(self, tag):
        if self._stack:
            self._stack.pop()

    def handle_data(self, data):
        if data and data.strip():
            self.text_chunks.append(data.strip())

    def has_selector(self, selector: str) -> bool:
        return any(_match_selector(element, selector) for element in self.elements)

    def text_contains(self, needle: str) -> bool:
        return needle in " ".join(self.text_chunks)


def _match_selector(element: dict, selector: str) -> bool:
    attrs = element.get("attrs", {})
    if selector.startswith("#"):
        return attrs.get("id") == selector[1:]
    if selector.startswith("."):
        return selector[1:] in attrs.get("class", "").split()
    if "." in selector:
        tag, cls = selector.split(".", 1)
        return element["tag"] == tag and cls in attrs.get("class", "").split()
    return element["tag"] == selector


def _parse_html(text: str) -> _AuthPageParser:
    parser = _AuthPageParser()
    parser.feed(text)
    return parser


@pytest.mark.feature("FR09-AUTH-01")
def test_fr09_register_returns_201(client):
    response = client.post(
        "/auth/register",
        headers={"X-Request-ID": "req-auth-register"},
        json={"email": "new-user@example.com", "password": "Password123"},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["success"] is True
    assert body["request_id"] == "req-auth-register"
    assert body["data"]["email"] == "new-user@example.com"
    assert body["data"]["tier"] == "Free"
    assert body["data"]["role"] == "user"
    assert body["data"]["email_verified"] is False
    assert body["data"]["message"] == "注册成功"
    assert "access_token=" in response.headers.get("set-cookie", "")
    # activation_url は P0-10 修正により response body に含まれない（URL 漏洩防止）
    assert "activation_url" not in body["data"]


@pytest.mark.feature("FR09-AUTH-01")
def test_fr09_register_establishes_cookie_session_for_page_gates(client):
    response = client.post(
        "/auth/register",
        json={"email": "cookie-register@example.com", "password": "Password123"},
    )

    assert response.status_code == 201
    assert "access_token=" in response.headers.get("set-cookie", "")

    login_page = client.get("/login", follow_redirects=False)
    register_page = client.get("/register", follow_redirects=False)

    assert login_page.status_code == 302
    assert login_page.headers.get("location") == "/"
    assert register_page.status_code == 302
    assert register_page.headers.get("location") == "/"


@pytest.mark.feature("FR09-AUTH-04")
@pytest.mark.feature("FR09-AUTH-08")
def test_fr09_refresh_token_rotation(client, create_user, monkeypatch):
    email = "rotate@example.com"
    password = "Password123"

    # 模拟邮件服务已配置，确保 EMAIL_NOT_VERIFIED 检查生效
    from app.core.config import settings as _settings
    monkeypatch.setattr(_settings, "user_email_enabled", True)
    monkeypatch.setattr(_settings, "user_email_smtp_host", "smtp.example.com")

    # 未激活用户无法登录（EMAIL_NOT_VERIFIED）
    client.post("/auth/register", json={"email": "unverified@example.com", "password": password})
    login_before_activation = client.post(
        "/auth/login",
        json={"email": "unverified@example.com", "password": password},
    )
    assert login_before_activation.status_code == 401
    assert login_before_activation.json()["error_code"] == "EMAIL_NOT_VERIFIED"

    # 已验证用户直接创建（P0-10：register 不再返回 activation_url）
    create_user(email=email, password=password, tier="Free", role="user", email_verified=True)

    login_response = client.post(
        "/auth/login",
        headers={"X-Request-ID": "req-auth-login"},
        json={"email": email, "password": password},
    )
    assert login_response.status_code == 200
    login_body = login_response.json()
    assert login_body["success"] is True
    assert login_body["request_id"] == "req-auth-login"
    refresh_token = login_body["data"]["refresh_token"]

    claims = decode_token(login_body["data"]["access_token"])
    for key in ("sub", "role", "tier", "exp", "jti", "sid", "type"):
        assert claims[key]
    assert claims["type"] == "access"
    assert claims["sub"] == login_body["data"]["user_id"]
    assert claims["role"] == "user"
    assert claims["tier"] == "Free"

    refresh_response = client.post(
        "/auth/refresh",
        json={"refresh_token": refresh_token},
    )
    assert refresh_response.status_code == 200
    refresh_body = refresh_response.json()
    assert refresh_body["success"] is True
    assert refresh_body["data"]["refresh_token"] != refresh_token

    replay_response = client.post(
        "/auth/refresh",
        json={"refresh_token": refresh_token},
    )
    assert replay_response.status_code == 401
    assert replay_response.json()["success"] is False
    assert replay_response.json()["error_code"] == "UNAUTHORIZED"
    assert replay_response.json()["data"] is None

    me_response = client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {refresh_body['data']['access_token']}"},
    )
    assert me_response.status_code == 200
    me_body = me_response.json()
    assert me_body["success"] is True
    assert me_body["data"]["email"] == email
    assert me_body["data"]["tier"] == "Free"
    assert me_body["data"]["user_id"] == me_body["data"]["id"]
    assert me_body["data"]["membership_level"] == "free"
    assert me_body["data"]["membership_expires_at"] == me_body["data"]["tier_expires_at"]


def test_fr09_auth_me_keeps_paid_membership_level_distinct_from_tier(client, create_user, db_session):
    from datetime import timedelta

    from app.models import User

    email = "paid-trial@example.com"
    password = "Password123"
    created = create_user(
        email=email,
        password=password,
        tier="Free",
        role="user",
        email_verified=True,
    )
    user = created["user"]
    user.membership_level = "monthly"
    user.membership_expires_at = user.created_at + timedelta(days=7)
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    login_response = client.post(
        "/auth/login",
        json={"email": email, "password": password},
    )
    assert login_response.status_code == 200
    token = login_response.json()["data"]["access_token"]

    me_response = client.get(
        "/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert me_response.status_code == 200
    me_body = me_response.json()["data"]
    assert me_body["tier"] == "Free"
    assert me_body["membership_level"] == "monthly"
    assert "advanced_reasoning" in me_body["permissions"]


def test_fr09_login_cookie_is_not_secure_on_localhost_http(client, create_user):
    create_user(
        email="localhost-cookie@example.com",
        password="Password123",
        tier="Pro",
        role="admin",
        email_verified=True,
    )

    response = client.post(
        "/auth/login",
        json={"email": "localhost-cookie@example.com", "password": "Password123"},
    )

    assert response.status_code == 200
    set_cookie = response.headers.get("set-cookie", "")
    parsed = SimpleCookie()
    parsed.load(set_cookie)
    morsel = parsed["access_token"]
    assert morsel.value
    assert morsel["httponly"]
    assert not morsel["secure"]


def test_fr09_cookie_helper_keeps_secure_on_https_origin(monkeypatch):
    from starlette.requests import Request
    from starlette.responses import Response

    from app.api.routes_auth import _set_auth_cookie
    from app.core.config import settings

    monkeypatch.setattr(settings, "debug", False)
    request = Request(
        {
            "type": "http",
            "scheme": "https",
            "method": "GET",
            "path": "/",
            "headers": [(b"host", b"testserver")],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 443),
        }
    )
    response = Response()
    _set_auth_cookie(response, "mock-token", request=request)
    set_cookie = response.headers.get("set-cookie", "")
    parsed = SimpleCookie()
    parsed.load(set_cookie)
    morsel = parsed["access_token"]
    assert morsel.value == "mock-token"
    assert morsel["secure"]


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_missing_email_fallback(client, db_session, monkeypatch):
    from app.core.config import settings
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(settings, "oauth_callback_base", "http://localhost:8000")
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "wechat-app-secret")

    async def mock_exchange_wechat_code(code: str):
        assert code == "mock-code"
        return "oauth-access-token", "wechat-user-001", None

    monkeypatch.setattr(oauth_service, "exchange_wechat_code", mock_exchange_wechat_code)

    # P1-15 修正：callback 前必须先持久化 OAuth state，否则返回 400
    raw_state1 = oauth_service.store_oauth_state(db_session, "wechat")
    db_session.commit()

    first = client.get(
        "/auth/oauth/wechat/callback",
        headers={"Accept": "application/json", "X-Request-ID": "req-fr09-oauth-1"},
        params={"code": "mock-code", "state": raw_state1},
    )
    assert first.status_code == 200
    first_body = first.json()
    assert first_body["success"] is True
    assert first_body["request_id"] == "req-fr09-oauth-1"
    assert first_body["data"]["is_new_user"] is True
    assert first_body["data"]["profile"]["email"] is None
    assert first_body["data"]["profile"]["email_verified"] is False
    assert first_body["data"]["profile"]["tier"] == "Free"
    assert first_body["data"]["tokens"]["access_token"]
    assert first_body["data"]["tokens"]["refresh_token"]

    # 第二次回调需要新 state（旧 state 已标记 used_at）
    raw_state2 = oauth_service.store_oauth_state(db_session, "wechat")
    db_session.commit()

    second = client.get(
        "/auth/oauth/wechat/callback",
        headers={"Accept": "application/json"},
        params={"code": "mock-code", "state": raw_state2},
    )
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["data"]["is_new_user"] is False
    assert second_body["data"]["profile"]["email"] is None

    oauth_table = Base.metadata.tables["oauth_identity"]
    app_user_table = Base.metadata.tables["app_user"]
    oauth_rows = db_session.execute(
        oauth_table.select().where(
            oauth_table.c.provider == "wechat",
            oauth_table.c.provider_user_id == "wechat-user-001",
        )
    ).fetchall()
    assert len(oauth_rows) == 1

    user_rows = db_session.execute(
        app_user_table.select().where(app_user_table.c.user_id == oauth_rows[0].user_id)
    ).fetchall()
    assert len(user_rows) == 1
    assert user_rows[0].email is None


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_callback_rolls_back_user_and_identity_when_token_issue_fails(client, db_session, monkeypatch):
    from app.core.config import settings
    import app.api.routes_auth as routes_auth
    import app.core.security as security
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(settings, "oauth_callback_base", "http://localhost:8000")
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "wechat-app-secret")

    async def mock_exchange_wechat_code(code: str):
        assert code == "rollback-code"
        return "oauth-access-token", "wechat-user-rollback", "rollback@example.com"

    fail_calls = {"count": 0}

    def fail_issue_token_pair(*args, **kwargs):
        fail_calls["count"] += 1
        raise RuntimeError("token issue failed")

    monkeypatch.setattr(oauth_service, "exchange_wechat_code", mock_exchange_wechat_code)
    monkeypatch.setattr(routes_auth, "issue_token_pair", fail_issue_token_pair)
    monkeypatch.setattr(security, "issue_token_pair", fail_issue_token_pair)

    state = oauth_service.store_oauth_state(db_session, "wechat")
    db_session.commit()

    client.get(
        "/auth/oauth/wechat/callback",
        headers={"Accept": "application/json"},
        params={"code": "rollback-code", "state": state},
    )

    assert fail_calls["count"] == 1

    oauth_table = Base.metadata.tables["oauth_identity"]
    app_user_table = Base.metadata.tables["app_user"]
    session_table = Base.metadata.tables["user_session"]
    refresh_table = Base.metadata.tables["refresh_token"]
    lease_table = Base.metadata.tables["access_token_lease"]
    oauth_rows = db_session.execute(
        oauth_table.select().where(
            oauth_table.c.provider == "wechat",
            oauth_table.c.provider_user_id == "wechat-user-rollback",
        )
    ).fetchall()
    user_rows = db_session.execute(
        app_user_table.select().where(app_user_table.c.email == "rollback@example.com")
    ).fetchall()
    session_rows = db_session.execute(session_table.select()).fetchall()
    refresh_rows = db_session.execute(refresh_table.select()).fetchall()
    lease_rows = db_session.execute(lease_table.select()).fetchall()
    assert oauth_rows == []
    assert user_rows == []
    assert session_rows == []
    assert refresh_rows == []
    assert lease_rows == []


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_callback_reuses_existing_identity_on_repeat_provider_user(client, db_session, monkeypatch):
    from app.core.config import settings
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(settings, "oauth_callback_base", "http://localhost:8000")
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "wechat-app-secret")

    async def mock_exchange_wechat_code(code: str):
        return "oauth-access-token", "wechat-user-repeat", "repeat@example.com"

    monkeypatch.setattr(oauth_service, "exchange_wechat_code", mock_exchange_wechat_code)

    first_state = oauth_service.store_oauth_state(db_session, "wechat")
    db_session.commit()
    first = client.get(
        "/auth/oauth/wechat/callback",
        headers={"Accept": "application/json"},
        params={"code": "repeat-first", "state": first_state},
    )
    assert first.status_code == 200
    assert first.json()["data"]["is_new_user"] is True

    second_state = oauth_service.store_oauth_state(db_session, "wechat")
    db_session.commit()
    second = client.get(
        "/auth/oauth/wechat/callback",
        headers={"Accept": "application/json"},
        params={"code": "repeat-second", "state": second_state},
    )
    assert second.status_code == 200
    assert second.json()["data"]["is_new_user"] is False

    oauth_table = Base.metadata.tables["oauth_identity"]
    app_user_table = Base.metadata.tables["app_user"]
    oauth_rows = db_session.execute(
        oauth_table.select().where(
            oauth_table.c.provider == "wechat",
            oauth_table.c.provider_user_id == "wechat-user-repeat",
        )
    ).fetchall()
    user_rows = db_session.execute(
        app_user_table.select().where(app_user_table.c.email == "repeat@example.com")
    ).fetchall()
    assert len(oauth_rows) == 1
    assert len(user_rows) == 1
    assert oauth_rows[0].user_id == user_rows[0].user_id


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_providers_only_expose_start_path(client, monkeypatch):
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(oauth_service, "get_oauth_authorize_url", lambda provider: f"https://oauth.example/{provider}")

    response = client.get("/auth/oauth/providers")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["contract_status"] == "contract-ready"
    assert data["provider_status"] == "configured"
    assert data["live_verified"] is False
    providers = data["providers"]
    assert providers
    for provider in providers:
        assert "url" not in provider
        assert provider["start_path"] == f"/auth/oauth/{provider['id']}/start"
        assert provider["method"] == "POST"


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_providers_report_not_configured_by_default(client, monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "oauth_callback_base", "")
    monkeypatch.setattr(settings, "qq_app_id", "")
    monkeypatch.setattr(settings, "qq_app_key", "")
    monkeypatch.setattr(settings, "wechat_app_id", "")
    monkeypatch.setattr(settings, "wechat_app_secret", "")

    response = client.get("/auth/oauth/providers")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["contract_status"] == "contract-ready"
    assert data["provider_status"] == "provider-not-configured"
    assert data["live_verified"] is False
    assert data["providers"] == []
    assert data["message"] == "当前暂未提供可用的第三方登录方式，请使用邮箱登录。"


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_start_redirects_with_persisted_state(client, db_session, monkeypatch):
    from app.core.config import settings
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(settings, "oauth_callback_base", "http://localhost:8000")
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "wechat-app-secret")

    captured: dict[str, str] = {}

    def mock_build_oauth_authorize_url(provider: str, state: str) -> str:
        captured["provider"] = provider
        captured["state"] = state
        return f"https://oauth.example/{provider}?state={state}"

    monkeypatch.setattr(oauth_service, "build_oauth_authorize_url", mock_build_oauth_authorize_url)

    response = client.post("/auth/oauth/wechat/start", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["location"] == f"https://oauth.example/wechat?state={captured['state']}"
    assert captured["provider"] == "wechat"
    assert captured["state"]

    token_table = Base.metadata.tables["auth_temp_token"]
    rows = db_session.execute(
        token_table.select().where(token_table.c.token_type == "OAUTH_STATE_WECHAT")
    ).fetchall()
    assert len(rows) == 1


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_start_scopes_next_cookie_to_state(client, monkeypatch):
    from app.core.config import settings
    import app.api.routes_auth as routes_auth
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(settings, "oauth_callback_base", "http://localhost:8000")
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "wechat-app-secret")

    captured: dict[str, str] = {}

    def mock_build_oauth_authorize_url(provider: str, state: str) -> str:
        captured["provider"] = provider
        captured["state"] = state
        return f"https://oauth.example/{provider}?state={state}"

    monkeypatch.setattr(oauth_service, "build_oauth_authorize_url", mock_build_oauth_authorize_url)

    response = client.post("/auth/oauth/wechat/start?next=/subscribe", follow_redirects=False)

    assert response.status_code == 302
    assert captured["provider"] == "wechat"
    cookie_key = routes_auth._oauth_next_cookie_key(captured["state"])
    set_cookie = response.headers.get("set-cookie", "")
    assert cookie_key is not None
    assert f"{cookie_key}=" in set_cookie
    assert "oauth_next=\"\"" in set_cookie


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_partial_provider_config_stays_fail_closed(client, db_session, monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "oauth_callback_base", "http://localhost:8000")
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "")

    response = client.get("/auth/oauth/providers")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["provider_status"] == "provider-not-configured"
    assert data["providers"] == []

    start = client.post("/auth/oauth/wechat/start", follow_redirects=False)
    assert start.status_code == 503
    body = start.json()
    assert body["success"] is False
    assert body["error_code"] == "OAUTH_PROVIDER_UNAVAILABLE"
    token_table = Base.metadata.tables["auth_temp_token"]
    rows = db_session.execute(
        token_table.select().where(token_table.c.token_type == "OAUTH_STATE_WECHAT")
    ).fetchall()
    assert rows == []


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_callback_returns_503_when_provider_not_ready(client, monkeypatch):
    from app.core.config import settings
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(settings, "oauth_callback_base", "http://localhost:8000")
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "")

    async def should_not_run(_code: str):
        raise AssertionError("exchange_wechat_code should not run when provider is not ready")

    monkeypatch.setattr(oauth_service, "exchange_wechat_code", should_not_run)

    response = client.get(
        "/auth/oauth/wechat/callback",
        headers={"Accept": "application/json"},
        params={"code": "mock-code", "state": "unused-state"},
    )
    assert response.status_code == 503
    body = response.json()
    assert body["success"] is False
    assert body["error_code"] == "OAUTH_PROVIDER_UNAVAILABLE"


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_html_callback_redirects_failure_back_to_login(client, monkeypatch):
    from app.core.config import settings
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(settings, "oauth_callback_base", "http://localhost:8000")
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "")

    async def should_not_run(_code: str):
        raise AssertionError("exchange_wechat_code should not run when provider is not ready")

    monkeypatch.setattr(oauth_service, "exchange_wechat_code", should_not_run)

    response = client.get(
        "/auth/oauth/wechat/callback",
        headers={"Accept": "text/html"},
        params={"code": "mock-code", "state": "unused-state"},
        follow_redirects=False,
    )

    assert response.status_code in (302, 303)
    location = response.headers["location"]
    assert location.startswith("http://localhost:8000/login?")
    assert "error=OAUTH_PROVIDER_UNAVAILABLE" in location


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_html_callback_rejects_suffix_error_codes(client, db_session, monkeypatch):
    from fastapi import HTTPException

    from app.core.config import settings
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(settings, "oauth_callback_base", "http://localhost:8000")
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "wechat-app-secret")

    async def raise_suffixed_http_exception(_code: str):
        raise HTTPException(status_code=503, detail="OAUTH_PROVIDER_UNAVAILABLE: provider not ready")

    monkeypatch.setattr(oauth_service, "exchange_wechat_code", raise_suffixed_http_exception)

    state = oauth_service.store_oauth_state(db_session, "wechat")
    db_session.commit()
    response = client.get(
        "/auth/oauth/wechat/callback",
        headers={"Accept": "text/html"},
        params={"code": "mock-code", "state": state},
        follow_redirects=False,
    )

    assert response.status_code in (302, 303)
    location = response.headers["location"]
    assert location.startswith("http://localhost:8000/login?")
    assert "error=UNAUTHORIZED" in location
    assert "OAUTH_PROVIDER_UNAVAILABLE%3A" not in location


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_json_callback_rejects_suffix_error_codes(client, db_session, monkeypatch):
    from fastapi import HTTPException

    from app.core.config import settings
    from app.core.error_codes import ERROR_CODE_WHITELIST
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(settings, "oauth_callback_base", "http://localhost:8000")
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "wechat-app-secret")

    def raise_suffixed_http_exception(*_args, **_kwargs):
        raise HTTPException(status_code=503, detail="UPSTREAM_TIMEOUT: provider returned noisy detail")

    monkeypatch.setattr(oauth_service, "verify_oauth_state", raise_suffixed_http_exception)

    state = oauth_service.store_oauth_state(db_session, "wechat")
    db_session.commit()
    response = client.get(
        "/auth/oauth/wechat/callback",
        headers={"Accept": "application/json"},
        params={"code": "mock-code", "state": state},
    )

    assert response.status_code == 503
    body = response.json()
    assert body["success"] is False
    assert body["error_code"] in ERROR_CODE_WHITELIST
    assert body["error_code"] != "UPSTREAM_TIMEOUT: provider returned noisy detail"
    assert ":" not in body["error_code"]


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_callback_uses_state_scoped_next_without_cross_tab_leak(client, db_session, monkeypatch):
    from app.core.config import settings
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(settings, "oauth_callback_base", "http://localhost:8000")
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "wechat-app-secret")

    states: list[str] = []

    def mock_build_oauth_authorize_url(provider: str, state: str) -> str:
        assert provider == "wechat"
        states.append(state)
        return f"https://oauth.example/{provider}?state={state}"

    async def mock_exchange_wechat_code(code: str):
        return "oauth-access-token", f"wechat-user-{code}", None

    monkeypatch.setattr(oauth_service, "build_oauth_authorize_url", mock_build_oauth_authorize_url)
    monkeypatch.setattr(oauth_service, "exchange_wechat_code", mock_exchange_wechat_code)

    first_start = client.post("/auth/oauth/wechat/start?next=/subscribe", follow_redirects=False)
    second_start = client.post("/auth/oauth/wechat/start?next=/profile", follow_redirects=False)

    assert first_start.status_code == 302
    assert second_start.status_code == 302
    assert len(states) == 2

    first_callback = client.get(
        "/auth/oauth/wechat/callback",
        headers={"Accept": "text/html"},
        params={"code": "first", "state": states[0]},
        follow_redirects=False,
    )
    second_callback = client.get(
        "/auth/oauth/wechat/callback",
        headers={"Accept": "text/html"},
        params={"code": "second", "state": states[1]},
        follow_redirects=False,
    )

    assert first_callback.status_code in (302, 303)
    assert second_callback.status_code in (302, 303)
    assert first_callback.headers["location"] == "/subscribe"
    assert second_callback.headers["location"] == "/profile"


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_html_callback_issues_session_directly_without_exchange_helper(client, db_session, monkeypatch):
    from app.core.config import settings
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(settings, "oauth_callback_base", "http://localhost:8000")
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "wechat-app-secret")

    async def mock_exchange_wechat_code(code: str):
        assert code == "mock-code"
        return "oauth-access-token", "wechat-user-html-001", None

    monkeypatch.setattr(oauth_service, "exchange_wechat_code", mock_exchange_wechat_code)

    state = oauth_service.store_oauth_state(db_session, "wechat")
    db_session.commit()

    session_table = Base.metadata.tables["user_session"]
    refresh_table = Base.metadata.tables["refresh_token"]
    lease_table = Base.metadata.tables["access_token_lease"]
    before_session = len(db_session.execute(session_table.select()).fetchall())
    before_refresh = len(db_session.execute(refresh_table.select()).fetchall())
    before_lease = len(db_session.execute(lease_table.select()).fetchall())

    callback = client.get(
        "/auth/oauth/wechat/callback",
        headers={"Accept": "text/html", "X-Request-ID": "req-fr09-oauth-html"},
        params={"code": "mock-code", "state": state},
        follow_redirects=False,
    )
    assert callback.status_code in (302, 303)
    location = callback.headers["location"]
    assert location == "/"
    assert "access_token=" in callback.headers.get("set-cookie", "")

    after_callback_session = len(db_session.execute(session_table.select()).fetchall())
    after_callback_refresh = len(db_session.execute(refresh_table.select()).fetchall())
    after_callback_lease = len(db_session.execute(lease_table.select()).fetchall())
    assert after_callback_session == before_session + 1
    assert after_callback_refresh == before_refresh + 1
    assert after_callback_lease == before_lease + 1

    exchange = client.get("/auth/oauth/exchange?temp_code=legacy-temp-code", follow_redirects=False)
    assert exchange.status_code == 410


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_oauth_json_callback_clears_state_scoped_next_cookie(client, monkeypatch):
    from app.core.config import settings
    import app.api.routes_auth as routes_auth
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(settings, "oauth_callback_base", "http://localhost:8000")
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "wechat-app-secret")

    captured: dict[str, str] = {}

    def mock_build_oauth_authorize_url(provider: str, state: str) -> str:
        captured["state"] = state
        return f"https://oauth.example/{provider}?state={state}"

    async def mock_exchange_wechat_code(code: str):
        assert code == "json-success"
        return "oauth-access-token", "wechat-user-json", None

    monkeypatch.setattr(oauth_service, "build_oauth_authorize_url", mock_build_oauth_authorize_url)
    monkeypatch.setattr(oauth_service, "exchange_wechat_code", mock_exchange_wechat_code)

    start = client.post("/auth/oauth/wechat/start?next=/subscribe", follow_redirects=False)
    assert start.status_code == 302

    callback = client.get(
        "/auth/oauth/wechat/callback",
        headers={"Accept": "application/json"},
        params={"code": "json-success", "state": captured["state"]},
    )

    assert callback.status_code == 200
    cookie_key = routes_auth._oauth_next_cookie_key(captured["state"])
    set_cookie = callback.headers.get("set-cookie", "")
    assert cookie_key is not None
    assert f"{cookie_key}=" in set_cookie
    assert "Max-Age=0" in set_cookie


def test_fr09_login_ip_rate_limit_is_persisted(client, isolated_app, create_user, db_session):
    user = create_user(
        email="persisted-ip-limit@example.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )

    for _ in range(5):
        response = client.post(
            "/auth/login",
            json={"email": user["user"].email, "password": "WrongPass123"},
        )
        assert response.status_code == 401

    with TestClient(isolated_app["app"], base_url="http://localhost") as second_client:
        blocked = second_client.post(
            "/auth/login",
            json={"email": "another-user@example.com", "password": "WrongPass123"},
        )

    assert blocked.status_code == 429
    token_table = Base.metadata.tables["auth_temp_token"]
    ip_failures = db_session.execute(
        token_table.select().where(token_table.c.token_type.like("LOGIN_FAIL_IP_%"))
    ).fetchall()
    assert len(ip_failures) >= 5


def test_fr09_sqlite_engine_enforces_wal_and_busy_timeout(tmp_path):
    engine = build_engine(f"sqlite:///{tmp_path / 'fr09-runtime.db'}")
    try:
        with engine.connect() as conn:
            journal_mode = conn.exec_driver_sql("PRAGMA journal_mode").scalar()
            busy_timeout = conn.exec_driver_sql("PRAGMA busy_timeout").scalar()
            foreign_keys = conn.exec_driver_sql("PRAGMA foreign_keys").scalar()

        assert str(journal_mode).lower() == "wal"
        assert int(busy_timeout) >= 30000
        assert int(foreign_keys) == 1
    finally:
        engine.dispose()


def test_fr09_login_db_locked_returns_503(client, monkeypatch):
    import app.api.routes_auth as routes_auth

    def mock_prune_login_failures(_db):
        raise OperationalError("DELETE FROM auth_temp_token", {}, Exception("database is locked"))

    monkeypatch.setattr(routes_auth, "_prune_login_failures", mock_prune_login_failures)

    response = client.post(
        "/auth/login",
        json={"email": "locked@example.com", "password": "Password123"},
    )

    assert response.status_code == 503
    body = response.json()
    assert body["success"] is False
    assert body["error_code"] == "UPSTREAM_TIMEOUT"
    assert body["error_message"] == "UPSTREAM_TIMEOUT"


def test_fr09_webhook_reconcile_poller(client, db_session, create_user):
    account, headers = _auth_headers(client, create_user)

    create_order_response = client.post(
        "/billing/create_order",
        headers=headers | {"X-Request-ID": "req-fr09-create-order"},
        json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
    )
    assert create_order_response.status_code == 201
    create_order_body = create_order_response.json()
    assert create_order_body["success"] is True
    assert create_order_body["request_id"] == "req-fr09-create-order"
    assert create_order_body["data"]["status"] == "CREATED"
    assert create_order_body["data"]["payment_url"] is None

    order_id = create_order_body["data"]["order_id"]
    order = db_session.get(BillingOrder, order_id)
    assert order is not None

    stale_created_at = datetime.now(timezone.utc) - timedelta(minutes=16)
    order.created_at = stale_created_at
    order.updated_at = stale_created_at
    order.expires_at = stale_created_at + timedelta(minutes=15)
    db_session.commit()

    def mock_provider_status_fetcher(order_row: BillingOrder):
        assert order_row.order_id == order_id
        return {
            "status": "PAID",
            "event_id": "evt-reconcile-001",
            "provider_order_id": "provider-order-001",
            "paid_amount": 29.9,
            "provider": "alipay",
        }

    reconcile_result = reconcile_pending_orders(
        db_session,
        now=stale_created_at + timedelta(minutes=16, seconds=10),
        provider_status_fetcher=mock_provider_status_fetcher,
    )
    db_session.commit()

    assert reconcile_result["checked_count"] == 1
    assert reconcile_result["reconciled_count"] == 1
    assert reconcile_result["expired_count"] == 0
    assert reconcile_result["items"][0]["order_id"] == order_id
    assert reconcile_result["items"][0]["status"] == "PAID"

    refreshed_order = db_session.get(BillingOrder, order_id)
    refreshed_user = db_session.get(User, account["user"].user_id)
    assert refreshed_order.status == "PAID"
    assert refreshed_order.provider_order_id == "provider-order-001"
    assert refreshed_user.tier == "Pro"
    assert refreshed_user.tier_expires_at is not None

    event_table = Base.metadata.tables["payment_webhook_event"]
    webhook_rows = db_session.execute(
        event_table.select().where(event_table.c.event_id == "evt-reconcile-001")
    ).fetchall()
    assert len(webhook_rows) == 1
    assert webhook_rows[0].processing_succeeded is True


def test_fr09_reconcile_poller_keeps_open_order_when_probe_missing(client, db_session, create_user):
    account, headers = _auth_headers(client, create_user)

    create_order_response = client.post(
        "/billing/create_order",
        headers=headers,
        json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
    )
    assert create_order_response.status_code == 201
    order_id = create_order_response.json()["data"]["order_id"]
    order = db_session.get(BillingOrder, order_id)
    assert order is not None

    stale_created_at = datetime.now(timezone.utc) - timedelta(minutes=16)
    order.created_at = stale_created_at
    order.updated_at = stale_created_at
    order.expires_at = stale_created_at + timedelta(minutes=15)
    db_session.commit()

    reconcile_result = reconcile_pending_orders(
        db_session,
        now=stale_created_at + timedelta(minutes=16, seconds=10),
    )
    db_session.commit()

    refreshed_order = db_session.get(BillingOrder, order_id)
    assert reconcile_result["checked_count"] == 1
    assert reconcile_result["reconciled_count"] == 0
    assert reconcile_result["expired_count"] == 0
    assert reconcile_result["items"][0]["status"] == "CREATED"
    assert reconcile_result["items"][0]["status_reason"] == "reconcile_probe_missing"
    assert refreshed_order.status == "CREATED"
    assert refreshed_order.status_reason == "reconcile_probe_missing"


def test_fr09_billing_poller_job_uses_configured_provider_probe(client, db_session, create_user, isolated_app, monkeypatch):
    from app.services import scheduler as scheduler_service

    account, headers = _auth_headers(client, create_user)

    create_order_response = client.post(
        "/billing/create_order",
        headers=headers,
        json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
    )
    assert create_order_response.status_code == 201
    order_id = create_order_response.json()["data"]["order_id"]
    order = db_session.get(BillingOrder, order_id)
    assert order is not None

    stale_created_at = datetime.now(timezone.utc) - timedelta(minutes=16)
    order.created_at = stale_created_at
    order.updated_at = stale_created_at
    order.expires_at = stale_created_at + timedelta(minutes=15)
    db_session.commit()

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload
            self.status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class _FakeSession:
        def get(self, endpoint, params=None, headers=None, timeout=None):
            assert endpoint == "https://billing-provider.test/status"
            assert params == {"order_id": order_id, "provider": "alipay"}
            assert headers == {"Authorization": "Bearer billing-token"}
            assert timeout == 3.5
            return _FakeResponse(
                {
                    "status": "PAID",
                    "event_id": "evt-poller-001",
                    "provider_order_id": "provider-order-001",
                    "paid_amount": 29.9,
                    "provider": "alipay",
                    "tier_id": "Pro",
                }
            )

    monkeypatch.setattr(scheduler_service, "SessionLocal", isolated_app["sessionmaker"])
    monkeypatch.setattr(scheduler_service, "_record_task_run", lambda *args, **kwargs: "task-run-billing")
    monkeypatch.setattr(scheduler_service, "_mark_task_success", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler_service, "_mark_task_failed", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler_service.requests, "Session", lambda: _FakeSession())
    monkeypatch.setenv("BILLING_PROVIDER_STATUS_URL", "https://billing-provider.test/status")
    monkeypatch.setenv("BILLING_PROVIDER_STATUS_TOKEN", "billing-token")
    monkeypatch.setenv("BILLING_PROVIDER_STATUS_TIMEOUT_SECONDS", "3.5")

    scheduler_service._billing_poller_job()

    db_session.expire_all()
    refreshed_order = db_session.get(BillingOrder, order_id)
    refreshed_user = db_session.get(User, account["user"].user_id)

    assert refreshed_order is not None
    assert refreshed_order.status == "PAID"
    assert refreshed_order.provider_order_id == "provider-order-001"
    assert refreshed_order.status_reason is None
    assert refreshed_user is not None
    assert refreshed_user.tier == "Pro"
    assert refreshed_user.tier_expires_at is not None


# ──────────────────────────────────────────────────────────────
# FR09-AUTH-03 logout 后旧 token 被拦截
# ──────────────────────────────────────────────────────────────

@pytest.mark.feature("FR09-AUTH-03")
def test_fr09_logout_revokes_access(client, create_user):
    """POST /auth/logout → 旧 access_token 不可再使用。"""
    account = create_user(
        email="logout-test@example.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": account["user"].email, "password": account["password"]},
    )
    assert login.status_code == 200
    token = login.json()["data"]["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # logout
    logout_resp = client.post("/auth/logout", headers=headers)
    assert logout_resp.status_code in (200, 204)

    # 旧 token 应已失效
    me_resp = client.get("/auth/me", headers=headers)
    assert me_resp.status_code == 401


# ──────────────────────────────────────────────────────────────
# FR09-AUTH-06 forgot-password → 200（防枚举）
# ──────────────────────────────────────────────────────────────


@pytest.mark.feature("FR09-BILLING-02")
def test_fr09_billing_webhook_rejects_invalid_signature(client, create_user):
    account, headers = _auth_headers(client, create_user)

    create_order_response = client.post(
        "/billing/create_order",
        headers=headers,
        json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
    )
    assert create_order_response.status_code == 201
    order_id = create_order_response.json()["data"]["order_id"]

    response = client.post(
        "/billing/webhook",
        headers={"Webhook-Signature": "invalid-signature"},
        json={
            "event_id": "evt-invalid-signature",
            "order_id": order_id,
            "user_id": account["user"].user_id,
            "tier_id": "Pro",
            "paid_amount": 29.9,
            "provider": "alipay",
            "signature": "invalid-signature",
        },
    )

    assert response.status_code == 400
    body = response.json()
    assert body["success"] is False
    assert body["error_code"] == "PAYMENT_SIGNATURE_INVALID"
    assert body["error_message"] == "PAYMENT_SIGNATURE_INVALID"


@pytest.mark.feature("FR09-BILLING-02")
def test_fr09_billing_webhook_marks_duplicate_event_without_double_grant(client, db_session, create_user):
    account, headers = _auth_headers(client, create_user)

    create_order_response = client.post(
        "/billing/create_order",
        headers=headers,
        json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
    )
    assert create_order_response.status_code == 201
    order_id = create_order_response.json()["data"]["order_id"]

    payload = {
        "event_id": "evt-duplicate-webhook",
        "order_id": order_id,
        "user_id": account["user"].user_id,
        "tier_id": "Pro",
        "paid_amount": 29.9,
        "provider": "alipay",
    }
    signature = build_webhook_signature(
        payload["event_id"],
        payload["order_id"],
        payload["user_id"],
        payload["tier_id"],
        payload["paid_amount"],
        payload["provider"],
    )

    first = client.post(
        "/billing/webhook",
        headers={"Webhook-Signature": signature},
        json={**payload, "signature": signature},
    )
    assert first.status_code == 200
    assert first.json()["data"]["duplicate"] is False

    second = client.post(
        "/billing/webhook",
        headers={"Webhook-Signature": signature},
        json={**payload, "signature": signature},
    )
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["success"] is True
    assert second_body["data"]["duplicate"] is True
    assert second_body["data"]["status_reason"] == "duplicate_event_ignored"

    event_table = Base.metadata.tables["payment_webhook_event"]
    webhook_row = db_session.execute(
        event_table.select().where(event_table.c.event_id == payload["event_id"])
    ).mappings().one()
    assert webhook_row["processing_succeeded"] is True
    assert webhook_row["duplicate_count"] == 1

    order = db_session.get(BillingOrder, order_id)
    assert order is not None
    assert order.status == "PAID"
    assert order.granted_tier == "Pro"


@pytest.mark.feature("FR09-BILLING-02")
def test_fr09_billing_webhook_rolls_back_event_and_order_on_internal_failure(client, db_session, create_user, monkeypatch):
    account, headers = _auth_headers(client, create_user)

    create_order_response = client.post(
        "/billing/create_order",
        headers=headers,
        json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
    )
    assert create_order_response.status_code == 201
    order_id = create_order_response.json()["data"]["order_id"]

    payload = {
        "event_id": "evt-rollback-webhook",
        "order_id": order_id,
        "user_id": account["user"].user_id,
        "tier_id": "Pro",
        "paid_amount": 29.9,
        "provider": "alipay",
    }
    signature = build_webhook_signature(
        payload["event_id"],
        payload["order_id"],
        payload["user_id"],
        payload["tier_id"],
        payload["paid_amount"],
        payload["provider"],
    )

    from app.services import membership as membership_service

    def _boom(*args, **kwargs):
        raise ValueError("INTERNAL_FAILURE")

    monkeypatch.setattr(membership_service, "grant_membership_order_entitlement", _boom)

    response = client.post(
        "/billing/webhook",
        headers={"Webhook-Signature": signature},
        json={**payload, "signature": signature},
    )

    assert response.status_code == 500
    body = response.json()
    assert body["success"] is False
    assert body["error_code"] == "INTERNAL_ERROR"
    assert body["error_message"] == "INTERNAL_ERROR"

    event_table = Base.metadata.tables["payment_webhook_event"]
    assert db_session.execute(
        event_table.select().where(event_table.c.event_id == payload["event_id"])
    ).fetchall() == []

    order = db_session.get(BillingOrder, order_id)
    assert order is not None
    assert order.status == "CREATED"
    assert order.granted_tier is None
    refreshed_user = db_session.get(User, account["user"].user_id)
    assert refreshed_user is not None
    assert refreshed_user.tier == "Free"
@pytest.mark.feature("FR09-AUTH-06")
def test_fr09_forgot_password_anti_enum(client):
    """POST /auth/forgot-password → 不存在的邮箱也返回200（防枚举）。"""
    resp = client.post(
        "/auth/forgot-password",
        json={"email": "nonexistent-xyzzy@example.com"},
    )
    # 应返回 200（不泄露用户是否存在）
    assert resp.status_code == 200


# ──────────────────────────────────────────────────────────────
# FR09-BILLING-01 create_order 基础断言
# ──────────────────────────────────────────────────────────────

@pytest.mark.feature("FR09-BILLING-01")
def test_fr09_billing_create_order_basic(client, create_user):
    """Pro tier 用户无法重复订购（409）。"""
    account = create_user(
        email="billing-pro@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": account["user"].email, "password": account["password"]},
    )
    token = login.json()["data"]["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    resp = client.post(
        "/billing/create_order",
        json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
        headers=headers,
    )
    # 已是Pro → 409/422 or endpoint not yet implemented → 404/405
    assert resp.status_code == 409
    body = resp.json()
    assert body["success"] is False
    assert body["error_code"] == "TIER_ALREADY_ACTIVE"


@pytest.mark.feature("FR09-BILLING-01")
def test_fr09_billing_create_order_provider_not_configured_projects_to_invalid_payload(
    client,
    create_user,
    monkeypatch,
):
    from app.core.config import settings

    monkeypatch.setattr(settings, "enable_mock_billing", False)
    account = create_user(
        email="billing-free@example.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": account["user"].email, "password": account["password"]},
    )
    token = login.json()["data"]["access_token"]

    response = client.post(
        "/billing/create_order",
        json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 503
    body = response.json()
    assert body["success"] is False
    assert body["error_code"] == "PAYMENT_PROVIDER_NOT_CONFIGURED"
    assert body["error_message"] == "PAYMENT_PROVIDER_NOT_CONFIGURED"


@pytest.mark.feature("FR09-BILLING-02")
def test_fr09_billing_webhook_projects_business_validation_failures_to_invalid_payload(client, create_user):
    account, headers = _auth_headers(client, create_user)

    create_order_response = client.post(
        "/billing/create_order",
        headers=headers,
        json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
    )
    assert create_order_response.status_code == 201
    order_id = create_order_response.json()["data"]["order_id"]
    invalid_user_id = "00000000-0000-0000-0000-000000000999"
    payload = {
        "event_id": "evt-invalid-user",
        "order_id": order_id,
        "user_id": invalid_user_id,
        "tier_id": "Pro",
        "paid_amount": 29.9,
        "provider": "alipay",
    }
    signature = build_webhook_signature(
        payload["event_id"],
        payload["order_id"],
        payload["user_id"],
        payload["tier_id"],
        payload["paid_amount"],
        payload["provider"],
    )

    response = client.post(
        "/billing/webhook",
        headers={"Webhook-Signature": signature},
        json={**payload, "signature": signature},
    )

    assert response.status_code == 422
    body = response.json()
    assert body["success"] is False
    assert body["error_code"] == "INVALID_PAYLOAD"
    assert body["error_message"] == "INVALID_PAYLOAD"


@pytest.mark.feature("FR09-BILLING-02")
def test_fr09_billing_webhook_fails_closed_when_signature_verifier_not_ready(client, create_user, monkeypatch):
    from app.core.config import settings

    account, headers = _auth_headers(client, create_user)

    create_order_response = client.post(
        "/billing/create_order",
        headers=headers,
        json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
    )
    assert create_order_response.status_code == 201
    order_id = create_order_response.json()["data"]["order_id"]

    monkeypatch.setattr(settings, "billing_webhook_secret", "")
    response = client.post(
        "/billing/webhook",
        headers={"Webhook-Signature": "unverified-signature"},
        json={
            "event_id": "evt-secret-missing",
            "order_id": order_id,
            "user_id": account["user"].user_id,
            "tier_id": "Pro",
            "paid_amount": 29.9,
            "provider": "alipay",
            "signature": "unverified-signature",
        },
    )

    assert response.status_code == 400
    body = response.json()
    assert body["success"] is False
    assert body["error_code"] == "PAYMENT_SIGNATURE_INVALID"
    assert body["error_message"] == "PAYMENT_SIGNATURE_INVALID"


@pytest.mark.feature("FR09-AUTH-06")
def test_fr09_forgot_password_existing_and_missing_share_same_fail_closed_payload(client, create_user, db_session):
    account = create_user(
        email="forgot-existing@example.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )

    existing = client.post("/auth/forgot-password", json={"email": account["user"].email})
    missing = client.post("/auth/forgot-password", json={"email": "forgot-missing@example.com"})

    assert existing.status_code == 200
    assert missing.status_code == 200
    assert existing.json()["data"] == missing.json()["data"]

    token_table = Base.metadata.tables["auth_temp_token"]
    reset_rows = db_session.execute(
        token_table.select().where(
            token_table.c.user_id == account["user"].user_id,
            token_table.c.token_type == "PASSWORD_RESET",
        )
    ).fetchall()
    assert len(reset_rows) == 1


@pytest.mark.feature("FR09-BILLING-03")
def test_fr09_subscription_status_route_is_retired(client, create_user):
    account = create_user(
        email="pro-status@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
        tier_expires_at=None,
    )
    login = client.post(
        "/auth/login",
        json={"email": account["user"].email, "password": account["password"]},
    )
    assert login.status_code == 200
    headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}

    response = client.get(
        "/api/v1/membership/subscription/status",
        headers=headers,
        params={"user_id": account["user"].user_id},
    )

    assert response.status_code == 410


@pytest.mark.feature("FR09-BILLING-03")
def test_fr09_subscription_status_preserves_unknown_when_paid_tier_has_no_expiry(db_session, create_user):
    account = create_user(
        email="pro-unknown-status@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
        tier_expires_at=None,
    )

    payload = subscription_status(db_session, str(account["user"].user_id))

    assert payload["tier"] == "Pro"
    assert payload["status"] == "unknown"
    assert payload["plan_code"] is None
    assert payload["tier_expires_at"] is None
    assert payload["status_reason"] == "expiry_unconfirmed"


@pytest.mark.feature("FR09-BILLING-03")
def test_fr09_membership_truth_audit_keeps_orderless_paid_null_expiry_non_repairable(db_session, create_user):
    account = create_user(
        email="truth-orderless@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
        tier_expires_at=None,
    )

    payload = classify_paid_null_expiry_user(db_session, account["user"])

    assert payload["repairable"] is False
    assert payload["repair_strategy"] is None
    assert payload["candidate_expires_at"] is None
    assert payload["classification"] == "missing_local_entitlement_fact"
    assert payload["evidence"]["truth_backed_paid_order_count"] == 0
    assert payload["evidence"]["successful_payment_event_count"] == 0


@pytest.mark.feature("FR09-BILLING-03")
def test_fr09_membership_truth_audit_repairs_synthetic_admin_without_local_fact(db_session, create_user):
    account = create_user(
        email="admin@example.com",
        password="Password123",
        tier="Pro",
        role="admin",
        email_verified=True,
        tier_expires_at=None,
    )

    payload = audit_membership_provider_truth(db_session, apply_safe_repairs=True)
    db_session.flush()
    row = payload["paid_tier_null_expiry"]["rows"][0]

    assert row["repairable"] is True
    assert row["repair_strategy"] == "downgrade_synthetic_admin_to_free"
    assert row["classification"] == "repairable_synthetic_admin_without_local_truth"
    assert row["repair_applied"] is True
    assert account["user"].tier == "Free"
    assert account["user"].tier_expires_at is None


@pytest.mark.feature("FR09-BILLING-03")
def test_fr09_membership_truth_audit_derives_candidate_from_single_truth_backed_order(db_session, create_user):
    from app.models import PaymentWebhookEvent

    paid_at = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
    account = create_user(
        email="truth-single-order@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
        tier_expires_at=None,
    )
    order = BillingOrder(
        user_id=account["user"].user_id,
        provider="alipay",
        expected_tier="Pro",
        period_months=1,
        granted_tier="Pro",
        amount_cny=29.9,
        currency="CNY",
        payment_url="",
        status="PAID",
        status_reason=None,
        created_at=paid_at - timedelta(minutes=1),
        paid_at=paid_at,
        expires_at=paid_at + timedelta(minutes=15),
        updated_at=paid_at,
    )
    db_session.add(order)
    db_session.flush()
    db_session.add(
        PaymentWebhookEvent(
            event_id="evt-truth-single-order",
            order_id=order.order_id,
            provider="alipay",
            event_type="PAYMENT_SUCCEEDED",
            payload_json={"source": "pytest"},
            processing_succeeded=True,
            duplicate_count=0,
            received_at=paid_at,
            processed_at=paid_at,
        )
    )
    db_session.commit()

    payload = classify_paid_null_expiry_user(db_session, account["user"])

    assert payload["repairable"] is True
    assert payload["repair_strategy"] == "single_truth_backed_paid_order"
    assert payload["candidate_expires_at"] == (paid_at + timedelta(days=30)).isoformat()
    assert payload["evidence"]["truth_backed_paid_order_count"] == 1
    assert payload["evidence"]["successful_payment_event_count"] == 1
    assert payload["evidence"]["candidate_order_id"] == order.order_id


@pytest.mark.feature("FR09-BILLING-03")
def test_fr09_membership_truth_audit_reports_zero_webhook_counts_when_table_empty(db_session, create_user):
    create_user(
        email="truth-empty-webhook@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
        tier_expires_at=None,
    )

    payload = audit_membership_provider_truth(db_session)

    assert payload["payment_webhook_event"]["total_count"] == 0
    assert payload["payment_webhook_event"]["processing_succeeded_true_count"] == 0
    assert payload["payment_webhook_event"]["processing_succeeded_false_count"] == 0


@pytest.mark.feature("FR09-BILLING-03")
def test_fr09_subscribe_page_serializes_unknown_subscription_state(client, create_user):
    account = create_user(
        email="pro-unknown-page@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
        tier_expires_at=None,
    )
    login = client.post(
        "/auth/login",
        json={"email": account["user"].email, "password": account["password"]},
    )
    assert login.status_code == 200
    headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}

    response = client.get("/subscribe", headers=headers)

    assert response.status_code == 200
    match = re.search(r"var subscriptionStatePayload = (.*?);", response.text)
    assert match is not None
    payload = json.loads(match.group(1))
    assert payload["tier"] == "Pro"
    assert payload["status"] == "unknown"
    assert payload["status_reason"] == "expiry_unconfirmed"


def test_fr09_subscribe_page_serializes_mock_provider_as_not_browser_checkout_ready(client, monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "enable_mock_billing", True)
    monkeypatch.setattr(settings, "alipay_app_id", "")
    monkeypatch.setattr(settings, "alipay_gateway_url", "")
    monkeypatch.setattr(settings, "wechat_pay_app_id", "")
    monkeypatch.setattr(settings, "wechat_pay_gateway_url", "")

    response = client.get("/subscribe")

    assert response.status_code == 200
    match = re.search(r"var billingCapability = (.*?);", response.text)
    assert match is not None
    payload = json.loads(match.group(1))
    assert payload["provider_status"] == "configured"
    assert payload["browser_checkout_ready"] is False
    assert {item["mode"] for item in payload["providers"]} == {"headless_mock"}


def test_fr09_subscribe_page_serializes_server_seeded_plans(client):
    from app.services.membership import get_plans_config

    response = client.get("/subscribe")

    assert response.status_code == 200
    match = re.search(r"var subscriptionPlansPayload = (.*?);", response.text)
    assert match is not None
    payload = json.loads(match.group(1))
    assert payload == get_plans_config()


def test_fr09_sim_dashboard_denies_unconfirmed_paid_membership(client, create_user):
    account = create_user(
        email="pro-unknown-sim@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
        tier_expires_at=None,
    )
    login = client.post(
        "/auth/login",
        json={"email": account["user"].email, "password": account["password"]},
    )
    assert login.status_code == 200
    headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}

    response = client.get("/portfolio/sim-dashboard", headers=headers)

    assert response.status_code == 403


def test_fr09_mock_pay_routes_retired(client, create_user):
    account, headers = _auth_headers(client, create_user)
    create_order_response = client.post(
        "/billing/create_order",
        headers=headers,
        json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
    )
    assert create_order_response.status_code == 201
    order_id = create_order_response.json()["data"]["order_id"]

    page_resp = client.get(f"/billing/mock-pay/{order_id}", headers=headers)
    confirm_resp = client.post(f"/billing/mock-pay/{order_id}/confirm", headers=headers)

    assert page_resp.status_code == 410
    assert confirm_resp.status_code == 410


test_fr09_mock_pay_routes_retired = pytest.mark.feature("OOS-MOCK-PAY-01")(
    test_fr09_mock_pay_routes_retired
)
test_fr09_mock_pay_routes_retired = pytest.mark.feature("OOS-MOCK-PAY-02")(
    test_fr09_mock_pay_routes_retired
)


@pytest.mark.feature("FR09-AUTH-07")
def test_fr09_login_page_shows_activation_success_banner(client):
    response = client.get("/login?activated=1")
    assert response.status_code == 200
    dom = _parse_html(response.text)
    assert dom.has_selector("#activation-success-message")
    assert dom.text_contains("邮箱已激活，请登录继续")


@pytest.mark.feature("FR09-AUTH-05")
def test_fr09_login_page_shows_oauth_error_banner(client):
    response = client.get("/login?error=INVALID_OAUTH_STATE")
    assert response.status_code == 200
    assert "第三方登录校验已失效，请重新发起登录。" in response.text
