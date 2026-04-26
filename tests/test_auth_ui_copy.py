import asyncio
import json
from pathlib import Path

from sqlalchemy.orm import sessionmaker

import app.api.routes_auth as routes_auth
import app.services.oauth_service as oauth_service
from app.core.db import build_engine
from app.core.security import hash_password
from app.models import Base, User


def _install_temp_sessionlocal(tmp_path, monkeypatch):
    db_path = tmp_path / "auth-ui-copy.db"
    engine = build_engine(f"sqlite:///{db_path.as_posix()}")
    Base.metadata.create_all(bind=engine)
    testing_session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    monkeypatch.setattr(routes_auth, "SessionLocal", testing_session_local)
    return testing_session_local


def test_auth_register_success_message_is_business_facing(tmp_path, monkeypatch):
    _install_temp_sessionlocal(tmp_path, monkeypatch)

    response = asyncio.run(
        routes_auth.auth_register(
            routes_auth.RegisterRequest(email="copy-register@example.com", password="Password123")
        )
    )

    body = json.loads(response.body.decode("utf-8"))
    assert body["data"]["message"] == "注册成功"
    assert "联系管理员" not in body["data"]["message"]
    assert "激活链接" not in body["data"]["message"]


def test_auth_forgot_password_success_message_is_business_facing(tmp_path, monkeypatch):
    testing_session_local = _install_temp_sessionlocal(tmp_path, monkeypatch)
    with testing_session_local() as db:
        db.add(
            User(
                email="copy-forgot@example.com",
                password_hash=hash_password("Password123"),
                tier="Free",
                role="user",
                email_verified=True,
            )
        )
        db.commit()

    payload = asyncio.run(
        routes_auth.auth_forgot_password(
            routes_auth.ForgotPasswordRequest(email="copy-forgot@example.com")
        )
    )

    assert payload["data"]["message"] == "若该邮箱已注册，重置请求已提交，请按后续指引完成密码重置。"
    assert payload["data"]["delivery_status"] == "manual_reset_required"
    assert "联系管理员" not in payload["data"]["message"]
    assert "当前环境" not in payload["data"]["message"]


def test_auth_oauth_provider_message_hides_impl_detail(monkeypatch):
    monkeypatch.setattr(oauth_service, "get_oauth_authorize_url", lambda provider: None)

    payload = asyncio.run(routes_auth.auth_oauth_providers())

    assert payload["data"]["provider_status"] == "provider-not-configured"
    assert payload["data"]["message"] == "当前暂未提供可用的第三方登录方式，请使用邮箱登录。"
    assert "未接入真实第三方登录" not in payload["data"]["message"]
    assert "live" not in payload["data"]["message"].lower()


def test_login_template_uses_inline_friendly_error_message():
    text = Path("app/web/templates/login.html").read_text(encoding="utf-8")
    assert 'id="login-message"' in text
    assert "showLoginMessage(getLoginErrorMessage(res))" in text
    assert "alert(res && res.message" not in text
    assert "/auth/oauth/exchange" not in text
    assert "temp_code" not in text
    assert "当前未接入真实第三方登录" not in text
    assert "当前暂未提供可用的第三方登录方式，请使用邮箱登录。" in text


def test_register_and_forgot_templates_hide_ops_copy():
    register_text = Path("app/web/templates/register.html").read_text(encoding="utf-8")
    forgot_text = Path("app/web/templates/forgot_password.html").read_text(encoding="utf-8")

    assert "联系管理员" not in register_text
    assert "当前环境不自动发邮件" not in forgot_text
    assert "联系管理员" not in forgot_text
    assert "若站点启用邮箱验证，注册后需完成激活。" in register_text
    assert "注册成功，请完成邮箱激活后再登录。" not in register_text
    assert "若该邮箱已注册，重置请求已提交，请按后续指引完成密码重置。" in forgot_text
