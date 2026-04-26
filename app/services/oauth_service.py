"""OAuth 第三方登录 (17 §2.1)：QQ / 微信 code 换 token、open_id 查/建 user"""
import hashlib
import json
import logging
import secrets
import urllib.parse
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx

from app.core.config import settings
from app.core.db import SessionLocal
from app.core.security import hash_password
from app.models import AuthTempToken, OAuthIdentity, User

logger = logging.getLogger(__name__)


def get_oauth_authorize_url(provider: str) -> str:
    """获取 QQ 或 WeChat 授权 URL。provider: qq | wechat"""
    base = (getattr(settings, "oauth_callback_base", None) or "").rstrip("/")
    cb = f"{base}/auth/oauth/callback"
    state = f"{provider}:{secrets.token_urlsafe(16)}"
    if provider == "qq":
        app_id = getattr(settings, "qq_app_id", None) or ""
        app_key = getattr(settings, "qq_app_key", None) or getattr(settings, "qq_app_secret", None) or ""
        if not app_id or not app_key:
            return ""
        params = {
            "response_type": "code",
            "client_id": app_id,
            "redirect_uri": cb,
            "state": state,
            "scope": "get_user_info",
        }
        return "https://graph.qq.com/oauth2.0/authorize?" + urllib.parse.urlencode(params)
    if provider == "wechat":
        app_id = getattr(settings, "wechat_app_id", None) or ""
        app_secret = getattr(settings, "wechat_app_secret", None) or ""
        if not app_id or not app_secret:
            return ""
        params = {
            "appid": app_id,
            "redirect_uri": cb,
            "response_type": "code",
            "scope": "snsapi_login",
            "state": state,
        }
        return "https://open.weixin.qq.com/connect/qrconnect?" + urllib.parse.urlencode(params) + "#wechat_redirect"
    return ""


def _parse_qq_callback(body: str) -> dict:
    """QQ 返回 callback( {...} ) 或 key=value&..."""
    data: dict = {}
    if "callback(" in body:
        start = body.find("(") + 1
        end = body.rfind(")")
        try:
            data = json.loads(body[start:end])
        except json.JSONDecodeError:
            pass
    elif "access_token=" in body:
        for part in body.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                data[k.strip()] = v.strip()
    return data


async def exchange_qq_code(code: str) -> tuple[str | None, str | None, str | None]:
    """QQ: code -> (access_token, openid, nickname)"""
    app_id = getattr(settings, "qq_app_id", None) or ""
    app_key = getattr(settings, "qq_app_key", None) or ""
    base = (getattr(settings, "oauth_callback_base", None) or "").rstrip("/")
    cb = f"{base}/auth/oauth/callback"
    url = "https://graph.qq.com/oauth2.0/token?grant_type=authorization_code&client_id=%s&client_secret=%s&code=%s&redirect_uri=%s" % (
        app_id,
        app_key,
        urllib.parse.quote(code),
        urllib.parse.quote(cb),
    )
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url)
        tok_data = _parse_qq_callback(r.text)
    access_token = tok_data.get("access_token")
    if not access_token:
        return None, None, None
    oid_url = f"https://graph.qq.com/oauth2.0/me?access_token={urllib.parse.quote(access_token)}"
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(oid_url)
        oid_data = _parse_qq_callback(r.text)
    openid = oid_data.get("openid")
    if not openid:
        return None, None, None
    ui_url = f"https://graph.qq.com/user/get_user_info?access_token={urllib.parse.quote(access_token)}&oauth_consumer_key={app_id}&openid={urllib.parse.quote(openid)}"
    nickname = None
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(ui_url)
        try:
            ui = r.json()
            if ui.get("ret") == 0:
                nickname = (ui.get("nickname") or "").strip()[:64] or None
        except Exception:
            pass
    return access_token, openid, nickname


async def exchange_wechat_code(code: str) -> tuple[str | None, str | None, str | None, str | None]:
    """微信: code -> (access_token, openid, unionid, email)"""
    app_id = getattr(settings, "wechat_app_id", None) or ""
    app_secret = getattr(settings, "wechat_app_secret", None) or ""
    url = "https://api.weixin.qq.com/sns/oauth2/access_token?appid=%s&secret=%s&code=%s&grant_type=authorization_code" % (
        app_id,
        app_secret,
        urllib.parse.quote(code),
    )
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url)
        try:
            resp = r.json()
        except Exception:
            return None, None, None, None
    openid = resp.get("openid")
    unionid = resp.get("unionid")
    if not openid:
        return None, None, None, None
    return resp.get("access_token"), openid, unionid, None


def get_or_create_oauth_user(provider: str, open_id: str, union_id: str | None, nickname: str | None, *, return_is_new: bool = False, db=None, email=None) -> User | None | tuple:
    """按 provider+open_id 查 oauth_identity，无则建 user 并写入 oauth_identity。
    
    If `db` is provided, uses that session and does NOT commit (caller controls transaction).
    If `db` is None, creates its own session and commits internally.
    """
    own_session = db is None
    if own_session:
        db = SessionLocal()
    try:
        db.expire_on_commit = False
        oa = db.query(OAuthIdentity).filter(OAuthIdentity.provider == provider, OAuthIdentity.provider_user_id == open_id).first()
        if oa:
            user = db.query(User).filter(User.user_id == oa.user_id).first()
            if user:
                # Ensure attributes are loaded before detaching
                _ = user.user_id, user.email, user.role, user.membership_level, user.tier, user.nickname
            return (user, False) if return_is_new else user
        user = User(
            email=email,
            phone=None,
            password_hash=hash_password(secrets.token_hex(32)),
            nickname=nickname,
            role="user",
            membership_level="free",
            membership_expires_at=None,
        )
        db.add(user)
        db.flush()
        oa = OAuthIdentity(
            oauth_identity_id=str(uuid4()),
            user_id=user.user_id,
            provider=provider,
            provider_user_id=open_id,
            provider_union_id=union_id,
        )
        db.add(oa)
        if own_session:
            db.commit()
            db.refresh(user)
        else:
            db.flush()
        return (user, True) if return_is_new else user
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


# ---------------------------------------------------------------------------
# CSRF-safe OAuth state helpers (P1-15)
# ---------------------------------------------------------------------------

def store_oauth_state(db, provider: str) -> str:
    """Persist a random OAuth state token; return raw state string."""
    raw_state = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(raw_state.encode()).hexdigest()
    now = datetime.now(timezone.utc)
    row = AuthTempToken(
        temp_token_id=str(uuid4()),
        user_id="__oauth__",
        token_type=f"OAUTH_STATE_{provider.upper()}",
        token_hash=token_hash,
        sent_at=now,
        expires_at=now + timedelta(minutes=10),
    )
    db.add(row)
    db.flush()
    return raw_state


def verify_oauth_state(db, provider: str, state: str) -> bool:
    """Verify an OAuth state token; mark as used on success."""
    token_hash = hashlib.sha256(state.encode()).hexdigest()
    token_type = f"OAUTH_STATE_{provider.upper()}"
    now = datetime.now(timezone.utc)
    row = (
        db.query(AuthTempToken)
        .filter(
            AuthTempToken.token_type == token_type,
            AuthTempToken.token_hash == token_hash,
            AuthTempToken.used_at.is_(None),
            AuthTempToken.expires_at > now,
        )
        .first()
    )
    if not row:
        return False
    row.used_at = now
    db.flush()
    return True


def build_oauth_authorize_url(provider: str, state: str) -> str:
    """Build OAuth authorize URL with explicit state parameter."""
    base = (getattr(settings, "oauth_callback_base", None) or "").rstrip("/")
    cb = f"{base}/auth/oauth/{provider}/callback"
    if provider == "qq":
        app_id = getattr(settings, "qq_app_id", None) or ""
        if not app_id:
            return ""
        params = {
            "response_type": "code",
            "client_id": app_id,
            "redirect_uri": cb,
            "state": state,
            "scope": "get_user_info",
        }
        return "https://graph.qq.com/oauth2.0/authorize?" + urllib.parse.urlencode(params)
    if provider == "wechat":
        app_id = getattr(settings, "wechat_app_id", None) or ""
        if not app_id:
            return ""
        params = {
            "appid": app_id,
            "redirect_uri": cb,
            "response_type": "code",
            "scope": "snsapi_login",
            "state": state,
        }
        return "https://open.weixin.qq.com/connect/qrconnect?" + urllib.parse.urlencode(params) + "#wechat_redirect"
    return ""
