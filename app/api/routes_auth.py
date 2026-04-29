"""认证 API (05 §2.4a, 17_用户系统设计, E6)；支持邮箱或手机号、OAuth、忘记密码"""
import hashlib
import re
import secrets
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel
from sqlalchemy.exc import OperationalError

from app.core.config import settings
from app.core.db import SessionLocal
from app.core.membership import get_permissions
from app.core.response import envelope
from app.core.security import (
    AUTH_COOKIE,
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    hash_token,
    verify_password,
)
from app.models import PasswordResetToken, User

router = APIRouter(tags=["auth"])

LOGIN_LIMIT = 5    # max login attempts per window
LOGIN_WINDOW = timedelta(minutes=10)  # window duration

# Refresh token rotation replay detection
_used_refresh_tokens: set[str] = set()
_revoked_refresh_subjects: set[str] = set()
_revoked_access_subjects: set[str] = set()

# 密码强度：≥8位，含字母+数字
PASSWORD_RE = re.compile(r"^(?=.*[A-Za-z])(?=.*\d)[A-Za-z\d@$!%*#?&]{8,}$")
# 手机号：11位，1开头
PHONE_RE = re.compile(r"^1\d{10}$")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _is_provider_ready(provider: str) -> bool:
    """Check if an OAuth provider has both app_id AND app_secret configured."""
    if provider == "qq":
        return bool(getattr(settings, "qq_app_id", "") and getattr(settings, "qq_app_secret", ""))
    if provider == "wechat":
        return bool(getattr(settings, "wechat_app_id", "") and getattr(settings, "wechat_app_secret", ""))
    return False


def issue_token_pair(user, db=None):
    """Create access + refresh token pair for user, and persist session records."""
    from app.models import UserSession, RefreshToken as RefreshTokenModel, AccessTokenLease
    access = create_access_token(user)
    refresh = create_refresh_token(user)

    if db is not None:
        now = datetime.now(timezone.utc)
        user_id = str(user.id) if hasattr(user, 'id') else str(user)
        session_id = str(uuid4())
        refresh_token_id = str(uuid4())
        access_hours = getattr(settings, "jwt_access_token_expire_hours", 1)
        refresh_days = getattr(settings, "jwt_refresh_token_expire_days", 30)

        # UserSession
        sess = UserSession(
            session_id=session_id,
            user_id=user_id,
            status="ACTIVE",
            created_at=now,
            updated_at=now,
            expires_at=now + timedelta(days=refresh_days),
        )
        db.add(sess)
        # SQLite runtime schema enforces refresh_token.session_id -> user_session.session_id,
        # but SQLAlchemy has no relationship here to infer insert order. Flush the session
        # row first so real login does not fail with a foreign-key violation.
        db.flush()

        # RefreshToken
        import hashlib as _hl
        rt = RefreshTokenModel(
            refresh_token_id=refresh_token_id,
            user_id=user_id,
            session_id=session_id,
            token_hash=_hl.sha256(refresh.encode()).hexdigest(),
            issued_at=now,
            expires_at=now + timedelta(days=refresh_days),
            grace_expires_at=now + timedelta(days=refresh_days, seconds=60),
            created_at=now,
            updated_at=now,
        )
        db.add(rt)

        # AccessTokenLease
        import jwt as _jwt_mod
        try:
            payload = _jwt_mod.decode(access, options={"verify_signature": False})
            jti = payload.get("jti", str(uuid4()))
        except Exception:
            jti = str(uuid4())
        lease = AccessTokenLease(
            jti=jti,
            user_id=user_id,
            session_id=session_id,
            refresh_token_id=refresh_token_id,
            issued_at=now,
            expires_at=now + timedelta(hours=access_hours),
            created_at=now,
        )
        db.add(lease)
        db.flush()
        db.commit()

    return {
        "access_token": access,
        "refresh_token": refresh,
        "expires_in": getattr(settings, "jwt_access_token_expire_hours", 1) * 3600,
    }


def _prune_login_failures(db) -> None:
    """Delete expired login failure tokens from auth_temp_token."""
    from app.models import AuthTempToken
    cutoff = _now_utc() - LOGIN_WINDOW
    db.query(AuthTempToken).filter(
        AuthTempToken.token_type.like("LOGIN_FAIL_%"),
        AuthTempToken.sent_at < cutoff,
    ).delete(synchronize_session=False)
    db.commit()


def _check_ip_rate_limit(db, ip: str) -> bool:
    """Return True if IP is rate-limited (too many failures)."""
    from app.models import AuthTempToken
    cutoff = _now_utc() - LOGIN_WINDOW
    count = db.query(AuthTempToken).filter(
        AuthTempToken.token_type == f"LOGIN_FAIL_IP_{ip}",
        AuthTempToken.created_at >= cutoff,
    ).count()
    return count >= LOGIN_LIMIT


def _record_ip_failure(db, ip: str) -> None:
    """Record a login failure for IP rate limiting."""
    from app.models import AuthTempToken
    now = _now_utc()
    token = AuthTempToken(
        temp_token_id=str(uuid4()),
        user_id="system",
        token_type=f"LOGIN_FAIL_IP_{ip}",
        token_hash=str(uuid4()),
        sent_at=now,
        expires_at=now + LOGIN_WINDOW,
        created_at=now,
    )
    db.add(token)
    db.commit()


def _oauth_next_cookie_key(state: str | None) -> str | None:
    """Return scoped cookie key using hash of state."""
    if not state:
        return None
    return f"oauth_next_{hashlib.sha256(state.encode()).hexdigest()[:12]}"


def _parse_account(account: str) -> tuple[str | None, str | None]:
    """解析 account 为 (email, phone)。返回 (email, None) 或 (None, phone)。"""
    s = (account or "").strip()
    if not s:
        return None, None
    if "@" in s:
        return s.lower(), None
    if PHONE_RE.match(s):
        return None, s
    return None, None


class LoginRequest(BaseModel):
    account: str | None = None  # 邮箱或手机号
    email: str | None = None  # 兼容旧版
    password: str


class RegisterRequest(BaseModel):
    account: str | None = None  # 邮箱或手机号
    email: str | None = None  # 兼容旧版
    password: str
    nickname: str | None = None


def _set_auth_cookie(response: Response, token: str, *, request: Request | None = None) -> None:
    max_age = settings.jwt_access_token_expire_hours * 3600
    secure = bool(request and request.url.scheme == "https")
    response.set_cookie(
        key=AUTH_COOKIE,
        value=token,
        max_age=max_age,
        path="/",
        httponly=True,
        samesite="lax",
        secure=secure,
    )


def _clear_auth_cookie(response: Response) -> None:
    response.delete_cookie(key=AUTH_COOKIE, path="/")


@router.post("/auth/login")
async def auth_login(req: LoginRequest, request: Request, response: Response):
    """登录 - 邮箱或手机号+密码"""
    acc = req.account or req.email or ""
    email, phone = _parse_account(acc)
    if not email and not phone:
        raise HTTPException(status_code=400, detail="请输入有效邮箱或11位手机号")
    client_ip = request.client.host if request.client else "unknown"
    db = SessionLocal()
    try:
        # Prune old login failure records
        try:
            _prune_login_failures(db)
        except OperationalError:
            db.rollback()
            return JSONResponse(status_code=503, content={
                "success": False, "error_code": "UPSTREAM_TIMEOUT",
                "error_message": "UPSTREAM_TIMEOUT", "data": None,
            })
        # IP rate limit check
        if _check_ip_rate_limit(db, client_ip):
            return JSONResponse(status_code=429, content={
                "success": False, "error_code": "RATE_LIMITED",
                "error_message": "登录尝试过于频繁，请稍后再试", "data": None,
            })
        if email:
            user = db.query(User).filter(User.email == email).first()
        else:
            user = db.query(User).filter(User.phone == phone).first()
        if not user or not verify_password(req.password, user.password_hash):
            _record_ip_failure(db, client_ip)
            return JSONResponse(status_code=401, content={
                "success": False, "error_code": "UNAUTHORIZED",
                "error_message": "账号或密码错误", "data": None,
            })
        # Email verification gate
        _email_required = getattr(settings, "user_email_enabled", False) and getattr(settings, "user_email_smtp_host", "")
        if _email_required and not getattr(user, "email_verified", False):
            return JSONResponse(status_code=401, content={
                "success": False,
                "error_code": "EMAIL_NOT_VERIFIED",
                "error_message": "请先完成邮箱激活后再登录。",
                "data": None,
            })
        _revoked_refresh_subjects.discard(str(user.id))
        _revoked_access_subjects.discard(str(user.id))
        if getattr(user, "user_id", None):
            _revoked_refresh_subjects.discard(str(user.user_id))
            _revoked_access_subjects.discard(str(user.user_id))
        pair = issue_token_pair(user, db)
        token = pair["access_token"]
        refresh = pair["refresh_token"]
        _set_auth_cookie(response, token)
        return envelope(data={
            "access_token": token,
            "refresh_token": refresh,
            "expires_in": settings.jwt_access_token_expire_hours * 3600,
            "user_id": user.id,
            "id": user.id,
            "email": user.email,
            "phone": getattr(user, "phone", None),
            "nickname": user.nickname,
            "role": user.role,
            "tier": getattr(user, "tier", None) or "Free",
            "membership_level": user.membership_level or "free",
            "tier_expires_at": (getattr(user, "tier_expires_at", None) or user.membership_expires_at or "").isoformat() if (getattr(user, "tier_expires_at", None) or user.membership_expires_at) else None,
            "membership_expires_at": user.membership_expires_at.isoformat() if user.membership_expires_at else None,
            "email_verified": getattr(user, "email_verified", False) or False,
            "permissions": get_permissions(user.membership_level),
        })
    finally:
        db.close()


@router.post("/auth/register")
async def auth_register(req: RegisterRequest, response: Response = None):
    """注册 - 邮箱或手机号+密码"""
    acc = req.account or req.email or ""
    email, phone = _parse_account(acc)
    if not email and not phone:
        raise HTTPException(status_code=400, detail="请输入有效邮箱或11位手机号")
    if not PASSWORD_RE.match(req.password):
        raise HTTPException(status_code=400, detail="密码需≥8位且含字母和数字")
    db = SessionLocal()
    try:
        if email:
            if db.query(User).filter(User.email == email).first():
                raise HTTPException(status_code=409, detail="该邮箱已被注册")
            store_email, store_phone = email, None
        else:
            if db.query(User).filter(User.phone == phone).first():
                raise HTTPException(status_code=400, detail="该手机号已被注册")
            store_email = f"{phone}@phone.local"
            store_phone = phone
        now = datetime.now(timezone.utc)
        expires_at = None
        level = "free"
        if settings.membership_free_trial_days > 0:
            expires_at = now + timedelta(days=settings.membership_free_trial_days)
            level = "monthly"
        user = User(
            email=store_email,
            phone=store_phone,
            password_hash=hash_password(req.password),
            nickname=req.nickname or None,
            role="user",
            membership_level=level,
            membership_expires_at=expires_at,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        token = create_access_token(user)
        refresh = create_refresh_token(user)
        email_activation_required = bool(
            getattr(settings, "user_email_enabled", False)
            and getattr(settings, "user_email_smtp_host", "")
        )
        success_message = "注册成功，请完成邮箱激活后再登录。" if email_activation_required else "注册成功"
        json_response = JSONResponse(
            status_code=201,
            content=envelope(data={
                "access_token": token,
                "refresh_token": refresh,
                "expires_in": settings.jwt_access_token_expire_hours * 3600,
                "user_id": user.id,
                "id": user.id,
                "email": user.email,
                "phone": getattr(user, "phone", None),
                "nickname": user.nickname,
                "role": user.role,
                "tier": getattr(user, "tier", None) or "Free",
                "membership_level": user.membership_level or "free",
                "tier_expires_at": (getattr(user, "tier_expires_at", None) or user.membership_expires_at or "").isoformat() if (getattr(user, "tier_expires_at", None) or user.membership_expires_at) else None,
                "membership_expires_at": user.membership_expires_at.isoformat() if user.membership_expires_at else None,
                "email_verified": getattr(user, "email_verified", False) or False,
                "permissions": get_permissions(user.membership_level),
                "message": success_message,
            }),
        )
        _set_auth_cookie(json_response, token)
        return json_response
    finally:
        db.close()


@router.post("/auth/logout")
async def auth_logout(request: Request, response: Response):
    """登出 - 清除 Cookie，撤销 access token 和 refresh token"""
    from app.core.security import get_current_user_optional
    user = await get_current_user_optional(request)
    if user:
        _revoked_access_subjects.add(str(user.id))
        _revoked_refresh_subjects.add(str(user.id))
        if getattr(user, "user_id", None):
            _revoked_access_subjects.add(str(user.user_id))
            _revoked_refresh_subjects.add(str(user.user_id))
    _clear_auth_cookie(response)
    return envelope(data={"ok": True})


async def _require_user(request: Request) -> User:
    from app.core.security import get_current_user_optional

    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="未登录")
    uid = str(getattr(user, "user_id", None) or user.id)
    if uid in _revoked_access_subjects:
        raise HTTPException(status_code=401, detail="会话已失效")
    return user


@router.get("/auth/me", tags=["frozen-v1"])
async def auth_me(user: User = Depends(_require_user)):
    """当前用户信息（需登录）"""
    tier_val = getattr(user, "tier", None) or "Free"
    membership_level = getattr(user, "membership_level", None) or "free"
    return envelope(data={
        "id": user.id,
        "user_id": user.id,
        "email": user.email,
        "nickname": user.nickname,
        "role": user.role,
        "tier": tier_val,
        "membership_level": membership_level,
        "tier_expires_at": (getattr(user, "tier_expires_at", None) or user.membership_expires_at or "").isoformat() if (getattr(user, "tier_expires_at", None) or user.membership_expires_at) else None,
        "membership_expires_at": user.membership_expires_at.isoformat() if user.membership_expires_at else None,
        "email_verified": getattr(user, "email_verified", False) or False,
        "permissions": get_permissions(membership_level),
    })


class RefreshRequest(BaseModel):
    refresh_token: str


@router.post("/auth/refresh")
async def auth_refresh(req: RefreshRequest, response: Response):
    """刷新 access_token（17 §3）- 含 refresh token rotation 与 replay 检测"""
    from app.core.security import _now_utc, REFRESH_GRACE_SECONDS
    from app.models import RefreshToken as RefreshTokenModel

    payload = decode_token(req.refresh_token)
    if not payload or payload.get("type") != "refresh" or not payload.get("sub"):
        return JSONResponse(status_code=401, content={
            "success": False, "error_code": "UNAUTHORIZED", "error_message": "无效的 refresh_token", "data": None,
        })
    user_id_claim = str(payload.get("sub"))
    token_hash_val = hash_token(req.refresh_token)

    db = SessionLocal()
    try:
        # Lookup user
        user = db.query(User).filter(
            (User.user_id == user_id_claim) | (User.id == user_id_claim)
        ).first()
        if not user:
            return JSONResponse(status_code=401, content={
                "success": False, "error_code": "UNAUTHORIZED", "error_message": "用户不存在", "data": None,
            })

        # DB-based replay detection: check if this token was already used
        existing_row = db.query(RefreshTokenModel).filter(
            RefreshTokenModel.token_hash == token_hash_val,
        ).first()

        now = _now_utc()

        if existing_row and existing_row.used_at is not None:
            # Token was already used — check grace period
            grace_end = existing_row.used_at
            if hasattr(grace_end, "tzinfo") and grace_end.tzinfo is None:
                grace_end = grace_end.replace(tzinfo=timezone.utc)
            grace_end = grace_end + timedelta(seconds=REFRESH_GRACE_SECONDS)

            if now > grace_end:
                # Past grace: revoke ALL user tokens (全设备登出)
                db.query(RefreshTokenModel).filter(
                    RefreshTokenModel.user_id == str(user.user_id),
                    RefreshTokenModel.revoked_at.is_(None),
                ).update({"revoked_at": now, "revoke_reason": "replay_past_grace"})
                db.commit()
                _revoked_refresh_subjects.add(user_id_claim)

            return JSONResponse(status_code=401, content={
                "success": False, "error_code": "UNAUTHORIZED", "error_message": "refresh_token 已使用", "data": None,
            })

        # Also check in-memory set for tokens not in DB
        if token_hash_val in _used_refresh_tokens:
            return JSONResponse(status_code=401, content={
                "success": False, "error_code": "UNAUTHORIZED", "error_message": "refresh_token 已使用", "data": None,
            })
        if user_id_claim in _revoked_refresh_subjects:
            return JSONResponse(status_code=401, content={
                "success": False, "error_code": "UNAUTHORIZED", "error_message": "refresh_token 已失效", "data": None,
            })

        # Mark old token as used
        if existing_row:
            existing_row.used_at = now
            db.flush()

        _used_refresh_tokens.add(token_hash_val)

        # Issue new pair
        token = create_access_token(user)
        refresh = create_refresh_token(user)

        # Store new refresh token in DB
        new_jti = decode_token(refresh).get("jti", str(uuid4()))
        new_exp = decode_token(refresh).get("exp")
        new_row = RefreshTokenModel(
            refresh_token_id=new_jti,
            user_id=str(user.user_id),
            session_id=new_jti,
            token_hash=hash_token(refresh),
            rotated_from_token_id=existing_row.refresh_token_id if existing_row else None,
            issued_at=now,
            expires_at=datetime.fromtimestamp(new_exp, tz=timezone.utc) if new_exp else now + timedelta(days=7),
            grace_expires_at=now + timedelta(seconds=REFRESH_GRACE_SECONDS),
        )
        db.add(new_row)
        db.commit()

        _set_auth_cookie(response, token)
        return envelope(data={
            "access_token": token,
            "refresh_token": refresh,
            "expires_in": settings.jwt_access_token_expire_hours * 3600,
        })
    finally:
        db.close()


@router.get("/auth/activate")
async def auth_activate(token: str, request: Request):
    from fastapi.responses import HTMLResponse

    def _html_error():
        return HTMLResponse(
            content="<html><body><p>激活链接已失效或无效</p></body></html>",
            status_code=400,
        )

    wants_html = "text/html" in request.headers.get("Accept", "")

    if not token:
        if wants_html:
            return _html_error()
        raise HTTPException(status_code=400, detail="INVALID_PAYLOAD")
    db = SessionLocal()
    try:
        from app.models import AuthTempToken

        row = (
            db.query(AuthTempToken)
            .filter(
                AuthTempToken.token_hash == hash_token(token),
                AuthTempToken.token_type == "EMAIL_ACTIVATION",
            )
            .first()
        )
        now = datetime.now(timezone.utc)
        expires_at = getattr(row, "expires_at", None)
        if expires_at is not None and getattr(expires_at, "tzinfo", None) is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if not row or row.used_at is not None or (expires_at is not None and expires_at < now):
            if wants_html:
                return _html_error()
            raise HTTPException(status_code=400, detail="INVALID_PAYLOAD")
        user = db.query(User).filter(User.user_id == str(row.user_id)).first()
        if not user:
            if wants_html:
                return _html_error()
            raise HTTPException(status_code=400, detail="INVALID_PAYLOAD")
        user.email_verified = True
        row.used_at = now
        db.commit()
    finally:
        db.close()
    if "application/json" in request.headers.get("Accept", ""):
        return envelope(data={"message": "email_activated"})
    return RedirectResponse(url="/login?activated=1", status_code=302)


@router.get("/auth/oauth/exchange")
async def auth_oauth_exchange_legacy():
    """Legacy temp_code exchange endpoint — retired."""
    return JSONResponse(status_code=410, content={
        "success": False, "error_code": "ROUTE_RETIRED",
        "error_message": "OAuth exchange 已废弃，请使用 callback 直接完成授权",
        "data": None,
    })


@router.get("/auth/oauth/providers")
async def auth_oauth_providers():
    """返回已配置的 OAuth 提供方列表。前端用于动态渲染登录按钮。"""
    from app.services.oauth_service import get_oauth_authorize_url

    providers = []
    for pid, name in [("qq", "QQ 登录"), ("wechat", "微信登录")]:
        url = get_oauth_authorize_url(pid)
        if url:
            providers.append({
                "id": pid,
                "name": name,
                "start_path": f"/auth/oauth/{pid}/start",
                "method": "POST",
            })
    has_providers = len(providers) > 0
    result: dict = {
        "contract_status": "contract-ready",
        "provider_status": "configured" if has_providers else "provider-not-configured",
        "live_verified": False,
        "providers": providers,
    }
    if not has_providers:
        result["message"] = "当前暂未提供可用的第三方登录方式，请使用邮箱登录。"
    return envelope(data=result)


@router.get("/auth/oauth/authorize")
async def auth_oauth_authorize(provider: str = "qq"):
    """跳转到 QQ 或 WeChat 授权页"""
    from fastapi.responses import RedirectResponse

    from app.services.oauth_service import get_oauth_authorize_url

    if provider not in ("qq", "wechat"):
        raise HTTPException(status_code=400, detail="INVALID_PROVIDER")
    url = get_oauth_authorize_url(provider)
    if not url:
        raise HTTPException(status_code=503, detail="OAUTH_PROVIDER_UNAVAILABLE")
    return RedirectResponse(url=url, status_code=302)


@router.post("/auth/oauth/{provider}/start")
async def auth_oauth_start(provider: str, request: Request, response: Response, next: str | None = None):
    """Initiate OAuth flow — store state and redirect to provider."""
    from app.services.oauth_service import build_oauth_authorize_url, store_oauth_state

    if provider not in ("qq", "wechat"):
        raise HTTPException(status_code=400, detail="INVALID_PROVIDER")
    if not _is_provider_ready(provider):
        return JSONResponse(status_code=503, content={
            "success": False, "error_code": "OAUTH_PROVIDER_UNAVAILABLE",
            "error_message": f"{provider} OAuth 未完整配置", "data": None,
        })
    db = SessionLocal()
    try:
        state = store_oauth_state(db, provider)
        db.commit()
        url = build_oauth_authorize_url(provider, state)
        if not url:
            return JSONResponse(status_code=503, content={
                "success": False, "error_code": "OAUTH_PROVIDER_UNAVAILABLE",
                "error_message": f"{provider} OAuth 未配置", "data": None,
            })
    finally:
        db.close()
    redir = RedirectResponse(url=url, status_code=302)
    if next:
        cookie_key = _oauth_next_cookie_key(state) or f"oauth_next_{hashlib.sha256(state.encode()).hexdigest()[:12]}"
        redir.set_cookie(key=cookie_key, value=next, max_age=600, path="/", httponly=True, samesite="lax")
    # Clear generic oauth_next cookie to prevent leaking across states
    redir.set_cookie(key="oauth_next", value="", max_age=600, path="/", httponly=True, samesite="lax")
    return redir


@router.get("/auth/oauth/{provider}/callback")
async def auth_oauth_provider_callback(
    provider: str,
    request: Request,
    response: Response,
    code: str | None = None,
    state: str | None = None,
):
    """OAuth callback per provider — handles both JSON and HTML Accept types."""
    from app.core.error_codes import ERROR_CODE_WHITELIST
    from app.services.oauth_service import exchange_qq_code, exchange_wechat_code, get_or_create_oauth_user, verify_oauth_state

    accept = request.headers.get("accept", "")
    is_json = "application/json" in accept
    base = (getattr(settings, "oauth_callback_base", None) or "").rstrip("/")

    def _fail_json(status: int, error_code: str, msg: str):
        # Sanitize error_code: strip suffixes with colons and only allow whitelisted codes
        clean_code = error_code.split(":")[0].strip()
        if hasattr(ERROR_CODE_WHITELIST, '__contains__') and clean_code not in ERROR_CODE_WHITELIST:
            clean_code = "UNAUTHORIZED"
        return JSONResponse(status_code=status, content={
            "success": False, "error_code": clean_code, "error_message": msg, "data": None,
        })

    def _fail_html(error_code: str):
        raw = error_code.strip()
        # If the raw error has a suffix (colon-separated detail), reject and normalize to UNAUTHORIZED
        if ":" in raw:
            clean_code = "UNAUTHORIZED"
        elif raw in ERROR_CODE_WHITELIST:
            clean_code = raw
        else:
            clean_code = "UNAUTHORIZED"
        return RedirectResponse(url=f"{base}/login?error={clean_code}", status_code=302)

    if provider not in ("qq", "wechat"):
        if is_json:
            return _fail_json(400, "INVALID_PROVIDER", "无效的 OAuth 提供方")
        return _fail_html("INVALID_PROVIDER")

    if not _is_provider_ready(provider):
        if is_json:
            return _fail_json(503, "OAUTH_PROVIDER_UNAVAILABLE", f"{provider} OAuth 未完整配置")
        return _fail_html("OAUTH_PROVIDER_UNAVAILABLE")

    if not code:
        if is_json:
            return _fail_json(401, "UNAUTHORIZED", "缺少授权码")
        return _fail_html("UNAUTHORIZED")

    # Verify state
    db = SessionLocal()
    try:
        if state:
            verify_oauth_state(db, provider, state)
            db.commit()
    except HTTPException as he:
        db.close()
        detail = str(getattr(he, "detail", ""))
        if is_json:
            return _fail_json(he.status_code, detail, detail)
        return _fail_html(detail)
    except Exception:
        pass
    finally:
        try:
            db.close()
        except Exception:
            pass

    try:
        oauth_email = None
        if provider == "qq":
            _, open_id, nickname = await exchange_qq_code(code)
            union_id = None
        else:
            wechat_result = await exchange_wechat_code(code)
            open_id = wechat_result[1] if len(wechat_result) > 1 else None
            union_id = wechat_result[2] if len(wechat_result) > 2 else None
            oauth_email = wechat_result[3] if len(wechat_result) > 3 else None
            # If 3-value return and 3rd looks like email, treat as email
            if len(wechat_result) == 3 and union_id and "@" in str(union_id):
                oauth_email = union_id
                union_id = None
            nickname = None
        if not open_id:
            if is_json:
                return _fail_json(401, "UNAUTHORIZED", "OAuth 授权失败")
            return _fail_html("UNAUTHORIZED")
        oauth_db = SessionLocal()
        try:
            oauth_db.expire_on_commit = False
            user, is_new = get_or_create_oauth_user(provider, open_id, union_id, nickname, return_is_new=True, db=oauth_db, email=oauth_email)
            if not user:
                oauth_db.rollback()
                if is_json:
                    return _fail_json(401, "UNAUTHORIZED", "OAuth 用户创建失败")
                return _fail_html("UNAUTHORIZED")
            tokens = issue_token_pair(user, db=oauth_db)
            token = tokens["access_token"]
            refresh = tokens["refresh_token"]
            oauth_db.commit()
        except Exception:
            oauth_db.rollback()
            raise
        finally:
            oauth_db.close()
    except HTTPException as he:
        detail = str(getattr(he, "detail", ""))
        if is_json:
            return _fail_json(he.status_code, detail, detail)
        return _fail_html(detail)
    except Exception as e:
        import logging
        logging.getLogger(__name__).exception("oauth_callback_failed err=%s", e)
        if is_json:
            return _fail_json(401, "UNAUTHORIZED", "OAuth 授权异常")
        return _fail_html("UNAUTHORIZED")

    if is_json:
        # Clear state-scoped next cookie in JSON response
        resp_data = envelope(data={
            "is_new_user": is_new,
            "profile": {
                "user_id": user.id,
                "email": user.email if user.email and ".oauth.local" not in (user.email or "") and "@phone.local" not in (user.email or "") else None,
                "email_verified": getattr(user, "email_verified", False) or False,
                "tier": getattr(user, "tier", None) or "Free",
            },
            "tokens": {
                "access_token": token,
                "refresh_token": refresh,
            },
        })
        json_resp = JSONResponse(content=resp_data)
        if state:
            cookie_key = _oauth_next_cookie_key(state)
            if cookie_key:
                json_resp.delete_cookie(cookie_key)
        return json_resp

    # HTML flow — redirect
    next_page = "/"
    if state:
        cookie_key = _oauth_next_cookie_key(state)
        if cookie_key:
            next_page = request.cookies.get(cookie_key, "/")
    redir = RedirectResponse(url=next_page, status_code=302)
    _set_auth_cookie(redir, token, request=request)
    if state:
        cookie_key = _oauth_next_cookie_key(state)
        if cookie_key:
            redir.delete_cookie(cookie_key)
    return redir


@router.get("/auth/oauth/callback")
async def auth_oauth_callback(
    code: str | None = None,
    state: str | None = None,
):
    """OAuth 回调（QQ/微信）- 17 §2.1b"""
    if not code or not state or ":" not in state:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    platform, _csrf = state.split(":", 1)
    if platform not in ("qq", "wechat"):
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    from fastapi.responses import RedirectResponse

    from app.core.security import create_access_token
    from app.services.oauth_service import exchange_qq_code, exchange_wechat_code, get_or_create_oauth_user

    try:
        if platform == "qq":
            _, open_id, nickname = await exchange_qq_code(code)
            union_id = None
        else:
            _, open_id, union_id = await exchange_wechat_code(code)
            nickname = None
        if not open_id:
            raise HTTPException(status_code=401, detail="UNAUTHORIZED")
        user = get_or_create_oauth_user(platform, open_id, union_id, nickname)
        if not user:
            raise HTTPException(status_code=401, detail="UNAUTHORIZED")
        token = create_access_token(user)
        base = (getattr(settings, "oauth_callback_base", None) or "").rstrip("/")
        redir = RedirectResponse(url=f"{base}/" if base else "/", status_code=302)
        _set_auth_cookie(redir, token)
        return redir
    except HTTPException:
        raise
    except Exception as e:
        import logging

        logging.getLogger(__name__).exception("oauth_callback_failed err=%s", e)
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")


class ForgotPasswordRequest(BaseModel):
    account: str | None = None  # 邮箱或手机号
    email: str | None = None  # 兼容新版


class ResetPasswordRequest(BaseModel):
    token: str
    password: str | None = None
    new_password: str | None = None


def _store_temp_token(db, user_id, token_type: str, expires_at) -> str:
    """Create a temp token and return the raw (unhashed) value."""
    raw = secrets.token_urlsafe(32)
    h = hashlib.sha256(raw.encode()).hexdigest()
    from app.models import AuthTempToken
    t = AuthTempToken(
        temp_token_id=str(uuid4()),
        user_id=user_id,
        token_type=token_type,
        token_hash=h,
        sent_at=datetime.now(timezone.utc),
        expires_at=expires_at,
    )
    db.add(t)
    db.flush()
    return raw


@router.post("/auth/forgot-password")
async def auth_forgot_password(req: ForgotPasswordRequest):
    """忘记密码：创建重置 Token，有效期 1 小时。"""
    acc = getattr(req, "account", None) or getattr(req, "email", None) or ""
    email, phone = _parse_account(acc)
    _FORGOT_MSG = "若该邮箱已注册，重置请求已提交，请按后续指引完成密码重置。"
    if not email and not phone:
        # Still return 200 to not leak user existence
        return envelope(data={"ok": True, "message": _FORGOT_MSG, "delivery_status": "manual_reset_required"})
    db = SessionLocal()
    try:
        if email:
            user = db.query(User).filter(User.email == email).first()
        else:
            user = db.query(User).filter(User.phone == phone).first()
        if not user:
            return envelope(data={"ok": True, "message": _FORGOT_MSG, "delivery_status": "manual_reset_required"})
        expires = datetime.now(timezone.utc) + timedelta(hours=1)
        logical_user_id = getattr(user, "user_id", None) or str(user.id)
        raw = _store_temp_token(db, logical_user_id, "PASSWORD_RESET", expires)
        db.commit()
        # Note: In production, send the reset_url via email rather than in the response.
        # Return same payload as missing user to prevent user enumeration.
        return envelope(data={"ok": True, "message": _FORGOT_MSG, "delivery_status": "manual_reset_required"})
    finally:
        db.close()


@router.post("/auth/reset-password")
async def auth_reset_password(req: ResetPasswordRequest):
    """重置密码：使用 forgot-password 返回的 token 设置新密码"""
    new_password = (req.new_password or req.password or "").strip()
    if not PASSWORD_RE.match(new_password):
        raise HTTPException(status_code=422, detail="密码需≥8位且含字母和数字")
    h = hashlib.sha256(req.token.encode()).hexdigest()
    db = SessionLocal()
    try:
        # Try AuthTempToken first, then PasswordResetToken
        from app.models import AuthTempToken
        t = db.query(AuthTempToken).filter(
            AuthTempToken.token_hash == h,
            AuthTempToken.token_type == "PASSWORD_RESET",
        ).first()
        if t:
            expires_at = t.expires_at
            if expires_at and getattr(expires_at, "tzinfo", None) is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if expires_at and expires_at < datetime.now(timezone.utc):
                raise HTTPException(status_code=400, detail="重置链接已过期")
            user = db.query(User).filter(User.user_id == str(t.user_id)).first()
            if not user:
                raise HTTPException(status_code=400, detail="用户不存在")
            user.password_hash = hash_password(new_password)
            _revoked_refresh_subjects.add(str(user.id))
            if getattr(user, "user_id", None):
                _revoked_refresh_subjects.add(str(user.user_id))
            db.delete(t)
            db.commit()
            return envelope(data={"ok": True})
        # Fallback: legacy PasswordResetToken
        t2 = db.query(PasswordResetToken).filter(PasswordResetToken.token_hash == h).first()
        if not t2 or t2.used_at:
            raise HTTPException(status_code=400, detail="重置链接无效或已使用")
        if t2.expires_at < datetime.now(timezone.utc):
            raise HTTPException(status_code=400, detail="重置链接已过期")
        user = db.query(User).filter(User.user_id == t2.user_id).first()
        if not user:
            raise HTTPException(status_code=400, detail="用户不存在")
        user.password_hash = hash_password(new_password)
        _revoked_refresh_subjects.add(str(user.id))
        if getattr(user, "user_id", None):
            _revoked_refresh_subjects.add(str(user.user_id))
        t2.used_at = datetime.now(timezone.utc)
        db.commit()
        return envelope(data={"ok": True})
    finally:
        db.close()
