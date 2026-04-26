import os
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import bcrypt
import jwt
from fastapi import Cookie, Header, HTTPException, Request

from app.core.config import settings
from app.models import User

# 使用 bcrypt 直接替代 passlib，避免 passlib 与 bcrypt 5.x 的 __about__ 兼容性问题
AUTH_COOKIE = "access_token"


def _now_utc() -> datetime:
    """Return current UTC datetime (timezone-aware)."""
    return datetime.now(timezone.utc)


def _jwt_secret() -> str:
    s = settings.jwt_secret
    if not s:
        s = "dev-secret-change-in-production"  # 开发态，生产必须配置 JWT_SECRET
    return s


def hash_password(password: str) -> str:
    """使用 bcrypt 加密密码。bcrypt 限制 72 字节，超长密码截断。"""
    pwd_bytes = password.encode("utf-8")[:72]
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(pwd_bytes, salt)
    return hashed.decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    """验证明文密码与哈希是否匹配。"""
    try:
        return bcrypt.checkpw(
            plain.encode("utf-8"),
            hashed.encode("utf-8") if isinstance(hashed, str) else hashed,
        )
    except (ValueError, TypeError):
        return False


def hash_token(token: str) -> str:
    """Hash a JWT/token string using SHA-256 for revocation storage."""
    import hashlib
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def create_access_token(user: User) -> str:
    import uuid
    expire = datetime.now(timezone.utc) + timedelta(hours=settings.jwt_access_token_expire_hours)
    sid = uuid.uuid4().hex
    payload = {
        "sub": str(user.id),
        "email": user.email,
        "role": user.role,
        "tier": getattr(user, "tier", None) or "Free",
        "membership_level": user.membership_level,
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "type": "access",
        "jti": uuid.uuid4().hex,
        "sid": sid,
    }
    return jwt.encode(payload, _jwt_secret(), algorithm=settings.jwt_algorithm)


def create_refresh_token(user: User) -> str:
    days = getattr(settings, "jwt_refresh_token_expire_days", 7)
    expire = datetime.now(timezone.utc) + timedelta(days=days)
    payload = {
        "sub": str(user.id),
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "jti": str(uuid4()),
        "type": "refresh",
    }
    return jwt.encode(payload, _jwt_secret(), algorithm=settings.jwt_algorithm)


REFRESH_GRACE_SECONDS: int = 60


def rotate_refresh_token(db, old_refresh_token: str):
    """Rotate a refresh token: verify old, issue new pair, return (status, tokens)."""
    claims = decode_token(old_refresh_token)
    if not claims or claims.get("type") != "refresh":
        return "invalid", None
    user_id = claims.get("sub")
    from app.models import User as UserModel
    user = db.query(UserModel).filter(
        (UserModel.user_id == user_id) | (UserModel.id == user_id)
    ).first()
    if not user:
        return "invalid", None
    pair = issue_token_pair(user, db)
    return "ok", pair


def issue_token_pair(user, db=None):
    """Create access + refresh token pair for user."""
    access = create_access_token(user)
    refresh = create_refresh_token(user)
    return {
        "access_token": access,
        "refresh_token": refresh,
        "expires_in": settings.jwt_access_token_expire_hours * 3600,
    }


def decode_token(token: str) -> dict[str, Any] | None:
    try:
        return jwt.decode(token, _jwt_secret(), algorithms=[settings.jwt_algorithm])
    except Exception:
        return None


def _token_from_request(request: Request, cookie_token: str | None = None) -> str | None:
    auth = request.headers.get("Authorization")
    if auth and auth.startswith("Bearer "):
        return auth[7:].strip()
    return cookie_token


def _detach_loaded_user(db, user: User | None) -> User | None:
    if user is None:
        return None
    try:
        db.refresh(user)
    except Exception:
        pass
    # 模板页会在 DB session 关闭后继续读取这些标量字段；这里先显式加载后再脱离 session，避免 DetachedInstanceError。
    for attr in (
        "user_id",
        "email",
        "phone",
        "nickname",
        "role",
        "tier",
        "tier_expires_at",
        "membership_level",
        "membership_expires_at",
        "email_verified",
        "last_login_at",
        "created_at",
        "updated_at",
    ):
        try:
            getattr(user, attr)
        except Exception:
            pass
    try:
        db.expunge(user)
    except Exception:
        pass
    return user


async def get_current_user_optional(request: Request | None = None) -> User | None:
    """从 Cookie 或 Authorization 解析 JWT，返回当前用户（未登录返回 None）。可传入 Request 直接调用。"""
    if request is None:
        return None
    cookie_token = request.cookies.get(AUTH_COOKIE) if hasattr(request, "cookies") else None
    token = _token_from_request(request, cookie_token=cookie_token)
    if not token:
        return None
    payload = decode_token(token)
    if not payload or not payload.get("sub"):
        return None
    from app.core.db import SessionLocal

    db = SessionLocal()
    try:
        # NFR-17: check AccessTokenLease expiry (fail-close)
        jti = payload.get("jti")
        if jti:
            from app.models import AccessTokenLease
            lease = db.get(AccessTokenLease, jti)
            if lease is not None:
                lease_exp = lease.expires_at
                if lease_exp is not None:
                    if hasattr(lease_exp, "tzinfo") and lease_exp.tzinfo is None:
                        lease_exp = lease_exp.replace(tzinfo=timezone.utc)
                    if lease_exp < datetime.now(timezone.utc):
                        return None
                if lease.revoked_at is not None:
                    return None
        user = db.query(User).filter(User.user_id == payload["sub"]).first()
        if user:
            # 刷新 membership_level（可能过期）
            _refresh_membership_if_expired(db, user)
        return _detach_loaded_user(db, user)
    finally:
        db.close()


def _refresh_membership_if_expired(db, user: User) -> None:
    if user.membership_level not in ("monthly", "annual") or not user.membership_expires_at:
        return
    try:
        exp = user.membership_expires_at
        if isinstance(exp, str):
            exp = datetime.fromisoformat(exp.replace("Z", "+00:00"))
        if hasattr(exp, "replace") and (exp.tzinfo is None):
            exp = exp.replace(tzinfo=timezone.utc)
        if exp < datetime.now(timezone.utc):
            user.membership_level = "free"
            user.membership_expires_at = None
            db.commit()
    except (TypeError, ValueError, AttributeError):
        pass


async def internal_auth(x_internal_token: str | None = Header(default=None, alias="X-Internal-Token")):
    accepted_tokens: list[str] = []
    if settings.internal_cron_token:
        accepted_tokens.append(settings.internal_cron_token)

    env_internal_token = (os.getenv("INTERNAL_TOKEN") or "").strip()
    if env_internal_token:
        accepted_tokens.append(env_internal_token)

    env_aliases = (os.getenv("INTERNAL_TOKEN_ALIASES") or "").strip()
    if env_aliases:
        accepted_tokens.extend(token.strip() for token in env_aliases.split(",") if token.strip())

    accepted_tokens = [token for token in dict.fromkeys(accepted_tokens) if token]
    if not accepted_tokens:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    if x_internal_token not in accepted_tokens:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    return True
