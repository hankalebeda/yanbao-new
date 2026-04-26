from __future__ import annotations

import uuid
from contextvars import ContextVar, Token

_request_id_ctx: ContextVar[str | None] = ContextVar("request_id", default=None)


def set_request_id(value: str | None):
    _request_id_ctx.set(_normalize_request_id(value))


def get_request_id() -> str | None:
    return _request_id_ctx.get()


def reset_request_id(token: Token | None = None) -> None:
    """Clear the current request-id context (used in teardown / tests)."""
    if token is not None:
        _request_id_ctx.reset(token)
    else:
        _request_id_ctx.set(None)


def _normalize_request_id(value: str | None) -> str | None:
    if value is None:
        return None
    v = str(value).strip()
    return v if v else None


def ensure_request_id(value: str | None = None) -> str:
    """Set or generate a request-id; always returns a non-None string."""
    normalized = _normalize_request_id(value)
    if normalized is not None:
        _request_id_ctx.set(normalized)
        return normalized
    existing = _normalize_request_id(get_request_id())
    if existing is not None:
        _request_id_ctx.set(existing)
        return existing
    generated = str(uuid.uuid4())
    _request_id_ctx.set(generated)
    return generated


def bind_request_id(value: str | None = None) -> tuple[str, Token]:
    """Set or generate a request-id; returns (request_id, reset_token)."""
    normalized = _normalize_request_id(value)
    rid = normalized if normalized is not None else (get_request_id() or str(uuid.uuid4()))
    token = _request_id_ctx.set(rid)
    return rid, token


def resolve_record_request_id(value: str | None = None, *, allow_none: bool = False) -> str | None:
    """Resolve a request-id for DB record writes."""
    normalized = _normalize_request_id(value)
    if normalized is not None:
        return normalized
    existing = _normalize_request_id(get_request_id())
    if existing is not None:
        _request_id_ctx.set(existing)
        return existing
    if allow_none:
        return None
    raise RuntimeError("request_id_context_missing")


def require_request_id() -> str:
    """Get the current request-id or raise."""
    request_id = resolve_record_request_id()
    if request_id is None:
        raise RuntimeError("request_id_context_missing")
    return request_id
