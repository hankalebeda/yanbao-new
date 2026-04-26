from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models import Base

logger = logging.getLogger(__name__)

COOKIE_ACCOUNT_KEY = "default"
_DEFAULT_COOKIE_TTL_HOURS = {
    "weibo": 24,
    "xueqiu": 48,
    "douyin": 24,
    "kuaishou": 24,
}
_PROBE_OUTCOME_TO_API_STATUS = {
    "success": "ok",
    "failed": "fail",
    "skipped": "skipped",
}
_SESSION_STATUS_TO_API_STATUS = {
    "ACTIVE": "ok",
    "EXPIRING": "ok",
    "EXPIRED": "fail",
    "REFRESH_FAILED": "fail",
    "SKIPPED": "skipped",
}

# FR03-COOKIE-02: 探活互斥锁 (单进程)
_probe_lock = threading.Lock()
# FR03-COOKIE-02: 分布式探活互斥 TTL（秒）
_PROBE_DISTRIBUTED_TTL_SEC = 30

# FR03-COOKIE-02: 各平台探活URL
_PROBE_URLS: dict[str, str] = {
    "weibo": "https://m.weibo.cn/api/config",
    "xueqiu": "https://stock.xueqiu.com/v5/stock/batch/quote.json?symbol=SH000001",
    "douyin": "https://www.douyin.com/aweme/v1/web/general/search/single/",
    "kuaishou": "https://www.kuaishou.com/graphql",
}
PROBE_TIMEOUT_SEC = 10
CONSECUTIVE_FAILURE_ALERT_THRESHOLD = 2


class CookieSessionNotFoundError(Exception):
    pass


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _ensure_utc(value).isoformat()


def _cookie_ttl_hours() -> dict[str, int]:
    raw = (getattr(settings, "cookie_ttl_hours", "") or "").strip()
    if not raw:
        return dict(_DEFAULT_COOKIE_TTL_HOURS)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return dict(_DEFAULT_COOKIE_TTL_HOURS)

    ttl_map = dict(_DEFAULT_COOKIE_TTL_HOURS)
    for key, value in parsed.items():
        try:
            ttl_map[str(key)] = int(value)
        except (TypeError, ValueError):
            continue
    return ttl_map


def _expires_at_for_provider(provider: str, now: datetime) -> datetime:
    ttl_hours = _cookie_ttl_hours().get(provider, _DEFAULT_COOKIE_TTL_HOURS.get(provider, 24))
    return now + timedelta(hours=ttl_hours)


def _cookie_snapshot(row) -> dict[str, object] | None:
    if row is None:
        return None
    return {
        "provider": row.provider,
        "account_key": row.account_key,
        "status": row.status,
        "expires_at": _iso_datetime(row.expires_at),
        "last_probe_at": _iso_datetime(row.last_probe_at),
        "last_refresh_at": _iso_datetime(row.last_refresh_at),
        "status_reason": row.status_reason,
        "cookie_present": bool(row.cookie_blob),
        "cookie_length": len(row.cookie_blob or ""),
    }


def _get_session_row(db: Session, *, login_source: str, session_id: str | None = None):
    table = Base.metadata.tables["cookie_session"]
    query = select(table).where(table.c.provider == login_source)
    if session_id:
        query = query.where(table.c.cookie_session_id == session_id)
    query = query.order_by(table.c.updated_at.desc(), table.c.cookie_session_id.asc())
    return db.execute(query).first()


def upsert_cookie_session(
    db: Session,
    *,
    login_source: str,
    cookie_string: str,
    now: datetime | None = None,
) -> dict[str, object]:
    now = _ensure_utc(now) or _now_utc()
    expires_at = _expires_at_for_provider(login_source, now)
    table = Base.metadata.tables["cookie_session"]
    existing = db.execute(
        select(table)
        .where(table.c.provider == login_source, table.c.account_key == COOKIE_ACCOUNT_KEY)
        .order_by(table.c.updated_at.desc(), table.c.cookie_session_id.asc())
    ).first()
    before_snapshot = _cookie_snapshot(existing)

    if existing:
        cookie_session_id = existing.cookie_session_id
        db.execute(
            table.update()
            .where(table.c.cookie_session_id == cookie_session_id)
            .values(
                status="ACTIVE",
                cookie_blob=cookie_string,
                last_refresh_at=now,
                expires_at=expires_at,
                status_reason=None,
                updated_at=now,
            )
        )
    else:
        cookie_session_id = str(uuid4())
        db.execute(
            table.insert().values(
                cookie_session_id=cookie_session_id,
                provider=login_source,
                account_key=COOKIE_ACCOUNT_KEY,
                status="ACTIVE",
                cookie_blob=cookie_string,
                last_probe_at=None,
                last_refresh_at=now,
                expires_at=expires_at,
                status_reason=None,
                created_at=now,
                updated_at=now,
            )
        )

    current = db.execute(select(table).where(table.c.cookie_session_id == cookie_session_id)).first()
    if current is None:
        return {
            "cookie_session_id": cookie_session_id,
            "result": {"status": "saved", "expires_at": None},
            "before_snapshot": before_snapshot,
            "after_snapshot": {},
        }
    return {
        "cookie_session_id": cookie_session_id,
        "result": {
            "status": "saved",
            "expires_at": _iso_datetime(current.expires_at),
        },
        "before_snapshot": before_snapshot,
        "after_snapshot": _cookie_snapshot(current),
    }


def refresh_cookie_session(
    db: Session,
    *,
    login_source: str,
    now: datetime | None = None,
) -> dict[str, object]:
    now = _ensure_utc(now) or _now_utc()
    table = Base.metadata.tables["cookie_session"]
    existing = db.execute(
        select(table)
        .where(table.c.provider == login_source, table.c.account_key == COOKIE_ACCOUNT_KEY)
        .order_by(table.c.updated_at.desc(), table.c.cookie_session_id.asc())
    ).first()
    if not existing:
        raise CookieSessionNotFoundError(login_source)

    expires_at = _expires_at_for_provider(login_source, now)
    db.execute(
        table.update()
        .where(table.c.cookie_session_id == existing.cookie_session_id)
        .values(
            status="ACTIVE",
            last_refresh_at=now,
            expires_at=expires_at,
            status_reason=None,
            updated_at=now,
        )
    )
    current = db.execute(
        select(table).where(table.c.cookie_session_id == existing.cookie_session_id)
    ).first()
    return {
        "cookie_session_id": existing.cookie_session_id,
        "status": current.status,
        "last_refresh_at": _iso_datetime(current.last_refresh_at),
        "expires_at": _iso_datetime(current.expires_at),
    }


def record_cookie_probe(
    db: Session,
    *,
    cookie_session_id: str,
    outcome: str,
    http_status: int | None,
    latency_ms: int | None,
    status_reason: str | None,
    now: datetime | None = None,
) -> dict[str, object]:
    now = _ensure_utc(now) or _now_utc()
    session_table = Base.metadata.tables["cookie_session"]
    probe_table = Base.metadata.tables["cookie_probe_log"]

    session_row = db.execute(
        select(session_table).where(session_table.c.cookie_session_id == cookie_session_id)
    ).first()
    if not session_row:
        raise CookieSessionNotFoundError(cookie_session_id)

    db.execute(
        probe_table.insert().values(
            probe_log_id=str(uuid4()),
            cookie_session_id=cookie_session_id,
            probe_outcome=outcome,
            http_status=http_status,
            latency_ms=latency_ms,
            status_reason=status_reason,
            probed_at=now,
        )
    )

    update_values = {
        "last_probe_at": now,
        "updated_at": now,
    }
    if outcome == "success":
        update_values.update(
            {
                "status": "ACTIVE",
                "last_refresh_at": now,
                "expires_at": _expires_at_for_provider(session_row.provider, now),
                "status_reason": None,
            }
        )
    elif outcome == "failed":
        update_values.update({"status": "REFRESH_FAILED", "status_reason": status_reason})
    elif outcome == "skipped":
        update_values.update({"status": "SKIPPED", "status_reason": status_reason})

    db.execute(
        session_table.update()
        .where(session_table.c.cookie_session_id == cookie_session_id)
        .values(**update_values)
    )
    current = db.execute(
        select(session_table).where(session_table.c.cookie_session_id == cookie_session_id)
    ).first()
    return {
        "status": _PROBE_OUTCOME_TO_API_STATUS[outcome],
        "last_refresh_at": _iso_datetime(current.last_refresh_at) if current else None,
        "status_reason": status_reason,
    }


def get_cookie_session_health(
    db: Session,
    *,
    login_source: str,
    session_id: str | None = None,
) -> dict[str, object]:
    probe_table = Base.metadata.tables["cookie_probe_log"]
    session_row = _get_session_row(db, login_source=login_source, session_id=session_id)
    if not session_row:
        raise CookieSessionNotFoundError(session_id or login_source)

    latest_probe = db.execute(
        select(probe_table)
        .where(probe_table.c.cookie_session_id == session_row.cookie_session_id)
        .order_by(probe_table.c.probed_at.desc(), probe_table.c.probe_log_id.asc())
    ).first()
    if latest_probe:
        status = _PROBE_OUTCOME_TO_API_STATUS[latest_probe.probe_outcome]
        status_reason = latest_probe.status_reason
    else:
        status = _SESSION_STATUS_TO_API_STATUS.get(session_row.status, "fail")
        status_reason = session_row.status_reason

    return {
        "status": status,
        "last_refresh_at": _iso_datetime(session_row.last_refresh_at),
        "status_reason": status_reason,
    }


# ── FR03-COOKIE-02: 真实探活逻辑 ────────────────────────

def _do_http_probe(provider: str, cookie_blob: str) -> tuple[str, int | None, int | None, str | None]:
    """发起 HTTP 请求探测 cookie 有效性。返回 (outcome, http_status, latency_ms, reason)."""
    url = _PROBE_URLS.get(provider)
    if not url:
        return "skipped", None, None, f"unknown_provider:{provider}"
    headers = {"Cookie": cookie_blob, "User-Agent": "Mozilla/5.0"}
    start = time.monotonic()
    try:
        resp = httpx.get(url, headers=headers, timeout=PROBE_TIMEOUT_SEC, follow_redirects=True)
        latency = int((time.monotonic() - start) * 1000)
        if resp.status_code < 400:
            return "success", resp.status_code, latency, None
        return "failed", resp.status_code, latency, f"http_{resp.status_code}"
    except httpx.TimeoutException:
        latency = int((time.monotonic() - start) * 1000)
        return "failed", None, latency, "timeout"
    except Exception as exc:
        latency = int((time.monotonic() - start) * 1000)
        return "failed", None, latency, str(exc)[:200]


def _count_consecutive_failures(db: Session, cookie_session_id: str) -> int:
    """计算最近连续失败次数（从最新记录开始，遇到非 failed 即停止）。"""
    probe_table = Base.metadata.tables["cookie_probe_log"]
    rows = db.execute(
        select(probe_table.c.probe_outcome)
        .where(probe_table.c.cookie_session_id == cookie_session_id)
        .order_by(probe_table.c.probed_at.desc())
        .limit(10)
    ).fetchall()
    count = 0
    for row in rows:
        if row[0] == "failed":
            count += 1
        else:
            break
    return count


def _trigger_cookie_failure_alert(provider: str, consecutive: int) -> None:
    """Emit a truthful NFR-13 operational alert without reusing FR-13 event_type."""
    try:
        from app.services.notification import emit_operational_alert

        status, reason, channel = emit_operational_alert(
            alert_type="COOKIE_CONSECUTIVE_FAILURE",
            fr_id="FR-03",
            message=f"cookie {provider} probe failed {consecutive} times consecutively",
            payload={
                "provider": provider,
                "consecutive_failures": consecutive,
            },
        )
        logger.warning(
            "cookie_failure_alert provider=%s consecutive=%s status=%s channel=%s reason=%s",
            provider,
            consecutive,
            status,
            channel,
            reason,
        )
    except Exception:
        logger.exception("cookie_failure_alert_dispatch_failed provider=%s", provider)


def execute_cookie_probe(db: Session, *, login_source: str) -> dict:
    """
    FR03-COOKIE-02: 对指定 provider 执行一次完整探活流程。
    1. DB 分布式互斥 — 检查 TTL 内是否有探活记录
    2. 进程内互斥锁 — 获锁失败→skipped
    3. HTTP 探测
    4. 记录结果
    5. 连续失败≥2→NFR-13 告警
    """
    session_row = _get_session_row(db, login_source=login_source)
    if not session_row:
        return {"outcome": "skipped", "reason": "no_session"}

    # ── 分布式互斥: 检查 TTL 内是否已有成功探活 ──
    probe_table = Base.metadata.tables["cookie_probe_log"]
    ttl_cutoff = _now_utc() - timedelta(seconds=_PROBE_DISTRIBUTED_TTL_SEC)
    recent_probe = db.execute(
        select(probe_table.c.probe_log_id)
        .where(
            probe_table.c.cookie_session_id == session_row.cookie_session_id,
            probe_table.c.probed_at >= ttl_cutoff,
            probe_table.c.probe_outcome == "success",
        )
        .limit(1)
    ).first()
    if recent_probe is not None:
        return {"outcome": "skipped", "reason": "distributed_ttl"}

    acquired = _probe_lock.acquire(blocking=False)
    if not acquired:
        record_cookie_probe(
            db,
            cookie_session_id=session_row.cookie_session_id,
            outcome="skipped",
            http_status=None,
            latency_ms=None,
            status_reason="mutex_busy",
        )
        db.commit()
        return {"outcome": "skipped", "reason": "mutex_busy"}

    try:
        cookie_blob = session_row.cookie_blob or ""
        outcome, http_status, latency_ms, reason = _do_http_probe(login_source, cookie_blob)
        result = record_cookie_probe(
            db,
            cookie_session_id=session_row.cookie_session_id,
            outcome=outcome,
            http_status=http_status,
            latency_ms=latency_ms,
            status_reason=reason,
        )
        # 检查连续失败次数
        trigger_failure_alert = False
        consecutive = 0
        if outcome == "failed":
            consecutive = _count_consecutive_failures(db, session_row.cookie_session_id)
            if consecutive >= CONSECUTIVE_FAILURE_ALERT_THRESHOLD:
                trigger_failure_alert = True
        db.commit()
        if trigger_failure_alert:
            _trigger_cookie_failure_alert(login_source, consecutive)
        return {"outcome": outcome, "http_status": http_status, "latency_ms": latency_ms, "reason": reason}
    finally:
        _probe_lock.release()


def run_all_cookie_probes(db: Session) -> list[dict]:
    """FR03-COOKIE-02: 对所有 ACTIVE cookie 会话执行探活。供 scheduler cron 调用。"""
    # FR03-COOKIE-03: 先扫描即将过期/已过期会话 → 迁移状态
    _transition_expiring_sessions(db)

    session_table = Base.metadata.tables["cookie_session"]
    active_sessions = db.execute(
        select(session_table.c.provider)
        .where(session_table.c.status.in_(("ACTIVE", "EXPIRING", "REFRESH_FAILED")))
        .group_by(session_table.c.provider)
    ).fetchall()
    results = []
    for row in active_sessions:
        provider = row[0]
        try:
            r = execute_cookie_probe(db, login_source=provider)
            results.append({"provider": provider, **r})
        except Exception:
            logger.exception("Cookie probe failed for %s", provider)
            results.append({"provider": provider, "outcome": "error"})
    return results


def _transition_expiring_sessions(db: Session) -> None:
    """FR03-COOKIE-03: ACTIVE→EXPIRING (距过期<30分钟) / ACTIVE→EXPIRED / EXPIRING→EXPIRED."""
    session_table = Base.metadata.tables["cookie_session"]
    now = _now_utc()
    expiring_threshold = now + timedelta(minutes=30)

    # ACTIVE + expires_at <= now → EXPIRED
    db.execute(
        session_table.update()
        .where(
            session_table.c.status == "ACTIVE",
            session_table.c.expires_at <= now,
        )
        .values(status="EXPIRED", status_reason="auto_expired", updated_at=now)
    )
    # EXPIRING + expires_at <= now → EXPIRED
    db.execute(
        session_table.update()
        .where(
            session_table.c.status == "EXPIRING",
            session_table.c.expires_at <= now,
        )
        .values(status="EXPIRED", status_reason="auto_expired", updated_at=now)
    )
    # ACTIVE + expires_at <= now+30min → EXPIRING
    db.execute(
        session_table.update()
        .where(
            session_table.c.status == "ACTIVE",
            session_table.c.expires_at <= expiring_threshold,
            session_table.c.expires_at > now,
        )
        .values(status="EXPIRING", status_reason="approaching_expiry", updated_at=now)
    )
