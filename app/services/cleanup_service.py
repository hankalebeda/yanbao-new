"""
FR-09-b 清理服务 — 过期会话/临时Token/僵尸任务/未激活账号/通知清理
SSOT: 01 §FR-09-b, 03 §FR-09-b, 04 §4.11/§6.2
"""
from __future__ import annotations

import logging
import os
import platform
import threading
import time
from datetime import date as date_type, datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import and_, delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import Base, User

logger = logging.getLogger(__name__)

# ── retention defaults ───────────────────────────────────
SESSION_RETENTION_DAYS = 7
TEMP_TOKEN_RETENTION_DAYS = 7
ACCESS_TOKEN_LEASE_RETENTION_DAYS = 7
REFRESH_TOKEN_RETENTION_DAYS = 7
TASK_HISTORY_RETENTION_DAYS = 30
STALE_TASK_EXPIRE_DAYS = 3
NOTIFICATION_RETENTION_DAYS = 30
UNVERIFIED_ACCOUNT_HOURS = 24
RUNTIME_TEST_USER_DOMAINS = frozenset({"example.com", "test.com"})
PRESERVED_RUNTIME_TEST_PREFIXES = ("audit_", "audit-", "audit.", "v79_")
PROTECTED_RUNTIME_TEST_ROLES = frozenset({"admin", "super_admin"})

# ── batched delete settings (SSOT: 01 §FR-09-b 边界) ────
BATCH_SIZE = 500
BATCH_SLEEP_SEC = 0.1

# ── cleanup mutex (single-process) ──────────────────────
_cleanup_lock = threading.Lock()
_CLEANUP_LOCK_KEY = "cleanup"
_STALE_LOCK_TTL_MINUTES = 10  # auto-release RUNNING locks older than this
_HOLDER_ID = f"{platform.node()}:{os.getpid()}"  # ARC-01: 实例标识

# ── protected domain (04 §6.1) — never delete ───────────
PROTECTED_TABLES = frozenset({
    "report", "report_citation", "instruction_card",
    "sim_trade_instruction", "report_data_usage", "report_data_usage_link",
    "kline_daily", "sim_account", "sim_position",
    "sim_equity_curve_point", "baseline_equity_curve_point",
    "settlement_result", "strategy_metric_snapshot",
    "baseline_metric_snapshot", "experiment_log",
    "audit_log", "billing_order", "payment_webhook_event", "business_event",
})


class CleanupLeaseLostError(RuntimeError):
    """Raised when a cleanup runner loses ownership before persisting facts."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _cleanup_day_value(cleanup_date: str | None, *, now: datetime) -> date_type:
    return date_type.fromisoformat(cleanup_date) if cleanup_date else now.date()


def _cleanup_lock_key(cleanup_date: str) -> str:
    return f"{_CLEANUP_LOCK_KEY}:{cleanup_date}@{_HOLDER_ID}"


def _cleanup_lock_token(cleanup_date: str) -> str:
    return f"{_cleanup_lock_key(cleanup_date)}:{uuid4().hex[:12]}"[:128]


def _normalize_utc(value: datetime | None, *, fallback: datetime) -> datetime:
    if value is None:
        return fallback
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _cleanup_lease_is_stale(updated_at: datetime | None, *, now: datetime) -> bool:
    return now - _normalize_utc(updated_at, fallback=now) >= timedelta(minutes=_STALE_LOCK_TTL_MINUTES)


def _running_cleanup_row(
    db: Session,
    *,
    cleanup_day: date_type | None = None,
    lock_key: str | None = None,
) -> dict | None:
    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    query = select(cleanup_task_t).where(cleanup_task_t.c.status == "RUNNING")
    if cleanup_day is not None:
        query = query.where(cleanup_task_t.c.cleanup_date == cleanup_day)
    if lock_key is not None:
        query = query.where(cleanup_task_t.c.lock_key == lock_key)
    return db.execute(
        query.order_by(cleanup_task_t.c.updated_at.desc(), cleanup_task_t.c.cleanup_id.desc())
    ).mappings().first()


def _start_cleanup_task(
    db: Session,
    *,
    cleanup_day: date_type,
    started_at: datetime,
    lock_key: str,
) -> tuple[str, bool]:
    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    cleanup_item_t = Base.metadata.tables["cleanup_task_item"]
    running_values = dict(
        cleanup_date=cleanup_day,
        status="RUNNING",
        request_id=None,
        lock_key=lock_key,
        deleted_session_count=0,
        deleted_temp_token_count=0,
        deleted_access_token_lease_count=0,
        deleted_report_generation_task_count=0,
        expired_stale_task_count=0,
        deleted_unverified_user_count=0,
        deleted_notification_count=0,
        duration_ms=None,
        status_reason=None,
        started_at=started_at,
        finished_at=None,
        updated_at=started_at,
    )
    cleanup_id = str(uuid4())

    for _attempt in range(3):
        existing = db.execute(
            select(cleanup_task_t.c.cleanup_id, cleanup_task_t.c.status, cleanup_task_t.c.updated_at)
            .where(cleanup_task_t.c.cleanup_date == cleanup_day)
            .limit(1)
        ).mappings().first()
        if existing is None:
            cleanup_id = str(uuid4())
            try:
                db.execute(
                    cleanup_task_t.insert().values(
                        cleanup_id=cleanup_id,
                        created_at=started_at,
                        **running_values,
                    )
                )
                db.commit()
                return cleanup_id, True
            except IntegrityError:
                db.rollback()
                continue

        cleanup_id = existing["cleanup_id"]
        existing_status = str(existing["status"] or "")
        existing_updated_at = existing["updated_at"]
        if existing_status == "RUNNING" and not _cleanup_lease_is_stale(existing_updated_at, now=started_at):
            return cleanup_id, False

        claim_query = (
            cleanup_task_t.update()
            .where(cleanup_task_t.c.cleanup_id == cleanup_id)
            .where(cleanup_task_t.c.status == existing_status)
        )
        if existing_updated_at is None:
            claim_query = claim_query.where(cleanup_task_t.c.updated_at.is_(None))
        else:
            claim_query = claim_query.where(cleanup_task_t.c.updated_at == existing_updated_at)

        claim_values = dict(running_values)
        claim_values["status_reason"] = "stale_lock_reclaimed" if existing_status == "RUNNING" else None
        result = db.execute(claim_query.values(**claim_values))
        if not result.rowcount:
            db.rollback()
            continue

        db.execute(delete(cleanup_item_t).where(cleanup_item_t.c.cleanup_id == cleanup_id))
        db.commit()
        return cleanup_id, True

    return cleanup_id, False


def _mark_cleanup_task_failed(
    db: Session,
    *,
    cleanup_id: str,
    cleanup_day: date_type,
    started_at: datetime,
    finished_at: datetime,
    lock_key: str,
    status_reason: str,
) -> bool:
    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    duration_ms = int((finished_at - started_at).total_seconds() * 1000)
    existing = db.execute(
        select(cleanup_task_t.c.cleanup_id)
        .where(cleanup_task_t.c.cleanup_id == cleanup_id)
        .limit(1)
    ).mappings().first()
    if existing is None:
        db.execute(
            cleanup_task_t.insert().values(
                cleanup_id=cleanup_id,
                cleanup_date=cleanup_day,
                status="FAILED",
                request_id=None,
                lock_key=lock_key,
                deleted_session_count=0,
                deleted_temp_token_count=0,
                deleted_access_token_lease_count=0,
                deleted_report_generation_task_count=0,
                expired_stale_task_count=0,
                deleted_unverified_user_count=0,
                deleted_notification_count=0,
                duration_ms=duration_ms,
                status_reason=status_reason[:500],
                started_at=started_at,
                finished_at=finished_at,
                updated_at=finished_at,
                created_at=started_at,
            )
        )
        db.commit()
        return True
    updated = db.execute(
        cleanup_task_t.update()
        .where(cleanup_task_t.c.cleanup_id == cleanup_id)
        .where(cleanup_task_t.c.status == "RUNNING")
        .where(cleanup_task_t.c.lock_key == lock_key)
        .values(
            cleanup_date=cleanup_day,
            status="FAILED",
            lock_key=lock_key,
            duration_ms=duration_ms,
            status_reason=status_reason[:500],
            started_at=started_at,
            finished_at=finished_at,
            updated_at=finished_at,
        )
    )
    if not updated.rowcount:
        db.rollback()
        logger.warning(
            "cleanup failure fact dropped after lease loss cleanup_id=%s cleanup_day=%s lock_key=%s",
            cleanup_id,
            cleanup_day,
            lock_key,
        )
        return False
    db.commit()
    return True


def _email_parts(email: str | None) -> tuple[str, str]:
    raw = str(email or "").strip().lower()
    if "@" not in raw:
        return raw, ""
    local, domain = raw.split("@", 1)
    return local, domain


def _is_runtime_test_user_email(email: str | None) -> bool:
    _, domain = _email_parts(email)
    return domain in RUNTIME_TEST_USER_DOMAINS


def _is_preserved_runtime_test_account(*, email: str | None, email_verified: bool) -> bool:
    local, _ = _email_parts(email)
    return bool(email_verified) and any(local.startswith(prefix) for prefix in PRESERVED_RUNTIME_TEST_PREFIXES)


def _collect_protected_user_ids(db: Session) -> set[str]:
    protected_user_ids: set[str] = set()
    reference_queries = (
        ("audit_log", "actor_user_id"),
        ("admin_operation", "actor_user_id"),
        ("report", "reviewed_by"),
        ("billing_order", "user_id"),
        ("notification", "recipient_user_id"),
    )
    for table_name, column_name in reference_queries:
        table = Base.metadata.tables.get(table_name)
        if table is None or not hasattr(table.c, column_name):
            continue
        rows = db.execute(
            select(getattr(table.c, column_name)).where(getattr(table.c, column_name).isnot(None))
        ).fetchall()
        protected_user_ids.update(str(row[0]) for row in rows if row and row[0])
    return protected_user_ids


def _select_runtime_test_user_candidates(db: Session) -> list[dict[str, object]]:
    protected_user_ids = _collect_protected_user_ids(db)
    rows = db.execute(
        select(
            User.user_id,
            User.email,
            User.role,
            User.tier,
            User.email_verified,
            User.created_at,
        ).where(User.email.isnot(None))
    ).mappings().all()
    candidates: list[dict[str, object]] = []
    for row in rows:
        user_id = str(row.get("user_id") or "")
        email = row.get("email")
        role = str(row.get("role") or "").lower()
        if not user_id or not _is_runtime_test_user_email(email):
            continue
        if user_id in protected_user_ids:
            continue
        if role in PROTECTED_RUNTIME_TEST_ROLES:
            continue
        if _is_preserved_runtime_test_account(email=email, email_verified=bool(row.get("email_verified"))):
            continue
        candidates.append(dict(row))
    return candidates


def _delete_runtime_test_user_pollution(db: Session, *, result: dict[str, object]) -> None:
    candidates = _select_runtime_test_user_candidates(db)
    user_ids = [str(row["user_id"]) for row in candidates if row.get("user_id")]
    if not user_ids:
        result["deleted_runtime_test_user_count"] = 0
        result["deleted_runtime_test_session_count"] = 0
        result["deleted_runtime_test_refresh_token_count"] = 0
        result["deleted_runtime_test_temp_token_count"] = 0
        result["deleted_runtime_test_access_token_lease_count"] = 0
        result["deleted_runtime_test_jti_blacklist_count"] = 0
        result["deleted_runtime_test_oauth_identity_count"] = 0
        result["deleted_runtime_test_notification_count"] = 0
        result["deleted_runtime_test_report_feedback_count"] = 0
        return

    refresh_token_t = Base.metadata.tables["refresh_token"]
    session_t = Base.metadata.tables["user_session"]
    temp_token_t = Base.metadata.tables["auth_temp_token"]
    atl_t = Base.metadata.tables["access_token_lease"]
    jti_blacklist_t = Base.metadata.tables["jti_blacklist"]
    oauth_identity_t = Base.metadata.tables["oauth_identity"]
    notification_t = Base.metadata.tables["notification"]
    report_feedback_t = Base.metadata.tables["report_feedback"]

    user_filter = User.__table__.c.user_id.in_(user_ids)
    result["deleted_runtime_test_refresh_token_count"] = _batched_delete(
        db,
        refresh_token_t,
        refresh_token_t.c.user_id.in_(user_ids),
    )
    result["deleted_runtime_test_access_token_lease_count"] = _batched_delete(
        db,
        atl_t,
        atl_t.c.user_id.in_(user_ids),
    )
    result["deleted_runtime_test_temp_token_count"] = _batched_delete(
        db,
        temp_token_t,
        temp_token_t.c.user_id.in_(user_ids),
    )
    result["deleted_runtime_test_jti_blacklist_count"] = _batched_delete(
        db,
        jti_blacklist_t,
        jti_blacklist_t.c.user_id.in_(user_ids),
    )
    result["deleted_runtime_test_oauth_identity_count"] = _batched_delete(
        db,
        oauth_identity_t,
        oauth_identity_t.c.user_id.in_(user_ids),
    )
    result["deleted_runtime_test_notification_count"] = _batched_delete(
        db,
        notification_t,
        notification_t.c.recipient_user_id.in_(user_ids),
    )
    result["deleted_runtime_test_report_feedback_count"] = _batched_delete(
        db,
        report_feedback_t,
        report_feedback_t.c.user_id.in_(user_ids),
    )
    result["deleted_runtime_test_session_count"] = _batched_delete(
        db,
        session_t,
        session_t.c.user_id.in_(user_ids),
    )
    result["deleted_runtime_test_user_count"] = _batched_delete(db, User.__table__, user_filter)

    desensitized: list[str] = []
    for row in candidates:
        email = row.get("email")
        if not email:
            continue
        local, domain = _email_parts(str(email))
        local_hint = (local[:3] if len(local) > 3 else local) + "***"
        desensitized.append(f"{local_hint}@{domain or 'unknown'}")
    if desensitized:
        result["deleted_runtime_test_user_emails_desensitized"] = sorted(desensitized)
        logger.info(
            "runtime_test_users_deleted count=%d emails=%s",
            len(desensitized),
            sorted(desensitized),
        )


def _batched_delete(db: Session, table, where_clause) -> int:
    """分批删除: 每批 ≤500 行, 批间 sleep 100ms (SSOT: 01 §FR-09-b)."""
    pk_col = list(table.primary_key.columns)[0]
    total = 0
    while True:
        batch_ids = [
            row[0]
            for row in db.execute(
                select(pk_col).where(where_clause).limit(BATCH_SIZE)
            ).fetchall()
        ]
        if not batch_ids:
            break
        deleted = db.execute(delete(table).where(pk_col.in_(batch_ids)))
        total += deleted.rowcount
        if len(batch_ids) < BATCH_SIZE:
            break
        time.sleep(BATCH_SLEEP_SEC)
    return total


def _count_rows(db: Session, table, where_clause) -> int:
    return int(db.scalar(select(func.count()).select_from(table).where(where_clause)) or 0)


def _record_protected_domain_block(
    cleanup_item_failures: list[dict[str, object]],
    *,
    table_name: str,
    affected_count: int,
) -> None:
    if affected_count <= 0:
        return
    logger.warning(
        "cleanup_protected_domain_blocked table=%s affected_count=%s",
        table_name,
        affected_count,
    )
    cleanup_item_failures.append(
        {
            "target_domain": "protected_domain_check",
            "result": "failed",
            "affected_count": affected_count,
            "status_reason": "protected_domain_forbidden",
        }
    )


def _emit_cleanup_protected_domain_alert(
    *,
    cleanup_date: str,
    orphan_sim_position_count: int,
    orphan_settlement_result_count: int,
) -> None:
    try:
        from app.services.notification import emit_operational_alert

        emit_operational_alert(
            alert_type="CLEANUP_PROTECTED_DOMAIN_BLOCKED",
            fr_id="FR-09-b",
            message=f"cleanup blocked protected-domain candidates for {cleanup_date}",
            payload={
                "cleanup_date": cleanup_date,
                "status_reason": "protected_domain_forbidden",
                "orphan_sim_position_count": orphan_sim_position_count,
                "orphan_settlement_result_count": orphan_settlement_result_count,
            },
        )
    except Exception:
        logger.exception(
            "cleanup_protected_domain_alert_dispatch_failed cleanup_date=%s",
            cleanup_date,
        )


def _run_cleanup_inner(
    db: Session,
    *,
    cleanup_date: str | None = None,
    cleanup_id: str | None = None,
    lock_key: str | None = None,
    purge_test_account_pollution: bool = False,
) -> dict:
    """Execute full cleanup cycle. Returns summary counts dict."""
    now = _utc_now()
    if cleanup_date is None:
        cleanup_date = now.strftime("%Y-%m-%d")

    result = {
        "cleanup_date": cleanup_date,
        "deleted_session_count": 0,
        "deleted_temp_token_count": 0,
        "deleted_access_token_lease_count": 0,
        "deleted_report_generation_task_count": 0,
        "expired_stale_task_count": 0,
        "deleted_unverified_user_count": 0,
        "deleted_unverified_session_count": 0,
        "deleted_unverified_refresh_token_count": 0,
        "deleted_unverified_temp_token_count": 0,
        "deleted_unverified_access_token_lease_count": 0,
        "deleted_unverified_jti_blacklist_count": 0,
        "deleted_unverified_oauth_identity_count": 0,
        "deleted_unverified_notification_count": 0,
        "deleted_unverified_report_feedback_count": 0,
        "deleted_orphan_auth_temp_token_count": 0,
        "deleted_notification_count": 0,
        "deleted_legacy_all_subscribers_notification_count": 0,
        "deleted_runtime_test_user_count": 0,
        "deleted_runtime_test_session_count": 0,
        "deleted_runtime_test_refresh_token_count": 0,
        "deleted_runtime_test_temp_token_count": 0,
        "deleted_runtime_test_access_token_lease_count": 0,
        "deleted_runtime_test_jti_blacklist_count": 0,
        "deleted_runtime_test_oauth_identity_count": 0,
        "deleted_runtime_test_notification_count": 0,
        "deleted_runtime_test_report_feedback_count": 0,
        "deleted_stale_sim_equity_curve_point_count": 0,
        "deleted_stale_baseline_equity_curve_point_count": 0,
        "deleted_stale_sim_dashboard_snapshot_count": 0,
        "deleted_stale_strategy_metric_snapshot_count": 0,
        "deleted_stale_baseline_metric_snapshot_count": 0,
        "reset_sim_account_count": 0,
    }
    cleanup_item_counts: dict[str, int] = {}
    cleanup_item_failures: list[dict[str, object]] = []

    # Step 1: expired sessions (> 7 days, status in EXPIRED/REVOKED/BLACKLISTED)
    session_t = Base.metadata.tables["user_session"]
    cutoff_session = now - timedelta(days=SESSION_RETENTION_DAYS)
    result["deleted_session_count"] = _batched_delete(
        db, session_t,
        and_(session_t.c.status.in_(("EXPIRED", "REVOKED", "BLACKLISTED")),
             session_t.c.updated_at < cutoff_session),
    )

    # Step 1b: expired temp tokens (> 7 days)
    temp_token_t = Base.metadata.tables["auth_temp_token"]
    cutoff_token = now - timedelta(days=TEMP_TOKEN_RETENTION_DAYS)
    result["deleted_temp_token_count"] = _batched_delete(
        db, temp_token_t, temp_token_t.c.expires_at < cutoff_token,
    )

    # Step 1c: expired access_token_lease (> 7 days past expiry)
    atl_t = Base.metadata.tables["access_token_lease"]
    cutoff_atl = now - timedelta(days=ACCESS_TOKEN_LEASE_RETENTION_DAYS)
    result["deleted_access_token_lease_count"] = _batched_delete(
        db, atl_t, atl_t.c.expires_at < cutoff_atl,
    )

    # Step 1d: expired refresh_token (> 7 days past expiry), only record in cleanup_task_item
    refresh_token_t = Base.metadata.tables["refresh_token"]
    cutoff_refresh = now - timedelta(days=REFRESH_TOKEN_RETENTION_DAYS)
    cleanup_item_counts["refresh_token"] = _batched_delete(
        db, refresh_token_t, refresh_token_t.c.expires_at < cutoff_refresh,
    )

    # Step 1e: expired jti_blacklist (delete immediately after expiry), only record in cleanup_task_item
    jti_blacklist_t = Base.metadata.tables["jti_blacklist"]
    cleanup_item_counts["jti_blacklist"] = _batched_delete(
        db, jti_blacklist_t, jti_blacklist_t.c.expires_at < now,
    )

    # Step 2: unverified accounts (> 24h, email_verified=false)
    cutoff_unverified = now - timedelta(hours=UNVERIFIED_ACCOUNT_HOURS)
    # FR09B-CLEAN-02: 先查询待删email，生成脱敏列表供审计
    unverified_rows = db.execute(
        select(User.user_id, User.email).where(
            User.email_verified == False,  # noqa: E712
            User.created_at < cutoff_unverified,
        )
    ).fetchall()
    unverified_user_ids = [str(row[0]) for row in unverified_rows if row and row[0]]
    unverified_emails = [row[1] for row in unverified_rows if row and row[1]]
    if unverified_user_ids:
        oauth_identity_t = Base.metadata.tables["oauth_identity"]
        notification_t = Base.metadata.tables["notification"]
        report_feedback_t = Base.metadata.tables["report_feedback"]
        result["deleted_unverified_refresh_token_count"] = _batched_delete(
            db, refresh_token_t, refresh_token_t.c.user_id.in_(unverified_user_ids)
        )
        result["deleted_unverified_access_token_lease_count"] = _batched_delete(
            db, atl_t, atl_t.c.user_id.in_(unverified_user_ids)
        )
        result["deleted_unverified_temp_token_count"] = _batched_delete(
            db, temp_token_t, temp_token_t.c.user_id.in_(unverified_user_ids)
        )
        result["deleted_unverified_jti_blacklist_count"] = _batched_delete(
            db, jti_blacklist_t, jti_blacklist_t.c.user_id.in_(unverified_user_ids)
        )
        result["deleted_unverified_oauth_identity_count"] = _batched_delete(
            db, oauth_identity_t, oauth_identity_t.c.user_id.in_(unverified_user_ids)
        )
        result["deleted_unverified_notification_count"] = _batched_delete(
            db, notification_t, notification_t.c.recipient_user_id.in_(unverified_user_ids)
        )
        result["deleted_unverified_report_feedback_count"] = _batched_delete(
            db, report_feedback_t, report_feedback_t.c.user_id.in_(unverified_user_ids)
        )
        result["deleted_unverified_session_count"] = _batched_delete(
            db, session_t, session_t.c.user_id.in_(unverified_user_ids)
        )
    result["deleted_unverified_user_count"] = _batched_delete(
        db, User.__table__,
        and_(User.email_verified == False, User.created_at < cutoff_unverified),  # noqa: E712
    )
    # 脱敏审计记录: 首3字符+***+@域名
    if unverified_emails:
        desensitized = []
        for email in unverified_emails:
            if not email:
                continue
            parts = str(email).split("@")
            local = parts[0][:3] + "***" if len(parts[0]) > 3 else parts[0] + "***"
            domain = parts[1] if len(parts) > 1 else "unknown"
            desensitized.append(f"{local}@{domain}")
        result["deleted_unverified_emails_desensitized"] = desensitized
        logger.info("unverified_accounts_deleted count=%d emails=%s", len(desensitized), desensitized)

    # Step 2b: controlled runtime test-account cleanup for shared example/test domains.
    # Default scheduled cleanup keeps prior FR09B semantics; explicit pollution cleanup opts in.
    if purge_test_account_pollution:
        _delete_runtime_test_user_pollution(db, result=result)

    result["deleted_orphan_auth_temp_token_count"] = _batched_delete(
        db,
        temp_token_t,
        temp_token_t.c.user_id.not_in(select(User.__table__.c.user_id)),
    )

    # Step 3a: stale Pending/Suspended tasks → Expired (> 3 days)
    task_t = Base.metadata.tables["report_generation_task"]
    cutoff_stale = now - timedelta(days=STALE_TASK_EXPIRE_DAYS)
    expired_stale = db.execute(
        update(task_t)
        .where(
            task_t.c.status.in_(("Pending", "Suspended")),
            task_t.c.updated_at < cutoff_stale,
        )
        .values(status="Expired", status_reason="stale_task_expired")
    )
    result["expired_stale_task_count"] = expired_stale.rowcount

    # Step 3b: completed/failed/expired tasks > 30 days (not referenced by report)
    cutoff_task = now - timedelta(days=TASK_HISTORY_RETENTION_DAYS)
    # Only delete tasks not referenced by any report
    report_t = Base.metadata.tables["report"]
    referenced_task_ids = select(report_t.c.generation_task_id).where(
        report_t.c.generation_task_id.isnot(None)
    ).scalar_subquery()
    result["deleted_report_generation_task_count"] = _batched_delete(
        db, task_t,
        and_(task_t.c.status.in_(("Completed", "Failed", "Expired")),
             task_t.c.updated_at < cutoff_task,
             task_t.c.task_id.not_in(select(report_t.c.generation_task_id).where(
                 report_t.c.generation_task_id.isnot(None)
             ))),
    )

    # Step 4a: purge historical aggregate user notifications that never
    # represented a truthful per-user delivery fact under FR-13.
    notif_t = Base.metadata.tables["notification"]
    result["deleted_legacy_all_subscribers_notification_count"] = _batched_delete(
        db,
        notif_t,
        and_(
            notif_t.c.recipient_scope == "user",
            notif_t.c.recipient_key == "all_subscribers",
            notif_t.c.recipient_user_id.is_(None),
        ),
    )

    # Step 4b: notification retention cleanup (sent/skipped/failed > 30 days)
    cutoff_notif = now - timedelta(days=NOTIFICATION_RETENTION_DAYS)
    result["deleted_notification_count"] = _batched_delete(
        db, notif_t,
        and_(notif_t.c.status.in_(("sent", "skipped", "failed")),
             notif_t.c.created_at < cutoff_notif),
    )

    outbox_t = Base.metadata.tables["outbox_event"]
    cleanup_item_counts["outbox_event"] = _batched_delete(
        db,
        outbox_t,
        and_(
            outbox_t.c.dispatch_status.in_(("DISPATCHED", "DISPATCH_FAILED")),
            func.coalesce(outbox_t.c.dispatched_at, outbox_t.c.updated_at, outbox_t.c.created_at) < cutoff_notif,
        ),
    )

    # Step 5/6: sim_position / settlement_result are protected domains.
    sim_pos_t = Base.metadata.tables["sim_position"]
    report_t = Base.metadata.tables["report"]
    orphan_sim_position_count = _count_rows(
        db,
        sim_pos_t,
        sim_pos_t.c.report_id.not_in(select(report_t.c.report_id)),
    )
    _record_protected_domain_block(
        cleanup_item_failures,
        table_name="sim_position",
        affected_count=orphan_sim_position_count,
    )

    settle_t = Base.metadata.tables["settlement_result"]
    orphan_settlement_result_count = _count_rows(
        db,
        settle_t,
        settle_t.c.report_id.not_in(select(report_t.c.report_id)),
    )
    _record_protected_domain_block(
        cleanup_item_failures,
        table_name="settlement_result",
        affected_count=orphan_settlement_result_count,
    )
    if cleanup_item_failures:
        _emit_cleanup_protected_domain_alert(
            cleanup_date=cleanup_date,
            orphan_sim_position_count=orphan_sim_position_count,
            orphan_settlement_result_count=orphan_settlement_result_count,
        )

    # Step 7: persist cleanup_task record (P1-34)
    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    cleanup_item_t = Base.metadata.tables["cleanup_task_item"]
    cleanup_day = date_type.fromisoformat(cleanup_date)
    existing_cleanup = db.execute(
        select(cleanup_task_t.c.cleanup_id, cleanup_task_t.c.status_reason)
        .where(cleanup_task_t.c.cleanup_date == cleanup_day)
        .limit(1)
    ).mappings().first()
    cleanup_id = str(cleanup_id or (existing_cleanup["cleanup_id"] if existing_cleanup else str(uuid4())))
    if lock_key is None:
        lock_key = _cleanup_lock_key(cleanup_date)
    preserved_status_reason = (
        existing_cleanup.get("status_reason") if existing_cleanup and existing_cleanup.get("status_reason") == "mutex_busy" else None
    )
    finished_at = _utc_now()
    duration_ms = int((finished_at - now).total_seconds() * 1000)
    cleanup_task_values = dict(
        cleanup_date=cleanup_day,
        status="COMPLETED",
        deleted_session_count=result.get("deleted_session_count", 0),
        deleted_temp_token_count=result.get("deleted_temp_token_count", 0),
        deleted_access_token_lease_count=result.get("deleted_access_token_lease_count", 0),
        deleted_report_generation_task_count=result.get("deleted_report_generation_task_count", 0),
        expired_stale_task_count=result.get("expired_stale_task_count", 0),
        deleted_unverified_user_count=result.get("deleted_unverified_user_count", 0),
        deleted_notification_count=result.get("deleted_notification_count", 0),
        duration_ms=duration_ms,
        status_reason=preserved_status_reason,
        started_at=now,
        finished_at=finished_at,
        updated_at=finished_at,
    )
    if existing_cleanup:
        updated = db.execute(
            cleanup_task_t.update()
            .where(cleanup_task_t.c.cleanup_id == cleanup_id)
            .where(cleanup_task_t.c.status == "RUNNING")
            .where(cleanup_task_t.c.lock_key == lock_key)
            .values(**cleanup_task_values)
        )
        if not updated.rowcount:
            db.rollback()
            raise CleanupLeaseLostError(f"cleanup lease lost before completion: {cleanup_id}")
        db.execute(delete(cleanup_item_t).where(cleanup_item_t.c.cleanup_id == cleanup_id))
    else:
        db.execute(
            cleanup_task_t.insert().values(
                cleanup_id=cleanup_id,
                created_at=now,
                **cleanup_task_values,
            )
        )
    step_no = 0
    cleanup_item_successes = (
        (
            "user_session",
            result["deleted_session_count"]
            + result["deleted_unverified_session_count"]
            + result["deleted_runtime_test_session_count"],
        ),
        (
            "auth_temp_token",
            result["deleted_temp_token_count"]
            + result["deleted_orphan_auth_temp_token_count"]
            + result["deleted_unverified_temp_token_count"]
            + result["deleted_runtime_test_temp_token_count"],
        ),
        (
            "access_token_lease",
            result["deleted_access_token_lease_count"]
            + result["deleted_unverified_access_token_lease_count"]
            + result["deleted_runtime_test_access_token_lease_count"],
        ),
        (
            "report_generation_task",
            result["deleted_report_generation_task_count"] + result["expired_stale_task_count"],
        ),
        (
            "notification",
            result["deleted_notification_count"]
            + result["deleted_legacy_all_subscribers_notification_count"]
            + result["deleted_unverified_notification_count"]
            + result["deleted_runtime_test_notification_count"],
        ),
        (
            "refresh_token",
            cleanup_item_counts.get("refresh_token", 0)
            + result["deleted_unverified_refresh_token_count"]
            + result["deleted_runtime_test_refresh_token_count"],
        ),
        (
            "jti_blacklist",
            cleanup_item_counts.get("jti_blacklist", 0)
            + result["deleted_unverified_jti_blacklist_count"]
            + result["deleted_runtime_test_jti_blacklist_count"],
        ),
        ("outbox_event", cleanup_item_counts.get("outbox_event", 0)),
        ("unverified_user", result["deleted_unverified_user_count"]),
    )
    for domain, count in cleanup_item_successes:
        if not count:
            continue
        step_no += 1
        db.execute(
            cleanup_item_t.insert().values(
                cleanup_task_item_id=str(uuid4()),
                cleanup_id=cleanup_id,
                step_no=step_no,
                target_domain=domain,
                result="success",
                affected_count=count,
                created_at=now,
            )
        )
    for failure in cleanup_item_failures:
        step_no += 1
        db.execute(
            cleanup_item_t.insert().values(
                cleanup_task_item_id=str(uuid4()),
                cleanup_id=cleanup_id,
                step_no=step_no,
                target_domain=failure["target_domain"],
                result=failure["result"],
                affected_count=failure["affected_count"],
                status_reason=failure["status_reason"],
                created_at=now,
            )
        )

    db.commit()
    return result


def run_cleanup(
    db: Session,
    *,
    cleanup_date: str | None = None,
    purge_test_account_pollution: bool = False,
) -> dict:
    """Persist RUNNING/FAILED/COMPLETED cleanup facts around the existing inner workflow."""
    started_at = _utc_now()
    effective_cleanup_date = cleanup_date or started_at.strftime("%Y-%m-%d")
    cleanup_day = _cleanup_day_value(effective_cleanup_date, now=started_at)
    lock_key = _cleanup_lock_token(effective_cleanup_date)
    cleanup_id: str | None = None
    acquired = _cleanup_lock.acquire(blocking=False)
    if not acquired:
        logger.warning("cleanup mutex busy cleanup_date=%s", effective_cleanup_date)
        running_row = _running_cleanup_row(db, cleanup_day=cleanup_day)
        if running_row is not None:
            return {
                "cleanup_date": effective_cleanup_date,
                "cleanup_id": running_row["cleanup_id"],
                "skipped": True,
                "reason": "mutex_busy",
            }
        return {"cleanup_date": effective_cleanup_date, "skipped": True, "reason": "mutex_busy"}

    try:
        cleanup_id, claimed = _start_cleanup_task(
            db,
            cleanup_day=cleanup_day,
            started_at=started_at,
            lock_key=lock_key,
        )
        if not claimed:
            return {
                "cleanup_date": effective_cleanup_date,
                "cleanup_id": cleanup_id,
                "skipped": True,
                "reason": "mutex_busy",
            }
        result = _run_cleanup_inner(
            db,
            cleanup_date=effective_cleanup_date,
            cleanup_id=cleanup_id,
            lock_key=lock_key,
            purge_test_account_pollution=purge_test_account_pollution,
        )
        result["cleanup_id"] = cleanup_id
        return result
    except CleanupLeaseLostError:
        db.rollback()
        logger.warning(
            "cleanup lease lost cleanup_date=%s cleanup_id=%s lock_key=%s",
            effective_cleanup_date,
            cleanup_id,
            lock_key,
        )
        return {
            "cleanup_date": effective_cleanup_date,
            "cleanup_id": cleanup_id,
            "skipped": True,
            "reason": "lease_lost",
        }
    except Exception as exc:
        db.rollback()
        persisted = _mark_cleanup_task_failed(
            db,
            cleanup_id=cleanup_id,
            cleanup_day=cleanup_day,
            started_at=started_at,
            finished_at=_utc_now(),
            lock_key=lock_key,
            status_reason=str(exc),
        )
        if not persisted:
            return {
                "cleanup_date": effective_cleanup_date,
                "cleanup_id": cleanup_id,
                "skipped": True,
                "reason": "lease_lost",
            }
        try:
            from app.services.notification import emit_operational_alert

            emit_operational_alert(
                alert_type="CLEANUP_FAILED",
                fr_id="FR-09-b",
                message=f"cleanup failed for {effective_cleanup_date}",
                payload={
                    "cleanup_date": effective_cleanup_date,
                    "status_reason": str(exc),
                },
            )
        except Exception:
            logger.exception("cleanup_failed_alert_dispatch_failed cleanup_date=%s", effective_cleanup_date)
        raise
    finally:
        _cleanup_lock.release()
