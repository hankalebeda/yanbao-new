"""
FR-09-b 清理服务 — 验收用例
冻结测试名: test_fr09b_cleanup_keeps_reports,
            test_fr09b_cleanup_expire_stale_pending_suspended,
            test_fr09b_cleanup_unverified_account_release
来源: docs/core/01_需求基线.md §FR-09-b 追溯矩阵
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest

pytestmark = [
    pytest.mark.feature("FR-09"),
    pytest.mark.feature("FR09B-CLEAN-03"),
    pytest.mark.feature("FR09B-CLEAN-04"),
]

from sqlalchemy import func, select, text
from sqlalchemy.orm import sessionmaker

from app.models import Base, Report, User
from app.services.cleanup_service import run_cleanup
from tests.helpers_ssot import (
    age_report_generation_task,
    insert_baseline_equity_curve_point,
    insert_baseline_metric_snapshot,
    insert_report_bundle_ssot,
    insert_sim_account,
    insert_sim_dashboard_snapshot,
    insert_sim_equity_curve_point,
    insert_strategy_metric_snapshot,
)


def _utc_now():
    return datetime.now(timezone.utc)


def _old(days: int) -> datetime:
    """Generate a timestamp N days in the past."""
    return _utc_now() - timedelta(days=days)


# ──── 1. 清理不删核心研报 ──────────────────────────────
def test_fr09b_cleanup_keeps_reports(db_session):
    """清理前后 report 表记录数不变 — 核心研报绝不删除"""
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2025-01-01",
        strategy_type="A",
        recommendation="BUY",
        confidence=0.7,
    )

    count_before = db_session.scalar(
        select(func.count()).select_from(Report.__table__)
    )

    result = run_cleanup(db_session)

    count_after = db_session.scalar(
        select(func.count()).select_from(Report.__table__)
    )
    assert count_after == count_before
    assert count_before >= 1


# ──── 2. 僵尸任务转 Expired ───────────────────────────
def test_fr09b_cleanup_expire_stale_pending_suspended(db_session):
    """Pending/Suspended 超 3 天 → 转 Expired 且不再被拉起"""
    task_t = Base.metadata.tables["report_generation_task"]
    task_id = age_report_generation_task(
        db_session,
        stock_code="600000.SH",
        trade_date="2025-01-01",
        status="Pending",
        updated_hours_ago=120,
    )

    result = run_cleanup(db_session)

    assert result["expired_stale_task_count"] >= 1

    # Verify task is now Expired
    row = db_session.execute(
        select(task_t.c.status, task_t.c.status_reason).where(task_t.c.task_id == task_id)
    ).first()
    assert row is not None
    assert row[0] == "Expired"
    assert row[1] == "stale_task_expired"


# ──── 3. 未激活账号释放 ───────────────────────────────
def test_fr09b_cleanup_unverified_account_release(db_session):
    """email_verified=false 且超 24h → 物理删除释放邮箱"""
    old_time = _old(2)  # 2 days ago
    auth_temp_token_t = Base.metadata.tables["auth_temp_token"]

    user = User()
    user.user_id = str(uuid4())
    user.email = f"unverified-{uuid4().hex[:8]}@test.com"
    user.password_hash = "hashed"
    user.email_verified = False
    user.role = "user"
    user.tier = "Free"
    # Manually set created_at to old time
    db_session.add(user)
    db_session.flush()
    db_session.execute(
        User.__table__.update()
        .where(User.__table__.c.user_id == user.user_id)
        .values(created_at=old_time, updated_at=old_time)
    )
    db_session.execute(
        auth_temp_token_t.insert().values(
            temp_token_id=str(uuid4()),
            user_id=user.user_id,
            token_type="EMAIL_VERIFY",
            token_hash=f"verify-{uuid4().hex}",
            sent_at=old_time,
            used_at=None,
            expires_at=old_time + timedelta(days=3),
            created_at=old_time,
        )
    )
    db_session.commit()

    saved_user_id = user.user_id
    result = run_cleanup(db_session)

    assert result["deleted_unverified_user_count"] >= 1
    assert result["deleted_unverified_temp_token_count"] == 1

    # User should be gone
    db_session.expire_all()
    remaining = db_session.execute(
        select(User).where(User.__table__.c.user_id == saved_user_id)
    ).scalars().first()
    assert remaining is None
    assert db_session.scalar(
        select(func.count()).select_from(auth_temp_token_t).where(auth_temp_token_t.c.user_id == saved_user_id)
    ) == 0


def test_fr09b_cleanup_removes_orphan_auth_temp_tokens(db_session):
    auth_temp_token_t = Base.metadata.tables["auth_temp_token"]
    bogus_user_id = str(uuid4())
    old_time = _old(2)
    db_session.execute(
        auth_temp_token_t.insert().values(
            temp_token_id=str(uuid4()),
            user_id=bogus_user_id,
            token_type="PASSWORD_RESET",
            token_hash=f"orphan-{uuid4().hex}",
            sent_at=old_time,
            used_at=None,
            expires_at=old_time + timedelta(days=3),
            created_at=old_time,
        )
    )
    db_session.commit()

    result = run_cleanup(db_session)

    assert result["deleted_orphan_auth_temp_token_count"] == 1
    assert db_session.scalar(
        select(func.count()).select_from(auth_temp_token_t).where(auth_temp_token_t.c.user_id == bogus_user_id)
    ) == 0


# ──── 4. cleanup_log 完整计数字段 ─────────────────────
def test_fr09b_cleanup_result_fields(db_session):
    """cleanup 返回完整计数字段"""
    result = run_cleanup(db_session)

    required_fields = {
        "deleted_session_count",
        "deleted_temp_token_count",
        "deleted_access_token_lease_count",
        "deleted_report_generation_task_count",
        "expired_stale_task_count",
        "deleted_unverified_user_count",
        "deleted_notification_count",
    }
    assert required_fields <= set(result.keys())
    for field in required_fields:
        assert isinstance(result[field], int)
    assert "deleted_orphan_sim_position_count" not in result
    assert "deleted_orphan_settlement_result_count" not in result


def test_fr09b_cleanup_purges_legacy_all_subscribers_notifications(db_session):
    business_event_t = Base.metadata.tables["business_event"]
    cursor_t = Base.metadata.tables["event_projection_cursor"]
    notification_t = Base.metadata.tables["notification"]
    now = _utc_now()

    for idx in range(2):
        business_event_id = str(uuid4())
        projection_cursor_id = str(uuid4())
        projection_key = f"BUY_SIGNAL_DAILY:2026-03-1{idx}"
        db_session.execute(
            cursor_t.insert().values(
                projection_cursor_id=projection_cursor_id,
                event_type="BUY_SIGNAL_DAILY",
                event_projection_key=projection_key,
                last_business_event_id=None,
                last_sent_at=None,
                dedup_until=None,
                last_state_value=None,
                recovered_at=None,
                created_at=now,
                updated_at=now,
            )
        )
        db_session.execute(
            business_event_t.insert().values(
                business_event_id=business_event_id,
                event_type="BUY_SIGNAL_DAILY",
                projection_cursor_id=projection_cursor_id,
                event_projection_key=projection_key,
                event_status="ENQUEUED",
                source_table="report",
                source_pk=f"report-{idx}",
                stock_code=None,
                trade_date=date(2026, 3, 10 + idx),
                capital_tier=None,
                payload_json={},
                dedup_until=None,
                status_reason=None,
                created_at=now,
                enqueued_at=now,
            )
        )
        db_session.execute(
            notification_t.insert().values(
                notification_id=str(uuid4()),
                business_event_id=business_event_id,
                event_type="BUY_SIGNAL_DAILY",
                channel="email",
                recipient_scope="user",
                recipient_key="all_subscribers",
                recipient_user_id=None,
                triggered_at=now,
                status="skipped",
                payload_summary="legacy aggregate notification",
                status_reason="legacy_all_subscribers_placeholder",
                sent_at=None,
                created_at=now,
            )
        )
    db_session.commit()

    result = run_cleanup(db_session, cleanup_date="2026-03-23")

    assert result["deleted_legacy_all_subscribers_notification_count"] == 2
    assert db_session.scalar(select(func.count()).select_from(notification_t)) == 0


def test_fr09b_cleanup_skips_protected_sim_position_with_audit_trail(db_session, caplog):
    sim_position_t = Base.metadata.tables["sim_position"]
    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    cleanup_item_t = Base.metadata.tables["cleanup_task_item"]
    position_id = str(uuid4())

    db_session.execute(text("PRAGMA foreign_keys=OFF"))
    try:
        db_session.execute(
            sim_position_t.insert().values(
                position_id=position_id,
                report_id="orphan-protected-report-id",
                stock_code="600519.SH",
                capital_tier="100k",
                position_status="OPEN",
                signal_date=date(2025, 1, 1),
                position_ratio=0.2,
                shares=100,
            )
        )
        db_session.commit()
    finally:
        db_session.execute(text("PRAGMA foreign_keys=ON"))

    with caplog.at_level("WARNING", logger="app.services.cleanup_service"):
        result = run_cleanup(db_session, cleanup_date="2026-03-13")

    assert "deleted_orphan_sim_position_count" not in result
    db_session.expire_all()
    preserved_position = db_session.execute(
        select(sim_position_t).where(sim_position_t.c.position_id == position_id)
    ).mappings().one()
    assert preserved_position["report_id"] == "orphan-protected-report-id"

    cleanup_task = db_session.execute(select(cleanup_task_t)).mappings().one()
    assert cleanup_task["status"] == "COMPLETED"
    protected_items = db_session.execute(
        select(cleanup_item_t).where(cleanup_item_t.c.cleanup_id == cleanup_task["cleanup_id"])
    ).mappings().all()
    assert protected_items == [
        {
            "cleanup_task_item_id": protected_items[0]["cleanup_task_item_id"],
            "cleanup_id": cleanup_task["cleanup_id"],
            "step_no": 1,
            "target_domain": "protected_domain_check",
            "result": "failed",
            "affected_count": 1,
            "status_reason": "protected_domain_forbidden",
            "created_at": protected_items[0]["created_at"],
        }
    ]
    assert any(
        "cleanup_protected_domain_blocked table=sim_position affected_count=1" in message
        for message in caplog.messages
    )



def test_fr09b_cleanup_removes_orphans_and_persists_cleanup_records(db_session):
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2025-01-01",
        strategy_type="A",
        recommendation="BUY",
        confidence=0.7,
    )

    sim_position_t = Base.metadata.tables["sim_position"]
    settlement_result_t = Base.metadata.tables["settlement_result"]
    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    cleanup_item_t = Base.metadata.tables["cleanup_task_item"]

    db_session.execute(text("PRAGMA foreign_keys=OFF"))
    try:
        db_session.execute(
            sim_position_t.insert().values(
                position_id=str(uuid4()),
                report_id="orphan-report-id",
                stock_code="600519.SH",
                capital_tier="100k",
                position_status="OPEN",
                signal_date=date(2025, 1, 1),
                position_ratio=0.2,
                shares=100,
            )
        )
        db_session.execute(
            settlement_result_t.insert().values(
                settlement_id=str(uuid4()),
                settlement_result_id=str(uuid4()),
                report_id="orphan-report-id",
                stock_code="600519.SH",
                trade_date=date(2025, 1, 1),
                signal_date=date(2025, 1, 1),
                window_days=7,
                strategy_type="A",
                settlement_status="settled",
                quality_flag="ok",
                status_reason=None,
                entry_trade_date=date(2025, 1, 1),
                exit_trade_date=date(2025, 1, 8),
                shares=100,
                buy_price=10.0,
                sell_price=11.0,
                buy_commission=5.0,
                sell_commission=5.0,
                stamp_duty=0.1,
                buy_slippage_cost=0.5,
                sell_slippage_cost=0.5,
                gross_return_pct=0.1,
                net_return_pct=0.08,
                is_misclassified=False,
                display_hint=None,
                created_at=_utc_now(),
                updated_at=_utc_now(),
            )
        )
        db_session.commit()
    finally:
        db_session.execute(text("PRAGMA foreign_keys=ON"))

    result = run_cleanup(db_session, cleanup_date="2026-03-13")

    assert "deleted_orphan_sim_position_count" not in result
    assert "deleted_orphan_settlement_result_count" not in result
    assert db_session.scalar(select(func.count()).select_from(sim_position_t)) == 1
    assert db_session.scalar(select(func.count()).select_from(settlement_result_t)) == 1
    assert db_session.scalar(select(func.count()).select_from(cleanup_task_t)) == 1
    cleanup_task = db_session.execute(select(cleanup_task_t)).mappings().one()
    assert cleanup_task["status"] == "COMPLETED"
    items = db_session.execute(select(cleanup_item_t)).mappings().all()
    blocked_items = [
        item for item in items
        if item["target_domain"] == "protected_domain_check"
    ]
    assert len(blocked_items) == 2
    assert {item["result"] for item in blocked_items} == {"failed"}
    assert {item["status_reason"] for item in blocked_items} == {"protected_domain_forbidden"}


def test_fr09b_cleanup_resets_empty_sim_runtime_state(db_session):
    sim_account_t = Base.metadata.tables["sim_account"]
    sim_position_t = Base.metadata.tables["sim_position"]
    settlement_result_t = Base.metadata.tables["settlement_result"]
    sim_dashboard_t = Base.metadata.tables["sim_dashboard_snapshot"]
    sim_equity_t = Base.metadata.tables["sim_equity_curve_point"]
    strategy_metric_t = Base.metadata.tables["strategy_metric_snapshot"]
    baseline_metric_t = Base.metadata.tables["baseline_metric_snapshot"]
    baseline_equity_t = Base.metadata.tables["baseline_equity_curve_point"]

    db_session.execute(sim_position_t.delete())
    db_session.execute(settlement_result_t.delete())
    db_session.execute(sim_dashboard_t.delete())
    db_session.execute(sim_equity_t.delete())
    db_session.execute(strategy_metric_t.delete())
    db_session.execute(baseline_metric_t.delete())
    db_session.execute(baseline_equity_t.delete())
    db_session.commit()

    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=1_000_000,
        cash_available=36_111.64,
        total_asset=1_092_805.64,
        peak_total_asset=1_092_805.64,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
        active_position_count=5,
        last_reconciled_trade_date="2026-03-12",
    )
    insert_sim_dashboard_snapshot(
        db_session,
        capital_tier="100k",
        snapshot_date="2026-03-12",
        data_status="READY",
        total_return_pct=0.092806,
        sample_size=0,
        display_hint="sample_lt_30",
    )
    insert_sim_equity_curve_point(
        db_session,
        capital_tier="100k",
        trade_date="2026-03-12",
        equity=1_092_805.64,
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-12",
        strategy_type="A",
        sample_size=0,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        max_drawdown_pct=None,
        cumulative_return_pct=None,
        display_hint="sample_lt_30",
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-12",
        baseline_type="baseline_random",
        sample_size=0,
        win_rate=None,
        profit_loss_ratio=None,
        alpha_annual=None,
        max_drawdown_pct=None,
        cumulative_return_pct=None,
        display_hint="sample_lt_30",
    )
    insert_baseline_equity_curve_point(
        db_session,
        capital_tier="100k",
        baseline_type="baseline_random",
        trade_date="2026-03-12",
        equity=101000,
    )

    result = run_cleanup(db_session, cleanup_date="2026-03-13")

    assert result["deleted_stale_sim_dashboard_snapshot_count"] == 0
    assert result["deleted_stale_sim_equity_curve_point_count"] == 0
    assert result["deleted_stale_strategy_metric_snapshot_count"] == 0
    assert result["deleted_stale_baseline_metric_snapshot_count"] == 0
    assert result["deleted_stale_baseline_equity_curve_point_count"] == 0
    assert result["reset_sim_account_count"] == 0

    account = db_session.execute(
        select(sim_account_t).where(sim_account_t.c.capital_tier == "100k")
    ).mappings().one()
    assert float(account["initial_cash"]) == 1000000.0
    assert float(account["cash_available"]) == 36111.64
    assert float(account["total_asset"]) == 1092805.64
    assert float(account["peak_total_asset"]) == 1092805.64
    assert float(account["max_drawdown_pct"]) == 0.0
    assert account["active_position_count"] == 5
    assert account["last_reconciled_trade_date"].isoformat() == "2026-03-12"
    assert db_session.scalar(select(func.count()).select_from(sim_dashboard_t)) == 1
    assert db_session.scalar(select(func.count()).select_from(sim_equity_t)) == 1
    assert db_session.scalar(select(func.count()).select_from(strategy_metric_t)) == 1
    assert db_session.scalar(select(func.count()).select_from(baseline_metric_t)) == 1
    assert db_session.scalar(select(func.count()).select_from(baseline_equity_t)) == 1

    cleanup_items = db_session.execute(
        select(Base.metadata.tables["cleanup_task_item"])
    ).mappings().all()
    blocked_items = [
        item for item in cleanup_items
        if item["target_domain"] == "protected_domain_check"
    ]
    assert blocked_items == []


def test_fr09b_cleanup_is_idempotent_per_cleanup_date(db_session):
    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    cleanup_item_t = Base.metadata.tables["cleanup_task_item"]

    first = run_cleanup(db_session, cleanup_date="2026-03-13")
    second = run_cleanup(db_session, cleanup_date="2026-03-13")

    assert first["cleanup_date"] == "2026-03-13"
    assert second["cleanup_date"] == "2026-03-13"
    assert db_session.scalar(select(func.count()).select_from(cleanup_task_t)) == 1
    cleanup_task = db_session.execute(select(cleanup_task_t)).mappings().one()
    assert cleanup_task["cleanup_date"].isoformat() == "2026-03-13"
    cleanup_id = cleanup_task["cleanup_id"]
    items = db_session.execute(
        select(cleanup_item_t).where(cleanup_item_t.c.cleanup_id == cleanup_id)
    ).mappings().all()
    step_nos = [item["step_no"] for item in items]
    assert step_nos == sorted(step_nos)


def test_fr09b_cleanup_persists_running_and_completed_truth(db_session, monkeypatch):
    from app.services import cleanup_service

    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    cleanup_date = "2026-03-14"
    original_batched_delete = cleanup_service._batched_delete
    saw_running = {"value": False}

    def tracking_batched_delete(db, table, where_clause):
        row = db.execute(
            select(cleanup_task_t).where(cleanup_task_t.c.cleanup_date == date.fromisoformat(cleanup_date))
        ).mappings().one()
        if row["status"] == "RUNNING":
            saw_running["value"] = True
        return original_batched_delete(db, table, where_clause)

    monkeypatch.setattr(cleanup_service, "_batched_delete", tracking_batched_delete)

    result = cleanup_service.run_cleanup(db_session, cleanup_date=cleanup_date)

    assert result["cleanup_id"]
    assert saw_running["value"] is True

    cleanup_task = db_session.execute(
        select(cleanup_task_t).where(cleanup_task_t.c.cleanup_id == result["cleanup_id"])
    ).mappings().one()
    assert cleanup_task["status"] == "COMPLETED"
    assert cleanup_task["started_at"] is not None
    assert cleanup_task["finished_at"] is not None


def test_fr09b_cleanup_mutex_conflict_keeps_running_task_fact(db_session):
    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    now = _utc_now()
    cleanup_date = date(2026, 3, 15)
    cleanup_id = str(uuid4())
    db_session.execute(
        cleanup_task_t.insert().values(
            cleanup_id=cleanup_id,
            cleanup_date=cleanup_date,
            status="RUNNING",
            request_id=None,
            lock_key="cleanup:2026-03-15",
            deleted_session_count=0,
            deleted_temp_token_count=0,
            deleted_access_token_lease_count=0,
            deleted_report_generation_task_count=0,
            expired_stale_task_count=0,
            deleted_unverified_user_count=0,
            deleted_notification_count=0,
            duration_ms=None,
            status_reason=None,
            started_at=now,
            finished_at=None,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.commit()

    result = run_cleanup(db_session, cleanup_date="2026-03-15")

    assert result["skipped"] is True
    assert result["reason"] == "mutex_busy"
    assert result["cleanup_id"] == cleanup_id

    row = db_session.execute(
        select(cleanup_task_t).where(cleanup_task_t.c.cleanup_id == cleanup_id)
    ).mappings().one()
    assert row["status"] == "RUNNING"
    assert row["lock_key"] == "cleanup:2026-03-15"
    assert row["status_reason"] is None
    assert row["updated_at"] == row["started_at"]


def test_fr09b_cleanup_marks_failed_task_when_cleanup_raises(db_session, monkeypatch):
    from app.services import cleanup_service

    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    monkeypatch.setattr(
        cleanup_service,
        "_batched_delete",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(RuntimeError, match="boom"):
        cleanup_service.run_cleanup(db_session, cleanup_date="2026-03-16")

    cleanup_task = db_session.execute(
        select(cleanup_task_t).where(cleanup_task_t.c.cleanup_date == date.fromisoformat("2026-03-16"))
    ).mappings().one()
    assert cleanup_task["status"] == "FAILED"
    assert cleanup_task["status_reason"] == "boom"
    assert cleanup_task["started_at"] is not None
    assert cleanup_task["finished_at"] is not None


def test_fr09b_cleanup_default_does_not_purge_verified_runtime_test_users(db_session):
    runtime_user = User(
        user_id=str(uuid4()),
        email=f"default-runtime-{uuid4().hex[:8]}@test.com",
        password_hash="hashed",
        email_verified=True,
        role="user",
        tier="Free",
        created_at=_utc_now(),
        updated_at=_utc_now(),
    )
    db_session.add(runtime_user)
    db_session.commit()

    result = run_cleanup(db_session, cleanup_date="2026-03-20")

    assert result["deleted_runtime_test_user_count"] == 0
    db_session.expire_all()
    assert db_session.get(User, runtime_user.user_id) is not None


def test_fr09b_cleanup_controlled_runtime_test_user_purge_keeps_admin_and_audit(db_session):
    user_session_t = Base.metadata.tables["user_session"]
    refresh_token_t = Base.metadata.tables["refresh_token"]
    auth_temp_token_t = Base.metadata.tables["auth_temp_token"]
    access_token_lease_t = Base.metadata.tables["access_token_lease"]
    jti_blacklist_t = Base.metadata.tables["jti_blacklist"]
    report_feedback_t = Base.metadata.tables["report_feedback"]

    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2025-01-01",
        strategy_type="A",
        recommendation="BUY",
        confidence=0.7,
    )

    now = _utc_now()

    cleanup_target = User(
        user_id=str(uuid4()),
        email=f"cleanup-target-{uuid4().hex[:8]}@test.com",
        password_hash="hashed",
        email_verified=True,
        role="user",
        tier="Pro",
        created_at=now,
        updated_at=now,
    )
    preserved_audit = User(
        user_id=str(uuid4()),
        email=f"audit_keep_{uuid4().hex[:8]}@example.com",
        password_hash="hashed",
        email_verified=True,
        role="user",
        tier="Free",
        created_at=now,
        updated_at=now,
    )
    preserved_admin = User(
        user_id=str(uuid4()),
        email=f"cleanup-admin-{uuid4().hex[:8]}@example.com",
        password_hash="hashed",
        email_verified=True,
        role="admin",
        tier="Free",
        created_at=now,
        updated_at=now,
    )
    preserved_v79 = User(
        user_id=str(uuid4()),
        email=f"v79_keep_{uuid4().hex[:8]}@test.com",
        password_hash="hashed",
        email_verified=True,
        role="user",
        tier="Pro",
        created_at=now,
        updated_at=now,
    )
    db_session.add_all([cleanup_target, preserved_audit, preserved_admin, preserved_v79])
    db_session.flush()

    cleanup_session_id = str(uuid4())
    db_session.execute(
        user_session_t.insert().values(
            session_id=cleanup_session_id,
            user_id=cleanup_target.user_id,
            status="ACTIVE",
            client_fingerprint="cleanup-target",
            ip_address="127.0.0.1",
            user_agent="pytest",
            created_at=now,
            expires_at=now + timedelta(days=30),
            revoked_at=None,
            updated_at=now,
        )
    )
    db_session.execute(
        user_session_t.insert().values(
            session_id=str(uuid4()),
            user_id=preserved_audit.user_id,
            status="ACTIVE",
            client_fingerprint="audit-keep",
            ip_address="127.0.0.1",
            user_agent="pytest",
            created_at=now,
            expires_at=now + timedelta(days=30),
            revoked_at=None,
            updated_at=now,
        )
    )
    db_session.execute(
        user_session_t.insert().values(
            session_id=str(uuid4()),
            user_id=preserved_admin.user_id,
            status="ACTIVE",
            client_fingerprint="admin-keep",
            ip_address="127.0.0.1",
            user_agent="pytest",
            created_at=now,
            expires_at=now + timedelta(days=30),
            revoked_at=None,
            updated_at=now,
        )
    )
    refresh_token_id = str(uuid4())
    db_session.execute(
        refresh_token_t.insert().values(
            refresh_token_id=refresh_token_id,
            user_id=cleanup_target.user_id,
            session_id=cleanup_session_id,
            token_hash=f"hash-{uuid4().hex}",
            rotated_from_token_id=None,
            issued_at=now,
            used_at=None,
            expires_at=now + timedelta(days=30),
            grace_expires_at=now + timedelta(days=31),
            revoked_at=None,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.execute(
        auth_temp_token_t.insert().values(
            temp_token_id=str(uuid4()),
            user_id=cleanup_target.user_id,
            token_type="PASSWORD_RESET",
            token_hash=f"temp-{uuid4().hex}",
            sent_at=now,
            used_at=None,
            expires_at=now + timedelta(days=3),
            created_at=now,
        )
    )
    db_session.execute(
        access_token_lease_t.insert().values(
            jti=f"jti-{uuid4().hex}",
            user_id=cleanup_target.user_id,
            session_id=cleanup_session_id,
            refresh_token_id=refresh_token_id,
            issued_at=now,
            expires_at=now + timedelta(days=3),
            revoked_at=None,
            revoke_source=None,
            created_at=now,
        )
    )
    db_session.execute(
        jti_blacklist_t.insert().values(
            jti=f"blk-{uuid4().hex}",
            user_id=cleanup_target.user_id,
            session_id=cleanup_session_id,
            source_action="logout",
            status_reason="cleanup-target",
            created_at=now,
            expires_at=now + timedelta(days=3),
        )
    )
    db_session.execute(
        report_feedback_t.insert().values(
            feedback_id=str(uuid4()),
            report_id=report.report_id,
            user_id=cleanup_target.user_id,
            feedback_type="negative",
            is_helpful=0,
            created_at=now,
        )
    )
    db_session.commit()
    cleanup_target_user_id = cleanup_target.user_id

    result = run_cleanup(
        db_session,
        cleanup_date="2026-03-20",
        purge_test_account_pollution=True,
    )

    assert result["deleted_runtime_test_user_count"] == 1
    assert result["deleted_runtime_test_session_count"] == 1
    assert result["deleted_runtime_test_refresh_token_count"] == 1
    assert result["deleted_runtime_test_temp_token_count"] == 1
    assert result["deleted_runtime_test_access_token_lease_count"] == 1
    assert result["deleted_runtime_test_jti_blacklist_count"] == 1
    assert result["deleted_runtime_test_report_feedback_count"] == 1

    db_session.expire_all()
    assert db_session.get(User, cleanup_target_user_id) is None
    assert db_session.get(User, preserved_audit.user_id) is not None
    assert db_session.get(User, preserved_admin.user_id) is not None
    assert db_session.get(User, preserved_v79.user_id) is not None
    assert db_session.scalar(
        select(func.count()).select_from(user_session_t).where(user_session_t.c.user_id == cleanup_target_user_id)
    ) == 0
    assert db_session.scalar(
        select(func.count()).select_from(refresh_token_t).where(refresh_token_t.c.user_id == cleanup_target_user_id)
    ) == 0
    assert db_session.scalar(
        select(func.count()).select_from(auth_temp_token_t).where(auth_temp_token_t.c.user_id == cleanup_target_user_id)
    ) == 0
    assert db_session.scalar(
        select(func.count()).select_from(access_token_lease_t).where(access_token_lease_t.c.user_id == cleanup_target_user_id)
    ) == 0
    assert db_session.scalar(
        select(func.count()).select_from(jti_blacklist_t).where(jti_blacklist_t.c.user_id == cleanup_target_user_id)
    ) == 0
    assert db_session.scalar(
        select(func.count()).select_from(report_feedback_t).where(report_feedback_t.c.user_id == cleanup_target_user_id)
    ) == 0


def test_fr09b_cleanup_persists_running_task_before_work(db_session, monkeypatch):
    from app.services import cleanup_service

    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    observed = {}

    def _assert_running(*args, **kwargs):
        row = db_session.execute(
            select(cleanup_task_t).where(cleanup_task_t.c.cleanup_date == date.fromisoformat("2026-03-21"))
        ).mappings().one()
        observed["status"] = row["status"]
        observed["lock_key"] = row["lock_key"]
        assert row["status"] == "RUNNING"
        assert row["started_at"] is not None
        assert row["finished_at"] is None
        return 0

    monkeypatch.setattr(cleanup_service, "_batched_delete", _assert_running)

    result = cleanup_service.run_cleanup(db_session, cleanup_date="2026-03-21")

    assert observed["status"] == "RUNNING"
    assert observed["lock_key"].startswith(cleanup_service._cleanup_lock_key("2026-03-21") + ":")
    row = db_session.execute(
        select(cleanup_task_t).where(cleanup_task_t.c.cleanup_id == result["cleanup_id"])
    ).mappings().one()
    assert row["status"] == "COMPLETED"


def test_fr09b_cleanup_persists_failed_task_fact_on_exception(db_session, monkeypatch):
    from app.services import cleanup_service

    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    monkeypatch.setattr(cleanup_service, "_batched_delete", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("cleanup_boom")))

    with pytest.raises(RuntimeError, match="cleanup_boom"):
        cleanup_service.run_cleanup(db_session, cleanup_date="2026-03-22")

    row = db_session.execute(
        select(cleanup_task_t).where(cleanup_task_t.c.cleanup_date == date.fromisoformat("2026-03-22"))
    ).mappings().one()
    assert row["status"] == "FAILED"
    assert "cleanup_boom" in (row["status_reason"] or "")
    assert row["lock_key"].startswith(cleanup_service._cleanup_lock_key("2026-03-22") + ":")
    assert row["started_at"] is not None
    assert row["finished_at"] is not None


def test_fr09b_cleanup_mutex_busy_returns_existing_running_task_fact(db_session):
    from app.services import cleanup_service

    cleanup_day = date.fromisoformat("2026-03-23")
    cleanup_id, claimed = cleanup_service._start_cleanup_task(
        db_session,
        cleanup_day=cleanup_day,
        started_at=_utc_now(),
        lock_key=cleanup_service._cleanup_lock_key("2026-03-23"),
    )
    assert claimed is True
    cleanup_service._cleanup_lock.acquire()
    try:
        result = cleanup_service.run_cleanup(db_session, cleanup_date="2026-03-23")
    finally:
        cleanup_service._cleanup_lock.release()

    assert result["skipped"] is True
    assert result["reason"] == "mutex_busy"
    assert result["cleanup_id"] == cleanup_id
    row = db_session.execute(
        select(Base.metadata.tables["cleanup_task"]).where(Base.metadata.tables["cleanup_task"].c.cleanup_id == cleanup_id)
    ).mappings().one()
    assert row["status"] == "RUNNING"


def test_fr09b_cleanup_releases_local_lock_when_db_claim_fails(db_session, monkeypatch):
    from app.services import cleanup_service

    monkeypatch.setattr(cleanup_service, "_running_cleanup_row", lambda *args, **kwargs: None)
    monkeypatch.setattr(cleanup_service, "_start_cleanup_task", lambda *args, **kwargs: ("race-cleanup-id", False))

    result = cleanup_service.run_cleanup(db_session, cleanup_date="2026-03-24")

    assert result == {
        "cleanup_date": "2026-03-24",
        "cleanup_id": "race-cleanup-id",
        "skipped": True,
        "reason": "mutex_busy",
    }
    assert cleanup_service._cleanup_lock.acquire(blocking=False) is True
    cleanup_service._cleanup_lock.release()


def test_fr09b_cleanup_reclaims_stale_running_task_with_new_lock_token(db_session):
    from app.services import cleanup_service

    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    stale_cleanup_date = date.fromisoformat("2026-03-25")
    stale_started_at = _utc_now() - timedelta(minutes=cleanup_service._STALE_LOCK_TTL_MINUTES + 2)
    stale_lock_key = "cleanup:2026-03-25@stale-holder"
    cleanup_id = str(uuid4())
    db_session.execute(
        cleanup_task_t.insert().values(
            cleanup_id=cleanup_id,
            cleanup_date=stale_cleanup_date,
            status="RUNNING",
            request_id=None,
            lock_key=stale_lock_key,
            deleted_session_count=0,
            deleted_temp_token_count=0,
            deleted_access_token_lease_count=0,
            deleted_report_generation_task_count=0,
            expired_stale_task_count=0,
            deleted_unverified_user_count=0,
            deleted_notification_count=0,
            duration_ms=None,
            status_reason=None,
            started_at=stale_started_at,
            finished_at=None,
            updated_at=stale_started_at,
            created_at=stale_started_at,
        )
    )
    db_session.commit()

    result = cleanup_service.run_cleanup(db_session, cleanup_date="2026-03-25")

    assert result["cleanup_id"] == cleanup_id
    assert result.get("skipped") is not True
    row = db_session.execute(
        select(cleanup_task_t).where(cleanup_task_t.c.cleanup_id == cleanup_id)
    ).mappings().one()
    assert row["status"] == "COMPLETED"
    assert row["lock_key"] != stale_lock_key
    assert row["lock_key"].startswith(cleanup_service._cleanup_lock_key("2026-03-25") + ":")
    assert row["started_at"] != stale_started_at


def test_fr09b_cleanup_does_not_overwrite_foreign_holder_after_takeover(db_session, monkeypatch):
    from app.services import cleanup_service

    cleanup_task_t = Base.metadata.tables["cleanup_task"]
    foreign_lock_key = "cleanup:2026-03-26@foreign-holder"
    takeover_done = {"done": False}
    original_batched_delete = cleanup_service._batched_delete

    def _take_over(*args, **kwargs):
        if not takeover_done["done"]:
            takeover_session = sessionmaker(bind=db_session.bind, autocommit=False, autoflush=False)()
            try:
                takeover_session.execute(
                    cleanup_task_t.update()
                    .where(cleanup_task_t.c.cleanup_date == date.fromisoformat("2026-03-26"))
                    .values(
                        status="RUNNING",
                        lock_key=foreign_lock_key,
                        status_reason="taken_over_by_foreign_holder",
                        updated_at=_utc_now(),
                    )
                )
                takeover_session.commit()
            finally:
                takeover_session.close()
            takeover_done["done"] = True
        return original_batched_delete(*args, **kwargs)

    monkeypatch.setattr(cleanup_service, "_batched_delete", _take_over)

    result = cleanup_service.run_cleanup(db_session, cleanup_date="2026-03-26")

    assert result["cleanup_date"] == "2026-03-26"
    assert result["skipped"] is True
    assert result["reason"] == "lease_lost"
    row = db_session.execute(
        select(cleanup_task_t).where(cleanup_task_t.c.cleanup_date == date.fromisoformat("2026-03-26"))
    ).mappings().one()
    assert row["status"] == "RUNNING"
    assert row["lock_key"] == foreign_lock_key
    assert row["status_reason"] == "taken_over_by_foreign_holder"


def test_fr09b_daily_cleanup_job_invokes_cleanup(monkeypatch):
    from app.services import scheduler as scheduler_service

    seen = {}

    class DummyDb:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    db = DummyDb()

    monkeypatch.setattr(scheduler_service, "SessionLocal", lambda: db)
    monkeypatch.setattr(
        scheduler_service,
        "run_cleanup",
        lambda session: seen.setdefault("called", session) or {"cleanup_date": "2026-03-26", "deleted_report_generation_task_count": 3, "expired_stale_task_count": 1},
    )

    scheduler_service._daily_cleanup_job()

    assert seen["called"] is db
    assert db.closed is True
