"""FR02-SCHED-04: DAG 分布式锁（try_acquire_lock / heartbeat_lock / fencing）验收测试
SSOT: 01 §FR02-SCHED-04, 03 §FR-02
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from app.models import Base
from app.services.dag_scheduler import heartbeat_lock, try_acquire_lock


@pytest.mark.feature("FR02-SCHED-04")
class TestDagSchedulerLock:
    def test_new_task_acquires_lock_version_1(self, db_session):
        """新任务首次 acquire → 返回 (task_run_id, 1)。"""
        task_run_id, version = try_acquire_lock(
            db_session,
            task_name="fr01_stock_pool",
            trade_date=date(2026, 3, 10),
        )
        db_session.commit()

        assert task_run_id is not None
        assert version == 1

    def test_held_lock_returns_none_zero(self, db_session):
        """锁被持有（TTL 未过期）→ 返回 (None, 0)。"""
        task_run_id_1, _ = try_acquire_lock(
            db_session,
            task_name="fr01_stock_pool",
            trade_date=date(2026, 3, 11),
        )
        db_session.commit()
        assert task_run_id_1 is not None

        task_run_id_2, version_2 = try_acquire_lock(
            db_session,
            task_name="fr01_stock_pool",
            trade_date=date(2026, 3, 11),
        )
        assert task_run_id_2 is None
        assert version_2 == 0

    def test_expired_lock_takeover_increments_fencing_token(self, db_session):
        """TTL 过期后 → 接管，lock_version 递增（fencing token 防止双写）。"""
        run_table = Base.metadata.tables["scheduler_task_run"]
        stale_time = datetime.now(timezone.utc) - timedelta(seconds=400)
        old_task_run_id = str(uuid4())

        db_session.execute(run_table.insert().values(
            task_run_id=old_task_run_id,
            task_name="fr01_stock_pool",
            trade_date=date(2026, 3, 12),
            schedule_slot="dag_event",
            status="RUNNING",
            retry_count=0,
            lock_key="dag:2026-03-12:fr01_stock_pool",
            lock_version=1,
            trigger_source="event",
            triggered_at=stale_time,
            started_at=stale_time,
            updated_at=stale_time,
            created_at=stale_time,
        ))
        db_session.commit()

        task_run_id, version = try_acquire_lock(
            db_session,
            task_name="fr01_stock_pool",
            trade_date=date(2026, 3, 12),
            lock_ttl_seconds=300,
        )
        db_session.commit()

        assert task_run_id == old_task_run_id
        assert version == 2  # fencing token incremented

    def test_heartbeat_lock_renews_updated_at(self, db_session):
        """heartbeat_lock 更新 updated_at → 返回 True。"""
        task_run_id, version = try_acquire_lock(
            db_session,
            task_name="fr05_market_state",
            trade_date=date(2026, 3, 10),
        )
        db_session.commit()
        assert task_run_id is not None

        renewed = heartbeat_lock(db_session, task_run_id, version)
        db_session.commit()

        assert renewed is True
        run_table = Base.metadata.tables["scheduler_task_run"]
        row = db_session.execute(
            run_table.select().where(run_table.c.task_run_id == task_run_id)
        ).fetchone()
        assert row is not None
        assert row.updated_at is not None

    def test_heartbeat_wrong_version_fenced_returns_false(self, db_session):
        """错误 lock_version → fencing 生效，heartbeat 返回 False。"""
        task_run_id, version = try_acquire_lock(
            db_session,
            task_name="fr05_market_state",
            trade_date=date(2026, 3, 13),
        )
        db_session.commit()
        assert task_run_id is not None

        result = heartbeat_lock(db_session, task_run_id, version + 99)
        db_session.commit()

        assert result is False

    def test_completed_task_not_re_acquired(self, db_session):
        """已完成（TERMINAL）的任务不会被重新 acquire。"""
        from app.services.dag_scheduler import mark_success
        task_run_id, version = try_acquire_lock(
            db_session,
            task_name="fr01_stock_pool",
            trade_date=date(2026, 3, 14),
        )
        db_session.commit()
        assert task_run_id is not None

        mark_success(db_session, task_run_id)
        db_session.commit()

        task_run_id_2, version_2 = try_acquire_lock(
            db_session,
            task_name="fr01_stock_pool",
            trade_date=date(2026, 3, 14),
        )
        assert task_run_id_2 is None
        assert version_2 == 0
