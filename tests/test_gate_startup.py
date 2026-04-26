"""硬门禁 1: 启动/重启门禁 — 验证 ENABLE_SCHEDULER=true 时正式执行链的可信度。

验证目标（来自审计方法论 4.6 §1）：
- 系统首次启动：scheduler 注册所有 SSOT 任务
- register_scheduler_run 必须写入 scheduler_task_run 表
- _record_task_run 在每个 job 中被调用
- 每个 job 的 task_run_id 应入库且状态最终为 SUCCESS/FAILED
- P0-06: 调度器改为 DAG 事件驱动，仅注册 dag_daily_chain + 辅助 job
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.models import Base


@pytest.mark.feature("FR02-SCHED-01")
def test_gate_startup_scheduler_registers_all_jobs(isolated_app):
    """硬门禁: start_scheduler() 必须注册 SSOT 要求的全部 job。"""
    from app.services.scheduler import scheduler, start_scheduler, stop_scheduler

    expected_jobs = {
        "dag_daily_chain",
        "sim_open_price",
        "billing_poller",
        "tier_expiry_sweep",
        "daily_cleanup",
        "cookie_probe",
    }

    try:
        start_scheduler()
        registered = {j.id for j in scheduler.get_jobs()}
        assert registered == expected_jobs, f"start_scheduler() 注册 job 异常: {registered}"
    finally:
        stop_scheduler()


def test_gate_startup_record_task_run_writes_db(db_session):
    """硬门禁: _record_task_run 必须真实写入 scheduler_task_run 表。"""
    from app.services.scheduler_ops_ssot import register_scheduler_run

    result = register_scheduler_run(
        db_session,
        task_name="gate_test_startup",
        trade_date="2026-01-01",
        schedule_slot="gate_test",
        trigger_source="cron",
    )
    db_session.commit()

    assert result["action"] == "created"
    assert result["task_run_id"] is not None

    # 回查数据库
    table = Base.metadata.tables["scheduler_task_run"]
    row = db_session.execute(
        table.select().where(table.c.task_run_id == result["task_run_id"])
    ).first()
    assert row is not None
    assert row.task_name == "gate_test_startup"
    assert row.status in {"RUNNING", "WAITING_UPSTREAM"}


def test_gate_startup_idempotent_rerun_skips(db_session):
    """硬门禁: 同 (task_name, trade_date, schedule_slot) 的 SUCCESS 任务不应重复执行。"""
    from app.services.scheduler_ops_ssot import mark_scheduler_run_success, register_scheduler_run

    first = register_scheduler_run(
        db_session,
        task_name="gate_idem",
        trade_date="2026-01-02",
        schedule_slot="daily",
        trigger_source="cron",
    )
    db_session.commit()
    mark_scheduler_run_success(db_session, task_run_id=first["task_run_id"])
    db_session.commit()

    second = register_scheduler_run(
        db_session,
        task_name="gate_idem",
        trade_date="2026-01-02",
        schedule_slot="daily",
        trigger_source="cron",
    )
    assert second["action"] == "skipped_existing_success"


def test_gate_startup_daily_pipeline_lock_prevents_concurrent(isolated_app):
    """硬门禁: daily_pipeline 的 _job_lock 必须防止并发。"""
    from app.services import scheduler as sched_mod

    # 模拟 _job_running=True
    original = sched_mod._job_running
    sched_mod._job_running = True
    try:
        with patch.object(sched_mod, "_record_task_run") as mock_record:
            sched_mod._daily_job_entry()
            # 不应进入任务执行
            mock_record.assert_not_called()
    finally:
        sched_mod._job_running = original


def test_gate_startup_cookie_probe_job_records_and_marks_success(monkeypatch):
    from app.services import cookie_session_ssot as cookie_mod
    from app.services import scheduler as sched_mod

    task_runs = []
    marks = []

    class DummyDb:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    db = DummyDb()

    monkeypatch.setattr(
        sched_mod,
        "_record_task_run",
        lambda task_name, schedule_slot, trade_date=None: task_runs.append((task_name, schedule_slot)) or "cookie-run-1",
    )
    monkeypatch.setattr(sched_mod, "_mark_task_success", lambda task_run_id: marks.append(("success", task_run_id)))
    monkeypatch.setattr(sched_mod, "_mark_task_failed", lambda task_run_id, error: marks.append(("failed", task_run_id, error)))
    monkeypatch.setattr(sched_mod, "SessionLocal", lambda: db)
    monkeypatch.setattr(cookie_mod, "run_all_cookie_probes", lambda session: [{"provider": "xueqiu", "status": "ok"}])

    sched_mod._cookie_probe_job()

    assert task_runs == [("cookie_probe", "interval_5m")]
    assert marks == [("success", "cookie-run-1")]
    assert db.closed is True


def test_gate_startup_cookie_probe_job_marks_failure_without_nameerror(monkeypatch):
    from app.services import cookie_session_ssot as cookie_mod
    from app.services import scheduler as sched_mod

    marks = []

    class DummyDb:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    db = DummyDb()

    monkeypatch.setattr(sched_mod, "_record_task_run", lambda task_name, schedule_slot, trade_date=None: "cookie-run-2")
    monkeypatch.setattr(sched_mod, "_mark_task_success", lambda task_run_id: marks.append(("success", task_run_id)))
    monkeypatch.setattr(sched_mod, "_mark_task_failed", lambda task_run_id, error: marks.append(("failed", task_run_id, error)))
    monkeypatch.setattr(sched_mod, "SessionLocal", lambda: db)

    def _raise_locked(session):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(cookie_mod, "run_all_cookie_probes", _raise_locked)

    sched_mod._cookie_probe_job()

    assert marks == [("failed", "cookie-run-2", "database is locked")]
    assert db.closed is True


def test_gate_startup_catchup_runs_regardless_of_time(monkeypatch):
    """启动补跑不再受 15:20 时间门禁限制，任何时间都可以触发。"""
    from app.services import scheduler as sched_mod

    before_trigger = datetime(2026, 3, 16, 6, 59, tzinfo=timezone.utc)  # 14:59 Asia/Shanghai
    monkeypatch.setattr(sched_mod, "is_trade_day", lambda dt=None: True)
    monkeypatch.setattr(sched_mod, "latest_trade_date_str", lambda dt=None: "2026-03-16")

    with patch("threading.Thread") as mock_thread:
        sched_mod._startup_catchup(now=before_trigger)
        mock_thread.assert_called_once()


def test_gate_startup_catchup_runs_after_daily_trigger(monkeypatch):
    """交易日 15:20 后启动，若当日日批未执行则允许进入补跑。"""
    from app.services import scheduler as sched_mod

    after_trigger = datetime(2026, 3, 16, 7, 21, tzinfo=timezone.utc)  # 15:21 Asia/Shanghai
    monkeypatch.setattr(sched_mod, "is_trade_day", lambda dt=None: True)
    monkeypatch.setattr(sched_mod, "latest_trade_date_str", lambda dt=None: "2026-03-16")

    with patch("threading.Thread") as mock_thread:
        sched_mod._startup_catchup(now=after_trigger)
        mock_thread.assert_called_once()


def test_gate_startup_lifespan_hits_real_startup_chain(isolated_app, monkeypatch):
    from apscheduler.schedulers.background import BackgroundScheduler

    import app.services.dag_scheduler as dag_mod
    from app.core.config import settings
    from app.services import scheduler as sched_mod

    expected_jobs = {
        "dag_daily_chain",
        "sim_open_price",
        "billing_poller",
        "tier_expiry_sweep",
        "daily_cleanup",
        "cookie_probe",
    }
    hit_markers: list[str] = []
    fresh_scheduler = BackgroundScheduler()

    monkeypatch.setattr(settings, "enable_scheduler", True)
    monkeypatch.setattr(sched_mod, "scheduler", fresh_scheduler)
    monkeypatch.setattr(sched_mod, "_startup_catchup", lambda now=None: hit_markers.append("startup_catchup"))
    monkeypatch.setattr(dag_mod, "start_timeout_watcher", lambda: hit_markers.append("timeout_watcher_started"))
    monkeypatch.setattr(dag_mod, "stop_timeout_watcher", lambda: hit_markers.append("timeout_watcher_stopped"))

    with TestClient(isolated_app["app"], base_url="http://localhost") as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert fresh_scheduler.running is True
        assert {job.id for job in fresh_scheduler.get_jobs()} == expected_jobs
        assert "startup_catchup" in hit_markers
        assert "timeout_watcher_started" in hit_markers

    assert fresh_scheduler.running is False
    assert "timeout_watcher_stopped" in hit_markers
