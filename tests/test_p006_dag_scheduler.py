"""
Tests for P0-06 DAG scheduler refactor.

Validates:
- DAG event emission and consumption
- Upstream dependency checking
- Lock acquisition with TTL and fencing
- Cascade timeout enforcement
- Idempotent restart (SUCCESS skip)
- Full DAG chain execution order
- Handler registration and execution
- WAITING_UPSTREAM → RUNNING transition
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from app.models import Base
from app.services.dag_scheduler import (
    DAG_DEPENDENCIES,
    EVENT_NAMES,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_SKIPPED,
    STATUS_SUCCESS,
    STATUS_WAITING,
    UPSTREAM_EVENTS,
    check_upstream_ready,
    emit_dag_event,
    enforce_cascade_timeout,
    execute_dag_node,
    heartbeat_lock,
    mark_failed,
    mark_skipped,
    mark_success,
    register_handler,
    try_acquire_lock,
)


TRADE_DATE = date(2026, 3, 10)


class TestDagEventEmission:
    def test_emit_event_creates_row(self, db_session):
        event_id = emit_dag_event(
            db_session,
            event_name="FR04_CORE_POOL_COLLECTION_COMPLETED",
            trade_date=TRADE_DATE,
            producer_task_run_id=None,
            payload={"stocks": 10},
        )
        db_session.commit()

        event_table = Base.metadata.tables["dag_event"]
        row = db_session.execute(
            event_table.select().where(event_table.c.dag_event_id == event_id)
        ).first()
        assert row is not None
        assert row.event_name == "FR04_CORE_POOL_COLLECTION_COMPLETED"
        assert row.trade_date == TRADE_DATE

    def test_emit_event_idempotent(self, db_session):
        id1 = emit_dag_event(
            db_session,
            event_name="FR04_CORE_POOL_COLLECTION_COMPLETED",
            trade_date=TRADE_DATE,
        )
        db_session.commit()
        id2 = emit_dag_event(
            db_session,
            event_name="FR04_CORE_POOL_COLLECTION_COMPLETED",
            trade_date=TRADE_DATE,
        )
        db_session.commit()
        assert id1 == id2


class TestUpstreamCheck:
    def test_no_deps_always_ready(self, db_session):
        assert check_upstream_ready(db_session, "fr01_stock_pool", TRADE_DATE) is True
        assert check_upstream_ready(db_session, "fr05_market_state", TRADE_DATE) is True

    def test_dep_not_ready(self, db_session):
        assert check_upstream_ready(db_session, "fr06_report_gen", TRADE_DATE) is False

    def test_dep_ready_after_event(self, db_session):
        emit_dag_event(
            db_session,
            event_name="FR05_NON_REPORT_TRUTH_MATERIALIZED",
            trade_date=TRADE_DATE,
        )
        db_session.commit()
        assert check_upstream_ready(db_session, "fr06_report_gen", TRADE_DATE) is True


class TestLockAcquisition:
    def test_acquire_new_lock(self, db_session):
        run_id, version = try_acquire_lock(
            db_session, task_name="fr01_stock_pool", trade_date=TRADE_DATE
        )
        db_session.commit()
        assert run_id is not None
        assert version == 1

        run_table = Base.metadata.tables["scheduler_task_run"]
        row = db_session.execute(
            run_table.select().where(run_table.c.task_run_id == run_id)
        ).first()
        assert row.status == STATUS_RUNNING
        assert row.lock_version == 1

    def test_lock_skip_on_success(self, db_session):
        run_id, _ = try_acquire_lock(
            db_session, task_name="fr01_stock_pool", trade_date=TRADE_DATE
        )
        mark_success(db_session, run_id)
        db_session.commit()

        run_id2, _ = try_acquire_lock(
            db_session, task_name="fr01_stock_pool", trade_date=TRADE_DATE
        )
        assert run_id2 is None  # Skipped — already SUCCESS

    def test_lock_held_by_another(self, db_session):
        run_id, _ = try_acquire_lock(
            db_session, task_name="fr01_stock_pool", trade_date=TRADE_DATE
        )
        db_session.commit()

        # Another instance tries to acquire
        run_id2, _ = try_acquire_lock(
            db_session, task_name="fr01_stock_pool", trade_date=TRADE_DATE,
            lock_ttl_seconds=9999,
        )
        assert run_id2 is None  # Lock held

    def test_waiting_upstream_on_missing_deps(self, db_session):
        run_id, version = try_acquire_lock(
            db_session, task_name="fr06_report_gen", trade_date=TRADE_DATE
        )
        db_session.commit()
        assert run_id is not None

        run_table = Base.metadata.tables["scheduler_task_run"]
        row = db_session.execute(
            run_table.select().where(run_table.c.task_run_id == run_id)
        ).first()
        assert row.status == STATUS_WAITING


class TestHeartbeat:
    def test_heartbeat_renews(self, db_session):
        run_id, version = try_acquire_lock(
            db_session, task_name="fr01_stock_pool", trade_date=TRADE_DATE
        )
        db_session.commit()

        result = heartbeat_lock(db_session, run_id, version)
        db_session.commit()
        assert result is True

    def test_heartbeat_fenced(self, db_session):
        run_id, version = try_acquire_lock(
            db_session, task_name="fr01_stock_pool", trade_date=TRADE_DATE
        )
        db_session.commit()

        # Wrong version → fenced out
        result = heartbeat_lock(db_session, run_id, version + 10)
        db_session.commit()
        assert result is False


class TestCascadeTimeout:
    def test_timeout_marks_stale_failed(self, db_session):
        # Create a WAITING_UPSTREAM task
        run_id, _ = try_acquire_lock(
            db_session, task_name="fr06_report_gen", trade_date=TRADE_DATE
        )
        db_session.commit()

        # Force deadline to be in the past
        with patch("app.services.dag_scheduler._parse_timeout_time") as mock_deadline:
            mock_deadline.return_value = datetime.now(timezone.utc) - timedelta(hours=1)
            affected = enforce_cascade_timeout(db_session, TRADE_DATE)
            db_session.commit()

        assert len(affected) >= 1
        run_table = Base.metadata.tables["scheduler_task_run"]
        row = db_session.execute(
            run_table.select().where(run_table.c.task_run_id == run_id)
        ).first()
        assert row.status == STATUS_FAILED
        assert row.status_reason == "upstream_timeout_next_open"


class TestDagNodeExecution:
    def test_execute_with_handler(self, db_session):
        mock_handler = MagicMock(return_value={"result": "ok"})
        register_handler("fr01_stock_pool", mock_handler)

        with patch("app.services.dag_scheduler.SessionLocal") as mock_sl:
            mock_sl.return_value = db_session
            with patch("app.services.dag_scheduler.is_trade_day", return_value=True):
                with patch("app.services.dag_scheduler._trigger_downstream", return_value=None):
                    result = execute_dag_node("fr01_stock_pool", TRADE_DATE)

        assert result["status"] == STATUS_SUCCESS
        mock_handler.assert_called_once_with(TRADE_DATE)

    def test_execute_non_trade_day_skipped(self, db_session):
        register_handler("fr01_stock_pool", MagicMock())

        with patch("app.services.dag_scheduler.SessionLocal") as mock_sl:
            mock_sl.return_value = db_session
            with patch("app.services.dag_scheduler.is_trade_day", return_value=False):
                with patch("app.services.dag_scheduler._trigger_downstream", return_value=None):
                    result = execute_dag_node("fr01_stock_pool", TRADE_DATE)

        assert result["status"] == STATUS_SKIPPED

    def test_fr07_does_not_emit_completion_event_before_settlement_pipeline_terminal(self, db_session):
        def _handler(_trade_date, *, force=False):
            raise RuntimeError("settlement_pipeline_not_completed:RUNNING")

        register_handler("fr07_settlement", _handler)
        emit_dag_event(
            db_session,
            event_name="FR06_BATCH_COMPLETED",
            trade_date=TRADE_DATE,
        )
        db_session.commit()

        with patch("app.services.dag_scheduler.SessionLocal") as mock_sl:
            mock_sl.return_value = db_session
            with patch("app.services.dag_scheduler.is_trade_day", return_value=True):
                with patch("app.services.dag_scheduler._trigger_downstream", return_value=None):
                    result = execute_dag_node("fr07_settlement", TRADE_DATE)

        assert result["status"] == STATUS_FAILED
        event_table = Base.metadata.tables["dag_event"]
        row = db_session.execute(
            event_table.select().where(
                event_table.c.event_name == "FR07_SETTLEMENT_COMPLETED",
                event_table.c.trade_date == TRADE_DATE,
            )
        ).first()
        assert row is None

    def test_real_fr07_handler_does_not_emit_completion_event_before_pipeline_terminal(self, db_session, monkeypatch):
        import app.services.scheduler as scheduler
        import app.services.settlement_ssot as settlement_ssot

        register_handler("fr07_settlement", scheduler._handler_fr07_settlement)
        emit_dag_event(
            db_session,
            event_name="FR06_BATCH_COMPLETED",
            trade_date=TRADE_DATE,
        )
        db_session.commit()

        monkeypatch.setattr(scheduler, "sim_storage_mode", lambda db: "ssot")
        monkeypatch.setattr(settlement_ssot, "submit_settlement_batch", lambda *args, **kwargs: [])
        monkeypatch.setattr(
            settlement_ssot,
            "wait_for_settlement_pipeline",
            lambda **kwargs: {"pipeline_status": "RUNNING", "status_reason": "still_running"},
        )

        with patch("app.services.dag_scheduler.SessionLocal") as mock_sl:
            mock_sl.return_value = db_session
            with patch("app.services.dag_scheduler.is_trade_day", return_value=True):
                with patch("app.services.dag_scheduler._trigger_downstream", return_value=None):
                    result = execute_dag_node("fr07_settlement", TRADE_DATE)

        assert result["status"] == STATUS_FAILED
        event_table = Base.metadata.tables["dag_event"]
        row = db_session.execute(
            event_table.select().where(
                event_table.c.event_name == "FR07_SETTLEMENT_COMPLETED",
                event_table.c.trade_date == TRADE_DATE,
            )
        ).first()
        assert row is None

    def test_fr07_postcondition_blocks_completion_event_when_handler_returns_before_pipeline_completed(
        self, db_session, monkeypatch
    ):
        def _handler(_trade_date, *, force=False):
            return {"accepted": True}

        register_handler("fr07_settlement", _handler)
        emit_dag_event(
            db_session,
            event_name="FR06_BATCH_COMPLETED",
            trade_date=TRADE_DATE,
        )
        db_session.commit()

        monkeypatch.setattr(
            "app.services.settlement_ssot.get_settlement_pipeline_status",
            lambda *args, **kwargs: {
                "pipeline_status": "RUNNING",
                "status_reason": "settlement_materialization_pending",
            },
        )

        with patch("app.services.dag_scheduler.SessionLocal") as mock_sl:
            mock_sl.return_value = db_session
            with patch("app.services.dag_scheduler.is_trade_day", return_value=True):
                with patch("app.services.dag_scheduler._trigger_downstream", return_value=None):
                    result = execute_dag_node("fr07_settlement", TRADE_DATE)

        assert result["status"] == STATUS_FAILED
        event_table = Base.metadata.tables["dag_event"]
        event_row = db_session.execute(
            event_table.select().where(
                event_table.c.event_name == "FR07_SETTLEMENT_COMPLETED",
                event_table.c.trade_date == TRADE_DATE,
            )
        ).first()
        assert event_row is None

        run_table = Base.metadata.tables["scheduler_task_run"]
        run_row = db_session.execute(
            run_table.select().where(
                run_table.c.task_name == "fr07_settlement",
                run_table.c.trade_date == TRADE_DATE,
            )
        ).first()
        assert run_row.status == STATUS_FAILED


class TestDagDependencyGraph:
    @pytest.mark.feature("FR02-SCHED-02")
    def test_truth_materialize_depends_on_fr04_and_fr05(self):
        assert set(DAG_DEPENDENCIES["fr05_non_report_truth_materialize"]) == {
            "fr04_data_collect",
            "fr05_market_state",
        }

    @pytest.mark.feature("FR02-SCHED-02")
    def test_fr06_depends_on_truth_materialize(self):
        assert "fr05_non_report_truth_materialize" in DAG_DEPENDENCIES["fr06_report_gen"]

    @pytest.mark.feature("FR02-SCHED-02")
    def test_fr07_depends_on_fr06(self):
        assert "fr06_report_gen" in DAG_DEPENDENCIES["fr07_settlement"]

    @pytest.mark.feature("FR02-SCHED-02")
    def test_fr08_depends_on_fr06(self):
        assert "fr06_report_gen" in DAG_DEPENDENCIES["fr08_sim_trade"]

    @pytest.mark.feature("FR02-SCHED-02")
    def test_root_nodes_have_no_deps(self):
        assert DAG_DEPENDENCIES["fr01_stock_pool"] == []
        assert DAG_DEPENDENCIES["fr05_market_state"] == []

    @pytest.mark.feature("FR02-SCHED-02")
    def test_upstream_events_correct(self):
        assert "FR05_NON_REPORT_TRUTH_MATERIALIZED" in UPSTREAM_EVENTS["fr06_report_gen"]
        assert "FR06_BATCH_COMPLETED" in UPSTREAM_EVENTS["fr07_settlement"]
        assert "FR06_BATCH_COMPLETED" in UPSTREAM_EVENTS["fr08_sim_trade"]


class TestStatusTransitions:
    def test_full_lifecycle(self, db_session):
        run_id, version = try_acquire_lock(
            db_session, task_name="fr05_market_state", trade_date=TRADE_DATE
        )
        db_session.commit()

        run_table = Base.metadata.tables["scheduler_task_run"]
        row = db_session.execute(
            run_table.select().where(run_table.c.task_run_id == run_id)
        ).first()
        assert row.status == STATUS_RUNNING

        mark_success(db_session, run_id)
        db_session.commit()

        row = db_session.execute(
            run_table.select().where(run_table.c.task_run_id == run_id)
        ).first()
        assert row.status == STATUS_SUCCESS
        assert row.finished_at is not None

    def test_failed_state(self, db_session):
        run_id, _ = try_acquire_lock(
            db_session, task_name="fr05_market_state", trade_date=TRADE_DATE
        )
        db_session.commit()

        mark_failed(db_session, run_id, "test error", "retries_exhausted")
        db_session.commit()

        run_table = Base.metadata.tables["scheduler_task_run"]
        row = db_session.execute(
            run_table.select().where(run_table.c.task_run_id == run_id)
        ).first()
        assert row.status == STATUS_FAILED
        assert "test error" in row.error_message
        assert row.status_reason == "retries_exhausted"

    def test_skipped_state(self, db_session):
        run_id, _ = try_acquire_lock(
            db_session, task_name="fr05_market_state", trade_date=TRADE_DATE
        )
        db_session.commit()

        mark_skipped(db_session, run_id, "non_trade_day")
        db_session.commit()

        run_table = Base.metadata.tables["scheduler_task_run"]
        row = db_session.execute(
            run_table.select().where(run_table.c.task_run_id == run_id)
        ).first()
        assert row.status == STATUS_SKIPPED
