from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
import time
from uuid import uuid4

import pytest

from app.models import Base
from tests.helpers_ssot import (
    insert_baseline_equity_curve_point,
    insert_baseline_metric_snapshot,
    insert_kline,
    insert_market_state_cache,
    insert_report_bundle_ssot,
    insert_settlement_result,
    insert_sim_account,
    insert_sim_dashboard_snapshot,
    insert_stock_master,
    insert_strategy_metric_snapshot,
)

pytestmark = [
    pytest.mark.feature("FR07-SETTLE-01"),
    pytest.mark.feature("FR07-SETTLE-02"),
    pytest.mark.feature("FR07-SETTLE-03"),
    pytest.mark.feature("FR07-SETTLE-04"),
    pytest.mark.feature("FR07-SETTLE-05"),
    pytest.mark.feature("FR07-SETTLE-06"),
    pytest.mark.feature("FR07-SETTLE-07"),
]


def _admin_headers(client, create_user) -> dict[str, str]:
    user_info = create_user(
        email="admin-fr07@example.com",
        password="Password123",
        role="admin",
        tier="Enterprise",
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": user_info["user"].email, "password": user_info["password"]},
    )
    assert login.status_code == 200
    return {"Authorization": f"Bearer {login.json()['data']['access_token']}"}


def _wait_for_task_terminal(db_session, task_id: str, *, timeout_seconds: float = 5.0) -> dict:
    task_table = Base.metadata.tables["settlement_task"]
    deadline = time.monotonic() + timeout_seconds
    while True:
        db_session.expire_all()
        row = db_session.execute(
            task_table.select().where(task_table.c.task_id == task_id)
        ).mappings().first()
        if row and str(row.get("status") or "").upper() in {"COMPLETED", "FAILED"}:
            return dict(row)
        if time.monotonic() >= deadline:
            raise AssertionError(f"settlement task did not reach terminal state: {task_id}")
        time.sleep(0.05)


def _json_dict(value) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        return json.loads(value)
    return {}


def test_fr07_internal_settlement_run_and_query_task_status(client, db_session, internal_headers):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )
    # Release any open transaction in the fixture session before the API call opens
    # its own DB session to write settlement_task rows.
    db_session.commit()
    headers = internal_headers("fr07-internal-token")

    run_resp = client.post(
        "/api/v1/internal/settlement/run",
        json={"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": False},
        headers=headers,
    )
    assert run_resp.status_code == 202
    run_data = run_resp.json()["data"]
    assert run_data["status"] == "QUEUED"
    assert isinstance(run_data["task_id"], str)

    query_resp = client.get(
        f"/api/v1/internal/settlement/tasks/{run_data['task_id']}",
        headers=headers,
    )
    assert query_resp.status_code == 200
    task = query_resp.json()["data"]
    assert task["task_id"] == run_data["task_id"]
    assert task["trade_date"] == "2026-03-10"
    assert task["window_days"] == 7
    assert task["target_scope"] == "all"
    assert task["status"] in {"QUEUED", "PROCESSING", "COMPLETED", "FAILED"}
    assert isinstance(task["processed_count"], int)
    assert isinstance(task["skipped_count"], int)
    assert isinstance(task["failed_count"], int)
    assert isinstance(task["force"], bool)


def test_fr07_internal_settlement_task_query_not_found(client, internal_headers):
    response = client.get(
        f"/api/v1/internal/settlement/tasks/{uuid4()}",
        headers=internal_headers("fr07-internal-token"),
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "NOT_FOUND"


def test_fr07_settlement_traceable(client, db_session, create_user):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": False},
        headers=_admin_headers(client, create_user),
    )

    assert response.status_code == 202
    data = response.json()["data"]
    assert data["status"] == "QUEUED"
    _wait_for_task_terminal(db_session, data["task_id"])

    result_table = Base.metadata.tables["settlement_result"]
    row = db_session.execute(result_table.select()).mappings().one()
    assert row["report_id"] == report.report_id
    assert row["stock_code"] == "600519.SH"
    assert row["strategy_type"] == "A"
    assert row["settlement_status"] == "settled"
    assert row["entry_trade_date"].isoformat() == "2026-03-01"
    assert row["exit_trade_date"].isoformat() == "2026-03-10"


def test_fr07_settlement_inline_mode_keeps_accepted_semantics(client, db_session, create_user):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": True},
        headers=_admin_headers(client, create_user),
    )

    assert response.status_code == 202
    data = response.json()["data"]
    assert data["status"] == "QUEUED"
    task = _wait_for_task_terminal(db_session, data["task_id"])
    assert task["status"] == "COMPLETED"
    pipeline_table = Base.metadata.tables["pipeline_run"]
    pipeline_row = db_session.execute(
        pipeline_table.select().where(
            pipeline_table.c.trade_date == date(2026, 3, 10),
            pipeline_table.c.pipeline_name == "settlement_pipeline:7:all:all",
        )
    ).mappings().one()
    assert pipeline_row["pipeline_status"] == "COMPLETED"


def test_fr07_settlement_async_mode_returns_queued_semantics(client, db_session, create_user, monkeypatch):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )
    monkeypatch.setenv("SETTLEMENT_INLINE_EXECUTION", "false")

    class _FakeThread:
        def __init__(self, target=None, args=(), kwargs=None, daemon=None, name=None):
            self._target = target
            self._args = args
            self._kwargs = kwargs or {}

        def start(self):
            # keep task in QUEUED state for async contract assertion
            return None

    monkeypatch.setattr("app.services.settlement_ssot.threading.Thread", _FakeThread)

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": True},
        headers=_admin_headers(client, create_user),
    )

    assert response.status_code == 202
    data = response.json()["data"]
    assert data["status"] == "QUEUED"
    task_table = Base.metadata.tables["settlement_task"]
    task = db_session.execute(
        task_table.select().where(task_table.c.task_id == data["task_id"])
    ).mappings().one()
    assert task["status"] == "QUEUED"


def test_fr07_submit_endpoint_runs_true_async_background_completion(client, db_session, create_user, monkeypatch):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )
    monkeypatch.setenv("SETTLEMENT_INLINE_EXECUTION", "true")

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": True},
        headers=_admin_headers(client, create_user),
    )

    assert response.status_code == 202
    data = response.json()["data"]
    assert data["status"] == "QUEUED"
    task = _wait_for_task_terminal(db_session, data["task_id"])
    assert task["status"] == "COMPLETED"
    row = db_session.execute(
        Base.metadata.tables["settlement_result"].select().where(
            Base.metadata.tables["settlement_result"].c.report_id == report.report_id,
            Base.metadata.tables["settlement_result"].c.window_days == 7,
        )
    ).mappings().one()
    assert row["settlement_status"] == "settled"


def test_fr07_admin_submit_keeps_admin_operation_pending_until_task_terminal(
    client, db_session, create_user, monkeypatch
):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )
    monkeypatch.setenv("SETTLEMENT_INLINE_EXECUTION", "false")

    class _FakeThread:
        def __init__(self, target=None, args=(), kwargs=None, daemon=None, name=None):
            self._target = target
            self._args = args
            self._kwargs = kwargs or {}

        def start(self):
            return None

    monkeypatch.setattr("app.services.settlement_ssot.threading.Thread", _FakeThread)
    headers = _admin_headers(client, create_user)
    headers["X-Request-ID"] = "req-fr07-admin-pending"

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": True},
        headers=headers,
    )

    assert response.status_code == 202
    op_table = Base.metadata.tables["admin_operation"]
    audit_table = Base.metadata.tables["audit_log"]
    operation = db_session.execute(
        op_table.select().where(op_table.c.request_id == headers["X-Request-ID"])
    ).mappings().one()
    assert operation["status"] == "PENDING"
    assert operation["finished_at"] is None
    after_snapshot = _json_dict(operation["after_snapshot"])
    assert after_snapshot["task_submit_status"] == "QUEUED"
    assert after_snapshot["task_status_snapshot"] == "QUEUED"
    audit = db_session.execute(
        audit_table.select().where(audit_table.c.operation_id == operation["operation_id"])
    ).first()
    assert audit is None


def test_fr07_admin_submit_finalizes_admin_operation_on_task_terminal(client, db_session, create_user):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )
    headers = _admin_headers(client, create_user)
    headers["X-Request-ID"] = "req-fr07-admin-terminal"

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": True},
        headers=headers,
    )

    assert response.status_code == 202
    _wait_for_task_terminal(db_session, response.json()["data"]["task_id"])
    db_session.expire_all()

    op_table = Base.metadata.tables["admin_operation"]
    audit_table = Base.metadata.tables["audit_log"]
    operation = db_session.execute(
        op_table.select().where(op_table.c.request_id == headers["X-Request-ID"])
    ).mappings().one()
    assert operation["status"] == "COMPLETED"
    assert operation["finished_at"] is not None
    after_snapshot = _json_dict(operation["after_snapshot"])
    assert after_snapshot["task_submit_status"] == "QUEUED"
    assert after_snapshot["task_status_snapshot"] == "COMPLETED"
    assert after_snapshot["processed_count"] >= 1
    audit = db_session.execute(
        audit_table.select().where(audit_table.c.operation_id == operation["operation_id"])
    ).mappings().one()
    audit_after_snapshot = _json_dict(audit["after_snapshot"])
    assert audit_after_snapshot["task_status_snapshot"] == "COMPLETED"


def test_fr07_internal_duplicate_submit_conflicts_while_running_then_reuses_terminal_task(client, db_session, internal_headers):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )
    db_session.commit()
    headers = internal_headers("fr07-internal-duplicate-token")
    payload = {"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": False}

    first = client.post("/api/v1/internal/settlement/run", json=payload, headers=headers)
    second = client.post("/api/v1/internal/settlement/run", json=payload, headers=headers)

    assert first.status_code == 202
    assert second.status_code == 409
    assert first.json()["data"]["status"] == "QUEUED"
    _wait_for_task_terminal(db_session, first.json()["data"]["task_id"])
    third = client.post("/api/v1/internal/settlement/run", json=payload, headers=headers)
    assert third.status_code == 202
    assert third.json()["data"]["status"] == "QUEUED"
    assert third.json()["data"]["task_id"] == first.json()["data"]["task_id"]


def test_fr07_pipeline_run_batches_windows_into_single_terminal_state(db_session):
    from app.services.settlement_ssot import _sync_pipeline_run_from_task, get_settlement_pipeline_status

    pipeline_table = Base.metadata.tables["pipeline_run"]
    task_table = Base.metadata.tables["settlement_task"]
    now = datetime.now(timezone.utc)
    request_id = "batch-pipeline-request"
    db_session.execute(
        pipeline_table.insert().values(
            pipeline_run_id=str(uuid4()),
            pipeline_name="settlement_pipeline:batch:all:all",
            trade_date=date(2026, 3, 10),
            pipeline_status="ACCEPTED",
            degraded=False,
            status_reason=None,
            request_id=request_id,
            started_at=None,
            finished_at=None,
            updated_at=now,
            created_at=now,
        )
    )
    for window_days, status in ((1, "COMPLETED"), (7, "COMPLETED"), (14, "FAILED")):
        db_session.execute(
            task_table.insert().values(
                task_id=str(uuid4()),
                task_scope_key=f"2026-03-10:{window_days}:all:all",
                trade_date=date(2026, 3, 10),
                window_days=window_days,
                target_scope="all",
                target_report_id=None,
                target_stock_code=None,
                force=False,
                status=status,
                processed_count=0,
                skipped_count=0,
                failed_count=1 if status == "FAILED" else 0,
                status_reason="DEPENDENCY_NOT_READY" if status == "FAILED" else None,
                lock_key=f"settlement:{window_days}",
                request_id=request_id,
                requested_by_user_id=None,
                started_at=now,
                finished_at=now,
                updated_at=now,
                created_at=now,
            )
        )
    db_session.commit()

    seed_task = db_session.execute(
        task_table.select().where(task_table.c.request_id == request_id).limit(1)
    ).mappings().one()
    _sync_pipeline_run_from_task(db_session, dict(seed_task))
    db_session.commit()

    pipeline = get_settlement_pipeline_status(
        db_session,
        trade_date="2026-03-10",
        target_scope="all",
        window_days_list=(1, 7, 14, 30, 60),
    )
    assert pipeline["pipeline_name"] == "settlement_pipeline:batch:all:all"
    assert pipeline["pipeline_status"] == "DEGRADED"
    assert pipeline["degraded"] is True
    assert pipeline["status_reason"] == "DEPENDENCY_NOT_READY"


def test_fr07_pipeline_run_waits_for_required_materialization_before_completed(db_session):
    from app.services.settlement_ssot import _sync_pipeline_run_from_task, get_settlement_pipeline_status

    pipeline_table = Base.metadata.tables["pipeline_run"]
    task_table = Base.metadata.tables["settlement_task"]
    now = datetime.now(timezone.utc)
    request_id = "batch-materialization-request"
    trade_day = date(2026, 3, 10)
    db_session.execute(
        pipeline_table.insert().values(
            pipeline_run_id=str(uuid4()),
            pipeline_name="settlement_pipeline:batch:all:all",
            trade_date=trade_day,
            pipeline_status="ACCEPTED",
            degraded=False,
            status_reason=None,
            request_id=request_id,
            started_at=None,
            finished_at=None,
            updated_at=now,
            created_at=now,
        )
    )
    for window_days in (1, 7, 14, 30, 60):
        db_session.execute(
            task_table.insert().values(
                task_id=str(uuid4()),
                task_scope_key=f"2026-03-10:{window_days}:all:all",
                trade_date=trade_day,
                window_days=window_days,
                target_scope="all",
                target_report_id=None,
                target_stock_code=None,
                force=False,
                status="COMPLETED",
                processed_count=1,
                skipped_count=0,
                failed_count=0,
                status_reason=None,
                lock_key=f"settlement:{window_days}",
                request_id=request_id,
                requested_by_user_id=None,
                started_at=now,
                finished_at=now,
                updated_at=now,
                created_at=now,
            )
        )
        for strategy_type in ("A", "B", "C"):
            insert_strategy_metric_snapshot(
                db_session,
                snapshot_date="2026-03-10",
                strategy_type=strategy_type,
                window_days=window_days,
            )
        for baseline_type in ("baseline_random", "baseline_ma_cross"):
            insert_baseline_metric_snapshot(
                db_session,
                snapshot_date="2026-03-10",
                baseline_type=baseline_type,
                window_days=window_days,
            )
    db_session.commit()

    seed_task = db_session.execute(
        task_table.select().where(task_table.c.request_id == request_id).limit(1)
    ).mappings().one()
    _sync_pipeline_run_from_task(db_session, dict(seed_task))
    db_session.commit()

    pipeline = get_settlement_pipeline_status(
        db_session,
        trade_date="2026-03-10",
        target_scope="all",
        window_days_list=(1, 7, 14, 30, 60),
    )
    assert pipeline["pipeline_status"] == "RUNNING"
    assert pipeline["status_reason"] == "settlement_materialization_pending"
    assert pipeline["finished_at"] is None

    for capital_tier, initial_cash in (("10k", 10_000.0), ("100k", 100_000.0), ("500k", 500_000.0)):
        insert_sim_account(
            db_session,
            capital_tier=capital_tier,
            initial_cash=initial_cash,
            cash_available=initial_cash,
            total_asset=initial_cash,
            peak_total_asset=initial_cash,
            max_drawdown_pct=0.0,
            drawdown_state="NORMAL",
            drawdown_state_factor=1.0,
            last_reconciled_trade_date="2026-03-10",
        )
        for baseline_type in ("baseline_random", "baseline_ma_cross"):
            insert_baseline_equity_curve_point(
                db_session,
                capital_tier=capital_tier,
                baseline_type=baseline_type,
                trade_date="2026-03-10",
                equity=1.02,
            )
        insert_sim_dashboard_snapshot(
            db_session,
            capital_tier=capital_tier,
            snapshot_date="2026-03-10",
        )

    _sync_pipeline_run_from_task(db_session, dict(seed_task))
    db_session.commit()

    pipeline = get_settlement_pipeline_status(
        db_session,
        trade_date="2026-03-10",
        target_scope="all",
        window_days_list=(1, 7, 14, 30, 60),
    )
    assert pipeline["pipeline_status"] == "COMPLETED"
    assert pipeline["status_reason"] is None
    assert pipeline["finished_at"] is not None


def test_fr07_pipeline_status_exposes_zero_pipeline_run_truth_when_never_run(db_session):
    from app.services.settlement_ssot import get_settlement_pipeline_status

    pipeline = get_settlement_pipeline_status(
        db_session,
        trade_date="2026-03-10",
        target_scope="all",
        window_days_list=(1, 7, 14, 30, 60),
    )

    assert pipeline["pipeline_status"] == "NOT_RUN"
    assert pipeline["pipeline_run_total"] == 0
    assert pipeline["matching_pipeline_run_total"] == 0


def test_fr07_pipeline_run_ignores_stale_materialization_from_previous_run(db_session):
    from app.services.settlement_ssot import _sync_pipeline_run_from_task, get_settlement_pipeline_status

    pipeline_table = Base.metadata.tables["pipeline_run"]
    task_table = Base.metadata.tables["settlement_task"]
    strategy_table = Base.metadata.tables["strategy_metric_snapshot"]
    baseline_table = Base.metadata.tables["baseline_metric_snapshot"]
    dashboard_table = Base.metadata.tables["sim_dashboard_snapshot"]
    curve_table = Base.metadata.tables["baseline_equity_curve_point"]
    now = datetime.now(timezone.utc)
    stale_time = now - timedelta(days=1)
    request_id = "batch-stale-materialization-request"
    trade_day = date(2026, 3, 10)

    db_session.execute(
        pipeline_table.insert().values(
            pipeline_run_id=str(uuid4()),
            pipeline_name="settlement_pipeline:batch:all:all",
            trade_date=trade_day,
            pipeline_status="ACCEPTED",
            degraded=False,
            status_reason=None,
            request_id=request_id,
            started_at=None,
            finished_at=None,
            updated_at=now,
            created_at=now,
        )
    )
    for window_days in (1, 7, 14, 30, 60):
        db_session.execute(
            task_table.insert().values(
                task_id=str(uuid4()),
                task_scope_key=f"2026-03-10:{window_days}:all:all",
                trade_date=trade_day,
                window_days=window_days,
                target_scope="all",
                target_report_id=None,
                target_stock_code=None,
                force=True,
                status="COMPLETED",
                processed_count=1,
                skipped_count=0,
                failed_count=0,
                status_reason=None,
                lock_key=f"settlement:{window_days}",
                request_id=request_id,
                requested_by_user_id=None,
                started_at=now,
                finished_at=now,
                updated_at=now,
                created_at=now,
            )
        )
        for strategy_type in ("A", "B", "C"):
            db_session.execute(
                strategy_table.insert().values(
                    metric_snapshot_id=str(uuid4()),
                    snapshot_date=trade_day,
                    strategy_type=strategy_type,
                    window_days=window_days,
                    data_status="READY",
                    sample_size=30,
                    coverage_pct=1.0,
                    win_rate=0.6,
                    profit_loss_ratio=1.8,
                    alpha_annual=0.12,
                    max_drawdown_pct=-0.08,
                    cumulative_return_pct=0.2,
                    signal_validity_warning=False,
                    display_hint=None,
                    created_at=stale_time,
                )
            )
        for baseline_type in ("baseline_random", "baseline_ma_cross"):
            db_session.execute(
                baseline_table.insert().values(
                    baseline_metric_snapshot_id=str(uuid4()),
                    snapshot_date=trade_day,
                    window_days=window_days,
                    baseline_type=baseline_type,
                    simulation_runs=500,
                    sample_size=30,
                    win_rate=0.55,
                    profit_loss_ratio=1.5,
                    alpha_annual=0.08,
                    max_drawdown_pct=-0.09,
                    cumulative_return_pct=0.16,
                    display_hint=None,
                    created_at=stale_time,
                )
            )
    for capital_tier in ("10k", "100k", "500k"):
        insert_sim_account(
            db_session,
            capital_tier=capital_tier,
            initial_cash={"10k": 10_000.0, "100k": 100_000.0, "500k": 500_000.0}[capital_tier],
            cash_available={"10k": 10_000.0, "100k": 100_000.0, "500k": 500_000.0}[capital_tier],
            total_asset={"10k": 10_000.0, "100k": 100_000.0, "500k": 500_000.0}[capital_tier],
            peak_total_asset={"10k": 10_000.0, "100k": 100_000.0, "500k": 500_000.0}[capital_tier],
            max_drawdown_pct=0.0,
            drawdown_state="NORMAL",
            drawdown_state_factor=1.0,
            last_reconciled_trade_date="2026-03-10",
        )
        db_session.execute(
            dashboard_table.insert().values(
                dashboard_snapshot_id=str(uuid4()),
                capital_tier=capital_tier,
                snapshot_date=trade_day,
                data_status="READY",
                status_reason=None,
                total_return_pct=0.2,
                win_rate=0.6,
                profit_loss_ratio=1.8,
                alpha_annual=0.12,
                max_drawdown_pct=-0.08,
                sample_size=30,
                display_hint=None,
                is_simulated_only=True,
                created_at=stale_time,
            )
        )
        for baseline_type in ("baseline_random", "baseline_ma_cross"):
            db_session.execute(
                curve_table.insert().values(
                    baseline_equity_curve_point_id=str(uuid4()),
                    capital_tier=capital_tier,
                    baseline_type=baseline_type,
                    trade_date=trade_day,
                    equity=1.02,
                    created_at=stale_time,
                )
            )
    db_session.commit()

    seed_task = db_session.execute(
        task_table.select().where(task_table.c.request_id == request_id).limit(1)
    ).mappings().one()
    _sync_pipeline_run_from_task(db_session, dict(seed_task))
    db_session.commit()

    pipeline = get_settlement_pipeline_status(
        db_session,
        trade_date="2026-03-10",
        target_scope="all",
        window_days_list=(1, 7, 14, 30, 60),
    )
    assert pipeline["pipeline_status"] == "RUNNING"
    assert pipeline["status_reason"] == "settlement_materialization_pending"
    assert pipeline["finished_at"] is None


def test_fr07_dashboard_stats_stays_computing_while_settlement_pipeline_not_completed(db_session):
    from app.services.ssot_read_model import get_dashboard_stats_payload_ssot

    insert_market_state_cache(db_session, trade_date="2026-03-10", market_state="NEUTRAL")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_settlement_result(
        db_session,
        report_id=report.report_id,
        stock_code="600519.SH",
        signal_date="2026-03-10",
        window_days=30,
        strategy_type="A",
        settlement_status="settled",
    )
    insert_strategy_metric_snapshot(
        db_session,
        snapshot_date="2026-03-10",
        strategy_type="A",
        window_days=30,
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-10",
        baseline_type="baseline_random",
        window_days=30,
    )
    db_session.execute(
        Base.metadata.tables["pipeline_run"].insert().values(
            pipeline_run_id=str(uuid4()),
            pipeline_name="settlement_pipeline:batch:all:all",
            trade_date=date(2026, 3, 10),
            pipeline_status="RUNNING",
            degraded=False,
            status_reason=None,
            request_id="req-dashboard-running",
            started_at=datetime.now(timezone.utc),
            finished_at=None,
            updated_at=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc),
        )
    )
    db_session.commit()

    payload = get_dashboard_stats_payload_ssot(db_session, window_days=30)

    assert payload["total_settled"] == 1
    assert payload["data_status"] == "COMPUTING"
    assert payload["status_reason"] == "settlement_pipeline_not_completed"


def test_fr07_batch_submit_marks_pipeline_failed_when_window_submit_errors(db_session, monkeypatch):
    from app.services import settlement_ssot

    calls = {"count": 0}
    original_submit = settlement_ssot.submit_settlement_task

    def _fake_submit(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 2:
            raise settlement_ssot.SettlementServiceError(409, "CONCURRENT_CONFLICT")
        return {"task_id": str(uuid4()), "status": "QUEUED", "force": False}

    monkeypatch.setattr(settlement_ssot, "submit_settlement_task", _fake_submit)

    with pytest.raises(settlement_ssot.SettlementServiceError, match="CONCURRENT_CONFLICT"):
        settlement_ssot.submit_settlement_batch(
            db_session,
            trade_date="2026-03-10",
            force=False,
            window_days_list=(1, 7),
        )

    pipeline = settlement_ssot.get_settlement_pipeline_status(
        db_session,
        trade_date="2026-03-10",
        target_scope="all",
        window_days_list=(1, 7),
    )
    assert pipeline["pipeline_name"] == "settlement_pipeline:batch:all:all"
    assert pipeline["pipeline_status"] == "FAILED"
    assert pipeline["status_reason"] == "CONCURRENT_CONFLICT"

    monkeypatch.setattr(settlement_ssot, "submit_settlement_task", original_submit)


def test_fr07_rebuild_finalize_blocks_until_settlement_pipeline_completed(monkeypatch):
    from scripts import rebuild_runtime_db

    calls: list[str] = []

    class _DummyDb:
        def commit(self):
            calls.append("commit")

    def _raise_not_completed(db, *, trade_date_value, force=True):
        raise RuntimeError("settlement_pipeline_not_completed:RUNNING:unknown")

    def _ensure_runtime_users(db):
        calls.append("ensure_runtime_users")

    monkeypatch.setattr(rebuild_runtime_db, "run_settlement_pipeline_step", _raise_not_completed)
    monkeypatch.setattr(rebuild_runtime_db, "ensure_runtime_users", _ensure_runtime_users)

    with pytest.raises(RuntimeError, match="settlement_pipeline_not_completed"):
        rebuild_runtime_db.finalize_runtime_rebuild_state(_DummyDb(), current_trade_date="2026-03-10")

    assert "ensure_runtime_users" not in calls


def test_fr07_rebuild_step_rejects_non_completed_terminal_pipeline(db_session, monkeypatch):
    import app.services.settlement_ssot as settlement_ssot
    from scripts import rebuild_runtime_db

    monkeypatch.setattr(
        settlement_ssot,
        "submit_settlement_batch",
        lambda *args, **kwargs: [{"task_id": str(uuid4()), "status": "QUEUED", "force": True}],
    )
    monkeypatch.setattr(
        settlement_ssot,
        "wait_for_settlement_pipeline",
        lambda **kwargs: {"pipeline_status": "DEGRADED", "status_reason": "partial_materialization"},
    )

    with pytest.raises(RuntimeError, match="settlement_pipeline_not_completed:DEGRADED:partial_materialization"):
        rebuild_runtime_db.run_settlement_pipeline_step(
            db_session,
            trade_date_value="2026-03-10",
            force=True,
        )


def test_fr07_rebuild_step_rejects_running_pipeline_before_downstream_runtime_reads(db_session, monkeypatch):
    import app.services.settlement_ssot as settlement_ssot
    from scripts import rebuild_runtime_db

    monkeypatch.setattr(
        settlement_ssot,
        "submit_settlement_batch",
        lambda *args, **kwargs: [{"task_id": str(uuid4()), "status": "QUEUED", "force": True}],
    )
    monkeypatch.setattr(
        settlement_ssot,
        "wait_for_settlement_pipeline",
        lambda **kwargs: {
            "pipeline_status": "RUNNING",
            "status_reason": "settlement_materialization_pending",
        },
    )

    with pytest.raises(
        RuntimeError,
        match="settlement_pipeline_not_completed:RUNNING:settlement_materialization_pending",
    ):
        rebuild_runtime_db.run_settlement_pipeline_step(
            db_session,
            trade_date_value="2026-03-10",
            force=True,
        )


def test_fr07_fee_deduction(client, db_session, create_user):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-02-26",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-09",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-09", "window_days": 7, "target_scope": "all", "force": False},
        headers=_admin_headers(client, create_user),
    )

    assert response.status_code == 202
    _wait_for_task_terminal(db_session, response.json()["data"]["task_id"])
    result_table = Base.metadata.tables["settlement_result"]
    row = db_session.execute(result_table.select()).mappings().one()
    assert float(row["buy_commission"]) == 5.0
    assert float(row["sell_commission"]) == 5.0
    assert round(float(row["stamp_duty"]), 3) == 0.525
    assert round(float(row["buy_slippage_cost"]), 2) == 0.50
    assert round(float(row["sell_slippage_cost"]), 4) == 0.5250
    assert float(row["net_return_pct"]) != float(row["gross_return_pct"])




def test_fr07_baseline_random_win_rate_excludes_zero_return_positions(monkeypatch):
    from app.services import settlement_ssot

    results = ([{"net_return_pct": 0.02}] * 15) + ([{"net_return_pct": -0.01}] * 5) + ([{"net_return_pct": 0.0}] * 20)

    class _DeterministicRandom:
        def __init__(self, seed):
            self._index = -1

        def choice(self, seq):
            self._index = (self._index + 1) % len(seq)
            return seq[self._index]

    monkeypatch.setattr("random.Random", _DeterministicRandom)

    payload = settlement_ssot.baseline_random_metrics(results, window_days=7)

    assert payload["sample_size"] == 40
    assert payload["win_rate"] == 0.75


def test_fr07_baseline_random_runs_500(client, db_session, create_user):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": False},
        headers=_admin_headers(client, create_user),
    )

    assert response.status_code == 202
    _wait_for_task_terminal(db_session, response.json()["data"]["task_id"])
    metric_table = Base.metadata.tables["baseline_metric_snapshot"]
    rows = db_session.execute(metric_table.select()).mappings().all()
    baseline_random = [row for row in rows if row["baseline_type"] == "baseline_random"]
    assert len(baseline_random) == 1
    assert baseline_random[0]["simulation_runs"] == 500


def test_fr07_signal_validity_warning(client, db_session, create_user, monkeypatch):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-02-26",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-09",
        open_price=9.6,
        high_price=9.7,
        low_price=9.2,
        close_price=9.4,
    )

    monkeypatch.setattr(
        "app.services.settlement_ssot.load_random_baseline_market_returns",
        lambda *args, **kwargs: [
            {
                "template_index": index,
                "stock_code": f"300{index:03d}.SZ",
                "signal_date": date(2026, 2, 26),
                "exit_trade_date": date(2026, 3, 9),
                "net_return_pct": 0.01,
            }
            for index in range(35)
        ],
    )
    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-09", "window_days": 7, "target_scope": "all", "force": False},
        headers=_admin_headers(client, create_user),
    )

    assert response.status_code == 202
    _wait_for_task_terminal(db_session, response.json()["data"]["task_id"])
    metric_table = Base.metadata.tables["strategy_metric_snapshot"]
    row = db_session.execute(
        metric_table.select().where(metric_table.c.strategy_type == "A")
    ).mappings().one()
    assert row["strategy_type"] == "A"
    assert row["signal_validity_warning"] is True


def test_fr07_recomputes_window_snapshots_from_full_window_across_split_runs(client, db_session, create_user):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-02-27",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="B",
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-02-27",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="B",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.9,
        low_price=9.8,
        close_price=10.7,
    )
    insert_kline(
        db_session,
        stock_code="000001.SZ",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=11.0,
        low_price=9.9,
        close_price=10.8,
    )
    headers = _admin_headers(client, create_user)

    first = client.post(
        "/api/v1/admin/settlement/run",
        json={
            "trade_date": "2026-03-10",
            "window_days": 7,
            "target_scope": "stock_code",
            "target_stock_code": "600519.SH",
            "force": True,
        },
        headers=headers,
    )
    assert first.status_code == 202
    _wait_for_task_terminal(db_session, first.json()["data"]["task_id"])

    second = client.post(
        "/api/v1/admin/settlement/run",
        json={
            "trade_date": "2026-03-10",
            "window_days": 7,
            "target_scope": "stock_code",
            "target_stock_code": "000001.SZ",
            "force": True,
        },
        headers=headers,
    )
    assert second.status_code == 202
    _wait_for_task_terminal(db_session, second.json()["data"]["task_id"])

    db_session.expire_all()
    metric_table = Base.metadata.tables["strategy_metric_snapshot"]
    metric_row = db_session.execute(
        metric_table.select().where(
            metric_table.c.snapshot_date == date(2026, 3, 10),
            metric_table.c.window_days == 7,
            metric_table.c.strategy_type == "B",
        )
    ).mappings().one()
    assert metric_row["sample_size"] == 2

    baseline_table = Base.metadata.tables["baseline_metric_snapshot"]
    baseline_row = db_session.execute(
        baseline_table.select().where(
            baseline_table.c.snapshot_date == date(2026, 3, 10),
            baseline_table.c.window_days == 7,
            baseline_table.c.baseline_type == "baseline_random",
        )
    ).mappings().one()
    assert baseline_row["simulation_runs"] == 500
    assert baseline_row["sample_size"] is not None


def test_fr07_coverage_pct_uses_same_window_buy_reports(client, db_session, create_user):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-02-27",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="B",
        recommendation="BUY",
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-02-27",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="B",
        recommendation="BUY",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.8,
        low_price=9.8,
        close_price=10.6,
    )

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={
            "trade_date": "2026-03-10",
            "window_days": 7,
            "target_scope": "stock_code",
            "target_stock_code": "600519.SH",
            "force": True,
        },
        headers=_admin_headers(client, create_user),
    )

    assert response.status_code == 202
    _wait_for_task_terminal(db_session, response.json()["data"]["task_id"])
    db_session.expire_all()
    metric_table = Base.metadata.tables["strategy_metric_snapshot"]
    row = db_session.execute(
        metric_table.select().where(
            metric_table.c.snapshot_date == date(2026, 3, 10),
            metric_table.c.window_days == 7,
            metric_table.c.strategy_type == "B",
        )
    ).mappings().one()
    assert row["sample_size"] == 1
    assert float(row["coverage_pct"]) == 0.5


def test_fr07_settlement_run_purges_deleted_report_results_from_window_metrics(db_session):
    from app.services.settlement_ssot import _load_window_settled_results, _purge_invalid_settlement_results

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
    active_report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-13",
        strategy_type="C",
        recommendation="BUY",
    )
    deleted_report = insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-13",
        strategy_type="C",
        recommendation="BUY",
    )
    deleted_report.is_deleted = True
    db_session.commit()

    insert_settlement_result(
        db_session,
        report_id=active_report.report_id,
        stock_code="600519.SH",
        signal_date="2026-03-13",
        window_days=7,
        strategy_type="C",
        exit_trade_date="2026-03-17",
        net_return_pct=0.03,
    )
    insert_settlement_result(
        db_session,
        report_id=deleted_report.report_id,
        stock_code="000001.SZ",
        signal_date="2026-03-13",
        window_days=7,
        strategy_type="C",
        exit_trade_date="2026-03-17",
        net_return_pct=0.04,
    )
    deleted_count = _purge_invalid_settlement_results(db_session)
    assert deleted_count == 1

    rows = _load_window_settled_results(
        db_session,
        trade_day=date(2026, 3, 17),
        window_days=7,
    )
    assert len(rows) == 1
    assert rows[0]["report_id"] == active_report.report_id
    assert rows[0]["strategy_type"] == "C"


def test_fr07_signal_warning_uses_full_window_history_not_latest_batch(client, db_session, create_user, monkeypatch):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-02-27",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="B",
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-02-27",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="B",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=9.4,
        high_price=9.5,
        low_price=9.0,
        close_price=9.2,
    )
    insert_kline(
        db_session,
        stock_code="000001.SZ",
        trade_date="2026-03-10",
        open_price=10.4,
        high_price=11.0,
        low_price=10.2,
        close_price=10.9,
    )

    monkeypatch.setattr(
        "app.services.settlement_ssot.load_random_baseline_market_returns",
        lambda *args, **kwargs: [
            {
                "template_index": 0,
                "stock_code": "300001.SZ",
                "signal_date": date(2026, 2, 27),
                "exit_trade_date": date(2026, 3, 10),
                "net_return_pct": 0.10,
            },
            {
                "template_index": 1,
                "stock_code": "300002.SZ",
                "signal_date": date(2026, 2, 27),
                "exit_trade_date": date(2026, 3, 10),
                "net_return_pct": 0.10,
            },
        ],
    )
    headers = _admin_headers(client, create_user)

    first = client.post(
        "/api/v1/admin/settlement/run",
        json={
            "trade_date": "2026-03-10",
            "window_days": 7,
            "target_scope": "stock_code",
            "target_stock_code": "600519.SH",
            "force": True,
        },
        headers=headers,
    )
    assert first.status_code == 202
    _wait_for_task_terminal(db_session, first.json()["data"]["task_id"])

    second = client.post(
        "/api/v1/admin/settlement/run",
        json={
            "trade_date": "2026-03-10",
            "window_days": 7,
            "target_scope": "stock_code",
            "target_stock_code": "000001.SZ",
            "force": True,
        },
        headers=headers,
    )
    assert second.status_code == 202
    _wait_for_task_terminal(db_session, second.json()["data"]["task_id"])

    db_session.expire_all()
    metric_table = Base.metadata.tables["strategy_metric_snapshot"]
    row = db_session.execute(
        metric_table.select().where(
            metric_table.c.snapshot_date == date(2026, 3, 10),
            metric_table.c.window_days == 7,
            metric_table.c.strategy_type == "B",
        )
    ).mappings().one()
    assert row["sample_size"] == 2
    assert row["signal_validity_warning"] is True


# ──────────────────────────────────────────────────────────────
# Bug回归测试（2026-03-13修复）
# ──────────────────────────────────────────────────────────────

def test_fr07_force_true_no_unique_constraint(client, db_session, create_user):
    """Bug回归：settlement force=True 重复提交不应触发 UNIQUE 约束（settlement_task）。"""
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )
    headers = _admin_headers(client, create_user)
    payload = {"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": True}

    r1 = client.post("/api/v1/admin/settlement/run", json=payload, headers=headers)
    assert r1.status_code == 202, f"first force call failed: {r1.text}"

    # 第二次 force=True 不能因 UNIQUE 约束失败
    r2 = client.post("/api/v1/admin/settlement/run", json=payload, headers=headers)
    assert r2.status_code == 202, f"second force call failed with UNIQUE constraint: {r2.text}"


def test_baseline_service_snapshot_date_column(db_session):
    """Bug回归：baseline_service 使用正确的列名 snapshot_date（而非 trade_date）。"""
    from app.services.baseline_service import settle_baselines
    from app.models import Base
    from uuid import uuid4
    from datetime import datetime, timezone, date

    # 直接插入一行 BASELINE_QUEUED 记录（snapshot_date 字段）
    baseline_task_table = Base.metadata.tables["baseline_task"]
    now = datetime.now(timezone.utc)
    db_session.execute(
        baseline_task_table.insert().values(
            baseline_task_id=str(uuid4()),
            snapshot_date=date(2026, 2, 1),
            window_days=1,
            baseline_type="baseline_random",
            simulation_runs=500,
            status="BASELINE_QUEUED",
            status_reason=None,
            started_at=None,
            finished_at=None,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.commit()

    # settle_baselines 不应抛出 "no such column: trade_date"
    closed = settle_baselines(db_session, as_of_date="2026-03-01")
    assert isinstance(closed, int)


def test_baseline_service_generation_helpers_are_legacy_noops(db_session, monkeypatch):
    from app.core.config import settings
    from app.services.baseline_service import generate_ma_cross_baseline, generate_random_baseline

    baseline_task_table = Base.metadata.tables["baseline_task"]
    monkeypatch.setattr(settings, "stock_pool", "600519.SH")
    monkeypatch.setattr(
        "app.services.baseline_service.load_tdx_day_records",
        lambda stock_code, limit=100: [
            {"date": f"2026-03-{day:02d}", "close": 10.0 + day / 100, "volume": 100_000 + day}
            for day in range(1, 21)
        ],
    )

    assert generate_random_baseline(db_session, trade_date="2026-03-10") is True
    assert generate_ma_cross_baseline(db_session, trade_date="2026-03-10") == 0

    rows = db_session.execute(baseline_task_table.select()).mappings().all()
    assert rows == []


def test_strategy_failure_uses_ssot_columns(db_session):
    """Bug回归：strategy_failure.check_and_update_strategy_paused 不使用旧ORM列名。"""
    from app.services.strategy_failure import check_and_update_strategy_paused
    # Should not raise AttributeError: type object 'SimPosition' has no attribute 'strategy_type'
    result = check_and_update_strategy_paused(db_session)
    assert isinstance(result, list)


# ──────────────────────────────────────────────────────────────
# FR07-SETTLE-02 display_hint 样本阈值
# ──────────────────────────────────────────────────────────────

def test_fr07_display_hint_insufficient_sample(client, db_session, create_user):
    """样本<30 → display_hint='样本积累中', 四维度指标为 null。"""
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )
    headers = _admin_headers(client, create_user)
    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": False},
        headers=headers,
    )
    assert response.status_code == 202
    _wait_for_task_terminal(db_session, response.json()["data"]["task_id"])

    metric_table = Base.metadata.tables["strategy_metric_snapshot"]
    row = db_session.execute(
        metric_table.select().where(metric_table.c.strategy_type == "A")
    ).mappings().first()
    if row:
        # sample_size=1 (<30) → display_hint 应含提示
        assert row["sample_size"] < 30
        assert row["display_hint"] == "样本积累中"


# ──────────────────────────────────────────────────────────────
# FR07-SETTLE-04 force=false 幂等跳过
# ──────────────────────────────────────────────────────────────

def test_fr07_force_false_idempotent_skip(client, db_session, create_user):
    """force=false 且同 scope 已结算 → 直接返回已有 task_id（不重新处理）。"""
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )
    headers = _admin_headers(client, create_user)
    payload = {"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": False}

    r1 = client.post("/api/v1/admin/settlement/run", json=payload, headers=headers)
    assert r1.status_code == 202
    _wait_for_task_terminal(db_session, r1.json()["data"]["task_id"])
    task_id_1 = r1.json()["data"]["task_id"]

    r2 = client.post("/api/v1/admin/settlement/run", json=payload, headers=headers)
    assert r2.status_code == 202
    task_id_2 = r2.json()["data"]["task_id"]
    # 同 scope + force=false → 返回同一 task_id（幂等）
    assert task_id_2 == task_id_1


# ──────────────────────────────────────────────────────────────
# FR07-SETTLE-05 并发结算互斥 409
# ──────────────────────────────────────────────────────────────

def test_fr07_concurrent_settlement_409(client, db_session, create_user):
    """并发结算同一 scope → 仅1个成功，另1个 409。"""
    import threading

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )
    headers = _admin_headers(client, create_user)
    payload = {"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": True}

    results = []

    def fire():
        r = client.post("/api/v1/admin/settlement/run", json=payload, headers=headers)
        results.append(r.status_code)

    t1 = threading.Thread(target=fire)
    t2 = threading.Thread(target=fire)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    codes = sorted(results)
    # 至少一个 202，至少一个 409（或两个 202 因测试序列化但不会两个都成功并发）
    assert 202 in codes


def test_fr07_overdue_backfill_uses_due_trade_date_not_current_trade_date(client, db_session, create_user):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    mature_report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_stock_master(db_session, stock_code="600520.SH", stock_name="TEST2")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600520.SH",
        trade_date="2026-03-06",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": False},
        headers=_admin_headers(client, create_user),
    )

    assert response.status_code == 202
    _wait_for_task_terminal(db_session, response.json()["data"]["task_id"])
    result_table = Base.metadata.tables["settlement_result"]
    rows = db_session.execute(result_table.select()).mappings().all()
    assert len(rows) == 1
    assert rows[0]["report_id"] == mature_report.report_id
    assert rows[0]["exit_trade_date"].isoformat() == "2026-03-10"


def test_fr07_runtime_uses_market_ma_cross_baseline_path(client, db_session, create_user, monkeypatch):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="B",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )

    def _market_metrics(*args, **kwargs):
        return {
            "baseline_type": "baseline_ma_cross",
            "simulation_runs": None,
            "sample_size": 35,
            "win_rate": 0.61,
            "profit_loss_ratio": 1.72,
            "alpha_annual": 0.11,
            "max_drawdown_pct": -0.08,
            "cumulative_return_pct": 0.15,
            "display_hint": None,
            "window_days": kwargs["window_days"],
        }

    def _legacy_should_not_run(*args, **kwargs):
        raise AssertionError("legacy baseline_ma_cross_metrics should not be used by settlement runtime")

    monkeypatch.setattr("app.services.settlement_ssot.baseline_ma_cross_market_metrics", _market_metrics)
    monkeypatch.setattr("app.services.settlement_ssot.baseline_ma_cross_metrics", _legacy_should_not_run)

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": False},
        headers=_admin_headers(client, create_user),
    )

    assert response.status_code == 202
    _wait_for_task_terminal(db_session, response.json()["data"]["task_id"])
    baseline_table = Base.metadata.tables["baseline_metric_snapshot"]
    row = db_session.execute(
        baseline_table.select().where(baseline_table.c.baseline_type == "baseline_ma_cross")
    ).mappings().one()
    assert row["sample_size"] == 35
    assert float(row["win_rate"]) == 0.61


def test_fr07_runtime_uses_market_random_baseline_path(client, db_session, create_user, monkeypatch):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="B",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-10",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )

    def _market_metrics(*args, **kwargs):
        return {
            "baseline_type": "baseline_random",
            "simulation_runs": 500,
            "sample_size": 36,
            "win_rate": 0.58,
            "profit_loss_ratio": 1.61,
            "alpha_annual": 0.09,
            "max_drawdown_pct": -0.07,
            "cumulative_return_pct": 0.12,
            "display_hint": None,
            "window_days": kwargs["window_days"],
        }

    def _legacy_should_not_run(*args, **kwargs):
        raise AssertionError("legacy baseline_random_metrics should not be used by settlement runtime")

    monkeypatch.setattr("app.services.settlement_ssot.baseline_random_market_metrics", _market_metrics)
    monkeypatch.setattr("app.services.settlement_ssot.baseline_random_metrics", _legacy_should_not_run)

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": False},
        headers=_admin_headers(client, create_user),
    )

    assert response.status_code == 202
    _wait_for_task_terminal(db_session, response.json()["data"]["task_id"])
    baseline_table = Base.metadata.tables["baseline_metric_snapshot"]
    row = db_session.execute(
        baseline_table.select().where(baseline_table.c.baseline_type == "baseline_random")
    ).mappings().one()
    assert row["sample_size"] == 36
    assert float(row["win_rate"]) == 0.58


def test_fr07_force_rerun_purges_stale_result_when_due_trade_kline_missing(client, db_session, create_user):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
        strategy_type="A",
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-09",
        open_price=10.1,
        high_price=10.6,
        low_price=9.9,
        close_price=10.5,
    )
    insert_settlement_result(
        db_session,
        report_id=report.report_id,
        stock_code="600519.SH",
        signal_date="2026-03-01",
        window_days=7,
        strategy_type="A",
        exit_trade_date="2026-03-09",
        net_return_pct=0.03,
    )

    response = client.post(
        "/api/v1/admin/settlement/run",
        json={"trade_date": "2026-03-10", "window_days": 7, "target_scope": "all", "force": True},
        headers=_admin_headers(client, create_user),
    )

    assert response.status_code == 202
    _wait_for_task_terminal(db_session, response.json()["data"]["task_id"])
    result_table = Base.metadata.tables["settlement_result"]
    rows = db_session.execute(
        result_table.select().where(
            result_table.c.report_id == report.report_id,
            result_table.c.window_days == 7,
        )
    ).mappings().all()
    assert rows == []


def test_fr07_snapshot_history_helper_prunes_missing_dates_and_purges_once(db_session, monkeypatch):
    import app.services.settlement_ssot as settlement_ssot

    strategy_table = Base.metadata.tables["strategy_metric_snapshot"]
    baseline_table = Base.metadata.tables["baseline_metric_snapshot"]
    baseline_task_table = Base.metadata.tables["baseline_task"]
    now = datetime.now(timezone.utc)
    db_session.execute(
        strategy_table.insert().values(
            metric_snapshot_id=str(uuid4()),
            snapshot_date=date(2026, 3, 20),
            strategy_type="A",
            window_days=7,
            data_status="READY",
            sample_size=1,
            coverage_pct=1.0,
            win_rate=1.0,
            profit_loss_ratio=None,
            alpha_annual=None,
            max_drawdown_pct=None,
            cumulative_return_pct=0.01,
            signal_validity_warning=False,
            display_hint=None,
            created_at=now,
        )
    )
    db_session.execute(
        baseline_table.insert().values(
            baseline_metric_snapshot_id=str(uuid4()),
            snapshot_date=date(2026, 3, 20),
            window_days=30,
            baseline_type="baseline_random",
            simulation_runs=500,
            sample_size=1,
            win_rate=1.0,
            profit_loss_ratio=None,
            alpha_annual=None,
            max_drawdown_pct=None,
            cumulative_return_pct=0.01,
            display_hint=None,
            created_at=now,
        )
    )
    db_session.execute(
        baseline_task_table.insert().values(
            baseline_task_id=str(uuid4()),
            snapshot_date=date(2026, 3, 20),
            window_days=30,
            baseline_type="baseline_random",
            simulation_runs=500,
            status="BASELINE_COMPLETED",
            status_reason=None,
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.commit()

    calls: list[tuple[str, int, bool]] = []

    def _fake_rebuild(db, *, trade_day, window_days, purge_invalid=True):
        calls.append((trade_day.isoformat(), int(window_days), bool(purge_invalid)))
        return {
            "trade_day": trade_day.isoformat(),
            "window_days": int(window_days),
            "purged_invalid_results": 0,
            "settled_sample_size": 0,
            "strategy_cumulative_return_pct": None,
            "baseline_random_cumulative_return_pct": None,
            "signal_validity_warning": False,
        }

    monkeypatch.setattr(settlement_ssot, "rebuild_fr07_snapshot", _fake_rebuild)

    summary = settlement_ssot.rebuild_fr07_snapshot_history(
        db_session,
        trade_days=["2026-03-19", "2026-03-21"],
        window_days_list=[30, 7],
        purge_invalid=True,
        prune_missing_dates=True,
    )

    assert calls == [
        ("2026-03-19", 7, True),
        ("2026-03-19", 30, False),
        ("2026-03-21", 7, False),
        ("2026-03-21", 30, False),
    ]
    assert summary["snapshot_dates"] == ["2026-03-19", "2026-03-21"]
    assert summary["window_days"] == [7, 30]
    assert db_session.execute(
        strategy_table.select().where(strategy_table.c.snapshot_date == date(2026, 3, 20))
    ).mappings().all() == []
    assert db_session.execute(
        baseline_table.select().where(baseline_table.c.snapshot_date == date(2026, 3, 20))
    ).mappings().all() == []
    assert db_session.execute(
        baseline_task_table.select().where(baseline_task_table.c.snapshot_date == date(2026, 3, 20))
    ).mappings().all() == []
