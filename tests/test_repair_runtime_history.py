from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import select

from app.models import Base
from scripts.repair_runtime_history import (
    _expire_repair_blocking_tasks,
    _expire_published_report_nonterminal_tasks,
    _rebuild_dashboard_window_snapshots,
    _rebuild_runtime_sim_history,
    _repair_fallback_lineage_and_usage,
    _repair_summary_is_expected_partial,
    _repair_trade_date,
    _stabilize_complete_public_batch_trace,
)
from tests.helpers_ssot import (
    age_report_generation_task,
    insert_kline,
    insert_pool_snapshot,
    insert_report_bundle_ssot,
    insert_settlement_result,
    insert_sim_account,
    insert_sim_dashboard_snapshot,
    insert_stock_master,
    utc_now,
)


def test_repair_runtime_history_timeout_defaults_to_45s_for_claude_cli():
    from scripts.repair_runtime_history import _effective_request_timeout_seconds

    assert _effective_request_timeout_seconds(None, router_primary="claude_cli") == 45
    assert _effective_request_timeout_seconds(None, router_primary="CLAUDE_CLI") == 45


def test_repair_runtime_history_timeout_keeps_explicit_override():
    from scripts.repair_runtime_history import _effective_request_timeout_seconds

    assert _effective_request_timeout_seconds(20, router_primary="claude_cli") == 20
    assert _effective_request_timeout_seconds(60, router_primary="claude_cli") == 60
    assert _effective_request_timeout_seconds(None, router_primary="codex_api") == 20
    assert _effective_request_timeout_seconds(1, router_primary="codex_api") == 5


def test_repair_runtime_history_partial_summary_requires_capped_published_progress():
    assert _repair_summary_is_expected_partial(
        {
            "complete_public_batch": False,
            "round_limit_hit": True,
            "remaining_missing_reports": 197,
            "published_generated_reports": 3,
        }
    ) is True
    assert _repair_summary_is_expected_partial(
        {
            "complete_public_batch": False,
            "round_limit_hit": True,
            "remaining_missing_reports": 197,
            "published_generated_reports": 0,
        }
    ) is False
    assert _repair_summary_is_expected_partial(
        {
            "complete_public_batch": True,
            "round_limit_hit": True,
            "remaining_missing_reports": 0,
            "published_generated_reports": 3,
        }
    ) is False


def test_repair_runtime_history_expires_blocking_nonterminal_tasks(db_session):
    task_table = Base.metadata.tables["report_generation_task"]
    processing_task_id = age_report_generation_task(
        db_session,
        stock_code="600397.SH",
        trade_date="2026-03-19",
        status="Processing",
        updated_hours_ago=1,
    )
    pending_task_id = age_report_generation_task(
        db_session,
        stock_code="688191.SH",
        trade_date="2026-03-19",
        status="Pending",
        updated_hours_ago=1,
    )
    completed_task_id = age_report_generation_task(
        db_session,
        stock_code="603019.SH",
        trade_date="2026-03-19",
        status="Completed",
        updated_hours_ago=1,
    )

    expired = _expire_repair_blocking_tasks(db_session, trade_date_value="2026-03-19")
    assert expired == 2

    rows = {
        row["task_id"]: row
        for row in db_session.execute(
            select(
                task_table.c.task_id,
                task_table.c.status,
                task_table.c.status_reason,
                task_table.c.finished_at,
            )
        ).mappings().all()
    }
    assert rows[processing_task_id]["status"] == "Expired"
    assert rows[processing_task_id]["status_reason"] == "repair_history_preempted_stale_task"
    assert rows[processing_task_id]["finished_at"] is not None
    assert rows[pending_task_id]["status"] == "Expired"
    assert rows[pending_task_id]["status_reason"] == "repair_history_preempted_stale_task"
    assert rows[completed_task_id]["status"] == "Completed"


def test_repair_runtime_history_can_target_single_stock_code(db_session):
    task_table = Base.metadata.tables["report_generation_task"]
    target_task_id = age_report_generation_task(
        db_session,
        stock_code="600397.SH",
        trade_date="2026-03-19",
        status="Processing",
        updated_hours_ago=1,
    )
    other_task_id = age_report_generation_task(
        db_session,
        stock_code="688191.SH",
        trade_date="2026-03-19",
        status="Pending",
        updated_hours_ago=1,
    )

    expired = _expire_repair_blocking_tasks(
        db_session,
        trade_date_value="2026-03-19",
        stock_code="600397.SH",
    )
    assert expired == 1

    rows = {
        row["task_id"]: row
        for row in db_session.execute(
            select(task_table.c.task_id, task_table.c.status, task_table.c.status_reason)
        ).mappings().all()
    }
    assert rows[target_task_id]["status"] == "Expired"
    assert rows[target_task_id]["status_reason"] == "repair_history_preempted_stale_task"
    assert rows[other_task_id]["status"] == "Pending"


def test_repair_runtime_history_exact_pool_codes_fall_back_to_latest_snapshot_task(db_session, monkeypatch):
    from scripts.repair_runtime_history import _repair_exact_pool_codes

    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-18",
        stock_codes=["600519.SH", "000001.SZ", "000002.SZ"],
        status="COMPLETED",
        pool_version=1,
    )
    monkeypatch.setattr("app.services.stock_pool.get_daily_stock_pool", lambda *args, **kwargs: [])

    pool_codes = _repair_exact_pool_codes(
        db_session,
        trade_date_value="2026-03-18",
    )

    assert pool_codes == ["600519.SH", "000001.SZ", "000002.SZ"]


def test_repair_runtime_history_exact_pool_codes_prefer_snapshot_over_config_fallback(db_session, monkeypatch):
    from scripts.repair_runtime_history import _repair_exact_pool_codes

    stock_codes = [f"{600000 + idx:06d}.SH" for idx in range(200)]
    insert_pool_snapshot(
        db_session,
        trade_date="2026-03-18",
        stock_codes=stock_codes,
        status="COMPLETED",
        pool_version=1,
    )
    monkeypatch.setattr(
        "app.services.stock_pool.get_daily_stock_pool",
        lambda *args, **kwargs: ["600519.SH", "000001.SZ", "300750.SZ"],
    )

    pool_codes = _repair_exact_pool_codes(
        db_session,
        trade_date_value="2026-03-18",
    )

    assert pool_codes == stock_codes


def test_repair_runtime_history_expires_published_report_nonterminal_tasks(db_session):
    from uuid import uuid4

    task_table = Base.metadata.tables["report_generation_task"]
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="贵州茅台",
        trade_date="2026-03-19",
    )
    now = utc_now()
    refresh_task_id = db_session.execute(
        select(task_table.c.refresh_task_id).where(
            task_table.c.trade_date == date.fromisoformat("2026-03-19"),
            task_table.c.stock_code == "600519.SH",
        )
    ).scalar_one()
    blocking_task_id = str(uuid4())
    db_session.execute(
        task_table.insert().values(
            task_id=blocking_task_id,
            trade_date=date.fromisoformat("2026-03-19"),
            stock_code="600519.SH",
            idempotency_key="repair-extra:600519.SH:2026-03-19",
            generation_seq=2,
            status="Processing",
            retry_count=0,
            quality_flag="ok",
            status_reason=None,
            llm_fallback_level="primary",
            risk_audit_status="completed",
            risk_audit_skip_reason=None,
            market_state_trade_date=date.fromisoformat("2026-03-19"),
            refresh_task_id=refresh_task_id,
            trigger_task_run_id=None,
            request_id=str(uuid4()),
            superseded_by_task_id=None,
            superseded_at=None,
            queued_at=now,
            started_at=now,
            finished_at=None,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.commit()
    unrelated_task_id = age_report_generation_task(
        db_session,
        stock_code="600397.SH",
        trade_date="2026-03-19",
        status="Processing",
        updated_hours_ago=1,
    )

    expired = _expire_published_report_nonterminal_tasks(
        db_session,
        trade_date_value="2026-03-19",
    )
    assert expired >= 1

    rows = {
        row["task_id"]: row
        for row in db_session.execute(
            select(task_table.c.task_id, task_table.c.status, task_table.c.status_reason)
        ).mappings().all()
    }
    assert rows[blocking_task_id]["status"] == "Expired"
    assert rows[blocking_task_id]["status_reason"] == "repair_history_report_already_published"
    assert rows[unrelated_task_id]["status"] == "Processing"


def test_repair_runtime_history_stabilizes_complete_public_batch_trace_by_expiring_published_tasks(db_session, monkeypatch):
    task_table = Base.metadata.tables["report_generation_task"]
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="贵州茅台",
        trade_date="2026-03-19",
    )
    now = utc_now()
    refresh_task_id = db_session.execute(
        select(task_table.c.refresh_task_id).where(
            task_table.c.trade_date == date.fromisoformat("2026-03-19"),
            task_table.c.stock_code == "600519.SH",
        )
    ).scalar_one()
    db_session.execute(
        task_table.insert().values(
            task_id="repair-extra-processing-task",
            trade_date=date.fromisoformat("2026-03-19"),
            stock_code="600519.SH",
            idempotency_key="repair-extra:600519.SH:2026-03-19",
            generation_seq=2,
            status="Processing",
            retry_count=0,
            quality_flag="ok",
            status_reason=None,
            llm_fallback_level="primary",
            risk_audit_status="completed",
            risk_audit_skip_reason=None,
            market_state_trade_date=date.fromisoformat("2026-03-19"),
            refresh_task_id=refresh_task_id,
            trigger_task_run_id=None,
            request_id="repair-extra-request",
            superseded_by_task_id=None,
            superseded_at=None,
            queued_at=now,
            started_at=now,
            finished_at=None,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.commit()

    def _fake_complete_public_batch_trace(db, *, trade_date: str) -> bool:
        nonterminal = db.execute(
            select(task_table.c.task_id).where(
                task_table.c.trade_date == date.fromisoformat(trade_date),
                task_table.c.status.in_(("Pending", "Processing", "Suspended")),
            )
        ).first()
        return nonterminal is None

    monkeypatch.setattr(
        "app.services.ssot_read_model._has_complete_public_batch_trace",
        _fake_complete_public_batch_trace,
    )

    assert _stabilize_complete_public_batch_trace(
        db_session,
        trade_date_value="2026-03-19",
        max_attempts=1,
        sleep_seconds=0.0,
    ) is True


def test_repair_runtime_history_backfills_fallback_batch_lineage_and_usage(db_session):
    now = utc_now()
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="贵州茅台")
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-19",
        open_price=10.0,
        high_price=10.5,
        low_price=9.8,
        close_price=10.2,
        batch_id="parent-batch-20260319",
    )
    db_session.execute(
        Base.metadata.tables["data_batch"].insert().values(
            batch_id="parent-batch-20260319",
            source_name="tdx_local",
            trade_date=date.fromisoformat("2026-03-19"),
            batch_scope="core_pool",
            batch_seq=1,
            batch_status="SUCCESS",
            quality_flag="ok",
            covered_stock_count=1,
            core_pool_covered_count=1,
            records_total=1,
            records_success=1,
            records_failed=0,
            status_reason=None,
            trigger_task_run_id=None,
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-20",
        open_price=10.2,
        high_price=10.5,
        low_price=10.0,
        close_price=10.2,
        batch_id="fallback_t_minus_1:2026-03-19",
    )
    db_session.execute(
        Base.metadata.tables["report_data_usage"].insert().values(
            usage_id="usage-fallback-1",
            trade_date=date.fromisoformat("2026-03-20"),
            stock_code="600519.SH",
            dataset_name="kline_daily",
            source_name="tdx_local",
            batch_id="generic-batch",
            fetch_time=now,
            status="ok",
            status_reason=None,
            created_at=now,
        )
    )
    db_session.execute(
        Base.metadata.tables["data_batch"].insert().values(
            batch_id="generic-batch",
            source_name="tdx_local",
            trade_date=date.fromisoformat("2026-03-20"),
            batch_scope="core_pool",
            batch_seq=1,
            batch_status="SUCCESS",
            quality_flag="ok",
            covered_stock_count=1,
            core_pool_covered_count=1,
            records_total=1,
            records_success=1,
            records_failed=0,
            status_reason=None,
            trigger_task_run_id=None,
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.commit()

    result = _repair_fallback_lineage_and_usage(
        db_session,
        trade_date_value="2026-03-20",
        stock_codes=["600519.SH"],
    )

    assert result["fallback_batches"] == 1
    assert result["lineage_links"] == 1
    assert result["usage_updates"] == 1

    batch_table = Base.metadata.tables["data_batch"]
    lineage_table = Base.metadata.tables["data_batch_lineage"]
    usage_table = Base.metadata.tables["report_data_usage"]
    batch_row = db_session.execute(
        batch_table.select().where(batch_table.c.batch_id == "fallback_t_minus_1:2026-03-19")
    ).mappings().one()
    assert batch_row["quality_flag"] == "stale_ok"
    assert batch_row["status_reason"] == "fallback_t_minus_1"
    assert batch_row["batch_scope"] == "repair_fallback"

    lineage_row = db_session.execute(
        lineage_table.select().where(
            lineage_table.c.child_batch_id == "fallback_t_minus_1:2026-03-19",
            lineage_table.c.parent_batch_id == "parent-batch-20260319",
        )
    ).mappings().one()
    assert lineage_row["lineage_role"] == "FALLBACK_FROM"

    usage_row = db_session.execute(
        usage_table.select().where(usage_table.c.usage_id == "usage-fallback-1")
    ).mappings().one()
    assert usage_row["batch_id"] == "fallback_t_minus_1:2026-03-19"
    assert usage_row["status"] == "stale_ok"
    assert usage_row["status_reason"] == "fallback_t_minus_1"


def test_repair_runtime_history_rebuilds_window_snapshots_with_market_baselines(db_session, monkeypatch):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="贵州茅台",
        trade_date="2026-03-10",
        strategy_type="A",
        recommendation="BUY",
    )
    insert_settlement_result(
        db_session,
        report_id=report.report_id,
        stock_code="600519.SH",
        signal_date="2026-03-10",
        window_days=7,
        strategy_type="A",
        exit_trade_date="2026-03-20",
        net_return_pct=0.05,
    )

    def _random_market(*args, **kwargs):
        return {
            "baseline_type": "baseline_random",
            "simulation_runs": 500,
            "sample_size": 31,
            "win_rate": 0.58,
            "profit_loss_ratio": 1.4,
            "alpha_annual": 0.08,
            "max_drawdown_pct": -0.07,
            "cumulative_return_pct": 0.03,
            "display_hint": None,
            "window_days": kwargs["window_days"],
        }

    monkeypatch.setattr("app.services.settlement_ssot.baseline_random_market_metrics", _random_market)
    monkeypatch.setattr(
        "app.services.settlement_ssot.baseline_ma_cross_market_metrics",
        lambda db, trade_day, window_days: {
            "baseline_type": "baseline_ma_cross",
            "simulation_runs": None,
            "sample_size": 30,
            "win_rate": 0.52,
            "profit_loss_ratio": 1.3,
            "alpha_annual": 0.05,
            "max_drawdown_pct": -0.06,
            "cumulative_return_pct": 0.01,
            "display_hint": None,
            "window_days": window_days,
        },
    )

    _rebuild_dashboard_window_snapshots(db_session, snapshot_date="2026-03-20")

    strategy_table = Base.metadata.tables["strategy_metric_snapshot"]
    row = db_session.execute(
        strategy_table.select().where(
            strategy_table.c.snapshot_date == date.fromisoformat("2026-03-20"),
            strategy_table.c.window_days == 7,
            strategy_table.c.strategy_type == "A",
        )
    ).mappings().one()

    assert row["sample_size"] == 1
    assert float(row["coverage_pct"]) == 1.0

    baseline_table = Base.metadata.tables["baseline_metric_snapshot"]
    random_row = db_session.execute(
        baseline_table.select().where(
            baseline_table.c.snapshot_date == date.fromisoformat("2026-03-20"),
            baseline_table.c.window_days == 7,
            baseline_table.c.baseline_type == "baseline_random",
        )
    ).mappings().one()
    assert random_row["sample_size"] == 31
    assert float(random_row["win_rate"]) == 0.58


def test_repair_runtime_history_snapshot_helper_delegates_to_history_rebuilder(db_session, monkeypatch):
    calls: list[dict[str, object]] = []

    def _fake_history_rebuild(
        db,
        *,
        trade_days,
        window_days_list,
        purge_invalid=True,
        prune_missing_dates=False,
    ):
        calls.append(
            {
                "trade_days": list(trade_days),
                "window_days_list": list(window_days_list),
                "purge_invalid": purge_invalid,
                "prune_missing_dates": prune_missing_dates,
            }
        )
        return {"rebuilt": [{"trade_day": "2026-03-20", "window_days": 7}]}

    monkeypatch.setattr(
        "app.services.settlement_ssot.rebuild_fr07_snapshot_history",
        _fake_history_rebuild,
    )

    summary = _rebuild_dashboard_window_snapshots(db_session, snapshot_date="2026-03-20")

    assert summary == [{"trade_day": "2026-03-20", "window_days": 7}]
    assert calls == [
        {
            "trade_days": ["2026-03-20"],
            "window_days_list": [1, 7, 14, 30, 60],
            "purge_invalid": True,
            "prune_missing_dates": False,
        }
    ]


def test_repair_runtime_history_restores_previous_sim_state_when_replay_fails(db_session, monkeypatch):
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100000.0,
        cash_available=98000.0,
        total_asset=123456.0,
        peak_total_asset=130000.0,
        max_drawdown_pct=-0.12,
        drawdown_state="REDUCE",
        drawdown_state_factor=0.8,
        active_position_count=2,
        last_reconciled_trade_date="2026-03-18",
    )
    insert_sim_dashboard_snapshot(
        db_session,
        capital_tier="100k",
        snapshot_date="2026-03-18",
        sample_size=12,
        total_return_pct=0.2345,
    )

    monkeypatch.setattr(
        "app.services.sim_positioning_ssot.process_trade_date",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("sim replay exploded")),
    )

    with pytest.raises(RuntimeError, match="sim replay exploded"):
        _rebuild_runtime_sim_history(
            db_session,
            runtime_trade_date="2026-03-20",
            replay_trade_dates=["2026-03-19"],
        )

    account_table = Base.metadata.tables["sim_account"]
    snapshot_table = Base.metadata.tables["sim_dashboard_snapshot"]
    restored_account = db_session.execute(
        account_table.select().where(account_table.c.capital_tier == "100k")
    ).mappings().one()
    restored_snapshot = db_session.execute(
        snapshot_table.select().where(
            snapshot_table.c.capital_tier == "100k",
            snapshot_table.c.snapshot_date == date.fromisoformat("2026-03-18"),
        )
    ).mappings().one()

    assert float(restored_account["total_asset"]) == 123456.0
    assert restored_account["active_position_count"] == 2
    assert float(restored_snapshot["total_return_pct"]) == 0.2345
    assert restored_snapshot["sample_size"] == 12



def test_repair_runtime_history_restores_previous_sim_state_when_replay_is_interrupted(db_session, monkeypatch):
    class _InterruptLike(BaseException):
        """Sentinel exception to simulate interruption during replay."""

    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100000.0,
        cash_available=97000.0,
        total_asset=120000.0,
        peak_total_asset=130000.0,
        max_drawdown_pct=-0.08,
        drawdown_state="REDUCE",
        drawdown_state_factor=0.8,
        active_position_count=1,
        last_reconciled_trade_date="2026-03-18",
    )
    insert_sim_dashboard_snapshot(
        db_session,
        capital_tier="100k",
        snapshot_date="2026-03-18",
        sample_size=10,
        total_return_pct=0.1111,
    )

    from scripts import repair_runtime_history as repair_history

    restore_calls: dict[str, int] = {}
    original_restore = repair_history._restore_runtime_sim_state

    def _wrapped_restore(db, *, snapshot):
        restore_calls["sim_account"] = len(snapshot.get("sim_account") or [])
        restore_calls["sim_dashboard_snapshot"] = len(snapshot.get("sim_dashboard_snapshot") or [])
        return original_restore(db, snapshot=snapshot)

    monkeypatch.setattr(
        "scripts.repair_runtime_history._restore_runtime_sim_state",
        _wrapped_restore,
    )
    monkeypatch.setattr(
        "app.services.sim_positioning_ssot.process_trade_date",
        lambda *args, **kwargs: (_ for _ in ()).throw(_InterruptLike()),
    )

    with pytest.raises(_InterruptLike):
        _rebuild_runtime_sim_history(
            db_session,
            runtime_trade_date="2026-03-20",
            replay_trade_dates=["2026-03-19"],
        )

    assert restore_calls["sim_account"] >= 1
    assert restore_calls["sim_dashboard_snapshot"] == 1


def test_repair_runtime_history_expires_lingering_processing_tasks_left_by_generation(db_session, monkeypatch):
    stock_codes = [f"600{i:03d}.SH" for i in range(200)]
    target_code = stock_codes[0]
    inserted_processing_task_id: dict[str, str | None] = {"task_id": None}

    monkeypatch.setattr("scripts.rebuild_runtime_db.count_kline_coverage", lambda db, trade_date_value: 200)
    monkeypatch.setattr(
        "scripts.rebuild_runtime_db.ensure_bootstrap_market_state_ready",
        lambda db, trade_date_value: None,
    )
    monkeypatch.setattr(
        "scripts.rebuild_runtime_db.ensure_report_usage_rows",
        lambda db, trade_date_value, stock_codes: None,
    )
    monkeypatch.setattr("app.services.stock_pool.refresh_stock_pool", lambda db, trade_date, force_rebuild=True: None)
    monkeypatch.setattr(
        "app.services.stock_pool.get_daily_stock_pool",
        lambda trade_date, exact_trade_date=True: stock_codes,
    )
    monkeypatch.setattr(
        "app.services.market_state.compute_and_persist_market_state",
        lambda db, trade_date: None,
    )
    monkeypatch.setattr(
        "scripts.repair_runtime_history._materialize_t_minus_1_klines",
        lambda db, trade_date_value, stock_codes: set(stock_codes),
    )

    def _fake_generate_report(db, *, stock_code, trade_date, force_same_day_rebuild, forced_strategy_type=None):
        if stock_code != target_code or inserted_processing_task_id["task_id"] is not None:
            return None
        inserted_processing_task_id["task_id"] = age_report_generation_task(
            db,
            stock_code=stock_code,
            trade_date=trade_date,
            status="Processing",
            updated_hours_ago=0,
        )
        return None

    monkeypatch.setattr(
        "app.services.report_generation_ssot.generate_report_ssot",
        _fake_generate_report,
    )

    summary = _repair_trade_date(db_session, trade_date_value="2026-03-19")

    assert summary["pool_size"] == 200
    assert inserted_processing_task_id["task_id"] is not None

    task_table = Base.metadata.tables["report_generation_task"]
    task_row = db_session.execute(
        select(
            task_table.c.status,
            task_table.c.status_reason,
            task_table.c.finished_at,
        ).where(task_table.c.task_id == inserted_processing_task_id["task_id"])
    ).mappings().one()

    assert task_row["status"] == "Expired"
    assert task_row["status_reason"] == "repair_history_preempted_stale_task"
    assert task_row["finished_at"] is not None


def test_repair_runtime_history_caps_generation_to_three_reports_one_per_strategy(db_session, monkeypatch):
    stock_codes = [f"{600000 + idx:06d}.SH" for idx in range(200)]
    generate_calls: list[tuple[str, str | None]] = []

    monkeypatch.setattr("scripts.rebuild_runtime_db.count_kline_coverage", lambda db, trade_date_value: 200)
    monkeypatch.setattr(
        "scripts.rebuild_runtime_db.ensure_bootstrap_market_state_ready",
        lambda db, trade_date_value: None,
    )
    monkeypatch.setattr(
        "scripts.rebuild_runtime_db.ensure_report_usage_rows",
        lambda db, trade_date_value, stock_codes: None,
    )
    monkeypatch.setattr("app.services.stock_pool.refresh_stock_pool", lambda db, trade_date, force_rebuild=True: None)
    monkeypatch.setattr(
        "app.services.market_state.compute_and_persist_market_state",
        lambda db, trade_date: None,
    )
    monkeypatch.setattr(
        "scripts.repair_runtime_history._repair_exact_pool_codes",
        lambda db, trade_date_value: stock_codes,
    )
    monkeypatch.setattr(
        "scripts.repair_runtime_history._materialize_t_minus_1_klines",
        lambda db, trade_date_value, stock_codes: set(stock_codes),
    )
    monkeypatch.setattr(
        "scripts.repair_runtime_history._stabilize_complete_public_batch_trace",
        lambda db, trade_date_value, max_attempts=3, sleep_seconds=0.2: False,
    )

    def _fake_generate_report(db, *, stock_code, trade_date, force_same_day_rebuild, forced_strategy_type=None):
        generate_calls.append((stock_code, forced_strategy_type))
        return {
            "published": True,
            "strategy_type": forced_strategy_type,
        }

    monkeypatch.setattr(
        "app.services.report_generation_ssot.generate_report_ssot",
        _fake_generate_report,
    )

    summary = _repair_trade_date(db_session, trade_date_value="2026-03-19")

    assert summary["generated_reports"] == 3
    assert summary["published_generated_reports"] == 3
    assert summary["requested_missing_reports"] == 200
    assert summary["scheduled_reports"] == 3
    assert summary["deferred_reports"] == 197
    assert summary["remaining_missing_reports"] == 197
    assert summary["round_limit"] == 3
    assert summary["round_limit_hit"] is True
    assert summary["complete_public_batch"] is False
    assert summary["strategy_distribution"] == {
        "A": [stock_codes[0]],
        "B": [stock_codes[1]],
        "C": [stock_codes[2]],
    }
    assert generate_calls == [
        (stock_codes[0], "A"),
        (stock_codes[1], "B"),
        (stock_codes[2], "C"),
    ]
