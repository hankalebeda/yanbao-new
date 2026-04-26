"""Round 34 permanent gates: dashboard/sim fake-ready and stale sim runtime state."""
from __future__ import annotations

from datetime import date

from sqlalchemy import select

from app.models import Base
from app.services.runtime_materialization import (
    materialize_sim_dashboard_snapshot_history,
    materialize_sim_dashboard_snapshots,
)
from app.services.ssot_read_model import _sanitize_baseline_curve_points, list_sim_account_snapshots_ssot
from app.services.settlement_ssot import baseline_ma_cross_metrics, baseline_random_metrics
from tests.helpers_ssot import (
    insert_baseline_equity_curve_point,
    insert_baseline_metric_snapshot,
    insert_sim_account,
    insert_sim_equity_curve_point,
    insert_report_bundle_ssot,
    utc_now,
)


def test_round34_empty_baseline_metrics_do_not_fabricate_sample():
    random_metrics = baseline_random_metrics([], window_days=30)
    ma_metrics = baseline_ma_cross_metrics([], window_days=30)

    assert random_metrics["sample_size"] == 0
    assert random_metrics["win_rate"] is None
    assert random_metrics["profit_loss_ratio"] is None
    assert ma_metrics["sample_size"] == 0
    assert ma_metrics["win_rate"] is None
    assert ma_metrics["profit_loss_ratio"] is None


def test_round34_materialization_marks_sim_dashboard_computing_without_real_sources(db_session):
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

    materialize_sim_dashboard_snapshots(db_session, snapshot_date="2026-03-13", capital_tiers=["100k"])

    table = Base.metadata.tables["sim_dashboard_snapshot"]
    row = db_session.execute(
        select(table).where(
            table.c.capital_tier == "100k",
            table.c.snapshot_date == "2026-03-13",
        )
    ).mappings().one()

    assert row["data_status"] == "COMPUTING"
    assert row["status_reason"] == "sim_dashboard_not_ready"
    assert float(row["total_return_pct"]) == 0.0
    assert row["sample_size"] == 0


def test_round34_materialization_history_respects_snapshot_date_and_backfills_summary(db_session):
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100_000,
        cash_available=100_000,
        total_asset=100_000,
        peak_total_asset=100_000,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
        active_position_count=0,
        last_reconciled_trade_date="2026-03-13",
    )
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-12", equity=101_000)
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-13", equity=102_000)
    insert_baseline_equity_curve_point(
        db_session,
        capital_tier="100k",
        baseline_type="baseline_random",
        trade_date="2026-03-12",
        equity=100_400,
    )
    insert_baseline_equity_curve_point(
        db_session,
        capital_tier="100k",
        baseline_type="baseline_random",
        trade_date="2026-03-13",
        equity=100_900,
    )
    insert_baseline_equity_curve_point(
        db_session,
        capital_tier="100k",
        baseline_type="baseline_ma_cross",
        trade_date="2026-03-12",
        equity=100_200,
    )
    insert_baseline_equity_curve_point(
        db_session,
        capital_tier="100k",
        baseline_type="baseline_ma_cross",
        trade_date="2026-03-13",
        equity=100_500,
    )
    report_one = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-11",
        ensure_pool_snapshot=False,
    )
    report_two = insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-12",
        ensure_pool_snapshot=False,
    )

    sim_position_table = Base.metadata.tables["sim_position"]
    now = utc_now()
    db_session.execute(
        sim_position_table.insert().values(
            position_id="pos-2026-03-12",
            report_id=report_one.report_id,
            stock_code="600519.SH",
            capital_tier="100k",
            position_status="TAKE_PROFIT",
            signal_date=date.fromisoformat("2026-03-11"),
            entry_date=date.fromisoformat("2026-03-11"),
            actual_entry_price=10.0,
            signal_entry_price=10.0,
            position_ratio=0.1,
            shares=100,
            atr_pct_snapshot=0.02,
            atr_multiplier_snapshot=1.5,
            stop_loss_price=9.5,
            target_price=10.8,
            exit_date=date.fromisoformat("2026-03-12"),
            exit_price=10.6,
            holding_days=1,
            net_return_pct=0.01,
            commission_total=5.0,
            stamp_duty=0.5,
            slippage_total=0.5,
            take_profit_pending_t1=False,
            stop_loss_pending_t1=False,
            suspended_pending=False,
            limit_locked_pending=False,
            skip_reason=None,
            status_reason=None,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.execute(
        sim_position_table.insert().values(
            position_id="pos-2026-03-13",
            report_id=report_two.report_id,
            stock_code="000001.SZ",
            capital_tier="100k",
            position_status="TAKE_PROFIT",
            signal_date=date.fromisoformat("2026-03-12"),
            entry_date=date.fromisoformat("2026-03-12"),
            actual_entry_price=10.0,
            signal_entry_price=10.0,
            position_ratio=0.1,
            shares=100,
            atr_pct_snapshot=0.02,
            atr_multiplier_snapshot=1.5,
            stop_loss_price=9.5,
            target_price=10.8,
            exit_date=date.fromisoformat("2026-03-13"),
            exit_price=10.7,
            holding_days=1,
            net_return_pct=0.02,
            commission_total=5.0,
            stamp_duty=0.5,
            slippage_total=0.5,
            take_profit_pending_t1=False,
            stop_loss_pending_t1=False,
            suspended_pending=False,
            limit_locked_pending=False,
            skip_reason=None,
            status_reason=None,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.commit()

    materialize_sim_dashboard_snapshot_history(db_session, capital_tiers=["100k"])

    table = Base.metadata.tables["sim_dashboard_snapshot"]
    rows = db_session.execute(
        select(
            table.c.snapshot_date,
            table.c.total_return_pct,
            table.c.sample_size,
            table.c.display_hint,
        )
        .where(table.c.capital_tier == "100k")
        .order_by(table.c.snapshot_date.asc())
    ).mappings().all()

    normalized_rows = [
        {
            "snapshot_date": row["snapshot_date"],
            "total_return_pct": float(row["total_return_pct"]),
            "sample_size": row["sample_size"],
            "display_hint": row["display_hint"],
        }
        for row in rows
    ]

    assert normalized_rows == [
        {
            "snapshot_date": date.fromisoformat("2026-03-12"),
            "total_return_pct": 0.01,
            "sample_size": 1,
            "display_hint": "sample_lt_30",
        },
        {
            "snapshot_date": date.fromisoformat("2026-03-13"),
            "total_return_pct": 0.02,
            "sample_size": 2,
            "display_hint": "sample_lt_30",
        },
    ]

    payload = list_sim_account_snapshots_ssot(db_session, capital_tier="100k", page=1, page_size=2)

    assert payload["total"] == 2
    assert payload["items"] == [
        {
            "snapshot_date": "2026-03-13",
            "capital_tier": "100k",
            "capital_tier_raw": "100k",
            "total_asset": 102000.0,
            "cumulative_return_pct": 2.0,
            "max_drawdown_pct": 0.0,
            "drawdown_state": "NORMAL",
            "open_positions": 0,
            "settled_trades": 2,
            "win_rate": 1.0,
            "pnl_ratio": None,
        },
        {
            "snapshot_date": "2026-03-12",
            "capital_tier": "100k",
            "capital_tier_raw": "100k",
            "total_asset": 101000.0,
            "cumulative_return_pct": 1.0,
            "max_drawdown_pct": 0.0,
            "drawdown_state": "NORMAL",
            "open_positions": 0,
            "settled_trades": 1,
            "win_rate": 1.0,
            "pnl_ratio": None,
        },
    ]


def test_round34_materialization_history_prunes_stale_dates_when_anchor_dates_provided(db_session):
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100_000,
        cash_available=100_000,
        total_asset=100_000,
        peak_total_asset=100_000,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
        active_position_count=0,
        last_reconciled_trade_date="2026-03-16",
    )
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-12", equity=101_000)
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-16", equity=102_000)

    table = Base.metadata.tables["sim_dashboard_snapshot"]
    now = utc_now()
    db_session.execute(
        table.insert().values(
            dashboard_snapshot_id="stale-2026-03-11",
            capital_tier="100k",
            snapshot_date=date.fromisoformat("2026-03-11"),
            data_status="READY",
            status_reason=None,
            total_return_pct=0.01,
            win_rate=1.0,
            profit_loss_ratio=None,
            alpha_annual=0.1,
            max_drawdown_pct=0.0,
            sample_size=1,
            display_hint=None,
            is_simulated_only=True,
            created_at=now,
        )
    )
    db_session.execute(
        table.insert().values(
            dashboard_snapshot_id="stale-2026-03-13",
            capital_tier="100k",
            snapshot_date=date.fromisoformat("2026-03-13"),
            data_status="READY",
            status_reason=None,
            total_return_pct=0.02,
            win_rate=1.0,
            profit_loss_ratio=None,
            alpha_annual=0.1,
            max_drawdown_pct=0.0,
            sample_size=1,
            display_hint=None,
            is_simulated_only=True,
            created_at=now,
        )
    )
    db_session.commit()

    materialize_sim_dashboard_snapshot_history(
        db_session,
        capital_tiers=["100k"],
        snapshot_dates=["2026-03-12", "2026-03-16"],
        prune_missing_dates=True,
    )

    snapshot_dates = db_session.execute(
        select(table.c.snapshot_date)
        .where(table.c.capital_tier == "100k")
        .order_by(table.c.snapshot_date.asc())
    ).scalars().all()
    assert snapshot_dates == [
        date.fromisoformat("2026-03-11"),
        date.fromisoformat("2026-03-12"),
        date.fromisoformat("2026-03-16"),
    ]


def test_round34_materialization_history_ignores_non_trade_snapshot_dates(db_session):
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100_000,
        cash_available=100_000,
        total_asset=100_000,
        peak_total_asset=100_000,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
        active_position_count=0,
        last_reconciled_trade_date="2026-03-02",
    )
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-02-27", equity=101_000)
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-01", equity=101_500)
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-02", equity=102_000)

    materialize_sim_dashboard_snapshot_history(
        db_session,
        capital_tiers=["100k"],
        prune_missing_dates=True,
    )

    table = Base.metadata.tables["sim_dashboard_snapshot"]
    snapshot_dates = db_session.execute(
        select(table.c.snapshot_date)
        .where(table.c.capital_tier == "100k")
        .order_by(table.c.snapshot_date.asc())
    ).scalars().all()

    assert snapshot_dates == [
        date.fromisoformat("2026-02-27"),
        date.fromisoformat("2026-03-02"),
    ]


def test_round34_partial_baseline_backfill_preserves_gap_day_continuity(db_session):
    from app.services.runtime_materialization import materialize_baseline_equity_curve_points

    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100_000,
        cash_available=100_000,
        total_asset=100_000,
        peak_total_asset=100_000,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
        active_position_count=0,
        last_reconciled_trade_date="2026-03-12",
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-12",
        baseline_type="baseline_random",
        cumulative_return_pct=0.004,
    )
    insert_baseline_equity_curve_point(
        db_session,
        capital_tier="100k",
        baseline_type="baseline_random",
        trade_date="2026-03-12",
        equity=100_400,
    )
    insert_baseline_equity_curve_point(
        db_session,
        capital_tier="100k",
        baseline_type="baseline_random",
        trade_date="2026-03-13",
        equity=100_400,
    )
    insert_baseline_metric_snapshot(
        db_session,
        snapshot_date="2026-03-16",
        baseline_type="baseline_random",
        cumulative_return_pct=0.006,
    )

    materialize_baseline_equity_curve_points(
        db_session,
        snapshot_date="2026-03-16",
        start_date="2026-03-13",
        purge_existing=True,
    )

    table = Base.metadata.tables["baseline_equity_curve_point"]
    rows = db_session.execute(
        select(table.c.trade_date, table.c.equity)
        .where(
            table.c.capital_tier == "100k",
            table.c.baseline_type == "baseline_random",
            table.c.trade_date.in_(
                    [
                        date.fromisoformat("2026-03-12"),
                        date.fromisoformat("2026-03-13"),
                        date.fromisoformat("2026-03-16"),
                    ]
                ),
            )
            .order_by(table.c.trade_date.asc())
        ).all()

    assert rows == [
        (date.fromisoformat("2026-03-12"), 100_400),
        (date.fromisoformat("2026-03-13"), 100_400),
        (date.fromisoformat("2026-03-16"), 100_600),
    ]


def test_round34_snapshot_list_fail_closes_when_summary_missing(db_session):
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100_000,
        cash_available=100_000,
        total_asset=100_000,
        peak_total_asset=100_000,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
        active_position_count=0,
        last_reconciled_trade_date="2026-03-13",
    )
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date="2026-03-13", equity=102_000)

    payload = list_sim_account_snapshots_ssot(db_session, capital_tier="100k", page=1, page_size=10)

    assert payload["total"] == 0
    assert payload["items"] == []


def test_round34_sim_dashboard_trims_leading_zero_baseline_pollution(db_session):
    points = [
        {"date": "2026-03-17", "equity": 0.0},
        {"date": "2026-03-18", "equity": 0.0},
        {"date": "2026-03-19", "equity": 101000.0},
    ]

    assert _sanitize_baseline_curve_points(points) == [{"date": "2026-03-19", "equity": 101000.0}]
