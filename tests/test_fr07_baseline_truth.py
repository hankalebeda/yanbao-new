from __future__ import annotations

from datetime import date

import pytest

pytestmark = pytest.mark.feature("FR-07")

from app.models import Base
from app.services.fr07_baseline_service import summarize_random_baseline_candidates
from app.services.settlement_ssot import rebuild_fr07_snapshot
from tests.helpers_ssot import insert_report_bundle_ssot, insert_settlement_result, insert_stock_master


def _strategy_row(db_session, *, snapshot_date: str, window_days: int, strategy_type: str) -> dict[str, object]:
    table = Base.metadata.tables["strategy_metric_snapshot"]
    row = db_session.execute(
        table.select().where(
            table.c.snapshot_date == date.fromisoformat(snapshot_date),
            table.c.window_days == window_days,
            table.c.strategy_type == strategy_type,
        )
    ).mappings().one()
    return dict(row)


def _baseline_row(db_session, *, snapshot_date: str, window_days: int, baseline_type: str) -> dict[str, object]:
    table = Base.metadata.tables["baseline_metric_snapshot"]
    row = db_session.execute(
        table.select().where(
            table.c.snapshot_date == date.fromisoformat(snapshot_date),
            table.c.window_days == window_days,
            table.c.baseline_type == baseline_type,
        )
    ).mappings().one()
    return dict(row)


def _seed_settled_reports(
    db_session,
    *,
    trade_date: str,
    signal_date: str,
    window_days: int,
    strategy_type: str,
    returns: list[float],
    quality_flag: str = "ok",
    published: bool = True,
    stock_code_prefix: str = "603",
    start_index: int = 0,
) -> list[str]:
    report_ids: list[str] = []
    for index, net_return_pct in enumerate(returns):
        stock_code = f"{stock_code_prefix}{start_index + index:03d}.SH"
        insert_stock_master(db_session, stock_code=stock_code, stock_name=stock_code)
        report = insert_report_bundle_ssot(
            db_session,
            stock_code=stock_code,
            stock_name=stock_code,
            trade_date=signal_date,
            strategy_type=strategy_type,
            quality_flag=quality_flag,
            published=published,
        )
        insert_settlement_result(
            db_session,
            report_id=report.report_id,
            stock_code=stock_code,
            signal_date=signal_date,
            window_days=window_days,
            strategy_type=strategy_type,
            exit_trade_date=trade_date,
            net_return_pct=net_return_pct,
        )
        report_ids.append(report.report_id)
    return report_ids


@pytest.mark.feature("FR07-SETTLE-06")
def test_fr07_baseline_random_is_independent_market_baseline(db_session, monkeypatch):
    trade_date = "2026-03-10"
    signal_date = "2026-03-01"
    window_days = 7
    report_returns = [-0.02] * 35
    seeded_report_ids = _seed_settled_reports(
        db_session,
        trade_date=trade_date,
        signal_date=signal_date,
        window_days=window_days,
        strategy_type="A",
        returns=report_returns,
    )

    captured_truth_rows: dict[str, list[dict[str, object]]] = {}
    market_candidates = [
        {
            "template_index": index,
            "stock_code": f"300{index:03d}.SZ",
            "signal_date": date.fromisoformat(signal_date),
            "exit_trade_date": date.fromisoformat(trade_date),
            "net_return_pct": 0.01,
        }
        for index in range(len(report_returns))
    ]
    expected_baseline = summarize_random_baseline_candidates(
        market_candidates,
        window_days=window_days,
        trade_day=date.fromisoformat(trade_date),
    )

    def _independent_market_loader(*args, **kwargs):
        captured_truth_rows["rows"] = list(kwargs.get("truth_rows") or [])
        return list(market_candidates)

    monkeypatch.setattr(
        "app.services.settlement_ssot.load_random_baseline_market_returns",
        _independent_market_loader,
    )

    rebuild_fr07_snapshot(
        db_session,
        trade_day=date.fromisoformat(trade_date),
        window_days=window_days,
        purge_invalid=True,
    )

    baseline_row = _baseline_row(
        db_session,
        snapshot_date=trade_date,
        window_days=window_days,
        baseline_type="baseline_random",
    )
    strategy_row = _strategy_row(
        db_session,
        snapshot_date=trade_date,
        window_days=window_days,
        strategy_type="A",
    )

    assert len(captured_truth_rows["rows"]) == len(report_returns)
    assert {row["report_id"] for row in captured_truth_rows["rows"]} == set(seeded_report_ids)
    assert baseline_row["sample_size"] == len(report_returns)
    assert float(baseline_row["cumulative_return_pct"]) == pytest.approx(
        expected_baseline["cumulative_return_pct"],
        rel=1e-6,
    )
    assert float(baseline_row["cumulative_return_pct"]) != pytest.approx(
        float(strategy_row["cumulative_return_pct"]),
        rel=1e-6,
    )


@pytest.mark.feature("FR07-SETTLE-02")
def test_fr07_cumulative_return_pct_has_single_math_definition(db_session, monkeypatch):
    trade_date = "2026-03-10"
    signal_date = "2026-03-01"
    window_days = 7
    strategy_returns = ([0.02] * 20) + ([-0.01] * 15)

    _seed_settled_reports(
        db_session,
        trade_date=trade_date,
        signal_date=signal_date,
        window_days=window_days,
        strategy_type="A",
        returns=strategy_returns,
    )

    market_candidates = [
        {
            "template_index": index,
            "stock_code": f"301{index:03d}.SZ",
            "signal_date": date.fromisoformat(signal_date),
            "exit_trade_date": date.fromisoformat(trade_date),
            "net_return_pct": net_return_pct,
        }
        for index, net_return_pct in enumerate(strategy_returns)
    ]
    expected_cumulative_return_pct = 1.0
    for net_return_pct in strategy_returns:
        expected_cumulative_return_pct *= 1.0 + net_return_pct
    expected_cumulative_return_pct = round(expected_cumulative_return_pct - 1.0, 6)

    monkeypatch.setattr(
        "app.services.settlement_ssot.load_random_baseline_market_returns",
        lambda *args, **kwargs: list(market_candidates),
    )

    rebuild_fr07_snapshot(
        db_session,
        trade_day=date.fromisoformat(trade_date),
        window_days=window_days,
        purge_invalid=True,
    )

    strategy_row = _strategy_row(
        db_session,
        snapshot_date=trade_date,
        window_days=window_days,
        strategy_type="A",
    )
    baseline_row = _baseline_row(
        db_session,
        snapshot_date=trade_date,
        window_days=window_days,
        baseline_type="baseline_random",
    )

    assert float(strategy_row["cumulative_return_pct"]) == pytest.approx(expected_cumulative_return_pct, rel=1e-6)
    assert float(baseline_row["cumulative_return_pct"]) == pytest.approx(expected_cumulative_return_pct, rel=1e-6)
    assert strategy_row["signal_validity_warning"] is False


@pytest.mark.feature("FR07-SETTLE-02")
@pytest.mark.feature("FR07-SETTLE-06")
def test_fr07_rebuild_after_purge_keeps_stats_consistent(db_session, monkeypatch):
    trade_date = "2026-03-10"
    signal_date = "2026-03-01"
    window_days = 7
    kept_report_ids = _seed_settled_reports(
        db_session,
        trade_date=trade_date,
        signal_date=signal_date,
        window_days=window_days,
        strategy_type="A",
        returns=[0.03],
    )
    degraded_report_id = _seed_settled_reports(
        db_session,
        trade_date=trade_date,
        signal_date=signal_date,
        window_days=window_days,
        strategy_type="A",
        returns=[0.25],
        quality_flag="degraded",
        published=True,
        stock_code_prefix="605",
    )[0]

    market_candidates = [
        {
            "template_index": 0,
            "stock_code": "399001.SZ",
            "signal_date": date.fromisoformat(signal_date),
            "exit_trade_date": date.fromisoformat(trade_date),
            "net_return_pct": 0.02,
        }
    ]
    monkeypatch.setattr(
        "app.services.settlement_ssot.load_random_baseline_market_returns",
        lambda *args, **kwargs: list(market_candidates),
    )

    summary = rebuild_fr07_snapshot(
        db_session,
        trade_day=date.fromisoformat(trade_date),
        window_days=window_days,
        purge_invalid=True,
    )

    result_table = Base.metadata.tables["settlement_result"]
    remaining_rows = db_session.execute(
        result_table.select().where(result_table.c.window_days == window_days)
    ).mappings().all()
    assert summary["purged_invalid_results"] == 1
    assert [row["report_id"] for row in remaining_rows] == kept_report_ids
    assert degraded_report_id not in {row["report_id"] for row in remaining_rows}

    strategy_row = _strategy_row(
        db_session,
        snapshot_date=trade_date,
        window_days=window_days,
        strategy_type="A",
    )
    baseline_row = _baseline_row(
        db_session,
        snapshot_date=trade_date,
        window_days=window_days,
        baseline_type="baseline_random",
    )

    assert strategy_row["sample_size"] == 1
    assert float(strategy_row["coverage_pct"]) == 1.0
    assert float(strategy_row["cumulative_return_pct"]) == pytest.approx(0.03, rel=1e-6)
    assert strategy_row["display_hint"] == "样本积累中"
    assert baseline_row["sample_size"] == 1
    assert float(baseline_row["cumulative_return_pct"]) == pytest.approx(0.02, rel=1e-6)
    assert baseline_row["display_hint"] == "样本积累中"
