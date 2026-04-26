from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

pytestmark = pytest.mark.feature("FR-07")

from app.models import Base
from scripts import rebuild_fr07_truth_snapshots as rebuild_script


def test_fr07_rebuild_script_resolves_single_trade_date():
    dates = rebuild_script._resolve_trade_dates(
        SimpleNamespace(trade_date="2026-03-10", start_date=None, end_date=None)
    )
    assert [item.isoformat() for item in dates] == ["2026-03-10"]


def test_fr07_rebuild_script_resolves_trade_day_range(monkeypatch):
    monkeypatch.setattr(
        rebuild_script,
        "trade_days_in_range",
        lambda start_date, end_date: ["2026-03-10", "2026-03-11"],
    )
    dates = rebuild_script._resolve_trade_dates(
        SimpleNamespace(trade_date=None, start_date="2026-03-10", end_date="2026-03-11")
    )
    assert [item.isoformat() for item in dates] == ["2026-03-10", "2026-03-11"]


def test_fr07_rebuild_script_rejects_invalid_window_days():
    with pytest.raises(SystemExit, match="Unsupported window_days"):
        rebuild_script._validate_window_days([7, 99])


def test_fr07_rebuild_script_deduplicates_window_days_preserving_order():
    assert rebuild_script._validate_window_days([30, 7, 30, 1, 7]) == [30, 7, 1]


def test_fr07_rebuild_script_rolls_back_partial_history_rebuild(db_session, monkeypatch):
    strategy_table = Base.metadata.tables["strategy_metric_snapshot"]

    monkeypatch.setattr(
        rebuild_script,
        "_parse_args",
        lambda: SimpleNamespace(
            trade_date="2026-03-10",
            start_date=None,
            end_date=None,
            window_days=[7],
            skip_purge=False,
        ),
    )
    monkeypatch.setattr(rebuild_script, "SessionLocal", lambda: db_session)

    def _boom(db, *, trade_days, window_days_list, purge_invalid):
        db.execute(
            strategy_table.insert().values(
                metric_snapshot_id=str(uuid4()),
                snapshot_date=date(2026, 3, 10),
                strategy_type="A",
                window_days=7,
                data_status="READY",
                sample_size=1,
                coverage_pct=1.0,
                win_rate=1.0,
                profit_loss_ratio=None,
                alpha_annual=None,
                max_drawdown_pct=None,
                cumulative_return_pct=0.03,
                signal_validity_warning=False,
                display_hint=None,
                created_at=datetime.now(timezone.utc),
            )
        )
        db.flush()
        raise RuntimeError("boom")

    monkeypatch.setattr(rebuild_script, "rebuild_fr07_snapshot_history", _boom)

    with pytest.raises(RuntimeError, match="boom"):
        rebuild_script.main()

    db_session.rollback()
    rows = db_session.execute(
        strategy_table.select().where(
            strategy_table.c.snapshot_date == date(2026, 3, 10),
            strategy_table.c.window_days == 7,
        )
    ).mappings().all()
    assert rows == []
