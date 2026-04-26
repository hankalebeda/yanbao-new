from __future__ import annotations

import pytest

pytestmark = pytest.mark.feature("FR-07")

import shutil
import tempfile
from contextlib import contextmanager
from datetime import date
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import Base
from app.services import settlement_ssot
from tests.helpers_ssot import (
    insert_kline,
    insert_report_bundle_ssot,
    insert_settlement_result,
    insert_stock_master,
)


@contextmanager
def _session():
    base_dir = Path("d:/yanbao/.pytest-tmp")
    base_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(prefix="fr07_warning_truth_", dir=base_dir))
    db_path = tmp_dir / "runtime.sqlite3"
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = Session()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()
        shutil.rmtree(tmp_dir, ignore_errors=True)


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


def _seed_settled_reports(
    db_session,
    *,
    trade_date: str,
    signal_date: str,
    window_days: int,
    strategy_type: str,
    returns: list[float],
    stock_code_prefix: str,
) -> None:
    for index, net_return_pct in enumerate(returns):
        stock_code = f"{stock_code_prefix}{index:03d}.SH"
        insert_stock_master(db_session, stock_code=stock_code, stock_name=stock_code)
        report = insert_report_bundle_ssot(
            db_session,
            stock_code=stock_code,
            stock_name=stock_code,
            trade_date=signal_date,
            strategy_type=strategy_type,
            quality_flag="ok",
            published=True,
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


def test_fr07_signal_validity_warning_is_scoped_per_strategy(monkeypatch):
    trade_date = "2026-03-10"
    signal_date = "2026-03-01"
    window_days = 7

    with _session() as db_session:
        _seed_settled_reports(
            db_session,
            trade_date=trade_date,
            signal_date=signal_date,
            window_days=window_days,
            strategy_type="A",
            returns=[-0.02] * 35,
            stock_code_prefix="603",
        )
        _seed_settled_reports(
            db_session,
            trade_date=trade_date,
            signal_date=signal_date,
            window_days=window_days,
            strategy_type="B",
            returns=[0.03] * 35,
            stock_code_prefix="605",
        )

        def _baseline_loader(*args, **kwargs):
            truth_rows = list(kwargs.get("truth_rows") or [])
            candidate_rows: list[dict[str, object]] = []
            for index, row in enumerate(truth_rows):
                strategy_type = str(row.get("strategy_type") or "")
                candidate_rows.append(
                    {
                        "template_index": index,
                        "stock_code": f"300{index:03d}.SZ",
                        "signal_date": date.fromisoformat(signal_date),
                        "exit_trade_date": date.fromisoformat(trade_date),
                        "net_return_pct": 0.01 if strategy_type == "A" else -0.01,
                    }
                )
            return candidate_rows

        monkeypatch.setattr(
            "app.services.settlement_ssot.load_random_baseline_market_returns",
            _baseline_loader,
        )

        settlement_ssot.rebuild_fr07_snapshot(
            db_session,
            trade_day=date.fromisoformat(trade_date),
            window_days=window_days,
            purge_invalid=True,
        )

        strategy_a_row = _strategy_row(
            db_session,
            snapshot_date=trade_date,
            window_days=window_days,
            strategy_type="A",
        )
        strategy_b_row = _strategy_row(
            db_session,
            snapshot_date=trade_date,
            window_days=window_days,
            strategy_type="B",
        )

        assert strategy_a_row["signal_validity_warning"] is True
        assert strategy_b_row["signal_validity_warning"] is False


def test_fr07_signal_validity_warning_requires_full_baseline_truth_alignment(monkeypatch):
    trade_date = "2026-03-10"
    signal_date = "2026-03-01"
    window_days = 7

    with _session() as db_session:
        _seed_settled_reports(
            db_session,
            trade_date=trade_date,
            signal_date=signal_date,
            window_days=window_days,
            strategy_type="A",
            returns=[-0.02, -0.02],
            stock_code_prefix="603",
        )

        monkeypatch.setattr(
            "app.services.settlement_ssot.load_random_baseline_market_returns",
            lambda *args, **kwargs: [
                {
                    "template_index": 0,
                    "stock_code": "300001.SZ",
                    "signal_date": date.fromisoformat(signal_date),
                    "exit_trade_date": date.fromisoformat(trade_date),
                    "net_return_pct": 0.05,
                }
            ],
        )

        summary = settlement_ssot.rebuild_fr07_snapshot(
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

        assert strategy_row["signal_validity_warning"] is False
        assert summary["signal_validity_warning"] is False


def test_fr07_random_baseline_loader_does_not_fallback_to_raw_reports(monkeypatch):
    signal_date = "2026-03-02"
    window_days = 1
    trade_day = date.fromisoformat("2026-03-10")

    with _session() as db_session:
        report = insert_report_bundle_ssot(
            db_session,
            stock_code="600519.SH",
            stock_name="MOUTAI",
            trade_date=signal_date,
            strategy_type="A",
            quality_flag="ok",
            published=True,
        )
        due_trade_date = settlement_ssot._due_trade_date(date.fromisoformat(signal_date), window_days).isoformat()
        insert_kline(
            db_session,
            stock_code="600519.SH",
            trade_date=due_trade_date,
            open_price=10.1,
            high_price=10.6,
            low_price=10.0,
            close_price=10.5,
        )

        monkeypatch.setattr(
            "app.services.ssot_read_model._has_complete_public_batch_trace",
            lambda db, *, trade_date: True,
        )

        rows = settlement_ssot.load_random_baseline_market_returns(
            db_session,
            trade_day=trade_day,
            window_days=window_days,
            truth_rows=None,
        )

        assert report.report_id
        assert rows == []
