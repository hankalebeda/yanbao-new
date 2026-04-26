from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.services.fr07_baseline_service import load_random_baseline_market_returns
from app.models import Base
from tests.helpers_ssot import insert_kline, insert_stock_master


@contextmanager
def _session():
    db_path = Path("d:/yanbao/.pytest-tmp/fr07_baseline_service.sqlite3")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = Session()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()
        if db_path.exists():
            db_path.unlink()


@pytest.mark.feature("FR07-SETTLE-06")
def test_fr07_baseline_service_filters_incomplete_public_batch_dates(monkeypatch):
    signal_date = "2026-03-09"
    exit_date = "2026-03-10"
    trade_day = date.fromisoformat(exit_date)

    with _session() as db_session:
        insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
        insert_kline(
            db_session,
            stock_code="600519.SH",
            trade_date=signal_date,
            open_price=10.0,
            high_price=10.2,
            low_price=9.9,
            close_price=10.0,
        )
        insert_kline(
            db_session,
            stock_code="600519.SH",
            trade_date=exit_date,
            open_price=10.1,
            high_price=10.6,
            low_price=10.0,
            close_price=10.5,
        )

        monkeypatch.setattr(
            "app.services.ssot_read_model._has_complete_public_batch_trace",
            lambda db, *, trade_date: False,
        )

        rows = load_random_baseline_market_returns(
            db_session,
            trade_day=trade_day,
            window_days=1,
            truth_rows=[
                {
                    "report_id": "r-1",
                    "signal_date": signal_date,
                    "exit_trade_date": exit_date,
                }
            ],
        )

        assert rows == []


@pytest.mark.feature("FR07-SETTLE-06")
def test_fr07_baseline_service_uses_truth_row_entry_exit_dates(monkeypatch):
    signal_date = "2026-03-09"
    exit_date = "2026-03-10"

    with _session() as db_session:
        insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
        insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
        for stock_code, start_close, end_close in (
            ("600519.SH", 10.0, 10.5),
            ("000001.SZ", 20.0, 19.0),
        ):
            insert_kline(
                db_session,
                stock_code=stock_code,
                trade_date=signal_date,
                open_price=start_close,
                high_price=start_close * 1.02,
                low_price=start_close * 0.98,
                close_price=start_close,
            )
            insert_kline(
                db_session,
                stock_code=stock_code,
                trade_date=exit_date,
                open_price=end_close,
                high_price=end_close * 1.02,
                low_price=end_close * 0.98,
                close_price=end_close,
            )

        monkeypatch.setattr(
            "app.services.ssot_read_model._has_complete_public_batch_trace",
            lambda db, *, trade_date: True,
        )

        rows = load_random_baseline_market_returns(
            db_session,
            trade_day=date.fromisoformat(exit_date),
            window_days=1,
            truth_rows=[
                {
                    "report_id": "r-1",
                    "signal_date": signal_date,
                    "exit_trade_date": exit_date,
                }
            ],
        )

        assert {row["stock_code"] for row in rows} == {"600519.SH", "000001.SZ"}
        assert {row["signal_date"].isoformat() for row in rows} == {signal_date}
        assert {row["exit_trade_date"].isoformat() for row in rows} == {exit_date}
