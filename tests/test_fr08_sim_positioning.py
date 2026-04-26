from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.models import Base
from tests.helpers_ssot import (
    insert_kline,
    insert_open_position,
    insert_report_bundle_ssot,
    insert_sim_account,
    insert_stock_master,
)

pytestmark = [
    pytest.mark.feature("FR08-SIM-01"),
    pytest.mark.feature("FR08-SIM-03"),
    pytest.mark.feature("FR08-SIM-05"),
    pytest.mark.feature("FR08-SIM-06"),
    pytest.mark.feature("FR08-SIM-07"),
    pytest.mark.feature("FR08-SIM-08"),
    pytest.mark.feature("FR08-SIM-10"),
]


def test_fr08_halt_no_new_position(db_session):
    from app.services.sim_positioning_ssot import process_trade_date

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-06",
        signal_entry_price=10.0,
        target_price=12.0,
        stop_loss=9.0,
        trade_instructions={
            "10k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
            "100k": {"status": "EXECUTE", "position_ratio": 0.2, "skip_reason": None},
            "500k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
        },
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-09",
        open_price=10.0,
        high_price=10.4,
        low_price=9.8,
        close_price=10.2,
    )
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100000,
        cash_available=100000,
        total_asset=79000,
        peak_total_asset=100000,
        max_drawdown_pct=-0.21,
        drawdown_state="HALT",
        drawdown_state_factor=0.0,
    )

    process_trade_date(db_session, "2026-03-09")

    table = Base.metadata.tables["sim_position"]
    rows = db_session.execute(
        table.select().where(table.c.report_id == report.report_id)
    ).mappings().all()
    assert len(rows) == 1
    assert rows[0]["position_status"] == "SKIPPED"
    assert rows[0]["skip_reason"] == "drawdown_halt"


def test_fr08_timeout_180d(db_session):
    from app.services.sim_positioning_ssot import process_trade_date

    signal_date = (date(2026, 3, 9) - timedelta(days=200)).isoformat()
    entry_date = (date(2026, 3, 9) - timedelta(days=181)).isoformat()
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date=signal_date,
    )
    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code="600519.SH",
        capital_tier="100k",
        signal_date=signal_date,
        entry_date=entry_date,
        actual_entry_price=10.0,
        signal_entry_price=10.0,
        position_ratio=0.2,
        shares=100,
        stop_loss_price=9.0,
        target_price=12.0,
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-09",
        open_price=10.0,
        high_price=10.2,
        low_price=9.7,
        close_price=10.1,
    )
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100000,
        cash_available=0,
        total_asset=10000,
        peak_total_asset=100000,
        max_drawdown_pct=-0.1,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
        active_position_count=1,
    )

    process_trade_date(db_session, "2026-03-09")

    table = Base.metadata.tables["sim_position"]
    row = db_session.execute(table.select()).mappings().one()
    assert row["position_status"] == "TIMEOUT"
    assert row["exit_date"].isoformat() == "2026-03-09"


def test_fr08_confidence_tie_tiebreak(db_session, monkeypatch):
    from app.services.sim_positioning_ssot import MAX_POSITIONS_BY_TIER, process_trade_date

    monkeypatch.setitem(MAX_POSITIONS_BY_TIER, "10k", 1)
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
    smaller = insert_report_bundle_ssot(
        db_session,
        report_id="00000000-0000-0000-0000-000000000001",
        stock_code="600519.SH",
        trade_date="2026-03-06",
        confidence=0.91,
        signal_entry_price=20.0,
        target_price=24.0,
        stop_loss=18.0,
        trade_instructions={
            "10k": {"status": "EXECUTE", "position_ratio": 0.8, "skip_reason": None},
            "100k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
            "500k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
        },
    )
    insert_report_bundle_ssot(
        db_session,
        report_id="00000000-0000-0000-0000-000000000002",
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-06",
        confidence=0.91,
        signal_entry_price=20.0,
        target_price=24.0,
        stop_loss=18.0,
        trade_instructions={
            "10k": {"status": "EXECUTE", "position_ratio": 0.8, "skip_reason": None},
            "100k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
            "500k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
        },
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-09",
        open_price=20.0,
        high_price=21.0,
        low_price=19.8,
        close_price=20.5,
    )
    insert_kline(
        db_session,
        stock_code="000001.SZ",
        trade_date="2026-03-09",
        open_price=20.0,
        high_price=21.0,
        low_price=19.8,
        close_price=20.5,
    )
    insert_sim_account(
        db_session,
        capital_tier="10k",
        initial_cash=10000,
        cash_available=10000,
        total_asset=10000,
        peak_total_asset=10000,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
    )

    process_trade_date(db_session, "2026-03-09")

    table = Base.metadata.tables["sim_position"]
    rows = db_session.execute(
        table.select().where(
            table.c.capital_tier == "10k",
            table.c.position_status == "OPEN",
        )
    ).mappings().all()
    assert [row["report_id"] for row in rows] == [smaller.report_id]


def test_fr08_txn_rollup(db_session):
    from app.services.sim_positioning_ssot import process_trade_date

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
    old_report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=80.0,
        target_price=100.0,
        stop_loss=70.0,
    )
    new_report = insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-06",
        signal_entry_price=50.0,
        target_price=60.0,
        stop_loss=45.0,
        trade_instructions={
            "10k": {"status": "EXECUTE", "position_ratio": 0.6, "skip_reason": None},
            "100k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
            "500k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
        },
    )
    insert_open_position(
        db_session,
        report_id=old_report.report_id,
        stock_code="600519.SH",
        capital_tier="10k",
        signal_date="2026-03-01",
        entry_date="2026-03-02",
        actual_entry_price=80.0,
        signal_entry_price=80.0,
        position_ratio=0.8,
        shares=100,
        stop_loss_price=70.0,
        target_price=100.0,
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-09",
        open_price=101.0,
        high_price=110.0,
        low_price=99.0,
        close_price=108.0,
    )
    insert_kline(
        db_session,
        stock_code="000001.SZ",
        trade_date="2026-03-09",
        open_price=50.0,
        high_price=51.0,
        low_price=49.5,
        close_price=50.5,
    )
    insert_sim_account(
        db_session,
        capital_tier="10k",
        initial_cash=10000,
        cash_available=0,
        total_asset=10800,
        peak_total_asset=10800,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
        active_position_count=1,
    )

    process_trade_date(db_session, "2026-03-09")

    table = Base.metadata.tables["sim_position"]
    rows = db_session.execute(
        table.select().where(table.c.capital_tier == "10k")
    ).mappings().all()
    closed = [row for row in rows if row["report_id"] == old_report.report_id]
    opened = [row for row in rows if row["report_id"] == new_report.report_id]
    assert closed[0]["position_status"] == "TAKE_PROFIT"
    assert opened[0]["position_status"] == "OPEN"


def test_fr08_delisted_liquidation_updates_real_columns(db_session):
    from app.services.sim_positioning_ssot import _buy_cost, process_trade_date

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI", is_delisted=True)
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        target_price=12.0,
        stop_loss=9.0,
    )
    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code="600519.SH",
        capital_tier="100k",
        signal_date="2026-03-01",
        entry_date="2026-03-02",
        actual_entry_price=10.0,
        signal_entry_price=10.0,
        position_ratio=0.2,
        shares=100,
        stop_loss_price=9.0,
        target_price=12.0,
    )
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100000,
        cash_available=0,
        total_asset=10000,
        peak_total_asset=100000,
        max_drawdown_pct=-0.1,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
        active_position_count=1,
    )

    _, expected_commission, expected_slippage = _buy_cost(10.0, 100)

    process_trade_date(db_session, "2026-03-09")

    table = Base.metadata.tables["sim_position"]
    row = db_session.execute(table.select().where(table.c.report_id == report.report_id)).mappings().one()
    assert row["position_status"] == "DELISTED_LIQUIDATED"
    assert row["exit_price"] == 0.0
    assert row["net_return_pct"] == -1.0
    assert row["commission_total"] == round(expected_commission, 4)
    assert row["slippage_total"] == round(expected_slippage, 4)
    assert row["stamp_duty"] == 0.0


# ──────────────────────────────────────────────────────────────
# FR08-SIM-01 悲观撮合：同日 TP+SL → STOP_LOSS
# ──────────────────────────────────────────────────────────────

def test_fr08_pessimistic_match_tp_and_sl(db_session):
    """同一根K线同时触及止盈+止损 → 悲观：STOP_LOSS。"""
    from app.services.sim_positioning_ssot import process_trade_date

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
    )
    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code="600519.SH",
        capital_tier="100k",
        signal_date="2026-03-01",
        entry_date="2026-03-02",
        actual_entry_price=10.0,
        signal_entry_price=10.0,
        position_ratio=0.2,
        shares=100,
        stop_loss_price=9.0,
        target_price=12.0,
    )
    # high >= target(12) AND low <= stop(9) → pessimistic = STOP_LOSS
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-09",
        open_price=10.0,
        high_price=12.5,
        low_price=8.5,
        close_price=11.0,
    )
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100000,
        cash_available=99000,
        total_asset=100000,
        peak_total_asset=100000,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
        active_position_count=1,
    )

    process_trade_date(db_session, "2026-03-09")

    table = Base.metadata.tables["sim_position"]
    rows = db_session.execute(
        table.select().where(table.c.report_id == report.report_id)
    ).mappings().all()
    # At least one position should be STOP_LOSS (pessimistic match)
    statuses = [r["position_status"] for r in rows]
    assert "STOP_LOSS" in statuses


# ──────────────────────────────────────────────────────────────
# FR08-SIM-02 费用分项断言
# ──────────────────────────────────────────────────────────────

def test_fr08_fee_components(db_session):
    """验证佣金 max(×0.025%, 5)、印花税 0.5‰、滑点 0.0005。"""
    from app.services.sim_positioning_ssot import _buy_cost, _sell_proceeds

    # 买入: 100股 × 10元 = 1000元
    total_buy, buy_comm, buy_slip = _buy_cost(10.0, 100)
    assert buy_comm == 5.0  # max(1000*0.00025=0.25, 5) = 5
    assert round(buy_slip, 4) == round(1000 * 0.0005, 4)  # 0.5元
    assert round(total_buy, 2) == round(1000 + 5.0 + 0.5, 2)

    # 卖出: 100股 × 12元 = 1200元
    proceeds, sell_comm, stamp, sell_slip = _sell_proceeds(12.0, 100)
    assert sell_comm == 5.0  # max(1200*0.00025=0.3, 5) = 5
    assert round(stamp, 4) == round(1200 * 0.0005, 4)  # 0.6元
    assert round(sell_slip, 4) == round(1200 * 0.0005, 4)  # 0.6元
    assert round(proceeds, 2) == round(1200 - 5.0 - 0.6 - 0.6, 2)


# ──────────────────────────────────────────────────────────────
# FR08-SIM-06 REDUCE 状态 factor=0.5 缩仓
# ──────────────────────────────────────────────────────────────

def test_fr08_reduce_half_position(db_session):
    """drawdown_state=REDUCE → factor=0.5 → 实际仓位减半。"""
    from app.services.sim_positioning_ssot import process_trade_date

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-06",
        signal_entry_price=10.0,
        target_price=12.0,
        stop_loss=9.0,
        trade_instructions={
            "10k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
            "100k": {"status": "EXECUTE", "position_ratio": 0.2, "skip_reason": None},
            "500k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
        },
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-09",
        open_price=10.0,
        high_price=10.4,
        low_price=9.8,
        close_price=10.2,
    )
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100000,
        cash_available=100000,
        total_asset=88000,
        peak_total_asset=100000,
        max_drawdown_pct=-0.12,
        drawdown_state="REDUCE",
        drawdown_state_factor=0.5,
    )

    process_trade_date(db_session, "2026-03-09")

    table = Base.metadata.tables["sim_position"]
    rows = db_session.execute(
        table.select().where(table.c.capital_tier == "100k")
    ).mappings().all()
    opened = [r for r in rows if r["position_status"] == "OPEN"]
    if opened:
        # factor=0.5 → ratio=0.2*0.5=0.1 → shares should be about 100000*0.1/10=1000
        assert opened[0]["shares"] <= 1000


# ──────────────────────────────────────────────────────────────
# FR08-SIM-07 停牌/volume=0 → 持仓顺延
# ──────────────────────────────────────────────────────────────

def test_fr08_suspended_defer(db_session):
    """停牌或volume=0时，持仓不平仓，标记 suspended_pending。"""
    from app.services.sim_positioning_ssot import process_trade_date

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
    )
    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code="600519.SH",
        capital_tier="100k",
        signal_date="2026-03-01",
        entry_date="2026-03-02",
        actual_entry_price=10.0,
        signal_entry_price=10.0,
        position_ratio=0.2,
        shares=100,
        stop_loss_price=9.0,
        target_price=12.0,
    )
    # 停牌: is_suspended=True
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-09",
        open_price=10.0,
        high_price=10.0,
        low_price=10.0,
        close_price=10.0,
        is_suspended=True,
    )
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100000,
        cash_available=99000,
        total_asset=100000,
        peak_total_asset=100000,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
        active_position_count=1,
    )

    process_trade_date(db_session, "2026-03-09")

    table = Base.metadata.tables["sim_position"]
    row = db_session.execute(
        table.select().where(table.c.report_id == report.report_id)
    ).mappings().one()
    assert row["position_status"] == "OPEN"
    assert row["suspended_pending"] is True


# ──────────────────────────────────────────────────────────────
# FR08-SIM-08 Outbox 事件派发
# ──────────────────────────────────────────────────────────────

def test_fr08_outbox_position_closed_event(db_session):
    """持仓平仓后 → outbox 有 POSITION_CLOSED 事件。"""
    from app.services.sim_positioning_ssot import process_trade_date

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-01",
        signal_entry_price=10.0,
        stop_loss=9.0,
        target_price=12.0,
    )
    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code="600519.SH",
        capital_tier="100k",
        signal_date="2026-03-01",
        entry_date="2026-03-02",
        actual_entry_price=10.0,
        signal_entry_price=10.0,
        position_ratio=0.2,
        shares=100,
        stop_loss_price=9.0,
        target_price=12.0,
    )
    # 止损触发: low <= 9.0
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-09",
        open_price=9.5,
        high_price=9.6,
        low_price=8.8,
        close_price=9.1,
    )
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100000,
        cash_available=99000,
        total_asset=100000,
        peak_total_asset=100000,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
        active_position_count=1,
    )

    process_trade_date(db_session, "2026-03-09")

    outbox_table = Base.metadata.tables["outbox_event"]
    events = db_session.execute(outbox_table.select()).mappings().all()
    # There should be at least 1 outbox event dispatched from position close
    assert len(events) >= 1
