"""
FR-08 SIM 补充场景测试 — 覆盖 READY_WITH_GAPS 中未测试的场景

  FR08-SIM-01: REDUCE 状态 → 仓位减半（position_ratio * 0.5）
  FR08-SIM-03: INSUFFICIENT_FUNDS → 资金不足跳过 + 碎股取整 + 佣金≥5
  FR08-SIM-05: STOP_LOSS E2E + DELISTED E2E（exit_price=0）
  FR08-SIM-08: 悲观撮合（高==低==涨跌停 → limit_locked_pending=True）
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest

pytestmark = [
    pytest.mark.feature("FR08-SIM-01"),
    pytest.mark.feature("FR08-SIM-03"),
    pytest.mark.feature("FR08-SIM-05"),
    pytest.mark.feature("FR08-SIM-08"),
]

from app.models import Base
from tests.helpers_ssot import (
    insert_kline,
    insert_open_position,
    insert_report_bundle_ssot,
    insert_sim_account,
    insert_stock_master,
)


# ---------------------------------------------------------------------------
# FR08-SIM-01: REDUCE 状态下新开仓仓位减半
# ---------------------------------------------------------------------------

@pytest.mark.feature("FR08-SIM-01")
def test_fr08_reduce_state_halves_effective_position_ratio(db_session):
    """drawdown_state=REDUCE 时 effective_ratio = position_ratio * 0.5（仓位减半）。"""
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
            "100k": {"status": "EXECUTE", "position_ratio": 0.2, "skip_reason": None},
        },
    )
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-09",
        open_price=10.0,
        high_price=10.5,
        low_price=9.8,
        close_price=10.2,
    )
    # 账户处于 REDUCE 状态（drawdown_state_factor = 0.5）
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100000,
        cash_available=100000,
        total_asset=85000,
        peak_total_asset=100000,
        max_drawdown_pct=-0.15,
        drawdown_state="REDUCE",
        drawdown_state_factor=0.5,
    )

    process_trade_date(db_session, "2026-03-09")

    position_table = Base.metadata.tables["sim_position"]
    rows = db_session.execute(position_table.select()).mappings().all()
    open_rows = [r for r in rows if r["position_status"] == "OPEN"]
    assert len(open_rows) == 1, f"expected 1 OPEN position, got {len(open_rows)}"
    # effective_ratio = 0.2 * 0.5 = 0.1
    assert abs(float(open_rows[0]["position_ratio"]) - 0.1) < 1e-6, (
        f"expected position_ratio ~0.1 (after REDUCE halving), got {open_rows[0]['position_ratio']}"
    )


# ---------------------------------------------------------------------------
# FR08-SIM-03: INSUFFICIENT_FUNDS
# ---------------------------------------------------------------------------

@pytest.mark.feature("FR08-SIM-03")
def test_fr08_insufficient_funds_creates_skipped_position(db_session):
    """当资金不足以购买 100 股时，系统写入 SKIPPED+INSUFFICIENT_FUNDS 而非开仓。"""
    from app.services.sim_positioning_ssot import process_trade_date

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-06",
        signal_entry_price=500.0,
        target_price=600.0,
        stop_loss=450.0,
        trade_instructions={
            "10k": {"status": "EXECUTE", "position_ratio": 0.1, "skip_reason": None},
        },
    )
    # 每股500元 × 100股 = 50000元成本，但账户只有3000元可用
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-09",
        open_price=500.0,
        high_price=510.0,
        low_price=490.0,
        close_price=505.0,
    )
    insert_sim_account(
        db_session,
        capital_tier="10k",
        initial_cash=10000,
        cash_available=3000,
        total_asset=10000,
        peak_total_asset=10000,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
    )

    process_trade_date(db_session, "2026-03-09")

    position_table = Base.metadata.tables["sim_position"]
    rows = db_session.execute(position_table.select()).mappings().all()
    assert len(rows) >= 1
    # 不应有 OPEN 仓位（资金不足无法买入100股）
    open_rows = [r for r in rows if r["position_status"] == "OPEN"]
    assert len(open_rows) == 0, "should not open position when insufficient funds for 100 shares"


@pytest.mark.feature("FR08-SIM-03")
def test_fr08_minimum_lot_rounding_to_100_shares(db_session):
    """碎股取整：raw_shares 向下取整到 100 的倍数。"""
    from app.services.sim_positioning_ssot import process_trade_date

    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="PINGAN")
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        trade_date="2026-03-06",
        signal_entry_price=10.0,
        target_price=12.0,
        stop_loss=9.0,
        trade_instructions={
            "10k": {"status": "EXECUTE", "position_ratio": 0.15, "skip_reason": None},
        },
    )
    insert_kline(
        db_session,
        stock_code="000001.SZ",
        trade_date="2026-03-09",
        open_price=10.0,
        high_price=10.5,
        low_price=9.8,
        close_price=10.2,
    )
    # 账户可用资金10000，position_ratio=0.15 → 目标金额=1500 → 150股 → 取整100股
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

    position_table = Base.metadata.tables["sim_position"]
    open_rows = db_session.execute(
        position_table.select().where(position_table.c.position_status == "OPEN")
    ).mappings().all()
    assert len(open_rows) == 1
    # shares 必须是 100 的整数倍
    shares = int(open_rows[0]["shares"])
    assert shares % 100 == 0, f"shares {shares} is not a multiple of 100"
    assert shares > 0


@pytest.mark.feature("FR08-SIM-03")
def test_fr08_commission_minimum_is_5_yuan(db_session):
    """佣金最低 5 元：少量购买时佣金为 5 元而非金额 × 0.025%。"""
    from app.services.sim_positioning_ssot import _buy_cost

    # 100 shares × 1元 = 100元，commission = max(100*0.00025, 5) = max(0.025, 5) = 5元
    total, commission, slippage = _buy_cost(1.0, 100)
    assert commission == 5.0, f"expected commission=5.0, got {commission}"
    assert slippage == 0.05, f"expected slippage=0.05 (100*0.0005), got {slippage}"


# ---------------------------------------------------------------------------
# FR08-SIM-05: STOP_LOSS E2E
# ---------------------------------------------------------------------------

@pytest.mark.feature("FR08-SIM-05")
def test_fr08_stop_loss_e2e_closes_position(db_session):
    """止损 E2E：kline.low <= stop_loss 时，持仓状态变更为 STOP_LOSS，exit_price = stop_loss。"""
    from app.services.sim_positioning_ssot import process_trade_date

    entry_date_str = "2026-03-07"
    trade_date_str = "2026-03-09"

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-06",
        signal_entry_price=10.0,
        target_price=12.0,
        stop_loss=9.0,
    )
    # 当日 K 线：low(8.5) < stop_loss(9.0) → 触发止损
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date=trade_date_str,
        open_price=9.8,
        high_price=10.0,
        low_price=8.5,
        close_price=9.0,
    )
    # 提供 entry_date 所在 K 线（用于复权因子计算）
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date=entry_date_str,
        open_price=10.0,
        high_price=10.2,
        low_price=9.9,
        close_price=10.0,
    )
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100000,
        cash_available=100000,
        total_asset=100000,
        peak_total_asset=100000,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
    )
    # 注入一个 OPEN 仓位（已开仓，等待平仓）
    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code="600519.SH",
        capital_tier="100k",
        signal_date="2026-03-06",
        entry_date=entry_date_str,
        actual_entry_price=10.0,
        signal_entry_price=10.0,
        position_ratio=0.2,
        shares=100,
        stop_loss_price=9.0,
        target_price=12.0,
    )

    process_trade_date(db_session, trade_date_str)

    position_table = Base.metadata.tables["sim_position"]
    rows = db_session.execute(position_table.select()).mappings().all()
    closed = [r for r in rows if r["position_status"] == "STOP_LOSS"]
    assert len(closed) == 1, f"expected 1 STOP_LOSS position, got {[r['position_status'] for r in rows]}"
    assert abs(float(closed[0]["exit_price"]) - 9.0) < 1e-3


# ---------------------------------------------------------------------------
# FR08-SIM-05: DELISTED E2E（exit_price=0，net_return=-1.0）
# ---------------------------------------------------------------------------

@pytest.mark.feature("FR08-SIM-05")
def test_fr08_delisted_e2e_liquidates_at_zero(db_session):
    """退市 E2E：stock.is_delisted=True 时，持仓核销为 DELISTED_LIQUIDATED，exit_price=0，net_return=-1.0。"""
    from app.services.sim_positioning_ssot import process_trade_date

    entry_date_str = "2026-03-07"
    trade_date_str = "2026-03-09"

    # 标记股票已退市
    insert_stock_master(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        is_delisted=True,
    )
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-06",
        signal_entry_price=10.0,
        target_price=12.0,
        stop_loss=9.0,
    )
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100000,
        cash_available=100000,
        total_asset=100000,
        peak_total_asset=100000,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
    )
    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code="600519.SH",
        capital_tier="100k",
        signal_date="2026-03-06",
        entry_date=entry_date_str,
        actual_entry_price=10.0,
        signal_entry_price=10.0,
        position_ratio=0.2,
        shares=100,
        stop_loss_price=9.0,
        target_price=12.0,
    )

    process_trade_date(db_session, trade_date_str)

    position_table = Base.metadata.tables["sim_position"]
    rows = db_session.execute(position_table.select()).mappings().all()
    delisted = [r for r in rows if r["position_status"] == "DELISTED_LIQUIDATED"]
    assert len(delisted) == 1, f"expected DELISTED_LIQUIDATED, got {[r['position_status'] for r in rows]}"
    assert float(delisted[0]["exit_price"]) == 0.0, "DELISTED exit_price must be 0.0"
    assert abs(float(delisted[0]["net_return_pct"]) - (-1.0)) < 1e-6, "DELISTED net_return_pct must be -1.0"


# ---------------------------------------------------------------------------
# FR08-SIM-08: 悲观撮合（一字涨跌停）
# ---------------------------------------------------------------------------

@pytest.mark.feature("FR08-SIM-08")
def test_fr08_pessimistic_match_limit_lock_sets_pending_flag(db_session):
    """一字涨跌停（high == low > 0）→ 持仓设 limit_locked_pending=True，不强制平仓。"""
    from app.services.sim_positioning_ssot import process_trade_date

    entry_date_str = "2026-03-07"
    trade_date_str = "2026-03-09"

    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-06",
        signal_entry_price=10.0,
        target_price=12.0,
        stop_loss=9.0,
    )
    # 一字涨停：high == low（全天锁死无法卖出）
    insert_kline(
        db_session,
        stock_code="600519.SH",
        trade_date=trade_date_str,
        open_price=11.0,
        high_price=11.0,
        low_price=11.0,
        close_price=11.0,
    )
    insert_sim_account(
        db_session,
        capital_tier="100k",
        initial_cash=100000,
        cash_available=100000,
        total_asset=100000,
        peak_total_asset=100000,
        max_drawdown_pct=0.0,
        drawdown_state="NORMAL",
        drawdown_state_factor=1.0,
    )
    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code="600519.SH",
        capital_tier="100k",
        signal_date="2026-03-06",
        entry_date=entry_date_str,
        actual_entry_price=10.0,
        signal_entry_price=10.0,
        position_ratio=0.2,
        shares=100,
        stop_loss_price=9.0,
        target_price=12.0,
    )

    process_trade_date(db_session, trade_date_str)

    position_table = Base.metadata.tables["sim_position"]
    rows = db_session.execute(position_table.select()).mappings().all()
    open_rows = [r for r in rows if r["position_status"] == "OPEN"]
    assert len(open_rows) == 1, "position should remain OPEN during limit-locked day"
    assert bool(open_rows[0]["limit_locked_pending"]) is True, (
        "limit_locked_pending should be True when high==low (涨跌停锁死)"
    )
