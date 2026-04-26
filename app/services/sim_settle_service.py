"""模拟持仓日度结算：止损/止盈/超时平仓。"""
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)


def _get_tier_config() -> tuple[list[str], dict[str, float]]:
    """从配置读取资金档位列表与金额。"""
    try:
        raw = getattr(settings, "capital_tiers", "") or "{}"
        tiers_cfg = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        tiers_cfg = {"1w": {"label": "1 万档", "amount": 10000}, "10w": {"label": "10 万档", "amount": 100000}, "50w": {"label": "50 万档", "amount": 500000}}
    tier_list = list(tiers_cfg.keys())
    tier_cap = {k: float((v or {}).get("amount", 100000)) for k, v in tiers_cfg.items()}
    return tier_list, tier_cap


from app.core.db import SessionLocal
from app.models import SimAccount, SimPosition
from app.services.trade_calendar import latest_trade_date_str

_EASTMONEY_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.eastmoney.com/"}
HOLD_DAYS_MAX = {"A": 10, "B": 20, "C": 15}


def _to_secid(stock_code: str) -> str:
    code = stock_code.replace(".SH", "").replace(".SZ", "").strip()
    return f"1.{code}" if code.startswith("6") else f"0.{code}"


def _fetch_quote(stock_code: str) -> dict | None:
    """获取当日收盘价、最高、最低、涨跌停价。"""
    secid = _to_secid(stock_code)
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {"secid": secid, "fields": "f43,f44,f45,f46,f51,f52,f47"}
    try:
        with httpx.Client(timeout=10, headers=_EASTMONEY_HEADERS) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
        data = resp.json().get("data") or {}
        factor = 100.0
        return {
            "close": (data.get("f43") or 0) / factor,
            "high": (data.get("f44") or 0) / factor,
            "low": (data.get("f45") or 0) / factor,
            "limit_up": (data.get("f51") or 0) / factor if data.get("f51") else None,
            "limit_down": (data.get("f52") or 0) / factor if data.get("f52") else None,
            "volume": data.get("f47"),
        }
    except Exception as e:
        logger.warning("sim_settle_fetch_quote_failed stock=%s err=%s", stock_code, e)
        return None


def _calc_hold_days(sim_open_date: str, today: str) -> int:
    """交易日天数（简化：按日期差估算，精确需交易日历）。"""
    try:
        from datetime import date

        d0 = date.fromisoformat(sim_open_date)
        d1 = date.fromisoformat(today)
        return max(0, (d1 - d0).days)
    except Exception:
        return 0


def _calc_pnl(open_price: float, close_price: float, qty: int) -> tuple[float, float, float]:
    """净盈亏（万三佣金 + 0.05% 印花税）。"""
    fee_buy = max(open_price * qty * 0.0003, 5)
    fee_sell = max(close_price * qty * 0.0003, 5)
    stamp = close_price * qty * 0.0005
    gross = (close_price - open_price) * qty
    net = gross - fee_buy - fee_sell - stamp
    pct = (net / (open_price * qty) * 100) if open_price and qty else 0
    return round(gross, 2), round(net, 2), round(pct, 2)


def run_settle() -> dict:
    """
    执行日度结算：遍历 OPEN 持仓，判断平仓条件，更新 sim_position，写入 sim_account。
    返回 {"closed": n, "errors": []}。
    """
    today = latest_trade_date_str()
    db = SessionLocal()
    closed = 0
    errors = []
    try:
        positions = db.query(SimPosition).filter(SimPosition.status == "OPEN").all()
        for pos in positions:
            q = _fetch_quote(pos.stock_code)
            if not q:
                errors.append(f"quote_failed:{pos.stock_code}")
                continue
            close_px = q["close"]
            high_px = q["high"]
            low_px = q["low"]
            limit_up = q.get("limit_up")
            limit_down = q.get("limit_down")
            entry = pos.actual_entry_price if pos.actual_entry_price is not None else pos.sim_open_price
            hold_days = _calc_hold_days(pos.sim_open_date, today)
            max_hold = HOLD_DAYS_MAX.get(pos.strategy_type, 15)
            valid_until = pos.valid_until or "9999-12-31"

            new_status = None
            close_price = close_px

            if low_px <= pos.stop_loss_price:
                if limit_down and close_px <= limit_down + 0.01:
                    close_price = limit_down
                    pos.close_blocked = True
                else:
                    close_price = pos.stop_loss_price
                new_status = "CLOSED_SL"
            elif pos.target_price_2 and high_px >= pos.target_price_2:
                close_price = pos.target_price_2
                new_status = "CLOSED_T2"
            elif pos.target_price_1 and hold_days >= 5 and high_px >= pos.target_price_1:
                close_price = pos.target_price_1
                new_status = "CLOSED_T1"
            elif today >= valid_until or hold_days >= max_hold:
                if limit_down and close_px <= limit_down + 0.01:
                    pos.close_blocked = True
                    close_price = limit_down
                new_status = "CLOSED_EXPIRED"

            if new_status:
                gross, net, pct = _calc_pnl(entry, close_price, pos.sim_qty)
                pos.status = new_status
                pos.sim_close_date = today
                pos.sim_close_price = close_price
                pos.sim_pnl_gross = gross
                pos.sim_pnl_net = net
                pos.sim_pnl_pct = pct
                pos.hold_days = hold_days
                closed += 1

        db.commit()

        tier_list, _ = _get_tier_config()
        for tier in tier_list:
            _write_account_snapshot(db, today, tier)

        db.commit()
    except Exception as e:
        db.rollback()
        logger.exception("sim_settle_failed err=%s", e)
        errors.append(str(e))
    finally:
        db.close()

    return {"closed": closed, "errors": errors}


def _write_account_snapshot(db, snapshot_date: str, capital_tier: str) -> None:
    """写入 sim_account 日度快照（简化版）。"""
    positions = db.query(SimPosition).filter(SimPosition.capital_tier == capital_tier).all()
    open_pos = [p for p in positions if p.status == "OPEN"]
    closed_pos = [p for p in positions if p.status and p.status.startswith("CLOSED")]

    _, tier_cap = _get_tier_config()
    initial = float(tier_cap.get(capital_tier, 100000))
    cash = initial
    position_value = 0.0
    for p in open_pos:
        q = _fetch_quote(p.stock_code)
        if q:
            position_value += q["close"] * p.sim_qty
        else:
            position_value += (p.actual_entry_price or p.sim_open_price) * p.sim_qty
    total_asset = cash + position_value
    daily_return = 0.0
    cum_return = (total_asset - initial) / initial * 100 if initial else 0
    from sqlalchemy import func

    peak_row = (
        db.query(func.max(SimAccount.total_asset).label("m"))
        .filter(SimAccount.capital_tier == capital_tier, SimAccount.snapshot_date <= snapshot_date)
        .scalar()
    )
    peak_asset = peak_row if peak_row is not None else total_asset
    max_dd = ((total_asset - peak_asset) / peak_asset * 100) if peak_asset and peak_asset > 0 else 0
    if max_dd > 0:
        max_dd = 0

    wins = [p for p in closed_pos if p.sim_pnl_net and p.sim_pnl_net > 0]
    settled = len(closed_pos)
    win_rate = len(wins) / settled if settled else None
    pnl_ratio = None
    if settled >= 30:
        losses = [p for p in closed_pos if p.sim_pnl_net and p.sim_pnl_net <= 0]
        avg_win = sum(p.sim_pnl_net for p in wins) / len(wins) if wins else 0
        avg_loss = abs(sum(p.sim_pnl_net for p in losses) / len(losses)) if losses else 1
        pnl_ratio = avg_win / avg_loss if avg_loss else None

    dd_state = "NORMAL"
    if max_dd <= settings.max_drawdown_halt_threshold * 100:
        dd_state = "HALT"
    elif max_dd <= settings.max_drawdown_reduce_threshold * 100:
        dd_state = "REDUCE"

    existing = (
        db.query(SimAccount)
        .filter(SimAccount.snapshot_date == snapshot_date, SimAccount.capital_tier == capital_tier)
        .first()
    )
    if existing:
        existing.total_asset = total_asset
        existing.cash = cash
        existing.position_value = position_value
        existing.daily_return_pct = daily_return
        existing.cumulative_return_pct = cum_return
        existing.max_drawdown_pct = max_dd
        existing.drawdown_state = dd_state
        existing.open_positions = len(open_pos)
        existing.settled_trades = settled
        existing.win_rate = win_rate
        existing.pnl_ratio = pnl_ratio
    else:
        db.add(
            SimAccount(
                snapshot_date=snapshot_date,
                capital_tier=capital_tier,
                initial_capital=initial,
                total_asset=total_asset,
                cash=cash,
                position_value=position_value,
                daily_return_pct=daily_return,
                cumulative_return_pct=cum_return,
                hs300_daily_pct=None,
                hs300_cum_pct=None,
                alpha_pct=None,
                max_drawdown_pct=max_dd,
                drawdown_state=dd_state,
                open_positions=len(open_pos),
                settled_trades=settled,
                win_rate=win_rate,
                pnl_ratio=pnl_ratio,
            )
        )
