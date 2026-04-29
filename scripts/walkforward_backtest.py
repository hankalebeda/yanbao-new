"""
Walk-Forward 历史回测脚本

设计来源：docs/core/12_模拟实盘追踪设计.md §6.0.1
用途：从通达信读取历史日线，按 A/B/C 信号规则生成信号并结算，结果写入 sim_position_backtest。

用法：
  python scripts/walkforward_backtest.py --start-date 2023-01-01 --end-date 2025-12-31
  python scripts/walkforward_backtest.py --stock-codes 600519.SH,000001.SZ --capital-tier 10w
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.config import settings
from app.core.db import SessionLocal
from app.models import SimPositionBacktest
from app.services.tdx_local_data import load_tdx_day_records
from app.services.trade_calendar import next_trade_date_str, trade_date_after_n_days, trade_days_in_range

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HOLD_DAYS_MAX = {"A": 10, "B": 20, "C": 15}
SIGNAL_CONFIG = {
    "A": {"atr_multiplier": 1.5, "valid_days": 2, "sim_open_multiplier": 1.045},
    "B": {"atr_multiplier": 2.0, "valid_days": 3, "sim_open_multiplier": 1.025},
    "C": {"atr_multiplier": 2.5, "valid_days": 5, "sim_open_multiplier": 1.025},
}


def _norm_date(raw: str) -> str:
    s = str(raw or "").strip().replace("-", "")
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return ""


def _calc_ma(closes: list[float], n: int) -> float | None:
    if len(closes) < n:
        return None
    return sum(closes[-n:]) / n


def _calc_atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 0.0
    tr_list = []
    for i in range(1, len(closes)):
        h, l_, prev = highs[i], lows[i], closes[i - 1]
        tr_list.append(max(h - l_, abs(h - prev), abs(l_ - prev)))
    return sum(tr_list[-period:]) / period if tr_list else 0.0


def _calc_volatility_20d(closes: list[float]) -> float:
    if len(closes) < 21:
        return 0.0
    rets = []
    for i in range(len(closes) - 20, len(closes) - 1):
        if closes[i] != 0:
            rets.append((closes[i + 1] - closes[i]) / closes[i])
    if not rets:
        return 0.0
    mean = sum(rets) / len(rets)
    return (sum((r - mean) ** 2 for r in rets) / len(rets)) ** 0.5


def _calc_volume_ratio(volumes: list[int]) -> float:
    if len(volumes) < 21:
        return 1.0
    avg20 = sum(volumes[-21:-1]) / 20.0 if sum(volumes[-21:-1]) > 0 else 0
    return volumes[-1] / avg20 if avg20 > 0 else 1.0


def _classify_signal(vol_rank: float) -> str:
    """按波动率分位分配类型：高四分位→A，低四分位→C，其余→B（12 §6.0.1）。"""
    if vol_rank >= 0.75:
        return "A"
    if vol_rank <= 0.25:
        return "C"
    return "B"


def _calc_pnl(open_price: float, close_price: float, qty: int) -> tuple[float, float, float]:
    fee_buy = max(open_price * qty * 0.0003, 5)
    fee_sell = max(close_price * qty * 0.0003, 5)
    stamp = close_price * qty * 0.0005
    gross = (close_price - open_price) * qty
    net = gross - fee_buy - fee_sell - stamp
    pct = (net / (open_price * qty) * 100) if open_price and qty else 0
    return round(gross, 2), round(net, 2), round(pct, 2)


@dataclass
class OpenPosition:
    report_id: str
    stock_code: str
    stock_name: str
    strategy_type: str
    signal_date: str
    sim_open_date: str
    sim_open_price: float
    sim_qty: int
    capital_tier: str
    stop_loss_price: float
    target_price_1: float | None
    target_price_2: float | None
    valid_until: str
    open_idx: int  # index in trade_days when opened


def _get_stock_pool(stock_codes_arg: str | None) -> list[str]:
    if stock_codes_arg:
        return [c.strip() for c in stock_codes_arg.split(",") if c.strip()]
    return [c.strip() for c in (getattr(settings, "stock_pool", "") or "").split(",") if c.strip()]


def _build_kline_by_date(records: list[dict]) -> dict[str, dict]:
    out = {}
    for r in records:
        d = _norm_date(r.get("date"))
        if d:
            out[d] = r
    return out


def _empty_stats() -> dict:
    return {
        "closed_count": 0,
        "win_rate": 0,
        "pnl_ratio": None,
        "total_pnl_net": 0,
        "annualized_pct": 0,
    }


def run_backtest(
    start_date: str,
    end_date: str,
    stock_codes: list[str],
    capital_tier: str,
) -> tuple[list[dict], dict]:
    """执行回测，返回已平仓记录列表和统计 dict。"""
    trade_days = trade_days_in_range(start_date, end_date)
    if not trade_days:
        logger.warning("无交易日，请检查 start/end 或 TDX 数据")
        return [], _empty_stats()

    # 预加载各股票 K 线（含 start 前 60 日以计算指标）
    d0 = trade_days[0]
    try:
        from datetime import date, timedelta
        pre_start = (date.fromisoformat(d0) - timedelta(days=120)).isoformat()
    except Exception:
        pre_start = d0
    all_days = trade_days_in_range(pre_start, end_date)
    klines: dict[str, dict[str, dict]] = {}
    vol_ranks: dict[str, float] = {}
    for code in stock_codes:
        rows = load_tdx_day_records(code, limit=500)
        if len(rows) < 30:
            continue
        klines[code] = _build_kline_by_date(rows)
        closes = [r["close"] for r in rows]
        vol = _calc_volatility_20d(closes)
        vol_ranks[code] = vol
    if vol_ranks:
        sorted_vols = sorted(vol_ranks.values())
        for code in vol_ranks:
            idx = sorted_vols.index(vol_ranks[code])
            vol_ranks[code] = (idx + 0.5) / len(sorted_vols) if sorted_vols else 0.5

    open_positions: list[OpenPosition] = []
    closed_records: list[dict] = []
    tier_cap = {"1w": 10000, "10w": 100000, "50w": 500000}
    initial = float(tier_cap.get(capital_tier, 100000))

    for i, t in enumerate(trade_days):
        # 1. 结算已有持仓
        still_open = []
        for pos in open_positions:
            if pos.sim_open_date > t:
                still_open.append(pos)
                continue
            k = klines.get(pos.stock_code, {})
            row = k.get(t)
            if not row:
                still_open.append(pos)
                continue
            high_px = row["high"]
            low_px = row["low"]
            close_px = row["close"]
            open_px = pos.sim_open_price
            hold_days = sum(1 for j in range(i) if trade_days[j] >= pos.sim_open_date and trade_days[j] <= t) - 1
            hold_days = max(0, hold_days)
            cfg = SIGNAL_CONFIG.get(pos.strategy_type, SIGNAL_CONFIG["B"])
            max_hold = HOLD_DAYS_MAX.get(pos.strategy_type, 15)
            valid_until = pos.valid_until or "9999-12-31"

            new_status = None
            close_price = close_px
            close_blocked = False

            if low_px <= pos.stop_loss_price:
                close_price = pos.stop_loss_price
                new_status = "CLOSED_SL"
            elif pos.target_price_2 and high_px >= pos.target_price_2:
                close_price = pos.target_price_2
                new_status = "CLOSED_T2"
            elif pos.target_price_1 and hold_days >= 5 and high_px >= pos.target_price_1:
                close_price = pos.target_price_1
                new_status = "CLOSED_T1"
            elif t >= valid_until or hold_days >= max_hold:
                close_price = close_px
                new_status = "CLOSED_EXPIRED"

            if new_status:
                gross, net, pct = _calc_pnl(open_px, close_price, pos.sim_qty)
                closed_records.append({
                    "report_id": pos.report_id,
                    "stock_code": pos.stock_code,
                    "stock_name": pos.stock_name,
                    "strategy_type": pos.strategy_type,
                    "signal_date": pos.signal_date,
                    "sim_open_date": pos.sim_open_date,
                    "sim_open_price": open_px,
                    "sim_qty": pos.sim_qty,
                    "capital_tier": pos.capital_tier,
                    "stop_loss_price": pos.stop_loss_price,
                    "target_price_1": pos.target_price_1,
                    "target_price_2": pos.target_price_2,
                    "valid_until": pos.valid_until,
                    "status": new_status,
                    "close_blocked": close_blocked,
                    "sim_close_date": t,
                    "sim_close_price": close_price,
                    "sim_pnl_gross": gross,
                    "sim_pnl_net": net,
                    "sim_pnl_pct": pct,
                    "hold_days": hold_days,
                })
            else:
                still_open.append(pos)
        open_positions = still_open

        # 2. 生成新信号（B 类：MA5>MA20 且 量比>1.0；A/C 按波动率分位）
        open_stocks = {p.stock_code for p in open_positions}
        for code in stock_codes:
            if code in open_stocks:
                continue
            k = klines.get(code, {})
            row = k.get(t)
            if not row:
                continue
            rows_list = [(d, v) for d, v in k.items() if d <= t]
            rows_list.sort(key=lambda x: x[0])
            rows_sorted = [x[1] for x in rows_list]
            if len(rows_sorted) < 25:
                continue
            closes = [r["close"] for r in rows_sorted]
            highs = [r["high"] for r in rows_sorted]
            lows = [r["low"] for r in rows_sorted]
            vols = [int(r.get("volume", 0) or 0) for r in rows_sorted]

            ma5 = _calc_ma(closes, 5)
            ma20 = _calc_ma(closes, 20)
            vol_ratio = _calc_volume_ratio(vols)
            vol_rank = vol_ranks.get(code, 0.5)

            # 最小实现：仅 MA5>MA20 且 量比>1.0 时产生 BUY；类型按波动率分位（12 §6.0.1）
            if ma5 is None or ma20 is None or ma5 <= ma20 or vol_ratio <= 1.0:
                continue
            stype = _classify_signal(vol_rank)

            cfg = SIGNAL_CONFIG.get(stype, SIGNAL_CONFIG["B"])
            atr14 = _calc_atr(highs, lows, closes, 14)
            close_px = closes[-1]
            stop_loss = max(close_px - atr14 * cfg["atr_multiplier"], close_px * 0.90)
            atr_risk = close_px - stop_loss
            t1 = min(close_px + atr_risk * 1.5, close_px * 1.30)
            t2 = min(close_px + atr_risk * 2.5, close_px * 1.50)
            sim_open = round(close_px * cfg["sim_open_multiplier"], 2)
            valid_days = cfg["valid_days"]
            sim_open_date = next_trade_date_str(t)
            valid_until = trade_date_after_n_days(t, valid_days + 1)

            qty = int(initial * 0.2 / sim_open / 100) * 100
            qty = max(100, qty)
            report_id = f"wf_{code}_{t.replace('-','')}"

            pos = OpenPosition(
                report_id=report_id,
                stock_code=code,
                stock_name=code,
                strategy_type=stype,
                signal_date=t,
                sim_open_date=sim_open_date,
                sim_open_price=sim_open,
                sim_qty=qty,
                capital_tier=capital_tier,
                stop_loss_price=round(stop_loss, 2),
                target_price_1=round(t1, 2),
                target_price_2=round(t2, 2),
                valid_until=valid_until,
                open_idx=i,
            )
            open_positions.append(pos)

    wins = [r for r in closed_records if r.get("sim_pnl_net", 0) > 0]
    settled = len(closed_records)
    win_rate = len(wins) / settled if settled else 0
    losses = [r for r in closed_records if r.get("sim_pnl_net", 0) <= 0]
    avg_win = sum(r["sim_pnl_net"] for r in wins) / len(wins) if wins else 0
    avg_loss = abs(sum(r["sim_pnl_net"] for r in losses) / len(losses)) if losses else 1
    pnl_ratio = avg_win / avg_loss if avg_loss else None
    total_pnl = sum(r.get("sim_pnl_net", 0) for r in closed_records)
    years = max(0.01, len(trade_days) / 250.0) if trade_days else 1
    annualized = (total_pnl / initial / years * 100) if initial else 0

    stats = {
        "closed_count": settled,
        "win_rate": round(win_rate * 100, 2),
        "pnl_ratio": round(pnl_ratio, 2) if pnl_ratio else None,
        "total_pnl_net": round(total_pnl, 2),
        "annualized_pct": round(annualized, 2),
    }
    return closed_records, stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Walk-Forward 历史回测")
    parser.add_argument("--start-date", default="2023-01-01", help="回测起始日")
    parser.add_argument("--end-date", default="2025-12-31", help="回测截止日")
    parser.add_argument("--stock-codes", default=None, help="逗号分隔股票代码，默认 STOCK_POOL")
    parser.add_argument("--capital-tier", default="10w", choices=("1w", "10w", "50w"))
    parser.add_argument("--output-json", default=None, help="可选：输出 JSON 报告路径")
    args = parser.parse_args()

    stocks = _get_stock_pool(args.stock_codes)
    if not stocks:
        logger.error("股票池为空，请设置 --stock-codes 或 STOCK_POOL")
        return 1

    logger.info("回测 %s ~ %s，股票 %s，资金档 %s", args.start_date, args.end_date, stocks, args.capital_tier)
    closed, stats = run_backtest(
        args.start_date, args.end_date, stocks, args.capital_tier
    )

    db = SessionLocal()
    try:
        for r in closed:
            db.add(
                SimPositionBacktest(
                    report_id=r["report_id"],
                    stock_code=r["stock_code"],
                    stock_name=r.get("stock_name"),
                    strategy_type=r["strategy_type"],
                    signal_date=r["signal_date"],
                    sim_open_date=r["sim_open_date"],
                    sim_open_price=r["sim_open_price"],
                    actual_entry_price=None,
                    sim_qty=r["sim_qty"],
                    capital_tier=r["capital_tier"],
                    stop_loss_price=r["stop_loss_price"],
                    target_price_1=r.get("target_price_1"),
                    target_price_2=r.get("target_price_2"),
                    valid_until=r.get("valid_until"),
                    status=r["status"],
                    close_blocked=r.get("close_blocked", False),
                    sim_close_date=r.get("sim_close_date"),
                    sim_close_price=r.get("sim_close_price"),
                    sim_pnl_gross=r.get("sim_pnl_gross"),
                    sim_pnl_net=r.get("sim_pnl_net"),
                    sim_pnl_pct=r.get("sim_pnl_pct"),
                    hold_days=r.get("hold_days"),
                    source="walkforward",
                )
            )
        db.commit()
    except Exception as e:
        db.rollback()
        logger.exception("写入 sim_position_backtest 失败: %s", e)
        return 1
    finally:
        db.close()

    print(f"回测完成: 平仓 {stats['closed_count']} 笔 | 胜率 {stats['win_rate']}% | 盈亏比 {stats['pnl_ratio']} | 净盈亏 {stats['total_pnl_net']} | 年化 {stats['annualized_pct']}%")

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({"records": closed, "stats": stats}, f, ensure_ascii=False, indent=2)
        logger.info("报告已保存: %s", out_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
