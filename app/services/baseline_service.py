"""E8 对照组基线服务：随机/MA金叉基线生成与结算。"""
from __future__ import annotations

import logging
import random
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from app.core.config import settings
from app.models import SimBaseline
from app.services.tdx_local_data import load_tdx_day_records
from app.services.trade_calendar import latest_trade_date_str, trade_date_after_n_days

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def _stock_pool() -> list[str]:
    raw = getattr(settings, "stock_pool", None) or ""
    return [x.strip() for x in raw.split(",") if x.strip()]


def _date_to_yyyymmdd(s: str) -> str:
    return s.replace("-", "") if s else ""


def _get_close_for_date(stock_code: str, trade_date: str) -> float | None:
    """获取指定交易日的收盘价。"""
    records = load_tdx_day_records(stock_code, limit=400)
    target = _date_to_yyyymmdd(trade_date)
    for r in reversed(records):
        if _date_to_yyyymmdd(str(r.get("date", ""))) == target:
            return float(r["close"]) if r.get("close") is not None else None
    return None


def generate_random_baseline(db: Session, trade_date: str | None = None) -> bool:
    """
    E8.2 随机基线：从股票池随机选 1 只，以当日收盘价为开仓价，持仓 10 日（不设止损）。
    """
    pool = _stock_pool()
    if not pool:
        return False
    trade_date = trade_date or latest_trade_date_str()
    stock_code = random.choice(pool)
    open_price = _get_close_for_date(stock_code, trade_date)
    if open_price is None:
        logger.warning("baseline_random_skip stock=%s no_close trade_date=%s", stock_code, trade_date)
        return False
    row = SimBaseline(
        baseline_type="random",
        trade_date=trade_date,
        stock_code=stock_code,
        open_price=open_price,
        hold_days=10,
    )
    db.add(row)
    db.commit()
    logger.info("baseline_random_created stock=%s trade_date=%s open=%.2f", stock_code, trade_date, open_price)
    return True


def generate_ma_cross_baseline(db: Session, trade_date: str | None = None) -> int:
    """
    E8.3 MA 金叉基线：对 MA5>MA20 且量比>1.0 的股票生成 BUY，仓位 20%，持 10 日。
    返回生成笔数。
    """
    trade_date = trade_date or latest_trade_date_str()
    pool = _stock_pool()
    created = 0
    for stock_code in pool:
        records = load_tdx_day_records(stock_code, limit=100)
        # 筛选 <= trade_date 的记录，按日期排序
        target = _date_to_yyyymmdd(trade_date)
        rows = [r for r in records if _date_to_yyyymmdd(str(r.get("date", ""))) <= target]
        rows.sort(key=lambda x: str(x.get("date", "")))
        if len(rows) < 20:
            continue
        last20 = rows[-20:]
        closes = [float(r["close"]) for r in last20 if r.get("close") is not None]
        vols = [int(r.get("volume", 0) or 0) for r in last20]
        if len(closes) < 20 or len(vols) < 20:
            continue
        ma5 = sum(closes[-5:]) / 5
        ma20 = sum(closes) / 20
        vol5 = sum(vols[-5:]) / 5 if vols[-5:] else 0
        vol20 = sum(vols) / 20 if vols else 0
        vol_ratio = vol5 / vol20 if vol20 > 0 else 0
        if ma5 <= ma20 or vol_ratio <= 1.0:
            continue
        open_price = closes[-1]
        row = SimBaseline(
            baseline_type="ma_cross",
            trade_date=trade_date,
            stock_code=stock_code,
            open_price=open_price,
            hold_days=10,
        )
        db.add(row)
        created += 1
    if created:
        db.commit()
        logger.info("baseline_ma_cross_created count=%d trade_date=%s", created, trade_date)
    return created


def settle_baselines(db: Session, as_of_date: str | None = None) -> int:
    """
    结算到期的基线持仓：close_price、pnl_pct 填充。
    返回结算笔数。
    """
    as_of_date = as_of_date or latest_trade_date_str()
    rows = db.query(SimBaseline).filter(
        SimBaseline.close_price.is_(None),
        SimBaseline.open_price.isnot(None),
        SimBaseline.hold_days.isnot(None),
    ).all()
    closed = 0
    for r in rows:
        close_date = trade_date_after_n_days(r.trade_date, r.hold_days or 0)
        if not close_date or close_date > as_of_date:
            continue
        close_price = _get_close_for_date(r.stock_code or "", close_date) if r.stock_code else None
        if close_price is None:
            continue
        r.close_price = close_price
        if r.open_price and r.open_price != 0:
            r.pnl_pct = round((close_price - r.open_price) / r.open_price * 100, 2)
        db.merge(r)
        closed += 1
    if closed:
        db.commit()
        logger.info("baseline_settled count=%d as_of=%s", closed, as_of_date)
    return closed
