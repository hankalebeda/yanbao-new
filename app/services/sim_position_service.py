"""模拟持仓服务：T+1开盘价采集、create_position 开仓等。"""
import logging

import httpx

from app.core.db import SessionLocal
from app.models import SimPosition
from app.services.sim_settle_service import HOLD_DAYS_MAX
from app.services.trade_calendar import next_trade_date_str

logger = logging.getLogger(__name__)

_EASTMONEY_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.eastmoney.com/"}


def _to_secid(stock_code: str) -> str:
    code = stock_code.replace(".SH", "").replace(".SZ", "").strip()
    if code.startswith("6"):
        return f"1.{code}"
    return f"0.{code}"


def update_open_prices() -> int:
    """
    更新所有 actual_entry_price 为空的 OPEN 持仓的 T+1 开盘价。
    若开盘即涨停则标记 execution_blocked=True。
    返回更新数量。
    """
    db = SessionLocal()
    updated = 0
    try:
        positions = (
            db.query(SimPosition)
            .filter(SimPosition.status == "OPEN", SimPosition.actual_entry_price.is_(None))
            .all()
        )
        for pos in positions:
            secid = _to_secid(pos.stock_code)
            url = "https://push2.eastmoney.com/api/qt/stock/get"
            params = {"secid": secid, "fields": "f46,f51,f58"}
            try:
                with httpx.Client(timeout=10, headers=_EASTMONEY_HEADERS) as client:
                    resp = client.get(url, params=params)
                    resp.raise_for_status()
                data = resp.json().get("data") or {}
                open_price = data.get("f46")
                limit_up = data.get("f51")
                if open_price is None or open_price == "-":
                    logger.warning("sim_open_price_missing stock=%s", pos.stock_code)
                    continue
                open_val = float(open_price) / 100.0
                limit_val = (float(limit_up) / 100.0) if limit_up and limit_up != "-" else None
                if limit_val is not None and abs(open_val - limit_val) < 0.01:
                    pos.execution_blocked = True
                    logger.info("sim_open_price_blocked stock=%s reason=limit_up", pos.stock_code)
                else:
                    pos.actual_entry_price = open_val
                updated += 1
            except Exception as e:
                logger.warning("sim_open_price_fetch_failed stock=%s err=%s", pos.stock_code, e)
        db.commit()
    finally:
        db.close()
    return updated


def create_position(
    db,
    report_id: str,
    stock_code: str,
    stock_name: str | None,
    signal_date: str,
    instruction: dict,
    capital_tier: str = "10w",
) -> SimPosition | None:
    """
    根据研报实操指令创建模拟持仓（模拟实盘追踪设计 §3.5）。
    条件：recommendation==BUY、confidence>=0.65、filtered_out==False、drawdown_state!=HALT 由调用方检查。
    """
    sim_open_price = instruction.get("sim_open_price")
    stop_loss_price = instruction.get("stop_loss_price")
    target_price_1 = instruction.get("target_price_1")
    target_price_2 = instruction.get("target_price_2")
    strategy_type = instruction.get("strategy_type", "B")
    sim_qty = int(instruction.get("sim_qty", 100))
    valid_until = instruction.get("valid_until")
    if not all(isinstance(x, (int, float)) and x > 0 for x in (sim_open_price, stop_loss_price)):
        logger.warning("create_position_skip report=%s missing_price", report_id)
        return None
    sim_open_date = next_trade_date_str(signal_date)
    if not valid_until:
        from datetime import date, timedelta
        hold_max = HOLD_DAYS_MAX.get(strategy_type, 15)
        try:
            d = date.fromisoformat(sim_open_date)
            valid_until = (d + timedelta(days=hold_max + 5)).isoformat()
        except Exception:
            valid_until = "9999-12-31"
    pos = SimPosition(
        report_id=report_id,
        stock_code=stock_code,
        stock_name=stock_name or stock_code,
        strategy_type=strategy_type,
        signal_date=signal_date,
        sim_open_date=sim_open_date,
        sim_open_price=float(sim_open_price),
        actual_entry_price=None,
        sim_qty=max(100, (sim_qty // 100) * 100),
        capital_tier=capital_tier,
        stop_loss_price=float(stop_loss_price),
        target_price_1=float(target_price_1) if target_price_1 else None,
        target_price_2=float(target_price_2) if target_price_2 else None,
        valid_until=valid_until,
        status="OPEN",
        execution_blocked=False,
    )
    db.add(pos)
    db.flush()
    logger.info("sim_position_created report_id=%s stock=%s strategy=%s", report_id, stock_code, strategy_type)
    return pos
