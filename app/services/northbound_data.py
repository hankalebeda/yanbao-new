"""北向资金个股数据（akshare stock_hsgt_individual_em）"""
from __future__ import annotations

import logging
from typing import Any

from app.core.proxy_utils import bypass_proxy

logger = logging.getLogger(__name__)


def _to_symbol(stock_code: str) -> str:
    """akshare 需要 6 位代码，如 600519"""
    code = (stock_code or "").strip().split(".")[0]
    return code if len(code) == 6 else ""


def _safe_float(v: Any) -> float | None:
    try:
        if v in (None, "", "-"):
            return None
        return float(v)
    except Exception:
        return None


def _sum_window(vals: list[float | None], n: int) -> float | None:
    arr = [x for x in vals[-n:] if isinstance(x, (int, float))]
    if not arr:
        return None
    return round(float(sum(arr)), 4)


def _streak_sign(vals: list[float | None]) -> int:
    streak = 0
    for v in reversed(vals):
        if not isinstance(v, (int, float)):
            break
        if v > 0:
            if streak >= 0:
                streak += 1
            else:
                break
        elif v < 0:
            if streak <= 0:
                streak -= 1
            else:
                break
        else:
            break
    return streak


def fetch_northbound_summary(stock_code: str) -> dict | None:
    """
    获取北向资金个股汇总。失败时返回 None（调用方保持 status: missing）。
    调用前会自动执行 bypass_proxy()。
    """
    symbol = _to_symbol(stock_code)
    if not symbol:
        return None
    try:
        bypass_proxy()
        import akshare as ak  # noqa: PLC0415
        df = ak.stock_hsgt_individual_em(symbol=symbol)
    except ImportError:
        logger.debug("akshare not installed, northbound data skipped")
        return None
    except Exception as e:
        logger.warning("northbound_fetch_failed stock_code=%s err=%s", stock_code, e)
        return None
    if df is None or len(df) == 0:
        return None
    cols = list(df.columns)
    if "今日增持资金" not in cols or "持股日期" not in cols:
        logger.warning("northbound_columns_missing stock_code=%s cols=%s", stock_code, cols)
        return None
    df_sorted = df.sort_values("持股日期", ascending=True)
    net_vals = [_safe_float(v) for v in df_sorted["今日增持资金"].tolist()]
    return {
        "status": "ok",
        "reason": "akshare_stock_hsgt_individual_em",
        "net_inflow_1d": _sum_window(net_vals, 1),
        "net_inflow_3d": _sum_window(net_vals, 3),
        "net_inflow_5d": _sum_window(net_vals, 5),
        "net_inflow_10d": _sum_window(net_vals, 10),
        "net_inflow_20d": _sum_window(net_vals, 20),
        "streak_days": _streak_sign(net_vals),
        "history_records": len(df),
    }
