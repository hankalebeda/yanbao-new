import asyncio
from datetime import datetime, timezone
from typing import Any

import httpx

from app.core.config import settings
from app.services.source_state import record_source_result, should_skip_source

_EASTMONEY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/",
}

# Windows 系统可能配置了本地代理（如 Clash/V2Ray 127.0.0.1:10808），
# 该代理会 reset 对东方财富等中国金融数据接口的 SSL 连接。
# 使用 trust_env=False 强制直连，绕过系统代理。
_HTTPX_DIRECT = {"trust_env": False}


def _to_secid(stock_code: str) -> str:
    code = stock_code.split(".")[0]
    if code.startswith("6"):
        return f"1.{code}"
    return f"0.{code}"


def _to_tdx_market(stock_code: str) -> int:
    code = stock_code.split(".")[0]
    return 1 if code.startswith("6") else 0


def _safe_float(value: str | float | int | None) -> float | None:
    try:
        if value is None or value == "-":
            return None
        return float(value)
    except Exception:
        return None


def _ma(values: list[float], n: int) -> float | None:
    if len(values) < n:
        return None
    segment = values[-n:]
    return round(sum(segment) / float(n), 4)


def _err_text(exc: Exception) -> str:
    s = str(exc).strip()
    return s if s else exc.__class__.__name__


async def _fetch_eastmoney_quote(stock_code: str) -> dict[str, Any] | None:
    secid = _to_secid(stock_code)
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "secid": secid,
        "fields": "f43,f44,f45,f46,f47,f48,f57,f58,f170",
    }
    async with httpx.AsyncClient(timeout=10, headers=_EASTMONEY_HEADERS, **_HTTPX_DIRECT) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json().get("data") or {}
        last = _safe_float(payload.get("f43"))
        high = _safe_float(payload.get("f44"))
        low = _safe_float(payload.get("f45"))
        open_price = _safe_float(payload.get("f46"))
        volume = _safe_float(payload.get("f47"))
        amount = _safe_float(payload.get("f48"))
        pct = _safe_float(payload.get("f170"))
        # Eastmoney push2 quote f43/f44/f45/f46 are in fen (cent) units — always divide by 100.
        # The old heuristic (> 500 → /100) fails for stocks priced exactly above ¥5.
        factor = 100.0
        return {
            "stock_code": stock_code,
            "name": payload.get("f58") or stock_code,
            "last_price": (last / factor) if last is not None else None,
            "open_price": (open_price / factor) if open_price is not None else None,
            "high_price": (high / factor) if high is not None else None,
            "low_price": (low / factor) if low is not None else None,
            "pct_change": (pct / 100.0) if pct is not None and abs(pct) > 1 else pct,
            "volume": volume,
            "amount": amount,
            "source": "eastmoney",
            "fetch_time": datetime.now(timezone.utc).isoformat(),
        }


def _fetch_tdx_quote(stock_code: str) -> dict[str, Any] | None:
    try:
        from mootdx.quotes import Quotes  # type: ignore
    except Exception:
        raise RuntimeError("mootdx_not_installed")

    code = str(stock_code.split(".")[0])
    client = Quotes.factory(market="std")
    rows = client.quotes(symbol=[code])
    if rows is None or len(rows) == 0:
        raise RuntimeError("tdx_empty_quote")

    row = rows.iloc[0]
    return {
        "stock_code": stock_code,
        "name": row.get("name", stock_code),
        "last_price": float(row.get("price", 0) or 0),
        "open_price": float(row.get("open", 0) or 0),
        "high_price": float(row.get("high", 0) or 0),
        "low_price": float(row.get("low", 0) or 0),
        "pct_change": float(row.get("zd", 0) or 0) / 100.0,
        "volume": float(row.get("vol", 0) or 0),
        "amount": float(row.get("amount", 0) or 0),
        "source": "tdx",
        "fetch_time": datetime.now(timezone.utc).isoformat(),
    }


def _pick_selected_snapshot(providers: list[str], snapshots: dict[str, dict | None]) -> tuple[dict | None, str | None]:
    for provider in providers:
        item = snapshots.get(provider)
        if item and item.get("last_price") is not None:
            return item, provider
    for provider in ("eastmoney", "tdx"):
        item = snapshots.get(provider)
        if item and item.get("last_price") is not None:
            return item, provider
    return None, None


def _build_dual_comparison(eastmoney: dict | None, tdx: dict | None) -> dict:
    out = {
        "both_available": False,
        "eastmoney_price": eastmoney.get("last_price") if eastmoney else None,
        "tdx_price": tdx.get("last_price") if tdx else None,
        "last_price_diff_abs": None,
        "last_price_diff_pct": None,
        "price_consistent": None,
    }
    if not eastmoney or not tdx:
        return out
    e = eastmoney.get("last_price")
    t = tdx.get("last_price")
    if not isinstance(e, (int, float)) or not isinstance(t, (int, float)):
        return out
    diff_abs = abs(float(e) - float(t))
    base = abs(float(e)) if abs(float(e)) > 1e-9 else 1.0
    diff_pct = diff_abs / base
    out.update(
        {
            "both_available": True,
            "last_price_diff_abs": round(diff_abs, 6),
            "last_price_diff_pct": round(diff_pct, 6),
            "price_consistent": diff_pct <= 0.003,
        }
    )
    return out


def _quote_view(v: dict | None) -> dict | None:
    if not v:
        return None
    return {
        "stock_code": v.get("stock_code"),
        "name": v.get("name"),
        "last_price": v.get("last_price"),
        "open_price": v.get("open_price"),
        "high_price": v.get("high_price"),
        "low_price": v.get("low_price"),
        "pct_change": v.get("pct_change"),
        "volume": v.get("volume"),
        "amount": v.get("amount"),
        "source": v.get("source"),
        "fetch_time": v.get("fetch_time"),
    }


async def fetch_quote_snapshot_dual(stock_code: str) -> dict[str, Any]:
    providers = [x.strip() for x in settings.market_provider_order.split(",") if x.strip()]
    errors: list[str] = []
    snapshots: dict[str, dict | None] = {"eastmoney": None, "tdx": None}

    if should_skip_source("market", "eastmoney"):
        errors.append("eastmoney:circuit_open")
    else:
        try:
            eastmoney = await _fetch_eastmoney_quote(stock_code)
            if eastmoney and eastmoney.get("last_price") is not None:
                record_source_result("market", "eastmoney", True)
                snapshots["eastmoney"] = eastmoney
            else:
                raise RuntimeError("empty_quote")
        except Exception as exc:
            errors.append(f"eastmoney:{_err_text(exc)}")
            record_source_result("market", "eastmoney", False, _err_text(exc))

    if should_skip_source("market", "tdx"):
        errors.append("tdx:circuit_open")
    else:
        try:
            tdx = _fetch_tdx_quote(stock_code)
            if tdx and tdx.get("last_price") is not None:
                record_source_result("market", "tdx", True)
                snapshots["tdx"] = tdx
            else:
                raise RuntimeError("empty_quote")
        except Exception as exc:
            errors.append(f"tdx:{_err_text(exc)}")
            record_source_result("market", "tdx", False, _err_text(exc))

    selected, selected_provider = _pick_selected_snapshot(providers, snapshots)
    comparison = _build_dual_comparison(snapshots.get("eastmoney"), snapshots.get("tdx"))
    if selected:
        dual_sources_view = {
            "eastmoney": _quote_view(snapshots.get("eastmoney")),
            "tdx": _quote_view(snapshots.get("tdx")),
        }
        selected["dual_sources"] = snapshots
        selected["dual_sources"] = dual_sources_view
        selected["dual_comparison"] = comparison
        selected["dual_status"] = (
            "ok_both"
            if comparison.get("both_available")
            else ("ok_eastmoney_only" if selected_provider == "eastmoney" else "ok_tdx_only")
        )
        selected["source_selected_by_order"] = selected_provider
        return selected

    record_source_result("market", "fallback", True)
    return {
        "stock_code": stock_code,
        "name": stock_code,
        "last_price": None,
        "open_price": None,
        "high_price": None,
        "low_price": None,
        "pct_change": None,
        "volume": None,
        "amount": None,
        "source": "fallback",
        "errors": errors,
        "fetch_time": datetime.now(timezone.utc).isoformat(),
        "dual_sources": snapshots,
        "dual_comparison": comparison,
        "dual_status": "missing",
        "source_selected_by_order": None,
    }


async def fetch_quote_snapshot(stock_code: str) -> dict[str, Any]:
    return await fetch_quote_snapshot_dual(stock_code)


async def fetch_price_return(stock_code: str, window_days: int) -> float | None:
    """最近 window_days 个交易日的收益率（用于无 report 时的兼容）。"""
    secid = _to_secid(stock_code)
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "klt": "101",
        "fqt": "1",
        "lmt": str(max(window_days + 2, 10)),
        "end": "20500000",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
    }
    async with httpx.AsyncClient(timeout=10, headers=_EASTMONEY_HEADERS, **_HTTPX_DIRECT) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            klines = (resp.json().get("data") or {}).get("klines") or []
            if len(klines) < window_days + 1:
                record_source_result("market", "eastmoney", False, "kline_not_enough")
                return None
            closes = [float(line.split(",")[2]) for line in klines]
            start = closes[-(window_days + 1)]
            end = closes[-1]
            if start == 0:
                record_source_result("market", "eastmoney", False, "start_zero")
                return None
            record_source_result("market", "eastmoney", True)
            return round((end - start) / start, 6)
        except Exception as exc:
            record_source_result("market", "eastmoney", False, _err_text(exc))
            return None


async def fetch_price_return_from_trade_date(
    stock_code: str, trade_date: str | None, window_days: int
) -> float | None:
    """
    从报告交易日 trade_date 起算 window_days 个交易日的实际收益率（用于结算）。
    trade_date 格式 YYYY-MM-DD；若为 None 或未找到该日，退回 fetch_price_return(最近 window_days)。
    """
    if not trade_date or not trade_date.strip():
        return await fetch_price_return(stock_code, window_days)
    trade_dt = trade_date.replace("-", "")  # 20250101
    klines = await fetch_recent_klines(stock_code, limit=min(400, settings.forecast_history_days))
    if len(klines) < window_days + 1:
        return await fetch_price_return(stock_code, window_days)
    # klines 时间顺序：旧→新，日期格式多为 YYYYMMDD
    for i, row in enumerate(klines):
        d = (row.get("date") or "").replace("-", "")
        if d != trade_dt:
            continue
        if i + window_days >= len(klines):
            break
        start = klines[i]["close"]
        end = klines[i + window_days]["close"]
        if start == 0:
            return None
        record_source_result("market", "eastmoney", True)
        return round((end - start) / start, 6)
    return await fetch_price_return(stock_code, window_days)


async def fetch_recent_klines(stock_code: str, limit: int = 60) -> list[dict[str, Any]]:
    secid = _to_secid(stock_code)
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    requested_limit = max(int(limit or 60), 1)
    request_limits: list[int] = []
    for candidate in (
        requested_limit,
        min(requested_limit, 120),
        min(requested_limit, 60),
        min(requested_limit, 30),
    ):
        if candidate not in request_limits:
            request_limits.append(candidate)
    async with httpx.AsyncClient(timeout=10, headers=_EASTMONEY_HEADERS, **_HTTPX_DIRECT) as client:
        last_error: Exception | None = None
        for candidate_limit in request_limits:
            params = {
                "secid": secid,
                "klt": "101",
                "fqt": "1",
                "lmt": str(candidate_limit),
                "end": "20500000",
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            }
            for attempt in range(3):
                try:
                    resp = await client.get(url, params=params)
                    resp.raise_for_status()
                    raw = (resp.json().get("data") or {}).get("klines") or []
                    if not raw:
                        raise RuntimeError("empty_history")
                    out: list[dict[str, Any]] = []
                    for line in raw[-requested_limit:]:
                        p = line.split(",")
                        out.append(
                            {
                                "date": p[0],
                                "open": float(p[1]),
                                "close": float(p[2]),
                                "high": float(p[3]),
                                "low": float(p[4]),
                                "volume": float(p[5]),
                                "amount": float(p[6]),
                            }
                        )
                    record_source_result("market", "eastmoney", True)
                    return out
                except Exception as exc:
                    last_error = exc
                    if attempt < 2:
                        await asyncio.sleep(0.25 * (attempt + 1))
            continue
        record_source_result("market", "eastmoney", False, _err_text(last_error or RuntimeError("empty_history")))
        return []


async def fetch_market_features(stock_code: str) -> dict[str, Any]:
    klines = await fetch_recent_klines(stock_code, settings.forecast_history_days)
    if not klines:
        return {"source": "eastmoney", "status": "missing", "features": {}}
    closes = [x["close"] for x in klines]
    ma5 = _ma(closes, 5)
    ma20 = _ma(closes, 20)
    ret5 = round((closes[-1] - closes[-6]) / closes[-6], 6) if len(closes) >= 6 and closes[-6] != 0 else None
    ret20 = round((closes[-1] - closes[-21]) / closes[-21], 6) if len(closes) >= 21 and closes[-21] != 0 else None
    trend = "震荡"
    if ma5 is not None and ma20 is not None:
        trend = "偏多" if ma5 > ma20 else "偏空"
    return {
        "source": "eastmoney",
        "status": "ok",
        "features": {
            "ma5": ma5,
            "ma20": ma20,
            "ret5": ret5,
            "ret20": ret20,
            "trend": trend,
            "sample_days": len(klines),
            "last_trade_date": klines[-1]["date"],
        },
    }
