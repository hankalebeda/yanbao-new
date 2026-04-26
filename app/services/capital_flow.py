from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import httpx
from app.services.tdx_local_data import load_tdx_day_records
try:
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
except Exception:  # pragma: no cover - optional dependency
    pc = None
    ds = None

_CACHE_ROOT = Path("data/capital_cache")
_LOCAL_LHB_ROOTS = [
    Path("data/events/lhb"),
    # Legacy developer path — kept as fallback, will be skipped if not found
    Path("C:/Users/Administrator/Desktop/AI/Automated stock trading/data/events/lhb"),
]
_LHB_DATASET = None


def _to_secid(stock_code: str) -> str:
    code = stock_code.split(".")[0]
    return f"1.{code}" if code.startswith("6") else f"0.{code}"


def _to_symbol(stock_code: str) -> str:
    code = stock_code.split(".")[0]
    return f"sh.{code}" if code.startswith("6") else f"sz.{code}"


def _safe_float(v: Any) -> float | None:
    try:
        if v in (None, "", "-"):
            return None
        return float(v)
    except Exception:
        return None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cache_file(stock_code: str) -> Path:
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT / f"{stock_code.replace('.', '_')}.json"


def _load_cache(stock_code: str) -> dict:
    fp = _cache_file(stock_code)
    if not fp.exists():
        return {"capital_flow_rows": [], "dragon_tiger_records": [], "margin_series": []}
    try:
        return json.loads(fp.read_text(encoding="utf-8"))
    except Exception:
        return {"capital_flow_rows": [], "dragon_tiger_records": [], "margin_series": []}


def _save_cache(stock_code: str, payload: dict) -> None:
    fp = _cache_file(stock_code)
    try:
        fp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _merge_by_key(old_rows: list[dict], new_rows: list[dict], key: str) -> list[dict]:
    merged: dict[str, dict] = {}
    for row in old_rows or []:
        k = str(row.get(key) or "")
        if k:
            merged[k] = row
    for row in new_rows or []:
        k = str(row.get(key) or "")
        if k:
            merged[k] = row
    out = list(merged.values())
    out.sort(key=lambda x: str(x.get(key) or ""), reverse=True)
    return out


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


async def _fetch_kline_capital_flow(stock_code: str, limit: int = 260) -> dict:
    """
    Eastmoney day-kline endpoint for close/amount/date history.
    main_net is populated by _fetch_fflow_daykline when possible;
    otherwise proxy (sign × amount) is applied upstream.
    """
    secid = _to_secid(stock_code)
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "klt": "101",
        "fqt": "1",
        "lmt": str(max(120, limit)),
        "end": "20500000",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
    }
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"}
    async with httpx.AsyncClient(timeout=12, headers=headers, trust_env=False) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        klines = ((r.json().get("data") or {}).get("klines")) or []
    rows = []
    for line in klines:
        p = str(line).split(",")
        if len(p) < 11:
            continue
        rows.append(
            {
                "date": p[0],
                "close": _safe_float(p[2]),
                "amount": _safe_float(p[6]) if len(p) > 6 else None,
                "turnover_rate": _safe_float(p[10]) if len(p) > 10 else None,
                "main_net": None,
                "super_large_net": None,
                "large_net": None,
            }
        )
    return {"status": "ok" if rows else "missing", "rows": rows}


async def _fetch_fflow_daykline(stock_code: str, limit: int = 260) -> list[dict]:
    """
    Eastmoney fflow/daykline — provides REAL main-force net inflow per day.
    Fields: f51=date, f52=main_net(元), f53=super_large_net, f54=large_net, f55=mid, f56=small
    This is the authoritative source for per-stock main-force capital flow history.
    """
    secid = _to_secid(stock_code)
    url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
    params = {
        "lmt": str(min(500, max(limit, 60))),
        "klt": "101",
        "secid": secid,
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
    }
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://data.eastmoney.com/zjlx/"}
    try:
        async with httpx.AsyncClient(timeout=12, headers=headers, trust_env=False) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            klines = ((r.json().get("data") or {}).get("klines")) or []
    except Exception:
        return []
    rows = []
    for line in klines:
        p = str(line).split(",")
        if len(p) < 2:
            continue
        rows.append({
            "date": p[0],
            "main_net": _safe_float(p[1]) if len(p) > 1 else None,
            "super_large_net": _safe_float(p[2]) if len(p) > 2 else None,
            "large_net": _safe_float(p[3]) if len(p) > 3 else None,
        })
    return rows


async def _fetch_margin_realtime(stock_code: str) -> dict:
    """
    Fetch today's margin financing balance from EastMoney push2 API.
    f277 = 融资余额(元), f278 = 融券余额(元), f45 = 融券余量(股), f46 = 融资融券余额(元)
    Returns single-day snapshot; no history.
    """
    secid = _to_secid(stock_code)
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "secid": secid,
        "fields": "f277,f278,f43,f44,f45,f46",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
    }
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"}
    try:
        async with httpx.AsyncClient(timeout=10, headers=headers, trust_env=False) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json().get("data") or {}
        rzye = _safe_float(data.get("f277"))
        rqye = _safe_float(data.get("f278"))
        rzrqye = _safe_float(data.get("f46"))
        return {
            "status": "ok" if rzye is not None else "missing",
            "source": "eastmoney_push2_realtime",
            "rzye": rzye,       # 融资余额
            "rqye": rqye,       # 融券余额
            "rzrqye": rzrqye,   # 融资融券余额
        }
    except Exception:
        return {"status": "missing", "source": "eastmoney_push2_realtime"}


async def _fetch_paginated(
    *,
    report_name: str,
    columns: str,
    filter_expr: str,
    page_size: int = 500,
    max_pages: int = 200,
) -> tuple[list[dict], dict]:
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    out: list[dict] = []
    total_pages_reported: int | None = None
    pages_fetched = 0
    truncated_by_safety_limit = False
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://data.eastmoney.com/"}
    async with httpx.AsyncClient(timeout=12, headers=headers, trust_env=False) as client:
        page = 1
        while True:
            if page > max_pages:
                truncated_by_safety_limit = True
                break
            params = {
                "sortColumns": "TRADE_DATE",
                "sortTypes": "-1",
                "pageSize": str(page_size),
                "pageNumber": str(page),
                "reportName": report_name,
                "columns": columns,
                "filter": filter_expr,
            }
            try:
                resp = await client.get(url, params=params)
            except Exception:
                break
            resp.raise_for_status()
            result = resp.json().get("result") or {}
            if total_pages_reported is None:
                try:
                    total_pages_reported = int(result.get("pages")) if result.get("pages") is not None else None
                except Exception:
                    total_pages_reported = None
            data = (result.get("data")) or []
            pages_fetched += 1
            if not data:
                break
            out.extend(data)
            if isinstance(total_pages_reported, int) and total_pages_reported > 0 and page >= total_pages_reported:
                break
            if total_pages_reported is None and len(data) < page_size:
                break
            page += 1
    stats = {
        "total_pages_reported": total_pages_reported,
        "pages_fetched": pages_fetched,
        "records_fetched": len(out),
        "truncated_by_safety_limit": truncated_by_safety_limit,
    }
    return out, stats


async def _fetch_dragon_tiger(stock_code: str, limit: int | None = None) -> dict:
    code = stock_code.split(".")[0]
    rows, page_stats = await _fetch_paginated(
        report_name="RPT_DAILYBILLBOARD_DETAILSNEW",
        columns="SECURITY_CODE,SECURITY_NAME_ABBR,TRADE_DATE,EXPLAIN,CLOSE_PRICE,CHANGE_RATE,BILLBOARD_NET_AMT,ACCUM_AMOUNT,DEAL_NET_RATIO,TURNOVERRATE",
        filter_expr=f'(SECURITY_CODE="{code}")',
        page_size=500,
        max_pages=200,
    )
    truncated_by_limit = False
    if isinstance(limit, int) and limit > 0 and len(rows) > limit:
        rows = rows[:limit]
        truncated_by_limit = True
    if not rows:
        page_stats["truncated_by_limit"] = truncated_by_limit
        return {"status": "missing", "records": [], "page_stats": page_stats}
    records = []
    for x in rows:
        records.append(
            {
                "trade_date": x.get("TRADE_DATE"),
                "reason": x.get("EXPLAIN"),
                "net_buy_amt": _safe_float(x.get("BILLBOARD_NET_AMT")),
                "net_buy_ratio": _safe_float(x.get("DEAL_NET_RATIO")),
                "turnover_rate": _safe_float(x.get("TURNOVERRATE")),
            }
        )
    page_stats["truncated_by_limit"] = truncated_by_limit
    page_stats["records_after_limit"] = len(records)
    return {"status": "ok", "records": records, "page_stats": page_stats}


def _get_local_lhb_dataset():
    global _LHB_DATASET
    if _LHB_DATASET is not None:
        return _LHB_DATASET
    if ds is None or pc is None:
        return None
    for root in _LOCAL_LHB_ROOTS:
        if root.exists():
            try:
                _LHB_DATASET = ds.dataset(str(root), format="parquet", partitioning=None)
                return _LHB_DATASET
            except Exception:
                continue
    return None


async def _fetch_dragon_tiger_local(stock_code: str, limit: int | None = None) -> dict:
    dataset = _get_local_lhb_dataset()
    if dataset is None:
        return {"status": "missing", "records": [], "reason": "local_lhb_dataset_unavailable"}
    symbol = _to_symbol(stock_code)
    try:
        table = dataset.to_table(
            filter=pc.field("symbol") == symbol,
            columns=["trade_date", "symbol", "interpretation", "buy_amount", "sell_amount", "net_amount"],
        )
    except Exception as exc:
        return {"status": "missing", "records": [], "reason": f"local_lhb_query_failed:{exc.__class__.__name__}"}
    rows = table.to_pylist() if table.num_rows > 0 else []
    if not rows:
        return {"status": "missing", "records": [], "reason": "local_lhb_no_rows"}

    def _date_key(x: dict) -> str:
        v = x.get("trade_date")
        return str(v)

    rows.sort(key=_date_key, reverse=True)
    if isinstance(limit, int) and limit > 0:
        rows = rows[:limit]
    records = []
    for x in rows:
        td = x.get("trade_date")
        td_s = str(td)[:10] if td is not None else None
        records.append(
            {
                "trade_date": td_s,
                "reason": x.get("interpretation"),
                "net_buy_amt": _safe_float(x.get("net_amount")),
                "net_buy_ratio": None,
                "turnover_rate": None,
            }
        )
    return {
        "status": "ok",
        "records": records,
        "source": "local_lhb_dataset",
        "page_stats": {
            "total_pages_reported": None,
            "pages_fetched": 1,
            "records_fetched": len(records),
            "truncated_by_safety_limit": False,
            "truncated_by_limit": False,
            "records_after_limit": len(records),
        },
    }


async def _fetch_margin_financing(stock_code: str, limit: int | None = None) -> dict:
    code = stock_code.split(".")[0]
    rows, page_stats = await _fetch_paginated(
        report_name="RPTA_WEB_MARGIN_DETAILS",
        columns="SECURITY_CODE,TRADE_DATE,RZYE,RQYE,RQYL,RZRQYE",
        filter_expr=f'(SECURITY_CODE="{code}")',
        page_size=500,
        max_pages=200,
    )
    truncated_by_limit = False
    if isinstance(limit, int) and limit > 0 and len(rows) > limit:
        rows = rows[:limit]
        truncated_by_limit = True
    series = []
    for x in rows:
        series.append(
            {
                "trade_date": x.get("TRADE_DATE"),
                "rzye": _safe_float(x.get("RZYE")),
                "rqye": _safe_float(x.get("RQYE")),
                "rzrqye": _safe_float(x.get("RZRQYE")),
            }
        )
    page_stats["truncated_by_limit"] = truncated_by_limit
    page_stats["records_after_limit"] = len(series)
    return {"status": "ok" if series else "missing", "series": series, "page_stats": page_stats}


def _build_capital_flow_summary(flow_rows: list[dict], stock_code: str) -> dict:
    # Ensure ascending order (oldest first) so _sum_window(vals, n) → vals[-n:] gives the most recent n values.
    flow_rows_asc = sorted(flow_rows or [], key=lambda x: str(x.get("date") or ""))
    main_net = [x.get("main_net") for x in flow_rows_asc]
    super_net = [x.get("super_large_net") for x in flow_rows_asc]
    large_net = [x.get("large_net") for x in flow_rows_asc]

    # 北向资金：尝试 akshare（已实装，原方案已归档）
    northbound: dict = {
        "status": "missing",
        "reason": "per_stock_northbound_unstable_source",
        "net_inflow_1d": None,
        "net_inflow_3d": None,
        "net_inflow_5d": None,
        "net_inflow_10d": None,
        "net_inflow_20d": None,
        "streak_days": 0,
    }
    try:
        from app.services.northbound_data import fetch_northbound_summary
        nb = fetch_northbound_summary(stock_code)
        if nb:
            northbound = nb
    except Exception:  # noqa: S110 - broad except intentional for optional akshare
        pass

    return {
        "stock_code": stock_code,
        "northbound": northbound,
        "main_force": {
            "status": "ok" if flow_rows_asc else "missing",
            "history_records": len(flow_rows_asc),
            "history_start_date": flow_rows_asc[0].get("date") if flow_rows_asc else None,
            "history_end_date": flow_rows_asc[-1].get("date") if flow_rows_asc else None,
            "net_inflow_1d": _sum_window(main_net, 1),
            "net_inflow_3d": _sum_window(main_net, 3),
            "net_inflow_5d": _sum_window(main_net, 5),
            "net_inflow_10d": _sum_window(main_net, 10),
            "net_inflow_20d": _sum_window(main_net, 20),
            "super_large_net_5d": _sum_window(super_net, 5),
            "large_net_5d": _sum_window(large_net, 5),
            "streak_days": _streak_sign(main_net),
            "recent_net_inflows": main_net[-10:] if main_net else [],
        },
        "etf_flow": {
            "status": "missing",
            "reason": "per_stock_etf_creation_redemption_not_directly_available",
            "net_creation_redemption_5d": None,
            "net_creation_redemption_20d": None,
        },
        "fetch_time": _utc_now(),
    }


def _build_dragon_tiger_summary(
    records: list[dict],
    stock_code: str,
    page_stats: dict | None = None,
    source: str | None = None,
    no_data_is_zero: bool = False,
) -> dict:
    net_vals = [x.get("net_buy_amt") for x in records if isinstance(x.get("net_buy_amt"), (int, float))]
    ratio_vals = [x.get("net_buy_ratio") for x in records if isinstance(x.get("net_buy_ratio"), (int, float))]
    recent30 = records[:30]
    recent90 = records[:90]
    recent250 = records[:250]
    zero_ok = no_data_is_zero and not records
    return {
        "stock_code": stock_code,
        "status": "ok" if records or zero_ok else "missing",
        "reason": "no_recent_lhb_records" if zero_ok else None,
        "history_records": len(records),
        "history_start_date": records[-1].get("trade_date") if records else None,
        "history_end_date": records[0].get("trade_date") if records else None,
        "lhb_count_30d": len(recent30),
        "lhb_count_90d": len(recent90),
        "lhb_count_250d": len(recent250),
        "net_buy_total": round(sum(net_vals), 4) if net_vals else None,
        "avg_net_buy_ratio": round(sum(ratio_vals) / len(ratio_vals), 4) if ratio_vals else None,
        "seat_concentration": {
            "status": "limited",
            "note": "seat_level_detail_not_available_in_current_free_endpoint",
        },
        "source": source or "eastmoney",
        "latest_records": recent30[:5],
        "page_stats": page_stats or {},
        "fetch_time": _utc_now(),
    }


def _build_margin_summary(series: list[dict], stock_code: str, page_stats: dict | None = None) -> dict:
    rz = [x.get("rzye") for x in series]
    rq = [x.get("rqye") for x in series]

    def _delta(vals: list[float | None], n: int) -> float | None:
        if len(vals) <= n:
            return None
        a = vals[0]
        b = vals[n]
        if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
            return None
        return round(a - b, 4)

    return {
        "stock_code": stock_code,
        "status": "ok" if series else "missing",
        "history_records": len(series),
        "history_start_date": series[-1].get("trade_date") if series else None,
        "history_end_date": series[0].get("trade_date") if series else None,
        "latest_rzye": rz[0] if rz else None,
        "latest_rqye": rq[0] if rq else None,
        "rzye_delta_5d": _delta(rz, 5),
        "rzye_delta_20d": _delta(rz, 20),
        "rqye_delta_5d": _delta(rq, 5),
        "rqye_delta_20d": _delta(rq, 20),
        "margin_balance_series": series,
        "page_stats": page_stats or {},
        "fetch_time": _utc_now(),
    }


def _build_local_proxy_capital_rows(stock_code: str, limit: int = 260) -> list[dict]:
    rows = load_tdx_day_records(stock_code, limit=max(120, limit + 1))
    if len(rows) < 5:
        return []
    out: list[dict] = []
    for i in range(1, len(rows)):
        prev_close = _safe_float(rows[i - 1].get("close"))
        close = _safe_float(rows[i].get("close"))
        amount = _safe_float(rows[i].get("amount"))
        if prev_close is None or close is None or amount is None:
            continue
        if close > prev_close:
            sign = 1.0
        elif close < prev_close:
            sign = -1.0
        else:
            sign = 0.0
        out.append(
            {
                "date": str(rows[i].get("date") or ""),
                "close": close,
                # Proxy-only metric: signed turnover as local main-force approximation.
                "main_net": round(sign * amount, 4),
                "super_large_net": None,
                "large_net": None,
            }
        )
    return out[-limit:]


def _build_local_proxy_margin_series(stock_code: str, limit: int = 260) -> list[dict]:
    rows = load_tdx_day_records(stock_code, limit=max(120, limit + 1))
    if len(rows) < 5:
        return []
    out: list[dict] = []
    level = 0.0
    for i in range(1, len(rows)):
        prev_close = _safe_float(rows[i - 1].get("close"))
        close = _safe_float(rows[i].get("close"))
        amount = _safe_float(rows[i].get("amount"))
        if prev_close is None or close is None or amount is None:
            continue
        # Proxy-only: financing balance trend proxy from signed turnover accumulation.
        diff = close - prev_close
        if diff > 0:
            level += amount
        elif diff < 0:
            level -= amount
        out.append(
            {
                "trade_date": str(rows[i].get("date") or ""),
                # Use abs() to ensure non-negative proxy value (proxy sign has no direct meaning).
                "rzye": round(abs(level), 4),
                "rqye": None,
                "rzrqye": None,
            }
        )
    return out[-limit:]


async def fetch_capital_dimensions(stock_code: str) -> dict:
    out = {
        "capital_flow": {"status": "missing"},
        "dragon_tiger": {"status": "missing"},
        "margin_financing": {"status": "missing"},
        "errors": [],
    }
    cache = _load_cache(stock_code)
    flow_rows: list[dict] = []
    lhb_records: list[dict] = []
    margin_series: list[dict] = []
    flow_fetch_failed = False
    margin_fetch_failed = False
    dragon_tiger_fetch_failed = False
    dragon_tiger_zero_ok = False
    lhb_raw: dict[str, Any] = {}

    # --- Step 1: Try real main-force flow from fflow/daykline (authoritative) ---
    fflow_rows: list[dict] = []
    try:
        fflow_rows = await _fetch_fflow_daykline(stock_code, limit=260)
    except Exception as exc:
        out["errors"].append(f"capital_flow:fflow:{str(exc) or exc.__class__.__name__}")

    kline_has_capital = False
    try:
        flow_raw = await _fetch_kline_capital_flow(stock_code, limit=5000)
        flow_rows = flow_raw.get("rows") or []
        kline_has_capital = any(r.get("main_net") is not None for r in flow_rows)
    except Exception as exc:
        flow_fetch_failed = True
        out["errors"].append(f"capital_flow:{str(exc) or exc.__class__.__name__}")

    if fflow_rows:
        # Merge real fflow data into kline rows by date
        fflow_map = {r["date"]: r for r in fflow_rows if r.get("date")}
        for row in flow_rows:
            d = row.get("date") or ""
            if d in fflow_map:
                row["main_net"] = fflow_map[d].get("main_net")
                row["super_large_net"] = fflow_map[d].get("super_large_net")
                row["large_net"] = fflow_map[d].get("large_net")
        # Also add fflow rows that are not in kline
        kline_dates = {r.get("date") for r in flow_rows}
        for r in fflow_rows:
            if r.get("date") and r["date"] not in kline_dates:
                flow_rows.append({"date": r["date"], "close": None, "amount": None,
                                  "main_net": r.get("main_net"),
                                  "super_large_net": r.get("super_large_net"),
                                  "large_net": r.get("large_net")})
        kline_has_capital = True
        out["errors"].append("capital_flow:fflow_daykline_ok")
    elif not flow_rows or not kline_has_capital:
        # Fallback 1: local TDX proxy
        proxy_rows = _build_local_proxy_capital_rows(stock_code, limit=260)
        if proxy_rows:
            flow_rows = proxy_rows
            out["errors"].append("capital_flow:remote_unavailable_use_local_tdx_proxy")
        elif flow_rows and not kline_has_capital:
            # Fallback 2: sign × amount proxy from kline data
            sorted_kl = sorted(flow_rows, key=lambda x: str(x.get("date") or ""))
            proxy_from_kline: list[dict] = []
            for i in range(1, len(sorted_kl)):
                prev_close = _safe_float(sorted_kl[i - 1].get("close"))
                close = _safe_float(sorted_kl[i].get("close"))
                amount = _safe_float(sorted_kl[i].get("amount"))
                if prev_close is None or close is None or amount is None:
                    continue
                sign = 1.0 if close > prev_close else (-1.0 if close < prev_close else 0.0)
                proxy_from_kline.append({
                    "date": str(sorted_kl[i].get("date") or ""),
                    "close": close,
                    "main_net": round(sign * amount, 4),
                    "super_large_net": None,
                    "large_net": None,
                })
            if proxy_from_kline:
                flow_rows = proxy_from_kline
                out["errors"].append("capital_flow:kline_proxy_sign_x_amount")

    flow_rows = _merge_by_key(cache.get("capital_flow_rows") or [], flow_rows, "date")
    out["capital_flow"] = _build_capital_flow_summary(flow_rows, stock_code)
    if "capital_flow:fflow_daykline_ok" in out["errors"]:
        mf = ((out.get("capital_flow") or {}).get("main_force") or {})
        mf["status"] = "ok"
        mf["reason"] = "eastmoney_fflow_daykline"
        mf["proxy"] = False
        out["capital_flow"]["main_force"] = mf
    elif flow_rows and "capital_flow:remote_unavailable_use_local_tdx_proxy" in out["errors"]:
        mf = ((out.get("capital_flow") or {}).get("main_force") or {})
        mf["status"] = "stale_ok"
        mf["reason"] = "remote_unavailable_use_local_tdx_proxy"
        mf["proxy"] = True
        out["capital_flow"]["main_force"] = mf
    elif flow_rows and "capital_flow:kline_proxy_sign_x_amount" in out["errors"]:
        mf = ((out.get("capital_flow") or {}).get("main_force") or {})
        mf["status"] = "proxy_ok"
        mf["reason"] = "kline_sign_x_amount_proxy"
        mf["proxy"] = True
        out["capital_flow"]["main_force"] = mf
    elif flow_fetch_failed and flow_rows:
        mf = ((out.get("capital_flow") or {}).get("main_force") or {})
        mf["status"] = "stale_ok"
        mf["reason"] = "remote_unavailable_use_cached_history"
        out["capital_flow"]["main_force"] = mf
    try:
        lhb_raw = await _fetch_dragon_tiger(stock_code, limit=None)
        lhb_records = lhb_raw.get("records") or []
    except Exception as exc:
        dragon_tiger_fetch_failed = True
        out["errors"].append(f"dragon_tiger:{str(exc) or exc.__class__.__name__}")
    if not lhb_records:
        local_lhb = await _fetch_dragon_tiger_local(stock_code, limit=None)
        local_records = local_lhb.get("records") or []
        if local_records:
            lhb_raw = local_lhb
            lhb_records = local_records
            out["errors"].append("dragon_tiger:remote_unavailable_use_local_dataset")
        else:
            remote_status = str(lhb_raw.get("status") or "missing").lower()
            if not dragon_tiger_fetch_failed and remote_status == "missing":
                dragon_tiger_zero_ok = True
                lhb_raw = {
                    "status": "ok",
                    "source": lhb_raw.get("source") or "eastmoney",
                    "reason": "no_recent_lhb_records",
                    "page_stats": lhb_raw.get("page_stats") or {},
                }
            else:
                out["errors"].append(f"dragon_tiger_local:{local_lhb.get('reason') or 'unavailable'}")
    lhb_records = _merge_by_key(cache.get("dragon_tiger_records") or [], lhb_records, "trade_date")
    out["dragon_tiger"] = _build_dragon_tiger_summary(
        lhb_records,
        stock_code,
        page_stats=lhb_raw.get("page_stats") or None,
        source=lhb_raw.get("source") or None,
        no_data_is_zero=dragon_tiger_zero_ok,
    )
    try:
        margin_raw = await _fetch_margin_financing(stock_code, limit=None)
        margin_series = margin_raw.get("series") or []
    except Exception as exc:
        margin_fetch_failed = True
        out["errors"].append(f"margin_financing:{str(exc) or exc.__class__.__name__}")

    # Supplement with realtime snapshot from push2 (f277=融资余额, f278=融券余额)
    margin_realtime: dict = {}
    try:
        margin_realtime = await _fetch_margin_realtime(stock_code)
    except Exception:
        pass

    if not margin_series:
        # Try local TDX proxy for historical margin trend
        proxy_margin = _build_local_proxy_margin_series(stock_code, limit=260)
        if proxy_margin:
            margin_series = proxy_margin
            out["errors"].append("margin_financing:remote_unavailable_use_local_tdx_proxy")

    margin_series = _merge_by_key(cache.get("margin_series") or [], margin_series, "trade_date")
    out["margin_financing"] = _build_margin_summary(
        margin_series, stock_code,
        page_stats=margin_raw.get("page_stats") if "margin_raw" in locals() else None
    )

    # Inject realtime snapshot values if historical series is missing
    if margin_realtime.get("status") == "ok":
        mfin = out["margin_financing"]
        if mfin.get("latest_rzye") is None:
            mfin["latest_rzye"] = margin_realtime.get("rzye")
        if mfin.get("latest_rqye") is None:
            mfin["latest_rqye"] = margin_realtime.get("rqye")
        if mfin.get("status") == "missing":
            mfin["status"] = "realtime_only"
            mfin["reason"] = "only_realtime_snapshot_available_no_history"
        out["errors"].append("margin_financing:realtime_snapshot_injected")

    if margin_series and "margin_financing:remote_unavailable_use_local_tdx_proxy" in out["errors"]:
        out["margin_financing"]["status"] = "stale_ok"
        out["margin_financing"]["reason"] = "remote_unavailable_use_local_tdx_proxy"
        out["margin_financing"]["proxy"] = True
    elif margin_fetch_failed and margin_series:
        out["margin_financing"]["status"] = "stale_ok"
        out["margin_financing"]["reason"] = "remote_unavailable_use_cached_history"

    _save_cache(
        stock_code,
        {
            "capital_flow_rows": flow_rows,
            "dragon_tiger_records": lhb_records,
            "margin_series": margin_series,
            "updated_at": _utc_now(),
        },
    )
    return out
