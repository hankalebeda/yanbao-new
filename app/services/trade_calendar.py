from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

from app.services.tdx_local_data import load_tdx_day_records

CN_TZ = ZoneInfo("Asia/Shanghai")
_BENCHMARK_CODES = ("000001.SZ", "399001.SZ", "600000.SH")

# Module-level engine placeholder (monkeypatched by tests)
engine = None

# Alias used by some tests for monkeypatching
_local_trade_days_set: set[str] | None = None


def clear_trade_calendar_cache() -> None:
    """Clear the lru_cache on trade day helpers."""
    _trade_days_set.cache_clear()


def _to_cn_date(dt: datetime | date | None = None) -> date:
    cur = dt or datetime.now(CN_TZ)
    if isinstance(cur, date) and not isinstance(cur, datetime):
        return cur
    if cur.tzinfo is None:
        cur = cur.replace(tzinfo=timezone.utc).astimezone(CN_TZ)
    else:
        cur = cur.astimezone(CN_TZ)
    return cur.date()


def _norm_date_str(raw: str | None) -> str | None:
    s = str(raw or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return None


@lru_cache(maxsize=1)
def _trade_days_set() -> set[str]:
    out: set[str] = set()
    for code in _BENCHMARK_CODES:
        rows = load_tdx_day_records(code, limit=None)
        for row in rows:
            ds = _norm_date_str(row.get("date"))
            if ds:
                out.add(ds)
        if out:
            break
    return out


def is_trade_day(dt: datetime | None = None) -> bool:
    d = _to_cn_date(dt)
    ds = d.isoformat()
    trade_days = _trade_days_set()
    if trade_days:
        return ds in trade_days
    # Fallback if local calendar is unavailable.
    return d.weekday() < 5


def latest_trade_date_str(dt: datetime | None = None) -> str:
    d = _to_cn_date(dt)
    trade_days = _trade_days_set()
    if trade_days:
        cur = d
        for _ in range(366):
            ds = cur.isoformat()
            if ds in trade_days:
                return ds
            cur = cur - timedelta(days=1)
    # Fallback only for missing local calendar.
    cur = d
    while cur.weekday() >= 5:
        cur = cur - timedelta(days=1)
    return cur.isoformat()


def trade_date_after_n_days(from_date: str, n_days: int) -> str:
    """from_date 之后第 n 个交易日（不含 from_date）；n=1 表示下一交易日。"""
    if n_days <= 0:
        return from_date.strip()
    d0 = date.fromisoformat(from_date.strip())
    end = (d0 + timedelta(days=max(n_days * 3, 90))).isoformat()
    days = [d for d in trade_days_in_range(from_date, end) if d > from_date.strip()]
    if len(days) >= n_days:
        return days[n_days - 1]
    return days[-1] if days else from_date


def trade_days_in_range(start_date: str, end_date: str) -> list[str]:
    """返回 [start_date, end_date] 内所有交易日（含首尾），YYYY-MM-DD 升序。"""
    try:
        d0 = date.fromisoformat(start_date.strip())
        d1 = date.fromisoformat(end_date.strip())
    except ValueError:
        return []
    if d0 > d1:
        d0, d1 = d1, d0
    trade_days = _trade_days_set()
    out: list[str] = []
    cur = d0
    while cur <= d1:
        ds = cur.isoformat()
        if trade_days and ds in trade_days:
            out.append(ds)
        elif not trade_days and cur.weekday() < 5:
            out.append(ds)
        cur = cur + timedelta(days=1)
    return out


def next_trade_date_str(after_date: str | None = None) -> str:
    """返回给定日期之后的下一个交易日（YYYY-MM-DD）。用于 sim_open_date。"""
    if after_date:
        try:
            d = date.fromisoformat(after_date.strip())
        except ValueError:
            d = _to_cn_date(None)
    else:
        d = _to_cn_date(None)
    trade_days = _trade_days_set()
    cur = d
    for _ in range(366):
        cur = cur + timedelta(days=1)
        ds = cur.isoformat()
        if trade_days and ds in trade_days:
            return ds
        if not trade_days and cur.weekday() < 5:
            return ds
    return (d + timedelta(days=1)).isoformat()

