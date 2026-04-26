from __future__ import annotations

import csv
import math
import os
import struct
from pathlib import Path


def _find_tdx_root() -> Path:
    """Auto-detect TDX install directory; fall back to legacy extracted path."""
    # 1. Check environment variable (highest priority)
    env_dir = os.environ.get("TDX_INSTALL_DIR", "")
    if env_dir and Path(env_dir).exists():
        return Path(env_dir)
    # 2. Try app config
    try:
        from app.core.config import get_settings
        cfg_dir = get_settings().tdx_install_dir
        if cfg_dir and Path(cfg_dir).exists():
            return Path(cfg_dir)
    except Exception:
        pass
    # 3. Common Windows TDX install paths
    for candidate in ["C:/new_tdx", "C:/TDX", "D:/new_tdx", "D:/TDX", "C:/通达信"]:
        p = Path(candidate)
        if (p / "vipdoc").exists():
            return p
    # 4. Legacy extracted path (fallback)
    return Path("data/tdx_downloads/extracted")


_TDX_ROOT = _find_tdx_root()
_USE_VIPDOC = (_TDX_ROOT / "vipdoc").exists()

# Map to vipdoc structure: sh/lday/sh600519.day or legacy shlday/sh600519.day
_SH_LDAY = _TDX_ROOT / "vipdoc" / "sh" / "lday" if _USE_VIPDOC else _TDX_ROOT / "shlday"
_SZ_LDAY = _TDX_ROOT / "vipdoc" / "sz" / "lday" if _USE_VIPDOC else _TDX_ROOT / "szlday"
_BJ_LDAY = _TDX_ROOT / "vipdoc" / "bj" / "lday" if _USE_VIPDOC else _TDX_ROOT / "bjlday"

# Keep ROOT alias for backward compat
ROOT = _TDX_ROOT


def _day_dirs_for_stock(stock_code: str) -> list[Path]:
    code = stock_code.split(".")[0]
    if code.startswith("6") or code.startswith("9"):
        return [_SH_LDAY]
    if code.startswith(("0", "2", "3")):
        return [_SZ_LDAY]
    if code.startswith(("4", "8")):
        return [_BJ_LDAY]
    return [_SH_LDAY, _SZ_LDAY, _BJ_LDAY]


def _day_file_candidates(stock_code: str) -> list[Path]:
    code = stock_code.split(".")[0]
    out = []
    for d in _day_dirs_for_stock(stock_code):
        out.extend([d / f"sh{code}.day", d / f"sz{code}.day", d / f"bj{code}.day", d / f"{code}.day"])
    return out


def load_tdx_day_records(stock_code: str, limit: int | None = 260) -> list[dict]:
    fpath = None
    for p in _day_file_candidates(stock_code):
        if p.exists():
            fpath = p
            break
    if not fpath:
        return []

    raw = fpath.read_bytes()
    n = len(raw) // 32
    start = max(0, n - limit) if isinstance(limit, int) and limit > 0 else 0
    out: list[dict] = []
    for i in range(start, n):
        rec = raw[i * 32 : (i + 1) * 32]
        date, op, hi, lo, cl, amt, vol, _ = struct.unpack("<IIIIIfII", rec)
        out.append(
            {
                "date": str(date),
                "open": op / 100.0,
                "high": hi / 100.0,
                "low": lo / 100.0,
                "close": cl / 100.0,
                "amount": float(amt),
                "volume": int(vol),
            }
        )
    return out


def _ret(closes: list[float], n: int) -> float | None:
    if len(closes) <= n or closes[-(n + 1)] == 0:
        return None
    return (closes[-1] - closes[-(n + 1)]) / closes[-(n + 1)]


def _std(vals: list[float]) -> float:
    if not vals:
        return 0.0
    m = sum(vals) / len(vals)
    return (sum((x - m) ** 2 for x in vals) / len(vals)) ** 0.5


def _ema(values: list[float], n: int) -> list[float]:
    out = []
    alpha = 2 / (n + 1)
    ema = values[0]
    out.append(ema)
    for v in values[1:]:
        ema = alpha * v + (1 - alpha) * ema
        out.append(ema)
    return out


def _macd(closes: list[float]) -> dict:
    if len(closes) < 26:
        return {"dif": None, "dea": None, "macd": None}
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    dif = [a - b for a, b in zip(ema12, ema26)]
    dea = _ema(dif, 9)
    # macd bar usually (dif - dea) * 2
    macd_bar = [(a - b) * 2 for a, b in zip(dif, dea)]
    return {"dif": dif[-1], "dea": dea[-1], "macd": macd_bar[-1]}


def _rsi(closes: list[float], n: int = 6) -> float | None:
    if len(closes) < n + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(0, x) for x in deltas]
    losses = [abs(min(0, x)) for x in deltas]
    
    # simple ma for first step to keep it simple and stateless-ish
    # or wilder smoothing. Let's use simple MA for recent n per request usually
    # but strictly RSI uses Wilder. Here using simple average for approximation 
    # if historical window is short, otherwise exponential.
    # We'll use SMA for the window for robustness on short series.
    avg_gain = sum(gains[-n:]) / n
    avg_loss = sum(losses[-n:]) / n
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _kdj(closes: list[float], highs: list[float], lows: list[float]) -> dict:
    if len(closes) < 9:
        return {"k": 50.0, "d": 50.0, "j": 50.0}
    # Standard KDJ 9,3,3
    k = 50.0
    d = 50.0
    # Iterative calculation
    for i in range(len(closes)):
        start = max(0, i - 8)
        # rsv window
        h_n = max(highs[start : i + 1])
        l_n = min(lows[start : i + 1])
        c = closes[i]
        if h_n == l_n:
             rsv = 50.0
        else:
             rsv = (c - l_n) / (h_n - l_n) * 100.0
        k = (2/3) * k + (1/3) * rsv
        d = (2/3) * d + (1/3) * k
    j = 3 * k - 2 * d
    return {"k": k, "d": d, "j": j}


def _boll(closes: list[float], n: int = 20, k: int = 2) -> dict:
    if len(closes) < n:
        return {"upper": None, "mid": None, "lower": None}
    mid = sum(closes[-n:]) / n
    std_val = _std(closes[-n:])
    return {"upper": mid + k * std_val, "mid": mid, "lower": mid - k * std_val}


def build_tdx_local_features(stock_code: str) -> dict:
    rows = load_tdx_day_records(stock_code, limit=None)
    if len(rows) < 30:
        return {"status": "missing", "features": {}, "series": rows}

    closes = [x["close"] for x in rows]
    highs = [x["high"] for x in rows]
    lows = [x["low"] for x in rows]
    vols = [x["volume"] for x in rows]
    rets = []
    for i in range(1, len(closes)):
        if closes[i - 1] != 0:
            rets.append((closes[i] - closes[i - 1]) / closes[i - 1])

    tr = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        tr.append(max(highs[i] - lows[i], abs(highs[i] - prev), abs(lows[i] - prev)))

    w = closes[-252:] if len(closes) >= 252 else closes
    min_w, max_w = min(w), max(w)
    price_pct = 0.5 if max_w == min_w else (closes[-1] - min_w) / (max_w - min_w)
    vol_ratio = 1.0
    if len(vols) >= 21:
        avg20 = sum(vols[-21:-1]) / 20.0 if sum(vols[-21:-1]) > 0 else 0
        vol_ratio = vols[-1] / avg20 if avg20 > 0 else 1.0

    def _pct_in_window(n: int) -> float | None:
        if len(closes) < n:
            return None
        wv = closes[-n:]
        lo = min(wv)
        hi = max(wv)
        if hi == lo:
            return 0.5
        return (closes[-1] - lo) / (hi - lo)

    def _extreme_in_window(n: int) -> dict | None:
        if len(rows) < n:
            return None
        w = rows[-n:]
        high_row = max(w, key=lambda x: x["high"])
        low_row = min(w, key=lambda x: x["low"])
        return {
            "window_days": n,
            "high_price": high_row["high"],
            "high_date": high_row["date"],
            "low_price": low_row["low"],
            "low_date": low_row["date"],
        }

    p60 = _pct_in_window(60)
    p252 = _pct_in_window(252)
    p756 = _pct_in_window(756)
    p1260 = _pct_in_window(1260)
    p_all = _pct_in_window(len(closes))
    ref = p756 if p756 is not None else p252
    if ref is None:
        pos_label = "历史样本不足"
    elif ref >= 0.85:
        pos_label = "历史高位区"
    elif ref >= 0.65:
        pos_label = "历史偏高区"
    elif ref >= 0.35:
        pos_label = "历史中位区"
    elif ref >= 0.15:
        pos_label = "历史偏低区"
    else:
        pos_label = "历史低位区"
    if p756 is None and p252 is not None:
        pos_label = f"{pos_label}（按近一年样本）"

    feat = {
        "ret1": _ret(closes, 1),
        "ret3": _ret(closes, 3),
        "ret5": _ret(closes, 5),
        "ret10": _ret(closes, 10),
        "ret20": _ret(closes, 20),
        "ret60": _ret(closes, 60),
        "ret120": _ret(closes, 120),
        "ret252": _ret(closes, 252),
        "volatility20": _std(rets[-20:]) if len(rets) >= 20 else _std(rets),
        "atr14": (sum(tr[-14:]) / 14.0) if len(tr) >= 14 else (sum(tr) / len(tr) if tr else 0.0),
        "volume_ratio20": vol_ratio,
        "price_percentile_252d": price_pct,
        "price_percentile_60d": p60,
        "price_percentile_3y": p756,
        "price_percentile_5y": p1260,
        "price_percentile_all": p_all,
        "historical_position_label": pos_label,
        "window_extreme_60d": _extreme_in_window(60),
        "window_extreme_252d": _extreme_in_window(252),
        "window_extreme_3y": _extreme_in_window(756),
        "window_extreme_5y": _extreme_in_window(1260),
        "window_extreme_all": _extreme_in_window(len(rows)),
        "sample_days": len(rows),
        "macd": _macd(closes),
        "rsi6": _rsi(closes, 6),
        "rsi12": _rsi(closes, 12),
        "rsi14": _rsi(closes, 14),   # standard 14-period RSI (Wilder SMA)
        "rsi24": _rsi(closes, 24),
        "kdj": _kdj(closes, highs, lows),
        "boll": _boll(closes, 20, 2),
    }
    # Keep enough history for long-window backtest while limiting payload size.
    return {"status": "ok", "features": feat, "series": rows[-2500:]}


def _rsi_n(closes: list[float], i: int, n: int) -> float:
    """Wilder's SMA-based RSI for any period n. Returns 50.0 when insufficient data."""
    if i < n:
        return 50.0
    deltas = [closes[j] - closes[j - 1] for j in range(i - n + 1, i + 1)]
    gains = [max(0.0, x) for x in deltas]
    losses_v = [abs(min(0.0, x)) for x in deltas]
    avg_gain = sum(gains) / n
    avg_loss = sum(losses_v) / n
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def build_features_series(stock_code: str) -> list[dict]:
    """Compute per-day technical indicator values for backtest signal models.

    Returns a list aligned with the day series from load_tdx_day_records,
    each element containing RSI6/RSI14, MACD DIF/DEA/bar, KDJ K/D/J,
    BOLL upper/mid/lower, and vol_ratio (volume vs 20-day average).
    """
    rows = load_tdx_day_records(stock_code, limit=None)
    if len(rows) < 30:
        return []

    closes = [x["close"] for x in rows]
    highs = [x["high"] for x in rows]
    lows = [x["low"] for x in rows]
    volumes = [float(x.get("volume") or x.get("vol") or 0) for x in rows]

    n_days = len(closes)
    result: list[dict] = []

    # Pre-compute full EMA series for MACD
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26) if n_days >= 26 else [0.0] * n_days
    dif_series = [a - b for a, b in zip(ema12, ema26)]
    dea_series = _ema(dif_series, 9) if n_days >= 26 else [0.0] * n_days
    macd_bar_series = [(a - b) * 2 for a, b in zip(dif_series, dea_series)]

    # KDJ iterative
    k_val, d_val = 50.0, 50.0
    kdj_series: list[dict] = []
    for i in range(n_days):
        start = max(0, i - 8)
        h_n = max(highs[start: i + 1])
        l_n = min(lows[start: i + 1])
        c = closes[i]
        rsv = 50.0 if h_n == l_n else (c - l_n) / (h_n - l_n) * 100.0
        k_val = (2 / 3) * k_val + (1 / 3) * rsv
        d_val = (2 / 3) * d_val + (1 / 3) * k_val
        j_val = 3 * k_val - 2 * d_val
        kdj_series.append({"k": k_val, "d": d_val, "j": j_val})

    for i in range(n_days):
        feat: dict = {}

        # RSI6 (SMA-based, 6 bars) — volatile, for backward compat
        feat["rsi6"] = _rsi_n(closes, i, 6)

        # RSI14 (Wilder, 14 bars) — standard RSI, much less noisy
        feat["rsi14"] = _rsi_n(closes, i, 14)

        # MACD
        feat["macd_dif"] = dif_series[i] if i < len(dif_series) else 0.0
        feat["macd_dea"] = dea_series[i] if i < len(dea_series) else 0.0
        feat["macd_bar"] = macd_bar_series[i] if i < len(macd_bar_series) else 0.0

        # KDJ
        feat["kdj_k"] = kdj_series[i]["k"]
        feat["kdj_d"] = kdj_series[i]["d"]
        feat["kdj_j"] = kdj_series[i]["j"]

        # BOLL (20-day, ±2σ)
        if i >= 19:
            mid = sum(closes[i - 19: i + 1]) / 20
            std_val = _std(closes[i - 19: i + 1])
            feat["boll_upper"] = mid + 2 * std_val
            feat["boll_mid"] = mid
            feat["boll_lower"] = mid - 2 * std_val
        else:
            feat["boll_upper"] = None
            feat["boll_mid"] = None
            feat["boll_lower"] = None

        # Volume ratio: current / 20-day average (quantifies abnormal trading activity)
        if i >= 19 and volumes[i] > 0:
            avg_vol = sum(volumes[i - 19: i]) / 20
            feat["vol_ratio"] = volumes[i] / avg_vol if avg_vol > 0 else 1.0
        else:
            feat["vol_ratio"] = 1.0

        result.append(feat)

    return result


def load_market_breadth_latest() -> dict:
    p = ROOT / "ScJyData_zbca" / "ScJyData_zbca.csv"
    if not p.exists():
        return {"status": "missing"}
    with p.open("r", encoding="utf-8", newline="") as f:
        reader = list(csv.DictReader(f))
    if not reader:
        return {"status": "missing"}
    row = reader[0]

    def _num(k: str) -> float:
        try:
            return float(row.get(k, "") or 0)
        except Exception:
            return 0.0

    zt = _num("LB24_ZTNUM")
    dt = _num("LB24_DTNUM")
    up = _num("UP31_NUM")
    down = _num("DOWN31_NUM")
    breadth = 0.0
    if up + down > 0:
        breadth = (up - down) / (up + down)
    return {
        "status": "ok",
        "date": row.get("date"),
        "zt_num": zt,
        "dt_num": dt,
        "up_num": up,
        "down_num": down,
        "breadth_score": breadth,
        "raw": row,
    }
