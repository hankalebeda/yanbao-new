"""市场状态机：计算 BULL/NEUTRAL/BEAR 并写入缓存。"""
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.trade_calendar import is_trade_day  # noqa: F401 — tests mock market_state.is_trade_day


_CN_TZ = timezone(timedelta(hours=8))


def _now_cn() -> datetime:
    """Return current time in Asia/Shanghai (UTC+8)."""
    return datetime.now(_CN_TZ)


@dataclass
class MarketStateMetrics:
    """FR-05 市场状态量化指标快照。"""

    reference_date: date
    hs300_ma5: float
    hs300_ma20: float
    hs300_ma20_5d_ago: float
    hs300_return_20d: float
    kline_batch_id: str = ""
    hotspot_batch_id: str = ""

logger = logging.getLogger(__name__)


def _resolve_market_state(*, is_bull: bool = False, is_bear: bool = False) -> str:
    """Simple helper: resolve market state from boolean flags. BEAR has priority."""
    if is_bear:
        return "BEAR"
    if is_bull:
        return "BULL"
    return "NEUTRAL"


def classify_market_state(metrics: MarketStateMetrics) -> str:
    """Pure classification: return BULL/NEUTRAL/BEAR from metrics."""
    ma5 = metrics.hs300_ma5
    ma20 = metrics.hs300_ma20
    ma20_prev = metrics.hs300_ma20_5d_ago
    ret_20d = metrics.hs300_return_20d

    is_bull = ma20 > ma20_prev and ret_20d > 0.03
    is_bear = ma5 < ma20 and ret_20d < -0.05
    if is_bear:
        return "BEAR"
    if is_bull:
        return "BULL"
    return "NEUTRAL"


CACHE_PATH = Path("data/market_state_cache.json")
_EASTMONEY_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def _fetch_index_kline(secid: str, days: int = 30) -> list[dict] | None:
    """获取指数K线，返回 [{date, close, volume}, ...]，f51-f56=日期/开/收/高/低/量"""
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "klt": 101,
        "fqt": 0,
        "lmt": days,
        "end": "20500101",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56",
    }
    try:
        with httpx.Client(timeout=10, headers=_EASTMONEY_HEADERS) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json().get("data") or {}
        klines = data.get("klines") or []
        result = []
        for row in klines:
            parts = row.split(",")
            if len(parts) >= 3:
                vol = float(parts[5]) if len(parts) >= 6 else 0.0
                result.append({"date": parts[0], "close": float(parts[2]), "volume": vol})
        return result
    except Exception as e:
        logger.warning("market_state_fetch_index_failed secid=%s err=%s", secid, e)
        return None


def _ma(values: list[float], n: int) -> float | None:
    if len(values) < n:
        return None
    return sum(values[-n:]) / n


def _previous_trade_date(service_date: date) -> date | None:
    """Return previous trade date before service_date using trade calendar helper."""
    cursor = service_date - timedelta(days=1)
    for _ in range(40):
        if is_trade_day(cursor):
            return cursor
        cursor -= timedelta(days=1)
    return None


def _load_reference_metrics(db: Session, reference_date: date | None) -> MarketStateMetrics | None:
    """Load reference-day metrics from kline_daily for HS300-based market state classification."""
    if reference_date is None:
        return None
    try:
        rows = db.execute(
            text(
                """
                SELECT trade_date, close
                FROM kline_daily
                WHERE stock_code = :stock_code
                  AND trade_date <= :reference_date
                ORDER BY trade_date DESC
                LIMIT 25
                """
            ),
            {
                "stock_code": settings.hs300_code,
                "reference_date": reference_date,
            },
        ).mappings().all()
    except Exception:
        return None

    if len(rows) < 20:
        return None

    closes_desc = [float(row.get("close") or 0.0) for row in rows if row.get("close") is not None]
    if len(closes_desc) < 20:
        return None
    closes = list(reversed(closes_desc))

    ma5 = _ma(closes, 5)
    ma20 = _ma(closes, 20)
    if ma5 is None or ma20 is None:
        return None
    if len(closes) >= 25:
        ma20_5d_ago = sum(closes[-25:-5]) / 20
    else:
        ma20_5d_ago = ma20
    base_price = closes[-20]
    hs300_return_20d = ((closes[-1] - base_price) / base_price) if base_price else 0.0

    return MarketStateMetrics(
        reference_date=reference_date,
        hs300_ma5=float(ma5),
        hs300_ma20=float(ma20),
        hs300_ma20_5d_ago=float(ma20_5d_ago),
        hs300_return_20d=float(hs300_return_20d),
        kline_batch_id="",
        hotspot_batch_id="",
    )


def calc_and_cache_market_state(*, trade_date: date | None = None) -> str:
    """
    计算市场状态（BULL/NEUTRAL/BEAR）并写入 data/market_state_cache.json。
    返回状态字符串；失败时返回 NEUTRAL 并写入 degraded 标注。
    """
    sh_secid = f"1.{settings.sh_index_code}"
    hs_secid = f"1.{settings.hs300_code}"

    sh_kline = _fetch_index_kline(sh_secid, 30)
    hs_kline = _fetch_index_kline(hs_secid, 30)

    if not sh_kline or len(sh_kline) < 20:
        cache = {"market_state": "NEUTRAL", "status": "degraded", "reason": "sh_index_data_missing"}
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.warning("market_state_degraded reason=sh_index_data_missing")
        return "NEUTRAL"

    closes = [r["close"] for r in sh_kline]
    sh_ma5 = _ma(closes, 5)
    sh_ma20 = _ma(closes, 20)
    sh_ma20_prev = _ma(closes[:-1], 20) if len(closes) > 20 else sh_ma20

    hs300_20d_return = 0.0
    if hs_kline and len(hs_kline) >= 20:
        p0 = hs_kline[-20]["close"]
        p1 = hs_kline[-1]["close"]
        if p0 and p0 > 0:
            hs300_20d_return = (p1 - p0) / p0

    # 量能比 = 最新日成交量 / 近5日均量（首页展示「相对5日均量」）
    volume_ratio_20d = 1.0
    if sh_kline and len(sh_kline) >= 6:
        vols = [r.get("volume", 0) for r in sh_kline if isinstance(r.get("volume"), (int, float))]
        if len(vols) >= 5 and sum(vols[-5:]) > 0:
            vol_ma5 = sum(vols[-5:]) / 5
            last_vol = vols[-1]
            if vol_ma5 > 0:
                volume_ratio_20d = round(last_vol / vol_ma5, 4)

    is_bull = (
        sh_ma20 is not None
        and sh_ma20_prev is not None
        and sh_ma20 > sh_ma20_prev
        and hs300_20d_return > 0.03
        and volume_ratio_20d > 0.8
    )
    is_bear = (
        sh_ma5 is not None
        and sh_ma20 is not None
        and sh_ma5 < sh_ma20
        and hs300_20d_return < -0.05
    )

    if is_bear:
        state = "BEAR"
    elif is_bull:
        state = "BULL"
    else:
        state = "NEUTRAL"

    sh_ma20_prev = _ma(closes[:-1], 20) if len(closes) > 20 else sh_ma20
    sh_index_close = closes[-1] if closes else 0
    cache_trade_date = trade_date.isoformat() if trade_date else sh_kline[-1]["date"]
    cache = {
        "market_state": state,
        "sh_index_close": sh_index_close,
        "sh_ma5": sh_ma5,
        "sh_ma20": sh_ma20,
        "sh_ma20_prev": sh_ma20_prev,
        "hs300_20d_return": hs300_20d_return,
        "volume_ratio_20d": volume_ratio_20d,
        "calc_date": cache_trade_date,
        "status": "ok",
    }
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(
        "market_state_updated state=%s sh_ma5=%.2f sh_ma20=%.2f hs300_20d=%.2f%%",
        state,
        sh_ma5 or 0,
        sh_ma20 or 0,
        hs300_20d_return * 100,
    )
    return state


def compute_and_persist_market_state(db=None, trade_date=None) -> str:
    """Alias accepting (db, trade_date) for DAG/repair callers."""
    return calc_and_cache_market_state()


def get_cached_market_state() -> str:
    """读取当日市场状态缓存，供 report_engine 研报生成时使用。缓存缺失或异常时返回 NEUTRAL。"""
    if not CACHE_PATH.exists():
        return "NEUTRAL"
    try:
        cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        return str(cache.get("market_state") or "NEUTRAL").upper()
    except Exception as e:
        logger.warning("market_state_read_cache_failed err=%s", e)
        return "NEUTRAL"


def _latest_cache_before(db, trade_date_str: str):
    """Return the most recent MarketStateCache row strictly before *trade_date_str*."""
    from app.models import MarketStateCache
    td = date.fromisoformat(str(trade_date_str))
    return (
        db.query(MarketStateCache)
        .filter(MarketStateCache.trade_date < td)
        .order_by(MarketStateCache.trade_date.desc())
        .first()
    )


def _latest_cache_on_or_before(db, trade_date_str: str):
    """Return the most recent MarketStateCache row on or before *trade_date_str*."""
    from app.models import MarketStateCache
    td = date.fromisoformat(str(trade_date_str))
    return (
        db.query(MarketStateCache)
        .filter(MarketStateCache.trade_date <= td)
        .order_by(MarketStateCache.trade_date.desc())
        .first()
    )
