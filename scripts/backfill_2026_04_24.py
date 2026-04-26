"""
Round 8 补数脚本: 2026-04-24 三股定向补数
- 从 eastmoney 拉取 600519/000001/300750 的 kline
- 从 eastmoney+weibo+douyin 拉取 hotspot
- 刷新 stock_pool_snapshot 为 FALLBACK 自 2026-04-16
- 计算 market_state 缓存

使用：服务必须停止（无SQLite写锁竞争）
python scripts/backfill_2026_04_24.py
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("MOCK_LLM", "false")
os.environ.setdefault("ENABLE_SCHEDULER", "false")

from datetime import date, datetime, timezone
from app.core.db import SessionLocal
from app.services.stock_pool import refresh_stock_pool
from app.services.market_data import fetch_recent_klines
from app.services.hotspot import fetch_eastmoney_hot, fetch_weibo_hot, fetch_douyin_hot
from app.services.multisource_ingest import ingest_market_data, HotspotContribution, HOTSPOT_SOURCE_PRIORITY
from app.services.market_state import calc_and_cache_market_state
from app.services.hotspot import link_topic_to_stock, infer_event_type, _topic_id

TARGET_DATE = date(2026, 4, 24)
CORE_STOCKS = ["600519.SH", "000001.SZ", "300750.SZ"]
STOCK_NAMES = {"600519.SH": "贵州茅台", "000001.SZ": "平安银行", "300750.SZ": "宁德时代"}
RELEVANCE_THRESHOLD = 0.25
TOP_N = 50


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def step1_refresh_pool():
    print("\n[Step1] Refreshing stock_pool for 2026-04-24 ...")
    db = SessionLocal()
    try:
        result = refresh_stock_pool(db, trade_date=TARGET_DATE, force_rebuild=True)
        print(f"  pool status={result.get('status')} fallback_from={result.get('fallback_from')} core_size={result.get('core_pool_size')}")
        return result
    except Exception as exc:
        print(f"  ERROR: {exc}")
        return {}
    finally:
        db.close()


def _fetch_kline(stock_code: str, td: date) -> list[dict]:
    rows = _run(fetch_recent_klines(stock_code, limit=120))
    td_str = td.isoformat()
    return [
        {
            "trade_date": str(r.get("date", ""))[:10],
            "open": r.get("open"),
            "high": r.get("high"),
            "low": r.get("low"),
            "close": r.get("close"),
            "volume": r.get("volume"),
            "amount": r.get("amount"),
        }
        for r in rows
        if str(r.get("date", ""))[:10] <= td_str
    ]


def _build_hotspot_fetcher():
    def fetch_hotspot_by_source(source_name: str, trade_date):
        if source_name == "eastmoney":
            raw = _run(fetch_eastmoney_hot(TOP_N))
        elif source_name == "weibo":
            raw = _run(fetch_weibo_hot(TOP_N))
        elif source_name == "douyin":
            raw = _run(fetch_douyin_hot(TOP_N))
        else:
            return []

        normalized = []
        for idx, topic in enumerate(raw or [], start=1):
            title = str(topic.get("title") or "").strip()
            source_url = str(topic.get("source_url") or "")
            if not title or not source_url.startswith("http"):
                continue

            matched_codes = []
            for stock_code in CORE_STOCKS:
                link = link_topic_to_stock(title, stock_code, stock_name=STOCK_NAMES.get(stock_code))
                if float(link.get("relevance_score") or 0.0) >= RELEVANCE_THRESHOLD:
                    matched_codes.append(stock_code)
            if not matched_codes:
                continue

            event_type = infer_event_type(title)
            normalized.append({
                "rank": int(topic.get("rank") or idx),
                "topic_title": title,
                "source_url": source_url,
                "fetch_time": topic.get("fetch_time"),
                "news_event_type": None if event_type == "general" else event_type,
                "hotspot_tags": [] if event_type == "general" else [event_type],
                "stock_codes": matched_codes,
            })
        print(f"    hotspot[{source_name}]: {len(raw or [])} raw → {len(normalized)} relevant")
        return normalized

    return fetch_hotspot_by_source


def step2_ingest_data():
    print("\n[Step2] Ingesting kline + hotspot for 2026-04-24 ...")
    fetch_hotspot = _build_hotspot_fetcher()
    db = SessionLocal()
    try:
        result = ingest_market_data(
            db,
            trade_date=TARGET_DATE,
            stock_codes=CORE_STOCKS,
            core_pool_codes=CORE_STOCKS,
            kline_source_name="eastmoney",
            fetch_kline_history=_fetch_kline,
            fetch_hotspot_by_source=fetch_hotspot,
        )
        db.commit()
        print(f"  ingest result: kline_quality={result.get('kline_quality_flag')} hotspot_quality={result.get('hotspot_quality_flag')} covered={result.get('covered_count')}")
        return result
    except Exception as exc:
        db.rollback()
        print(f"  ERROR: {exc}")
        import traceback; traceback.print_exc()
        return {}
    finally:
        db.close()


def step3_market_state():
    print("\n[Step3] Computing market_state for 2026-04-24 ...")
    try:
        result = calc_and_cache_market_state(trade_date=TARGET_DATE)
        print(f"  market_state={result}")
        return result
    except Exception as exc:
        print(f"  ERROR: {exc}")
        return {}


if __name__ == "__main__":
    step1_refresh_pool()
    step2_ingest_data()
    step3_market_state()
    print("\n[Done] Backfill completed.")
