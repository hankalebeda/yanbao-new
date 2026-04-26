"""FR-04 真实数据引导脚本

执行顺序（与计划第3阶段一致）：
  1. stock_master   — 全量 A 股主数据（akshare）
  2. kline_daily    — 全市场日线 + 派生因子（eastmoney）
  3. core_pool      — 八因子打分 → 200 核心池
  4. hotspot         — 多源热搜采集 + 合并 Top50
  5. northbound     — 核心池北向资金
  6. market_state   — 市场状态机

用法：
  python scripts/bootstrap_real_data.py                     # 全量执行
  python scripts/bootstrap_real_data.py --step stock_master  # 仅灌主数据
  python scripts/bootstrap_real_data.py --step kline         # 仅灌日线
  python scripts/bootstrap_real_data.py --step pool          # 仅刷新核心池
  python scripts/bootstrap_real_data.py --step hotspot       # 仅采热搜
  python scripts/bootstrap_real_data.py --step northbound    # 仅采北向
  python scripts/bootstrap_real_data.py --step market_state  # 仅算市场状态
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

# 强制绕过系统代理（东方财富/akshare 等国内站点）
os.environ.setdefault("NO_PROXY", "*")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import settings
from app.core.db import SessionLocal, engine
from app.models import Base, StockMaster, KlineDaily, DataBatch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("bootstrap")

STEPS = ("stock_master", "kline", "pool", "hotspot", "northbound", "market_state")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="FR-04 真实数据引导脚本")
    p.add_argument("--step", choices=STEPS, default=None, help="仅执行某一步（默认全量）")
    p.add_argument("--trade-date", default=None, help="目标交易日（YYYY-MM-DD），默认自动推断")
    p.add_argument("--kline-limit", type=int, default=120, help="每股拉取 K 线天数（默认 120）")
    p.add_argument("--batch-size", type=int, default=50, help="K 线并发批次大小")
    p.add_argument("--pool-only-top", type=int, default=0, help="仅对前 N 只股票灌日线（0=全市场）")
    return p.parse_args()


# ===========================================================================
# Step 1: stock_master — 全量 A 股主数据
# ===========================================================================

def step_stock_master(db):
    """从 akshare 灌入全量 A 股主数据到 stock_master。"""
    logger.info("=== Step 1: 灌入 stock_master ===")
    import akshare as ak

    # 获取全量 A 股列表
    logger.info("获取 A 股列表...")
    df = ak.stock_info_a_code_name()
    logger.info("akshare 返回 %d 只股票", len(df))

    # 标准化
    now = datetime.now(timezone.utc)
    existing_batch_seq = db.query(DataBatch.batch_seq).filter(
        DataBatch.source_name == "eastmoney",
        DataBatch.trade_date == trade_date,
        DataBatch.batch_scope == "full_market",
    ).order_by(DataBatch.batch_seq.desc()).first()
    next_batch_seq = int(existing_batch_seq[0]) + 1 if existing_batch_seq and existing_batch_seq[0] is not None else 1
    inserted = 0
    updated = 0
    skipped = 0

    for _, row in df.iterrows():
        code = str(row.get("code", "")).strip()
        name = str(row.get("name", "")).strip()
        if not code or not name:
            skipped += 1
            continue

        # 交易所映射
        if code.startswith("6"):
            exchange = "SH"
            stock_code = f"{code}.SH"
        elif code.startswith(("0", "2", "3")):
            exchange = "SZ"
            stock_code = f"{code}.SZ"
        elif code.startswith(("8", "4")):
            exchange = "BJ"
            stock_code = f"{code}.BJ"
        else:
            skipped += 1
            continue

        is_st = "ST" in name or "*ST" in name

        existing = db.query(StockMaster).filter(StockMaster.stock_code == stock_code).first()
        if existing:
            existing.stock_name = name
            existing.is_st = is_st
            existing.updated_at = now
            updated += 1
        else:
            db.add(StockMaster(
                stock_code=stock_code,
                stock_name=name,
                exchange=exchange,
                industry=None,
                list_date=None,
                circulating_shares=None,
                is_st=is_st,
                is_suspended=False,
                is_delisted=False,
                created_at=now,
                updated_at=now,
            ))
            inserted += 1

    db.commit()
    total = db.query(StockMaster).count()
    logger.info("stock_master: inserted=%d, updated=%d, skipped=%d, total=%d", inserted, updated, skipped, total)
    return total


# ===========================================================================
# Step 2: kline_daily — 全市场日线 + 派生因子
# ===========================================================================

async def _fetch_klines_one(
    stock_code: str,
    limit: int,
    *,
    max_attempts: int = 3,
    base_delay_sec: float = 0.8,
) -> list[dict]:
    """从东方财富获取单只股票的历史日线。"""
    import httpx
    code = stock_code.split(".")[0]
    secid = f"1.{code}" if code.startswith("6") else f"0.{code}"
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "klt": "101",
        "fqt": "1",
        "lmt": str(limit),
        "end": "20500000",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/",
    }
    last_error: Exception | None = None
    timeout = httpx.Timeout(connect=10.0, read=25.0, write=10.0, pool=10.0)
    async with httpx.AsyncClient(timeout=timeout, headers=headers, trust_env=False, http2=False) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                raw = (resp.json().get("data") or {}).get("klines") or []
                out = []
                for line in raw:
                    p = line.split(",")
                    if len(p) < 7:
                        continue
                    out.append({
                        "date": p[0],
                        "open": float(p[1]),
                        "close": float(p[2]),
                        "high": float(p[3]),
                        "low": float(p[4]),
                        "volume": float(p[5]),
                        "amount": float(p[6]),
                    })
                return out
            except Exception as exc:
                last_error = exc
                if attempt >= max_attempts:
                    break
                await asyncio.sleep(base_delay_sec * attempt + random.uniform(0.0, 0.4))
    raise RuntimeError(f"kline_fetch_failed:{stock_code}:{last_error}") from last_error


def _compute_derived_fields(klines: list[dict]) -> list[dict]:
    """为每条 K 线计算 ma5/ma10/ma20/ma60/atr_pct/volatility_20d。"""
    closes = [k["close"] for k in klines]
    highs = [k["high"] for k in klines]
    lows = [k["low"] for k in klines]

    for i, k in enumerate(klines):
        # MA
        for n, key in [(5, "ma5"), (10, "ma10"), (20, "ma20"), (60, "ma60")]:
            if i + 1 >= n:
                segment = closes[i + 1 - n : i + 1]
                k[key] = round(sum(segment) / n, 4)
            else:
                k[key] = None

        # ATR 14
        if i >= 14:
            tr_list = []
            for j in range(i - 13, i + 1):
                tr = max(
                    highs[j] - lows[j],
                    abs(highs[j] - closes[j - 1]) if j > 0 else 0,
                    abs(lows[j] - closes[j - 1]) if j > 0 else 0,
                )
                tr_list.append(tr)
            atr = sum(tr_list) / 14
            k["atr_pct"] = round(atr / closes[i], 6) if closes[i] else None
        else:
            k["atr_pct"] = None

        # Volatility 20d (收益率标准差)
        if i >= 20:
            rets = []
            for j in range(i - 19, i + 1):
                if closes[j - 1] and closes[j - 1] != 0:
                    rets.append((closes[j] - closes[j - 1]) / closes[j - 1])
            if len(rets) >= 10:
                mean_r = sum(rets) / len(rets)
                var_r = sum((r - mean_r) ** 2 for r in rets) / len(rets)
                k["volatility_20d"] = round(var_r ** 0.5, 6)
            else:
                k["volatility_20d"] = None
        else:
            k["volatility_20d"] = None

    return klines


async def _fetch_batch(stock_codes: list[str], limit: int, delay_ms: int = 200) -> dict[str, list[dict]]:
    """批量获取多只股票的 K 线，带速率限制。"""
    result = {}
    for code in stock_codes:
        try:
            klines = await _fetch_klines_one(code, limit)
            if klines:
                result[code] = _compute_derived_fields(klines)
        except Exception as e:
            logger.warning("kline fetch failed for %s: %s", code, e)
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)
    return result


def step_kline(db, trade_date: date, kline_limit: int, batch_size: int, pool_only_top: int):
    """从东方财富灌入全市场日线到 kline_daily。"""
    logger.info("=== Step 2: 灌入 kline_daily ===")

    # 获取股票列表
    stocks = db.query(StockMaster).filter(
        StockMaster.is_delisted == False,
    ).all()
    stock_codes = [s.stock_code for s in stocks]
    if pool_only_top > 0:
        stock_codes = stock_codes[:pool_only_top]
    logger.info("待采集 K 线：%d 只股票，每只 %d 天", len(stock_codes), kline_limit)

    # 创建 data_batch
    now = datetime.now(timezone.utc)
    existing_batch_seq = db.query(DataBatch.batch_seq).filter(
        DataBatch.source_name == "eastmoney",
        DataBatch.trade_date == trade_date,
        DataBatch.batch_scope == "full_market",
    ).order_by(DataBatch.batch_seq.desc()).first()
    next_batch_seq = int(existing_batch_seq[0]) + 1 if existing_batch_seq and existing_batch_seq[0] is not None else 1
    batch = DataBatch(
        batch_id=str(uuid4()),
        source_name="eastmoney",
        trade_date=trade_date,
        batch_scope="full_market",
        batch_seq=next_batch_seq,
        batch_status="RUNNING",
        quality_flag="ok",
        started_at=now,
        updated_at=now,
        created_at=now,
    )
    db.add(batch)
    db.commit()

    # 分批采集
    total_inserted = 0
    total_failed = 0
    total_succeeded = 0
    for i in range(0, len(stock_codes), batch_size):
        chunk = stock_codes[i:i + batch_size]
        logger.info("采集 K 线 batch %d/%d (%d只)...",
                     i // batch_size + 1,
                     (len(stock_codes) + batch_size - 1) // batch_size,
                     len(chunk))
        kline_data = asyncio.run(_fetch_batch(chunk, kline_limit))
        total_succeeded += len(kline_data)
        total_failed += max(len(chunk) - len(kline_data), 0)

        for code, klines in kline_data.items():
            # 查找 circulating_shares 用于换手率
            sm = db.query(StockMaster).filter(StockMaster.stock_code == code).first()
            circ = float(sm.circulating_shares) if sm and sm.circulating_shares else None

            for k in klines:
                td = date.fromisoformat(k["date"])
                # 检查是否已存在
                existing = db.query(KlineDaily).filter(
                    KlineDaily.stock_code == code,
                    KlineDaily.trade_date == td,
                ).first()
                if existing:
                    continue

                turnover = None
                if circ and circ > 0 and k["volume"]:
                    turnover = round(k["volume"] / circ, 6)

                db.add(KlineDaily(
                    kline_id=str(uuid4()),
                    stock_code=code,
                    trade_date=td,
                    open=k["open"],
                    high=k["high"],
                    low=k["low"],
                    close=k["close"],
                    volume=k["volume"],
                    amount=k["amount"],
                    adjust_type="front_adjusted",
                    atr_pct=k.get("atr_pct"),
                    turnover_rate=turnover,
                    ma5=k.get("ma5"),
                    ma10=k.get("ma10"),
                    ma20=k.get("ma20"),
                    ma60=k.get("ma60"),
                    volatility_20d=k.get("volatility_20d"),
                    hs300_return_20d=None,
                    is_suspended=False,
                    source_batch_id=batch.batch_id,
                    created_at=now,
                ))
                total_inserted += 1

        db.commit()
        logger.info("已插入 %d 条 K 线", total_inserted)

    # 更新 batch 状态
    if total_succeeded == 0:
        batch.batch_status = "FAILED"
        batch.quality_flag = "degraded"
    elif total_failed > 0:
        batch.batch_status = "PARTIAL_SUCCESS"
        batch.quality_flag = "degraded"
    else:
        batch.batch_status = "SUCCESS"
        batch.quality_flag = "ok"
    batch.records_total = total_inserted + total_failed
    batch.records_success = total_inserted
    batch.records_failed = total_failed
    batch.covered_stock_count = len(set(
        r[0] for r in db.query(KlineDaily.stock_code).filter(
            KlineDaily.source_batch_id == batch.batch_id
        ).distinct()
    ))
    batch.finished_at = datetime.now(timezone.utc)
    batch.updated_at = datetime.now(timezone.utc)
    db.commit()

    # 统计
    if total_succeeded == 0:
        raise RuntimeError(f"kline_import_failed_all_stocks trade_date={trade_date} total_failed={total_failed}")
    total_kline = db.query(KlineDaily).count()
    unique_stocks = db.query(KlineDaily.stock_code).distinct().count()
    latest_date = db.query(KlineDaily.trade_date).order_by(KlineDaily.trade_date.desc()).first()
    logger.info("kline_daily: total=%d, stocks=%d, latest_date=%s",
                total_kline, unique_stocks, latest_date[0] if latest_date else "N/A")
    return total_inserted


# ===========================================================================
# Step 3: core_pool — 八因子打分 → 200 核心池
# ===========================================================================

def step_pool(db, trade_date: date):
    """调用 refresh_stock_pool 刷新核心池。"""
    logger.info("=== Step 3: 刷新核心池 ===")
    from app.services.stock_pool import refresh_stock_pool

    result = refresh_stock_pool(db, trade_date=trade_date, force_rebuild=True)
    status = result.get("status", "unknown")
    core_size = result.get("core_pool_size", 0)
    logger.info("core_pool: status=%s, core_size=%d, standby_size=%s",
                status, core_size, result.get("standby_pool_size", 0))
    return core_size


# ===========================================================================
# Step 4: hotspot — 多源热搜
# ===========================================================================

def step_hotspot(db, trade_date: date):
    """通过 ingest_market_data 的热搜管线采集。"""
    logger.info("=== Step 4: 采集热搜 ===")
    from app.services.multisource_ingest import ingest_market_data
    from app.services.hotspot import fetch_weibo_hot, fetch_douyin_hot
    from app.services.stock_pool import get_daily_stock_pool

    core_codes = get_daily_stock_pool(trade_date=trade_date, tier=1)
    if not core_codes:
        logger.warning("核心池为空，跳过热搜采集")
        return 0

    # 定义热搜获取器 — 公开源（同步包装 async）
    import asyncio
    def fetch_hotspot_by_source(source_name: str, target_date=None):
        async def _inner():
            if source_name == "weibo":
                return await fetch_weibo_hot(50)
            if source_name == "douyin":
                return await fetch_douyin_hot(50)
            return []
        raw = asyncio.run(_inner())
        # 统一映射字段名以匹配 ingest_market_data 期望
        mapped = []
        for item in raw:
            mapped.append({
                "topic_title": item.get("title") or item.get("topic_title") or "",
                "source_url": item.get("source_url") or "",
                "rank": item.get("rank"),
                "source_rank": item.get("rank"),
                "fetch_time": item.get("fetch_time"),
                "news_event_type": item.get("news_event_type"),
                "hotspot_tags": item.get("hotspot_tags", []),
                "stock_codes": item.get("stock_codes", []),
            })
        return mapped

    def fetch_etf_for_hotspot(td):
        from app.services.etf_flow_data import fetch_etf_flow_summary_global
        return fetch_etf_flow_summary_global(td)

    result = ingest_market_data(
        db,
        trade_date=trade_date,
        stock_codes=[],
        core_pool_codes=core_codes,
        fetch_kline_history=None,
        fetch_hotspot_by_source=fetch_hotspot_by_source,
        fetch_northbound_summary=None,
        fetch_etf_flow_summary=fetch_etf_for_hotspot,
    )
    hotspot_count = result.get("hotspot_merged_count", 0)
    logger.info("hotspot: merged=%d", hotspot_count)
    return hotspot_count


# ===========================================================================
# Step 5: northbound — 北向资金
# ===========================================================================

def step_northbound(db, trade_date: date):
    """通过 ingest_market_data 注入北向资金全局 summary。"""
    logger.info("=== Step 5: 采集北向资金 ===")
    from app.services.multisource_ingest import ingest_market_data
    from app.services.stock_pool import get_daily_stock_pool

    core_codes = get_daily_stock_pool(trade_date=trade_date, tier=1)
    if not core_codes:
        logger.warning("核心池为空，跳过北向采集")
        return 0

    def fetch_northbound_global(target_date):
        """全局北向 summary — ingest 期望 fetcher(trade_date)->dict."""
        try:
            from app.services.northbound_data import bypass_proxy
            bypass_proxy()
            import akshare as ak
            df = ak.stock_hsgt_hist_em(symbol="沪股通")
            if df is not None and len(df) > 0:
                latest = df.iloc[-1]
                return {
                    "status": "ok",
                    "reason": "akshare_hsgt_hist",
                    "net_inflow_1d": float(latest.get("当日资金流入", 0) or 0),
                    "history_records": len(df),
                }
        except Exception as e:
            logger.warning("northbound_global_fetch_err: %s", e)
        return {"status": "missing", "reason": "fetch_failed"}

    def fetch_etf_summary(td):
        from app.services.etf_flow_data import fetch_etf_flow_summary_global
        return fetch_etf_flow_summary_global(td)

    result = ingest_market_data(
        db,
        trade_date=trade_date,
        stock_codes=[],
        core_pool_codes=core_codes,
        fetch_kline_history=None,
        fetch_hotspot_by_source=None,
        fetch_northbound_summary=fetch_northbound_global,
        fetch_etf_flow_summary=fetch_etf_summary,
    )
    nb_status = result.get("northbound_summary", {}).get("status", "unknown")
    logger.info("northbound: status=%s", nb_status)
    return 1 if nb_status == "ok" else 0


# ===========================================================================
# Step 6: market_state — 市场状态
# ===========================================================================

def step_market_state(db, trade_date: date):
    """计算并缓存市场状态。"""
    logger.info("=== Step 6: 计算市场状态 ===")
    from app.services.market_state import calc_and_cache_market_state

    result = calc_and_cache_market_state(trade_date=trade_date)
    logger.info("market_state: %s", result)
    return result


# ===========================================================================
# Main
# ===========================================================================

def _infer_trade_date() -> date:
    """推断最近完整交易日：如果今天是交易日且已收盘(>=15:30)用今天，否则回退。"""
    from datetime import datetime
    now = datetime.now()
    d = now.date()

    # 周末回退到周五
    if d.weekday() == 5:
        d = d - timedelta(days=1)
    elif d.weekday() == 6:
        d = d - timedelta(days=2)
    elif now.hour < 16:
        # 盘中或盘前，用昨天
        d = d - timedelta(days=1)
        if d.weekday() == 5:
            d = d - timedelta(days=1)
        elif d.weekday() == 6:
            d = d - timedelta(days=2)

    return d


def main():
    args = parse_args()
    trade_date = date.fromisoformat(args.trade_date) if args.trade_date else _infer_trade_date()
    logger.info("目标交易日: %s", trade_date)

    db = SessionLocal()
    try:
        steps_to_run = [args.step] if args.step else list(STEPS)

        if "stock_master" in steps_to_run:
            t0 = time.time()
            total = step_stock_master(db)
            logger.info("stock_master 完成: %d 只, 耗时 %.1fs", total, time.time() - t0)

        if "kline" in steps_to_run:
            t0 = time.time()
            count = step_kline(db, trade_date, args.kline_limit, args.batch_size, args.pool_only_top)
            logger.info("kline 完成: %d 条, 耗时 %.1fs", count, time.time() - t0)

        if "pool" in steps_to_run:
            t0 = time.time()
            core_size = step_pool(db, trade_date)
            logger.info("pool 完成: core=%d, 耗时 %.1fs", core_size, time.time() - t0)

        if "hotspot" in steps_to_run:
            t0 = time.time()
            count = step_hotspot(db, trade_date)
            logger.info("hotspot 完成: %d 条, 耗时 %.1fs", count, time.time() - t0)

        if "northbound" in steps_to_run:
            t0 = time.time()
            count = step_northbound(db, trade_date)
            logger.info("northbound 完成: %d 条, 耗时 %.1fs", count, time.time() - t0)

        if "market_state" in steps_to_run:
            t0 = time.time()
            state = step_market_state(db, trade_date)
            logger.info("market_state 完成: %s, 耗时 %.1fs", state, time.time() - t0)

    finally:
        db.close()

    logger.info("=== 数据引导完成 ===")


if __name__ == "__main__":
    main()
