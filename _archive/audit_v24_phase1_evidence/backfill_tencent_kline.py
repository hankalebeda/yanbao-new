"""
Backfill kline_daily using Tencent API (eastmoney is blocked).
Directly insert KlineDaily rows and create usage records for 200 stocks × 8 dates.
"""
import os, sys, json, asyncio
from datetime import datetime, date
from uuid import uuid4

os.environ['NO_PROXY'] = '*'
sys.path.insert(0, 'd:/yanbao-new')

import httpx
from app.core.db import SessionLocal
from app.models import KlineDaily, StockMaster
from app.services.multisource_ingest import _create_batch, _create_usage_row, _normalize_kline_row, _load_stock_map

TARGET_DATES = ['2026-04-07','2026-04-08','2026-04-09','2026-04-10',
                '2026-04-13','2026-04-14','2026-04-15','2026-04-16']

with open('_archive/audit_v24_phase1_evidence/core_pool.json') as f:
    CORE_STOCKS = json.load(f)['core_stocks']
print(f"[INFO] 核心股票池: {len(CORE_STOCKS)} 只")


def _to_tencent_code(stock_code: str) -> str:
    """Convert 601888.SH -> sh601888"""
    code, market = stock_code.split('.')
    return f"{market.lower()}{code}"


async def fetch_tencent_kline(stock_code: str, start: str, end: str, client: httpx.AsyncClient) -> list[dict]:
    """Fetch daily kline from Tencent API. Returns list of {date, open, close, high, low, volume, amount}"""
    tc_code = _to_tencent_code(stock_code)
    url = f"https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
    params = {
        'param': f"{tc_code},day,{start},{end},150,qfq"
    }
    try:
        r = await client.get(url, params=params, timeout=12)
        if r.status_code != 200:
            return []
        data = r.json()
        if data.get('code') != 0:
            return []
        stock_data = data.get('data', {}).get(tc_code, {})
        # Try qfqday first (qfq adjusted), fall back to day
        klines = stock_data.get('qfqday') or stock_data.get('day') or []
        out = []
        for k in klines:
            if len(k) < 6:
                continue
            try:
                out.append({
                    'trade_date': k[0],
                    'open': float(k[1]),
                    'close': float(k[2]),
                    'high': float(k[3]),
                    'low': float(k[4]),
                    'volume': float(k[5]) * 100,  # tencent volume is in 手 (100 shares)
                    'amount': float(k[8]) * 10000 if len(k) > 8 and k[8] else 0.0,  # 成交额万元
                })
            except (ValueError, TypeError):
                continue
        return out
    except Exception as e:
        return []


async def fetch_all_histories(stocks: list, start: str, end: str, concurrency: int = 10) -> dict:
    """Fetch kline histories for all stocks. Returns {stock_code: [kline_rows]}"""
    results = {}
    semaphore = asyncio.Semaphore(concurrency)
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://gu.qq.com/'}
    
    async with httpx.AsyncClient(timeout=15, trust_env=False, headers=headers) as client:
        async def _fetch_one(sc):
            async with semaphore:
                rows = await fetch_tencent_kline(sc, start, end, client)
                return sc, rows
        
        tasks = [_fetch_one(sc) for sc in stocks]
        completed = 0
        for task in asyncio.as_completed(tasks):
            sc, rows = await task
            results[sc] = rows
            completed += 1
            if completed % 20 == 0:
                print(f"  进度: {completed}/{len(stocks)}")
    
    return results


def backfill_for_date(db, trade_date: str, histories: dict, stock_map: dict) -> dict:
    """For a given trade_date, insert missing KlineDaily rows and create usage records."""
    target = date.fromisoformat(trade_date)
    now = datetime.utcnow()
    
    # Find stocks with kline for this date
    existing = set(
        row[0] for row in 
        db.query(KlineDaily.stock_code)
        .filter(KlineDaily.trade_date == target)
        .all()
    )
    
    # Create backfill batch
    batch = _create_batch(db, source_name='tencent', trade_date=target,
        batch_scope='backfill_missing', batch_status='RUNNING', quality_flag='ok',
        started_at=now, finished_at=now)
    
    inserted = 0
    skipped = 0
    failed = 0
    
    for stock_code in CORE_STOCKS:
        if stock_code not in stock_map:
            failed += 1
            continue
        
        rows = histories.get(stock_code, [])
        # Find the row for this date
        target_row = next((r for r in rows if r['trade_date'] == trade_date), None)
        if not target_row:
            failed += 1
            continue
        
        # Build full history for this stock (up to target_date)
        history = [r for r in rows if r['trade_date'] <= trade_date]
        
        if stock_code in existing:
            # Already has kline - just create usage record
            _create_usage_row(db, trade_date=target, stock_code=stock_code,
                dataset_name='kline_daily', source_name='tencent',
                batch_id=batch.batch_id, fetch_time=now, status='ok', status_reason=None)
            skipped += 1
            continue
        
        # Insert KlineDaily row
        try:
            normalized = _normalize_kline_row(stock_map[stock_code], history)
            db.add(KlineDaily(
                kline_id=str(uuid4()),
                stock_code=stock_code,
                trade_date=target,
                open=normalized['open'],
                high=normalized['high'],
                low=normalized['low'],
                close=normalized['close'],
                volume=normalized['volume'],
                amount=normalized['amount'],
                adjust_type=normalized['adjust_type'],
                atr_pct=normalized['atr_pct'],
                turnover_rate=normalized['turnover_rate'],
                ma5=normalized['ma5'],
                ma10=normalized['ma10'],
                ma20=normalized['ma20'],
                ma60=normalized['ma60'],
                volatility_20d=normalized['volatility_20d'],
                hs300_return_20d=normalized['hs300_return_20d'],
                is_suspended=normalized['is_suspended'],
                source_batch_id=batch.batch_id,
                created_at=datetime.utcnow(),
            ))
            _create_usage_row(db, trade_date=target, stock_code=stock_code,
                dataset_name='kline_daily', source_name='tencent',
                batch_id=batch.batch_id, fetch_time=now, status='ok', status_reason=None)
            inserted += 1
        except Exception as e:
            failed += 1
            print(f"    [FAIL] {stock_code}: {e}")
    
    # Update batch
    batch.batch_status = 'SUCCESS' if failed == 0 else 'PARTIAL_SUCCESS'
    batch.quality_flag = 'ok' if failed < 20 else 'degraded'
    batch.records_total = len(CORE_STOCKS)
    batch.records_success = inserted + skipped
    batch.records_failed = failed
    batch.covered_stock_count = inserted + skipped
    batch.core_pool_covered_count = inserted + skipped
    batch.finished_at = datetime.utcnow()
    db.commit()
    
    return {'inserted': inserted, 'skipped': skipped, 'failed': failed}


async def main():
    print("=" * 60)
    print("腾讯K线批量回填")
    print("=" * 60)
    
    # Fetch all histories once (150 days, covers all target dates)
    print(f"\n[STEP 1] 抓取 {len(CORE_STOCKS)} 只股票历史K线 (2026-03-01 to 2026-04-17)...")
    histories = await fetch_all_histories(CORE_STOCKS, '2026-03-01', '2026-04-17', concurrency=15)
    
    got_data = sum(1 for rows in histories.values() if rows)
    print(f"[STEP 1] 完成: {got_data}/{len(CORE_STOCKS)} 只股票获取成功")
    
    # Save histories
    with open('_archive/audit_v24_phase1_evidence/tencent_histories.json', 'w') as f:
        json.dump(histories, f)
    print("  已保存到 tencent_histories.json")
    
    # Load stock map
    db = SessionLocal()
    try:
        stock_map = _load_stock_map(db, CORE_STOCKS)
        print(f"[STEP 2] stock_map: {len(stock_map)} 只")
    finally:
        db.close()
    
    # Backfill for each date
    print(f"\n[STEP 3] 逐日回填kline_daily...")
    all_results = {}
    for td in TARGET_DATES:
        db = SessionLocal()
        try:
            result = backfill_for_date(db, td, histories, stock_map)
            print(f"  {td}: inserted={result['inserted']}, skipped={result['skipped']}, failed={result['failed']}")
            all_results[td] = result
        finally:
            db.close()
    
    # Summary
    print(f"\n[SUMMARY] Total inserted: {sum(r['inserted'] for r in all_results.values())}")
    print(f"          Total skipped: {sum(r['skipped'] for r in all_results.values())}")
    print(f"          Total failed: {sum(r['failed'] for r in all_results.values())}")
    
    # Final kline_ok count per date
    print(f"\n[VERIFY] kline_ok counts per date:")
    import sqlite3
    c = sqlite3.connect('data/app.db')
    cur = c.cursor()
    pool_str = ','.join([f"'{s}'" for s in CORE_STOCKS])
    for td in TARGET_DATES:
        cur.execute(f"""SELECT count(DISTINCT stock_code) FROM report_data_usage 
        WHERE trade_date='{td}' AND dataset_name='kline_daily' AND status='ok'
        AND stock_code IN ({pool_str})""")
        kline_ok = cur.fetchone()[0]
        print(f"  {td}: kline_ok={kline_ok}/200")


if __name__ == '__main__':
    asyncio.run(main())
