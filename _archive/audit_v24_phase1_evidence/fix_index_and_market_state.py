"""回填 HS300/SH指数K线并重算 market_state"""
import os, sys, json, asyncio
from datetime import datetime, date, timedelta
from uuid import uuid4

os.environ['NO_PROXY'] = '*'
sys.path.insert(0, 'd:/yanbao-new')

import httpx
from sqlalchemy import text
from app.core.db import SessionLocal
from app.models import KlineDaily, MarketStateCache
from app.services.multisource_ingest import _create_batch, _create_usage_row


INDEX_CODES = {
    '000300': 'sh000300',  # 沪深300
    '000001': 'sh000001',  # 上证综指
}


async def fetch_tencent_index(tc_code: str, start: str, end: str) -> list[dict]:
    """Fetch index kline from Tencent."""
    url = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
    params = {'param': f"{tc_code},day,{start},{end},250,qfq"}
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://gu.qq.com/'}
    async with httpx.AsyncClient(timeout=15, trust_env=False, headers=headers) as client:
        r = await client.get(url, params=params)
        if r.status_code != 200: return []
        data = r.json()
        if data.get('code') != 0: return []
        stock_data = data.get('data', {}).get(tc_code, {})
        klines = stock_data.get('qfqday') or stock_data.get('day') or []
        out = []
        for k in klines:
            if len(k) < 6: continue
            try:
                out.append({
                    'trade_date': k[0],
                    'open': float(k[1]), 'close': float(k[2]),
                    'high': float(k[3]), 'low': float(k[4]),
                    'volume': float(k[5]) * 100,
                    'amount': float(k[8])*10000 if len(k)>8 and k[8] else 0.0,
                })
            except: continue
        return out


async def main():
    print("="*60)
    print("回填指数K线 (HS300/SH综指)")
    print("="*60)
    
    # Fetch both indices
    for idx_code, tc_code in INDEX_CODES.items():
        print(f"\n[{idx_code}] fetching {tc_code}...")
        rows = await fetch_tencent_index(tc_code, '2026-02-01', '2026-04-17')
        print(f"  got {len(rows)} rows, latest: {rows[-1]['trade_date'] if rows else 'none'}")
        
        if not rows: continue
        
        db = SessionLocal()
        try:
            # Create batch
            now = datetime.utcnow()
            batch = _create_batch(db, source_name='tencent', trade_date=date.fromisoformat(rows[-1]['trade_date']),
                batch_scope='backfill_index', batch_status='RUNNING', quality_flag='ok',
                started_at=now, finished_at=now)
            
            # Find existing dates
            existing = set(r[0] for r in db.execute(
                text("SELECT trade_date FROM kline_daily WHERE stock_code=:sc"), {"sc": idx_code}
            ).fetchall())
            
            inserted = 0
            for row in rows:
                td = date.fromisoformat(row['trade_date'])
                if td in existing or row['trade_date'] in existing:
                    continue
                db.add(KlineDaily(
                    kline_id=str(uuid4()),
                    stock_code=idx_code,
                    trade_date=td,
                    open=row['open'], high=row['high'], low=row['low'],
                    close=row['close'], volume=row['volume'], amount=row['amount'],
                    adjust_type='none',
                    atr_pct=None, turnover_rate=None,
                    ma5=None, ma10=None, ma20=None, ma60=None,
                    volatility_20d=None, hs300_return_20d=None,
                    is_suspended=False,
                    source_batch_id=batch.batch_id,
                    created_at=datetime.utcnow(),
                ))
                inserted += 1
            
            batch.batch_status = 'SUCCESS'
            batch.records_total = len(rows)
            batch.records_success = inserted
            batch.covered_stock_count = 1
            batch.core_pool_covered_count = 0
            batch.finished_at = datetime.utcnow()
            db.commit()
            print(f"  inserted {inserted} new rows")
        except Exception as e:
            db.rollback()
            print(f"  ERROR: {e}")
            import traceback; traceback.print_exc()
        finally:
            db.close()
    
    # Now recompute market_state for problematic dates
    print(f"\n[RECOMPUTE market_state]")
    from app.services.market_state import _load_reference_metrics, classify_market_state, _previous_trade_date
    
    problem_dates = ['2026-04-08','2026-04-09','2026-04-10','2026-04-13','2026-04-14','2026-04-15','2026-04-16','2026-04-17']
    
    db = SessionLocal()
    try:
        for td_str in problem_dates:
            td = date.fromisoformat(td_str)
            ref_date = _previous_trade_date(td)
            if not ref_date:
                print(f"  {td_str}: no previous trade date")
                continue
            metrics = _load_reference_metrics(db, ref_date)
            if not metrics:
                print(f"  {td_str}: still no metrics (ref={ref_date}) - SKIP")
                continue
            state = classify_market_state(metrics)
            
            # Update market_state_cache
            row = db.get(MarketStateCache, td)
            now = datetime.utcnow()
            reason = f"computed_from_reference_date={ref_date.isoformat()};cache_status=FRESH;recomputed"
            if row:
                row.market_state = state
                row.cache_status = 'FRESH'
                row.state_reason = reason
                row.reference_date = ref_date
                row.market_state_degraded = False
                row.computed_at = now
            else:
                db.add(MarketStateCache(
                    trade_date=td, market_state=state, cache_status='FRESH',
                    state_reason=reason, reference_date=ref_date,
                    market_state_degraded=False, computed_at=now, created_at=now
                ))
            print(f"  {td_str}: state={state} ref={ref_date} hs300_20d={metrics.hs300_return_20d*100:.2f}%")
        db.commit()
    finally:
        db.close()
    
    print("\nDONE")


if __name__ == '__main__':
    asyncio.run(main())
