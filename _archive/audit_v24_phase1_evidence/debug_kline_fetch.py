"""Check kline data and recent batches for 2026-04-07"""
import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()

# Most recent kline backfill batch
cur.execute("""SELECT batch_id, batch_status, quality_flag, records_total, records_success, records_failed, status_reason
FROM data_batch WHERE trade_date='2026-04-07' AND source_name='eastmoney' 
ORDER BY created_at DESC LIMIT 3""")
batches = cur.fetchall()
print('Kline backfill batches for 2026-04-07:', batches)

# Check kline records for 2026-04-07
cur.execute("""SELECT count(*) FROM kline_daily WHERE trade_date='2026-04-07'""")
print('\nTotal kline records for 2026-04-07:', cur.fetchone()[0])

# Sample of stocks with kline for 2026-04-07
cur.execute("""SELECT stock_code FROM kline_daily WHERE trade_date='2026-04-07' LIMIT 10""")
print('Sample stocks with kline on 2026-04-07:', [r[0] for r in cur.fetchall()])

# Try fetching kline for one specific stock to debug
print('\n=== Testing kline fetch directly ===')
import os, sys
os.environ['NO_PROXY'] = '*'
sys.path.insert(0, 'd:/yanbao-new')
import asyncio
from app.services.market_data import fetch_recent_klines

async def test():
    # Try a few stocks from the failing list
    stocks = ['600000.SH', '000001.SZ', '601398.SH']
    for s in stocks:
        rows = await fetch_recent_klines(s, limit=30)
        if rows:
            dates = [r.get('date', r.get('trade_date', '?'))[:10] for r in rows[-5:]]
            print(f'  {s}: last 5 dates: {dates}')
        else:
            print(f'  {s}: NO DATA')

asyncio.run(test())
