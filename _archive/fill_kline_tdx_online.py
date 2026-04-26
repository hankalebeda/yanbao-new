"""
Phase 2: 用 mootdx 在线 API 补充近期 kline_daily
覆盖 2026-03-14 至 2026-04-24（TDX 本地数据截止后的部分）
对所有 stock_master 中 active 的股票执行
"""
import sqlite3, uuid, sys, os, time
sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime
from pathlib import Path

DB_PATH = 'data/app.db'
DATE_FROM = '2026-03-14'
DATE_TO = '2026-04-24'
SOURCE_BATCH_ID = 'mootdx_online_' + datetime.now().strftime('%Y%m%d')
BATCH_SIZE = 500
OFFSET = 50  # Last 50 trading bars (covers ~2.5 months, sufficient for 2026-03-14 onwards)

print('Phase 2: mootdx online API 近期K线补齐', flush=True)

# Get all active stocks
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("SELECT stock_code FROM stock_master WHERE is_delisted=0 OR is_delisted IS NULL ORDER BY stock_code")
all_stocks = [r[0] for r in cur.fetchall()]

# Load existing coverage for the online date range
cur.execute(f"SELECT stock_code, trade_date FROM kline_daily WHERE trade_date BETWEEN '{DATE_FROM}' AND '{DATE_TO}'")
existing = set((r[0], r[1]) for r in cur.fetchall())
conn.close()
print(f'Active stocks: {len(all_stocks)}, existing in date range: {len(existing)}', flush=True)

# Import mootdx
from mootdx.quotes import Quotes
client = Quotes.factory(market='std')
print('mootdx Quotes client ready', flush=True)

# DB connection
conn = sqlite3.connect(DB_PATH)
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA synchronous=NORMAL')

sql = """INSERT OR IGNORE INTO kline_daily
    (kline_id,stock_code,trade_date,open,high,low,close,volume,amount,
     adjust_type,is_suspended,source_batch_id,created_at)
    VALUES(?,?,?,?,?,?,?,?,?,'qfq',0,?,datetime('now'))"""

total_inserted = total_skipped = total_failed = 0
batch = []

for i, sc in enumerate(all_stocks):
    code = sc.split('.')[0]
    try:
        df = client.bars(symbol=code, frequency=9, offset=OFFSET)
        if df is None or df.empty:
            continue
        
        for idx, row in df.iterrows():
            # Get trade_date from datetime index
            if hasattr(idx, 'strftime'):
                td = idx.strftime('%Y-%m-%d')
            else:
                td = str(idx)[:10]
            
            if td < DATE_FROM or td > DATE_TO:
                continue
            if (sc, td) in existing:
                total_skipped += 1
                continue
            
            op = round(float(row.get('open', 0) or 0), 4)
            hi = round(float(row.get('high', 0) or 0), 4)
            lo = round(float(row.get('low', 0) or 0), 4)
            cl = round(float(row.get('close', 0) or 0), 4)
            vol = int(row.get('vol', row.get('volume', 0)) or 0)
            amt = float(row.get('amount', 0) or 0)
            
            if cl <= 0:
                continue
            
            batch.append((str(uuid.uuid4()), sc, td, op, hi, lo, cl, vol, amt, SOURCE_BATCH_ID))
            existing.add((sc, td))
    
    except Exception as e:
        total_failed += 1
        if total_failed <= 10:
            print(f'  {sc} failed: {e}', flush=True)
    
    if len(batch) >= BATCH_SIZE:
        conn.executemany(sql, batch)
        conn.commit()
        total_inserted += len(batch)
        batch = []
    
    if (i+1) % 200 == 0:
        print(f'[{i+1}/{len(all_stocks)}] inserted={total_inserted} skipped={total_skipped} failed={total_failed}', flush=True)

# Final flush
if batch:
    conn.executemany(sql, batch)
    conn.commit()
    total_inserted += len(batch)
conn.close()

print(f'\nPhase 2 done: inserted={total_inserted}, skipped={total_skipped}, failed={total_failed}', flush=True)

# Summary
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM kline_daily')
print(f'Total kline_daily: {cur.fetchone()[0]}', flush=True)
for dt in ['2026-03-14', '2026-03-31', '2026-04-16', '2026-04-23', '2026-04-24']:
    cur.execute('SELECT COUNT(*) FROM kline_daily WHERE trade_date=?', (dt,))
    print(f'  {dt}: {cur.fetchone()[0]} stocks', flush=True)
conn.close()
