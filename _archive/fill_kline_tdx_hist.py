"""
用TDX本地文件全量补充kline_daily历史数据
日期范围：2024-12-17 至 2026-03-13
覆盖所有active股票
"""
import sqlite3, struct, uuid, sys, os
sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime
from pathlib import Path

DB_PATH = 'data/app.db'
TDX_VIPDOC = Path('C:/new_tdx/vipdoc')
DATE_FROM = '2024-12-17'
TDX_LAST_DATE = '2026-03-13'
SOURCE_BATCH_ID = 'tdx_bulk_' + datetime.now().strftime('%Y%m%d')
BATCH_SIZE = 2000

print('TDX Phase 1 - Loading all active stocks...', flush=True)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("SELECT stock_code FROM stock_master WHERE is_delisted=0 OR is_delisted IS NULL ORDER BY stock_code")
all_stocks = [r[0] for r in cur.fetchall()]
# Load existing coverage for the date range
cur.execute(f"SELECT stock_code, trade_date FROM kline_daily WHERE trade_date BETWEEN '{DATE_FROM}' AND '{TDX_LAST_DATE}'")
existing = set((r[0], r[1]) for r in cur.fetchall())
conn.close()
print(f'Active stocks: {len(all_stocks)}, existing kline in range: {len(existing)}', flush=True)

def find_day_file(stock_code):
    code = stock_code.split('.')[0]
    exch = stock_code.split('.')[1].lower() if '.' in stock_code else ''
    if exch == 'sh' or code.startswith(('6', '5', '9')):
        p = TDX_VIPDOC / 'sh' / 'lday' / f'sh{code}.day'
        if p.exists(): return p
    if exch == 'sz' or code.startswith(('0', '1', '2', '3')):
        p = TDX_VIPDOC / 'sz' / 'lday' / f'sz{code}.day'
        if p.exists(): return p
    if exch == 'bj' or code.startswith(('4', '8')):
        p = TDX_VIPDOC / 'bj' / 'lday' / f'bj{code}.day'
        if p.exists(): return p
    return None

def read_file(fpath):
    raw = fpath.read_bytes()
    out = []
    for i in range(len(raw) // 32):
        rec = raw[i*32:(i+1)*32]
        di, op, hi, lo, cl, amt, vol, _ = struct.unpack('<IIIIIfII', rec)
        ds = str(di)
        if len(ds) != 8: continue
        td = f'{ds[:4]}-{ds[4:6]}-{ds[6:]}'
        if DATE_FROM <= td <= TDX_LAST_DATE:
            out.append((td, round(op/100.0,4), round(hi/100.0,4), round(lo/100.0,4),
                        round(cl/100.0,4), int(vol), float(amt)))
    return out

conn = sqlite3.connect(DB_PATH)
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA synchronous=NORMAL')
sql = """INSERT OR IGNORE INTO kline_daily
    (kline_id,stock_code,trade_date,open,high,low,close,volume,amount,
     adjust_type,is_suspended,source_batch_id,created_at)
    VALUES(?,?,?,?,?,?,?,?,?,'qfq',0,?,datetime('now'))"""

total_inserted = total_skipped = no_file = 0
batch = []

for i, sc in enumerate(all_stocks):
    fp = find_day_file(sc)
    if not fp:
        no_file += 1
        continue
    for (td, op, hi, lo, cl, vol, amt) in read_file(fp):
        if (sc, td) in existing:
            total_skipped += 1
        else:
            batch.append((str(uuid.uuid4()), sc, td, op, hi, lo, cl, vol, amt, SOURCE_BATCH_ID))
            existing.add((sc, td))
        if len(batch) >= BATCH_SIZE:
            conn.executemany(sql, batch)
            conn.commit()
            total_inserted += len(batch)
            batch = []
    if (i+1) % 500 == 0:
        print(f'[{i+1}/{len(all_stocks)}] inserted={total_inserted} skipped={total_skipped} no_file={no_file}', flush=True)

if batch:
    conn.executemany(sql, batch)
    conn.commit()
    total_inserted += len(batch)
conn.close()

print(f'\nPhase1 done: inserted={total_inserted}, skipped={total_skipped}, no_file={no_file}', flush=True)
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM kline_daily')
print(f'Total kline_daily: {cur.fetchone()[0]}', flush=True)
for dt in ['2025-12-31', '2026-01-31', '2026-02-28', '2026-03-13']:
    cur.execute('SELECT COUNT(*) FROM kline_daily WHERE trade_date=?', (dt,))
    cnt = cur.fetchone()[0]
    print(f'  {dt}: {cnt} stocks', flush=True)
conn.close()
