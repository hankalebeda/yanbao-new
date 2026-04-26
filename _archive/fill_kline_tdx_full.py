"""
用通达信数据全量补充 kline_daily：
Phase 1: 用 TDX 本地 .day 文件读取历史数据 (up to 2026-03-13)
Phase 2: 用 mootdx 在线 API 补充近期数据 (2026-03-14 to 2026-04-24)

策略：
- INSERT OR IGNORE：不覆盖已有数据，只填缺口
- 日期范围：2024-12-17 至今（约365天+缓冲）
- 对所有 stock_master 中的 active 股票执行
"""
import sqlite3
import struct
import uuid
import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, date
from pathlib import Path

DB_PATH = 'data/app.db'
TDX_VIPDOC = Path('C:/new_tdx/vipdoc')
BATCH_SIZE = 500  # DB insert batch size

# Date range
DATE_FROM = '2024-12-17'
DATE_TO = '2026-04-24'
TDX_LAST_DATE = '2026-03-13'  # Last date in local TDX files
ONLINE_FROM = '2026-03-14'   # Fetch from online for dates after this

SOURCE_BATCH_ID = 'tdx_bulk_fill_' + datetime.now().strftime('%Y%m%d')

def get_all_active_stocks():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT stock_code FROM stock_master WHERE is_delisted=0 OR is_delisted IS NULL ORDER BY stock_code")
    codes = [r[0] for r in cur.fetchall()]
    conn.close()
    return codes

def get_existing_kline_coverage():
    """Return set of (stock_code, trade_date) already in kline_daily"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f"SELECT stock_code, trade_date FROM kline_daily WHERE trade_date >= '{DATE_FROM}'")
    existing = set((r[0], r[1]) for r in cur.fetchall())
    conn.close()
    return existing

def day_file_for_stock(stock_code: str) -> Path | None:
    code = stock_code.split('.')[0]
    exch = stock_code.split('.')[1].lower() if '.' in stock_code else ''
    
    candidates = []
    if exch == 'sh' or code.startswith('6') or code.startswith('9'):
        candidates.append(TDX_VIPDOC / 'sh' / 'lday' / f'sh{code}.day')
    if exch == 'sz' or code.startswith(('0', '2', '3')):
        candidates.append(TDX_VIPDOC / 'sz' / 'lday' / f'sz{code}.day')
    if exch in ('bj', 'bjex') or code.startswith(('4', '8')):
        candidates.append(TDX_VIPDOC / 'bj' / 'lday' / f'bj{code}.day')
    
    # Fallback: try all exchanges
    if not candidates:
        candidates = [
            TDX_VIPDOC / 'sh' / 'lday' / f'sh{code}.day',
            TDX_VIPDOC / 'sz' / 'lday' / f'sz{code}.day',
            TDX_VIPDOC / 'bj' / 'lday' / f'bj{code}.day',
        ]
    
    for p in candidates:
        if p.exists():
            return p
    return None

def read_tdx_day_file(fpath: Path) -> list[dict]:
    """Read TDX .day file, return list of OHLCV dicts"""
    raw = fpath.read_bytes()
    n = len(raw) // 32
    out = []
    for i in range(n):
        rec = raw[i*32:(i+1)*32]
        date_int, op, hi, lo, cl, amt, vol, _ = struct.unpack('<IIIIIfII', rec)
        date_str = str(date_int)
        if len(date_str) != 8:
            continue
        yyyy, mm, dd = date_str[:4], date_str[4:6], date_str[6:]
        trade_date = f'{yyyy}-{mm}-{dd}'
        out.append({
            'trade_date': trade_date,
            'open': round(op / 100.0, 4),
            'high': round(hi / 100.0, 4),
            'low': round(lo / 100.0, 4),
            'close': round(cl / 100.0, 4),
            'amount': float(amt),
            'volume': int(vol),
        })
    return out

def insert_kline_batch(conn, rows: list):
    """Bulk insert kline rows using INSERT OR IGNORE"""
    if not rows:
        return 0
    cur = conn.cursor()
    sql = """INSERT OR IGNORE INTO kline_daily 
        (kline_id, stock_code, trade_date, open, high, low, close, volume, amount,
         adjust_type, is_suspended, source_batch_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'qfq', 0, ?, datetime('now'))"""
    data = [(str(uuid.uuid4()), r['stock_code'], r['trade_date'],
             r['open'], r['high'], r['low'], r['close'],
             r['volume'], r['amount'], SOURCE_BATCH_ID) for r in rows]
    cur.executemany(sql, data)
    inserted = cur.rowcount
    conn.commit()
    return inserted

# ======================================
# Phase 1: TDX local files (historical)
# ======================================
print('='*60)
print('Phase 1: TDX 本地文件历史数据')
print('='*60)

all_stocks = get_all_active_stocks()
print(f'Active stocks in stock_master: {len(all_stocks)}')

# Get coverage set to avoid unnecessary work
print('Loading existing kline coverage...')
existing = get_existing_kline_coverage()
print(f'Existing kline rows (since {DATE_FROM}): {len(existing)}')

conn = sqlite3.connect(DB_PATH)
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA synchronous=NORMAL')

total_inserted_p1 = 0
total_skipped_p1 = 0
no_file = 0
batch_rows = []

for i, stock_code in enumerate(all_stocks):
    fpath = day_file_for_stock(stock_code)
    if not fpath:
        no_file += 1
        continue
    
    records = read_tdx_day_file(fpath)
    for r in records:
        if r['trade_date'] < DATE_FROM or r['trade_date'] > TDX_LAST_DATE:
            continue
        if (stock_code, r['trade_date']) in existing:
            total_skipped_p1 += 1
            continue
        r['stock_code'] = stock_code
        batch_rows.append(r)
        existing.add((stock_code, r['trade_date']))  # Track to avoid duplicates
        
        if len(batch_rows) >= BATCH_SIZE:
            total_inserted_p1 += insert_kline_batch(conn, batch_rows)
            batch_rows = []
    
    if (i+1) % 500 == 0:
        # Flush remaining
        if batch_rows:
            total_inserted_p1 += insert_kline_batch(conn, batch_rows)
            batch_rows = []
        print(f'  [{i+1}/{len(all_stocks)}] inserted={total_inserted_p1} skipped={total_skipped_p1} no_file={no_file}')

# Final flush
if batch_rows:
    total_inserted_p1 += insert_kline_batch(conn, batch_rows)
    batch_rows = []

conn.close()
print(f'\nPhase 1 完成: inserted={total_inserted_p1}, skipped={total_skipped_p1}, no_file={no_file}')

# ======================================
# Phase 2: mootdx online (recent dates)
# ======================================
print()
print('='*60)
print(f'Phase 2: mootdx 在线获取 {ONLINE_FROM} 至 {DATE_TO}')
print('='*60)

try:
    from mootdx.quotes import Quotes
    client = Quotes.factory(market='std')
    print('mootdx Quotes 连接成功')
except Exception as e:
    print(f'mootdx 连接失败: {e}')
    client = None

if client:
    conn = sqlite3.connect(DB_PATH)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    
    # Reload existing after phase 1
    existing_p2 = get_existing_kline_coverage()
    
    total_inserted_p2 = 0
    total_failed_p2 = 0
    batch_rows = []
    
    for i, stock_code in enumerate(all_stocks):
        code = stock_code.split('.')[0]
        try:
            df = client.bars(symbol=code, frequency=9, offset=50)
            if df is None or df.empty:
                continue
            
            for _, row in df.iterrows():
                # datetime index
                if hasattr(row.name, 'date'):
                    trade_date = row.name.strftime('%Y-%m-%d')
                else:
                    trade_date = str(row.name)[:10]
                
                if trade_date < ONLINE_FROM or trade_date > DATE_TO:
                    continue
                if (stock_code, trade_date) in existing_p2:
                    continue
                
                batch_rows.append({
                    'stock_code': stock_code,
                    'trade_date': trade_date,
                    'open': round(float(row.get('open', 0)), 4),
                    'high': round(float(row.get('high', 0)), 4),
                    'low': round(float(row.get('low', 0)), 4),
                    'close': round(float(row.get('close', 0)), 4),
                    'volume': int(row.get('vol', row.get('volume', 0))),
                    'amount': float(row.get('amount', 0)),
                })
                existing_p2.add((stock_code, trade_date))
                
                if len(batch_rows) >= BATCH_SIZE:
                    total_inserted_p2 += insert_kline_batch(conn, batch_rows)
                    batch_rows = []
        
        except Exception as e:
            total_failed_p2 += 1
            if total_failed_p2 <= 5:
                print(f'  {stock_code} 失败: {e}')
        
        if (i+1) % 200 == 0:
            if batch_rows:
                total_inserted_p2 += insert_kline_batch(conn, batch_rows)
                batch_rows = []
            print(f'  [{i+1}/{len(all_stocks)}] online inserted={total_inserted_p2} failed={total_failed_p2}')
    
    # Final flush
    if batch_rows:
        total_inserted_p2 += insert_kline_batch(conn, batch_rows)
    
    conn.close()
    print(f'\nPhase 2 完成: inserted={total_inserted_p2}, failed={total_failed_p2}')

# ======================================
# Final summary
# ======================================
print()
print('='*60)
print('最终统计')
print('='*60)
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM kline_daily')
print(f'Total kline_daily rows: {cur.fetchone()[0]}')
cur.execute(f"SELECT trade_date, COUNT(*) FROM kline_daily WHERE trade_date >= '2026-04-01' GROUP BY trade_date ORDER BY trade_date")
for r in cur.fetchall():
    print(f'  {r[0]}: {r[1]} stocks')
conn.close()
