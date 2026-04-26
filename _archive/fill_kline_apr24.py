"""
填充 2026-04-24 的 kline_daily 数据
先检查 Tencent API 是否有 2026-04-24 数据，然后填充
"""
import sqlite3
import httpx
import time
import json

DB_PATH = 'data/app.db'

def tencent_sym(stock_code: str) -> str:
    code, exchange = stock_code.split('.')
    return f'sh{code}' if exchange == 'SH' else f'sz{code}'

def fetch_kline(sym: str, end_date: str = '2026-04-24', limit: int = 5):
    url = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
    params = {'param': f'{sym},day,,{end_date},{limit},qfq', '_var': 'kline_day'}
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.qq.com'}
    with httpx.Client(timeout=10, trust_env=False) as c:
        r = c.get(url, params=params, headers=headers)
    text = r.text
    if text.startswith('kline_day='):
        text = text[10:]
    data = json.loads(text)
    stock_data = data['data'].get(sym, {})
    kline = stock_data.get('qfqday', stock_data.get('day', []))
    return kline

# Get all pool stocks (from stock_pool_snapshot for recent dates)
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Get stocks in pool for 2026-04-23 (most complete date)
cur.execute("""
    SELECT DISTINCT stock_code FROM stock_pool_snapshot 
    WHERE trade_date = '2026-04-23'
    ORDER BY stock_code
""")
pool_stocks = [r[0] for r in cur.fetchall()]
print(f"Pool stocks for 2026-04-23: {len(pool_stocks)}")

# Check which already have 2026-04-24 data
cur.execute("SELECT stock_code FROM kline_daily WHERE trade_date = '2026-04-24'")
already_have = {r[0] for r in cur.fetchall()}
print(f"Already have 2026-04-24: {len(already_have)} stocks: {sorted(already_have)}")

# Check existing data for 2026-04-23 to know the stocks
cur.execute("""
    SELECT DISTINCT k.stock_code FROM kline_daily k
    WHERE k.trade_date = '2026-04-23'
    ORDER BY k.stock_code
""")
have_23 = [r[0] for r in cur.fetchall()]
print(f"Have 2026-04-23: {len(have_23)} stocks")

# Test first stock to see if 2026-04-24 has data
test_sym = tencent_sym(have_23[0])
kline = fetch_kline(test_sym, '2026-04-24', 3)
print(f"\nTest {have_23[0]} ({test_sym}) last 3 rows:")
for k in kline[-3:]:
    print(f"  {k}")

has_apr24 = any(k[0] == '2026-04-24' for k in kline)
print(f"Has 2026-04-24 data: {has_apr24}")

if not has_apr24:
    print("\n2026-04-24 data not yet available from Tencent. Skipping fill.")
    conn.close()
    exit(0)

# Fill 2026-04-24 for all stocks
print(f"\nFilling 2026-04-24 for {len(have_23)} stocks...")
inserted_total = 0
errors = 0

for stock_code in have_23:
    if stock_code in already_have:
        continue
    sym = tencent_sym(stock_code)
    try:
        kline = fetch_kline(sym, '2026-04-24', 3)
        row_24 = next((k for k in kline if k[0] == '2026-04-24'), None)
        if not row_24:
            continue
        
        # Get previous rows to compute MA
        cur.execute("""
            SELECT close FROM kline_daily 
            WHERE stock_code = ? AND trade_date < '2026-04-24'
            ORDER BY trade_date DESC LIMIT 60
        """, (stock_code,))
        prev_closes = [r[0] for r in cur.fetchall()]
        
        open_p = float(row_24[1])
        close_p = float(row_24[2])
        high_p = float(row_24[3])
        low_p = float(row_24[4])
        vol = float(row_24[5])
        
        # Tencent qfq: 688xxx volume is shares, others are lots
        if stock_code.startswith('688'):
            volume = vol
            amount = vol * (open_p + close_p) / 2
        else:
            volume = vol * 100
            amount = vol * 100 * (open_p + close_p) / 2
        
        # Compute MA
        all_closes = [close_p] + prev_closes
        ma5 = sum(all_closes[:5]) / min(5, len(all_closes)) if all_closes else None
        ma10 = sum(all_closes[:10]) / min(10, len(all_closes)) if len(all_closes) >= 3 else None
        ma20 = sum(all_closes[:20]) / min(20, len(all_closes)) if len(all_closes) >= 3 else None
        ma60 = sum(all_closes[:60]) / min(60, len(all_closes)) if len(all_closes) >= 3 else None
        
        cur.execute("""
            INSERT INTO kline_daily 
            (stock_code, trade_date, open, high, low, close, volume, amount, 
             ma5, ma10, ma20, ma60, adjust_type, is_suspended, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'qfq', 0, datetime('now'))
        """, (stock_code, '2026-04-24', open_p, high_p, low_p, close_p, volume, amount,
              ma5, ma10, ma20, ma60))
        inserted_total += 1
        
    except Exception as e:
        errors += 1
        if errors <= 5:
            print(f"  Error {stock_code}: {e}")
    
    time.sleep(0.05)
    if inserted_total % 50 == 0 and inserted_total > 0:
        conn.commit()
        print(f"  Progress: {inserted_total} inserted...")

conn.commit()
print(f"\nFilled 2026-04-24: {inserted_total} stocks, {errors} errors")

# Final check
cur.execute("SELECT trade_date, COUNT(*) FROM kline_daily WHERE trade_date >= '2026-04-21' GROUP BY trade_date ORDER BY trade_date")
print("\nkline_daily counts:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")

conn.close()
