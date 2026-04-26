import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

# Check kline_daily structure and recent 600519 data
print('=== kline_daily structure ===')
cur.execute('PRAGMA table_info("kline_daily")')
for c in cur.fetchall():
    print(c)

print()
print('=== 600519 recent kline rows ===')
cur.execute("SELECT * FROM kline_daily WHERE stock_code='600519.SH' ORDER BY trade_date DESC LIMIT 5")
for r in cur.fetchall():
    print(r)

# Check what stocks are in 2026-04-16 full pool
print()
print('=== 2026-04-16 pool stocks (first 20) ===')
cur.execute("SELECT DISTINCT stock_code FROM stock_pool_snapshot WHERE trade_date='2026-04-16' ORDER BY stock_code LIMIT 20")
for r in cur.fetchall():
    print(r[0])
    
# Count of unique stocks that NEED kline for 2026-04-17 to 2026-04-24
print()
print('=== Stocks in 2026-04-24 pool with kline gaps ===')
cur.execute("SELECT stock_code FROM stock_pool_snapshot WHERE trade_date='2026-04-24'")
pool_stocks = [r[0] for r in cur.fetchall()]
print(f'Pool stocks: {pool_stocks}')
for sc in pool_stocks:
    cur.execute("SELECT trade_date FROM kline_daily WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1", (sc,))
    row = cur.fetchone()
    latest = row[0] if row else 'NONE'
    print(f'  {sc}: latest kline = {latest}')

conn.close()
