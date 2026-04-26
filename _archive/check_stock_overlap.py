import sqlite3
conn = sqlite3.connect('data/app.db')
c = conn.cursor()

# What are the stocks in kline_daily on 2026-05-10?
c.execute("SELECT stock_code FROM kline_daily WHERE trade_date='2026-05-10' LIMIT 10")
print('Sample K-line stocks on 2026-05-10:', [r[0] for r in c.fetchall()])

# What are the stocks in published reports (ok/stale_ok)?
c.execute("SELECT DISTINCT stock_code FROM report WHERE published=1 AND is_deleted=0 AND quality_flag IN ('ok','stale_ok') LIMIT 10")
print('Sample report stocks:', [r[0] for r in c.fetchall()])

# Count overlap
c.execute("""
    SELECT COUNT(DISTINCT r.stock_code)
    FROM report r
    JOIN kline_daily k ON k.stock_code = r.stock_code
    WHERE r.published=1 AND r.is_deleted=0 AND r.quality_flag IN ('ok','stale_ok')
    AND k.trade_date='2026-05-10'
""")
print('Reports whose stock has kline on 2026-05-10 (distinct stocks):', c.fetchone()[0])

# Check all distinct dates in kline on recent dates
c.execute("SELECT DISTINCT trade_date FROM kline_daily ORDER BY trade_date DESC LIMIT 10")
print('Recent K-line dates:', [r[0] for r in c.fetchall()])

# Check what report stock_codes have ANY kline data
c.execute("""
    SELECT COUNT(DISTINCT r.stock_code), COUNT(DISTINCT k.stock_code)
    FROM report r
    LEFT JOIN kline_daily k ON k.stock_code = r.stock_code
    WHERE r.published=1 AND r.is_deleted=0 AND r.quality_flag IN ('ok','stale_ok')
""")
row = c.fetchone()
print(f'Report stocks total: {row[0]}, with any kline: {row[1]}')

conn.close()
