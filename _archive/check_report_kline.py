import sqlite3
conn = sqlite3.connect('data/app.db')
c = conn.cursor()

# What recent dates do report stocks have kline for?
c.execute("""
    SELECT DISTINCT k.trade_date
    FROM kline_daily k
    WHERE k.stock_code IN (
        SELECT DISTINCT stock_code FROM report 
        WHERE published=1 AND is_deleted=0 AND quality_flag IN ('ok','stale_ok')
    )
    ORDER BY k.trade_date DESC LIMIT 20
""")
print('Recent kline dates for REPORT stocks:')
for r in c.fetchall():
    print(' ', r[0])

# How many distinct stocks from reports have kline data?
c.execute("""
    SELECT COUNT(DISTINCT k.stock_code)
    FROM kline_daily k
    WHERE k.stock_code IN (
        SELECT DISTINCT stock_code FROM report 
        WHERE published=1 AND is_deleted=0 AND quality_flag IN ('ok','stale_ok')
    )
""")
print(f'\nDistinct report stocks with ANY kline: {c.fetchone()[0]}')

# For each report stock, what is the latest kline date?
c.execute("""
    SELECT k.stock_code, MAX(k.trade_date) max_date
    FROM kline_daily k
    WHERE k.stock_code IN (
        SELECT DISTINCT stock_code FROM report 
        WHERE published=1 AND is_deleted=0 AND quality_flag IN ('ok','stale_ok')
    )
    GROUP BY k.stock_code
    ORDER BY max_date DESC LIMIT 10
""")
print('\nTop stocks by latest kline date (report stocks):')
for r in c.fetchall():
    print(f'  {r[0]}: {r[1]}')

# What is the LATEST kline date for any report stock?
c.execute("""
    SELECT MAX(k.trade_date)
    FROM kline_daily k
    WHERE k.stock_code IN (
        SELECT DISTINCT stock_code FROM report 
        WHERE published=1 AND is_deleted=0 AND quality_flag IN ('ok','stale_ok')
    )
""")
print(f'\nLatest kline date for report stocks: {c.fetchone()[0]}')

conn.close()
