import sqlite3
conn = sqlite3.connect('data/app.db')
c = conn.cursor()

c.execute("SELECT COUNT(DISTINCT stock_code) FROM kline_daily WHERE trade_date='2026-05-10'")
print('Stocks with kline on 2026-05-10:', c.fetchone()[0])

c.execute("SELECT COUNT(DISTINCT stock_code) FROM kline_daily WHERE trade_date='2026-04-30'")
print('Stocks with kline on 2026-04-30:', c.fetchone()[0])

c.execute("""
    SELECT COUNT(1) 
    FROM report r 
    WHERE r.published=1 AND r.is_deleted=0 
    AND r.quality_flag IN ('ok', 'stale_ok')
    AND EXISTS (
        SELECT 1 FROM kline_daily k 
        WHERE k.stock_code = r.stock_code AND k.trade_date='2026-05-10'
    )
""")
print('Reports with stock kline on 2026-05-10:', c.fetchone()[0])

# Check what's already in settlement_result
c.execute("SELECT COUNT(1) FROM settlement_result WHERE exit_trade_date='2026-05-10'")
print('Settlement records with exit 2026-05-10:', c.fetchone()[0])

# Check sample of reports without kline on exit date
c.execute("""
    SELECT r.stock_code, COUNT(1) cnt
    FROM report r
    WHERE r.published=1 AND r.is_deleted=0 
    AND r.quality_flag IN ('ok', 'stale_ok')
    AND NOT EXISTS (
        SELECT 1 FROM kline_daily k 
        WHERE k.stock_code = r.stock_code AND k.trade_date='2026-05-10'
    )
    GROUP BY r.stock_code
    ORDER BY cnt DESC LIMIT 5
""")
print('Top stocks without kline on 2026-05-10:')
for r in c.fetchall():
    print(' ', r)

conn.close()
