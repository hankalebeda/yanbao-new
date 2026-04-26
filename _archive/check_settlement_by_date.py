import sqlite3
conn = sqlite3.connect('data/app.db')
c = conn.cursor()

# Count stocks with kline per day for report stocks
c.execute("""
    SELECT k.trade_date, COUNT(DISTINCT k.stock_code) stock_count
    FROM kline_daily k
    WHERE k.stock_code IN (
        SELECT DISTINCT stock_code FROM report 
        WHERE published=1 AND is_deleted=0 AND quality_flag IN ('ok','stale_ok')
    )
    GROUP BY k.trade_date
    ORDER BY k.trade_date DESC
    LIMIT 30
""")
print('K-line coverage per day (report stocks):')
for r in c.fetchall():
    print(f'  {r[0]}: {r[1]} stocks')

# For exec_date = 2026-04-07, how many reports are eligible (due_date <= 2026-04-07)?
# Reports with trade_date < 2026-04-07 and trade_date + 7 trading days <= 2026-04-07
# For simplification: reports with trade_date <= 2026-03-27 (approx)
c.execute("""
    SELECT COUNT(1) 
    FROM report r
    WHERE r.published=1 AND r.is_deleted=0 AND r.quality_flag IN ('ok','stale_ok')
    AND r.trade_date <= '2026-03-26'
""")
print(f'\nReports with trade_date <= 2026-03-26: {c.fetchone()[0]}')

c.execute("""
    SELECT COUNT(1) 
    FROM report r
    WHERE r.published=1 AND r.is_deleted=0 AND r.quality_flag IN ('ok','stale_ok')
    AND r.trade_date <= '2026-04-01'
""")
print(f'Reports with trade_date <= 2026-04-01: {c.fetchone()[0]}')

conn.close()
