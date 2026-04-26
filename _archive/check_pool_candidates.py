import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

# Check 2026-04-23 kline vs stock_master join
cur.execute("""
    SELECT k.stock_code, k.close, k.amount, k.ma20, s.circulating_shares, s.list_date, s.is_st
    FROM kline_daily k
    JOIN stock_master s ON s.stock_code = k.stock_code
    WHERE k.trade_date = '2026-04-23'
    AND s.circulating_shares IS NOT NULL
    AND k.amount > 0
    ORDER BY k.amount DESC
    LIMIT 20
""")
rows = cur.fetchall()
print(f'Total qualifying rows for 2026-04-23: {len(rows)}')
print()
for r in rows[:10]:
    stock_code, close, amount, ma20, shares, list_date, is_st = r
    market_cap = (close or 0) * (shares or 0)
    print(f'{stock_code}: close={close}, amount={amount:.0f}, mktcap={market_cap:.0f}, list={list_date}, is_st={is_st}')

# Count how many pass all filters
cur.execute("""
    SELECT COUNT(*) FROM (
        SELECT k.stock_code
        FROM kline_daily k
        JOIN stock_master s ON s.stock_code = k.stock_code
        WHERE k.trade_date = '2026-04-23'
        AND s.circulating_shares IS NOT NULL
        AND k.amount >= 30000000
        AND k.close * s.circulating_shares >= 5000000000
        AND s.list_date <= date('2026-04-23', '-365 days')
        AND (s.is_st IS NULL OR s.is_st = 0)
        AND (s.is_delisted IS NULL OR s.is_delisted = 0)
    )
""")
cnt = cur.fetchone()[0]
print(f'\nStocks passing ALL filters for 2026-04-23: {cnt}')

# Check 2026-04-16 for comparison
cur.execute("""
    SELECT COUNT(*) FROM (
        SELECT k.stock_code
        FROM kline_daily k
        JOIN stock_master s ON s.stock_code = k.stock_code
        WHERE k.trade_date = '2026-04-16'
        AND s.circulating_shares IS NOT NULL
        AND k.amount >= 30000000
        AND k.close * s.circulating_shares >= 5000000000
        AND s.list_date <= date('2026-04-16', '-365 days')
        AND (s.is_st IS NULL OR s.is_st = 0)
        AND (s.is_delisted IS NULL OR s.is_delisted = 0)
    )
""")
cnt16 = cur.fetchone()[0]
print(f'Stocks passing ALL filters for 2026-04-16: {cnt16}')

conn.close()
