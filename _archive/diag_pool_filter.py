"""
Diagnose why _build_candidates returns < 200 stocks for 2026-04-16.
Check each filter step.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlite3

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'app.db')

TARGET_DATE = '2026-04-16'
MIN_MARKET_CAP = 5_000_000_000
MIN_AMOUNT = 30_000_000

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# Step 1: Total stocks in kline for this date
r1 = conn.execute("SELECT COUNT(*) FROM kline_daily WHERE trade_date=?", (TARGET_DATE,)).fetchone()
print(f"kline_daily rows on {TARGET_DATE}: {r1[0]}")

# Step 2: Close stats
r2 = conn.execute("""
    SELECT COUNT(*), MIN(close), MAX(close), AVG(close),
           SUM(CASE WHEN close <= 0 THEN 1 ELSE 0 END) as zero_close
    FROM kline_daily WHERE trade_date=?
""", (TARGET_DATE,)).fetchone()
print(f"close: count={r2[0]}, min={r2[1]:.4f}, max={r2[2]:.4f}, avg={r2[3]:.4f}, zero_close={r2[4]}")

# Step 3: Amount stats
r3 = conn.execute("""
    SELECT COUNT(*), MIN(amount), MAX(amount), AVG(amount),
           SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) as zero_amt,
           SUM(CASE WHEN amount > 0 AND amount < 30000000 THEN 1 ELSE 0 END) as small_amt,
           SUM(CASE WHEN amount >= 30000000 THEN 1 ELSE 0 END) as ok_amt
    FROM kline_daily WHERE trade_date=?
""", (TARGET_DATE,)).fetchone()
print(f"amount: count={r3[0]}, min={r3[1]:.0f}, max={r3[2]:.0f}, avg={r3[3]:.0f}")
print(f"  zero_amt={r3[4]}, small_amt(0<amt<30M)={r3[5]}, ok_amt(>=30M)={r3[6]}")

# Step 4: Join with stock_master - circulating_shares
r4 = conn.execute("""
    SELECT COUNT(*),
           SUM(CASE WHEN s.circulating_shares IS NULL OR s.circulating_shares=0 THEN 1 ELSE 0 END) as no_shares,
           SUM(CASE WHEN s.circulating_shares > 0 THEN 1 ELSE 0 END) as has_shares
    FROM kline_daily k JOIN stock_master s ON s.stock_code=k.stock_code
    WHERE k.trade_date=?
""", (TARGET_DATE,)).fetchone()
print(f"joined with stock_master: total={r4[0]}, no_shares={r4[1]}, has_shares={r4[2]}")

# Step 5: Market cap filter
r5 = conn.execute("""
    SELECT COUNT(*),
           SUM(CASE WHEN k.close * s.circulating_shares < 5000000000 THEN 1 ELSE 0 END) as below_5b,
           SUM(CASE WHEN k.close * s.circulating_shares >= 5000000000 THEN 1 ELSE 0 END) as above_5b
    FROM kline_daily k JOIN stock_master s ON s.stock_code=k.stock_code
    WHERE k.trade_date=? AND k.close > 0 AND s.circulating_shares > 0 AND k.amount > 0
""", (TARGET_DATE,)).fetchone()
print(f"market_cap filter: below_5B={r5[1]}, above_5B={r5[2]}")

# Step 6: Amount filter (on stocks passing market cap)
r6 = conn.execute("""
    SELECT COUNT(*),
           SUM(CASE WHEN k.amount < 30000000 THEN 1 ELSE 0 END) as below_30m,
           SUM(CASE WHEN k.amount >= 30000000 THEN 1 ELSE 0 END) as above_30m
    FROM kline_daily k JOIN stock_master s ON s.stock_code=k.stock_code
    WHERE k.trade_date=? AND k.close > 0 AND s.circulating_shares > 0 AND k.amount > 0
      AND k.close * s.circulating_shares >= 5000000000
""", (TARGET_DATE,)).fetchone()
print(f"amount filter (on 5B+ stocks): below_30M={r6[1]}, above_30M={r6[2]}")

# Step 7: ST and delisted filter
r7 = conn.execute("""
    SELECT COUNT(*),
           SUM(CASE WHEN s.is_st THEN 1 ELSE 0 END) as is_st,
           SUM(CASE WHEN s.is_delisted THEN 1 ELSE 0 END) as is_delisted
    FROM kline_daily k JOIN stock_master s ON s.stock_code=k.stock_code
    WHERE k.trade_date=? AND k.close > 0 AND s.circulating_shares > 0 AND k.amount > 0
      AND k.close * s.circulating_shares >= 5000000000
      AND k.amount >= 30000000
""", (TARGET_DATE,)).fetchone()
print(f"qualified (pre-ST filter): count={r7[0]}, is_st={r7[1]}, is_delisted={r7[2]}")

# Step 8: Listing days
r8 = conn.execute("""
    SELECT COUNT(*),
           SUM(CASE WHEN (julianday('2026-04-16') - julianday(s.list_date)) < 365 THEN 1 ELSE 0 END) as new_stock,
           SUM(CASE WHEN (julianday('2026-04-16') - julianday(s.list_date)) >= 365 THEN 1 ELSE 0 END) as old_enough
    FROM kline_daily k JOIN stock_master s ON s.stock_code=k.stock_code
    WHERE k.trade_date=? AND k.close > 0 AND s.circulating_shares > 0 AND k.amount > 0
      AND k.close * s.circulating_shares >= 5000000000
      AND k.amount >= 30000000
      AND NOT s.is_st AND NOT s.is_delisted
""", (TARGET_DATE,)).fetchone()
print(f"listing days filter: new_stock(<365d)={r8[1]}, old_enough(>=365d)={r8[2]}")

# Step 9: Sample some stocks to verify data looks correct
print(f"\nSample stocks passing all filters:")
rows = conn.execute("""
    SELECT k.stock_code, k.close, k.amount, s.circulating_shares,
           k.close * s.circulating_shares as market_cap
    FROM kline_daily k JOIN stock_master s ON s.stock_code=k.stock_code
    WHERE k.trade_date=? AND k.close > 0 AND s.circulating_shares > 0 AND k.amount > 0
      AND k.close * s.circulating_shares >= 5000000000
      AND k.amount >= 30000000
      AND NOT s.is_st AND NOT s.is_delisted
      AND (julianday('2026-04-16') - julianday(s.list_date)) >= 365
    ORDER BY market_cap DESC LIMIT 10
""", (TARGET_DATE,)).fetchall()
for row in rows:
    print(f"  {row[0]}: close={row[1]:.2f}, amount={row[2]:.0f}, shares={row[3]:.0f}, mktcap={row[4]:.0f}")

conn.close()
