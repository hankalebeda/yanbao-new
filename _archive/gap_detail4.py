#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect("data/app.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Find kline batch for 2026-04-24
cur.execute("""
    SELECT batch_id, source_name, trade_date, batch_status, quality_flag, covered_stock_count, created_at
    FROM data_batch WHERE trade_date = '2026-04-24' AND source_name LIKE '%tdx%' OR (trade_date = '2026-04-24' AND batch_scope = 'full_market')
    ORDER BY created_at DESC LIMIT 10
""")
rows = cur.fetchall()
print("Kline batches for 2026-04-24:")
for r in rows:
    print(" ", r["batch_id"][:8], r["source_name"], r["batch_status"], r["quality_flag"], r["covered_stock_count"])

# Check for full_market scope batch
cur.execute("""
    SELECT batch_id, source_name, trade_date, batch_scope, batch_status, quality_flag, covered_stock_count, created_at
    FROM data_batch WHERE trade_date = '2026-04-24' AND batch_scope = 'full_market'
    ORDER BY created_at DESC LIMIT 5
""")
rows = cur.fetchall()
print("\nfull_market scope batches for 2026-04-24:")
for r in rows:
    print(" ", r["batch_id"][:8], r["source_name"], r["batch_status"], r["quality_flag"])

# Check 2026-04-24 kline_daily stock_code from kline table
cur.execute("SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM kline_daily WHERE trade_date = '2026-04-24'")
r = cur.fetchone()
print(f"\nkline_daily for 2026-04-24: count={r[0]}, min={r[1]}, max={r[2]}")

# Check source_batch_id for kline_daily on 2026-04-24
cur.execute("""
    SELECT source_batch_id, COUNT(*) as cnt
    FROM kline_daily
    WHERE trade_date = '2026-04-24'
    GROUP BY source_batch_id
    ORDER BY cnt DESC LIMIT 5
""")
print("\nkline_daily source_batch_id for 2026-04-24:")
for r in cur.fetchall():
    print(" ", r["source_batch_id"], "cnt=", r["cnt"])

# Verify the batch in data_batch
cur.execute("""
    SELECT k.source_batch_id, d.source_name, d.batch_status, d.quality_flag, d.covered_stock_count
    FROM kline_daily k
    LEFT JOIN data_batch d ON d.batch_id = k.source_batch_id
    WHERE k.trade_date = '2026-04-24'
    GROUP BY k.source_batch_id
    LIMIT 5
""")
print("\nkline batch details:")
for r in cur.fetchall():
    print(" ", r["source_batch_id"][:8] if r["source_batch_id"] else "None", r["source_name"], r["batch_status"], r["quality_flag"])

conn.close()
print("DONE")
