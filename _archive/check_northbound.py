#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect("data/app.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# Check northbound detail
cur.execute("""
    SELECT rdu.status, COUNT(*) as cnt
    FROM stock_pool_snapshot ps
    LEFT JOIN report_data_usage rdu ON rdu.stock_code = ps.stock_code
        AND rdu.trade_date = '2026-04-24'
        AND rdu.dataset_name = 'northbound_summary'
    WHERE ps.trade_date = '2026-04-24'
    GROUP BY rdu.status
""")
print("Northbound RDU status breakdown for pool 2026-04-24:")
for r in cur.fetchall():
    print(f"  status={r['status']} count={r['cnt']}")

# Check what the 30 missing ones look like
cur.execute("""
    SELECT ps.stock_code, rdu.status, rdu.usage_id
    FROM stock_pool_snapshot ps
    LEFT JOIN report_data_usage rdu ON rdu.stock_code = ps.stock_code
        AND rdu.trade_date = '2026-04-24'
        AND rdu.dataset_name = 'northbound_summary'
    WHERE ps.trade_date = '2026-04-24' AND (rdu.status = 'missing' OR rdu.usage_id IS NULL)
    ORDER BY ps.stock_code
    LIMIT 10
""")
rows = cur.fetchall()
print(f"\nStocks with missing/null northbound (first 10):")
for r in rows:
    print(f"  {r['stock_code']} status={r['status']} usage_id={r['usage_id']}")

conn.close()
print("DONE")
