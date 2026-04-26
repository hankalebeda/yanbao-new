#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect("data/app.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("PRAGMA table_info(data_batch)")
cols = [r["name"] for r in cur.fetchall()]
print("data_batch cols:", cols)

cur.execute("SELECT * FROM data_batch ORDER BY created_at DESC LIMIT 3")
rows = cur.fetchall()
for r in rows:
    print(dict(r))

print()
# Find the kline batch for 2026-04-24
cur.execute("""
    SELECT batch_id, source_name, trade_date, batch_status, quality_flag, records_success, created_at
    FROM data_batch WHERE trade_date >= '2026-04-24' ORDER BY created_at DESC LIMIT 20
""")
for r in cur.fetchall():
    print("BATCH:", r["batch_id"][:8], r["source_name"], r["trade_date"], r["batch_status"], r["quality_flag"])

print()
# Find which pool stocks are missing kline RDU for 2026-04-24
cur.execute("""
    SELECT ps.stock_code
    FROM stock_pool_snapshot ps
    LEFT JOIN report_data_usage rdu ON rdu.stock_code = ps.stock_code 
        AND rdu.trade_date = '2026-04-24'
        AND rdu.dataset_name = 'kline_daily'
    WHERE ps.trade_date = '2026-04-24' AND rdu.usage_id IS NULL
    ORDER BY ps.stock_code
""")
missing_kline = [r["stock_code"] for r in cur.fetchall()]
print(f"Pool stocks missing kline RDU for 2026-04-24: {len(missing_kline)}")
print("First 10:", missing_kline[:10])

print()
# Find which pool stocks are missing northbound for 2026-04-24
cur.execute("""
    SELECT ps.stock_code
    FROM stock_pool_snapshot ps
    LEFT JOIN report_data_usage rdu ON rdu.stock_code = ps.stock_code 
        AND rdu.trade_date = '2026-04-24'
        AND rdu.dataset_name = 'northbound_summary'
        AND rdu.status NOT IN ('missing')
    WHERE ps.trade_date = '2026-04-24' AND rdu.usage_id IS NULL
    ORDER BY ps.stock_code
""")
missing_northbound = [r["stock_code"] for r in cur.fetchall()]
print(f"Pool stocks missing northbound RDU for 2026-04-24: {len(missing_northbound)}")
print("First 10:", missing_northbound[:10])

conn.close()
print("DONE")
