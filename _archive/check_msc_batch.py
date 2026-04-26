import sqlite3
import json

conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

# market_state_cache structure and coverage
cur.execute("PRAGMA table_info(market_state_cache)")
cols = [r[1] for r in cur.fetchall()]
print("market_state_cache cols:", cols)
cur.execute("SELECT trade_date, market_state, cache_status FROM market_state_cache ORDER BY trade_date DESC LIMIT 20")
print("\nmarket_state_cache recent entries:")
for r in cur.fetchall():
    print(f"  {r[0]}: state={r[1]}, status={r[2]}")

# data_batch source coverage
cur.execute("""
    SELECT source_name, MAX(trade_date), COUNT(*) 
    FROM data_batch 
    GROUP BY source_name 
    ORDER BY MAX(trade_date) DESC
""")
print("\ndata_batch source coverage:")
for r in cur.fetchall():
    print(f"  {r[0]}: last={r[1]}, batches={r[2]}")

# market_hotspot_batch - check if table exists
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='market_hotspot_batch'")
if cur.fetchone():
    cur.execute("SELECT * FROM market_hotspot_batch ORDER BY created_at DESC LIMIT 5")
    rows = cur.fetchall()
    cur.execute("PRAGMA table_info(market_hotspot_batch)")
    bcols = [r[1] for r in cur.fetchall()]
    print(f"\nmarket_hotspot_batch cols: {bcols}")
    for r in rows:
        print(f"  {dict(zip(bcols, r))}")
else:
    print("\nmarket_hotspot_batch: NOT EXISTS")

# Report generation tasks - what dates have tasks/reports
cur.execute("""
    SELECT target_date, COUNT(*), MAX(created_at)
    FROM report_generation_task
    GROUP BY target_date
    ORDER BY target_date DESC
    LIMIT 15
""")
print("\nreport_generation_task by date:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} tasks, last={r[2]}")

conn.close()
