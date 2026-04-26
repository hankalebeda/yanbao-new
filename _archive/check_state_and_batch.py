import sqlite3
import json
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

# Check market_state_cache structure and recent data
cur.execute("PRAGMA table_info(market_state_cache)")
cols = [r[1] for r in cur.fetchall()]
print("market_state_cache cols:", cols)
cur.execute("SELECT * FROM market_state_cache ORDER BY rowid DESC LIMIT 5")
rows = cur.fetchall()
for r in rows:
    row_dict = dict(zip(cols, r))
    # Truncate long JSON
    for k, v in row_dict.items():
        if isinstance(v, str) and len(v) > 100:
            row_dict[k] = v[:100] + '...'
    print(row_dict)

print()
# Check data_batch - what types are there
cur.execute("PRAGMA table_info(data_batch)")
db_cols = [r[1] for r in cur.fetchall()]
print("data_batch cols:", db_cols)
cur.execute("SELECT batch_type, COUNT(*), MAX(created_at) FROM data_batch GROUP BY batch_type ORDER BY MAX(created_at) DESC")
print("data_batch by type:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} rows, last={r[2]}")

print()
# Check capital flow in data_batch or other tables
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE '%capital%' OR name LIKE '%flow%' OR name LIKE '%north%' OR name LIKE '%etf%')")
print("Capital/flow/north/etf tables:", [r[0] for r in cur.fetchall()])

# Check if capital flow data is in data_batch
cur.execute("""
    SELECT batch_type, trade_date, status, summary_json
    FROM data_batch 
    WHERE batch_type IN ('capital_flow', 'northbound', 'etf_flow', 'capital')
    ORDER BY trade_date DESC LIMIT 10
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"batch: {r[0]}, date={r[1]}, status={r[2]}, summary={str(r[3])[:100] if r[3] else None}")
else:
    # Try different approach
    cur.execute("SELECT DISTINCT batch_type FROM data_batch ORDER BY batch_type")
    print("All batch_types:", [r[0] for r in cur.fetchall()])

conn.close()
