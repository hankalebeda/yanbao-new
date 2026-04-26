"""Check actual column names for key tables."""
import sqlite3

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()

key_tables = ['report', 'app_user', 'kline_daily', 'sim_account', 'sim_position', 'stock_pool_snapshot', 'market_state_cache', 'data_batch']
for t in key_tables:
    try:
        cols = cur.execute(f"PRAGMA table_info([{t}])").fetchall()
        col_names = [c[1] for c in cols]
        print(f"\n{t} ({len(col_names)} cols): {', '.join(col_names)}")
        # Show first row
        row = cur.execute(f"SELECT * FROM [{t}] LIMIT 1").fetchone()
        if row:
            print(f"  sample: {dict(zip(col_names, row))}")
    except Exception as e:
        print(f"{t}: ERROR - {e}")

conn.close()
