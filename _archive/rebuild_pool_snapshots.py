"""
Rebuild stock_pool_snapshot for trading dates 2026-04-08 to 2026-04-24.
Deletes existing FALLBACK rows first, then calls refresh_stock_pool for each date.
"""
import sys
import os
sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlite3
from datetime import date, timedelta

# Dates to rebuild: 2026-04-08 to 2026-04-24
# We'll detect trading days by checking if kline_daily has records
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'app.db')

def get_trading_dates():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        "SELECT DISTINCT trade_date FROM kline_daily "
        "WHERE trade_date >= '2026-04-08' AND trade_date <= '2026-04-24' "
        "ORDER BY trade_date"
    )
    dates = [row[0] for row in cur.fetchall()]
    conn.close()
    return dates

def check_existing_pool():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        "SELECT t.trade_date, t.status, COUNT(s.stock_code) as cnt "
        "FROM stock_pool_refresh_task t "
        "LEFT JOIN stock_pool_snapshot s ON s.refresh_task_id = t.task_id "
        "WHERE t.trade_date >= '2026-04-08' AND t.trade_date <= '2026-04-24' "
        "GROUP BY t.trade_date, t.status ORDER BY t.trade_date"
    )
    rows = cur.fetchall()
    conn.close()
    return rows

# Show existing pool state
print("=== Current pool snapshot state ===")
existing = check_existing_pool()
for row in existing:
    print(f"  {row[0]}: status={row[1]}, count={row[2]}")

# Get trading dates
trade_dates = get_trading_dates()
print(f"\nTrading dates to process ({len(trade_dates)}): {trade_dates}")

# Now use the app's service layer
import app.models  # noqa: F401 — register all ORM models in metadata
from app.core.db import SessionLocal, Base, engine
from app.services.stock_pool import refresh_stock_pool

# Delete existing FALLBACK tasks and their snapshots for these dates
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")

# Get FALLBACK task_ids for these dates
fallback_task_ids = []
for td in trade_dates:
    cur = conn.execute(
        "SELECT task_id FROM stock_pool_refresh_task WHERE trade_date=? AND status='FALLBACK'",
        (td,)
    )
    rows = cur.fetchall()
    for r in rows:
        fallback_task_ids.append(r[0])

if fallback_task_ids:
    print(f"\nDeleting {len(fallback_task_ids)} FALLBACK tasks and their snapshots...")
    for tid in fallback_task_ids:
        conn.execute("DELETE FROM stock_pool_snapshot WHERE refresh_task_id=?", (tid,))
        conn.execute("DELETE FROM stock_pool_refresh_task WHERE task_id=?", (tid,))
    conn.commit()
    print("Deleted existing FALLBACK records.")

# Also delete stock_score entries for these dates (UNIQUE constraint blocks re-insert)
dates_placeholder = ','.join(['?' for _ in trade_dates])
deleted_scores = conn.execute(
    f"DELETE FROM stock_score WHERE pool_date IN ({dates_placeholder})",
    trade_dates
).rowcount
conn.commit()
if deleted_scores:
    print(f"Deleted {deleted_scores} existing stock_score rows for these dates.")

conn.close()

# Now rebuild each date
print("\n=== Rebuilding pool snapshots ===")
total_ok = 0
total_fallback = 0
total_fail = 0

for td in trade_dates:
    db = SessionLocal()
    try:
        result = refresh_stock_pool(db, trade_date=td, force_rebuild=True)
        status = result.get("status", "?")
        core = result.get("core_pool_size", 0)
        standby = result.get("standby_pool_size", 0)
        print(f"  {td}: status={status}, core={core}, standby={standby}")
        if status == "COMPLETED":
            total_ok += 1
        elif status == "FALLBACK":
            total_fallback += 1
            print(f"    -> FALLBACK reason: {result.get('status_reason', '?')}")
        else:
            total_fail += 1
    except Exception as e:
        print(f"  {td}: ERROR - {e}")
        total_fail += 1
    finally:
        db.close()

print(f"\n=== Done: COMPLETED={total_ok}, FALLBACK={total_fallback}, FAIL={total_fail} ===")

# Final verification
print("\n=== Final pool snapshot state ===")
existing2 = check_existing_pool()
for row in existing2:
    print(f"  {row[0]}: status={row[1]}, count={row[2]}")
