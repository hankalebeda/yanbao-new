#!/usr/bin/env python3
"""Final verification - simple queries."""
import sqlite3

conn = sqlite3.connect("data/app.db", timeout=60)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL")
cur = conn.cursor()

TRADE_DATE = "2026-04-24"
DATASETS = [
    "stock_profile", "main_force_flow", "dragon_tiger_list",
    "northbound_summary", "etf_flow_summary", "margin_financing",
    "hotspot_top50", "kline_daily"
]

print(f"FINAL VERIFICATION {TRADE_DATE}")

cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_pool_snapshot WHERE trade_date = ?", (TRADE_DATE,))
pool_size = cur.fetchone()[0]
print(f"Pool size: {pool_size}")

print("\nRDU coverage per dataset:")
for ds in DATASETS:
    cur.execute("""
        SELECT status, COUNT(*) as cnt FROM report_data_usage
        WHERE trade_date = ? AND dataset_name = ?
        GROUP BY status
    """, (TRADE_DATE, ds))
    status_rows = cur.fetchall()
    total_rdu = sum(r["cnt"] for r in status_rows)
    status_str = " | ".join(f"{r['status']}:{r['cnt']}" for r in status_rows) or "NONE"
    mark = "✓" if total_rdu >= pool_size else "✗"
    print(f"  {mark} {ds}: {total_rdu} RDU entries [{status_str}]")

print()
cur.execute("SELECT COUNT(*) FROM hotspot_top50 WHERE trade_date = ?", (TRADE_DATE,))
print(f"hotspot_top50 rows: {cur.fetchone()[0]}")

cur.execute("SELECT COUNT(*) FROM kline_daily WHERE trade_date = ?", (TRADE_DATE,))
print(f"kline_daily rows: {cur.fetchone()[0]}")

cur.execute("SELECT market_state, cache_status FROM market_state_cache WHERE trade_date = ?", (TRADE_DATE,))
r = cur.fetchone()
print(f"market_state_cache: {dict(r) if r else 'MISSING'}")

conn.close()
print("DONE")
