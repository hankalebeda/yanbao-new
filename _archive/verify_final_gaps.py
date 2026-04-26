#!/usr/bin/env python3
"""Final verification of all data gaps for 2026-04-24."""
import sqlite3

conn = sqlite3.connect("data/app.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

TRADE_DATE = "2026-04-24"
DATASETS = [
    "stock_profile", "main_force_flow", "dragon_tiger_list",
    "northbound_summary", "etf_flow_summary", "margin_financing",
    "hotspot_top50", "kline_daily"
]

print("=" * 60)
print(f"FINAL GAP VERIFICATION for {TRADE_DATE}")
print("=" * 60)

# 1. Pool snapshot
cur.execute("SELECT COUNT(DISTINCT stock_code) as cnt FROM stock_pool_snapshot WHERE trade_date = ?", (TRADE_DATE,))
pool_size = cur.fetchone()["cnt"]
print(f"\nPool size: {pool_size}")

# 2. RDU coverage per dataset
print(f"\nRDU Coverage (2026-04-24):")
all_covered = True
for ds in DATASETS:
    cur.execute("""
        SELECT COUNT(DISTINCT rdu.stock_code) as covered
        FROM stock_pool_snapshot ps
        LEFT JOIN report_data_usage rdu ON rdu.stock_code = ps.stock_code
            AND rdu.trade_date = ?
            AND rdu.dataset_name = ?
        WHERE ps.trade_date = ?
    """, (TRADE_DATE, ds, TRADE_DATE))
    covered = cur.fetchone()["covered"]

    # Status breakdown
    cur.execute("""
        SELECT rdu.status, COUNT(*) as cnt
        FROM stock_pool_snapshot ps
        JOIN report_data_usage rdu ON rdu.stock_code = ps.stock_code
            AND rdu.trade_date = ?
            AND rdu.dataset_name = ?
        WHERE ps.trade_date = ?
        GROUP BY rdu.status
    """, (TRADE_DATE, ds, TRADE_DATE))
    status_rows = cur.fetchall()
    status_str = ", ".join(f"{r['status']}:{r['cnt']}" for r in status_rows)

    mark = "✓" if covered == pool_size else "✗"
    print(f"  {mark} {ds}: {covered}/{pool_size} [{status_str}]")
    if covered < pool_size:
        all_covered = False

# 3. hotspot_top50 table
cur.execute("SELECT COUNT(*) as cnt, MAX(trade_date) as latest FROM hotspot_top50")
r = cur.fetchone()
print(f"\nhotspot_top50 table: {r['cnt']} rows, latest trade_date={r['latest']}")

# 4. kline_daily table
cur.execute("SELECT COUNT(*) as cnt FROM kline_daily WHERE trade_date = ?", (TRADE_DATE,))
print(f"kline_daily table: {cur.fetchone()['cnt']} rows for {TRADE_DATE}")

# 5. market_state_cache
cur.execute("SELECT trade_date, market_state, cache_status FROM market_state_cache WHERE trade_date = ?", (TRADE_DATE,))
r = cur.fetchone()
if r:
    print(f"\nmarket_state_cache: {r['trade_date']} state={r['market_state']} status={r['cache_status']}")
else:
    print(f"\nmarket_state_cache: No entry for {TRADE_DATE} ✗")

# 6. stock_score
cur.execute("SELECT COUNT(*) as cnt, MAX(pool_date) as latest FROM stock_score")
r = cur.fetchone()
print(f"stock_score: {r['cnt']} total rows, latest pool_date={r['latest']}")

# 7. Data batch summary for 2026-04-24
print(f"\nData batches for {TRADE_DATE} (SUCCESS only):")
cur.execute("""
    SELECT source_name, batch_scope, batch_status, quality_flag, records_success
    FROM data_batch
    WHERE trade_date = ? AND batch_status = 'SUCCESS'
    GROUP BY source_name, batch_scope
    ORDER BY source_name
""", (TRADE_DATE,))
for r in cur.fetchall():
    print(f"  {r['source_name']} ({r['batch_scope']}): {r['batch_status']} {r['quality_flag']} success={r['records_success']}")

print(f"\n{'=' * 60}")
print(f"SUMMARY: {'ALL GAPS CLOSED ✓' if all_covered else 'SOME GAPS REMAIN ✗'}")
print("=" * 60)

conn.close()
