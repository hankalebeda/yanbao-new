#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect("data/app.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=== MARKET_STATE_CACHE ===")
cur.execute("SELECT trade_date, market_state, cache_status, state_reason, computed_at FROM market_state_cache ORDER BY trade_date DESC LIMIT 10")
rows = cur.fetchall()
if rows:
    for r in rows:
        print("  td=", r["trade_date"], "state=", r["market_state"], "status=", r["cache_status"], "computed=", r["computed_at"])
else:
    print("  EMPTY!")

print("\n=== HOTSPOT_TOP50 SCHEMA CHECK ===")
cur.execute("SELECT COUNT(*) FROM hotspot_top50")
print("  hotspot_top50 count:", cur.fetchone()[0])

print("\n=== RDU FOR 2026-04-24 COVERAGE ===")
td = "2026-04-24"
# How many pool stocks with each dataset
for ds in ["stock_profile", "main_force_flow", "dragon_tiger_list", "northbound_summary", "etf_flow_summary", "margin_financing", "hotspot_top50", "kline_daily"]:
    cur.execute(
        "SELECT COUNT(DISTINCT rdu.stock_code) FROM report_data_usage rdu "
        "JOIN stock_pool_snapshot ps ON ps.stock_code = rdu.stock_code AND ps.trade_date = ? "
        "WHERE rdu.trade_date = ? AND rdu.dataset_name = ? AND rdu.status NOT IN ('missing')",
        (td, td, ds)
    )
    covered = cur.fetchone()[0]
    print(f"  {ds}: {covered}/250 covered")

print("\n=== RDU FOR 2026-04-16 COVERAGE ===")
td16 = "2026-04-16"
for ds in ["stock_profile", "main_force_flow", "dragon_tiger_list", "northbound_summary", "etf_flow_summary", "margin_financing", "hotspot_top50", "kline_daily"]:
    cur.execute(
        "SELECT COUNT(DISTINCT rdu.stock_code) FROM report_data_usage rdu "
        "JOIN stock_pool_snapshot ps ON ps.stock_code = rdu.stock_code AND ps.trade_date = ? "
        "WHERE rdu.trade_date = ? AND rdu.dataset_name = ? AND rdu.status NOT IN ('missing')",
        (td16, td16, ds)
    )
    covered = cur.fetchone()[0]
    print(f"  {ds}: {covered}/250 covered (2026-04-16)")

print("\n=== MISSING POOL STOCKS (2026-04-24) with < 6 datasets ===")
cur.execute("""
    SELECT ps.stock_code, COUNT(DISTINCT rdu.dataset_name) as n_datasets
    FROM stock_pool_snapshot ps
    LEFT JOIN report_data_usage rdu ON rdu.stock_code = ps.stock_code 
        AND rdu.trade_date = ? 
        AND rdu.status NOT IN ('missing')
    WHERE ps.trade_date = ?
    GROUP BY ps.stock_code
    HAVING n_datasets < 6
    ORDER BY n_datasets ASC
    LIMIT 20
""", (td, td))
for r in cur.fetchall():
    print("  ", r["stock_code"], "datasets=", r["n_datasets"])

print("\n=== DATASETS IN RDU for 2026-04-24 (stock 000001.SZ) ===")
cur.execute("SELECT dataset_name, status FROM report_data_usage WHERE trade_date = ? AND stock_code = ? ORDER BY dataset_name", (td, "000001.SZ"))
for r in cur.fetchall():
    print("  ", r["dataset_name"], "->", r["status"])

print("\n=== STOCK SCORE latest ===")
cur.execute("SELECT MAX(pool_date) as latest, COUNT(*) FROM stock_score")
r = cur.fetchone()
print("  latest pool_date:", r[0], "total:", r[1])

print("\n=== data_batch recent ===")
cur.execute("SELECT id, batch_seq, status, created_at, batch_scope FROM data_batch ORDER BY id DESC LIMIT 10")
cur2 = conn.cursor()
cur2.execute("PRAGMA table_info(data_batch)")
cols = [r[1] for r in cur2.fetchall()]
print("  data_batch cols:", cols)
cur.execute("SELECT * FROM data_batch ORDER BY id DESC LIMIT 5")
rows = cur.fetchall()
for r in rows:
    print("  ", dict(r))

conn.close()
print("\n=== DONE ===")
