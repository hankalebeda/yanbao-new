#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect("data/app.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=== HOTSPOT_RAW ===")
cur.execute("SELECT platform, COUNT(*) as cnt, MAX(fetch_time) as latest FROM hotspot_raw GROUP BY platform")
for r in cur.fetchall():
    print("  platform=", r["platform"], "cnt=", r["cnt"], "latest=", r["latest"])

print("\n=== HOTSPOT_NORMALIZED ===")
cur.execute("SELECT COUNT(*) as cnt, MAX(created_at) as latest FROM hotspot_normalized")
r = cur.fetchone()
print("  cnt=", r["cnt"], "latest=", r["latest"])

print("\n=== HOTSPOT_TOP50 details ===")
cur.execute("SELECT source_name, trade_date, COUNT(*) as cnt FROM hotspot_top50 GROUP BY source_name, trade_date ORDER BY trade_date DESC LIMIT 20")
for r in cur.fetchall():
    print("  source=", r["source_name"], "date=", r["trade_date"], "cnt=", r["cnt"])

print("\n=== MARKET_STATE_CACHE ===")
cur.execute("SELECT state_key, state_value, updated_at FROM market_state_cache ORDER BY updated_at DESC")
for r in cur.fetchall():
    print("  key=", r["state_key"], "val=", str(r["state_value"])[:60], "updated=", r["updated_at"])

TODAY = "2026-04-24"

print("\n=== POOL_STOCKS for " + TODAY + " ===")
cur.execute("SELECT stock_code FROM stock_pool_snapshot WHERE trade_date = ?", (TODAY,))
pool_codes = [r["stock_code"] for r in cur.fetchall()]
print("  total pool:", len(pool_codes), "first 10:", pool_codes[:10])

print("\n=== RDU coverage for " + TODAY + " ===")
cur.execute("SELECT DISTINCT dataset_name FROM report_data_usage WHERE trade_date = ?", (TODAY,))
ds_today = [r["dataset_name"] for r in cur.fetchall()]
print("  datasets with today data:", ds_today)

cur.execute("SELECT stock_code, COUNT(DISTINCT dataset_name) as n_datasets FROM report_data_usage WHERE trade_date = ? GROUP BY stock_code ORDER BY n_datasets DESC LIMIT 10", (TODAY,))
for r in cur.fetchall():
    print("  ", r["stock_code"], "n_datasets=", r["n_datasets"])

print("\n=== POOL STOCKS COVERAGE for " + TODAY + " ===")
# How many pool stocks have each dataset for today
for ds in ["stock_profile", "main_force_flow", "dragon_tiger_list", "northbound_summary", "etf_flow_summary", "margin_financing"]:
    cur.execute(
        "SELECT COUNT(DISTINCT rdu.stock_code) FROM report_data_usage rdu "
        "JOIN stock_pool_snapshot ps ON ps.stock_code = rdu.stock_code AND ps.trade_date = ? "
        "WHERE rdu.trade_date = ? AND rdu.dataset_name = ? AND rdu.status NOT IN ('missing')",
        (TODAY, TODAY, ds)
    )
    covered = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_pool_snapshot WHERE trade_date = ?", (TODAY,))
    total = cur.fetchone()[0]
    print(f"  {ds}: {covered}/{total} covered")

print("\n=== POOL STOCKS WITH COMPLETE RDU FOR TODAY ===")
# Which pool stocks have all 6 key datasets today
cur.execute("""
    SELECT ps.stock_code, COUNT(DISTINCT rdu.dataset_name) as n_datasets
    FROM stock_pool_snapshot ps
    LEFT JOIN report_data_usage rdu ON rdu.stock_code = ps.stock_code AND rdu.trade_date = ? AND rdu.status NOT IN ('missing')
    WHERE ps.trade_date = ?
    GROUP BY ps.stock_code
    ORDER BY n_datasets ASC
    LIMIT 10
""", (TODAY, TODAY))
for r in cur.fetchall():
    print("  ", r["stock_code"], "n_datasets=", r["n_datasets"])

print("\n=== STOCK SCORE COVERAGE ===")
cur.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_score")
r = cur.fetchone()
print("  stocks with scores:", r[0])
cur.execute("SELECT MAX(score_date) FROM stock_score")
r = cur.fetchone()
print("  latest score_date:", r[0])

# Find pool stocks without scores
cur.execute("""
    SELECT COUNT(DISTINCT ps.stock_code) 
    FROM stock_pool_snapshot ps
    LEFT JOIN stock_score ss ON ss.stock_code = ps.stock_code
    WHERE ps.trade_date = ? AND ss.stock_code IS NULL
""", (TODAY,))
r = cur.fetchone()
print("  pool stocks without any scores:", r[0])

print("\n=== SETTLEMENT RESULT coverage ===")
cur.execute("SELECT COUNT(*), MAX(report_date), COUNT(DISTINCT stock_code) FROM settlement_result")
r = cur.fetchone()
print("  total=", r[0], "latest=", r[1], "stocks=", r[2])

print("\n=== PREDICTION OUTCOME ===")
cur.execute("SELECT COUNT(*), MAX(outcome_date), MIN(outcome_date) FROM prediction_outcome")
r = cur.fetchone()
print("  total=", r[0], "latest=", r[1], "earliest=", r[2])

print("\n=== STOCK_POOL latest ===")
cur.execute("SELECT MAX(pool_date) as latest FROM stock_pool")
pool_latest = cur.fetchone()[0]
print("  latest pool_date:", pool_latest)

print("\n=== HOTSPOT_STOCK_LINK ===")
cur.execute("SELECT COUNT(*) FROM hotspot_stock_link")
r = cur.fetchone()
print("  hotspot_stock_link count:", r[0])

cur.execute("SELECT COUNT(*) FROM market_hotspot_item")
r = cur.fetchone()
print("  market_hotspot_item count:", r[0])

conn.close()
print("\n=== DONE ===")
