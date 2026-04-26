#!/usr/bin/env python3
import sqlite3
import json

DB_PATH = "data/app.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# 1. stock_master
cur.execute("SELECT stock_code, stock_name, is_delisted, list_date FROM stock_master WHERE is_delisted=0")
stocks = [dict(r) for r in cur.fetchall()]
print("=== stock_master (active) ===")
for s in stocks:
    print("  ", s["stock_code"], s["stock_name"], "list_date=", s["list_date"])

# 2. kline_daily
print("\n=== kline_daily ===")
cur.execute("SELECT stock_code, COUNT(*) as cnt, MAX(trade_date) as latest FROM kline_daily GROUP BY stock_code ORDER BY stock_code")
for r in cur.fetchall():
    print("  ", r["stock_code"], "cnt=", r["cnt"], "latest=", r["latest"])

# 3. stock_pool_snapshot
print("\n=== stock_pool_snapshot (recent) ===")
cur.execute("SELECT snapshot_date, COUNT(*) as cnt FROM stock_pool_snapshot GROUP BY snapshot_date ORDER BY snapshot_date DESC LIMIT 10")
for r in cur.fetchall():
    print("  ", r["snapshot_date"], "cnt=", r["cnt"])

# 4. hotspot_top50
print("\n=== hotspot_top50 (recent) ===")
cur.execute("SELECT source, fetch_date, COUNT(*) as cnt FROM hotspot_top50 GROUP BY source, fetch_date ORDER BY fetch_date DESC LIMIT 20")
for r in cur.fetchall():
    print("  source=", r["source"], "date=", r["fetch_date"], "cnt=", r["cnt"])

# 5. hotspot_raw/normalized
print("\n=== hotspot_raw/normalized ===")
cur.execute("SELECT COUNT(*), MAX(fetch_date) FROM hotspot_raw")
r = cur.fetchone()
print("  hotspot_raw: cnt=", r[0], "latest=", r[1])
cur.execute("SELECT COUNT(*), MAX(fetch_date) FROM hotspot_normalized")
r = cur.fetchone()
print("  hotspot_normalized: cnt=", r[0], "latest=", r[1])

# 6. report_data_usage
print("\n=== report_data_usage ===")
cur.execute("SELECT stock_code, dataset, status, COUNT(*) as cnt, MAX(collect_date) as latest FROM report_data_usage GROUP BY stock_code, dataset, status ORDER BY stock_code, dataset, collect_date DESC")
for r in cur.fetchall():
    print("  ", r["stock_code"], "|", r["dataset"], "|", r["status"], "| cnt=", r["cnt"], "| latest=", r["latest"])

# 7. market_state_cache
print("\n=== market_state_cache ===")
cur.execute("SELECT COUNT(*), MAX(updated_at) FROM market_state_cache")
r = cur.fetchone()
print("  cnt=", r[0], "latest=", r[1])
cur.execute("SELECT state_key, state_value, updated_at FROM market_state_cache ORDER BY updated_at DESC LIMIT 5")
for r in cur.fetchall():
    print("  ", r["state_key"], "=", str(r["state_value"])[:80], "updated=", r["updated_at"])

# 8. stock_score
print("\n=== stock_score ===")
cur.execute("SELECT stock_code, COUNT(*) as cnt, MAX(score_date) as latest FROM stock_score GROUP BY stock_code ORDER BY stock_code")
for r in cur.fetchall():
    print("  ", r["stock_code"], "cnt=", r["cnt"], "latest=", r["latest"])

# 9. data_usage_fact
print("\n=== data_usage_fact ===")
cur.execute("SELECT COUNT(*), MAX(fact_date) FROM data_usage_fact")
r = cur.fetchone()
print("  cnt=", r[0], "latest=", r[1])

# 10. data_batch
print("\n=== data_batch (recent 10) ===")
cur.execute("SELECT id, batch_seq, status, created_at FROM data_batch ORDER BY id DESC LIMIT 10")
for r in cur.fetchall():
    print("  id=", r["id"], "seq=", r["batch_seq"], "status=", r["status"], "created=", r["created_at"])

# 11. market_hotspot_item
print("\n=== market_hotspot_item ===")
cur.execute("SELECT COUNT(*), MAX(created_at) FROM market_hotspot_item")
r = cur.fetchone()
print("  cnt=", r[0], "latest=", r[1])

# 12. prediction_outcome
print("\n=== prediction_outcome ===")
cur.execute("SELECT COUNT(*), MAX(outcome_date) FROM prediction_outcome")
r = cur.fetchone()
print("  cnt=", r[0], "latest=", r[1])

# 13. settlement_result
print("\n=== settlement_result (recent 5) ===")
cur.execute("SELECT stock_code, strategy_type, status, report_date FROM settlement_result ORDER BY id DESC LIMIT 10")
for r in cur.fetchall():
    print("  ", r["stock_code"], r["strategy_type"], r["status"], r["report_date"])

# 14. baseline_result
print("\n=== baseline_result ===")
cur.execute("SELECT COUNT(*), MAX(run_date) FROM baseline_result")
r = cur.fetchone()
print("  cnt=", r[0], "latest=", r[1])

# 15. stock_pool (pool_date)
print("\n=== stock_pool ===")
cur.execute("SELECT pool_date, strategy_type, COUNT(*) as cnt FROM stock_pool GROUP BY pool_date, strategy_type ORDER BY pool_date DESC LIMIT 15")
for r in cur.fetchall():
    print("  pool_date=", r["pool_date"], "strategy=", r["strategy_type"], "cnt=", r["cnt"])

conn.close()
print("\n== DONE ==")
