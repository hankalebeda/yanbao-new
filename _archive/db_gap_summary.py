#!/usr/bin/env python3
# Quick summary focused on key gaps
import sqlite3

DB_PATH = "data/app.db"
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=== 1. ACTIVE STOCKS ===")
cur.execute("SELECT stock_code, stock_name, is_delisted, list_date FROM stock_master WHERE is_delisted=0")
stocks = [dict(r) for r in cur.fetchall()]
print(f"  Total active: {len(stocks)}")
for s in stocks[:10]:
    print(f"  {s['stock_code']} {s['stock_name']} list_date={s['list_date']}")
if len(stocks) > 10:
    print(f"  ... and {len(stocks)-10} more")

active_codes = [s["stock_code"] for s in stocks]

print("\n=== 2. KLINE_DAILY SUMMARY ===")
cur.execute("SELECT COUNT(DISTINCT stock_code) as n_stocks, COUNT(*) as total_rows, MAX(trade_date) as latest, MIN(trade_date) as earliest FROM kline_daily")
r = cur.fetchone()
print(f"  stocks={r['n_stocks']}, total={r['total_rows']}, latest={r['latest']}, earliest={r['earliest']}")

cur.execute("SELECT stock_code FROM kline_daily WHERE trade_date >= '2026-04-24' GROUP BY stock_code")
today_stocks = set(r[0] for r in cur.fetchall())
print(f"  Stocks with 2026-04-24 data: {len(today_stocks)}")
missing_today = [c for c in active_codes if c not in today_stocks]
print(f"  Active stocks missing 2026-04-24: {len(missing_today)}")
for c in missing_today[:5]:
    print(f"    {c}")

print("\n=== 3. STOCK_POOL_SNAPSHOT ===")
cur.execute("SELECT snapshot_date, COUNT(*) as cnt FROM stock_pool_snapshot GROUP BY snapshot_date ORDER BY snapshot_date DESC LIMIT 5")
for r in cur.fetchall():
    print(f"  {r['snapshot_date']}: {r['cnt']} stocks")

print("\n=== 4. HOTSPOT_TOP50 ===")
cur.execute("SELECT source, COUNT(DISTINCT fetch_date) as days, MAX(fetch_date) as latest FROM hotspot_top50 GROUP BY source")
for r in cur.fetchall():
    print(f"  source={r['source']}: {r['days']} days, latest={r['latest']}")

print("\n=== 5. HOTSPOT_RAW ===")
cur.execute("SELECT COUNT(*), MAX(fetch_date) FROM hotspot_raw")
r = cur.fetchone()
print(f"  total={r[0]}, latest={r[1]}")

print("\n=== 6. REPORT_DATA_USAGE (summary) ===")
cur.execute("SELECT stock_code, dataset, status, MAX(collect_date) as latest FROM report_data_usage GROUP BY stock_code, dataset ORDER BY stock_code, dataset")
rdu_rows = [dict(r) for r in cur.fetchall()]
# Build status map
status_map = {}
for row in rdu_rows:
    code = row["stock_code"]
    ds = row["dataset"]
    if code not in status_map:
        status_map[code] = {}
    existing = status_map[code].get(ds)
    # take best status
    priority = {"ok": 0, "proxy_ok": 1, "realtime_only": 2, "stale_ok": 3, "missing": 4}
    new_p = priority.get(row["status"], 5)
    if existing is None or new_p < priority.get(existing.get("status"), 5):
        status_map[code][ds] = row

CORE_DATASETS = ["kline_daily", "stock_profile", "capital_flow", "hotspot_top50", "northbound_flow", "etf_flow"]
print(f"  Total rdu rows: {len(rdu_rows)}")
print(f"\n  Per stock summary:")
for code in active_codes:
    cov = status_map.get(code, {})
    row_summary = {ds: cov.get(ds, {}).get("status", "MISSING") for ds in CORE_DATASETS}
    print(f"  {code}: {row_summary}")

print("\n=== 7. GAPS IN REPORT_DATA_USAGE ===")
gaps = []
for code in active_codes:
    cov = status_map.get(code, {})
    for ds in CORE_DATASETS:
        info = cov.get(ds)
        if info is None:
            gaps.append(f"MISSING: {code}/{ds}")
        elif info.get("status") == "missing":
            gaps.append(f"STATUS_MISSING: {code}/{ds} (latest={info.get('latest')})")
print(f"  Total gaps: {len(gaps)}")
for g in gaps[:30]:
    print(f"  {g}")

print("\n=== 8. STOCK_SCORE ===")
cur.execute("SELECT COUNT(DISTINCT stock_code) as n, MAX(score_date) as latest FROM stock_score")
r = cur.fetchone()
print(f"  stocks={r['n']}, latest={r['latest']}")
cur.execute("SELECT stock_code FROM stock_score GROUP BY stock_code")
score_codes = set(r[0] for r in cur.fetchall())
missing_score = [c for c in active_codes if c not in score_codes]
print(f"  Active stocks missing scores: {len(missing_score)}")

print("\n=== 9. MARKET_STATE_CACHE ===")
cur.execute("SELECT state_key, updated_at FROM market_state_cache ORDER BY updated_at DESC LIMIT 5")
for r in cur.fetchall():
    print(f"  {r['state_key']}: updated={r['updated_at']}")

print("\n=== 10. SETTLEMENT_RESULT ===")
cur.execute("SELECT COUNT(*), MAX(report_date), MIN(report_date) FROM settlement_result")
r = cur.fetchone()
print(f"  total={r[0]}, latest={r[1]}, earliest={r[2]}")

print("\n=== 11. STOCK_POOL (latest) ===")
cur.execute("SELECT MAX(pool_date) as latest FROM stock_pool")
latest_pool = cur.fetchone()[0]
print(f"  Latest pool_date: {latest_pool}")
if latest_pool:
    cur.execute("SELECT strategy_type, COUNT(*) as cnt FROM stock_pool WHERE pool_date=? GROUP BY strategy_type", (latest_pool,))
    for r in cur.fetchall():
        print(f"  strategy={r['strategy_type']}, cnt={r['cnt']}")

print("\n=== 12. DATA_BATCH recent ===")
cur.execute("SELECT id, batch_seq, status, created_at FROM data_batch ORDER BY id DESC LIMIT 5")
for r in cur.fetchall():
    print(f"  id={r['id']}, seq={r['batch_seq']}, status={r['status']}, created={r['created_at']}")

conn.close()
print("\n=== DONE ===")
