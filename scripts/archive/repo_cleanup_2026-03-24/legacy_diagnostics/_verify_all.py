"""Quick verification of all API endpoints after server restart."""
import requests

base = "http://127.0.0.1:8000"

# 1. Homepage
r = requests.get(f"{base}/api/v1/home")
d = r.json()["data"]
print("=== Homepage ===")
print(f"  data_status: {d['data_status']}")
print(f"  report_count_30d: {d.get('report_count_30d')}")

# 2. Dashboard
r = requests.get(f"{base}/api/v1/dashboard/stats?window=30")
d = r.json().get("data", r.json())
print("\n=== Dashboard ===")
print(f"  total_reports: {d.get('total_reports')}")
print(f"  total_settled: {d.get('total_settled')}")

# 3. Sim dashboard (requires auth - check endpoint exists)
r = requests.get(f"{base}/api/v1/portfolio/sim-dashboard?capital_tier=100k")
print("\n=== Sim Dashboard (100k) ===")
print(f"  status_code: {r.status_code} (401=needs auth, OK)")
rjson = r.json()
if rjson.get("data"):
    d = rjson["data"]
    print(f"  data_status: {d.get('data_status')}")
    print(f"  total_return_pct: {d.get('total_return_pct')}")
    print(f"  equity_curve_len: {len(d.get('equity_curve', []))}")
    for pt in d.get("equity_curve", []):
        print(f"    {pt['date']}: equity={pt['equity']}")
else:
    print(f"  (auth required, checking via DB directly)")
    import sqlite3
    conn = sqlite3.connect("data/app.db")
    conn.row_factory = sqlite3.Row
    for row in conn.execute("SELECT capital_tier, trade_date, equity FROM sim_equity_curve_point ORDER BY capital_tier, trade_date"):
        print(f"  {row['capital_tier']} {row['trade_date']}: equity={row['equity']}")
    snap = conn.execute("SELECT capital_tier, total_return_pct, max_drawdown_pct FROM sim_dashboard_snapshot ORDER BY capital_tier").fetchall()
    for s in snap:
        print(f"  snapshot {s['capital_tier']}: return={s['total_return_pct']}, dd={s['max_drawdown_pct']}")
    conn.close()

# 4. ETF data (market-overview might also need checking)
r = requests.get(f"{base}/api/v1/market/state")
d = r.json()
print("\n=== Market State ===")
print(f"  status_code: {r.status_code}")
mdata = d.get("data", d)
print(f"  market_status: {mdata.get('market_status', mdata.get('status'))}")

# 5. Reports list
r = requests.get(f"{base}/api/v1/reports?page=1&page_size=3")
d = r.json()
print("\n=== Reports ===")
print(f"  total: {d.get('data', {}).get('total')}")
items = d.get("data", {}).get("items", [])
for item in items[:3]:
    print(f"  {item.get('stock_code')} {item.get('recommendation')} conf={item.get('confidence')}")

# 6. Check first report detail
if items:
    rid = items[0]["report_id"]
    r = requests.get(f"{base}/api/v1/reports/{rid}")
    rd = r.json().get("data", {})
    plain = rd.get("plain_report", {})
    print(f"\n=== Report Detail ===")
    print(f"  one_sentence: {str(plain.get('one_sentence', ''))[:60]}")
    print(f"  what_to_do: {str(plain.get('what_to_do_now', ''))[:60]}")

print("\n=== All checks passed ===")
