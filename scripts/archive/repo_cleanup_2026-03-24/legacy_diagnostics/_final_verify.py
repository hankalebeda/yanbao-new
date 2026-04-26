"""Legacy ad-hoc verification script.

Do not use this as the release gate. The official entrypoints are
scripts/pre_release_check.ps1 and scripts/round_verify.py.
"""
import requests

base = "http://127.0.0.1:8000"

pages = {
    "/": "Homepage",
    "/reports": "Reports List",
    "/dashboard": "Dashboard",
    "/login": "Login",
    "/register": "Register",
    "/subscribe": "Subscribe",
    "/admin": "Admin",
    "/portfolio/sim-dashboard": "Sim Dashboard",
}

print("=== HTML Pages ===")
for path, name in pages.items():
    r = requests.get(f"{base}{path}", allow_redirects=False)
    err = "ERROR" if ("Internal Server Error" in r.text or "Traceback" in r.text) else "OK"
    print(f"  {name:20s} {r.status_code:>3d} {len(r.text):>6d}b {err}")

print("\n=== API Endpoints ===")
apis = {
    "/api/v1/home": "Home",
    "/api/v1/dashboard/stats?window=30": "Dashboard Stats",
    "/api/v1/reports?page=1&page_size=3": "Reports",
    "/api/v1/market/state": "Market State",
    "/api/v1/platform/config": "Platform Config",
    "/api/v1/platform/summary": "Platform Summary",
    "/health": "Health",
}
for path, name in apis.items():
    r = requests.get(f"{base}{path}")
    j = r.json()
    ok = j.get("success", True) if isinstance(j, dict) else True
    print(f"  {name:20s} {r.status_code:>3d} {'OK' if ok else 'FAIL'}")

print("\n=== Key Data Points ===")
# Home
r = requests.get(f"{base}/api/v1/home")
d = r.json()["data"]
print(f"  Home data_status: {d['data_status']}")
print(f"  Home today_report_count: {d.get('today_report_count')}")
print(f"  Home pool_size: {d.get('pool_size')}")

# Dashboard
r = requests.get(f"{base}/api/v1/dashboard/stats?window=30")
d = r.json()["data"]
print(f"  Dashboard total_reports: {d.get('total_reports')}")
print(f"  Dashboard total_settled: {d.get('total_settled')}")

# Equity curve (from DB)
import sqlite3
conn = sqlite3.connect("data/app.db")
for tier in ["10k", "100k", "500k"]:
    rows = conn.execute(
        "SELECT trade_date, equity FROM sim_equity_curve_point "
        "WHERE capital_tier = ? ORDER BY trade_date", (tier,)
    ).fetchall()
    if rows:
        vals = [f"{r[0]}:{r[1]}" for r in rows]
        print(f"  Equity {tier}: {', '.join(vals)}")

# Dashboard snapshot returns
for tier in ["10k", "100k", "500k"]:
    row = conn.execute(
        "SELECT total_return_pct FROM sim_dashboard_snapshot "
        "WHERE capital_tier = ? ORDER BY snapshot_date DESC LIMIT 1", (tier,)
    ).fetchone()
    if row:
        print(f"  Return {tier}: {row[0]:.2%}")

conn.close()

# Health
r = requests.get(f"{base}/health")
d = r.json()["data"]
print(f"  Scheduler: {d.get('scheduler_status')}")
print(f"  LLM Router: {d.get('llm_router_status')}")
print(f"  DB: {d.get('database_status')}")

print("\n=== All Checks Complete ===")
