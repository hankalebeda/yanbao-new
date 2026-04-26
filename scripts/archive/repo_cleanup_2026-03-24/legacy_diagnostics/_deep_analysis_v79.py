"""v7.9 Deep data analysis — find all inconsistencies"""
import httpx
import json
from collections import Counter

BASE = "http://127.0.0.1:8099"

def login(email, pw):
    c = httpx.Client(base_url=BASE, follow_redirects=False, timeout=30)
    r = c.post("/auth/login", json={"email": email, "password": pw})
    token = r.json().get("data", {}).get("access_token", "")
    c.headers["Authorization"] = f"Bearer {token}"
    return c

pro = login("browser_pro@test.com", "Pro123456")
admin = login("browser_admin@test.com", "Admin123456")

# 1. Report detail deep inspection
print("=== 1. REPORT DETAILS (first 3) ===")
r = pro.get("/api/v1/reports?page=1&page_size=3")
items = r.json().get("data", {}).get("items", [])
for it in items[:3]:
    rid = it.get("report_id")
    rd = pro.get(f"/api/v1/reports/{rid}").json().get("data", {})
    print(f"\nReport {rid[:12]}:")
    for k in sorted(rd.keys()):
        v = rd[k]
        vstr = json.dumps(v, ensure_ascii=False)[:120] if v is not None else "null"
        print(f"  {k}: {vstr}")

# 2. Dashboard windows should show different data
print("\n=== 2. DASHBOARD CONSISTENCY ===")
window_data = {}
for wd in [1, 7, 14, 30, 60]:
    r = pro.get(f"/api/v1/dashboard/stats?window_days={wd}")
    d = r.json().get("data", {})
    rp = d.get("total_reports")
    se = d.get("total_settled")
    wr = d.get("overall_win_rate")
    window_data[wd] = (rp, se, wr)
    print(f"  window={wd}d: reports={rp}, settled={se}, win_rate={wr}")

# Check: 7d should have <= 30d
if window_data[7][0] == window_data[30][0]:
    print("  ** ISSUE: 7d and 30d have SAME report count — date filter not working?")
if window_data[1][0] == window_data[60][0]:
    print("  ** ISSUE: 1d and 60d have SAME report count — all data from same date?")

# 3. Pool stocks
print("\n=== 3. POOL STOCKS DEEP ===")
r = pro.get("/api/v1/pool/stocks")
print(f"  status={r.status_code}")
d = r.json()
print(f"  response: {json.dumps(d, ensure_ascii=False)[:400]}")

# 4. Home vs Admin pool_size
print("\n=== 4. CROSS-PAGE DATA CONSISTENCY ===")
r = pro.get("/api/v1/home")
home = r.json().get("data", {})
home_pool = home.get("pool_size")
home_market = home.get("market_state")

r = admin.get("/api/v1/admin/overview")
adm = r.json().get("data", {})
adm_pool = adm.get("pool_size")

print(f"  Home pool_size={home_pool}")
print(f"  Admin pool_size={adm_pool}")
print(f"  Home market_state={home_market}")

# Check admin overview fields
print(f"\n  Admin overview keys: {sorted(adm.keys())}")
for k in sorted(adm.keys()):
    v = adm[k]
    vstr = json.dumps(v, ensure_ascii=False)[:150] if v is not None else "null"
    print(f"    {k}: {vstr}")

# 5. Strategy distribution
print("\n=== 5. ALL REPORTS STRATEGY/REC ===")
all_items = []
for page in range(1, 7):
    r = pro.get(f"/api/v1/reports?page={page}&page_size=50")
    items = r.json().get("data", {}).get("items", [])
    all_items.extend(items)
    if len(items) < 50:
        break

stypes = Counter(it.get("strategy_type") for it in all_items)
recs = Counter(it.get("recommendation") for it in all_items)
mstates = Counter(it.get("market_state") for it in all_items)
print(f"  Total reports: {len(all_items)}")
print(f"  Strategy types: {dict(stypes)}")
print(f"  Recommendations: {dict(recs)}")
print(f"  Market states: {dict(mstates)}")

# If ALL are B, that means A and C never trigger
if stypes.get("A", 0) == 0:
    print("  ** ISSUE: Zero A-type (event-driven) reports!")
if stypes.get("C", 0) == 0:
    print("  ** ISSUE: Zero C-type (low-vol) reports!")

# 6. Advanced area - Free truncation
print("\n=== 6. FREE ADVANCED AREA TRUNCATION ===")
free = login("browser_free@test.com", "Free123456")
if items:
    rid = items[0].get("report_id")
    r = free.get(f"/api/v1/reports/{rid}/advanced")
    if r.status_code == 200:
        ad = r.json().get("data", {})
        rc = ad.get("reasoning_chain", "")
        trunc = ad.get("truncated")
        print(f"  Free: len={len(rc)}, truncated={trunc}")
        if len(rc) > 210:
            print(f"  ** ISSUE: Free gets {len(rc)} chars, should be <=200")
        # Check if truncation marker exists
        if trunc is None or trunc is False:
            print(f"  ** ISSUE: truncated={trunc}, should be True for Free")
    else:
        print(f"  Free advanced: {r.status_code}")

# 7. Sim dashboard - check if Free gets forbidden on 500k
print("\n=== 7. SIM RBAC ===")
r = free.get("/api/v1/portfolio/sim-dashboard?capital_tier=500k")
print(f"  Free 500k: {r.status_code}")

# 8. Check HTML pages for data issues
print("\n=== 8. HTML PAGE DATA ISSUES ===")
# Home page HTML
r = pro.get("/")
html = r.text
# Check if market_state is rendered
if "NEUTRAL" in html or "牛市" in html or "熊市" in html or "震荡" in html:
    print("  index.html: market_state rendered OK")
else:
    print("  ** ISSUE: index.html doesn't show market_state")

# Check dashboard page
r = pro.get("/dashboard")
html = r.text
if "window_days" in html.lower() or "window-days" in html.lower() or "tab" in html.lower():
    print("  dashboard.html: has window tabs")
else:
    print("  ** ISSUE: dashboard.html missing window tabs")

# Check sim-dashboard
r = pro.get("/portfolio/sim-dashboard")
html = r.text
if "svg" in html.lower() or "canvas" in html.lower() or "chart" in html.lower():
    print("  sim_dashboard.html: has chart/SVG")
else:
    print("  ** ISSUE: sim_dashboard.html missing chart")

# Check admin page
r = admin.get("/admin")
html = r.text
if "流水线" in html or "pipeline" in html.lower() or "进度" in html:
    print("  admin.html: has pipeline stages")
else:
    print("  ** ISSUE: admin.html missing pipeline/progress")

# 9. Subscribe prices
print("\n=== 9. SUBSCRIBE PRICES ===")
r = pro.get("/subscribe")
html = r.text
prices = ["29.9", "79.9", "299.9", "99.9", "269.9", "999.9"]
for p in prices:
    if p in html:
        print(f"  ¥{p}: found")
    else:
        print(f"  ** ISSUE: ¥{p} NOT found in subscribe.html")

# 10. Anon access to sim-dashboard
print("\n=== 10. ANON SIM-DASHBOARD ===")
anon = httpx.Client(base_url=BASE, follow_redirects=False, timeout=30)
r = anon.get("/portfolio/sim-dashboard")
print(f"  Anon sim-dashboard page: {r.status_code}")
r = anon.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k")
print(f"  Anon sim-dashboard API: {r.status_code}")
if r.status_code == 200:
    print("  ** ISSUE: Anonymous user can access sim-dashboard API!")

pro.close()
admin.close()
free.close()
anon.close()
