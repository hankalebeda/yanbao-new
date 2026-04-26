"""
Comprehensive API endpoint testing v2 — with correct paths from OpenAPI spec.
Tests every FR domain function point with evidence collection.
"""
import json
import time
import httpx
import sys

BASE = "http://127.0.0.1:8010"
results = []
problems = []


def test_ep(method, path, desc, fr_id, angles, expect_codes=None, **kwargs):
    """Test a single endpoint and record evidence."""
    url = f"{BASE}{path}"
    try:
        with httpx.Client(timeout=15) as client:
            r = client.request(method, url, **kwargs)
        status = r.status_code
        try:
            body = r.json()
        except Exception:
            body = {"html_len": len(r.text), "snippet": r.text[:200]}
        
        if expect_codes:
            ok = status in expect_codes
        else:
            ok = status < 500
        
        result = {
            "fr": fr_id, "endpoint": f"{method} {path}", "desc": desc,
            "status": status, "ok": ok, "angles": angles,
            "evidence": body if isinstance(body, dict) else {"raw": str(body)[:300]},
        }
    except Exception as e:
        result = {
            "fr": fr_id, "endpoint": f"{method} {path}", "desc": desc,
            "status": 0, "ok": False, "angles": angles,
            "evidence": {"error": str(e)},
        }

    icon = "PASS" if result["ok"] else "FAIL"
    sc = result["status"]
    print(f"  [{icon}] {sc:3d} {method:6s} {path:55s} {desc}")
    results.append(result)
    if not result["ok"]:
        problems.append(result)
    return result


# Helper: common admin headers
admin_h = {"Authorization": "Bearer admin-test-token"}

print("=" * 80)
print("A股研报平台 — 全量API端点功能诊断 v2 (OpenAPI-verified paths)")
print("=" * 80)

# Wait for server
for i in range(5):
    try:
        httpx.get(f"{BASE}/health", timeout=3)
        break
    except:
        time.sleep(2)

# ------ FR-00 Data Authenticity ------
print("\n--- FR-00 Data Authenticity (Angles: 1,5,6,8) ---")
r = test_ep("GET", "/api/v1/platform/config", "Platform config SSOT", "FR-00", [1,5])
r = test_ep("GET", "/api/v1/platform/summary", "Platform summary coherence", "FR-00", [8,29])
r = test_ep("GET", "/health", "Health check", "FR-00", [29])

# ------ FR-01 Stock Pool ------
print("\n--- FR-01 Stock Pool (Angles: 1,2,5,9,10) ---")
r = test_ep("GET", "/api/v1/pool/stocks", "Stock pool query", "FR-01", [1,2])
if r["ok"] and isinstance(r["evidence"], dict):
    items = r["evidence"].get("data", r["evidence"].get("items", []))
    cnt = len(items) if isinstance(items, list) else r["evidence"].get("total", "?")
    print(f"    → Pool stocks count: {cnt}")

# ------ FR-02 Scheduler ------
print("\n--- FR-02 Scheduler (Angles: 2,9,14) ---")
test_ep("GET", "/api/v1/admin/scheduler/status", "Scheduler status", "FR-02", [2,9],
        headers=admin_h, expect_codes=[200,401,403])

# ------ FR-03 Cookie/Session ------
print("\n--- FR-03 Cookie/Session (Angles: 2,3,22,24) ---")
test_ep("GET", "/api/v1/admin/cookie-sessions", "Cookie list", "FR-03", [2,22],
        headers=admin_h, expect_codes=[200,401])
test_ep("GET", "/api/v1/admin/cookie-session/health", "Cookie health", "FR-03", [11,29],
        headers=admin_h, expect_codes=[200,401])

# ------ FR-04 Hot Spot ------
print("\n--- FR-04 Hot Spot (Angles: 1,5,6,11) ---")
test_ep("GET", "/api/v1/market/hot-stocks", "Hot stocks list", "FR-04", [1,5,6])
test_ep("GET", "/api/v1/internal/hotspot/health", "Hotspot collection health", "FR-04", [11,29])
test_ep("GET", "/api/v1/internal/source/fallback-status", "Data source fallback", "FR-04", [11])

# ------ FR-05 Market State ------
print("\n--- FR-05 Market State (Angles: 1,5,9) ---")
r = test_ep("GET", "/api/v1/market/state", "Market state", "FR-05", [1,5,9])
if r["ok"] and isinstance(r["evidence"], dict):
    state = r["evidence"].get("state", r["evidence"].get("market_state", "?"))
    print(f"    → Market state: {state}")

# ------ FR-06 LLM/Report ------
print("\n--- FR-06 LLM/Report (Angles: 1,6,11,30) ---")
test_ep("GET", "/api/v1/internal/llm/health", "LLM health", "FR-06", [11,29])
test_ep("GET", "/api/v1/internal/llm/version", "LLM version", "FR-06", [1,3])
r = test_ep("GET", "/api/v1/reports?page=1&page_size=3", "Reports list API", "FR-06", [1,8])
if r["ok"] and isinstance(r["evidence"], dict):
    total = r["evidence"].get("total", r["evidence"].get("count", "?"))
    print(f"    → Total reports: {total}")

# Get a specific report for detail testing
report_id = None
if r["ok"] and isinstance(r["evidence"], dict):
    items = r["evidence"].get("data", r["evidence"].get("items", []))
    if isinstance(items, list) and len(items) > 0:
        report_id = items[0].get("report_id", items[0].get("id"))

if report_id:
    test_ep("GET", f"/api/v1/reports/{report_id}", "Report detail", "FR-06", [1,6])
    test_ep("GET", f"/api/v1/reports/{report_id}/advanced", "Report advanced section", "FR-06", [6,27])

# ------ FR-07 Settlement ------
print("\n--- FR-07 Settlement (Angles: 1,7,9,10,12) ---")
r = test_ep("GET", "/api/v1/predictions/stats", "Prediction stats", "FR-07", [7,8])
if r["ok"] and isinstance(r["evidence"], dict):
    print(f"    → Prediction stats: {json.dumps(r['evidence'], ensure_ascii=False, default=str)[:200]}")

# ------ FR-08 Simulation ------
print("\n--- FR-08 Simulation (Angles: 2,8,27,28) ---")
test_ep("GET", "/api/v1/portfolio/sim-dashboard", "Sim dashboard (API)", "FR-08", [2,27,28])
test_ep("GET", "/api/v1/sim/account/summary", "Sim account summary", "FR-08", [2,28])
test_ep("GET", "/api/v1/sim/account/snapshots", "Sim account snapshots", "FR-08", [2,8])
test_ep("GET", "/sim-dashboard", "Sim dashboard (HTML)", "FR-08", [25,26,27])

# ------ FR-09 Auth/Billing ------
print("\n--- FR-09 Auth (Angles: 1,2,17,18) ---")
test_ep("GET", "/api/v1/platform/plans", "Membership plans", "FR-09", [1,3])
test_ep("POST", "/auth/login", "Login (bad creds→401)", "FR-09", [17,18],
        json={"email": "test@bad.com", "password": "wrong"}, expect_codes=[401,422])
test_ep("POST", "/auth/register",
        "Register (duplicate test)", "FR-09", [17],
        json={"email": "test_diag@x.com", "password": "Test1234!", "username": "diag_test"},
        expect_codes=[200,201,409,422])
test_ep("GET", "/auth/oauth/providers", "OAuth providers", "FR-09", [22])

# ------ FR-10 Site/Dashboard ------
print("\n--- FR-10 Site/Dashboard (Angles: 8,25,26,27,28,29) ---")
test_ep("GET", "/", "Homepage HTML", "FR-10", [25,26])
test_ep("GET", "/reports", "Reports list HTML", "FR-10", [25,26])
test_ep("GET", "/dashboard", "Dashboard HTML", "FR-10", [27,28,29])
test_ep("GET", "/api/v1/dashboard/stats", "Dashboard stats API", "FR-10", [8,29])
test_ep("GET", "/api/v1/home", "Home API", "FR-10", [8,25])
test_ep("GET", "/features", "Features page HTML", "FR-10", [34,35])
test_ep("GET", "/admin", "Admin page HTML", "FR-10", [17,21])

# ------ FR-11 Feedback ------
print("\n--- FR-11 Feedback (Angles: 1,2) ---")
test_ep("POST", "/api/v1/report-feedback", "Feedback (missing data→422)", "FR-11", [1,2],
        json={}, expect_codes=[422])

# ------ FR-12 Admin ------
print("\n--- FR-12 Admin (Angles: 17,18,21) ---")
test_ep("GET", "/api/v1/admin/overview", "Admin overview", "FR-12", [17,21],
        headers=admin_h, expect_codes=[200,401])
test_ep("GET", "/api/v1/admin/users", "Admin users", "FR-12", [17,18],
        headers=admin_h, expect_codes=[200,401])
test_ep("GET", "/api/v1/admin/reports", "Admin reports", "FR-12", [17,18],
        headers=admin_h, expect_codes=[200,401])
test_ep("GET", "/api/v1/admin/system-status", "System status", "FR-12", [21,29],
        headers=admin_h, expect_codes=[200,401])

# ------ FR-13 Events ------
print("\n--- FR-13 Events (Angles: 2,3,22,35) ---")
test_ep("GET", "/api/v1/internal/metrics/summary", "Internal metrics", "FR-13", [29,35])

# ------ Governance ------
print("\n--- Governance (Angles: 34,35) ---")
test_ep("GET", "/api/v1/features/catalog", "Feature catalog", "GOV", [34,35],
        headers=admin_h, expect_codes=[200,401])
test_ep("GET", "/api/v1/governance/catalog", "Governance catalog", "GOV", [34,35],
        headers=admin_h, expect_codes=[200,401])

# ------ Retired (410) ------
print("\n--- Retired (expect 410) ---")
test_ep("POST", "/api/v1/admin/dag/retrigger", "DAG retrigger", "RETIRED", [20],
        headers=admin_h, expect_codes=[410,401])
test_ep("GET", "/api/v1/membership/subscription/status", "Subscription status", "RETIRED", [20],
        expect_codes=[410,401,200])

# ======================================================================
# ANALYSIS & REPORT
# ======================================================================
print("\n" + "=" * 80)
print("DIAGNOSTIC RESULTS")
print("=" * 80)

total_t = len(results)
ok_t = sum(1 for r in results if r["ok"])
fail_t = total_t - ok_t

print(f"\nTotal endpoints tested: {total_t}")
print(f"  PASS: {ok_t}")
print(f"  FAIL: {fail_t}")

# Analyze data issues
data_problems = []
for r in results:
    ev = r["evidence"]
    if isinstance(ev, dict):
        # Check for empty data
        data = ev.get("data", ev.get("items", ev.get("stocks", ev.get("tasks", None))))
        if isinstance(data, list) and len(data) == 0:
            data_problems.append((r["fr"], r["endpoint"], "Empty data list"))
        total = ev.get("total", ev.get("count"))
        if total == 0:
            data_problems.append((r["fr"], r["endpoint"], f"Zero count"))

if data_problems:
    print(f"\n{'DATA INSUFFICIENCY ISSUES':}")
    for fr, ep, issue in data_problems:
        print(f"  [{fr}] {ep} — {issue}")

# Analyze 404s (path not found)
not_found = [r for r in results if r["status"] == 404]
if not_found:
    print(f"\n{'404 NOT FOUND (possible path issues)':}")
    for r in not_found:
        print(f"  [{r['fr']}] {r['endpoint']} — {r['desc']}")

# Per-FR summary
print(f"\n{'PER-FR SUMMARY':}")
fr_stats = {}
for r in results:
    fr = r["fr"]
    if fr not in fr_stats:
        fr_stats[fr] = {"ok": 0, "fail": 0, "s5xx": 0, "s404": 0, "s401": 0}
    if r["ok"]:
        fr_stats[fr]["ok"] += 1
    else:
        fr_stats[fr]["fail"] += 1
    if r["status"] >= 500:
        fr_stats[fr]["s5xx"] += 1
    if r["status"] == 404:
        fr_stats[fr]["s404"] += 1
    if r["status"] == 401:
        fr_stats[fr]["s401"] += 1

for fr, s in sorted(fr_stats.items()):
    t = s["ok"] + s["fail"]
    icon = "PASS" if s["fail"] == 0 else "ISSUE"
    extra = []
    if s["s5xx"]: extra.append(f"{s['s5xx']} server errors")
    if s["s404"]: extra.append(f"{s['s404']} not found")
    if s["s401"]: extra.append(f"{s['s401']} auth required")
    note = f" ({', '.join(extra)})" if extra else ""
    print(f"  {fr:12s}: {s['ok']}/{t} [{icon}]{note}")

# Save to file
with open("_archive/api_diag_v2_20260413.json", "w", encoding="utf-8") as f:
    json.dump({"results": results, "problems": problems, "data_issues": data_problems}, 
              f, ensure_ascii=False, indent=2, default=str)
print(f"\nResults saved to _archive/api_diag_v2_20260413.json")
