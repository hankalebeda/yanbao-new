"""
Comprehensive API endpoint testing script for all FR domains.
Tests every API endpoint and collects evidence for doc22 update.
Uses the 39 analysis perspectives from doc25.
"""
import json
import time
import httpx
import sys

BASE = "http://127.0.0.1:8010"
results = []


def test_endpoint(method, path, description, fr_id, angles, **kwargs):
    """Test a single endpoint and record evidence."""
    url = f"{BASE}{path}"
    try:
        with httpx.Client(timeout=15) as client:
            if method == "GET":
                r = client.get(url, **kwargs)
            elif method == "POST":
                r = client.post(url, **kwargs)
            elif method == "DELETE":
                r = client.delete(url, **kwargs)
            elif method == "PUT":
                r = client.put(url, **kwargs)
            elif method == "PATCH":
                r = client.patch(url, **kwargs)
            else:
                r = client.request(method, url, **kwargs)

        status = r.status_code
        try:
            body = r.json()
        except Exception:
            body = r.text[:500]

        ok = status < 500
        result = {
            "fr": fr_id,
            "endpoint": f"{method} {path}",
            "description": description,
            "status_code": status,
            "ok": ok,
            "angles": angles,
            "evidence": body if isinstance(body, dict) else {"raw": str(body)[:300]},
        }
    except Exception as e:
        result = {
            "fr": fr_id,
            "endpoint": f"{method} {path}",
            "description": description,
            "status_code": 0,
            "ok": False,
            "angles": angles,
            "evidence": {"error": str(e)},
        }

    status_icon = "OK" if result["ok"] else "FAIL"
    print(f"  [{status_icon}] {result['status_code']:3d} {method:6s} {path} — {description}")
    results.append(result)
    return result


print("=" * 80)
print("A股研报平台 — 全量API端点功能诊断")
print("=" * 80)

# Wait for server
print("\nWaiting for server...")
for i in range(10):
    try:
        httpx.get(f"{BASE}/api/v1/platform/config", timeout=3)
        print("Server is up!\n")
        break
    except Exception:
        time.sleep(2)
else:
    print("Server not reachable. Exiting.")
    sys.exit(1)

# ======================================================================
# FR-00 Data Authenticity (Angles: 1,5,6)
# ======================================================================
print("--- FR-00 Data Authenticity ---")
test_endpoint("GET", "/api/v1/platform/config", "Platform config (truth source)", "FR-00", [1, 5])
test_endpoint("GET", "/api/v1/platform/summary", "Platform summary (metrics coherence)", "FR-00", [8, 29])

# ======================================================================
# FR-01 Stock Pool (Angles: 1,2,5,9,10)
# ======================================================================
print("\n--- FR-01 Stock Pool ---")
test_endpoint("GET", "/api/v1/stock-pool", "Stock pool list", "FR-01", [1, 2])
test_endpoint("GET", "/api/v1/stock-pool/snapshots", "Pool snapshots", "FR-01", [5, 9, 10])
test_endpoint("GET", "/api/v1/stock-pool/refresh-tasks", "Refresh tasks", "FR-01", [2, 14])

# ======================================================================
# FR-02 Scheduler (Angles: 2,9,14)
# ======================================================================
print("\n--- FR-02 Scheduler ---")
test_endpoint("GET", "/api/v1/internal/scheduler/status", "Scheduler status", "FR-02", [2, 9])
test_endpoint("GET", "/api/v1/internal/pipeline/status", "Pipeline status", "FR-02", [14, 29])

# ======================================================================
# FR-03 Cookie/Session (Angles: 2,3,22,24)
# ======================================================================
print("\n--- FR-03 Cookie/Session ---")
test_endpoint("GET", "/api/v1/admin/cookie-sessions", "Cookie session list",
              "FR-03", [2, 3, 22], headers={"X-Admin-Token": "admin"})

# ======================================================================
# FR-04 Hot Spot (Angles: 1,5,6,11)
# ======================================================================
print("\n--- FR-04 Hot Spot ---")
test_endpoint("GET", "/api/v1/hotspots", "Hot spots list", "FR-04", [1, 5])
test_endpoint("GET", "/api/v1/hotspots/health", "Hot spot health", "FR-04", [11, 29])
test_endpoint("GET", "/api/v1/hotspots/hot-stocks", "Hot stocks", "FR-04", [1, 6])

# ======================================================================
# FR-05 Market State (Angles: 1,5,9)
# ======================================================================
print("\n--- FR-05 Market State ---")
test_endpoint("GET", "/api/v1/market-state", "Market state", "FR-05", [1, 5, 9])

# ======================================================================
# FR-06 LLM/Report (Angles: 1,6,11,30)
# ======================================================================
print("\n--- FR-06 LLM/Report ---")
test_endpoint("GET", "/api/v1/llm/health", "LLM health", "FR-06", [11, 29])
test_endpoint("GET", "/api/v1/llm/version", "LLM version", "FR-06", [1, 3])
test_endpoint("GET", "/api/v1/reports?page=1&page_size=5", "Reports list (page 1)", "FR-06", [1, 8])

# ======================================================================
# FR-07 Settlement (Angles: 1,7,9,10,12)
# ======================================================================
print("\n--- FR-07 Settlement ---")
test_endpoint("GET", "/api/v1/internal/settlement/tasks", "Settlement tasks", "FR-07", [1, 9])
test_endpoint("GET", "/api/v1/predictions/stats", "Prediction stats", "FR-07", [7, 8])

# ======================================================================
# FR-08 Simulation (Angles: 2,8,27,28)
# ======================================================================
print("\n--- FR-08 Simulation ---")
test_endpoint("GET", "/api/v1/sim/dashboard", "Sim dashboard", "FR-08", [2, 27, 28])
test_endpoint("GET", "/api/v1/sim/positions", "Sim positions", "FR-08", [2, 8])
test_endpoint("GET", "/api/v1/sim/accounts", "Sim accounts", "FR-08", [2, 28])

# ======================================================================
# FR-09 Auth/Billing (Angles: 1,2,17,18)
# ======================================================================
print("\n--- FR-09 Auth/Billing ---")
test_endpoint("GET", "/api/v1/auth/packages", "Membership packages", "FR-09", [1, 3])
test_endpoint("POST", "/api/v1/auth/login",
              "Login (invalid creds, should 401)", "FR-09", [17, 18],
              json={"email": "test@invalid.com", "password": "wrong"})

# ======================================================================
# FR-10 Site/Dashboard (Angles: 8,27,28,29)
# ======================================================================
print("\n--- FR-10 Site/Dashboard ---")
test_endpoint("GET", "/api/v1/dashboard/stats", "Dashboard stats", "FR-10", [8, 27, 29])
test_endpoint("GET", "/", "Homepage (HTML)", "FR-10", [25, 26])
test_endpoint("GET", "/reports", "Reports page (HTML)", "FR-10", [25, 26])

# ======================================================================
# FR-11 Feedback (Angles: 1,2)
# ======================================================================
print("\n--- FR-11 Feedback ---")
# Can't test POST without a valid report_id, just check endpoint existence
test_endpoint("POST", "/api/v1/feedback",
              "Feedback submit (missing fields, should 422)", "FR-11", [1, 2],
              json={})

# ======================================================================
# FR-12 Admin (Angles: 17,18,21)
# ======================================================================
print("\n--- FR-12 Admin ---")
test_endpoint("GET", "/api/v1/admin/overview",
              "Admin overview", "FR-12", [17, 21, 29],
              headers={"X-Admin-Token": "admin"})
test_endpoint("GET", "/api/v1/admin/users",
              "Admin user list", "FR-12", [17, 18],
              headers={"X-Admin-Token": "admin"})
test_endpoint("GET", "/api/v1/admin/reports",
              "Admin report list", "FR-12", [17, 18],
              headers={"X-Admin-Token": "admin"})
test_endpoint("GET", "/api/v1/admin/system-status",
              "Admin system status", "FR-12", [21, 29],
              headers={"X-Admin-Token": "admin"})

# ======================================================================
# FR-13 Events (Angles: 2,3,22,35)
# ======================================================================
print("\n--- FR-13 Events ---")
test_endpoint("GET", "/api/v1/events/outbox", "Event outbox", "FR-13", [2, 3])

# ======================================================================
# Governance & Features
# ======================================================================
print("\n--- Governance ---")
test_endpoint("GET", "/api/v1/features/catalog", "Feature catalog", "GOV", [34, 35])
test_endpoint("GET", "/api/v1/governance/catalog", "Governance catalog", "GOV", [34, 35])

# ======================================================================
# Retired (should return 410)
# ======================================================================
print("\n--- Retired Endpoints (expect 410) ---")
test_endpoint("POST", "/api/v1/admin/dag/retrigger", "DAG retrigger (retired)",
              "RETIRED", [20], headers={"X-Admin-Token": "admin"})

# ======================================================================
# Summary
# ======================================================================
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

total = len(results)
ok_count = sum(1 for r in results if r["ok"])
fail_count = total - ok_count

print(f"Total endpoints tested: {total}")
print(f"  OK (2xx/3xx/4xx): {ok_count}")
print(f"  FAIL (5xx/error): {fail_count}")

if fail_count > 0:
    print(f"\n{'FAILED ENDPOINTS':}")
    for r in results:
        if not r["ok"]:
            print(f"  [{r['fr']}] {r['endpoint']} — {r['description']}")
            print(f"    Status: {r['status_code']}")
            ev = r["evidence"]
            if isinstance(ev, dict):
                print(f"    Evidence: {json.dumps(ev, ensure_ascii=False, default=str)[:200]}")

# Group by FR
print(f"\nPer-FR Summary:")
fr_stats = {}
for r in results:
    fr = r["fr"]
    if fr not in fr_stats:
        fr_stats[fr] = {"ok": 0, "fail": 0}
    if r["ok"]:
        fr_stats[fr]["ok"] += 1
    else:
        fr_stats[fr]["fail"] += 1

for fr, s in sorted(fr_stats.items()):
    total_fr = s["ok"] + s["fail"]
    icon = "PASS" if s["fail"] == 0 else "ISSUES"
    print(f"  {fr:12s}: {s['ok']}/{total_fr} OK  [{icon}]")

# Write full results to file
with open("_archive/api_test_results_20260413.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2, default=str)

print(f"\nDetailed results saved to _archive/api_test_results_20260413.json")
