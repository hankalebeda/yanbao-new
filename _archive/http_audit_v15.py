"""Phase 1: HTTP audit of all 23 warning feature points."""
import requests
import json
import sys
from datetime import date, timedelta

BASE = "http://127.0.0.1:8010"

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def test(label, method, path, **kwargs):
    url = f"{BASE}{path}"
    try:
        resp = getattr(requests, method)(url, timeout=10, **kwargs)
        status = resp.status_code
        try:
            body = resp.json()
        except Exception:
            body = resp.text[:500]
        # Summarize
        if isinstance(body, dict):
            summary = {k: (str(v)[:100] if isinstance(v, (str, list)) else v)
                       for k, v in list(body.items())[:10]}
        else:
            summary = str(body)[:300]
        icon = "✅" if 200 <= status < 300 else ("⚠️" if status < 500 else "❌")
        print(f"  {icon} [{status}] {label}")
        print(f"     {method.upper()} {path}")
        print(f"     Response: {json.dumps(summary, ensure_ascii=False, default=str)[:200]}")
        return status, body
    except Exception as e:
        print(f"  ❌ {label}: {e}")
        return 0, None

# First get admin token
section("AUTH - Get Admin Token")
# Register a test admin or login
status, body = test("Login admin", "post", "/auth/login",
    json={"email": "admin@test.com", "password": "Admin123!"})
if status == 200 and body:
    token = body.get("access_token") or body.get("data", {}).get("access_token")
else:
    # Try register first
    test("Register", "post", "/auth/register",
        json={"email": "audit@test.com", "password": "Audit123!", "nickname": "auditor"})
    status, body = test("Login", "post", "/auth/login",
        json={"email": "audit@test.com", "password": "Audit123!"})
    token = body.get("access_token") or (body.get("data", {}).get("access_token") if isinstance(body, dict) else None)

if not token:
    print(f"  ⚠️ No token obtained. body={body}")
    # Try extracting differently
    if isinstance(body, dict):
        for k, v in body.items():
            if "token" in k.lower():
                token = v
                break
    if not token:
        print("  FATAL: Cannot get auth token. Using empty.")
        token = ""

headers = {"Authorization": f"Bearer {token}"}
print(f"  Token: {token[:30]}...")

# Internal auth token (for /internal/* routes)
internal_headers = {"X-Internal-Token": "test-internal-token"}

section("FR-01 POOL-06: Concurrent Mutex")
test("Pool status", "get", "/api/v1/admin/scheduler/status", headers=headers)

section("FR-03 COOKIE: Session CRUD")
test("Cookie list", "get", "/api/v1/admin/cookie-session/health", headers=headers)
test("Cookie create", "post", "/api/v1/admin/cookie-session", headers=headers,
     json={"login_source": "weibo", "cookie_string": "SUB=test_cookie_value_123"})
test("Cookie list after", "get", "/api/v1/admin/cookie-session/health", headers=headers)

section("FR-04 HOTSPOT: Data Collection")
test("Hotspot health", "get", "/api/v1/internal/hotspot/health", headers=internal_headers)
test("Hot stocks", "get", "/api/v1/market/hot-stocks")
test("Hotspot collect", "post", "/api/v1/internal/hotspot/collect",
     headers=internal_headers, json={"platform": "weibo", "top_n": 10})

section("FR-05 MARKET STATE")
test("Market state", "get", "/api/v1/market/state")

section("FR-06 LLM: Strategy Engine")
test("LLM health", "get", "/api/v1/admin/llm/health", headers=headers)

section("FR-07 SETTLEMENT")
test("Settlement run", "post", "/api/v1/admin/settlement/run", headers=headers,
     json={"trade_date": str(date.today() - timedelta(days=7)),
            "window_days": 7, "target_scope": "all"})
test("Prediction stats", "get", "/api/v1/predictions/stats", headers=headers)

section("FR-08 SIMULATION")
test("Sim positions", "get", "/api/v1/sim/positions", headers=headers)
test("Sim account summary", "get", "/api/v1/sim/account/summary", headers=headers)
test("Sim account snapshots", "get", "/api/v1/sim/account/snapshots", headers=headers)
test("Sim dashboard", "get", "/api/v1/portfolio/sim-dashboard", headers=headers)

section("FR-10 SITE/DASHBOARD")
test("Home data", "get", "/api/v1/home")
test("Dashboard stats 7d", "get", "/api/v1/dashboard/stats?window_days=7", headers=headers)
test("Dashboard stats 30d", "get", "/api/v1/dashboard/stats?window_days=30", headers=headers)
test("Platform summary", "get", "/api/v1/platform/summary")
test("Platform config", "get", "/api/v1/platform/config")

section("FR-11 FEEDBACK")
test("Feedback submit", "post", "/api/v1/reports/1/feedback", headers=headers,
     json={"rating": 4, "comment": "audit test"})

section("FR-12 ADMIN")
test("Admin overview", "get", "/api/v1/admin/overview", headers=headers)
test("System status", "get", "/api/v1/admin/system-status", headers=headers)
test("User list", "get", "/api/v1/admin/users", headers=headers)

section("FR-13 EVENTS")
test("Features catalog", "get", "/api/v1/features/catalog", headers=headers)
test("Governance catalog", "get", "/api/v1/governance/catalog", headers=headers)

section("HEALTH & PAGES")
test("Health", "get", "/health")
test("Homepage HTML", "get", "/")
test("Reports list HTML", "get", "/reports")
test("Login page HTML", "get", "/login")

section("RETIRED ROUTES (should be 410)")
test("DAG retrigger", "post", "/api/v1/admin/dag/retrigger", headers=headers)
test("Subscription status", "get", "/api/v1/subscription/status", headers=headers)

print("\n\n=== AUDIT COMPLETE ===")
