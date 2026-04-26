"""Phase 1: Full HTTP audit with admin + internal auth."""
import requests
import json
from datetime import date, timedelta

BASE = "http://127.0.0.1:8010"
INTERNAL_TOKEN = "kestra-internal-2026"

results = []

def test(label, method, path, auth_type="none", **kwargs):
    url = f"{BASE}{path}"
    try:
        resp = getattr(requests, method)(url, timeout=15, **kwargs)
        status = resp.status_code
        try:
            body = resp.json()
        except Exception:
            body = resp.text[:300]
        icon = "PASS" if 200 <= status < 300 else ("WARN" if status < 500 else "FAIL")
        result = {"label": label, "status": status, "icon": icon, "method": method.upper(), "path": path}
        results.append(result)
        if isinstance(body, dict):
            summary = json.dumps({k: (str(v)[:80] if isinstance(v, (str, list, dict)) else v)
                                  for k, v in list(body.items())[:6]}, ensure_ascii=False, default=str)[:200]
        else:
            summary = str(body)[:150]
        print(f"  {icon} [{status}] {label}: {summary}")
        return status, body
    except Exception as e:
        print(f"  FAIL [ERR] {label}: {e}")
        results.append({"label": label, "status": 0, "icon": "FAIL", "method": method.upper(), "path": path})
        return 0, None

# 1. Get admin token
print("=== AUTH ===")
s, b = test("Login admin", "post", "/auth/login",
    json={"email": "audit@test.com", "password": "Audit123!"})
token = b.get("data", {}).get("access_token") if isinstance(b, dict) else None
if not token:
    print("FATAL: No admin token")
    exit(1)
admin_h = {"Authorization": f"Bearer {token}"}
internal_h = {"X-Internal-Token": INTERNAL_TOKEN}
print(f"  Admin token OK, Internal token OK\n")

# 2. All API endpoints grouped by FR
test_sets = [
    # FR-00 Data Integrity
    ("FR00: Health endpoint", "get", "/health"),

    # FR-01 Pool
    ("FR01-01: Pool refresh tasks", "get", "/api/v1/pool/stocks"),
    ("FR01-06: Scheduler status (admin)", "get", "/api/v1/admin/scheduler/status", "admin"),

    # FR-03 Cookie
    ("FR03-01: Cookie health (admin)", "get", "/api/v1/admin/cookie-session/health", "admin"),
    ("FR03-02: Cookie create (admin)", "post", "/api/v1/admin/cookie-session", "admin",
     {"json": {"login_source": "weibo", "cookie_string": "SUB=audit_v15_test_cookie_001; SUBP=test123"}}),
    ("FR03-02b: Cookie create douyin", "post", "/api/v1/admin/cookie-session", "admin",
     {"json": {"login_source": "douyin", "cookie_string": "sessionid=dy_audit_test_002"}}),
    ("FR03-02c: Cookie create xueqiu", "post", "/api/v1/admin/cookie-session", "admin",
     {"json": {"login_source": "xueqiu", "cookie_string": "xq_a_token=xq_audit_003"}}),
    ("FR03-03: Cookie health after", "get", "/api/v1/admin/cookie-session/health", "admin"),

    # FR-04 Hotspot
    ("FR04-03: Hotspot health (int)", "get", "/api/v1/internal/hotspot/health", "internal"),
    ("FR04-06: Hot stocks", "get", "/api/v1/market/hot-stocks"),
    ("FR04-04: Data source status", "get", "/api/v1/internal/source/fallback-status", "internal"),

    # FR-05 Market State
    ("FR05-01: Market state", "get", "/api/v1/market/state"),

    # FR-06 LLM
    ("FR06-03: LLM health (int)", "get", "/api/v1/internal/llm/health", "internal"),
    ("FR06-04: LLM version (int)", "get", "/api/v1/internal/llm/version", "internal"),

    # FR-07 Settlement
    ("FR07-04: Prediction stats", "get", "/api/v1/predictions/stats"),
    ("FR07-01: Settlement run (admin)", "post", "/api/v1/admin/settlement/run", "admin",
     {"json": {"trade_date": str(date.today() - timedelta(days=7)),
               "window_days": 7, "target_scope": "all"}}),

    # FR-08 Simulation
    ("FR08-01: Sim positions", "get", "/api/v1/sim/positions", "admin"),
    ("FR08-05: Sim snapshots", "get", "/api/v1/sim/account/snapshots", "admin"),
    ("FR08-06: Sim summary", "get", "/api/v1/sim/account/summary", "admin"),
    ("FR08-sim-dash: Sim dashboard", "get", "/api/v1/portfolio/sim-dashboard", "admin"),

    # FR-09 Auth
    ("FR09-08: Auth me", "get", "/auth/me", "admin"),
    ("FR09-09: Plans", "get", "/api/v1/platform/plans"),

    # FR-10 Site
    ("FR10-01: Home API", "get", "/api/v1/home"),
    ("FR10-01b: Home HTML", "get", "/"),
    ("FR10-02: Reports list HTML", "get", "/reports"),
    ("FR10-03: Dashboard stats", "get", "/api/v1/dashboard/stats?window_days=30", "admin"),
    ("FR10-04: Platform summary", "get", "/api/v1/platform/summary"),
    ("FR10-05: Platform config", "get", "/api/v1/platform/config"),
    ("FR10-06: Features page (admin)", "get", "/api/v1/features/catalog", "admin"),

    # FR-11 Feedback (need valid report_id)
    ("FR11-01: Feedback", "post", "/api/v1/reports/placeholder/feedback", "admin",
     {"json": {"rating": 5, "comment": "v15 audit"}}),

    # FR-12 Admin
    ("FR12-01: Users (admin)", "get", "/api/v1/admin/users", "admin"),
    ("FR12-05: Admin overview", "get", "/api/v1/admin/overview", "admin"),
    ("FR12-06: System status", "get", "/api/v1/admin/system-status", "admin"),

    # FR-13 Events
    ("FR13: Governance catalog", "get", "/api/v1/governance/catalog", "admin"),

    # Retired routes
    ("RETIRED: DAG retrigger", "post", "/api/v1/admin/dag/retrigger", "admin"),
    ("RETIRED: Subscription", "get", "/api/v1/subscription/status", "admin"),
]

for item in test_sets:
    label = item[0]
    method = item[1]
    path = item[2]
    auth_type = item[3] if len(item) > 3 else "none"
    extra = item[4] if len(item) > 4 else {}

    headers = {}
    if auth_type == "admin":
        headers = admin_h.copy()
    elif auth_type == "internal":
        headers = internal_h.copy()

    test(label, method, path, auth_type, headers=headers, **extra)

# Summary
print("\n=== SUMMARY ===")
pass_count = sum(1 for r in results if r["icon"] == "PASS")
warn_count = sum(1 for r in results if r["icon"] == "WARN")
fail_count = sum(1 for r in results if r["icon"] == "FAIL")
print(f"PASS: {pass_count}, WARN: {warn_count}, FAIL: {fail_count}")
print("\nWarnings/Failures:")
for r in results:
    if r["icon"] in ("WARN", "FAIL"):
        print(f"  {r['icon']} [{r['status']}] {r['label']} — {r['method']} {r['path']}")
