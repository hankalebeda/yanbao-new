"""v16 HTTP audit — adapts v15_full for port 8000 + phase1 token + current creds."""
import json
from datetime import date, timedelta
import requests

BASE = "http://127.0.0.1:8000"
INTERNAL_TOKEN = "phase1-audit-token-20260417"
PROXIES = {"http": None, "https": None}
RESULTS = []


def test(label, method, path, auth="none", **kwargs):
    url = f"{BASE}{path}"
    try:
        resp = requests.request(method.upper(), url, timeout=20, proxies=PROXIES, **kwargs)
        status = resp.status_code
        try:
            body = resp.json()
        except Exception:
            body = resp.text[:200]
        icon = "PASS" if 200 <= status < 300 else ("WARN" if status < 500 else "FAIL")
        RESULTS.append({"label": label, "status": status, "icon": icon, "method": method.upper(), "path": path, "auth": auth})
        head = json.dumps(body, ensure_ascii=False, default=str)[:160] if isinstance(body, (dict, list)) else str(body)[:160]
        print(f"  {icon} [{status}] {label}: {head}")
        return status, body
    except Exception as exc:
        print(f"  FAIL [ERR] {label}: {exc}")
        RESULTS.append({"label": label, "status": 0, "icon": "FAIL", "method": method.upper(), "path": path, "auth": auth})
        return 0, None


print("=== AUTH ===")
# Try known creds
status, body = test("Login admin", "post", "/api/v1/auth/login", json={"email": "audit@test.com", "password": "Audit123!"})
token = None
if isinstance(body, dict):
    token = (body.get("data") or {}).get("access_token")
if not token:
    status, body = test("Login admin (/auth/login alias)", "post", "/auth/login", json={"email": "audit@test.com", "password": "Audit123!"})
    if isinstance(body, dict):
        token = (body.get("data") or {}).get("access_token")

admin_h = {"Authorization": f"Bearer {token}"} if token else {}
internal_h = {"X-Internal-Token": INTERNAL_TOKEN}
print(f"  admin_token={'OK' if token else 'NONE'}  internal_token=OK\n")

print("=== ENDPOINTS ===")
tests = [
    # FR-00 Health
    ("FR00: /api/v1/health", "get", "/api/v1/health", "none"),
    ("FR00: /health alias", "get", "/health", "none"),
    # FR-10 Home/Pages
    ("FR10: / (home HTML)", "get", "/", "none"),
    ("FR10: /reports (HTML)", "get", "/reports", "none"),
    ("FR10: /api/v1/home", "get", "/api/v1/home", "none"),
    ("FR10: /api/v1/platform/summary", "get", "/api/v1/platform/summary", "none"),
    ("FR10: /api/v1/platform/config", "get", "/api/v1/platform/config", "none"),
    ("FR10: /api/v1/dashboard/stats window=7", "get", "/api/v1/dashboard/stats?window_days=7", "none"),
    ("FR10: /api/v1/dashboard/stats window=1", "get", "/api/v1/dashboard/stats?window_days=1", "none"),
    ("FR10: /api/v1/dashboard/stats window=30", "get", "/api/v1/dashboard/stats?window_days=30", "none"),
    # Reports
    ("FR-REP: list reports", "get", "/api/v1/reports?limit=5", "none"),
    ("FR-REP: reports/recent (expected: 404)", "get", "/api/v1/reports/recent?limit=5", "none"),
    # Pool / Market
    ("FR01: pool stocks", "get", "/api/v1/pool/stocks", "none"),
    ("FR04: hot-stocks", "get", "/api/v1/market/hot-stocks", "none"),
    ("FR05: market state", "get", "/api/v1/market/state", "none"),
    # LLM health
    ("FR06: llm/health (int)", "get", "/api/v1/internal/llm/health", "internal"),
    ("FR06: llm/version (int)", "get", "/api/v1/internal/llm/version", "internal"),
    # Hotspot health
    ("FR04: hotspot/health (int)", "get", "/api/v1/internal/hotspot/health", "internal"),
    ("FR04: source/fallback-status (int)", "get", "/api/v1/internal/source/fallback-status", "internal"),
    # Settlement / predictions
    ("FR07: predictions/stats", "get", "/api/v1/predictions/stats", "none"),
    # Sim
    ("FR08: sim positions", "get", "/api/v1/sim/positions", "admin"),
    ("FR08: sim account summary", "get", "/api/v1/sim/account/summary", "admin"),
    ("FR08: sim-dashboard", "get", "/api/v1/portfolio/sim-dashboard", "admin"),
    # Auth
    ("FR09: /api/v1/auth/me", "get", "/api/v1/auth/me", "admin"),
    ("FR09: plans", "get", "/api/v1/platform/plans", "none"),
    # Admin
    ("FR12: admin users", "get", "/api/v1/admin/users", "admin"),
    ("FR12: admin overview", "get", "/api/v1/admin/overview", "admin"),
    ("FR12: admin system-status", "get", "/api/v1/admin/system-status", "admin"),
    ("FR12: admin scheduler status", "get", "/api/v1/admin/scheduler/status", "admin"),
    ("FR03: admin cookie health", "get", "/api/v1/admin/cookie-session/health?login_source=weibo", "admin"),
    # Governance
    ("FR13: /api/v1/governance/catalog", "get", "/api/v1/governance/catalog", "admin"),
    ("FR13: /api/v1/features/catalog", "get", "/api/v1/features/catalog", "admin"),
    # OpenAPI
    ("Meta: openapi.json", "get", "/openapi.json", "none"),
]

for item in tests:
    label, method, path, auth = item
    headers = {}
    if auth == "admin":
        headers = admin_h.copy()
    elif auth == "internal":
        headers = internal_h.copy()
    test(label, method, path, auth, headers=headers)

print("\n=== SUMMARY ===")
passc = sum(1 for r in RESULTS if r["icon"] == "PASS")
warnc = sum(1 for r in RESULTS if r["icon"] == "WARN")
failc = sum(1 for r in RESULTS if r["icon"] == "FAIL")
print(f"PASS={passc} WARN={warnc} FAIL={failc}")
print("\nNon-PASS:")
for r in RESULTS:
    if r["icon"] != "PASS":
        print(f"  {r['icon']} [{r['status']}] {r['label']} {r['method']} {r['path']}")

with open("_archive/audit_v16_http_result.json", "w", encoding="utf-8") as f:
    json.dump(RESULTS, f, ensure_ascii=False, indent=2)
print("\nSaved to _archive/audit_v16_http_result.json")
