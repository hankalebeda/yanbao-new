"""Phase 1.2b: Authenticated API endpoint testing"""
import urllib.request
import json

BASE = "http://127.0.0.1:8010"

def http(method, path, data=None, headers=None):
    url = f"{BASE}{path}"
    try:
        if data:
            req = urllib.request.Request(url, data=json.dumps(data).encode(), method=method)
            req.add_header('Content-Type', 'application/json')
        else:
            req = urllib.request.Request(url, method=method)
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode('utf-8', errors='replace')
            return resp.status, json.loads(body) if body.strip().startswith('{') or body.strip().startswith('[') else body
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace') if e.fp else ''
        try:
            return e.code, json.loads(body)
        except:
            return e.code, body
    except Exception as e:
        return 0, str(e)

# Step 1: Register a test admin user
print("=== REGISTER TEST USER ===")
status, resp = http("POST", "/api/v1/auth/register", {
    "email": "admin_audit@test.com",
    "password": "TestAdmin123!",
    "nickname": "AuditAdmin"
})
print(f"Register: [{status}] {json.dumps(resp, ensure_ascii=False)[:200] if isinstance(resp, dict) else str(resp)[:200]}")

# Step 2: Login to get token
print("\n=== LOGIN ===")
status, resp = http("POST", "/api/v1/auth/login", {
    "email": "admin_audit@test.com",
    "password": "TestAdmin123!"
})
print(f"Login: [{status}]")
token = None
if isinstance(resp, dict):
    # Try to extract token from various response structures
    data = resp.get('data', resp)
    token = data.get('access_token') or data.get('token')
    print(f"Token: {token[:30]}..." if token else f"Response: {json.dumps(resp, ensure_ascii=False)[:200]}")

if not token:
    # Try getting a user from DB and check existing credentials
    print("\nNo token from login, trying to find existing admin...")
    # Try default admin login combinations
    for email in ["admin@example.com", "admin@admin.com", "test@test.com"]:
        status, resp = http("POST", "/api/v1/auth/login", {
            "email": email,
            "password": "admin123"
        })
        if isinstance(resp, dict):
            data = resp.get('data', resp)
            token = data.get('access_token') or data.get('token')
            if token:
                print(f"Login success with {email}: token={token[:30]}...")
                break
        print(f"  [{status}] {email}: {json.dumps(resp, ensure_ascii=False)[:100] if isinstance(resp, dict) else str(resp)[:100]}")

if not token:
    print("\nFailed to get auth token. Check auth setup.")
    exit(1)

auth_headers = {"Authorization": f"Bearer {token}"}

# Step 3: Check if we need admin role
print("\n=== CHECK AUTH/ME ===")
# Try different auth/me paths
for path in ["/api/v1/auth/me", "/auth/me"]:
    status, resp = http("GET", path, headers=auth_headers)
    print(f"  [{status}] {path}: {json.dumps(resp, ensure_ascii=False)[:200] if isinstance(resp, dict) else str(resp)[:200]}")

# Step 4: Test admin endpoints with auth
print("\n=== ADMIN ENDPOINTS (AUTHENTICATED) ===")
admin_endpoints = [
    ("GET", "/api/v1/admin/overview", "Admin overview"),
    ("GET", "/api/v1/admin/system-status", "System status"),
    ("GET", "/api/v1/admin/users", "Admin users"),
    ("GET", "/api/v1/admin/reports?page=1&page_size=3", "Admin reports"),
    ("GET", "/api/v1/admin/cookie-sessions", "Cookie sessions"),
    ("GET", "/api/v1/admin/scheduler/status", "Scheduler status"),
]
for method, path, name in admin_endpoints:
    status, resp = http(method, path, headers=auth_headers)
    body_str = json.dumps(resp, ensure_ascii=False)[:200] if isinstance(resp, dict) else str(resp)[:200]
    marker = "✅" if status == 200 else ("🔒" if status in (401, 403) else "❌")
    print(f"{marker} [{status}] {name}: {body_str}")

# Step 5: Test internal endpoints with INTERNAL_CRON_TOKEN
print("\n=== INTERNAL ENDPOINTS ===")
# Check if there's an INTERNAL_CRON_TOKEN in .env
import os
cron_token = None
try:
    with open('.env') as f:
        for line in f:
            if line.startswith('INTERNAL_CRON_TOKEN='):
                cron_token = line.split('=', 1)[1].strip().strip('"').strip("'")
                break
except:
    pass

internal_headers = {}
if cron_token:
    internal_headers["X-Internal-Token"] = cron_token
    print(f"Using INTERNAL_CRON_TOKEN: {cron_token[:10]}...")
else:
    # Try with admin auth
    internal_headers = auth_headers
    print("Using admin auth for internal endpoints")

internal_endpoints = [
    ("GET", "/api/v1/internal/llm/health", "LLM health"),
    ("GET", "/api/v1/internal/llm/version", "LLM version"),
    ("GET", "/api/v1/internal/source/fallback-status", "Data source status"),
    ("GET", "/api/v1/internal/hotspot/health", "Hotspot health"),
    ("GET", "/api/v1/internal/metrics/summary", "Metrics summary"),
    ("GET", "/api/v1/internal/runtime/gates", "Runtime gates"),
    ("GET", "/api/v1/internal/audit/context", "Audit context"),
]
for method, path, name in internal_endpoints:
    status, resp = http(method, path, headers=internal_headers)
    body_str = json.dumps(resp, ensure_ascii=False)[:200] if isinstance(resp, dict) else str(resp)[:200]
    marker = "✅" if status == 200 else ("🔒" if status in (401, 403) else "❌")
    print(f"{marker} [{status}] {name}: {body_str}")

# Step 6: Test features/governance
print("\n=== FEATURES/GOVERNANCE ===")
for method, path, name in [
    ("GET", "/api/v1/features/catalog", "Features catalog"),
    ("GET", "/api/v1/governance/catalog", "Governance catalog"),
]:
    status, resp = http(method, path, headers=auth_headers)
    body_str = json.dumps(resp, ensure_ascii=False)[:200] if isinstance(resp, dict) else str(resp)[:200]
    marker = "✅" if status == 200 else "❌"
    print(f"{marker} [{status}] {name}: {body_str}")

# Step 7: Test a specific report detail + advanced
print("\n=== REPORT DETAIL + ADVANCED ===")
status, resp = http("GET", "/api/v1/reports?page=1&page_size=1")
if isinstance(resp, dict) and resp.get('data', {}).get('items'):
    report_id = resp['data']['items'][0]['report_id']
    stock_code = resp['data']['items'][0]['stock_code']
    print(f"Testing report: {report_id} ({stock_code})")
    
    status, resp2 = http("GET", f"/api/v1/reports/{report_id}")
    print(f"  Detail [{status}]: has data={bool(resp2.get('data') if isinstance(resp2, dict) else False)}")
    if isinstance(resp2, dict) and resp2.get('data'):
        d = resp2['data']
        has_conclusion = bool(d.get('conclusion_text'))
        has_reasoning = bool(d.get('reasoning_chain_md'))
        has_recommendation = bool(d.get('recommendation'))
        print(f"  conclusion_text: {'✅' if has_conclusion else '❌'}")
        print(f"  reasoning_chain_md: {'✅' if has_reasoning else '❌'}")
        print(f"  recommendation: {'✅' if has_recommendation else '❌'}")
    
    status, resp3 = http("GET", f"/api/v1/reports/{report_id}/advanced")
    print(f"  Advanced [{status}]: {json.dumps(resp3, ensure_ascii=False)[:200] if isinstance(resp3, dict) else str(resp3)[:200]}")

# Step 8: Test sim endpoints with auth
print("\n=== SIM ENDPOINTS (AUTHENTICATED) ===")
sim_endpoints = [
    ("GET", "/api/v1/sim/positions", "Sim positions"),
    ("GET", "/api/v1/sim/account/summary", "Sim account summary"),
    ("GET", "/api/v1/sim/account/snapshots", "Sim account snapshots"),
    ("GET", "/api/v1/portfolio/sim-dashboard", "Sim dashboard"),
]
for method, path, name in sim_endpoints:
    status, resp = http(method, path, headers=auth_headers)
    body_str = json.dumps(resp, ensure_ascii=False)[:200] if isinstance(resp, dict) else str(resp)[:200]
    marker = "✅" if status == 200 else "❌"
    print(f"{marker} [{status}] {name}: {body_str}")

# Step 9: Test report by stock code (legacy route)
print("\n=== LEGACY REPORT BY CODE ===")
status, resp = http("GET", "/report/600519.SH")
print(f"  [{status}] /report/600519.SH: {'HTML' if isinstance(resp, str) and '<html' in resp.lower() else resp}")

print("\n=== DONE ===")
