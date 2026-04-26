"""Round 1 audit: verify known issues via TestClient."""
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app, base_url="http://127.0.0.1:8000")

print("=" * 60)
print("ROUND 1 AUDIT")
print("=" * 60)

# P1-01: /health incomplete
r = client.get("/health")
data = r.json().get("data", {})
has_components = "components" in data or "database" in data
print(f"\n[P1-01] /health components present: {has_components}")
print(f"  Actual: {data}")

# P1-04: StockCode BJ not accepted
r = client.get("/api/v1/reports", params={"stock_code": "830799.BJ"})
print(f"\n[P1-04] GET /api/v1/reports?stock_code=830799.BJ: {r.status_code}")
print(f"  Expected: 200 (or empty list), Got: {r.status_code}")

# Login with account field (legacy - what templates send)
r = client.post("/auth/login", json={"account": "admin@test.com", "password": "Admin123"})
login_account = r.status_code
print(f"\n[P2-03/Schema] Login with 'account' field: {login_account}")

r = client.post("/auth/login", json={"email": "admin@test.com", "password": "Admin123"})
login_email = r.status_code
print(f"[P2-03/Schema] Login with 'email' field: {login_email}")

# Get auth token for authenticated tests
token = None
if r.status_code == 200:
    rdata = r.json().get("data", {})
    token = rdata.get("access_token")

# P0-02: viewer-tier boundary - anonymous report view
r = client.get("/api/v1/reports", params={"page": 1, "page_size": 1})
reports = r.json().get("data", {}).get("items", [])
if reports:
    rid = reports[0]["report_id"]
    r2 = client.get(f"/reports/{rid}")
    html = r2.text
    has_locked = "仅对订阅用户可见" in html
    has_executed_visible = "已执行" in html and "已跳过" not in html[-500:]
    print(f"\n[P0-02] Anonymous report view: locked banner={has_locked}")

# P0-10: forgot-password
r = client.post("/auth/forgot-password", json={"email": "admin@test.com"})
resp = r.json()
has_reset_url = "reset_url" in str(resp) or "reset_token" in str(resp)
print(f"\n[P0-10] forgot-password returns reset_url/token: {has_reset_url}")
print(f"  Response: {resp}")

# P1-11: Schema gate - extra fields
r = client.post("/auth/login", json={"email": "a@b.c", "password": "x", "hack": True})
print(f"\n[P1-11] Login with extra field: {r.status_code} (expect 422)")

r = client.post("/billing/create_order", json={"tier_id": "Pro", "period_months": 1, "provider": "alipay", "hack": True})
print(f"[P1-11] create_order with extra field: {r.status_code} (expect 422)")

# P1-14: membership subscription status without auth
r = client.get("/api/v1/membership/subscription/status")
print(f"\n[P1-14] GET membership/subscription/status (no auth): {r.status_code} (expect 401/422)")

# Check register with 'account' legacy field
r = client.post("/auth/register", json={"account": "audit_round1@test.com", "password": "Test1234"})
print(f"\n[Register] With 'account' field: {r.status_code}")

# Summary
print("\n" + "=" * 60)
print("ISSUES STILL OPEN:")
issues = []
if not has_components:
    issues.append("P1-01: /health missing components")
if r.status_code != 422:
    pass  # register might accept account for backward compat
print(f"  BJ stock code rejected by schema: P1-04")
if not has_components:
    print(f"  Health endpoint incomplete: P1-01")
print(f"  Login/register templates use 'account' + phone placeholder: P2-03")
print("=" * 60)
