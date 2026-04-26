"""Phase 0: Comprehensive endpoint test for system audit."""
import http.client
import json


def get(path, headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", 8010, timeout=10)
    h = headers or {}
    conn.request("GET", path, headers=h)
    resp = conn.getresponse()
    body = resp.read().decode()
    conn.close()
    return resp.status, body


def post(path, data=None, headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", 8010, timeout=10)
    h = headers or {}
    h["Content-Type"] = "application/json"
    conn.request("POST", path, body=json.dumps(data or {}), headers=h)
    resp = conn.getresponse()
    body = resp.read().decode()
    conn.close()
    return resp.status, body


def trunc(s, n=400):
    return s[:n] + "..." if len(s) > n else s


def test_endpoint(label, method, path, data=None, headers=None):
    try:
        if method == "GET":
            s, b = get(path, headers)
        else:
            s, b = post(path, data, headers)
        try:
            j = json.loads(b)
            preview = json.dumps(j, indent=2, ensure_ascii=False)[:400]
        except Exception:
            preview = b[:400]
        print(f"\n{'='*60}")
        print(f"[{s}] {label}: {method} {path}")
        print(preview)
        return s, b
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"[ERR] {label}: {method} {path} => {e}")
        return 0, str(e)


# ===== Phase 0: Core Health =====
print("=" * 60)
print("PHASE 0: CORE HEALTH & ENDPOINT SURVEY")
print("=" * 60)

s, b = test_endpoint("Health", "GET", "/health")

# ===== Reports =====
s, b = test_endpoint("Reports List", "GET", "/api/v1/reports?page=1&page_size=2")
if s == 200:
    d = json.loads(b)
    total = d.get("data", {}).get("total", "?")
    print(f"  >>> Total reports: {total}")

# First report detail
if s == 200:
    items = json.loads(b).get("data", {}).get("items", [])
    if items:
        rid = items[0]["report_id"]
        test_endpoint("Report Detail", "GET", f"/api/v1/reports/{rid}")
        test_endpoint("Report Advanced", "GET", f"/api/v1/reports/{rid}/advanced")

# ===== Market =====
test_endpoint("Market State", "GET", "/api/v1/market/state")

# ===== Dashboard =====
test_endpoint("Dashboard Stats", "GET", "/api/v1/dashboard/stats")

# ===== Admin (likely needs auth) =====
test_endpoint("Admin Overview", "GET", "/api/v1/admin/overview")

# ===== Governance =====
test_endpoint("Governance Catalog", "GET", "/api/v1/governance/catalog")

# ===== Sim =====
test_endpoint("Sim Positions", "GET", "/api/v1/sim/positions")
test_endpoint("Sim Account Summary", "GET", "/api/v1/sim/account/summary")
test_endpoint("Sim Account Snapshots", "GET", "/api/v1/sim/account/snapshots")

# ===== Internal =====
test_endpoint("LLM Health", "GET", "/api/v1/internal/llm/health")
test_endpoint("Runtime Gates", "GET", "/api/v1/internal/runtime/gates")

# ===== Auth - Login to get token =====
s, b = test_endpoint("Auth Login", "POST", "/api/v1/auth/login",
                      data={"username": "admin", "password": "admin123"})
token = None
if s == 200:
    d = json.loads(b)
    token = d.get("data", {}).get("access_token")
    if token:
        print(f"  >>> Got token: {token[:20]}...")

# If no token, try other credentials
if not token:
    for creds in [
        {"username": "admin", "password": "password"},
        {"username": "admin@example.com", "password": "admin123"},
        {"email": "admin@example.com", "password": "admin123"},
    ]:
        s, b = test_endpoint("Auth Login (alt)", "POST", "/api/v1/auth/login", data=creds)
        if s == 200:
            d = json.loads(b)
            token = d.get("data", {}).get("access_token")
            if token:
                print(f"  >>> Got token: {token[:20]}...")
                break

# ===== Re-test auth-protected endpoints =====
if token:
    auth_h = {"Authorization": f"Bearer {token}"}
    test_endpoint("Admin Overview (auth)", "GET", "/api/v1/admin/overview", headers=auth_h)
    test_endpoint("Sim Positions (auth)", "GET", "/api/v1/sim/positions", headers=auth_h)
    test_endpoint("Sim Summary (auth)", "GET", "/api/v1/sim/account/summary", headers=auth_h)
    test_endpoint("Report Advanced (auth)", "GET",
                  f"/api/v1/reports/{items[0]['report_id']}/advanced" if items else "/api/v1/reports/test/advanced",
                  headers=auth_h)
    test_endpoint("LLM Health (auth)", "GET", "/api/v1/internal/llm/health", headers=auth_h)
    test_endpoint("Runtime Gates (auth)", "GET", "/api/v1/internal/runtime/gates", headers=auth_h)
    # Cookie sessions
    test_endpoint("Cookie Sessions (auth)", "GET", "/api/v1/admin/cookies", headers=auth_h)
    # Dashboard
    test_endpoint("Dashboard Stats (auth)", "GET", "/api/v1/dashboard/stats", headers=auth_h)

# ===== Pages =====
pages = ["/", "/reports", "/login", "/register", "/dashboard",
         "/admin", "/portfolio/sim-dashboard", "/features", "/profile"]
print(f"\n{'='*60}")
print("PAGE CHECKS")
for p in pages:
    s, b = get(p)
    title = ""
    if "<title>" in b:
        start = b.index("<title>") + 7
        end = b.index("</title>", start)
        title = b[start:end]
    print(f"  [{s}] {p} => {title}")

# ===== DB Stats =====
print(f"\n{'='*60}")
print("DB TABLE ROW COUNTS")
import sqlite3
conn = sqlite3.connect("data/app.db")
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
empty_tables = []
for t in tables:
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{t}"')
        cnt = cur.fetchone()[0]
        if cnt == 0:
            empty_tables.append(t)
        else:
            print(f"  {t}: {cnt}")
    except Exception as e:
        print(f"  {t}: ERROR {e}")
print(f"\n  Empty tables ({len(empty_tables)}): {', '.join(empty_tables)}")
conn.close()

print(f"\n{'='*60}")
print("PHASE 0 COMPLETE")
