"""Round 2 audit verification."""
from fastapi.testclient import TestClient
from app.main import app
client = TestClient(app, base_url="http://127.0.0.1:8000")

# Check duplicate admin users route
print("=== Check routes_business admin users ===")
from app.api import routes_business
for route in routes_business.router.routes:
    path = getattr(route, "path", "")
    if "admin" in path.lower() and "user" in path.lower():
        print(f"  Found: {route.methods} {path}")

# P0-03: Home vs Dashboard Stats
print()
print("=== P0-03: Home vs Dashboard Stats ===")
r = client.get("/api/v1/home")
home = r.json()["data"]
print("Home overall_status:", home.get("overall_status"))
sim = home.get("sim_dashboard")
if sim:
    print("Home sim_dashboard status_100k:", sim.get("status_100k") if sim.get("status_100k") else "N/A")

r = client.get("/api/v1/dashboard/stats", params={"days": 30})
stats = r.json()["data"]
print("Dashboard total_settled:", stats.get("total_settled"))

# Market state endpoint
print()
print("=== Market state endpoint ===")
r = client.get("/api/v1/market/state")
print("/api/v1/market/state:", r.status_code)

# Hot stocks endpoint
r = client.get("/api/v1/hot-stocks", params={"limit": 3})
print("/api/v1/hot-stocks:", r.status_code)

# P0-05: Capital game summary contradiction
print()
print("=== P0-05: Capital game summary ===")
r = client.get("/api/v1/reports", params={"page": 1, "page_size": 1})
reports = r.json()["data"]["items"]
if reports:
    rid = reports[0]["report_id"]
    r2 = client.get("/api/v1/reports/" + rid)
    report = r2.json()["data"]
    capital = report.get("capital_game_summary") or {}
    headline = capital.get("headline", "NONE")
    print("capital_game_summary headline:", headline)
    used_data = report.get("used_data") or []
    for d in used_data:
        dn = (d.get("dataset_name") or "").lower()
        if "etf" in dn or "capital" in dn or "north" in dn:
            print("  Related data:", d.get("dataset_name"), "status=", d.get("status"))

# Check register page activation_url leak
print()
print("=== Register activation_url check ===")
import uuid
email = f"audit_reg_{uuid.uuid4().hex[:8]}@test.com"
r = client.post("/auth/register", json={"email": email, "password": "Test1234"})
resp = r.json()
has_activation = "activation_url" in str(resp) or "activation_token" in str(resp)
print("Register response has activation_url:", has_activation)
print("Response:", resp)

# Check forgot-password page
print()
print("=== Forgot password page ===")
r = client.get("/forgot-password")
print("/forgot-password:", r.status_code)
