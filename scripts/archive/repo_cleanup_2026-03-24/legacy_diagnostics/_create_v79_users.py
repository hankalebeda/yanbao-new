"""Create test users for v7.9 browser testing"""
import httpx
import json

BASE = "http://127.0.0.1:8099"

users_to_create = [
    {"email": "v79_pro@test.com", "password": "TestPro123!", "tier": "Pro", "role": "user"},
    {"email": "v79_free@test.com", "password": "TestFree123!", "tier": "Free", "role": "user"},
]

with httpx.Client(timeout=15) as client:
    for u in users_to_create:
        # Register
        resp = client.post(f"{BASE}/auth/register", json={
            "email": u["email"],
            "password": u["password"],
        })
        print(f"Register {u['email']}: {resp.status_code}")
        try:
            print(f"  Response: {json.dumps(resp.json(), ensure_ascii=False, indent=2)[:300]}")
        except:
            print(f"  Response: {resp.text[:300]}")

    # Now login as admin and update tiers
    admin_resp = client.post(f"{BASE}/auth/login", json={
        "email": "admin@example.com",
        "password": "Qwer1234..",
    })
    print(f"\nAdmin login: {admin_resp.status_code}")
    try:
        admin_data = admin_resp.json()
        token = admin_data.get("data", admin_data).get("access_token")
        print(f"  Token: {token[:30]}..." if token else "  No token")
    except:
        print(f"  {admin_resp.text[:200]}")
        token = None

    if token:
        headers = {"Authorization": f"Bearer {token}"}
        # Activate and set tier for test users
        for u in users_to_create:
            # Login to get user_id
            login_resp = client.post(f"{BASE}/auth/login", json={
                "email": u["email"],
                "password": u["password"],
            })
            print(f"\nLogin {u['email']}: {login_resp.status_code}")
            try:
                login_data = login_resp.json()
                print(f"  {json.dumps(login_data, ensure_ascii=False)[:300]}")
            except:
                print(f"  {login_resp.text[:200]}")

    # Test: login admin
    print("\n=== Verify admin login ===")
    resp = client.post(f"{BASE}/auth/login", json={
        "email": "admin@example.com",
        "password": "Qwer1234..",
    })
    print(f"Status: {resp.status_code}")
    try:
        data = resp.json()
        print(json.dumps(data, ensure_ascii=False, indent=2)[:500])
    except:
        print(resp.text[:500])
