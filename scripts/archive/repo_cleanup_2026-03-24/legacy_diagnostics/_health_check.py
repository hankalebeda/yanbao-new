"""Quick endpoint health check."""
import requests

BASE = "http://127.0.0.1:8000"
endpoints = [
    ("GET", "/", "Homepage"),
    ("GET", "/login", "Login Page"),
    ("GET", "/register", "Register Page"),
    ("GET", "/reports", "Reports List"),
    ("GET", "/sim/dashboard", "Sim Dashboard"),
    ("GET", "/admin", "Admin Page"),
    ("GET", "/api/v1/platform/summary", "Platform Summary API"),
    ("GET", "/api/v1/market/state", "Market State API"),
    ("GET", "/api/v1/market/hot-stocks", "Hot Stocks API"),
    ("GET", "/api/v1/reports?page=1&page_size=5", "Reports API"),
    ("GET", "/api/v1/platform/config", "Platform Config API"),
    ("GET", "/api/v1/sim/account/summary?tier=100k", "Sim Account API (needs auth)"),
    ("GET", "/docs", "OpenAPI Docs"),
]

print(f"{'Method':<6} {'Status':<8} {'Endpoint':<50} {'Note'}")
print("-" * 100)

for method, path, name in endpoints:
    try:
        if method == "GET":
            r = requests.get(f"{BASE}{path}", timeout=10, allow_redirects=False)
        else:
            r = requests.post(f"{BASE}{path}", timeout=10, allow_redirects=False)
        note = ""
        if r.status_code == 307:
            note = f"-> {r.headers.get('location', '?')}"
        elif r.status_code == 200 and "application/json" in r.headers.get("content-type", ""):
            data = r.json()
            if isinstance(data, dict) and "code" in data:
                note = f"code={data['code']}, msg={str(data.get('message',''))[:40]}"
        print(f"{method:<6} {r.status_code:<8} {name:<50} {note}")
    except Exception as e:
        print(f"{method:<6} {'ERR':<8} {name:<50} {str(e)[:50]}")
