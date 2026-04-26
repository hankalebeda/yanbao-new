"""Quick HTTP probe of key endpoints for v12.11 audit"""
import requests
import json

BASE = "http://127.0.0.1:8010"
TOKEN_INTERNAL = "kestra-internal-20260327"

PROBES = [
    # Public endpoints
    ("GET", "/api/v1/health", {}, None, "health"),
    ("GET", "/api/v1/home", {}, None, "home"),
    ("GET", "/api/v1/reports?limit=10", {}, None, "reports_list"),
    ("GET", "/api/v1/market/state", {}, None, "market_state"),
    ("GET", "/api/v1/market/hotspots", {}, None, "market_hotspots"),
    ("GET", "/api/v1/search?q=600519", {}, None, "search"),
    ("GET", "/api/v1/dashboard/stats?window_days=7", {}, None, "dashboard_stats"),
    ("GET", "/api/v1/platform/summary", {}, None, "platform_summary"),
    # Auth endpoints
    ("GET", "/api/v1/auth/providers", {}, None, "auth_providers"),
    # Internal endpoints
    ("GET", "/api/v1/internal/runtime/anchors", {"X-Internal-Token": TOKEN_INTERNAL}, None, "runtime_anchors"),
    ("GET", "/api/v1/internal/market/state/current", {"X-Internal-Token": TOKEN_INTERNAL}, None, "internal_market_state"),
    # Admin endpoints (expect 401)
    ("GET", "/api/v1/admin/reports", {}, None, "admin_reports_noauth"),
    ("GET", "/admin", {}, None, "admin_page"),
    ("GET", "/features", {}, None, "features_page"),
]

results = []
for method, path, extra_headers, body, name in PROBES:
    headers = {"Content-Type": "application/json"}
    headers.update(extra_headers)
    try:
        resp = requests.request(method, f"{BASE}{path}", headers=headers, 
                                json=body, timeout=10, allow_redirects=False)
        ct = resp.headers.get("Content-Type", "")
        preview = ""
        if "application/json" in ct:
            try:
                data = resp.json()
                if isinstance(data, dict):
                    # Show key fields
                    if "data" in data:
                        d = data["data"]
                        if isinstance(d, dict):
                            preview = {k: v for k, v in list(d.items())[:5]}
                        else:
                            preview = str(d)[:200]
                    else:
                        preview = {k: v for k, v in list(data.items())[:5]}
            except Exception:
                preview = resp.text[:200]
        else:
            preview = f"[{ct[:30]}] {resp.text[:100]}"
        
        icon = "✅" if resp.status_code < 300 else ("⚠️" if resp.status_code == 401 else "🔴")
        print(f"{icon} {resp.status_code} {method} {path}")
        if resp.status_code not in (401, 403) and preview:
            print(f"   preview: {preview}")
        results.append({"name": name, "status": resp.status_code, "preview": str(preview)[:300]})
    except Exception as e:
        print(f"💥 {method} {path}: {e}")
        results.append({"name": name, "error": str(e)})

print("\n=== SUMMARY ===")
ok = sum(1 for r in results if r.get("status", 999) < 300)
auth = sum(1 for r in results if r.get("status", 0) in (401, 403))
err = sum(1 for r in results if r.get("status", 0) >= 400 and r.get("status", 0) not in (401, 403))
print(f"OK: {ok}, AuthBlocked: {auth}, Error: {err}")
