"""Phase 1.2: API endpoint comprehensive testing"""
import urllib.request
import json
import ssl

BASE = "http://127.0.0.1:8010"
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def test_endpoint(method, path, data=None, headers=None):
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
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode('utf-8', errors='replace')[:2000]
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')[:500] if e.fp else ''
        return e.code, body
    except Exception as e:
        return 0, str(e)[:200]

endpoints = [
    # Home & Dashboard
    ("GET", "/api/v1/home", "Home data"),
    ("GET", "/api/v1/dashboard/stats", "Dashboard stats"),
    ("GET", "/api/v1/portfolio/sim-dashboard", "Sim dashboard"),
    ("GET", "/api/v1/pool/stocks", "Pool stocks"),
    
    # Reports
    ("GET", "/api/v1/reports", "Report list"),
    ("GET", "/api/v1/reports?page=1&page_size=3", "Report list paginated"),
    
    # Market
    ("GET", "/api/v1/market/state", "Market state"),
    ("GET", "/api/v1/market/hot-stocks", "Hot stocks"),
    
    # Predictions / Settlement
    ("GET", "/api/v1/predictions/stats", "Prediction stats"),
    
    # Sim
    ("GET", "/api/v1/sim/positions", "Sim positions"),
    ("GET", "/api/v1/sim/account/summary", "Sim account summary"),
    
    # Platform
    ("GET", "/api/v1/platform/config", "Platform config"),
    ("GET", "/api/v1/platform/summary", "Platform summary"),
    
    # Admin (may need auth)
    ("GET", "/api/v1/admin/overview", "Admin overview"),
    ("GET", "/api/v1/admin/system-status", "System status"),
    ("GET", "/api/v1/admin/users", "Admin users"),
    ("GET", "/api/v1/admin/reports", "Admin reports"),
    ("GET", "/api/v1/admin/cookie-sessions", "Cookie sessions"),
    
    # Internal
    ("GET", "/api/v1/internal/llm/health", "LLM health"),
    ("GET", "/api/v1/internal/llm/version", "LLM version"),
    ("GET", "/api/v1/internal/source/fallback-status", "Data source status"),
    ("GET", "/api/v1/internal/hotspot/health", "Hotspot health"),
    ("GET", "/api/v1/internal/metrics/summary", "Metrics summary"),
    ("GET", "/api/v1/internal/runtime/gates", "Runtime gates"),
    
    # Auth (test login)
    ("GET", "/api/v1/auth/me", "Auth me (no token)"),
    
    # Features / Governance
    ("GET", "/api/v1/features/catalog", "Features catalog"),
    ("GET", "/api/v1/governance/catalog", "Governance catalog"),
    
    # HTML pages
    ("GET", "/", "Home page"),
    ("GET", "/reports", "Reports page"),
    ("GET", "/login", "Login page"),
    ("GET", "/register", "Register page"), 
    ("GET", "/subscribe", "Subscribe page"),
    ("GET", "/dashboard", "Dashboard page"),
    ("GET", "/portfolio/sim-dashboard", "Sim dashboard page"),
    ("GET", "/admin", "Admin page"),
    ("GET", "/features", "Features page"),
    ("GET", "/profile", "Profile page"),
    ("GET", "/health", "Health check"),
]

print("=" * 80)
print("API ENDPOINT COMPREHENSIVE TEST")
print("=" * 80)

issues = []
for method, path, name in endpoints:
    status, body = test_endpoint(method, path)
    # Determine health
    is_json = body.strip().startswith('{') or body.strip().startswith('[')
    
    # Check for issues
    issue = None
    if status == 0:
        issue = "CONNECTION_ERROR"
    elif status >= 500:
        issue = f"SERVER_ERROR_{status}"
    elif status == 404:
        issue = "NOT_FOUND"
    elif status == 403:
        # Some endpoints need auth, that's expected
        if path.startswith('/api/v1/admin') or path == '/admin' or path == '/features':
            pass  # Auth required, expected
        else:
            issue = "FORBIDDEN_UNEXPECTED"
    elif is_json:
        try:
            data = json.loads(body)
            # Check for empty data indicators
            if isinstance(data, dict):
                if data.get('detail') and 'error' in str(data.get('detail','')).lower():
                    issue = f"ERROR_RESPONSE: {str(data['detail'])[:80]}"
        except:
            pass
    
    marker = "✅" if not issue and status < 400 else ("⚠️" if issue else ("🔒" if status == 403 else "❌"))
    body_preview = body[:120].replace('\n', ' ') if body else '(empty)'
    print(f"{marker} [{status}] {method} {path}")
    print(f"   Name: {name}")
    print(f"   Body: {body_preview}")
    if issue:
        issues.append((name, path, status, issue))
        print(f"   ISSUE: {issue}")
    print()

print("\n" + "=" * 80)
print(f"ISSUES FOUND: {len(issues)}")
print("=" * 80)
for name, path, status, issue in issues:
    print(f"  [{status}] {path}: {issue}")
