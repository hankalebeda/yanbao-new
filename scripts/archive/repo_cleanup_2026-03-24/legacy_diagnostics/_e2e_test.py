"""
LEGACY SCRIPT — DO NOT USE AS GATE/VERIFICATION.
This script references old auth fields (account), old imports (requests), and old endpoints.
For current gate verification, use: pytest tests/ or scripts/verify_lightweight.py

Original: End-to-end functional test: Login -> Browse -> Generate -> View -> Sim Dashboard.
"""
import requests
import json
import time

BASE = "http://127.0.0.1:8000"
API = f"{BASE}/api/v1"

def e2e_test():
    session = requests.Session()
    results = []
    
    # ---- 1. Login ----
    print("=" * 60)
    print("E2E TEST 1: Login")
    print("=" * 60)
    r = session.post(f"{BASE}/auth/login", json={"account": "admin@example.com", "password": "Qwer1234.."})
    assert r.status_code == 200, f"Login failed: {r.status_code} {r.text}"
    data = r.json()
    assert data.get("code") == 0 or data.get("success"), f"Login response unexpected: {data}"
    token = data.get("data", {}).get("access_token") or data.get("access_token")
    assert token, f"No access_token in response: {data}"
    print(f"  PASS: Login successful, got token {token[:20]}...")
    results.append(("Login", True))
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # ---- 2. Browse Reports List ----
    print("\n" + "=" * 60)
    print("E2E TEST 2: Browse Reports")
    print("=" * 60)
    r = session.get(f"{API}/reports?page=1&page_size=10", headers=headers)
    assert r.status_code == 200, f"Reports list failed: {r.status_code}"
    data = r.json()
    items = data.get("data", {}).get("items", [])
    print(f"  Reports found: {len(items)}")
    if items:
        first = items[0]
        print(f"  First report: {first.get('stock_code')} {first.get('recommendation')} conf={first.get('confidence')}")
        report_id = first.get("report_id")
    else:
        print(f"  WARN: No reports found")
        report_id = None
    results.append(("Reports List", len(items) > 0))
    
    # ---- 3. View Report Detail ----
    print("\n" + "=" * 60)
    print("E2E TEST 3: View Report Detail")
    print("=" * 60)
    if report_id:
        r = session.get(f"{BASE}/reports/{report_id}", headers=headers)
        assert r.status_code == 200, f"Report view failed: {r.status_code}"
        html = r.text
        has_stock = any(kw in html for kw in ["stock_code", "stock-code", "研报详情", "结论", "recommendation"])
        print(f"  PASS: Report detail page loaded ({len(html)} bytes)")
        print(f"  Contains stock info: {has_stock}")
        
        # Also test API endpoint
        r_api = session.get(f"{API}/reports/{report_id}", headers=headers)
        api_data = r_api.json()
        report_detail = api_data.get("data", {})
        print(f"  API fields: {list(report_detail.keys())[:10]}...")
        print(f"  Stock: {report_detail.get('stock_code')}")
        print(f"  Recommendation: {report_detail.get('recommendation')}")
        print(f"  Strategy: {report_detail.get('strategy_type')}")
        print(f"  Quality: {report_detail.get('quality_flag')}")
        results.append(("Report Detail", True))
    else:
        results.append(("Report Detail", False))
    
    # ---- 4. View Advanced Section ----
    print("\n" + "=" * 60)
    print("E2E TEST 4: Report Advanced Section")
    print("=" * 60)
    if report_id:
        r = session.get(f"{API}/reports/{report_id}/advanced", headers=headers)
        if r.status_code == 200:
            adv = r.json().get("data", {})
            print(f"  Advanced fields: {list(adv.keys())}")
            print(f"  Has reasoning_chain: {bool(adv.get('reasoning_chain'))}")
            print(f"  Used data lineage: {len(adv.get('used_data_lineage', []))} items")
            results.append(("Advanced Section", True))
        else:
            print(f"  Status: {r.status_code} (may require Pro tier)")
            results.append(("Advanced Section", r.status_code in (200, 403)))
    else:
        results.append(("Advanced Section", False))
    
    # ---- 5. Market State ----
    print("\n" + "=" * 60)
    print("E2E TEST 5: Market State")
    print("=" * 60)
    r = session.get(f"{API}/market/state")
    data = r.json().get("data", {})
    print(f"  Market State: {data.get('market_state')}")
    print(f"  Date: {data.get('market_state_date')}")
    results.append(("Market State", data.get("market_state") in ("BULL", "NEUTRAL", "BEAR")))
    
    # ---- 6. Hot Stocks ----
    print("\n" + "=" * 60)
    print("E2E TEST 6: Hot Stocks")
    print("=" * 60)
    r = session.get(f"{API}/market/hot-stocks?limit=5")
    data = r.json().get("data", {})
    items = data.get("items", [])
    print(f"  Hot stocks: {len(items)} items")
    for item in items[:3]:
        print(f"    {item.get('topic_title', item.get('keyword', '?'))}")
    results.append(("Hot Stocks", True))
    
    # ---- 7. Sim Dashboard ----
    print("\n" + "=" * 60)
    print("E2E TEST 7: Sim Account Summary")
    print("=" * 60)
    r = session.get(f"{API}/sim/account/summary?capital_tier=100k", headers=headers)
    if r.status_code == 200:
        data = r.json().get("data", {})
        print(f"  Tier: 100k")
        for k in ["total_trades", "win_rate", "pnl_ratio", "alpha", "max_drawdown", "cumulative_return"]:
            print(f"    {k}: {data.get(k)}")
        results.append(("Sim Account", True))
    else:
        print(f"  Status: {r.status_code}")
        results.append(("Sim Account", False))
    
    # ---- 8. Sim Positions ----
    print("\n" + "=" * 60)
    print("E2E TEST 8: Sim Positions")
    print("=" * 60)
    r = session.get(f"{API}/sim/positions?page_size=5", headers=headers)
    if r.status_code == 200:
        data = r.json().get("data", {})
        items = data.get("items", [])
        print(f"  Positions: {len(items)}")
        for p in items[:3]:
            print(f"    {p.get('stock_code')} {p.get('position_status')} entry={p.get('actual_entry_price')}")
        results.append(("Sim Positions", True))
    else:
        print(f"  Status: {r.status_code}")
        results.append(("Sim Positions", False))
    
    # ---- 9. Platform Summary ----
    print("\n" + "=" * 60)
    print("E2E TEST 9: Platform Summary")
    print("=" * 60)
    r = session.get(f"{API}/platform/summary")
    data = r.json().get("data", {})
    print(f"  Win rate: {data.get('win_rate')}")
    print(f"  PnL ratio: {data.get('pnl_ratio')}")
    print(f"  Total trades: {data.get('total_trades')}")
    results.append(("Platform Summary", True))
    
    # ---- 10. Admin System Status ----
    print("\n" + "=" * 60)
    print("E2E TEST 10: Admin System Status")
    print("=" * 60)
    r = session.get(f"{API}/admin/system-status", headers=headers)
    if r.status_code == 200:
        data = r.json().get("data", {})
        print(f"  Reports today: {data.get('counts', {}).get('reports_today')}")
        print(f"  Total reports: {data.get('counts', {}).get('reports')}")
        print(f"  Users: {data.get('counts', {}).get('users')}")
        results.append(("Admin Status", True))
    else:
        print(f"  Status: {r.status_code}")
        results.append(("Admin Status", False))
    
    # ---- 11. Page rendering tests ----
    print("\n" + "=" * 60)
    print("E2E TEST 11: Page Rendering")
    print("=" * 60)
    pages = [
        ("/", "Homepage"),
        ("/reports", "Reports List"),
        ("/login", "Login"),
        ("/register", "Register"),
        ("/portfolio/sim-dashboard", "Sim Dashboard"),
        ("/admin", "Admin"),
        ("/dashboard", "Dashboard"),
    ]
    for path, name in pages:
        r = session.get(f"{BASE}{path}", headers=headers, allow_redirects=True)
        ok = r.status_code == 200
        print(f"  {name}: {r.status_code} {'OK' if ok else 'FAIL'} ({len(r.text)} bytes)")
        results.append((f"Page:{name}", ok))
    
    # ---- Summary ----
    print("\n" + "=" * 60)
    print("E2E TEST SUMMARY")
    print("=" * 60)
    passed = sum(1 for _, ok in results if ok)
    total = len(results)
    for name, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'} {name}")
    print(f"\n  Total: {passed}/{total} passed")
    return passed == total

if __name__ == "__main__":
    ok = e2e_test()
    exit(0 if ok else 1)
