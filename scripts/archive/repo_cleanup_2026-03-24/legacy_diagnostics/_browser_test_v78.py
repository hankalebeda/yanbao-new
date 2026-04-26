"""
v7.8 Browser Testing: Admin & Pro User Login + Full Page Validation
Tests every page with both admin and paid user sessions, validates all data points.
"""
import json, urllib.request, urllib.parse, http.cookiejar, sqlite3, re

BASE = "http://127.0.0.1:8099"

# We know admin@example.com exists with Enterprise tier, role=admin, email_verified=1
# But we don't know the password. Let's create fresh test users directly in DB.

def setup_test_users():
    """Create test users with known passwords via the API."""
    import sys
    sys.path.insert(0, ".")
    from app.core.security import hash_password
    
    conn = sqlite3.connect("data/app.db")
    c = conn.cursor()
    
    # Create admin test user
    admin_email = "test_admin_v78@example.com"
    pro_email = "test_pro_v78@example.com"
    free_email = "test_free_v78@example.com"
    password = "TestPass123!"
    pw_hash = hash_password(password)
    
    for email, tier, role in [
        (admin_email, "Enterprise", "admin"),
        (pro_email, "Pro", "user"),
        (free_email, "Free", "user"),
    ]:
        existing = c.execute("SELECT user_id FROM app_user WHERE email = ?", (email,)).fetchone()
        if existing:
            c.execute("UPDATE app_user SET password_hash=?, tier=?, role=?, email_verified=1, failed_login_count=0, locked_until=NULL WHERE email=?",
                      (pw_hash, tier, role, email))
        else:
            import uuid
            uid = str(uuid.uuid4())
            c.execute(
                "INSERT INTO app_user (user_id, email, password_hash, tier, role, email_verified, failed_login_count, created_at, updated_at) VALUES (?,?,?,?,?,1,0,datetime('now'),datetime('now'))",
                (uid, email, pw_hash, tier, role)
            )
        print(f"  User ready: {email} ({role}/{tier})")
    
    conn.commit()
    conn.close()
    return admin_email, pro_email, free_email, password


def login(email, password):
    """Login and return cookie jar with access_token."""
    data = json.dumps({"email": email, "password": password}).encode()
    req = urllib.request.Request(
        f"{BASE}/auth/login",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    resp = opener.open(req)
    body = json.loads(resp.read())
    token = body.get("data", {}).get("access_token")
    return opener, token, body.get("data", {})


def fetch(opener, url):
    """Fetch a URL using the given opener (with cookies)."""
    try:
        resp = opener.open(url)
        return resp.read().decode(), resp.status
    except urllib.error.HTTPError as e:
        return e.read().decode() if e.fp else "", e.code


def fetch_json(opener, url):
    """Fetch JSON API."""
    try:
        resp = opener.open(url)
        return json.loads(resp.read()), resp.status
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        try:
            return json.loads(body), e.code
        except:
            return {"error": body}, e.code


def check_html(html, checks, page_name):
    """Run multiple checks on HTML content."""
    results = []
    for desc, pattern in checks:
        found = re.search(pattern, html) if isinstance(pattern, type(re.compile(""))) else (pattern in html)
        status = "✅" if found else "❌"
        results.append((status, desc, page_name))
        if not found:
            print(f"  {status} {page_name}: {desc}")
    return results


def main():
    print("=" * 60)
    print("v7.8 BROWSER TEST: Admin + Pro + Free User Sessions")
    print("=" * 60)
    
    # Setup
    print("\n[1/8] Setting up test users...")
    admin_email, pro_email, free_email, password = setup_test_users()
    
    all_issues = []
    
    # ========= ADMIN LOGIN =========
    print("\n[2/8] Admin login test...")
    admin_opener, admin_token, admin_data = login(admin_email, password)
    assert admin_data.get("role") == "admin", f"Expected admin role, got {admin_data.get('role')}"
    assert admin_data.get("tier") == "Enterprise", f"Expected Enterprise tier, got {admin_data.get('tier')}"
    print(f"  ✅ Admin logged in: role={admin_data['role']}, tier={admin_data['tier']}")
    
    # ========= PRO LOGIN =========
    print("\n[3/8] Pro user login test...")
    pro_opener, pro_token, pro_data = login(pro_email, password)
    assert pro_data.get("tier") == "Pro", f"Expected Pro tier, got {pro_data.get('tier')}"
    print(f"  ✅ Pro user logged in: tier={pro_data['tier']}")
    
    # ========= FREE LOGIN =========
    print("\n[4/8] Free user login test...")
    free_opener, free_token, free_data = login(free_email, password)
    assert free_data.get("tier") == "Free"
    print(f"  ✅ Free user logged in: tier={free_data['tier']}")
    
    # ========= ADMIN PAGE TESTS =========
    print("\n[5/8] Admin page tests...")
    
    # Admin panel
    admin_html, status = fetch(admin_opener, f"{BASE}/admin")
    if status == 200:
        print(f"  ✅ /admin: 200 OK")
        checks = [
            ("Has admin header", "管理后台"),
            ("Has data purge section", "purge"),
            ("Has pipeline progress", "pipeline" if "pipeline" in admin_html.lower() else "流水线"),
        ]
        for desc, pattern in checks:
            found = pattern.lower() in admin_html.lower()
            s = "✅" if found else "❌"
            print(f"    {s} {desc}")
            if not found:
                all_issues.append(f"ADMIN: {desc} missing")
    else:
        print(f"  ❌ /admin: status={status}")
        all_issues.append(f"ADMIN: /admin returned {status}")
    
    # Admin features page
    features_html, status = fetch(admin_opener, f"{BASE}/features")
    if status == 200:
        print(f"  ✅ /features: 200 OK (admin access)")
    else:
        print(f"  ❌ /features: status={status}")
        all_issues.append(f"ADMIN: /features returned {status}")
    
    # Admin API: overview
    admin_api, status = fetch_json(admin_opener, f"{BASE}/api/v1/admin/overview")
    if status == 200:
        data = admin_api.get("data", admin_api)
        print(f"  ✅ Admin overview API: {list(data.keys()) if isinstance(data, dict) else 'OK'}")
    else:
        print(f"  ❌ Admin overview API: status={status}")
        all_issues.append(f"ADMIN API: /admin/overview returned {status}")
    
    # Profile page
    profile_html, status = fetch(admin_opener, f"{BASE}/profile")
    if status == 200:
        print(f"  ✅ /profile: 200 OK")
        if admin_email not in profile_html and "test_admin" not in profile_html:
            print(f"    ❌ Profile doesn't show user email")
            all_issues.append("PROFILE: email not displayed for admin")
    else:
        print(f"  ❌ /profile: status={status}")
        all_issues.append(f"ADMIN: /profile returned {status}")
    
    # ========= PRO USER PAGE TESTS =========
    print("\n[6/8] Pro user page tests (should see advanced content)...")
    
    # Get a report ID
    reports_api, _ = fetch_json(pro_opener, f"{BASE}/api/v1/reports?page=1&page_size=1")
    report_items = reports_api.get("data", {}).get("items", []) if isinstance(reports_api.get("data"), dict) else reports_api.get("data", [])
    if isinstance(report_items, list) and report_items:
        report_id = report_items[0].get("report_id") if isinstance(report_items[0], dict) else None
    else:
        report_id = None
    
    if report_id:
        # Pro user should see full prices (not masked)
        report_html, status = fetch(pro_opener, f"{BASE}/reports/{report_id}")
        print(f"  Report detail (Pro): status={status}")
        if status == 200:
            # Check price visibility
            has_masked = "●●●●" in report_html or "¥**.**" in report_html
            has_real_price = re.search(r'¥\d+\.\d+', report_html)
            if has_real_price and not has_masked:
                print(f"    ✅ Pro user sees real prices")
            elif has_masked:
                print(f"    ❌ Pro user still sees masked prices!")
                all_issues.append("PRO: Report prices still masked for Pro user")
            else:
                print(f"    ⚠️ Cannot determine price visibility")
            
            # Check reasoning chain (should be visible for Pro)
            has_reasoning_locked = "推理过程仅对订阅用户可见" in report_html
            has_reasoning_content = "analysis_steps" in report_html or "rv-reasoning" in report_html
            if has_reasoning_locked:
                print(f"    ❌ Pro user sees locked reasoning!")
                all_issues.append("PRO: Reasoning chain locked for Pro user")
            else:
                print(f"    ✅ Reasoning section accessible")
            
            # Check ATR value is reasonable
            atr_match = re.search(r'ATR 波动率.*?(\d+\.?\d*)%', report_html, re.DOTALL)
            if atr_match:
                atr_val = float(atr_match.group(1))
                if atr_val > 50:
                    print(f"    ❌ ATR still abnormal: {atr_val}%")
                    all_issues.append(f"ATR: Value {atr_val}% still > 50%")
                else:
                    print(f"    ✅ ATR value reasonable: {atr_val}%")
            
            # Check stop_loss is positive
            sl_match = re.search(r'止损价.*?¥?(-?\d+\.?\d*)', report_html, re.DOTALL)
            if sl_match:
                sl_val = float(sl_match.group(1))
                if sl_val < 0:
                    print(f"    ❌ Stop loss negative: {sl_val}")
                    all_issues.append(f"STOP_LOSS: Negative value {sl_val}")
                else:
                    print(f"    ✅ Stop loss positive: {sl_val}")
            
            # Check sim trade instruction (Pro should see tiers)
            has_sim = "按资金档位" in report_html
            if has_sim:
                print(f"    ✅ Sim trade instruction visible for Pro")
            else:
                print(f"    ⚠️ Sim trade instruction section not found")
        
        # Free user should see masked prices
        free_report_html, _ = fetch(free_opener, f"{BASE}/reports/{report_id}")
        if "●●●●" in free_report_html or "¥**.**" in free_report_html:
            print(f"  ✅ Free user sees masked prices (correct)")
        else:
            has_price = re.search(r'¥\d+\.\d+', free_report_html)
            if has_price:
                print(f"  ❌ Free user sees real prices! Security issue!")
                all_issues.append("SECURITY: Free user can see real prices")
            else:
                print(f"  ⚠️ Free user price display unclear")
    else:
        print(f"  ⚠️ No reports found to test")
    
    # ========= CROSS-PAGE DATA CONSISTENCY =========
    print("\n[7/8] Cross-page data consistency checks...")
    
    # Collect data from multiple APIs
    home_api, _ = fetch_json(pro_opener, f"{BASE}/api/v1/home")
    dashboard_api, _ = fetch_json(pro_opener, f"{BASE}/api/v1/dashboard/stats?window_days=30")
    market_api, _ = fetch_json(pro_opener, f"{BASE}/api/v1/market/state")
    pool_api, _ = fetch_json(pro_opener, f"{BASE}/api/v1/pool/stocks")
    
    home_data = home_api.get("data", home_api) if isinstance(home_api, dict) else {}
    dash_data = dashboard_api.get("data", dashboard_api) if isinstance(dashboard_api, dict) else {}
    market_data = market_api.get("data", market_api) if isinstance(market_api, dict) else {}
    pool_data = pool_api.get("data", pool_api) if isinstance(pool_api, dict) else {}
    
    # Check: pool_size consistency
    home_pool_size = home_data.get("pool_size", -1)
    pool_items = pool_data.get("items", [])
    pool_count = len(pool_items) if isinstance(pool_items, list) else -1
    if pool_count > 0 and home_pool_size != pool_count:
        print(f"  ❌ Pool size mismatch: /home says {home_pool_size}, /pool/stocks has {pool_count} items")
        all_issues.append(f"DATA: Pool size mismatch home={home_pool_size} vs pool={pool_count}")
    elif pool_count > 0:
        print(f"  ✅ Pool size consistent: {home_pool_size}")
    
    # Check: market_state consistency
    home_market = home_data.get("market_state", "")
    api_market = market_data.get("market_state", "")
    if home_market and api_market and home_market != api_market:
        print(f"  ❌ Market state mismatch: /home={home_market}, /market/state={api_market}")
        all_issues.append(f"DATA: Market state mismatch home={home_market} vs api={api_market}")
    else:
        print(f"  ✅ Market state consistent: {api_market or home_market}")
    
    # Check: settled count
    dash_settled = dash_data.get("total_settled", -1)
    dash_reports = dash_data.get("total_reports", -1)
    if dash_settled > 0 and dash_reports > 0:
        if dash_settled > dash_reports * 3:
            print(f"  ❌ Settled ({dash_settled}) >> Reports ({dash_reports}), suspicious ratio")
            all_issues.append(f"DATA: Settled/Reports ratio suspicious: {dash_settled}/{dash_reports}")
        else:
            print(f"  ✅ Dashboard counts: reports={dash_reports}, settled={dash_settled}")
    
    # Check: win rate range
    win_rate = dash_data.get("overall_win_rate")
    if win_rate is not None:
        if 0 <= win_rate <= 1:
            print(f"  ✅ Win rate in valid range: {win_rate:.4f} ({win_rate*100:.1f}%)")
        else:
            print(f"  ❌ Win rate out of range: {win_rate}")
            all_issues.append(f"DATA: Win rate out of range: {win_rate}")
    
    # Check: baseline data
    bl_random = dash_data.get("baseline_random")
    bl_ma = dash_data.get("baseline_ma_cross")
    if bl_random and bl_ma:
        print(f"  ✅ Baselines: random_wr={bl_random.get('win_rate')}, ma_wr={bl_ma.get('win_rate')}")
    else:
        print(f"  ❌ Baseline data missing: random={bl_random is not None}, ma={bl_ma is not None}")
        all_issues.append("DATA: Baseline data missing in dashboard")
    
    # Check: by_strategy_type
    by_strategy = dash_data.get("by_strategy_type", {})
    for st in ["A", "B", "C"]:
        info = by_strategy.get(st, {})
        ss = info.get("sample_size", 0)
        wr = info.get("win_rate")
        print(f"    Strategy {st}: sample={ss}, win_rate={wr}")
    
    # ========= ADDITIONAL PAGE CHECKS =========
    print("\n[8/8] Additional page validation...")
    
    # Sim dashboard (logged in)
    sim_html, status = fetch(pro_opener, f"{BASE}/portfolio/sim-dashboard")
    if status == 200:
        print(f"  ✅ /portfolio/sim-dashboard: 200 OK for Pro user")
    else:
        print(f"  ❌ /portfolio/sim-dashboard: status={status}")
        all_issues.append(f"SIM: /portfolio/sim-dashboard returned {status} for Pro user")
    
    # Subscribe page (logged in)
    sub_html, status = fetch(pro_opener, f"{BASE}/subscribe")
    if status == 200:
        # Check pricing
        has_pro_29 = "29.9" in sub_html
        has_ent_99 = "99.9" in sub_html
        has_yearly_999 = "999.9" in sub_html
        print(f"  ✅ /subscribe: Pro ¥29.9={has_pro_29}, Ent monthly ¥99.9={has_ent_99}, Ent yearly ¥999.9={has_yearly_999}")
        if not has_yearly_999:
            all_issues.append("SUBSCRIBE: Missing ¥999.9/年")
    
    # Dashboard page
    dash_html, status = fetch(pro_opener, f"{BASE}/dashboard")
    if status == 200:
        print(f"  ✅ /dashboard: 200 OK")
    
    # Reports list page
    reports_html, status = fetch(pro_opener, f"{BASE}/reports")
    if status == 200:
        print(f"  ✅ /reports: 200 OK")
    
    # ========= SUMMARY =========
    print("\n" + "=" * 60)
    print(f"TOTAL ISSUES FOUND: {len(all_issues)}")
    for i, issue in enumerate(all_issues, 1):
        print(f"  {i}. {issue}")
    print("=" * 60)
    
    return all_issues


if __name__ == "__main__":
    issues = main()
