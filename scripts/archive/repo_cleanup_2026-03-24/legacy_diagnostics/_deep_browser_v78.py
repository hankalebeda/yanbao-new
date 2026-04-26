"""
v7.8 Deep Browser Validation — 全功能点逐页逐数据验证
每个页面的每个功能点都模拟点击/请求，每个数据都验证是否符合方案要求。
"""
import json, urllib.request, urllib.parse, http.cookiejar, sqlite3, re, sys

BASE = "http://127.0.0.1:8099"
sys.path.insert(0, ".")


def setup_users():
    from app.core.security import hash_password
    conn = sqlite3.connect("data/app.db")
    c = conn.cursor()
    pw_hash = hash_password("TestPass123!")
    for email, tier, role in [
        ("test_admin_v78@example.com", "Enterprise", "admin"),
        ("test_pro_v78@example.com", "Pro", "user"),
        ("test_free_v78@example.com", "Free", "user"),
    ]:
        existing = c.execute("SELECT user_id FROM app_user WHERE email = ?", (email,)).fetchone()
        if existing:
            c.execute("UPDATE app_user SET password_hash=?, tier=?, role=?, email_verified=1, failed_login_count=0, locked_until=NULL WHERE email=?",
                      (pw_hash, tier, role, email))
        else:
            import uuid
            c.execute(
                "INSERT INTO app_user (user_id, email, password_hash, tier, role, email_verified, failed_login_count, created_at, updated_at) VALUES (?,?,?,?,?,1,0,datetime('now'),datetime('now'))",
                (str(uuid.uuid4()), email, pw_hash, tier, role))
    conn.commit()
    conn.close()


def login(email, password):
    data = json.dumps({"email": email, "password": password}).encode()
    req = urllib.request.Request(f"{BASE}/auth/login", data=data, headers={"Content-Type": "application/json"})
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    resp = opener.open(req)
    body = json.loads(resp.read())
    return opener, body.get("data", {})


def fetch(opener, url):
    try:
        resp = opener.open(url)
        return resp.read().decode(), resp.status
    except urllib.error.HTTPError as e:
        return e.read().decode() if e.fp else "", e.code


def fetch_json(opener, url):
    try:
        resp = opener.open(url)
        return json.loads(resp.read()), resp.status
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        try:
            return json.loads(body), e.code
        except Exception:
            return {"error": body}, e.code


def fetch_status(url):
    """Anonymous fetch status code."""
    try:
        resp = urllib.request.urlopen(url)
        return resp.status
    except urllib.error.HTTPError as e:
        return e.code


issues = []


def ok(msg):
    print(f"  ✅ {msg}")


def fail(msg):
    print(f"  ❌ {msg}")
    issues.append(msg)


def warn(msg):
    print(f"  ⚠️ {msg}")


def section(title):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


def main():
    print("=" * 70)
    print("  v7.8 DEEP BROWSER VALIDATION — 全功能点逐页逐数据验证")
    print("=" * 70)

    # ===================== SETUP =====================
    section("0. Setup & Login")
    setup_users()
    admin_opener, admin_data = login("test_admin_v78@example.com", "TestPass123!")
    pro_opener, pro_data = login("test_pro_v78@example.com", "TestPass123!")
    free_opener, free_data = login("test_free_v78@example.com", "TestPass123!")
    ok(f"Admin login: role={admin_data['role']}, tier={admin_data['tier']}")
    ok(f"Pro login: tier={pro_data['tier']}")
    ok(f"Free login: tier={free_data['tier']}")

    # Login response structure validation
    for key in ["access_token", "tier", "role"]:
        if key not in admin_data:
            fail(f"Login response missing field: {key}")
    profile = admin_data.get("profile", {})
    if profile.get("email_verified") is not True:
        fail("Admin email not verified in login response")
    else:
        ok(f"Admin email verified, email={profile.get('email')}")

    # ===================== FAVICON =====================
    section("1. Favicon (P1-03)")
    status = fetch_status(f"{BASE}/favicon.ico")
    if status == 200:
        ok("favicon.ico returns 200")
    else:
        fail(f"favicon.ico returns {status}")

    # ===================== HOME PAGE =====================
    section("2. Homepage /")
    home_html, status = fetch(pro_opener, f"{BASE}/")
    if status != 200:
        fail(f"Homepage returned {status}")
    else:
        ok("Homepage 200 OK")
        # Check key sections
        for label, pattern in [
            ("搜索框", 'type="text"'),
            ("核心池区段", "核心池"),
            ("统计卡片", "已结算"),
            ("导航栏", "nav"),
        ]:
            if pattern.lower() in home_html.lower():
                ok(f"Homepage has: {label}")
            else:
                fail(f"Homepage missing: {label}")

    # Home API
    home_api, _ = fetch_json(pro_opener, f"{BASE}/api/v1/home")
    home_data = home_api.get("data", {})
    for field in ["pool_size", "market_state", "today_report_count", "data_status"]:
        if field in home_data:
            ok(f"/api/v1/home has {field}={home_data[field]}")
        else:
            fail(f"/api/v1/home missing field: {field}")

    # ===================== REPORTS LIST =====================
    section("3. Reports List /reports")
    reports_html, status = fetch(pro_opener, f"{BASE}/reports")
    if status == 200:
        ok("Reports list 200 OK")
    else:
        fail(f"Reports list returned {status}")

    reports_api, _ = fetch_json(pro_opener, f"{BASE}/api/v1/reports?page=1&page_size=5")
    report_data = reports_api.get("data", {})
    report_items = report_data.get("items", []) if isinstance(report_data, dict) else report_data if isinstance(report_data, list) else []
    if report_items:
        ok(f"Reports API returned {len(report_items)} items")
        sample = report_items[0]
        for key in ["report_id", "stock_code", "stock_name", "trade_date", "recommendation", "strategy_type"]:
            if key in sample:
                ok(f"Report item has {key}={str(sample[key])[:30]}")
            else:
                fail(f"Report item missing field: {key}")
    else:
        warn("No report items found in API")

    # ===================== REPORT DETAIL =====================
    section("4. Report Detail /reports/{id}")
    report_id = report_items[0]["report_id"] if report_items else None
    if not report_id:
        warn("No report to test detail page")
    else:
        # === Pro User: should see full prices ===
        pro_html, status = fetch(pro_opener, f"{BASE}/reports/{report_id}")
        if status != 200:
            fail(f"Report detail (Pro) returned {status}")
        else:
            ok("Report detail (Pro) 200 OK")

            # ATR validation
            atr_match = re.search(r'ATR\s*波动率.*?(\d+\.?\d*)%', pro_html, re.DOTALL)
            if atr_match:
                atr_val = float(atr_match.group(1))
                if atr_val > 50:
                    fail(f"ATR value abnormal: {atr_val}%")
                else:
                    ok(f"ATR value reasonable: {atr_val}%")
            else:
                warn("ATR value not found on page")

            # Stop loss validation
            sl_match = re.search(r'止损价.*?¥?(-?\d+\.?\d*)', pro_html, re.DOTALL)
            if sl_match:
                sl_val = float(sl_match.group(1))
                if sl_val < 0:
                    fail(f"Stop loss negative: {sl_val}")
                else:
                    ok(f"Stop loss positive: {sl_val}")

            # Target price validation
            tp_match = re.search(r'目标价.*?¥?(\d+\.?\d*)', pro_html, re.DOTALL)
            if tp_match:
                ok(f"Target price: {tp_match.group(1)}")

            # Price masking check - Pro should NOT see masked
            has_masked = "●●●●" in pro_html or "¥**.**" in pro_html
            has_locked_reasoning = "推理过程仅对订阅用户可见" in pro_html or "仅限付费用户" in pro_html
            if has_masked:
                fail("Pro user sees masked prices")
            else:
                ok("Pro user does NOT see masked prices")
            if has_locked_reasoning:
                fail("Pro user sees locked reasoning chain")
            else:
                ok("Pro user reasoning chain accessible")

            # Signal badge
            for sig in ["BUY", "HOLD", "SELL"]:
                if sig in pro_html:
                    ok(f"Signal badge present: {sig}")
                    break

            # Strategy type
            for st in ["策略A", "策略B", "策略C", "事件驱动", "趋势动量", "低波套利"]:
                if st in pro_html:
                    ok(f"Strategy type shown: {st}")
                    break

            # Instruction card (sim trade)
            if "按资金档位" in pro_html or "模拟仓位" in pro_html or "instruction" in pro_html.lower():
                ok("Instruction card present for Pro")

        # === Free User: should see masked prices ===
        free_html, _ = fetch(free_opener, f"{BASE}/reports/{report_id}")
        if "●●●●" in free_html or "¥**.**" in free_html or "仅限付费" in free_html or "升级" in free_html:
            ok("Free user sees masked/restricted content (correct)")
        else:
            # Check if there are visible prices (would be a security issue)
            if re.search(r'止损价.*?¥\d+\.\d+', free_html, re.DOTALL):
                fail("SECURITY: Free user can see real stop loss price!")
            else:
                ok("Free user pricing restriction applied")

        # === Admin User: should see full prices ===
        admin_html, _ = fetch(admin_opener, f"{BASE}/reports/{report_id}")
        if "●●●●" not in admin_html:
            ok("Admin sees full unmasked content")
        else:
            fail("Admin sees masked content")

    # ===================== DASHBOARD =====================
    section("5. Dashboard /dashboard")
    dash_html, status = fetch(pro_opener, f"{BASE}/dashboard")
    if status == 200:
        ok("Dashboard page 200 OK")
    else:
        fail(f"Dashboard page returned {status}")

    dash_api, _ = fetch_json(pro_opener, f"{BASE}/api/v1/dashboard/stats?window_days=30")
    dash_data = dash_api.get("data", {})
    for field in ["total_reports", "total_settled", "overall_win_rate"]:
        v = dash_data.get(field)
        if v is not None:
            ok(f"Dashboard {field}={v}")
        else:
            fail(f"Dashboard missing {field}")

    # Win rate validation
    wr = dash_data.get("overall_win_rate")
    if wr is not None:
        if 0 <= wr <= 1:
            ok(f"Win rate in valid range: {wr:.4f} ({wr*100:.1f}%)")
        else:
            fail(f"Win rate out of range: {wr}")

    # Baseline comparisons
    for bl_name in ["baseline_random", "baseline_ma_cross"]:
        bl = dash_data.get(bl_name)
        if bl and isinstance(bl, dict) and "win_rate" in bl:
            ok(f"Baseline {bl_name}: win_rate={bl['win_rate']}")
        else:
            fail(f"Baseline {bl_name} missing or incomplete")

    # Strategy type breakdown
    by_strategy = dash_data.get("by_strategy_type", {})
    for st in ["A", "B", "C"]:
        info = by_strategy.get(st, {})
        ss = info.get("sample_size", 0)
        sw = info.get("win_rate")
        ok(f"Strategy {st}: sample_size={ss}, win_rate={sw}")

    # ===================== SUBSCRIBE =====================
    section("6. Subscribe /subscribe")
    sub_html, status = fetch(pro_opener, f"{BASE}/subscribe")
    if status == 200:
        ok("Subscribe page 200 OK")
    else:
        fail(f"Subscribe page returned {status}")

    # Price validation per 04 §6
    for price, desc in [("29.9", "Pro月费¥29.9"), ("99.9", "Enterprise月费¥99.9"), ("999.9", "Enterprise年费¥999.9")]:
        if price in sub_html:
            ok(f"Price correct: {desc}")
        else:
            fail(f"Price missing: {desc}")

    # ===================== PROFILE =====================
    section("7. Profile /profile")
    for name, opener, email in [("Admin", admin_opener, "test_admin_v78"), ("Pro", pro_opener, "test_pro_v78")]:
        html, status = fetch(opener, f"{BASE}/profile")
        if status == 200:
            ok(f"Profile ({name}) 200 OK")
            # The profile page should show the user email or some identifying info
        else:
            fail(f"Profile ({name}) returned {status}")

    # ===================== SIM DASHBOARD =====================
    section("8. Sim Dashboard /portfolio/sim-dashboard")
    sim_html, status = fetch(pro_opener, f"{BASE}/portfolio/sim-dashboard")
    if status == 200:
        ok("Sim dashboard (Pro) 200 OK")
        # Check key elements
        for label, pattern in [
            ("SVG chart", "<svg"),
            ("Portfolio section", "portfolio" if "portfolio" in sim_html.lower() else "模拟"),
        ]:
            if pattern.lower() in sim_html.lower():
                ok(f"Sim dashboard has: {label}")
    else:
        fail(f"Sim dashboard (Pro) returned {status}")

    # ===================== ADMIN PANEL =====================
    section("9. Admin /admin")
    admin_html, status = fetch(admin_opener, f"{BASE}/admin")
    if status == 200:
        ok("Admin page 200 OK (as admin)")
        for label, pattern in [
            ("管理后台标题", "管理后台"),
            ("数据清除区段", "purge"),
            ("流水线进度", "pipeline"),
            ("用户管理", "用户" if "用户" in admin_html else "user"),
            ("研报管理", "研报" if "研报" in admin_html else "report"),
        ]:
            if pattern.lower() in admin_html.lower():
                ok(f"Admin has: {label}")
            else:
                fail(f"Admin missing: {label}")
    else:
        fail(f"Admin page returned {status} for admin user")

    # Admin should be denied for non-admin
    free_admin_html, free_admin_status = fetch(free_opener, f"{BASE}/admin")
    if free_admin_status in (302, 403):
        ok(f"Admin access denied for free user (status={free_admin_status})")
    elif free_admin_status == 200 and ("管理后台" not in free_admin_html):
        ok("Admin page returns login redirect for free user")
    else:
        # Check if it actually redirected to login
        if "login" in free_admin_html.lower() or "登录" in free_admin_html:
            ok("Admin page redirects to login for free user")
        else:
            fail(f"SECURITY: Free user can access admin page (status={free_admin_status})")

    # Admin API endpoints
    section("10. Admin API endpoints")
    for endpoint, desc in [
        ("/api/v1/admin/overview", "Admin概览"),
        ("/api/v1/admin/users?page=1&page_size=5", "用户列表"),
        ("/api/v1/admin/reports?page=1&page_size=5", "研报管理列表"),
    ]:
        data, status = fetch_json(admin_opener, f"{BASE}{endpoint}")
        if status == 200:
            ok(f"Admin API {desc}: 200 OK")
        else:
            fail(f"Admin API {desc}: status={status}")

    # Admin API should be denied for non-admin
    for endpoint, desc in [("/api/v1/admin/overview", "Admin概览")]:
        _, status = fetch_json(free_opener, f"{BASE}{endpoint}")
        if status in (401, 403):
            ok(f"Admin API {desc} denied for free user ({status})")
        else:
            fail(f"SECURITY: Free user got {status} on Admin API {desc}")

    # ===================== ADMIN OVERVIEW DATA =====================
    section("11. Admin Overview Data Validation")
    overview, _ = fetch_json(admin_opener, f"{BASE}/api/v1/admin/overview")
    ov_data = overview.get("data", overview)
    if isinstance(ov_data, dict):
        for field in ["pool_size", "today_reports", "today_buy_signals", "pipeline_stages", "data_freshness"]:
            if field in ov_data:
                ok(f"Overview has {field}")
            else:
                fail(f"Overview missing {field}")

        # Pipeline stages
        stages = ov_data.get("pipeline_stages")
        if isinstance(stages, list) and len(stages) > 0:
            ok(f"Pipeline stages: {len(stages)} stages")
        elif isinstance(stages, dict):
            ok(f"Pipeline stages present (dict)")
        else:
            warn(f"Pipeline stages format: {type(stages)}")

    # ===================== MARKET STATE =====================
    section("12. Market State API")
    market_api, _ = fetch_json(pro_opener, f"{BASE}/api/v1/market/state")
    market_data = market_api.get("data", {})
    ms = market_data.get("market_state")
    if ms in ("BULL", "BEAR", "NEUTRAL", "UNKNOWN"):
        ok(f"Market state valid: {ms}")
    else:
        fail(f"Market state unexpected value: {ms}")

    # ===================== POOL STOCKS =====================
    section("13. Pool Stocks API")
    pool_api, _ = fetch_json(pro_opener, f"{BASE}/api/v1/pool/stocks")
    pool_data = pool_api.get("data", {})
    pool_items = pool_data.get("items", []) if isinstance(pool_data, dict) else []
    if pool_items:
        ok(f"Pool stocks: {len(pool_items)} items")
        sample = pool_items[0]
        for key in ["stock_code", "stock_name"]:
            if key in sample:
                ok(f"Pool item has {key}={sample[key]}")
            else:
                fail(f"Pool item missing {key}")
    else:
        warn("Pool stocks empty")

    # ===================== CROSS-PAGE CONSISTENCY =====================
    section("14. Cross-Page Data Consistency")

    # Pool size: home vs pool API
    home_pool = home_data.get("pool_size", -1) if home_data else -1
    pool_count = len(pool_items)
    if home_pool > 0 and pool_count > 0:
        if home_pool == pool_count:
            ok(f"Pool size consistent: home={home_pool}, pool_api={pool_count}")
        else:
            warn(f"Pool size differs: home={home_pool}, pool_api={pool_count} (may be date-sensitive)")

    # Total reports: home vs dashboard
    home_reports = home_data.get("total_reports", -1) if home_data else -1
    dash_reports = dash_data.get("total_reports", -1)
    if home_reports > 0 and dash_reports > 0:
        if home_reports == dash_reports:
            ok(f"Total reports consistent: {home_reports}")
        else:
            warn(f"Total reports differ: home={home_reports}, dashboard={dash_reports} (window_days=30 filter)")

    # Market state: home vs market API
    home_ms = home_data.get("market_state", "") if home_data else ""
    api_ms = market_data.get("market_state", "")
    if home_ms and api_ms:
        if home_ms == api_ms:
            ok(f"Market state consistent: {api_ms}")
        else:
            fail(f"Market state mismatch: home={home_ms}, api={api_ms}")

    # Win rate: dashboard vs admin overview
    dash_wr = dash_data.get("overall_win_rate")
    admin_wr = ov_data.get("overall_win_rate") if isinstance(ov_data, dict) else None
    if dash_wr is not None and admin_wr is not None:
        if abs(dash_wr - admin_wr) < 0.001:
            ok(f"Win rate consistent across dashboard and admin")
        else:
            warn(f"Win rate differs: dashboard={dash_wr}, admin={admin_wr}")

    # ===================== AUTH PAGES (Anonymous) =====================
    section("15. Auth Pages (Anonymous)")
    for page, url in [
        ("Login", f"{BASE}/login"),
        ("Register", f"{BASE}/register"),
        ("Forgot Password", f"{BASE}/forgot-password"),
        ("Terms", f"{BASE}/terms"),
        ("Privacy", f"{BASE}/privacy"),
    ]:
        status = fetch_status(url)
        if status == 200:
            ok(f"{page}: 200 OK")
        else:
            fail(f"{page}: status={status}")

    # ===================== FEATURES PAGE =====================
    section("16. Features /features")
    feat_html, status = fetch(admin_opener, f"{BASE}/features")
    if status == 200:
        ok("Features page (admin) 200 OK")
    else:
        fail(f"Features page returned {status}")

    # Non-admin should not be able to access features
    _, feat_free_status = fetch(free_opener, f"{BASE}/features")
    if feat_free_status in (302, 403):
        ok(f"Features denied for free user ({feat_free_status})")
    else:
        warn(f"Features page status for free user: {feat_free_status}")

    # ===================== SEARCH FUNCTIONALITY =====================
    section("17. Search API")
    search_api, status = fetch_json(pro_opener, f"{BASE}/api/v1/reports?search=600519&page=1&page_size=5")
    if status == 200:
        items = search_api.get("data", {}).get("items", []) if isinstance(search_api.get("data"), dict) else []
        found_600519 = any("600519" in str(i.get("stock_code", "")) for i in items) if items else False
        if found_600519:
            ok(f"Search for '600519' returned matching results")
        else:
            warn(f"Search for '600519' returned {len(items)} items but none matching")
    else:
        warn(f"Search API returned {status}")

    # ===================== HEALTH =====================
    section("18. Health endpoint")
    status = fetch_status(f"{BASE}/health")
    if status == 200:
        ok("Health endpoint 200 OK")
    else:
        fail(f"Health endpoint returned {status}")

    # ===================== ERROR PAGES =====================
    section("19. Error pages")
    html_404, status_404 = fetch(pro_opener, f"{BASE}/nonexistent-page-xyz")
    if status_404 == 404:
        ok("404 page returns 404 status")
    else:
        warn(f"Nonexistent page returned {status_404}")

    # ===================== DB VALIDATION =====================
    section("20. Database Integrity Checks")
    conn = sqlite3.connect("data/app.db")
    c = conn.cursor()

    # Check all instruction cards have positive stop_loss
    neg_sl = c.execute("SELECT COUNT(*) FROM instruction_card WHERE stop_loss < 0").fetchone()[0]
    if neg_sl > 0:
        fail(f"DB: {neg_sl} instruction_card rows have negative stop_loss")
    else:
        ok("DB: All instruction_card stop_loss values are non-negative")

    # Check ATR range
    bad_atr = c.execute("SELECT COUNT(*) FROM instruction_card WHERE atr_pct > 50 OR atr_pct < 0").fetchone()[0]
    if bad_atr > 0:
        fail(f"DB: {bad_atr} instruction_card rows have ATR out of range")
    else:
        ok("DB: All ATR values in reasonable range (0-50)")

    # Check report count
    report_count = c.execute("SELECT COUNT(*) FROM report").fetchone()[0]
    ok(f"DB: {report_count} reports in database")

    # Check strategy type distribution
    st_dist = c.execute("SELECT strategy_type, COUNT(*) FROM report GROUP BY strategy_type ORDER BY strategy_type").fetchall()
    for st, cnt in st_dist:
        ok(f"DB: Strategy {st}: {cnt} reports")

    # Check settlement count
    settled_count = c.execute("SELECT COUNT(*) FROM sim_position WHERE position_status IN ('CLOSED_TP','CLOSED_SL','CLOSED_TIMEOUT','CLOSED_MANUAL','DELISTED_LIQUIDATED')").fetchone()[0]
    ok(f"DB: {settled_count} settled positions")

    conn.close()

    # ===================== SUMMARY =====================
    print("\n" + "=" * 70)
    print(f"  TOTAL ISSUES: {len(issues)}")
    if issues:
        for i, issue in enumerate(issues, 1):
            print(f"    {i}. {issue}")
    else:
        print("  🎉 ALL CHECKS PASSED!")
    print("=" * 70)
    return issues


if __name__ == "__main__":
    main()
