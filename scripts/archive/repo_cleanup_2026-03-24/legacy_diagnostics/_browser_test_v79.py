"""v7.9 全页面浏览器测试脚本 — 多角色认证+数据验证+交叉一致性"""
import httpx
import json
import sys

BASE = "http://127.0.0.1:8099"
ISSUES = []

def add_issue(cat, desc):
    ISSUES.append(f"[{cat}] {desc}")
    print(f"  ** ISSUE: {desc}")

def login(email, pw):
    c = httpx.Client(base_url=BASE, follow_redirects=False, timeout=30)
    r = c.post("/auth/login", json={"email": email, "password": pw})
    if r.status_code == 200:
        data = r.json()
        token = data.get("data", {}).get("access_token", "")
        c.headers["Authorization"] = f"Bearer {token}"
        tier = data.get("data", {}).get("tier", "?")
        role = data.get("data", {}).get("role", "?")
        print(f"  Login OK: {email} tier={tier} role={role}")
    else:
        print(f"  Login FAIL: {email} -> {r.status_code}")
        add_issue("AUTH", f"Login failed for {email}: {r.status_code}")
    return c

# 1. Login all roles
print("=" * 60)
print("STEP 1: LOGIN TESTS")
print("=" * 60)
admin = login("browser_admin@test.com", "Admin123456")
pro = login("browser_pro@test.com", "Pro123456")
free_client = login("browser_free@test.com", "Free123456")
anon = httpx.Client(base_url=BASE, follow_redirects=False, timeout=30)

# 2. Anonymous page tests
print("\n" + "=" * 60)
print("STEP 2: ANONYMOUS HTML PAGE TESTS")
print("=" * 60)
pages_anon = [
    ("/", "首页"),
    ("/login", "登录页"),
    ("/register", "注册页"),
    ("/terms", "服务条款"),
    ("/privacy", "隐私政策"),
    ("/subscribe", "订阅页"),
    ("/forgot-password", "忘记密码"),
    ("/reports", "研报列表"),
    ("/dashboard", "统计看板"),
    ("/favicon.ico", "Favicon"),
]
for path, name in pages_anon:
    r = anon.get(path)
    status = "OK" if r.status_code == 200 else f"FAIL({r.status_code})"
    print(f"  {name:12s} {path:30s} -> {status} ({len(r.text)} bytes)")
    if r.status_code != 200:
        add_issue("PAGE", f"{name} {path} returned {r.status_code}")

# 3. Auth-required HTML pages
print("\n" + "=" * 60)
print("STEP 3: AUTH-REQUIRED HTML PAGE TESTS")
print("=" * 60)
pages_auth = [
    ("/profile", "个人中心", "user"),
    ("/admin", "管理后台", "admin"),
    ("/features", "功能地图", "admin"),
    ("/portfolio/sim-dashboard", "模拟看板", "user"),
]
for path, name, req_role in pages_auth:
    for client, label in [(admin, "Admin"), (pro, "Pro"), (free_client, "Free"), (anon, "Anon")]:
        r = client.get(path)
        print(f"  {name:12s} {path:30s} [{label:5s}] -> {r.status_code}")
        # Check access control
        if label == "Anon" and path in ["/profile", "/portfolio/sim-dashboard"]:
            if r.status_code not in [302, 307, 401, 403]:
                add_issue("RBAC", f"Anon accessing {path} got {r.status_code}, expected redirect/401/403")
        if label == "Free" and path in ["/admin", "/features"]:
            if r.status_code not in [302, 307, 401, 403]:
                add_issue("RBAC", f"Free accessing {path} got {r.status_code}, expected 403")

# 4. Home API data verification
print("\n" + "=" * 60)
print("STEP 4: HOME API DATA")
print("=" * 60)
r = pro.get("/api/v1/home")
home_data = r.json().get("data", {})
market_state = home_data.get("market_state")
pool_size = home_data.get("pool_size")
data_status = home_data.get("data_status")
latest = home_data.get("latest_reports", [])
today_count = home_data.get("today_report_count")
print(f"  market_state={market_state}")
print(f"  pool_size={pool_size}")
print(f"  data_status={data_status}")
print(f"  today_report_count={today_count}")
print(f"  latest_reports count={len(latest)}")

# Check latest reports fields
if latest:
    rpt = latest[0]
    print(f"  latest[0] keys: {sorted(rpt.keys())}")
    for f in ["report_id", "stock_code", "stock_name", "trade_date", "recommendation"]:
        v = rpt.get(f, "MISSING")
        if v == "MISSING":
            add_issue("HOME", f"latest_reports missing field: {f}")
        print(f"    {f}={v}")

# 5. Reports List
print("\n" + "=" * 60)
print("STEP 5: REPORTS LIST API")
print("=" * 60)
r = pro.get("/api/v1/reports?page=1&page_size=5")
rlist = r.json().get("data", {})
total = rlist.get("total", 0)
print(f"  total={total}, page={rlist.get('page')}, page_size={rlist.get('page_size')}")
items = rlist.get("items", [])
if items:
    item0 = items[0]
    print(f"  item[0] keys: {sorted(item0.keys())}")
    for f in ["report_id", "stock_code", "stock_name", "trade_date", "recommendation", "strategy_type", "confidence"]:
        v = item0.get(f, "MISSING")
        if v == "MISSING":
            add_issue("REPORT_LIST", f"item missing field: {f}")
        print(f"    {f}={v}")
    # Check data quality
    for item in items[:5]:
        sc = item.get("stock_code", "")
        sn = item.get("stock_name", "")
        if not sn or sn == "None":
            add_issue("REPORT_LIST", f"stock_name empty for {sc}")
        st = item.get("strategy_type", "")
        if st not in ["A", "B", "C", "", None]:
            add_issue("REPORT_LIST", f"invalid strategy_type={st} for {sc}")

# 6. Report Detail — Pro vs Free
print("\n" + "=" * 60)
print("STEP 6: REPORT DETAIL (Pro vs Free)")
print("=" * 60)
rid = items[0].get("report_id", "") if items else ""
if rid:
    # Pro sees full
    rp = pro.get(f"/api/v1/reports/{rid}")
    dp = rp.json().get("data", {})
    print(f"  report_id={rid[:8]}...")
    print(f"  recommendation={dp.get('recommendation')}")
    print(f"  confidence={dp.get('confidence')}")
    print(f"  strategy_type={dp.get('strategy_type')}")
    entry = dp.get("signal_entry_price")
    print(f"  signal_entry_price={entry}")
    
    atr = dp.get("atr_pct")
    print(f"  atr_pct={atr}")
    if atr is not None:
        try:
            atr_f = float(atr)
            if atr_f > 50:
                add_issue("REPORT_DETAIL", f"atr_pct={atr_f} too large (>50), double-conversion?")
            if atr_f < 0:
                add_issue("REPORT_DETAIL", f"atr_pct={atr_f} is negative")
        except:
            pass
    
    # Check instruction card
    ic = dp.get("sim_trade_instruction") or dp.get("instruction_card")
    if ic:
        print(f"  instruction_card type={type(ic).__name__}")
        cards = ic if isinstance(ic, list) else [ic]
        for card in cards[:3]:
            tier_name = card.get("capital_tier", card.get("tier", "?"))
            sl = card.get("stop_loss_price", card.get("stop_loss"))
            tp = card.get("target_price")
            ep = card.get("entry_price", entry)
            skipped = card.get("skipped") or card.get("status") == "SKIPPED"
            print(f"    tier={tier_name}: entry={ep}, stop_loss={sl}, target={tp}, skipped={skipped}")
            if sl is not None and not skipped:
                try:
                    sl_f = float(sl)
                    if sl_f < 0:
                        add_issue("REPORT_DETAIL", f"stop_loss={sl_f} is NEGATIVE for tier={tier_name}")
                    if ep and float(ep) > 0 and sl_f > float(ep):
                        add_issue("REPORT_DETAIL", f"stop_loss={sl_f} > entry={ep} INVERTED for tier={tier_name}")
                except:
                    pass
    
    # Free should see masked
    rf = free_client.get(f"/api/v1/reports/{rid}")
    df = rf.json().get("data", {})
    free_entry = df.get("signal_entry_price", "")
    free_ic = df.get("sim_trade_instruction")
    print(f"  Free: entry_price={free_entry}, instruction_card={type(free_ic).__name__}")
    if free_entry and str(free_entry) not in ["None", "null", ""] and "**" not in str(free_entry):
        add_issue("RBAC", f"Free user sees unmasked price: {free_entry}")
    if free_ic is not None and free_ic != [] and free_ic:
        add_issue("RBAC", f"Free user sees instruction_card (should be null)")

    # Check multiple reports for data consistency
    print("\n  --- Checking 10 reports for data consistency ---")
    r10 = pro.get("/api/v1/reports?page=1&page_size=10")
    items10 = r10.json().get("data", {}).get("items", [])
    neg_sl_count = 0
    large_atr_count = 0
    for it in items10:
        rid2 = it.get("report_id", "")
        rd = pro.get(f"/api/v1/reports/{rid2}").json().get("data", {})
        a2 = rd.get("atr_pct")
        if a2 is not None:
            try:
                if float(a2) > 50:
                    large_atr_count += 1
            except:
                pass
        ic2 = rd.get("sim_trade_instruction") or rd.get("instruction_card")
        if ic2 and isinstance(ic2, list):
            for c2 in ic2:
                sl2 = c2.get("stop_loss_price", c2.get("stop_loss"))
                if sl2 is not None and not c2.get("skipped"):
                    try:
                        if float(sl2) < 0:
                            neg_sl_count += 1
                    except:
                        pass
    if neg_sl_count > 0:
        add_issue("DATA", f"{neg_sl_count}/10 reports have NEGATIVE stop_loss_price")
    if large_atr_count > 0:
        add_issue("DATA", f"{large_atr_count}/10 reports have atr_pct > 50 (double-conversion)")
    print(f"  neg_stop_loss={neg_sl_count}/10, large_atr={large_atr_count}/10")

# 7. Dashboard Stats — cross-check
print("\n" + "=" * 60)
print("STEP 7: DASHBOARD STATS (cross-check)")
print("=" * 60)
for wd in [7, 30, 60]:
    r = pro.get(f"/api/v1/dashboard/stats?window_days={wd}")
    d = r.json().get("data", {})
    wreports = d.get("total_reports")
    wsettled = d.get("total_settled")
    wrate = d.get("overall_win_rate")
    wpnl = d.get("overall_profit_loss_ratio")
    hint = d.get("display_hint")
    warn = d.get("signal_validity_warning") if "signal_validity_warning" in d else "N/A"
    br = d.get("baseline_random")
    bm = d.get("baseline_ma_cross")
    print(f"  window={wd}d: reports={wreports}, settled={wsettled}, win_rate={wrate}, pnl_ratio={wpnl}")
    print(f"    hint={hint}, warning={warn}")
    if br:
        print(f"    baseline_random: wr={br.get('win_rate')}, pnl={br.get('profit_loss_ratio')}")
    if bm:
        print(f"    baseline_ma_cross: wr={bm.get('win_rate')}, pnl={bm.get('profit_loss_ratio')}")
    # Check: if settled >= 30, should have win_rate
    if wsettled and int(wsettled) >= 30:
        if wrate is None:
            add_issue("DASHBOARD", f"window={wd}: settled={wsettled}>=30 but win_rate is null")

# 7b. Dashboard vs Home consistency
print(f"\n  Home market_state={market_state}")
r = pro.get("/api/v1/dashboard/stats?window_days=30")
d30 = r.json().get("data", {})
# The home shows total_reports and total_settled — check consistency
home_total_reports = total  # from reports list total
dash_reports = d30.get("total_reports")
print(f"  Home reports list total={home_total_reports}")
print(f"  Dashboard 30d total_reports={dash_reports}")

# 8. Admin Overview
print("\n" + "=" * 60)
print("STEP 8: ADMIN OVERVIEW")
print("=" * 60)
r = admin.get("/api/v1/admin/overview")
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    d = r.json().get("data", {})
    adm_pool = d.get("pool_size")
    adm_reports = d.get("today_reports")
    adm_buys = d.get("today_buy_signals")
    adm_review = d.get("pending_review")
    adm_active = d.get("active_positions")
    print(f"  pool_size={adm_pool}, today_reports={adm_reports}, buy_signals={adm_buys}")
    print(f"  pending_review={adm_review}, active_positions={adm_active}")
    # Cross-check: admin pool_size vs home pool_size
    if adm_pool != pool_size:
        add_issue("CROSS_CHECK", f"Admin pool_size={adm_pool} vs Home pool_size={pool_size}")
    print(f"  pipeline_stages present: {'pipeline_stages' in d}")
    print(f"  strategy_distribution present: {'strategy_distribution' in d}")
    print(f"  data_freshness present: {'data_freshness' in d}")
else:
    add_issue("ADMIN", f"overview returned {r.status_code}")

# 9. Pool stocks
print("\n" + "=" * 60)
print("STEP 9: POOL STOCKS")
print("=" * 60)
r = pro.get("/api/v1/pool/stocks")
pd = r.json().get("data", {})
pool_total = pd.get("total")
pool_date = pd.get("trade_date")
print(f"  total={pool_total}, trade_date={pool_date}")
# Cross-check with home
if pool_total != pool_size:
    add_issue("CROSS_CHECK", f"Pool stocks total={pool_total} vs Home pool_size={pool_size}")

# 10. Sim Dashboard
print("\n" + "=" * 60)
print("STEP 10: SIM DASHBOARD")
print("=" * 60)
for tier in ["10k", "100k", "500k"]:
    r = pro.get(f"/api/v1/portfolio/sim-dashboard?capital_tier={tier}")
    print(f"  Pro {tier}: status={r.status_code}")
    if r.status_code == 200:
        sd = r.json().get("data", {})
        ec = sd.get("equity_curve", [])
        warn = sd.get("signal_validity_warning")
        hint = sd.get("display_hint")
        four_d = {k: sd.get(k) for k in ["win_rate", "profit_loss_ratio", "max_drawdown", "annualized_alpha"]}
        print(f"    equity_curve_points={len(ec)}, warning={warn}, hint={hint}")
        print(f"    four_dims={four_d}")

# Free: only 100k should work
r = free_client.get("/api/v1/portfolio/sim-dashboard?capital_tier=100k")
print(f"  Free 100k: status={r.status_code}")
r = free_client.get("/api/v1/portfolio/sim-dashboard?capital_tier=500k")
print(f"  Free 500k: status={r.status_code}")
if r.status_code != 403:
    add_issue("RBAC", f"Free user accessing 500k sim-dashboard got {r.status_code}, expected 403")

# 11. Advanced Area
print("\n" + "=" * 60)
print("STEP 11: ADVANCED AREA (reasoning chain)")
print("=" * 60)
if rid:
    r = anon.get(f"/api/v1/reports/{rid}/advanced")
    print(f"  Anon: {r.status_code}")
    if r.status_code not in [401, 403]:
        add_issue("RBAC", f"Anon accessing advanced area got {r.status_code}")
    
    r = free_client.get(f"/api/v1/reports/{rid}/advanced")
    print(f"  Free: {r.status_code}")
    if r.status_code == 200:
        ad = r.json().get("data", {})
        rc_len = len(ad.get("reasoning_chain", ""))
        trunc = ad.get("truncated")
        print(f"    reasoning_chain_len={rc_len}, truncated={trunc}")
        if rc_len > 210:  # some margin
            add_issue("RBAC", f"Free reasoning_chain length={rc_len} exceeds 200 limit")
    
    r = pro.get(f"/api/v1/reports/{rid}/advanced")
    print(f"  Pro: {r.status_code}")
    if r.status_code == 200:
        ad = r.json().get("data", {})
        rc_len = len(ad.get("reasoning_chain", ""))
        trunc = ad.get("truncated")
        print(f"    reasoning_chain_len={rc_len}, truncated={trunc}")

# 12. Scheduler status (admin only)
print("\n" + "=" * 60)
print("STEP 12: SCHEDULER STATUS")
print("=" * 60)
r = admin.get("/api/v1/admin/scheduler/status")
print(f"  Admin: {r.status_code}")
if r.status_code == 200:
    sd = r.json().get("data", {})
    if isinstance(sd, list):
        print(f"  task_runs={len(sd)}")
    elif isinstance(sd, dict):
        print(f"  keys={list(sd.keys())}")

# 13. HTML page content verification
print("\n" + "=" * 60)
print("STEP 13: HTML CONTENT VERIFICATION")
print("=" * 60)
# Check index.html for correct data rendering
r = pro.get("/")
html = r.text
# Check subscribe page prices
r2 = anon.get("/subscribe")
sub_html = r2.text
if "¥99.9/月" in sub_html and "Enterprise" in sub_html:
    # Check if it's the problematic enterprise yearly
    if "¥999.9/年" not in sub_html:
        add_issue("SUBSCRIBE", "Enterprise年费¥999.9/年展示缺失")
    else:
        print("  subscribe.html: Enterprise prices OK")
else:
    print("  subscribe.html: checking prices...")

# Check for nav active states
print(f"  index.html: nav contains 'active': {'active' in html}")

# Check reports list page
r3 = pro.get("/reports")
rlist_html = r3.text
print(f"  reports_list.html: has filter controls: {'filter' in rlist_html.lower()}")

# 14. Database data quality spot check
print("\n" + "=" * 60)
print("STEP 14: DATA QUALITY SPOT CHECK")
print("=" * 60)
# Check several reports for consistent data
r = pro.get("/api/v1/reports?page=1&page_size=20")
items20 = r.json().get("data", {}).get("items", [])
strategy_counts = {"A": 0, "B": 0, "C": 0, "": 0, "None": 0}
rec_counts = {"BUY": 0, "SELL": 0, "HOLD": 0}
for it in items20:
    st = str(it.get("strategy_type", ""))
    if st in strategy_counts:
        strategy_counts[st] += 1
    rec = it.get("recommendation", "")
    if rec in rec_counts:
        rec_counts[rec] += 1
print(f"  strategy_type distribution (top 20): {strategy_counts}")
print(f"  recommendation distribution (top 20): {rec_counts}")

# 15. FINAL SUMMARY
print("\n" + "=" * 60)
print(f"FINAL SUMMARY: {len(ISSUES)} issues found")
print("=" * 60)
for i, iss in enumerate(ISSUES, 1):
    print(f"  {i}. {iss}")

if not ISSUES:
    print("  No issues found! All tests passed.")

admin.close()
pro.close()
free_client.close()
anon.close()
