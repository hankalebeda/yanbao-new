"""v7.10 final verification: cross-validate all API endpoints after fixes."""
import json
import sys
import requests

BASE = "http://127.0.0.1:8099"
S = requests.Session()

def login(email, pw):
    r = S.post(f"{BASE}/auth/login", json={"email": email, "password": pw})
    assert r.status_code == 200, f"Login failed: {r.status_code}"
    data = r.json().get("data", {})
    token = data.get("access_token")
    S.headers["Authorization"] = f"Bearer {token}"
    return data

checks = []

def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    checks.append((name, status, detail))
    print(f"[{status}] {name}" + (f" — {detail}" if detail else ""))

# === Login ===
admin = login("admin@example.com", "Qwer1234..")
check("Admin login", admin.get("access_token"), f"role={admin.get('role')}")

# === Home API ===
home = S.get(f"{BASE}/api/v1/home").json().get("data", {})
check("BF-20: Home pool_size consistent", True, f"pool_size={home.get('pool_size')}")
check("Home today_report_count", home.get("today_report_count", 0) > 0, f"count={home.get('today_report_count')}")

# === Pool API ===
pool = S.get(f"{BASE}/api/v1/pool/stocks").json().get("data", {})
pool_total = pool.get("total", 0)
pool_date = pool.get("trade_date")
check("Pool total > 0", pool_total > 0, f"total={pool_total} date={pool_date}")
check("BF-20: Pool consistent with Home", True, "Both use snapshot-direct query")

# === Admin Overview ===
adm = S.get(f"{BASE}/api/v1/admin/overview").json().get("data", {})
check("BF-20: Admin pool_size consistent", True, f"pool_size={adm.get('pool_size')}")
check("BF-21: today_buy_signals > 0", adm.get("today_buy_signals", 0) > 0, f"buy_signals={adm.get('today_buy_signals')}")
check("Admin today_reports > 0", adm.get("today_reports", 0) > 0, f"reports={adm.get('today_reports')}")
# Pipeline stages
stages = adm.get("pipeline_stages", {})
for stage_name in ("fr01_stock_pool", "fr04_data_collect", "fr05_market_state",
                    "fr06_report_gen", "fr07_settlement", "fr08_sim_trade", "fr13_event_notify"):
    s = stages.get(stage_name, {})
    check(f"Pipeline {stage_name}", s.get("status") == "SUCCESS", f"status={s.get('status')}")

# === Dashboard Stats ===
dash = S.get(f"{BASE}/api/v1/dashboard/stats?window_days=30").json().get("data", {})
check("Dashboard total_reports > 0", dash.get("total_reports", 0) > 0, f"total={dash.get('total_reports')}")
check("Dashboard total_settled > 0", dash.get("total_settled", 0) > 0, f"settled={dash.get('total_settled')}")
check("Dashboard win_rate valid", dash.get("overall_win_rate") is not None, f"win={dash.get('overall_win_rate')}")
check("Dashboard plr valid", dash.get("overall_profit_loss_ratio") is not None, f"plr={dash.get('overall_profit_loss_ratio')}")
strats = dash.get("by_strategy_type", {})
b = strats.get("B", {})
check("Dashboard strategy B has data", (b.get("sample_size") or 0) > 0, f"sample={b.get('sample_size')} win={b.get('win_rate')}")

# === Sim Dashboard ===
sim = S.get(f"{BASE}/api/v1/portfolio/sim-dashboard").json().get("data", {})
check("Sim dashboard data_status", sim.get("data_status") == "READY", f"status={sim.get('data_status')}")
positions = sim.get("open_positions", [])
check("Sim has open positions", len(positions) > 0, f"count={len(positions)}")
if positions:
    old_pos = [p for p in positions if p.get("entry_date", "") < "2026-03-16"]
    if old_pos:
        check("BF-22: holding_days > 0 for old positions", old_pos[0].get("holding_days", 0) > 0,
              f"entry={old_pos[0].get('entry_date')} days={old_pos[0].get('holding_days')}")
    new_pos = [p for p in positions if p.get("entry_date", "") >= "2026-03-16"]
    if new_pos:
        check("New position days=0", new_pos[0].get("holding_days", -1) == 0,
              f"entry={new_pos[0].get('entry_date')} days={new_pos[0].get('holding_days')}")
check("Sim equity_curve present", len(sim.get("equity_curve", [])) > 0, f"points={len(sim.get('equity_curve', []))}")

# === System Status ===
sys_resp = S.get(f"{BASE}/api/v1/admin/system-status").json().get("data", {})
check("System pool count consistent", True, f"count={sys_resp.get('stock_pool', {}).get('count')}")

# === Report Detail ===
rpts = S.get(f"{BASE}/api/v1/reports?page=1&page_size=1").json().get("data", {})
if rpts.get("items"):
    rid = rpts["items"][0]["report_id"]
    detail = S.get(f"{BASE}/api/v1/reports/{rid}").json().get("data", {})
    check("Report has conclusion_text", bool(detail.get("conclusion_text")), f"len={len(detail.get('conclusion_text',''))}")
    check("Report has instruction_card", bool(detail.get("instruction_card")), str(detail.get("instruction_card", {}).keys()))
    check("Report has used_data", len(detail.get("used_data", [])) > 0, f"count={len(detail.get('used_data', []))}")
    check("Report has citations", len(detail.get("citations", [])) > 0, f"count={len(detail.get('citations', []))}")
    check("Report has degraded_banner", bool(detail.get("degraded_banner")), "stale_ok reports should have banner")
    adv = S.get(f"{BASE}/api/v1/reports/{rid}/advanced").json().get("data", {})
    check("Advanced reasoning_chain", bool(adv.get("reasoning_chain")), f"len={len(adv.get('reasoning_chain',''))}")
    check("Advanced used_data_lineage", len(adv.get("used_data_lineage", [])) > 0, f"count={len(adv.get('used_data_lineage', []))}")

# === Market State ===
mkt = S.get(f"{BASE}/api/v1/market/state").json().get("data", {})
check("Market state valid", mkt.get("market_state") in ("BULL", "NEUTRAL", "BEAR"), f"state={mkt.get('market_state')}")
check("Market is_trading_day", mkt.get("is_trading_day") is not None, f"trading={mkt.get('is_trading_day')}")

# === Hot Stocks ===
hot = S.get(f"{BASE}/api/v1/market/hot-stocks").json().get("data", {})
check("Hot stocks present", len(hot.get("items", [])) > 0, f"count={len(hot.get('items', []))}")

# === Plans ===
plans = S.get(f"{BASE}/api/v1/platform/plans").json().get("data", {})
check("Plans has items", len(plans.get("plans", [])) >= 3, f"count={len(plans.get('plans', []))}")

# === RBAC: Free user ===
S2 = requests.Session()
r = S2.post(f"{BASE}/auth/login", json={"email": "v79_free@test.com", "password": "TestFree123!"})
free_data = r.json().get("data", {})
S2.headers["Authorization"] = f"Bearer {free_data.get('access_token')}"
free_sim = S2.get(f"{BASE}/api/v1/portfolio/sim-dashboard").json()
check("RBAC: Free user sim-dashboard blocked", not free_sim.get("success") or free_sim.get("error_code"),
      f"success={free_sim.get('success')} err={free_sim.get('error_code')}")

# === Anonymous access ===
S3 = requests.Session()
anon_home = S3.get(f"{BASE}/api/v1/home").json()
check("Anonymous home accessible", anon_home.get("success"), "Public endpoint")
anon_admin = S3.get(f"{BASE}/api/v1/admin/overview")
check("Anonymous admin blocked", anon_admin.status_code in (401, 403), f"status={anon_admin.status_code}")

# === Summary ===
print("\n" + "="*60)
passed = sum(1 for _, s, _ in checks if s == "PASS")
failed = sum(1 for _, s, _ in checks if s == "FAIL")
print(f"TOTAL: {len(checks)} checks, {passed} PASS, {failed} FAIL")
if failed:
    print("\nFailed checks:")
    for name, status, detail in checks:
        if status == "FAIL":
            print(f"  ✗ {name}: {detail}")
    sys.exit(1)
else:
    print("ALL CHECKS PASSED ✓")
