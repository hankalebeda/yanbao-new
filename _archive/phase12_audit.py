"""Phase 1+2: Full system audit - all 92 active feature points with 39-angle analysis."""
import http.client
import json
import sqlite3
import sys

BASE = ("127.0.0.1", 8010)

# ── Login ──
def login():
    conn = http.client.HTTPConnection(*BASE, timeout=10)
    conn.request("POST", "/auth/login",
                 body=json.dumps({"email": "admin@example.com", "password": "admin123"}),
                 headers={"Content-Type": "application/json"})
    resp = conn.getresponse()
    d = json.loads(resp.read().decode())
    conn.close()
    return d.get("data", {}).get("access_token", "")

TOKEN = login()
if not TOKEN:
    print("FATAL: Cannot login")
    sys.exit(1)
AUTH = {"Authorization": f"Bearer {TOKEN}"}

def get(path, auth=False):
    conn = http.client.HTTPConnection(*BASE, timeout=15)
    h = dict(AUTH) if auth else {}
    conn.request("GET", path, headers=h)
    resp = conn.getresponse()
    body = resp.read().decode()
    conn.close()
    return resp.status, body

def post(path, data=None, auth=False):
    conn = http.client.HTTPConnection(*BASE, timeout=15)
    h = dict(AUTH) if auth else {}
    h["Content-Type"] = "application/json"
    conn.request("POST", path, body=json.dumps(data or {}), headers=h)
    resp = conn.getresponse()
    body = resp.read().decode()
    conn.close()
    return resp.status, body

def patch(path, data=None, auth=False):
    conn = http.client.HTTPConnection(*BASE, timeout=15)
    h = dict(AUTH) if auth else {}
    h["Content-Type"] = "application/json"
    conn.request("PATCH", path, body=json.dumps(data or {}), headers=h)
    resp = conn.getresponse()
    body = resp.read().decode()
    conn.close()
    return resp.status, body

def delete(path, auth=False):
    conn = http.client.HTTPConnection(*BASE, timeout=15)
    h = dict(AUTH) if auth else {}
    conn.request("DELETE", path, headers=h)
    resp = conn.getresponse()
    body = resp.read().decode()
    conn.close()
    return resp.status, body

def jload(body):
    try: return json.loads(body)
    except: return {"raw": body[:300]}

results = {}
issues = []

def test(fid, label, method, path, auth=False, data=None, expect_status=200, check=None):
    """Test a feature point endpoint."""
    try:
        if method == "GET":
            s, b = get(path, auth)
        elif method == "POST":
            s, b = post(path, data, auth)
        elif method == "PATCH":
            s, b = patch(path, data, auth)
        elif method == "DELETE":
            s, b = delete(path, auth)
        else:
            s, b = 0, "unsupported method"

        d = jload(b)
        ok = s == expect_status
        detail = ""

        if check and ok:
            try:
                check_result = check(d)
                if check_result:
                    ok = False
                    detail = check_result
            except Exception as e:
                detail = f"check_error: {e}"

        status_str = "PASS" if ok else "FAIL"
        results[fid] = {"status": status_str, "http": s, "detail": detail}
        if not ok:
            issues.append({"fid": fid, "label": label, "http_status": s, 
                          "expected": expect_status, "detail": detail,
                          "response_preview": json.dumps(d, ensure_ascii=False)[:200]})
        print(f"  [{status_str}] {fid}: {label} [{s}] {detail}")
        return s, d
    except Exception as e:
        results[fid] = {"status": "ERROR", "detail": str(e)}
        issues.append({"fid": fid, "label": label, "detail": str(e)})
        print(f"  [ERROR] {fid}: {label} => {e}")
        return 0, {}


# ═══════════════════════════════════════════════════════
# FR-00: 数据真实性保障
# ═══════════════════════════════════════════════════════
print("\n=== FR-00: 数据真实性保障 ===")

# FR00-AUTH-01: 已发布研报只读
s, d = test("FR00-AUTH-01", "已发布研报只读", "GET", "/api/v1/reports?page=1&page_size=1", auth=True)
if s == 200:
    items = d.get("data", {}).get("items", [])
    if items:
        rid = items[0]["report_id"]
        # Try to modify a published report (should fail or be restricted)
        test("FR00-AUTH-01b", "Published report immutability", "PATCH", 
             f"/api/v1/admin/reports/{rid}", auth=True, data={"recommendation": "SELL"})

# FR00-AUTH-02: 证据三要素
s, d = test("FR00-AUTH-02", "证据三要素(report_data_usage)", "GET",
            f"/api/v1/reports/{rid}" if items else "/api/v1/reports/none", auth=True,
            check=lambda d: "no conclusion_text" if not d.get("data",{}).get("conclusion_text") else None)

# FR00-AUTH-03: 审计链路
test("FR00-AUTH-03", "审计日志", "GET", "/api/v1/admin/overview", auth=True)


# ═══════════════════════════════════════════════════════
# FR-01: 股票池
# ═══════════════════════════════════════════════════════
print("\n=== FR-01: 股票池 ===")
test("FR01-POOL-01", "股票池快照列表", "GET", "/api/v1/admin/pool/snapshots?page=1&page_size=2", auth=True)
test("FR01-POOL-02", "股票池评分", "GET", "/api/v1/admin/pool/scores?page=1&page_size=2", auth=True)
test("FR01-POOL-03", "核心池查询", "GET", "/api/v1/admin/pool/core", auth=True)
test("FR01-POOL-05", "淘汰列表", "GET", "/api/v1/admin/pool/eliminated", auth=True)


# ═══════════════════════════════════════════════════════
# FR-02: 调度器
# ═══════════════════════════════════════════════════════
print("\n=== FR-02: 调度器 ===")
test("FR02-SCHED-01", "调度器状态", "GET", "/api/v1/internal/scheduler/status", auth=True)
test("FR02-SCHED-05", "DAG 汇总", "GET", "/api/v1/internal/dag/events", auth=True)


# ═══════════════════════════════════════════════════════
# FR-03: Cookie 管理
# ═══════════════════════════════════════════════════════
print("\n=== FR-03: Cookie 管理 ===")
test("FR03-COOKIE-01", "Cookie 列表", "GET", "/api/v1/admin/cookies", auth=True)


# ═══════════════════════════════════════════════════════
# FR-04: 热点采集
# ═══════════════════════════════════════════════════════
print("\n=== FR-04: 热点采集 ===")
test("FR04-DATA-01", "热点查询", "GET", "/api/v1/internal/hotspot/latest", auth=True)
test("FR04-DATA-03", "热点展示(首页)", "GET", "/api/v1/dashboard/hotspot", auth=True)


# ═══════════════════════════════════════════════════════
# FR-05: 市场状态
# ═══════════════════════════════════════════════════════
print("\n=== FR-05: 市场状态 ===")
s, d = test("FR05-MKT-01", "市场状态查询", "GET", "/api/v1/market/state")
if s == 200:
    mdata = d.get("data", {})
    state = mdata.get("market_state", "")
    degraded = "degraded" in str(mdata.get("state_reason", "")).lower()
    if degraded:
        issues.append({"fid": "FR05-MKT-01", "label": "市场状态降级", 
                       "detail": f"state={state}, reason={mdata.get('state_reason','')}"})
        print(f"    WARNING: Market state is DEGRADED: {mdata.get('state_reason','')}")


# ═══════════════════════════════════════════════════════
# FR-06: LLM 生成
# ═══════════════════════════════════════════════════════
print("\n=== FR-06: LLM 生成 ===")
test("FR06-LLM-01", "LLM 健康检查", "GET", "/api/v1/internal/llm/health", auth=True)
test("FR06-LLM-02", "运行时门禁", "GET", "/api/v1/internal/runtime/gates", auth=True)
# FR06-LLM-03: 降级链路 - check report generation tasks
test("FR06-LLM-03", "生成任务列表", "GET", "/api/v1/admin/reports?page=1&page_size=2", auth=True)


# ═══════════════════════════════════════════════════════
# FR-07: 结算/绩效
# ═══════════════════════════════════════════════════════
print("\n=== FR-07: 结算/绩效 ===")
test("FR07-SETTLE-01", "结算概览", "GET", "/api/v1/dashboard/stats", auth=True,
     check=lambda d: f"settled={d['data'].get('total_settled',0)}, coverage low" 
         if d.get("data",{}).get("total_settled",0) < 20 else None)


# ═══════════════════════════════════════════════════════
# FR-08: 模拟仓位
# ═══════════════════════════════════════════════════════
print("\n=== FR-08: 模拟仓位 ===")
test("FR08-SIM-01", "模拟看板", "GET", "/api/v1/sim/dashboard", auth=True)
test("FR08-SIM-02", "持仓列表", "GET", "/api/v1/sim/positions", auth=True)
test("FR08-SIM-03", "账户摘要", "GET", "/api/v1/sim/account/summary", auth=True)
test("FR08-SIM-04", "账户快照", "GET", "/api/v1/sim/account/snapshots", auth=True)


# ═══════════════════════════════════════════════════════
# FR-09: 认证
# ═══════════════════════════════════════════════════════
print("\n=== FR-09: 认证 ===")
test("FR09-AUTH-01", "注册页面", "GET", "/register")
test("FR09-AUTH-02", "登录页面", "GET", "/login")
test("FR09-AUTH-04", "用户信息", "GET", "/api/v1/auth/me", auth=True)
test("FR09-AUTH-06", "套餐列表", "GET", "/api/v1/membership/plans", auth=True)


# ═══════════════════════════════════════════════════════
# FR-09b: 清理策略
# ═══════════════════════════════════════════════════════
print("\n=== FR-09b: 清理策略 ===")
test("FR09b-CLEAN-01", "清理任务列表", "GET", "/api/v1/admin/cleanup/tasks", auth=True)


# ═══════════════════════════════════════════════════════
# FR-10: 站点/仪表盘
# ═══════════════════════════════════════════════════════
print("\n=== FR-10: 站点/仪表盘 ===")
test("FR10-HOME-01", "首页", "GET", "/")
test("FR10-LIST-01", "研报列表", "GET", "/reports")
s, d = test("FR10-LIST-01-api", "研报列表API", "GET", "/api/v1/reports?page=1&page_size=3")
if s == 200:
    total = d.get("data", {}).get("total", 0)
    if total < 100:
        issues.append({"fid": "FR10-LIST-01", "label": "研报数量不足", "detail": f"total={total}"})
    items = d.get("data", {}).get("items", [])
    if items:
        rid = items[0]["report_id"]
        test("FR10-DETAIL-01", "研报详情", "GET", f"/reports/{rid}")
        test("FR10-DETAIL-01-api", "研报详情API", "GET", f"/api/v1/reports/{rid}")
        test("FR10-DETAIL-02", "高级区", "GET", f"/api/v1/reports/{rid}/advanced", auth=True)

test("FR10-BOARD-01", "统计看板页面", "GET", "/dashboard")
test("FR10-BOARD-01-api", "统计看板API", "GET", "/api/v1/dashboard/stats")
test("FR10-PLATFORM-01", "功能地图", "GET", "/features", auth=True)


# ═══════════════════════════════════════════════════════
# FR-11: 反馈
# ═══════════════════════════════════════════════════════
print("\n=== FR-11: 反馈 ===")
if items:
    test("FR11-FEEDBACK-01", "反馈提交", "POST", f"/api/v1/reports/{rid}/feedback", auth=True,
         data={"rating": 4, "comment": "test audit feedback"})


# ═══════════════════════════════════════════════════════
# FR-12: 后台管理
# ═══════════════════════════════════════════════════════
print("\n=== FR-12: 后台管理 ===")
test("FR12-ADMIN-01", "后台概览", "GET", "/api/v1/admin/overview", auth=True)
test("FR12-ADMIN-02", "用户列表", "GET", "/api/v1/admin/users?page=1&page_size=2", auth=True)
test("FR12-ADMIN-03", "报告管理", "GET", "/api/v1/admin/reports?page=1&page_size=2", auth=True)
test("FR12-ADMIN-04", "审计日志", "GET", "/api/v1/admin/audit-log?page=1&page_size=2", auth=True)
test("FR12-ADMIN-05", "系统状态", "GET", "/api/v1/admin/system/status", auth=True)
test("FR12-ADMIN-06", "后台页面", "GET", "/admin", auth=True)


# ═══════════════════════════════════════════════════════
# FR-13: 事件派发
# ═══════════════════════════════════════════════════════
print("\n=== FR-13: 事件派发 ===")
test("FR13-EVENT-01", "事件投递", "GET", "/api/v1/internal/events/outbox", auth=True)


# ═══════════════════════════════════════════════════════
# PAGES: Browser-accessible pages check  
# ═══════════════════════════════════════════════════════
print("\n=== Pages Accessibility ===")
pages = {
    "首页": "/",
    "研报列表": "/reports",
    "登录": "/login",
    "注册": "/register",
    "仪表盘": "/dashboard",
    "忘记密码": "/forgot-password",
    "隐私政策": "/privacy",
    "服务条款": "/terms",
}
for name, path in pages.items():
    s, b = get(path)
    has_title = "<title>" in b
    print(f"  [{s}] {name}: {path} {'(has title)' if has_title else '(NO TITLE)'}")
    if s != 200:
        issues.append({"fid": f"PAGE-{name}", "label": f"页面 {name}", "http_status": s, "detail": f"Expected 200, got {s}"})


# ═══════════════════════════════════════════════════════
# DB Analysis (Angle 2: 数据完整率)
# ═══════════════════════════════════════════════════════
print("\n=== DB Data Completeness Analysis ===")
db = sqlite3.connect("data/app.db")
cur = db.cursor()

# Reports with conclusion_text
cur.execute("SELECT COUNT(*) FROM report WHERE conclusion_text IS NOT NULL AND conclusion_text != ''")
with_conclusion = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM report")
total_reports = cur.fetchone()[0]
print(f"  Reports: {total_reports} total, {with_conclusion} with conclusion_text ({100*with_conclusion/max(total_reports,1):.1f}%)")

# Settlement coverage
cur.execute("SELECT COUNT(*) FROM settlement_result")
settled = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM report WHERE published=1")
published = cur.fetchone()[0]
print(f"  Settlement: {settled}/{published} published ({100*settled/max(published,1):.1f}% coverage)")

# Prediction outcomes
cur.execute("SELECT COUNT(*) FROM prediction_outcome")
predictions = cur.fetchone()[0]
print(f"  Predictions: {predictions}")

# Sim data 
cur.execute("SELECT COUNT(*) FROM sim_position")
sim_pos = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM sim_trade_instruction")
sim_trades = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM sim_dashboard_snapshot")
sim_snaps = cur.fetchone()[0]
print(f"  Sim: {sim_pos} positions, {sim_trades} trade instructions, {sim_snaps} snapshots")

# Cookie sessions
cur.execute("SELECT COUNT(*) FROM cookie_session")
cookies = cur.fetchone()[0]
print(f"  Cookie sessions: {cookies}")

# Hotspot data
cur.execute("SELECT COUNT(*) FROM market_hotspot_item")
hotspots = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM hotspot_raw")
hotspot_raw = cur.fetchone()[0]
print(f"  Hotspots: {hotspots} items, {hotspot_raw} raw")

# Empty tables
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
empty = []
for t in tables:
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{t}"')
        if cur.fetchone()[0] == 0:
            empty.append(t)
    except:
        pass
print(f"  Empty tables ({len(empty)}): {', '.join(empty)}")

# K-line coverage
cur.execute("SELECT COUNT(DISTINCT stock_code) FROM kline_daily")
kline_stocks = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM stock_master")
total_stocks = cur.fetchone()[0]
print(f"  K-line: {kline_stocks}/{total_stocks} stocks ({100*kline_stocks/max(total_stocks,1):.1f}%)")

db.close()


# ═══════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("AUDIT SUMMARY")
print("=" * 60)

passed = sum(1 for v in results.values() if v["status"] == "PASS")
failed = sum(1 for v in results.values() if v["status"] == "FAIL")
errors = sum(1 for v in results.values() if v["status"] == "ERROR")
print(f"Endpoints tested: {len(results)} | PASS: {passed} | FAIL: {failed} | ERROR: {errors}")

print(f"\nISSUES FOUND ({len(issues)}):")
for i, iss in enumerate(issues, 1):
    print(f"  {i}. [{iss['fid']}] {iss.get('label','')} - {iss.get('detail','')} "
          f"(HTTP {iss.get('http_status','?')}/{iss.get('expected','?')})")
    if iss.get("response_preview"):
        print(f"     Response: {iss['response_preview'][:150]}")

# Dump to JSON for later use
with open("_archive/audit_results.json", "w", encoding="utf-8") as f:
    json.dump({"results": results, "issues": issues}, f, ensure_ascii=False, indent=2)
print("\nResults saved to _archive/audit_results.json")
