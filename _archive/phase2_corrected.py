"""Phase 2 Corrected: Full endpoint re-test with CORRECT route paths from route dump."""
import http.client
import json
import sqlite3

BASE = ("127.0.0.1", 8010)

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
AUTH = {"Authorization": f"Bearer {TOKEN}"}

def req(method, path, auth=False, data=None):
    conn = http.client.HTTPConnection(*BASE, timeout=15)
    h = dict(AUTH) if auth else {}
    if data is not None:
        h["Content-Type"] = "application/json"
        conn.request(method, path, body=json.dumps(data), headers=h)
    else:
        conn.request(method, path, headers=h)
    resp = conn.getresponse()
    body = resp.read().decode()
    conn.close()
    try:
        return resp.status, json.loads(body)
    except:
        return resp.status, {"_raw": body[:300]}

issues = []
passed = 0
failed = 0

def check(fid, label, method, path, auth=False, data=None, expect=200, verify=None):
    global passed, failed
    s, d = req(method, path, auth, data)
    ok = s == expect
    detail = ""
    if ok and verify:
        try:
            result = verify(s, d)
            if result:
                ok = False
                detail = result
        except Exception as e:
            ok = False
            detail = f"verify_err: {e}"
    if ok:
        passed += 1
        tag = "OK"
    else:
        failed += 1
        tag = "FAIL"
        detail = detail or f"got {s}, expected {expect}"
        issues.append({"fid": fid, "label": label, "detail": detail, "http": s, "expect": expect,
                       "preview": json.dumps(d, ensure_ascii=False)[:150]})
    print(f"  [{tag}] {fid}: {label} [{method} {path}] [{s}] {detail}")
    return s, d


# ═════════════════════════════════════════════════════════════
print("=" * 70)
print("CORRECTED FULL ENDPOINT AUDIT")
print("=" * 70)

# ── FR-00: Data Authenticity ──
print("\n=== FR-00: 数据真实性保障 ===")
s, d = check("FR00-01", "研报列表可读", "GET", "/api/v1/reports?page=1&page_size=2")
items = d.get("data", {}).get("items", []) if s == 200 else []
rid = items[0]["report_id"] if items else None
if rid:
    check("FR00-02", "研报详情含证据", "GET", f"/api/v1/reports/{rid}",
          verify=lambda s, d: "no conclusion" if not d.get("data", {}).get("conclusion_text") else None)
    check("FR00-03", "高级区(含血缘)", "GET", f"/api/v1/reports/{rid}/advanced", auth=True)

# ── FR-01: Stock Pool ──
print("\n=== FR-01: 股票池 ===")
check("FR01-01", "池刷新(admin)", "POST", "/api/v1/admin/pool/refresh", auth=True,
      data={"dry_run": True})
check("FR01-02", "池股票列表", "GET", "/api/v1/pool/stocks")

# ── FR-02: Scheduler ──
print("\n=== FR-02: 调度器 ===")
check("FR02-01", "调度器状态(admin)", "GET", "/api/v1/admin/scheduler/status", auth=True)
check("FR02-02", "DAG 重触", "POST", "/api/v1/admin/dag/retrigger", auth=True,
      data={"dag_name": "test", "dry_run": True}, expect=200)

# ── FR-03: Cookie Management ──
print("\n=== FR-03: Cookie 管理 ===")
check("FR03-01", "Cookie 列表", "GET", "/api/v1/admin/cookie-sessions", auth=True)
check("FR03-02", "Cookie 健康", "GET", "/api/v1/admin/cookie-session/health", auth=True)

# ── FR-04: Hotspot Collection ──
print("\n=== FR-04: 热点采集 ===")
check("FR04-01", "热点健康", "GET", "/api/v1/internal/hotspot/health", auth=True)
check("FR04-02", "首页(含热点)", "GET", "/api/v1/home")
check("FR04-03", "热门股票", "GET", "/api/v1/market/hot-stocks")

# ── FR-05: Market State ──
print("\n=== FR-05: 市场状态 ===")
check("FR05-01", "市场状态", "GET", "/api/v1/market/state",
      verify=lambda s, d: "DEGRADED" if "degraded" in str(d.get("data", {}).get("state_reason", "")).lower() else None)

# ── FR-06: LLM Generation ──
print("\n=== FR-06: LLM 生成 ===")
check("FR06-01", "LLM 健康", "GET", "/api/v1/internal/llm/health", auth=True)
check("FR06-02", "LLM 版本", "GET", "/api/v1/internal/llm/version", auth=True)
check("FR06-03", "运行时门禁", "GET", "/api/v1/internal/runtime/gates", auth=True)
check("FR06-04", "数据源降级状态", "GET", "/api/v1/internal/source/fallback-status", auth=True)
check("FR06-05", "指标汇总", "GET", "/api/v1/internal/metrics/summary", auth=True)

# ── FR-07: Settlement ──
print("\n=== FR-07: 结算/绩效 ===")
check("FR07-01", "看板统计(含结算)", "GET", "/api/v1/dashboard/stats",
      verify=lambda s, d: f"settled_only_{d['data'].get('total_settled',0)}" if d.get("data", {}).get("total_settled", 0) < 20 else None)
check("FR07-02", "预测统计", "GET", "/api/v1/predictions/stats")

# ── FR-08: Sim Positions ──
print("\n=== FR-08: 模拟仓位 ===")
check("FR08-01", "模拟看板API", "GET", "/api/v1/portfolio/sim-dashboard", auth=True)
check("FR08-02", "持仓列表", "GET", "/api/v1/sim/positions", auth=True)
check("FR08-03", "账户摘要", "GET", "/api/v1/sim/account/summary", auth=True)
check("FR08-04", "账户快照", "GET", "/api/v1/sim/account/snapshots", auth=True)

# ── FR-09: Auth ──
print("\n=== FR-09: 认证 ===")
check("FR09-01", "用户信息", "GET", "/auth/me", auth=True)
check("FR09-02", "平台套餐", "GET", "/api/v1/platform/plans")
check("FR09-03", "会员状态", "GET", "/api/v1/membership/subscription/status", auth=True)

# ── FR-09b: Cleanup ──
print("\n=== FR-09b: 清理策略 ===")
# cleanup routes: /api/v1/internal/reports/clear and /api/v1/internal/stats/clear
check("FR09b-01", "清理报告(dry)", "POST", "/api/v1/internal/reports/clear", auth=True,
      data={"dry_run": True, "confirm": False})
check("FR09b-02", "清理统计(dry)", "POST", "/api/v1/internal/stats/clear", auth=True,
      data={"dry_run": True, "confirm": False})

# ── FR-10: Site / Dashboard ──
print("\n=== FR-10: 站点/仪表盘 ===")
check("FR10-01", "首页HTML", "GET", "/")
check("FR10-02", "研报列表HTML", "GET", "/reports")
check("FR10-03", "研报列表API", "GET", "/api/v1/reports?page=1&page_size=3")
if rid:
    check("FR10-04", "研报详情HTML", "GET", f"/reports/{rid}")
    check("FR10-05", "研报详情API", "GET", f"/api/v1/reports/{rid}")
check("FR10-06", "统计看板HTML", "GET", "/dashboard")
check("FR10-07", "统计看板API", "GET", "/api/v1/dashboard/stats")
check("FR10-08", "功能地图HTML", "GET", "/features", auth=True)
check("FR10-09", "功能目录API", "GET", "/api/v1/features/catalog", auth=True)
check("FR10-10", "平台配置", "GET", "/api/v1/platform/config")
check("FR10-11", "平台汇总", "GET", "/api/v1/platform/summary")
check("FR10-12", "首页API", "GET", "/api/v1/home")

# ── FR-11: Feedback ──
print("\n=== FR-11: 反馈 ===")
if rid:
    check("FR11-01", "反馈提交(路径)", "POST", f"/api/v1/reports/{rid}/feedback", auth=True,
          data={"rating": 4, "comment": "audit test feedback"})
    check("FR11-02", "反馈提交(全局)", "POST", "/api/v1/report-feedback", auth=True,
          data={"report_id": rid, "rating": 5, "comment": "global feedback test"})

# ── FR-12: Admin ──
print("\n=== FR-12: 后台管理 ===")
check("FR12-01", "后台概览", "GET", "/api/v1/admin/overview", auth=True)
check("FR12-02", "用户列表", "GET", "/api/v1/admin/users?page=1&page_size=2", auth=True)
check("FR12-03", "报告管理", "GET", "/api/v1/admin/reports?page=1&page_size=2", auth=True)
check("FR12-04", "系统状态", "GET", "/api/v1/admin/system-status", auth=True)
check("FR12-05", "审计上下文", "GET", "/api/v1/internal/audit/context", auth=True)
check("FR12-06", "后台页面", "GET", "/admin", auth=True)

# ── FR-13: Event Dispatch ──
print("\n=== FR-13: 事件派发 ===")
check("FR13-01", "自主循环状态", "GET", "/api/v1/internal/autonomy/loop", auth=True)
check("FR13-02", "自主循环state", "GET", "/api/v1/internal/autonomy/loop/state", auth=True)

# ── Additional pages ──
print("\n=== Additional Pages ===")
for name, path in [
    ("模拟看板", "/portfolio/sim-dashboard"),
    ("关注列表", "/watchlist"),
    ("个人中心", "/profile"),
    ("订阅", "/subscribe"),
    ("忘记密码", "/forgot-password"),
    ("重置密码", "/reset-password"),
    ("隐私政策", "/privacy"),
    ("服务条款", "/terms"),
    ("Demo", "/demo"),
]:
    s, d = req("GET", path, auth=True)
    ok = s in (200, 302)  # 302 redirect is OK for auth-protected pages
    tag = "OK" if ok else "FAIL"
    print(f"  [{tag}] PAGE-{name}: [{s}] {path}")
    if not ok:
        issues.append({"fid": f"PAGE-{name}", "label": f"页面 {name}", "detail": f"HTTP {s}", "http": s})

# ═════════════════════════════════════════════════════════════
# SUMMARY
# ═════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("CORRECTED AUDIT SUMMARY")
print("=" * 70)
print(f"Tested: {passed + failed} | PASS: {passed} | FAIL: {failed}")
print(f"\nISSUES ({len(issues)}):")
for i, iss in enumerate(issues, 1):
    print(f"  {i}. [{iss['fid']}] {iss.get('label','')} => {iss.get('detail','')} (HTTP {iss.get('http','?')})")
    if iss.get("preview"):
        print(f"     {iss['preview']}")

with open("_archive/audit_corrected.json", "w", encoding="utf-8") as f:
    json.dump({"passed": passed, "failed": failed, "issues": issues}, f, ensure_ascii=False, indent=2)
print("\nSaved to _archive/audit_corrected.json")
