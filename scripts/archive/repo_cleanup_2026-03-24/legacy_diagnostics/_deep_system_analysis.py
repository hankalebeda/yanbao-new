"""Deep system analysis script - analyze all pages and APIs."""
import requests
import json
import sys

BASE = "http://127.0.0.1:8000"

def safe_get(url, **kwargs):
    try:
        r = requests.get(url, timeout=10, **kwargs)
        return r
    except Exception as e:
        return None

def safe_post(url, **kwargs):
    try:
        r = requests.post(url, timeout=10, **kwargs)
        return r
    except Exception as e:
        return None

def check_api(method, path, desc, headers=None, json_data=None):
    url = f"{BASE}{path}"
    try:
        if method == "GET":
            r = requests.get(url, timeout=10, headers=headers)
        else:
            r = requests.post(url, timeout=10, headers=headers, json=json_data or {})
        
        ct = r.headers.get("content-type", "")
        is_json = "json" in ct
        
        result = {
            "path": path,
            "desc": desc,
            "status": r.status_code,
            "content_type": ct,
            "is_json": is_json,
        }
        
        if is_json:
            try:
                data = r.json()
                result["success"] = data.get("success")
                result["error"] = data.get("error")
                if "data" in data:
                    d = data["data"]
                    if isinstance(d, dict):
                        result["data_keys"] = list(d.keys())[:20]
                    elif isinstance(d, list):
                        result["data_count"] = len(d)
                    else:
                        result["data_type"] = type(d).__name__
            except:
                pass
        else:
            result["body_len"] = len(r.content)
            # Check if it's HTML
            if "html" in ct:
                text = r.text[:200]
                result["html_preview"] = text
        
        return result
    except Exception as e:
        return {"path": path, "desc": desc, "error": str(e)}

def check_page(path, desc):
    url = f"{BASE}{path}"
    try:
        r = requests.get(url, timeout=10, allow_redirects=False)
        ct = r.headers.get("content-type", "")
        result = {
            "path": path,
            "desc": desc,
            "status": r.status_code,
            "content_type": ct,
            "body_len": len(r.content),
        }
        if r.status_code in (301, 302, 307, 308):
            result["redirect_to"] = r.headers.get("location", "")
        if "html" in ct:
            text = r.text
            # Check for error indicators
            if "500" in text and "Internal Server Error" in text:
                result["has_500_error"] = True
            if "404" in text and ("Not Found" in text or "not found" in text):
                result["has_404_error"] = True
            # Check title
            import re
            title_match = re.search(r"<title>(.*?)</title>", text, re.DOTALL)
            if title_match:
                result["title"] = title_match.group(1).strip()[:100]
            # Check for broken resources
            css_refs = re.findall(r'href="([^"]*\.css[^"]*)"', text)
            js_refs = re.findall(r'src="([^"]*\.js[^"]*)"', text)
            result["css_refs"] = css_refs[:10]
            result["js_refs"] = js_refs[:10]
        return result
    except Exception as e:
        return {"path": path, "desc": desc, "error": str(e)}

print("=" * 80)
print("DEEP SYSTEM ANALYSIS - A股研报平台")
print("=" * 80)

# ========== 1. Health Check ==========
print("\n### 1. 系统健康检查")
r = safe_get(f"{BASE}/health")
if r:
    d = r.json()
    print(json.dumps(d, indent=2, ensure_ascii=False))

# ========== 2. All Web Pages ==========
print("\n### 2. 所有网页路由检查")
pages = [
    ("/", "首页"),
    ("/reports", "研报列表"),
    ("/dashboard", "统计看板"),
    ("/login", "登录"),
    ("/register", "注册"),
    ("/forgot-password", "忘记密码"),
    ("/reset-password", "重置密码"),
    ("/subscribe", "订阅"),
    ("/terms", "服务条款"),
    ("/privacy", "隐私政策"),
    ("/profile", "个人中心"),
    ("/admin", "管理后台"),
    ("/features", "功能治理"),
    ("/portfolio/sim-dashboard", "模拟收益看板"),
    ("/logout", "登出"),
    # Error pages
    ("/nonexistent-page-404", "404测试"),
    # Report pages
    ("/report/601838.SH", "研报详情-成都银行"),
    ("/report/INVALID_CODE", "研报详情-无效代码"),
    ("/report/601838.SH/status", "研报状态查询"),
    ("/demo/report/601838.SH", "兼容路由"),
]

for path, desc in pages:
    result = check_page(path, desc)
    status = result.get("status", "ERROR")
    title = result.get("title", "N/A")
    redirect = result.get("redirect_to", "")
    body_len = result.get("body_len", 0)
    issues = []
    if result.get("has_500_error"):
        issues.append("500内部错误")
    if result.get("has_404_error"):
        issues.append("404页面")
    if result.get("error"):
        issues.append(f"异常: {result['error']}")
    
    issue_str = f"  ⚠️ {', '.join(issues)}" if issues else ""
    redirect_str = f" -> {redirect}" if redirect else ""
    print(f"  [{status}] {path:40s} {desc:15s} title={title:40s} size={body_len:6d}{redirect_str}{issue_str}")

# ========== 3. API Endpoints ==========
print("\n### 3. API 端点检查")
apis = [
    ("GET", "/api/v1/home", "首页数据"),
    ("GET", "/api/v1/pool/stocks", "股票池"),
    ("GET", "/api/v1/reports", "研报列表"),
    ("GET", "/api/v1/reports?page=1&page_size=5", "研报列表分页"),
    ("GET", "/api/v1/dashboard/stats", "看板默认"),
    ("GET", "/api/v1/dashboard/stats?window=7", "看板7日"),
    ("GET", "/api/v1/dashboard/stats?window=14", "看板14日"),
    ("GET", "/api/v1/dashboard/stats?window=30", "看板30日"),
    ("GET", "/api/v1/dashboard/stats?window=60", "看板60日"),
    ("GET", "/api/v1/predictions/stats", "预测统计"),
    ("GET", "/api/v1/platform/config", "平台配置"),
    ("GET", "/api/v1/platform/plans", "会员计划"),
    ("GET", "/api/v1/membership/subscription/status", "会员状态"),
    ("GET", "/api/v1/market/hot-stocks", "热门股票"),
    ("GET", "/api/v1/market/state", "市场状态"),
    ("GET", "/api/v1/auth/me", "当前用户"),
    ("GET", "/api/v1/auth/oauth/providers", "OAuth提供商"),
    ("GET", "/api/v1/features/catalog", "功能目录"),
    ("GET", "/api/v1/sim/positions", "模拟持仓"),
    ("GET", "/api/v1/sim/account/summary", "账户汇总"),
    ("GET", "/api/v1/platform/summary", "平台汇总"),
    ("GET", "/api/v1/portfolio/sim-dashboard", "模拟看板API"),
    ("GET", "/api/v1/admin/overview", "管理概览"),
    ("GET", "/api/v1/admin/scheduler/status", "调度状态"),
    ("GET", "/api/v1/admin/users", "用户列表"),
    ("GET", "/api/v1/admin/reports", "报告管理"),
]

for method, path, desc in apis:
    result = check_api(method, path, desc)
    status = result.get("status", "ERROR")
    success = result.get("success", "N/A")
    error = result.get("error", "")
    data_keys = result.get("data_keys", [])
    data_count = result.get("data_count", "")
    
    extra = ""
    if data_keys:
        extra = f" keys={data_keys}"
    if data_count != "":
        extra = f" items={data_count}"
    if error:
        extra = f" ⚠️ error={error}"
    
    print(f"  [{status}] {method:4s} {path:50s} {desc:15s} success={str(success):5s}{extra}")

# ========== 4. Dashboard Deep Analysis ==========
print("\n### 4. 统计看板深度分析")
for window in [1, 7, 14, 30, 60]:
    r = safe_get(f"{BASE}/api/v1/dashboard/stats?window={window}")
    if r and r.status_code == 200:
        d = r.json().get("data", {})
        print(f"\n  --- {window}日窗口 ---")
        print(f"  date_range: {d.get('date_range')}")
        print(f"  data_status: {d.get('data_status')}")
        print(f"  status_reason: {d.get('status_reason')}")
        print(f"  display_hint: {d.get('display_hint')}")
        print(f"  total_reports: {d.get('total_reports')}")
        print(f"  total_settled: {d.get('total_settled')}")
        print(f"  win_rate: {d.get('win_rate')}")
        print(f"  profit_loss_ratio: {d.get('profit_loss_ratio')}")
        strats = d.get("by_strategy", [])
        for s in strats:
            print(f"    策略{s.get('strategy')}: sample={s.get('sample_size')}, coverage={s.get('coverage_pct')}%, win_rate={s.get('win_rate')}, display_hint={s.get('display_hint')}")
        baseline = d.get("baseline_comparison", {})
        if baseline:
            print(f"  baseline: random_win_rate={baseline.get('random_win_rate')}, ma_cross_win_rate={baseline.get('ma_cross_win_rate')}")

# ========== 5. Home API Deep Analysis ==========
print("\n### 5. 首页API深度分析")
r = safe_get(f"{BASE}/api/v1/home")
if r and r.status_code == 200:
    d = r.json().get("data", {})
    print(f"  trade_date: {d.get('trade_date')}")
    print(f"  pool_size: {d.get('pool_size')}")
    print(f"  today_report_count: {d.get('today_report_count')}")
    print(f"  data_status: {d.get('data_status')}")
    print(f"  status_reason: {d.get('status_reason')}")
    print(f"  market_state: {d.get('market_state')}")
    print(f"  hot_stocks: {len(d.get('hot_stocks', []))} items")
    print(f"  latest_reports: {len(d.get('latest_reports', []))} items")
    stats = d.get("stats_summary", {})
    print(f"  stats_summary.win_rate: {stats.get('win_rate')}")
    print(f"  stats_summary.profit_loss_ratio: {stats.get('profit_loss_ratio')}")
    print(f"  stats_summary.total_settled: {stats.get('total_settled')}")
    print(f"  stats_summary.total_reports: {stats.get('total_reports')}")
    print(f"  stats_summary.display_hint: {stats.get('display_hint')}")
    
    # Check if reports have today's date
    for rep in d.get("latest_reports", [])[:3]:
        print(f"  latest_report: {rep.get('stock_code')} trade_date={rep.get('trade_date')} recommendation={rep.get('recommendation')}")

# ========== 6. Report Detail Analysis ==========
print("\n### 6. 研报详情API分析")
# Get a report from list first
r = safe_get(f"{BASE}/api/v1/reports?page=1&page_size=3")
if r and r.status_code == 200:
    reports = r.json().get("data", {})
    items = reports.get("items", [])
    for item in items[:2]:
        rid = item.get("report_id") or item.get("id")
        print(f"\n  报告 {rid}:")
        print(f"    stock_code: {item.get('stock_code')}")
        print(f"    recommendation: {item.get('recommendation')}")
        print(f"    confidence: {item.get('confidence')}")
        print(f"    strategy_type: {item.get('strategy_type')}")
        print(f"    trade_date: {item.get('trade_date')}")
        
        # Get full detail
        if rid:
            r2 = safe_get(f"{BASE}/api/v1/reports/{rid}")
            if r2 and r2.status_code == 200:
                det = r2.json().get("data", {})
                print(f"    detail keys: {list(det.keys())[:15]}")
                print(f"    has_analysis_steps: {'analysis_steps' in det}")
                print(f"    has_instruction_card: {'instruction_card' in det}")
                print(f"    has_evidence_items: {'evidence_items' in det or 'report_data_usage' in det}")
                print(f"    data_quality: {det.get('data_quality')}")

# ========== 7. Market State ==========
print("\n### 7. 市场状态分析")
r = safe_get(f"{BASE}/api/v1/market/state")
if r and r.status_code == 200:
    d = r.json().get("data", {})
    print(json.dumps(d, indent=2, ensure_ascii=False))

# ========== 8. Hot Stocks ==========
print("\n### 8. 热门股票")
r = safe_get(f"{BASE}/api/v1/market/hot-stocks")
if r and r.status_code == 200:
    d = r.json().get("data", {})
    if isinstance(d, list):
        print(f"  热门股票数: {len(d)}")
        for s in d[:5]:
            print(f"    {s}")
    elif isinstance(d, dict):
        print(f"  keys: {list(d.keys())}")
        items = d.get("items", d.get("hot_stocks", []))
        print(f"  items count: {len(items)}")
        for s in items[:5]:
            if isinstance(s, dict):
                print(f"    {s.get('stock_code')} {s.get('stock_name')} source={s.get('source')}")

# ========== 9. Static Resources ==========
print("\n### 9. 静态资源检查")
static_paths = [
    "/static/demo.css",
    "/static/css/style.css",
    "/static/js/main.js",
    "/favicon.ico",
]
for path in static_paths:
    r = safe_get(f"{BASE}{path}")
    if r:
        print(f"  [{r.status_code}] {path:40s} size={len(r.content):6d} type={r.headers.get('content-type','')}")
    else:
        print(f"  [ERROR] {path}")

# ========== 10. Error Pages ==========
print("\n### 10. 错误页面检查")
error_tests = [
    ("GET", "/api/v1/reports/nonexistent-id", "不存在的报告ID"),
    ("GET", "/report/XXXXXX", "无效股票代码"),
    ("POST", "/api/v1/auth/login", "空登录请求"),
    ("POST", "/api/v1/auth/register", "空注册请求"),
    ("GET", "/api/v1/admin/overview", "未认证管理接口"),
]
for method, path, desc in error_tests:
    result = check_api(method, path, desc, json_data={} if method == "POST" else None)
    print(f"  [{result.get('status')}] {method} {path:50s} {desc:20s} success={result.get('success')} error={result.get('error','')}")

# ========== 11. Platform Config ==========
print("\n### 11. 平台配置")
r = safe_get(f"{BASE}/api/v1/platform/config")
if r and r.status_code == 200:
    d = r.json().get("data", {})
    print(json.dumps(d, indent=2, ensure_ascii=False))

# ========== 12. Sim Dashboard API ==========
print("\n### 12. 模拟收益API")
r = safe_get(f"{BASE}/api/v1/portfolio/sim-dashboard")
if r:
    print(f"  status: {r.status_code}")
    if r.status_code == 200:
        d = r.json().get("data", {})
        print(f"  keys: {list(d.keys())[:15]}")
    else:
        print(f"  body: {r.text[:300]}")

print("\n" + "=" * 80)
print("分析完成")
print("=" * 80)
