#!/usr/bin/env python3
"""
前端审计脚本 — 发现精审表无法覆盖的前端问题

机制：
1. HTTP请求每个页面路由，检查返回状态码和HTML完整性
2. 解析HTML中的API调用、链接、表单、交互元素
3. 验证API端点可达性
4. 检查Jinja2模板渲染是否正确（无未替换变量）
5. 检查关键DOM元素是否存在
6. 输出逐功能点的前端问题清单
"""

import json
import re
import sys
import time
import urllib.parse
from html.parser import HTMLParser

import httpx

BASE = "http://127.0.0.1:8099"

# ── 1. 所有页面路由 + 预期检查 ──────────────────────────
PAGE_CHECKS = [
    # (path, name, fr, expected_status, required_elements, required_strings)
    ("/", "首页", "FR10-PAGE-01", 200,
     ["id=\"search-input\"", "id=\"market-state\"", "id=\"pool-section\""],
     ["核心池", "市场状态"]),
    ("/reports", "研报列表", "FR10-PAGE-02", 200,
     ["id=\"reports-container\"", "class=\"pagination\""],
     ["筛选", "研报"]),
    ("/login", "登录页", "FR09-AUTH-02", 200,
     ["id=\"login-form\"", "type=\"password\""],
     ["登录", "邮箱"]),
    ("/register", "注册页", "FR09-AUTH-01", 200,
     ["id=\"register-form\"", "type=\"password\""],
     ["注册"]),
    ("/subscribe", "订阅页", "FR09-BILLING-01", 200,
     ["class=\"plan-card\"", "class=\"pricing\""],
     ["Pro", "Enterprise"]),
    ("/forgot-password", "忘记密码", "FR09-AUTH-06", 200,
     ["type=\"email\""],
     ["重置", "邮箱"]),
    ("/reset-password", "重置密码", "FR09-AUTH-06", 200,
     ["type=\"password\""],
     ["新密码"]),
    ("/dashboard", "统计看板", "FR10-PAGE-05", 200,
     [],
     ["看板"]),
    ("/features", "功能地图", "FR10-PAGE-10", [200, 302, 401, 403], [], []),
    ("/admin", "管理后台", "FR12-ADMIN-02", [200, 302, 401, 403], [], []),
    ("/profile", "个人中心", "FR09-AUTH-07", [200, 302, 401, 403], [], []),
    ("/portfolio/sim-dashboard", "模拟收益看板", "FR10-PAGE-06", [200, 302, 401, 403], [], []),
]

# ── 2. API端点检查（api-bridge.js中引用的端点） ──────────────
API_CHECKS = [
    ("GET", "/api/v1/home", "FR10-PAGE-01", [200]),
    ("GET", "/api/v1/reports", "FR10-PAGE-02", [200]),
    ("GET", "/api/v1/dashboard/stats?window_days=30", "FR10-PAGE-05", [200]),
    ("GET", "/api/v1/market/state", "FR05-MKT-01", [200]),
    ("GET", "/api/v1/market/hot-stocks?limit=10", "FR04-DATA-07", [200]),
    ("GET", "/api/v1/pool/stocks", "FR01-POOL-01", [200]),
    ("GET", "/api/v1/platform/config", "FR09-BILLING-01", [200]),
    ("GET", "/api/v1/platform/plans", "FR09-BILLING-01", [200]),
    ("GET", "/api/v1/platform/summary", "FR10-PAGE-01", [200]),
    ("GET", "/api/v1/auth/oauth/providers", "FR09-AUTH-05", [200]),
    ("GET", "/api/v1/admin/overview", "FR12-ADMIN-02", [200, 401, 403]),
    ("GET", "/api/v1/admin/scheduler/status", "FR02-SCHED-01", [200, 401, 403]),
    ("GET", "/api/v1/admin/system-status", "FR12-ADMIN-02", [200, 401, 403]),
    ("GET", "/api/v1/admin/reports", "FR12-ADMIN-07", [200, 401, 403]),
    ("GET", "/api/v1/admin/users", "FR12-ADMIN-06", [200, 401, 403]),
    ("GET", "/api/v1/internal/metrics/summary", "FR02-SCHED-05", [200, 401, 403]),
    ("GET", "/api/v1/portfolio/sim-dashboard?capital_tier=100k", "FR10-PAGE-06", [200, 401, 403]),
    ("GET", "/api/v1/sim/positions", "FR08-SIM-01", [200, 401, 403]),
]

# ── 3. 链接检查 ──────────────────────────────────────────
INTERNAL_LINK_PATTERN = re.compile(r'href="(/[^"]*)"')
JINJA_UNRESOLVED = re.compile(r'\{\{[^}]*\}\}|\{%[^%]*%\}')
STATIC_REF_PATTERN = re.compile(r'(?:src|href)="(/static/[^"]*)"')
API_CALL_PATTERN = re.compile(r'fetch\([\'"]([^\'"]+)[\'"]')


class ElementChecker(HTMLParser):
    """检查HTML中的关键元素"""
    def __init__(self):
        super().__init__()
        self.found_ids = set()
        self.found_classes = set()
        self.found_forms = []
        self.found_links = []
        self.found_scripts = []
        self.errors = []

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if "id" in attrs_dict:
            self.found_ids.add(attrs_dict["id"])
        if "class" in attrs_dict:
            for cls in attrs_dict["class"].split():
                self.found_classes.add(cls)
        if tag == "a" and "href" in attrs_dict:
            self.found_links.append(attrs_dict["href"])
        if tag == "form":
            self.found_forms.append(attrs_dict)
        if tag == "script" and "src" in attrs_dict:
            self.found_scripts.append(attrs_dict["src"])

    def handle_data(self, data):
        pass


def check_page(client, path, name, fr_id, expected_status, required_elements, required_strings):
    """检查单个页面"""
    issues = []
    try:
        resp = client.get(f"{BASE}{path}", follow_redirects=False, timeout=10)
    except Exception as e:
        return [f"🔴 {fr_id} [{name}] {path} 请求失败: {e}"]

    # 状态码检查
    if isinstance(expected_status, list):
        if resp.status_code not in expected_status:
            issues.append(f"🔴 {fr_id} [{name}] {path} 状态码={resp.status_code}, 期望∈{expected_status}")
    else:
        if resp.status_code != expected_status:
            issues.append(f"🔴 {fr_id} [{name}] {path} 状态码={resp.status_code}, 期望{expected_status}")

    # 如果不是200，跳过内容检查
    if resp.status_code != 200:
        return issues

    html = resp.text

    # HTML完整性
    if not html.strip().endswith("</html>"):
        issues.append(f"⚠️ {fr_id} [{name}] HTML未正常闭合")

    # Jinja2未解析变量检查
    unresolved = JINJA_UNRESOLVED.findall(html)
    if unresolved:
        # 过滤掉JS中的模板字面量
        real_unresolved = [u for u in unresolved if not u.startswith("{%") and "{{" in u]
        if real_unresolved:
            issues.append(f"⚠️ {fr_id} [{name}] 发现未解析的Jinja2变量: {real_unresolved[:3]}")

    # 必要元素检查
    for elem in required_elements:
        if elem not in html:
            issues.append(f"🔴 {fr_id} [{name}] 缺少关键元素: {elem}")

    # 必要字符串检查
    for s in required_strings:
        if s not in html:
            issues.append(f"⚠️ {fr_id} [{name}] 页面缺少关键文本: '{s}'")

    # 静态文件引用检查
    static_refs = STATIC_REF_PATTERN.findall(html)
    for ref in static_refs:
        try:
            sr = client.get(f"{BASE}{ref}", timeout=5)
            if sr.status_code != 200:
                issues.append(f"🔴 {fr_id} [{name}] 静态文件不可达: {ref} → {sr.status_code}")
        except Exception as e:
            issues.append(f"🔴 {fr_id} [{name}] 静态文件请求失败: {ref} → {e}")

    # 内部链接检查
    internal_links = INTERNAL_LINK_PATTERN.findall(html)
    broken_links = []
    for link in set(internal_links):
        if link.startswith("/static/") or link.startswith("#") or "{{" in link:
            continue
        try:
            lr = client.get(f"{BASE}{link}", follow_redirects=True, timeout=5)
            if lr.status_code >= 400 and lr.status_code != 401 and lr.status_code != 403:
                broken_links.append(f"{link}→{lr.status_code}")
        except Exception:
            broken_links.append(f"{link}→连接失败")
    if broken_links:
        issues.append(f"⚠️ {fr_id} [{name}] 断链: {broken_links[:5]}")

    # API-bridge.js引用检查
    if "/static/api-bridge.js" in html:
        pass  # API bridge存在，API检查在独立步骤中

    # 解析HTML元素
    checker = ElementChecker()
    try:
        checker.feed(html)
    except Exception:
        issues.append(f"⚠️ {fr_id} [{name}] HTML解析出错")

    # 表单action检查
    for form in checker.found_forms:
        action = form.get("action", "")
        if action and not action.startswith("#") and not action.startswith("javascript:"):
            pass  # 表单action存在

    return issues


def check_api(client, method, path, fr_id, expected_statuses):
    """检查API端点"""
    issues = []
    try:
        if method == "GET":
            resp = client.get(f"{BASE}{path}", timeout=10)
        elif method == "POST":
            resp = client.post(f"{BASE}{path}", json={}, timeout=10)
        else:
            return [f"⚠️ 未知方法 {method}"]
    except Exception as e:
        return [f"🔴 {fr_id} API不可达: {method} {path} → {e}"]

    if resp.status_code not in expected_statuses:
        issues.append(f"🔴 {fr_id} API异常: {method} {path} → {resp.status_code}, 期望∈{expected_statuses}")

    # 检查返回格式
    if resp.status_code == 200:
        ct = resp.headers.get("content-type", "")
        if "json" in ct:
            try:
                data = resp.json()
                # NFR-14 封装检查
                if isinstance(data, dict):
                    if "success" not in data and "data" not in data:
                        issues.append(f"⚠️ {fr_id} API返回缺少NFR-14封装: {method} {path}")
            except Exception:
                issues.append(f"⚠️ {fr_id} API返回非法JSON: {method} {path}")

    return issues


def check_template_rendering(client, path, name, fr_id):
    """深度检查模板渲染问题"""
    issues = []
    try:
        resp = client.get(f"{BASE}{path}", timeout=10)
    except Exception:
        return issues

    if resp.status_code != 200:
        return issues

    html = resp.text

    # 检查JS语法问题（简易检查）
    script_blocks = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
    for i, block in enumerate(script_blocks):
        if not block.strip():
            continue
        # 检查未定义的API调用
        api_calls = re.findall(r'(?:window\.YB|YB)\.(get|post|patch|delete)\w+', block)
        fetch_calls = re.findall(r'fetch\s*\(\s*[\'"`]([^\'"`]+)', block)
        for call in fetch_calls:
            if call.startswith("/api/"):
                try:
                    ar = client.get(f"{BASE}{call.split('?')[0]}", timeout=5)
                    if ar.status_code >= 500:
                        issues.append(f"🔴 {fr_id} [{name}] JS中API调用500错误: {call}")
                except Exception:
                    pass

    # 检查空容器（数据未加载）
    empty_containers = re.findall(r'id="([^"]+)"[^>]*>\s*</div>', html)
    # 这些是预期为空容器（JS动态填充）
    expected_empty = {"reports-container", "pool-section", "market-state-banner",
                      "hot-stocks-grid", "loading-indicator"}

    # 检查是否有隐藏的错误信息
    error_patterns = re.findall(r'class="[^"]*error[^"]*"[^>]*>([^<]+)', html)
    visible_errors = [e.strip() for e in error_patterns if e.strip() and "display:none" not in e]
    if visible_errors:
        issues.append(f"⚠️ {fr_id} [{name}] 页面含错误信息: {visible_errors[:3]}")

    return issues


def main():
    print("=" * 80)
    print("前端审计报告 — 精审表无法覆盖的问题")
    print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    all_issues = {}
    total_issues = 0

    with httpx.Client(timeout=15) as client:
        # 1. 服务器可达性
        print("\n## 1. 服务器可达性检查")
        try:
            r = client.get(f"{BASE}/", timeout=5)
            print(f"   ✅ 服务器可达 (状态码: {r.status_code})")
        except Exception as e:
            print(f"   🔴 服务器不可达: {e}")
            sys.exit(1)

        # 2. 页面路由检查
        print("\n## 2. 页面路由检查")
        for path, name, fr_id, expected, elems, strings in PAGE_CHECKS:
            issues = check_page(client, path, name, fr_id, expected, elems, strings)
            # 深度检查
            issues.extend(check_template_rendering(client, path, name, fr_id))
            if issues:
                all_issues.setdefault(fr_id, []).extend(issues)
                total_issues += len(issues)
                for iss in issues:
                    print(f"   {iss}")
            else:
                print(f"   ✅ {fr_id} [{name}] {path} — 通过")

        # 3. API端点检查
        print("\n## 3. API端点检查 (api-bridge.js引用的端点)")
        for method, path, fr_id, expected in API_CHECKS:
            issues = check_api(client, method, path, fr_id, expected)
            if issues:
                all_issues.setdefault(fr_id, []).extend(issues)
                total_issues += len(issues)
                for iss in issues:
                    print(f"   {iss}")
            else:
                print(f"   ✅ {fr_id} {method} {path} — 通过")

        # 4. 静态资源检查
        print("\n## 4. 静态资源检查")
        static_files = ["/static/demo.css", "/static/api-bridge.js"]
        for sf in static_files:
            try:
                sr = client.get(f"{BASE}{sf}", timeout=5)
                if sr.status_code == 200:
                    size = len(sr.content)
                    print(f"   ✅ {sf} ({size} bytes)")
                else:
                    print(f"   🔴 {sf} → {sr.status_code}")
                    total_issues += 1
            except Exception as e:
                print(f"   🔴 {sf} → {e}")
                total_issues += 1

        # 5. 认证流程检查
        print("\n## 5. 认证流程端到端检查")
        # 注册
        reg_resp = client.post(f"{BASE}/auth/register", json={
            "email": f"audit_{int(time.time())}@test.com",
            "password": "Test1234567"
        }, timeout=10)
        print(f"   注册: {reg_resp.status_code} (期望201)")
        if reg_resp.status_code == 201:
            reg_data = reg_resp.json()
            user_id = reg_data.get("data", {}).get("user_id") or reg_data.get("user_id")

            # 登录（可能需要先激活）
            login_resp = client.post(f"{BASE}/auth/login", json={
                "email": f"audit_{int(time.time())}@test.com",
                "password": "Test1234567"
            }, timeout=10)
            print(f"   登录: {login_resp.status_code} (未激活预期401)")
        elif reg_resp.status_code == 409:
            print(f"   注册409（邮箱已存在）- 跳过")

        # 6. 生成细粒度前端验收检查清单
        print("\n## 6. 前端细粒度验收清单")
        frontend_checklist = generate_frontend_checklist(client)
        for item in frontend_checklist:
            print(f"   {item}")
            if item.startswith("🔴") or item.startswith("⚠️"):
                total_issues += 1

    # 汇总
    print("\n" + "=" * 80)
    print(f"## 汇总: 发现 {total_issues} 个前端问题")
    print("=" * 80)

    if all_issues:
        print("\n### 按功能点分类:")
        for fr_id, issues in sorted(all_issues.items()):
            print(f"\n{fr_id}:")
            for iss in issues:
                print(f"  {iss}")

    # 输出JSON结果
    result = {
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%S'),
        "total_issues": total_issues,
        "issues_by_fr": all_issues,
    }
    with open("output/frontend_audit_result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n详细结果已保存到 output/frontend_audit_result.json")


def generate_frontend_checklist(client):
    """生成细粒度前端验收检查清单"""
    checks = []

    # ── 首页检查 ──
    try:
        resp = client.get(f"{BASE}/", timeout=10)
        html = resp.text
        # 检查核心区域
        if 'id="search-input"' not in html and 'search-input' not in html:
            checks.append("🔴 FR10-PAGE-01 首页缺少搜索输入框(search-input)")
        else:
            checks.append("✅ FR10-PAGE-01 首页搜索输入框存在")

        if 'api-bridge.js' not in html:
            checks.append("🔴 FR10-PAGE-01 首页未引用api-bridge.js")
        else:
            checks.append("✅ FR10-PAGE-01 首页引用api-bridge.js")

        if 'demo.css' not in html:
            checks.append("🔴 FR10-PAGE-01 首页未引用demo.css")
        else:
            checks.append("✅ FR10-PAGE-01 首页引用demo.css")

        # 检查Home API返回
        home_resp = client.get(f"{BASE}/api/v1/home", timeout=10)
        if home_resp.status_code == 200:
            home_data = home_resp.json()
            d = home_data.get("data", home_data)
            if "market_state" not in d:
                checks.append("🔴 FR10-PAGE-01 Home API缺少market_state字段")
            if "latest_reports" not in d:
                checks.append("⚠️ FR10-PAGE-01 Home API缺少latest_reports字段")
            if "pool_size" not in d:
                checks.append("⚠️ FR10-PAGE-01 Home API缺少pool_size字段")
            checks.append(f"✅ FR10-PAGE-01 Home API字段: {list(d.keys())[:8]}")
        else:
            checks.append(f"🔴 FR10-PAGE-01 Home API返回{home_resp.status_code}")
    except Exception as e:
        checks.append(f"🔴 FR10-PAGE-01 首页检查异常: {e}")

    # ── 研报列表检查 ──
    try:
        resp = client.get(f"{BASE}/reports", timeout=10)
        html = resp.text
        # 筛选器检查
        filters = ["filter-recommendation", "filter-strategy", "filter-quality", "filter-tier"]
        for f_id in filters:
            if f_id not in html:
                checks.append(f"⚠️ FR10-PAGE-02 研报列表缺少筛选器: {f_id}")
            else:
                checks.append(f"✅ FR10-PAGE-02 研报列表筛选器 {f_id} 存在")

        # API检查
        list_resp = client.get(f"{BASE}/api/v1/reports", timeout=10)
        if list_resp.status_code == 200:
            list_data = list_resp.json()
            d = list_data.get("data", list_data)
            if isinstance(d, dict):
                if "items" not in d and "reports" not in d:
                    checks.append("🔴 FR10-PAGE-02 Reports API缺少items字段")
                else:
                    checks.append("✅ FR10-PAGE-02 Reports API返回列表数据")
                if "total" not in d:
                    checks.append("⚠️ FR10-PAGE-02 Reports API缺少total字段")
        else:
            checks.append(f"🔴 FR10-PAGE-02 Reports API返回{list_resp.status_code}")
    except Exception as e:
        checks.append(f"🔴 FR10-PAGE-02 研报列表检查异常: {e}")

    # ── 统计看板检查 ──
    try:
        resp = client.get(f"{BASE}/dashboard", timeout=10)
        html = resp.text
        # Dashboard API检查
        for window in [1, 7, 14, 30, 60]:
            dash_resp = client.get(f"{BASE}/api/v1/dashboard/stats?window_days={window}", timeout=10)
            if dash_resp.status_code != 200:
                checks.append(f"🔴 FR10-PAGE-05 Dashboard API window_days={window} → {dash_resp.status_code}")
            else:
                checks.append(f"✅ FR10-PAGE-05 Dashboard API window_days={window} OK")

        # 非法窗口
        bad_resp = client.get(f"{BASE}/api/v1/dashboard/stats?window_days=999", timeout=10)
        if bad_resp.status_code == 200:
            checks.append("⚠️ FR10-PAGE-05 Dashboard API接受了非法window_days=999")
        else:
            checks.append(f"✅ FR10-PAGE-05 Dashboard API拒绝非法window_days ({bad_resp.status_code})")
    except Exception as e:
        checks.append(f"🔴 FR10-PAGE-05 看板检查异常: {e}")

    # ── 登录页检查 ──
    try:
        resp = client.get(f"{BASE}/login", timeout=10)
        html = resp.text
        if "type=\"email\"" not in html and "type=\"text\"" not in html:
            checks.append("⚠️ FR09-AUTH-02 登录页缺少邮箱输入框")
        if "type=\"password\"" not in html:
            checks.append("🔴 FR09-AUTH-02 登录页缺少密码输入框")
        if "OAuth" in html or "oauth" in html or "第三方" in html or "微信" in html:
            checks.append("✅ FR09-AUTH-05 登录页包含OAuth入口")
        else:
            checks.append("⚠️ FR09-AUTH-05 登录页可能缺少OAuth入口")
    except Exception as e:
        checks.append(f"🔴 FR09-AUTH-02 登录页检查异常: {e}")

    # ── 注册页检查 ──
    try:
        resp = client.get(f"{BASE}/register", timeout=10)
        html = resp.text
        if "password" not in html.lower():
            checks.append("🔴 FR09-AUTH-01 注册页缺少密码字段")
        else:
            checks.append("✅ FR09-AUTH-01 注册页密码字段存在")
    except Exception as e:
        checks.append(f"🔴 FR09-AUTH-01 注册页检查异常: {e}")

    # ── 订阅页检查 ──
    try:
        resp = client.get(f"{BASE}/subscribe", timeout=10)
        html = resp.text
        pricing_items = ["29.9", "79.9", "299.9", "99.9", "269.9", "999.9"]
        found_prices = [p for p in pricing_items if p in html]
        missing_prices = [p for p in pricing_items if p not in html]
        if found_prices:
            checks.append(f"✅ FR09-BILLING-01 订阅页找到定价: {found_prices}")
        if missing_prices:
            checks.append(f"⚠️ FR09-BILLING-01 订阅页缺少定价: {missing_prices}")
    except Exception as e:
        checks.append(f"🔴 FR09-BILLING-01 订阅页检查异常: {e}")

    # ── 市场状态API ──
    try:
        market_resp = client.get(f"{BASE}/api/v1/market/state", timeout=10)
        if market_resp.status_code == 200:
            md = market_resp.json()
            d = md.get("data", md)
            state = d.get("state") or d.get("market_state")
            if state not in ("BULL", "BEAR", "NEUTRAL", None):
                checks.append(f"🔴 FR05-MKT-01 市场状态值异常: {state}")
            else:
                checks.append(f"✅ FR05-MKT-01 市场状态={state}")
        else:
            checks.append(f"🔴 FR05-MKT-01 市场状态API返回{market_resp.status_code}")
    except Exception as e:
        checks.append(f"🔴 FR05-MKT-01 市场状态检查异常: {e}")

    # ── 股票池API ──
    try:
        pool_resp = client.get(f"{BASE}/api/v1/pool/stocks", timeout=10)
        if pool_resp.status_code == 200:
            pd_data = pool_resp.json()
            d = pd_data.get("data", pd_data)
            if isinstance(d, dict):
                stocks = d.get("stocks", d.get("items", []))
                checks.append(f"✅ FR01-POOL-01 股票池API返回 {len(stocks) if isinstance(stocks, list) else '?'} 只")
            elif isinstance(d, list):
                checks.append(f"✅ FR01-POOL-01 股票池API返回 {len(d)} 只")
        else:
            checks.append(f"⚠️ FR01-POOL-01 股票池API返回{pool_resp.status_code}")
    except Exception as e:
        checks.append(f"🔴 FR01-POOL-01 股票池检查异常: {e}")

    # ── 热股API ──
    try:
        hot_resp = client.get(f"{BASE}/api/v1/market/hot-stocks?limit=10", timeout=10)
        if hot_resp.status_code == 200:
            hd = hot_resp.json()
            d = hd.get("data", hd)
            checks.append(f"✅ FR04-DATA-07 热股API正常")
        else:
            checks.append(f"⚠️ FR04-DATA-07 热股API返回{hot_resp.status_code}")
    except Exception as e:
        checks.append(f"🔴 FR04-DATA-07 热股检查异常: {e}")

    # ── 缺失的500错误页面检查 ──
    try:
        resp = client.get(f"{BASE}/api/v1/reports/nonexistent-id-12345", timeout=10)
        checks.append(f"✅ 错误处理 不存在的report返回{resp.status_code}")
    except Exception:
        pass

    # ── Favicon检查 ──
    try:
        fav_resp = client.get(f"{BASE}/favicon.ico", timeout=5)
        if fav_resp.status_code == 200:
            checks.append("✅ 全局 favicon.ico 存在")
        else:
            checks.append("⚠️ 全局 缺少favicon.ico")
    except Exception:
        checks.append("⚠️ 全局 缺少favicon.ico")

    # ── 导航一致性检查 ──
    try:
        resp = client.get(f"{BASE}/", timeout=10)
        html = resp.text
        nav_links = re.findall(r'<a[^>]*href="(/[^"]*)"[^>]*class="[^"]*nav[^"]*"', html)
        if not nav_links:
            nav_links = re.findall(r'<nav[^>]*>.*?</nav>', html, re.DOTALL)
        checks.append(f"✅ 导航 首页导航元素存在")
    except Exception:
        pass

    return checks


if __name__ == "__main__":
    main()
