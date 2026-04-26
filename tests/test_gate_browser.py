"""硬门禁 2: 真实浏览器最终态门禁 — 验证正式 HTML 页面可达性与关键结构。

验证目标（来自审计方法论 4.6 §2）：
- 正式 HTML 页面（非 demo）的 HTTP 200 可达性
- 关键 DOM 结构存在性（表单、导航、关键区块）
- 页面 JS 依赖字段与 API 返回字段一致性
- 注意：真实浏览器 DOM/Network 需人工或 Playwright 补充验证，
  本测试用 TestClient 做结构锚点校验（证据等级 3）
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
import re
from uuid import uuid4

from fastapi import HTTPException
from fastapi.testclient import TestClient


class _GatePageParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements: list[dict] = []
        self._stack: list[dict] = []
        self.text_chunks: list[str] = []

    def handle_starttag(self, tag, attrs):
        elem = {"tag": tag, "attrs": dict(attrs), "text": ""}
        self.elements.append(elem)
        self._stack.append(elem)

    def handle_endtag(self, tag):
        if self._stack:
            self._stack.pop()

    def handle_data(self, data):
        if self._stack:
            self._stack[-1]["text"] += data
        if data and data.strip():
            self.text_chunks.append(data.strip())

    def has_selector(self, selector: str) -> bool:
        return any(_match_selector(elem, selector) for elem in self.elements)

    def text_contains(self, needle: str) -> bool:
        return needle in " ".join(self.text_chunks)

    def attribute_values(self, tag: str, attr: str) -> list[str]:
        return [
            elem["attrs"].get(attr, "")
            for elem in self.elements
            if elem["tag"] == tag and attr in elem["attrs"]
        ]


def _match_selector(elem: dict, selector: str) -> bool:
    attrs = elem.get("attrs", {})
    if selector.startswith("#"):
        return attrs.get("id") == selector[1:]
    if selector.startswith("."):
        return selector[1:] in attrs.get("class", "").split()
    match = re.match(r"(\w+)\[(\w+)=['\"](.+)['\"]\]", selector)
    if match:
        tag, attr, value = match.groups()
        return elem["tag"] == tag and attrs.get(attr) == value
    if "." in selector:
        tag, cls = selector.split(".", 1)
        return elem["tag"] == tag and cls in attrs.get("class", "").split()
    return elem["tag"] == selector


def _parse_html(text: str) -> _GatePageParser:
    parser = _GatePageParser()
    parser.feed(text)
    return parser


def _truncate_dynamic_routes(app, baseline_count: int) -> None:
    del app.router.routes[baseline_count:]
    app.openapi_schema = None


# ---------- 匿名可达面 ----------

def test_gate_browser_index_page_structure(client):
    """硬门禁: 首页 / 必须 200 且包含关键结构锚点。"""
    r = client.get("/")
    assert r.status_code == 200
    dom = _parse_html(r.text)
    for selector in (
        "#trading-day-flag",
        "#market-reason",
        "#home-trade-date",
        "#latest-batch-trade-date",
        "#pool-toggle",
        "#latest-reports",
    ):
        assert dom.has_selector(selector), f"home missing selector: {selector}"
    assert any(src.startswith("/static/api-bridge.js") for src in dom.attribute_values("script", "src"))
    html = r.text
    dom = _parse_html(html)
    assert "trading-day-flag" in html or "data-status" in html, "首页缺少交易日状态标记"
    assert "market-reason" in html or "state_reason" in html or "status_reason" in html, "首页缺少状态说明区"
    assert 'id="home-trade-date"' in html, "首页缺少 authoritative 批次日期"
    assert 'id="latest-batch-trade-date"' in html, "首页缺少 authoritative 最新批次日期"
    assert "/static/api-bridge.js" in html, "首页缺少共享 API bridge"
    assert "pool-toggle" in html, "首页缺少核心池展开控制"
    assert 'id="latest-reports"' in html, "首页缺少最新研报容器"


def test_gate_browser_login_page_uses_email(client):
    """硬门禁: /login 页面必须使用 email 字段（非 account）。"""
    r = client.get("/login")
    assert r.status_code == 200
    html = r.text
    dom = _parse_html(html)
    assert dom.has_selector("#login-form")
    assert dom.has_selector("input[name='email']")
    assert not dom.has_selector("input[name='account']")
    assert "res.code" not in html, "/login 浠嶄娇鐢?legacy res.code===0 鍒ゆ柇"
    assert "res.success" in html, "/login 缂哄皯 res.success 鎴愬姛鍒ゆ柇"
    assert "account" not in html.lower() or "email" in html.lower(), "/login 仍包含 legacy account 字段"
    # 成功判断必须用 res.success 而非 res.code===0
    assert "res.code" not in html or "res.success" in html, "/login 仍使用 legacy res.code===0 判断"


def test_gate_browser_login_page_does_not_overclaim_oauth(client):
    """硬门禁: 未接入 provider 时，/login 不得静态宣称支持第三方账号登录。"""
    r = client.get("/login")
    assert r.status_code == 200
    html = r.text
    dom = _parse_html(html)
    assert dom.has_selector("#oauth-note")
    assert dom.has_selector("#oauth-providers")
    assert "—— 或 使用第三方账号 ——" not in html, "/login 仍静态宣称支持第三方账号"
    assert "当前暂未提供可用的第三方登录方式，请使用邮箱登录。" in html, "/login 缺少第三方登录未显示时的用户提示"


def test_gate_browser_forgot_password_uses_email(client):
    """硬门禁: /forgot-password 必须提交 {email} 而非 {account}。"""
    r = client.get("/forgot-password")
    assert r.status_code == 200
    html = r.text
    dom = _parse_html(html)
    assert dom.has_selector("#forgot-form")
    assert dom.has_selector("input[name='email']")
    assert not dom.has_selector("input[name='account']")
    assert "联系管理员" not in html, "/forgot-password 不应暴露运维处理话术"
    # 检查提交字段不是 account
    assert "{account" not in html, "/forgot-password 仍提交 legacy {account} 字段"


def test_gate_browser_reset_password_invalid_token_hides_form(client):
    r = client.get("/reset-password?token=bad")
    assert r.status_code == 200
    html = r.text
    dom = _parse_html(html)
    assert not dom.has_selector("#reset-form")
    assert dom.has_selector("a[href='/forgot-password']")
    assert 'id="reset-form"' not in html
    assert "重新申请" in html
    assert "/forgot-password" in html


def test_gate_browser_register_page_structure(client):
    """硬门禁: /register 必须使用 email 字段且结构与 login 对齐。"""
    r = client.get("/register")
    assert r.status_code == 200
    html = r.text
    dom = _parse_html(html)
    assert dom.has_selector("#register-form")
    assert dom.has_selector("input[name='email']")
    assert dom.has_selector("input[type='email']")
    assert dom.has_selector("#btn-submit")
    assert "若站点启用邮箱验证，注册后需完成激活。" in html, "/register 未明确提示邮箱验证按站点配置生效"
    assert "type=\"email\"" in html or "type='email'" in html, "/register 缺 email input type"


def test_gate_browser_reports_list_page(client):
    """硬门禁: /reports 列表页必须 200。"""
    r = client.get("/reports")
    assert r.status_code == 200
    dom = _parse_html(r.text)
    assert dom.has_selector("#filter-quality")
    assert dom.has_selector("#filter-tier")
    assert not dom.has_selector("option[value='missing']")
    assert 'id="filter-quality"' in r.text
    assert 'value="missing"' not in r.text


def test_gate_browser_subscribe_page(client):
    """硬门禁: /subscribe 订阅页必须 200，且不能内联兜底套餐继续售卖。"""
    r = client.get("/subscribe")
    assert r.status_code == 200
    html = r.text
    dom = _parse_html(html)
    assert dom.has_selector("#subscription-state")
    assert dom.has_selector("#subscribe-grid")
    assert "res.code" not in html, "/subscribe 浠嶄娇鐢?legacy res.code===0 鍒ゆ柇"
    assert "订阅方案" in html
    assert "正在确认当前权益" in html
    assert "var subscriptionPlansPayload =" in html
    assert "var plans = [" not in html
    assert "pro_1m" in html
    assert "enterprise_12m" in html
    assert "feature_registry" not in html
    if "res.code" in html:
        assert "res.success" in html, "/subscribe 仍使用 legacy res.code===0 判断"


# ---------- 认证后可达面 ----------

def _login_headers(client, create_user, role="user"):
    """创建用户并登录，返回 Authorization header。"""
    user = create_user(
        email=f"gate_browser_{role}@example.com",
        password="Password123",
        role=role,
        email_verified=True,
    )
    r = client.post("/auth/login", json={"email": user["user"].email, "password": user["password"]})
    assert r.status_code == 200
    token = r.json()["data"]["access_token"]
    return {"Authorization": f"Bearer {token}", "Cookie": f"access_token={token}"}


def test_gate_browser_admin_page_requires_auth(client):
    """硬门禁: /admin 未认证时必须拒绝或重定向。"""
    r = client.get("/admin", follow_redirects=False)
    assert r.status_code in {302, 303, 401, 403}, "/admin 匿名可直达"


def test_gate_browser_profile_redirects_to_login(client):
    """硬门禁: /profile 匿名访问必须跳转到登录页。"""
    r = client.get("/profile", follow_redirects=False)
    assert r.status_code == 302
    assert "/login?next=/profile" in r.headers.get("location", "")


def test_gate_browser_profile_uses_business_copy(client, create_user):
    headers = _login_headers(client, create_user, role="user")
    r = client.get("/profile", headers=headers)
    assert r.status_code == 200
    html = r.text
    assert "订阅权益" in html
    assert "访问权限" in html
    assert "身份角色" not in html
    assert "feedback_type" not in html


def test_gate_browser_login_page_redirects_when_already_logged_in(client, create_user):
    headers = _login_headers(client, create_user, role="user")
    r = client.get("/login", headers=headers, follow_redirects=False)
    assert r.status_code == 302
    assert r.headers.get("location") == "/"


def test_gate_browser_register_page_redirects_when_already_logged_in(client, create_user):
    headers = _login_headers(client, create_user, role="user")
    r = client.get("/register", headers=headers, follow_redirects=False)
    assert r.status_code == 302
    assert r.headers.get("location") == "/"


def test_gate_browser_home_admin_entry_uses_business_copy(client, create_user):
    headers = _login_headers(client, create_user, role="admin")
    r = client.get("/", headers=headers)
    assert r.status_code == 200
    html = r.text
    assert "页面与接口总览" in html
    assert "治理区" not in html
    assert "FR-08" not in html


def test_gate_browser_home_expired_membership_user_does_not_500(client, create_user, db_session):
    account = create_user(
        email="gate_browser_expired_admin@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )
    user = account["user"]
    user.membership_level = "monthly"
    user.membership_expires_at = datetime.now(timezone.utc) - timedelta(days=1)
    db_session.add(user)
    db_session.commit()

    login = client.post("/auth/login", json={"email": user.email, "password": account["password"]})
    assert login.status_code == 200
    token = login.json()["data"]["access_token"]

    r = client.get("/", headers={"Authorization": f"Bearer {token}", "Cookie": f"access_token={token}"})
    assert r.status_code == 200
    assert "服务器开小差了" not in r.text


def test_gate_browser_admin_page_admin_accessible(client, db_session, create_user):
    """硬门禁: admin 角色可访问 /admin 且页面包含关键区块。"""
    headers = _login_headers(client, create_user, role="admin")
    r = client.get("/admin", headers=headers)
    assert r.status_code == 200
    html = r.text
    assert "admin" in html.lower(), "/admin 页面缺少管理相关内容"
    assert "用户概览" in html, "/admin 用户区仍沿用误导性的“用户管理”文案"


def test_gate_browser_admin_page_non_admin_uses_html_403(client, create_user):
    """硬门禁: 非管理员访问 /admin 应返回站内 403 HTML 页面。"""
    headers = _login_headers(client, create_user, role="user")
    r = client.get("/admin", headers=headers)
    assert r.status_code == 403
    assert "text/html" in r.headers.get("content-type", "")
    dom = _parse_html(r.text)
    assert dom.has_selector(".error-page")
    assert dom.text_contains("403")
    assert dom.text_contains("无权限访问")


def test_gate_browser_features_403_does_not_force_admin_login(client, create_user):
    headers = _login_headers(client, create_user, role="user")
    r = client.get("/features", headers=headers)
    assert r.status_code == 403
    html = r.text
    assert "/login?next=/admin" not in html
    assert "/reports" in html


def test_gate_browser_admin_page_hides_purge_button_for_plain_admin(client, create_user):
    headers = _login_headers(client, create_user, role="admin")
    r = client.get("/admin", headers=headers)
    assert r.status_code == 200
    assert "清除旧数据" not in r.text


def test_gate_browser_admin_page_discloses_read_only_user_overview(client, create_user):
    headers = _login_headers(client, create_user, role="admin")
    r = client.get("/admin", headers=headers)
    assert r.status_code == 200
    dom = _parse_html(r.text)
    assert dom.text_contains("当前页面展示只读用户概览")


def test_gate_browser_admin_page_hides_undocumented_purge_button_for_super_admin(client, create_user):
    headers = _login_headers(client, create_user, role="super_admin")
    r = client.get("/admin", headers=headers)
    assert r.status_code == 200
    assert "清除旧数据" not in r.text


def test_gate_browser_super_admin_nav_shows_admin_link(client, db_session, create_user):
    """硬门禁: super_admin 导航必须显示管理后台入口（P2-07 门禁）。"""
    headers = _login_headers(client, create_user, role="super_admin")
    r = client.get("/", headers=headers)
    assert r.status_code == 200
    html = r.text
    assert "admin" in html.lower() or "管理" in html, "super_admin 首页导航缺少管理后台入口"


def test_gate_browser_sim_dashboard_anonymous_limited(client):
    """硬门禁: /portfolio/sim-dashboard 匿名用户必须先跳转登录。"""
    r = client.get("/portfolio/sim-dashboard", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"] == "/login?next=/portfolio/sim-dashboard"


def test_gate_browser_unknown_html_path_renders_site_404(client):
    """硬门禁: 非 API 未知路径必须返回站内 404 HTML 页面。"""
    r = client.get(f"/not-found-gate-{uuid4().hex}")
    assert r.status_code == 404
    assert "text/html" in r.headers.get("content-type", "")
    dom = _parse_html(r.text)
    assert dom.has_selector(".error-page")
    assert dom.text_contains("404")
    assert dom.text_contains("页面不见了")


def test_gate_browser_unknown_api_path_stays_json_envelope(client):
    """硬门禁: API 未知路径继续返回 JSON envelope，不走 HTML 错误页。"""
    r = client.get(f"/api/v1/not-found-gate-{uuid4().hex}")
    assert r.status_code == 404
    assert "application/json" in r.headers.get("content-type", "")
    body = r.json()
    assert body["success"] is False
    assert body["error_code"] == "NOT_FOUND"
    assert body["data"] is None


def test_gate_browser_html_500_renders_site_error_page(isolated_app):
    """硬门禁: 非 API 页面异常应进入站内 500 HTML 页面。"""
    app = isolated_app["app"]
    route_path = f"/_test/boom-html-{uuid4().hex}"
    baseline_count = len(app.router.routes)

    async def _boom_html():
        raise RuntimeError("boom-html")

    app.add_api_route(route_path, _boom_html, methods=["GET"])
    try:
        with TestClient(app, base_url="http://localhost", raise_server_exceptions=False) as local_client:
            r = local_client.get(route_path)

        assert r.status_code == 500
        assert "text/html" in r.headers.get("content-type", "")
        dom = _parse_html(r.text)
        assert dom.has_selector(".error-page")
        assert dom.text_contains("500")
        assert dom.text_contains("服务器开小差了")
    finally:
        _truncate_dynamic_routes(app, baseline_count)


def test_gate_browser_html_operational_error_uses_site_error_page(isolated_app):
    from sqlalchemy.exc import OperationalError

    app = isolated_app["app"]
    route_path = f"/_test/operational-html-{uuid4().hex}"
    baseline_count = len(app.router.routes)

    async def _boom_operational():
        raise OperationalError("SELECT 1", {}, Exception("database is locked"))

    app.add_api_route(route_path, _boom_operational, methods=["GET"])
    try:
        with TestClient(app, base_url="http://localhost", raise_server_exceptions=False) as local_client:
            r = local_client.get(route_path)

        assert r.status_code == 503
        assert "text/html" in r.headers.get("content-type", "")
        dom = _parse_html(r.text)
        assert dom.has_selector(".error-page")
        assert dom.text_contains("500")
    finally:
        _truncate_dynamic_routes(app, baseline_count)


def test_gate_browser_html_400_uses_site_error_page(isolated_app):
    app = isolated_app["app"]
    route_path = f"/_test/bad-request-html-{uuid4().hex}"
    baseline_count = len(app.router.routes)

    async def _bad_request_html():
        raise HTTPException(status_code=400, detail="参数错误")

    app.add_api_route(route_path, _bad_request_html, methods=["GET"])
    try:
        with TestClient(app, base_url="http://localhost", raise_server_exceptions=False) as local_client:
            r = local_client.get(route_path)

        assert r.status_code == 400
        assert "text/html" in r.headers.get("content-type", "")
        dom = _parse_html(r.text)
        assert dom.has_selector("main.error-page")
        assert dom.text_contains("请求参数不正确")
    finally:
        _truncate_dynamic_routes(app, baseline_count)

def test_gate_browser_html_422_uses_site_error_page(isolated_app):
    app = isolated_app["app"]
    route_path = f"/_test/validate-html-{uuid4().hex}"
    baseline_count = len(app.router.routes)

    async def _validate_html(limit: int):
        return {"limit": limit}

    app.add_api_route(route_path, _validate_html, methods=["GET"])
    try:
        with TestClient(app, base_url="http://localhost", raise_server_exceptions=False) as local_client:
            r = local_client.get(route_path, params={"limit": "oops"})

        assert r.status_code == 422
        assert "text/html" in r.headers.get("content-type", "")
        dom = _parse_html(r.text)
        assert dom.has_selector("main.error-page")
        assert dom.text_contains("请求参数不正确")
    finally:
        _truncate_dynamic_routes(app, baseline_count)


def test_gate_browser_api_500_stays_json_envelope(isolated_app):
    """硬门禁: API 路径异常继续返回 JSON envelope。"""
    app = isolated_app["app"]
    route_path = f"/api/v1/_test/boom-api-{uuid4().hex}"
    baseline_count = len(app.router.routes)

    async def _boom_api():
        raise RuntimeError("boom-api")

    app.add_api_route(route_path, _boom_api, methods=["GET"])
    try:
        with TestClient(app, base_url="http://localhost", raise_server_exceptions=False) as local_client:
            r = local_client.get(route_path)

        assert r.status_code == 500
        assert "application/json" in r.headers.get("content-type", "")
        body = r.json()
        assert body["success"] is False
        assert body["error_code"] == "INTERNAL_ERROR"
        assert body["data"] is None
    finally:
        _truncate_dynamic_routes(app, baseline_count)
