"""
Doc-driven verification entrypoint.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.trade_calendar import latest_trade_date_str
from scripts.doc_driven.page_expectations import PAGE_EXPECTATIONS
from tests.helpers_ssot import (
    insert_market_state_cache,
    insert_pool_snapshot,
    insert_report_bundle_ssot,
    insert_stock_master,
)

pytestmark = [pytest.mark.doc_driven]

API_CONTRACT_FIELDS = {
    "/api/v1/home": ["market_state", "pool_size", "today_report_count", "hot_stocks"],
    "/api/v1/reports": ["items", "total", "page", "page_size"],
    "/api/v1/dashboard/stats": ["window_days", "total_reports", "total_settled", "data_status", "status_reason", "by_strategy_type"],
}

REPORT_MIN_FIELDS = {"report_id", "stock_code", "trade_date", "recommendation", "confidence", "published"}

UNIVERSAL_FORBIDDEN = [
    "Traceback (most recent call last)",
    "Internal Server Error",
    "jinja2.exceptions",
    "TemplateSyntaxError",
]


def _login_as(client: TestClient, create_user, *, tier: str = "Free", role: str = "user") -> dict[str, str]:
    password = "TestPass123!"
    user_data = create_user(
        email=f"{role}_{tier}_{uuid4().hex[:8]}@test.com".lower(),
        password=password,
        tier=tier,
        role=role,
        email_verified=True,
    )
    resp = client.post("/auth/login", json={"email": user_data["user"].email, "password": password})
    assert resp.status_code == 200, f"login failed: {resp.status_code} {resp.text}"
    body = resp.json()
    assert body["success"] is True
    token = body["data"].get("access_token")
    assert token, "login response missing access_token"
    return {"Authorization": f"Bearer {token}"}


def _assert_envelope(resp_json: dict, success: bool = True):
    assert "success" in resp_json, "response missing success"
    assert resp_json["success"] is success
    assert "request_id" in resp_json, "response missing request_id"
    assert isinstance(resp_json["request_id"], str), "request_id must be a string"
    assert resp_json["request_id"].strip(), "request_id must not be blank"


def _assert_no_forbidden(text: str, forbidden: list[str], context: str = ""):
    for item in forbidden + UNIVERSAL_FORBIDDEN:
        assert item not in text, f"{context} contains forbidden content: {item}"


def _parse_html(text: str):
    from html.parser import HTMLParser

    class SimpleDOM(HTMLParser):
        def __init__(self):
            super().__init__()
            self.elements: list[dict] = []
            self._stack: list[dict] = []

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

        def has_selector(self, selector: str) -> bool:
            return any(_match_selector(elem, selector) for elem in self.elements)

        def text_contains(self, needle: str) -> bool:
            return needle in self.text_content()

        def text_content(self) -> str:
            return " ".join((elem.get("text") or "").strip() for elem in self.elements if (elem.get("text") or "").strip())

    dom = SimpleDOM()
    dom.feed(text)
    return dom


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


class TestAPIContracts:
    def test_FR10_SITE_01_home_api_contract(self, client, db_session, seed_report_bundle):
        seed_report_bundle()
        resp = client.get("/api/v1/home")
        assert resp.status_code == 200
        body = resp.json()
        _assert_envelope(body)
        data = body["data"]
        for field in API_CONTRACT_FIELDS["/api/v1/home"]:
            assert field in data, f"/api/v1/home missing field: {field}"

    def test_FR10_SITE_03_reports_list_contract(self, client, db_session, seed_report_bundle):
        seed_report_bundle()
        resp = client.get("/api/v1/reports")
        assert resp.status_code == 200
        body = resp.json()
        _assert_envelope(body)
        data = body["data"]
        for field in API_CONTRACT_FIELDS["/api/v1/reports"]:
            assert field in data, f"/api/v1/reports missing field: {field}"
        items = data["items"]
        assert items, "/api/v1/reports returned no items after seeding"
        for field in REPORT_MIN_FIELDS:
            assert field in items[0], f"report item missing field: {field}"

    def test_FR10_SITE_03_report_has_stock_name(self, client, db_session, seed_report_bundle):
        seed_report_bundle(stock_name="贵州茅台")
        resp = client.get("/api/v1/reports")
        assert resp.status_code == 200
        items = resp.json()["data"]["items"]
        assert items, "reports list unexpectedly empty after seeding"
        report = items[0]
        stock_name = report.get("stock_name") or report.get("stock_name_snapshot") or report.get("name")
        assert stock_name, f"report list item missing stock_name-related field; actual keys: {sorted(report.keys())}"

    def test_FR10_SITE_01_home_pool_consistency(self, client, db_session):
        trade_date = latest_trade_date_str()
        insert_stock_master(db_session, stock_code="600519.SH", stock_name="贵州茅台")
        insert_stock_master(db_session, stock_code="000001.SZ", stock_name="平安银行", exchange="SZ")
        insert_pool_snapshot(db_session, trade_date=trade_date, stock_codes=["600519.SH", "000001.SZ"])
        insert_market_state_cache(db_session, trade_date=trade_date, market_state="BULL")
        insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="贵州茅台", trade_date=trade_date)
        insert_report_bundle_ssot(db_session, stock_code="000001.SZ", stock_name="平安银行", trade_date=trade_date)

        home_resp = client.get("/api/v1/home")
        pool_resp = client.get("/api/v1/pool/stocks", params={"trade_date": trade_date})
        assert home_resp.status_code == 200
        assert pool_resp.status_code == 200

        home_data = home_resp.json()["data"]
        pool_data = pool_resp.json()["data"]
        pool_items = pool_data.get("items", pool_data.get("stocks", []))
        assert isinstance(home_data.get("pool_size"), int), "home payload missing integer pool_size"
        assert pool_items, "pool stocks endpoint returned no items after seeding"
        assert home_data["pool_size"] == len(pool_items), f"home pool_size={home_data['pool_size']} but pool endpoint returned {len(pool_items)}"

    def test_FR10_SITE_05_dashboard_contract(self, client, db_session, seed_report_bundle):
        seed_report_bundle()
        resp = client.get("/api/v1/dashboard/stats")
        assert resp.status_code == 200
        body = resp.json()
        _assert_envelope(body)
        data = body["data"]
        for field in API_CONTRACT_FIELDS["/api/v1/dashboard/stats"]:
            assert field in data, f"/api/v1/dashboard/stats missing field: {field}"
        assert data["data_status"], "dashboard payload missing data_status"
        assert data.get("status_reason"), "dashboard payload missing status_reason"

    def test_FR10_SITE_04_soft_deleted_report_detail_returns_not_available(self, client, db_session, seed_report_bundle):
        report = seed_report_bundle()
        db_session.execute(
            text(
                """
                UPDATE report
                SET is_deleted = 1,
                    published = 0,
                    publish_status = 'UNPUBLISHED'
                WHERE report_id = :report_id
                """
            ),
            {"report_id": report.report_id},
        )
        db_session.commit()

        resp = client.get(f"/api/v1/reports/{report.report_id}")
        assert resp.status_code == 404
        body = resp.json()
        assert body.get("success") is False
        assert body.get("error_code") == "REPORT_NOT_AVAILABLE"

    def test_FR10_SITE_04_non_ok_report_detail_returns_not_available(self, client, db_session, seed_report_bundle):
        report = seed_report_bundle(quality_flag="stale_ok")

        resp = client.get(f"/api/v1/reports/{report.report_id}")
        assert resp.status_code == 404
        body = resp.json()
        assert body.get("success") is False
        assert body.get("error_code") == "REPORT_NOT_AVAILABLE"

    @pytest.mark.feature("FR10-DETAIL-02")
    def test_FR09_AUTH_01_anonymous_advanced_area_401(self, client):
        resp = client.get("/api/v1/reports/fake-id/advanced")
        assert resp.status_code in (401, 403), f"anonymous advanced area returned {resp.status_code}"

    @pytest.mark.feature("FR09-AUTH-02")
    def test_FR09_AUTH_01_login_response_contract(self, client, create_user):
        password = "TestPass123!"
        create_user(email="login_test@test.com", password=password, email_verified=True)
        resp = client.post("/auth/login", json={"email": "login_test@test.com", "password": password})
        assert resp.status_code == 200
        body = resp.json()
        _assert_envelope(body)
        token = body.get("data", {}).get("access_token") or resp.cookies.get("access_token") or resp.cookies.get("session")
        assert token, "login succeeded but no token/cookie was returned"


class TestDOMVerification:
    def test_FR10_SITE_01_home_dom(self, client, db_session, seed_report_bundle):
        seed_report_bundle()
        resp = client.get("/")
        assert resp.status_code == 200
        text = resp.text
        _assert_no_forbidden(text, [], context="/")
        dom = _parse_html(text)
        for sel in ["#hero-form", "#hero-stock-code", "#home-status", "#market-state", "#market-reason", "#pool-toggle", "#latest-reports", ".market-bar", ".home-shell"]:
            assert dom.has_selector(sel), f"home missing DOM selector: {sel}"

    def test_FR10_SITE_03_reports_list_dom(self, client, db_session, seed_report_bundle):
        seed_report_bundle()
        resp = client.get("/reports")
        assert resp.status_code == 200
        _assert_no_forbidden(resp.text, [], context="/reports")
        dom = _parse_html(resp.text)
        for sel in [".filter-bar", "#reports-list", "#apply-filters"]:
            assert dom.has_selector(sel), f"reports page missing DOM selector: {sel}"

    def test_FR10_SITE_04_report_detail_dom(self, client, db_session, seed_report_bundle):
        report = seed_report_bundle()
        resp = client.get(f"/reports/{report.report_id}")
        assert resp.status_code == 200
        _assert_no_forbidden(resp.text, [], context=f"/reports/{report.report_id}")
        dom = _parse_html(resp.text)
        for sel in [".rv-hero", ".rv-conclusion"]:
            assert dom.has_selector(sel), f"report detail missing DOM selector: {sel}"

    @pytest.mark.feature("FR10-DETAIL-01")
    def test_FR10_SITE_04_report_detail_anonymous_instruction_prices_show_lock_copy(self, client, db_session, seed_report_bundle):
        report = seed_report_bundle()
        resp = client.get(f"/reports/{report.report_id}")
        assert resp.status_code == 200
        dom = _parse_html(resp.text)
        assert dom.text_contains("登录后可见")
        assert "¥**.**" not in resp.text

    @pytest.mark.feature("FR09-AUTH-02")
    def test_FR09_AUTH_01_login_page_dom(self, client):
        resp = client.get("/login")
        assert resp.status_code == 200
        dom = _parse_html(resp.text)
        assert dom.has_selector("form")
        assert dom.has_selector("input[name='email']")
        assert dom.has_selector("#oauth-note")

    @pytest.mark.feature("FR09-AUTH-01")
    def test_FR09_AUTH_02_register_page_dom(self, client):
        resp = client.get("/register")
        assert resp.status_code == 200
        dom = _parse_html(resp.text)
        assert dom.has_selector("form")
        assert dom.has_selector("input[name='email']")

    def test_FR09_BILL_01_subscribe_page_dom(self, client):
        resp = client.get("/subscribe")
        assert resp.status_code == 200
        _assert_no_forbidden(resp.text, ["401", "Unauthorized"], context="/subscribe")
        dom = _parse_html(resp.text)
        assert dom.has_selector("#subscribe-grid")

    def test_FR09_AUTH_06_profile_redirect_anonymous(self, client):
        resp = client.get("/profile", follow_redirects=False)
        assert resp.status_code == 302
        assert "/login" in resp.headers.get("location", "")

    def test_FR12_ADMIN_01_admin_redirect_non_admin(self, client, create_user):
        headers = _login_as(client, create_user, tier="Free", role="user")
        resp = client.get("/admin", headers=headers, follow_redirects=False)
        assert resp.status_code == 403, f"/admin non-admin returned {resp.status_code}"

    def test_FR10_SITE_05_dashboard_dom(self, client, db_session, seed_report_bundle):
        seed_report_bundle()
        resp = client.get("/dashboard")
        assert resp.status_code == 200
        _assert_no_forbidden(resp.text, [], context="/dashboard")
        dom = _parse_html(resp.text)
        for sel in ["#window-tabs", "#dashboard-status", "#date-range", "#strategy-grid"]:
            assert dom.has_selector(sel), f"dashboard missing DOM selector: {sel}"

    def test_FR08_SIM_06_sim_dashboard_dom(self, client, db_session, seed_report_bundle, create_user):
        seed_report_bundle()
        headers = _login_as(client, create_user, tier="Pro", role="user")
        resp = client.get("/portfolio/sim-dashboard", headers=headers)
        assert resp.status_code == 200
        dom = _parse_html(resp.text)
        assert dom.has_selector("#tier-tabs")

    @pytest.mark.feature("FR10-FEATURE-01")
    def test_FR10_SITE_08_features_page_dom(self, client, create_user):
        headers = _login_as(client, create_user, tier="Enterprise", role="admin")
        resp = client.get("/features", headers=headers)
        assert resp.status_code == 200
        _assert_no_forbidden(resp.text, [], context="/features")
        dom = _parse_html(resp.text)
        for sel in [".feature-summary", ".fr-group", ".feature-card"]:
            assert dom.has_selector(sel), f"features page missing DOM selector: {sel}"

    @pytest.mark.feature("FR09-AUTH-06")
    def test_FR09_AUTH_04_forgot_password_dom(self, client):
        resp = client.get("/forgot-password")
        assert resp.status_code == 200
        dom = _parse_html(resp.text)
        assert dom.has_selector("form")
        assert dom.has_selector("input[name='email']")

    @pytest.mark.feature("FR09-AUTH-06")
    def test_FR09_AUTH_05_reset_password_dom(self, client):
        resp = client.get("/reset-password")
        assert resp.status_code == 200
        dom = _parse_html(resp.text)
        assert not dom.has_selector("form")
        assert "/forgot-password" in resp.text

        bad_resp = client.get("/reset-password?token=bad")
        assert bad_resp.status_code == 200
        bad_dom = _parse_html(bad_resp.text)
        assert not bad_dom.has_selector("form")
        assert "/forgot-password" in bad_resp.text

    def test_FR10_terms_dom(self, client):
        resp = client.get("/terms")
        assert resp.status_code == 200
        dom = _parse_html(resp.text)
        assert dom.has_selector(".legal-page")

    def test_FR10_privacy_dom(self, client):
        resp = client.get("/privacy")
        assert resp.status_code == 200
        dom = _parse_html(resp.text)
        assert dom.has_selector(".legal-page")

    def test_FR10_report_error_dom(self, client, db_session):
        insert_stock_master(db_session, stock_code="600519.SH", stock_name="贵州茅台")
        db_session.commit()
        resp = client.get("/report/600519.SH?cached_only=true")
        assert resp.status_code == 404
        _assert_no_forbidden(resp.text, [], context="/report/{stock_code}?cached_only=true")
        dom = _parse_html(resp.text)
        for sel in [".wrap", ".card", "h2", "a[href=\"/reports\"]"]:
            assert dom.has_selector(sel), f"report_error missing DOM selector: {sel}"

    def test_FR10_legacy_report_loading_dom(self, client, db_session):
        insert_stock_master(db_session, stock_code="600519.SH", stock_name="贵州茅台")
        db_session.commit()
        resp = client.get("/report/600519.SH")
        assert resp.status_code == 404
        _assert_no_forbidden(resp.text, [], context="/report/{stock_code}")
        dom = _parse_html(resp.text)
        assert not dom.has_selector(".progress-bar")
        assert dom.text_contains("查看研报列表")


class TestCrossAPIConsistency:
    def test_FR10_home_vs_reports_trade_date(self, client, db_session, seed_report_bundle):
        seed_report_bundle()
        home_resp = client.get("/api/v1/home")
        reports_resp = client.get("/api/v1/reports")
        assert home_resp.status_code == 200
        assert reports_resp.status_code == 200
        home = home_resp.json()["data"]
        reports = reports_resp.json()["data"]
        home_count = home.get("today_report_count")
        list_total = reports.get("total")
        assert isinstance(home_count, int), "home today_report_count must be an integer"
        assert isinstance(list_total, int), "reports total must be an integer"
        assert home_count <= list_total, f"home today_report_count={home_count} > reports total={list_total}"

    def test_FR08_sim_capital_tier_enum(self, client, create_user, db_session, seed_report_bundle):
        seed_report_bundle()
        headers = _login_as(client, create_user, tier="Free", role="user")
        resp = client.get("/api/v1/portfolio/sim-dashboard", headers=headers, params={"capital_tier": "100k"})
        assert resp.status_code == 403
        assert resp.json()["error_code"] == "TIER_NOT_AVAILABLE"


class TestViewerTierAccess:
    @pytest.mark.feature("FR09-AUTH-08")
    def test_FR09_auth_me_truth_for_admin_pro_free(self, client, create_user):
        specs = (
            ("admin@example.com", "admin", "Free"),
            ("v79_pro@test.com", "user", "Pro"),
            ("v79_free@test.com", "user", "Free"),
        )

        for email, role, tier in specs:
            account = create_user(
                email=email,
                password="Password123",
                tier=tier,
                role=role,
                email_verified=True,
            )
            login = client.post("/auth/login", json={"email": email, "password": account["password"]})
            assert login.status_code == 200
            token = login.json()["data"]["access_token"]
            resp = client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
            assert resp.status_code == 200
            data = resp.json()["data"]
            assert data["role"] == role
            assert data["tier"] == tier
            assert data["membership_level"] == "free"

    @pytest.mark.feature("FR10-DETAIL-02")
    def test_FR09_anonymous_no_advanced(self, client, db_session, seed_report_bundle):
        report = seed_report_bundle()
        resp = client.get(f"/api/v1/reports/{report.report_id}/advanced")
        assert resp.status_code in (401, 403)

    @pytest.mark.feature("FR10-DETAIL-02")
    def test_FR09_free_advanced_truncated(self, client, create_user, db_session, seed_report_bundle):
        report = seed_report_bundle()
        headers = _login_as(client, create_user, tier="Free", role="user")
        resp = client.get(f"/api/v1/reports/{report.report_id}/advanced", headers=headers)
        assert resp.status_code == 200, f"free advanced area returned {resp.status_code}"
        data = resp.json()["data"]
        text = data.get("reasoning_chain", "")
        assert isinstance(text, str), "free advanced payload did not return text content"
        assert len(text) <= 200, f"free advanced text must be truncated to <=200 chars, got {len(text)}"

    @pytest.mark.feature("FR10-DETAIL-01")
    def test_FR09_free_report_detail_shows_advanced_preview(self, client, create_user, db_session, seed_report_bundle):
        report = seed_report_bundle()
        headers = _login_as(client, create_user, tier="Free", role="user")
        resp = client.get(f"/reports/{report.report_id}", headers=headers)
        assert resp.status_code == 200
        dom = _parse_html(resp.text)
        assert dom.text_contains("当前仅展示前 200 字预览")
        assert dom.text_contains("完整推理过程仅对订阅用户可见")
        assert dom.has_selector('a[href="/subscribe"]')

    @pytest.mark.feature("FR10-DETAIL-01")
    def test_FR09_free_report_detail_preview_hides_internal_fallback_terms(
        self, client, create_user, db_session, seed_report_bundle
    ):
        report = seed_report_bundle()
        db_session.execute(
            text(
                """
                UPDATE report
                SET llm_fallback_level = 'failed',
                    conclusion_text = :conclusion_text,
                    reasoning_chain_md = :reasoning_chain_md
                WHERE report_id = :report_id
                """
            ),
            {
                "report_id": report.report_id,
                "conclusion_text": "石化油服 600871.SH 研报生成（LLM降级，规则兜底）",
                "reasoning_chain_md": "## 分析过程（LLM降级，规则兜底）\nmarket_state=NEUTRAL\nstrategy_type=B\nquality_flag=ok",
            },
        )
        db_session.commit()
        headers = _login_as(client, create_user, tier="Free", role="user")
        resp = client.get(f"/reports/{report.report_id}", headers=headers)
        assert resp.status_code == 200
        assert "## 分析过程（LLM降级，规则兜底）" not in resp.text
        assert "market_state=NEUTRAL" not in resp.text
        assert "strategy_type=B" not in resp.text

    @pytest.mark.feature("FR10-DETAIL-02")
    def test_FR09_pro_advanced_full(self, client, create_user, db_session, seed_report_bundle):
        report = seed_report_bundle()
        headers = _login_as(client, create_user, tier="Pro", role="user")
        resp = client.get(f"/api/v1/reports/{report.report_id}/advanced", headers=headers)
        assert resp.status_code == 200, f"pro advanced area returned {resp.status_code}"
        data = resp.json()["data"]
        text = data.get("reasoning_chain", "")
        assert "reasoning_chain" in data, "pro advanced payload missing reasoning_chain"
        assert isinstance(text, str), "pro advanced payload returned non-string reasoning_chain"

    @pytest.mark.feature("FR10-DETAIL-02")
    def test_FR09_pro_advanced_hides_internal_fallback_terms(
        self, client, create_user, db_session, seed_report_bundle
    ):
        report = seed_report_bundle()
        db_session.execute(
            text(
                """
                UPDATE report
                SET llm_fallback_level = 'failed',
                    conclusion_text = :conclusion_text,
                    reasoning_chain_md = :reasoning_chain_md
                WHERE report_id = :report_id
                """
            ),
            {
                "report_id": report.report_id,
                "conclusion_text": "石化油服 600871.SH 研报生成（LLM降级，规则兜底）",
                "reasoning_chain_md": "## 分析过程（LLM降级，规则兜底）\nmarket_state=NEUTRAL\nstrategy_type=B\nquality_flag=ok\nfallback=rule_based",
            },
        )
        db_session.commit()
        headers = _login_as(client, create_user, tier="Pro", role="user")
        resp = client.get(f"/api/v1/reports/{report.report_id}/advanced", headers=headers)
        assert resp.status_code == 200
        reasoning_text = resp.json()["data"].get("reasoning_chain", "")
        assert "market_state=" not in reasoning_text
        assert "strategy_type=" not in reasoning_text
        assert "fallback=rule_based" not in reasoning_text
        assert "基础信号整理" in reasoning_text
        assert "## " not in reasoning_text

    def test_FR08_free_sim_html_and_api_unified_denied(self, client, create_user, db_session, seed_report_bundle):
        seed_report_bundle()
        headers = _login_as(client, create_user, tier="Free", role="user")

        html_resp = client.get("/portfolio/sim-dashboard", headers=headers)
        assert html_resp.status_code == 403
        html_dom = _parse_html(html_resp.text)
        assert html_dom.has_selector(".cold-start-banner")
        assert html_dom.text_contains("仅对付费会员和管理员开放")
        assert html_dom.has_selector('a[href="/subscribe"]')

        api_default = client.get("/api/v1/portfolio/sim-dashboard", headers=headers, params={"capital_tier": "100k"})
        assert api_default.status_code == 403
        assert api_default.json()["error_code"] == "TIER_NOT_AVAILABLE"

        api_forbidden = client.get("/api/v1/portfolio/sim-dashboard", headers=headers, params={"capital_tier": "500k"})
        assert api_forbidden.status_code == 403
        assert api_forbidden.json()["error_code"] == "TIER_NOT_AVAILABLE"

    def test_FR09_admin_free_role_keeps_paid_area_access(self, client, create_user, db_session, seed_report_bundle):
        report = seed_report_bundle()
        admin = create_user(
            email="admin@example.com",
            password="Password123",
            tier="Free",
            role="admin",
            email_verified=True,
        )
        login = client.post("/auth/login", json={"email": admin["user"].email, "password": admin["password"]})
        assert login.status_code == 200
        headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}

        profile_resp = client.get("/profile", headers=headers)
        assert profile_resp.status_code == 200
        profile_dom = _parse_html(profile_resp.text)
        assert profile_dom.has_selector(".profile-layout")
        assert profile_dom.text_contains("基础权益")
        assert profile_dom.text_contains("管理员")

        admin_resp = client.get("/admin", headers=headers)
        assert admin_resp.status_code == 200

        sim_resp = client.get("/portfolio/sim-dashboard", headers=headers)
        assert sim_resp.status_code == 200
        sim_dom = _parse_html(sim_resp.text)
        assert sim_dom.has_selector("#tier-tabs")

        detail_resp = client.get(f"/reports/{report.report_id}", headers=headers)
        assert detail_resp.status_code == 200
        detail_dom = _parse_html(detail_resp.text)
        assert detail_dom.text_contains("123.45")
        assert not detail_dom.text_contains("完整价格需订阅")


def test_page_expectations_feature_ids_are_registry_style():
    feature_ids = {feature_id for page in PAGE_EXPECTATIONS for feature_id in page.fr_ids}
    assert "FR10-SITE-01" not in feature_ids
    assert "FR09-BILL-01" not in feature_ids
    assert "FR10-HOME-01" in feature_ids
    assert "FR10-LIST-01" in feature_ids
    assert "FR10-DETAIL-01" in feature_ids
    assert "FR10-FEATURE-01" in feature_ids
