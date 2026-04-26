"""
Structured expectations for user-facing routes and compatibility entrypoints.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PageExpectation:
    page_id: str
    route: str
    template: str
    contract_kind: str = "html_page"
    retention_mode: str = "active"
    auth_required: bool = False
    min_role: str = "anonymous"
    min_tier: str = "anonymous"
    fr_ids: list[str] = field(default_factory=list)
    must_have_selectors: list[str] = field(default_factory=list)
    forbidden_content: list[str] = field(default_factory=list)
    expected_api_calls: list[str] = field(default_factory=list)
    seed_scenario: str = "default"
    notes: str = ""
    expect_dom_reference: bool = True
    expect_browser_reference: bool = True


PAGE_EXPECTATIONS: list[PageExpectation] = [
    PageExpectation(
        page_id="home",
        route="/",
        template="index.html",
        fr_ids=["FR10-HOME-01"],
        must_have_selectors=[
            "#hero-form",
            "#hero-stock-code",
            "#home-status",
            "#market-state",
            "#market-reason",
            "#pool-size",
            "#today-report-count",
            "#pool-toggle",
            "#latest-reports",
            ".market-bar",
            ".home-hero",
            ".home-shell",
        ],
        forbidden_content=["Traceback", "Internal Server Error", "undefined", "NaN", "${", "{{", "{%"],
        expected_api_calls=["/api/v1/home", "/api/v1/pool/stocks"],
        seed_scenario="home_with_reports",
    ),
    PageExpectation(
        page_id="reports_list",
        route="/reports",
        template="reports_list.html",
        fr_ids=["FR10-LIST-01"],
        must_have_selectors=[".filter-bar", "#reports-list", ".report-row"],
        forbidden_content=["Traceback", "undefined", "NaN"],
        expected_api_calls=["/api/v1/reports"],
        seed_scenario="reports_list",
    ),
    PageExpectation(
        page_id="report_detail",
        route="/reports/{report_id}",
        template="report_view.html",
        fr_ids=["FR10-DETAIL-01"],
        must_have_selectors=[".report-hero", ".rv-conclusion", ".rv-conf-row", ".ev-grid", ".rv-feedback"],
        forbidden_content=["Traceback", "None", "undefined"],
        expected_api_calls=["/api/v1/reports/{report_id}"],
        seed_scenario="report_detail",
    ),
    PageExpectation(
        page_id="dashboard",
        route="/dashboard",
        template="dashboard.html",
        fr_ids=["FR10-BOARD-01"],
        must_have_selectors=["#window-tabs", "#dashboard-status", "#date-range", "#strategy-grid"],
        forbidden_content=["Traceback", "undefined"],
        expected_api_calls=["/api/v1/dashboard/stats"],
        seed_scenario="dashboard",
    ),
    PageExpectation(
        page_id="sim_dashboard",
        route="/portfolio/sim-dashboard",
        template="sim_dashboard.html",
        auth_required=True,
        min_tier="Pro",
        fr_ids=["FR08-SIM-01", "FR10-BOARD-02"],
        must_have_selectors=[".page-title", "#tier-tabs", "#sim-status", "#open-positions"],
        forbidden_content=["Traceback", "undefined"],
        expected_api_calls=["/api/v1/portfolio/sim-dashboard"],
        seed_scenario="sim_dashboard",
    ),
    PageExpectation(
        page_id="login",
        route="/login",
        template="login.html",
        fr_ids=["FR09-AUTH-02"],
        must_have_selectors=["form", "input[name='email']", "input[name='password']", "button[type='submit']"],
        forbidden_content=["Traceback", "undefined"],
        expected_api_calls=["/auth/login"],
        seed_scenario="auth_pages",
    ),
    PageExpectation(
        page_id="register",
        route="/register",
        template="register.html",
        fr_ids=["FR09-AUTH-01"],
        must_have_selectors=["form", "input[name='email']", "input[name='password']", "button[type='submit']"],
        forbidden_content=["Traceback", "undefined"],
        expected_api_calls=["/auth/register"],
        seed_scenario="auth_pages",
    ),
    PageExpectation(
        page_id="subscribe",
        route="/subscribe",
        template="subscribe.html",
        fr_ids=["FR09-BILLING-01", "FR09-AUTH-09"],
        must_have_selectors=["#subscribe-grid", ".collapsible", ".section-intro"],
        forbidden_content=["Traceback", "undefined", "401"],
        expected_api_calls=["/billing/create_order"],
        seed_scenario="subscribe",
    ),
    PageExpectation(
        page_id="profile",
        route="/profile",
        template="profile.html",
        auth_required=True,
        min_tier="Free",
        fr_ids=["FR09-BILLING-03", "FR09-AUTH-08"],
        must_have_selectors=[".profile-layout", ".profile-main", "#account", "#membership", "#feedback"],
        forbidden_content=["Traceback", "undefined"],
        expected_api_calls=["/profile"],
        seed_scenario="profile",
    ),
    PageExpectation(
        page_id="mock_pay_retired",
        route="/billing/mock-pay/{order_id}",
        template="json",
        contract_kind="compat_json",
        retention_mode="retired_compat",
        fr_ids=["OOS-MOCK-PAY-01"],
        must_have_selectors=["body"],
        forbidden_content=["Traceback", "undefined"],
        expected_api_calls=["/billing/mock-pay/{order_id}"],
        notes="Retired mock-pay compatibility route; tracked to avoid silent expectation gaps.",
        expect_dom_reference=False,
        expect_browser_reference=False,
    ),
    PageExpectation(
        page_id="admin",
        route="/admin",
        template="admin.html",
        auth_required=True,
        min_role="admin",
        fr_ids=["FR12-ADMIN-01", "FR12-ADMIN-02", "FR12-ADMIN-05"],
        must_have_selectors=[".admin", "#scheduler-body"],
        forbidden_content=["Traceback", "undefined"],
        expected_api_calls=["/api/v1/admin/overview"],
        seed_scenario="admin",
    ),
    PageExpectation(
        page_id="features",
        route="/features",
        template="features.html",
        auth_required=True,
        min_role="admin",
        fr_ids=["FR10-FEATURE-01"],
        must_have_selectors=[".feature-summary", ".fr-group", ".feature-card"],
        forbidden_content=["Traceback", "undefined"],
        expected_api_calls=["/api/v1/features/catalog"],
        seed_scenario="default",
    ),
    PageExpectation(
        page_id="forgot_password",
        route="/forgot-password",
        template="forgot_password.html",
        fr_ids=["FR09-AUTH-06"],
        must_have_selectors=["form", "input[name='email']"],
        forbidden_content=["Traceback", "undefined"],
        expected_api_calls=["/auth/forgot-password"],
        seed_scenario="auth_pages",
    ),
    PageExpectation(
        page_id="reset_password",
        route="/reset-password",
        template="reset_password.html",
        must_have_selectors=["form", "#password", "#password2"],
        forbidden_content=["Traceback", "undefined"],
        expected_api_calls=["/auth/reset-password"],
        seed_scenario="auth_pages",
    ),
    PageExpectation(
        page_id="terms",
        route="/terms",
        template="terms.html",
        must_have_selectors=[".legal-page", ".nav-links"],
        forbidden_content=["Traceback", "undefined"],
        seed_scenario="default",
    ),
    PageExpectation(
        page_id="privacy",
        route="/privacy",
        template="privacy.html",
        must_have_selectors=[".legal-page", ".nav-links"],
        forbidden_content=["Traceback", "undefined"],
        seed_scenario="default",
    ),
    PageExpectation(
        page_id="403",
        route="/_errors/403",
        template="403.html",
        must_have_selectors=[".error-page", ".error-code", ".error-title"],
        forbidden_content=["Traceback", "undefined"],
        seed_scenario="default",
        notes="Formal forbidden page template expectation.",
        expect_dom_reference=False,
        expect_browser_reference=False,
    ),
    PageExpectation(
        page_id="404",
        route="/_errors/404",
        template="404.html",
        must_have_selectors=[".error-page", ".error-code", ".error-title"],
        forbidden_content=["Traceback", "undefined"],
        seed_scenario="default",
        notes="Formal not-found page template expectation.",
        expect_dom_reference=False,
        expect_browser_reference=False,
    ),
    PageExpectation(
        page_id="500",
        route="/_errors/500",
        template="500.html",
        must_have_selectors=[".error-page", ".error-code", ".error-title"],
        forbidden_content=["Traceback", "undefined"],
        seed_scenario="default",
        notes="Formal internal-error page template expectation.",
        expect_dom_reference=False,
        expect_browser_reference=False,
    ),
    PageExpectation(
        page_id="report_error",
        route="/reports/{report_id}",
        template="report_error.html",
        fr_ids=["FR10-DETAIL-01"],
        must_have_selectors=[".wrap", ".card", "h2", "a[href=\"/reports\"]"],
        forbidden_content=["Traceback", "undefined"],
        expected_api_calls=["/reports/{report_id}"],
        seed_scenario="report_error",
        notes="Displayed when report generation fails.",
    ),
    PageExpectation(
        page_id="report_not_ready",
        route="/reports/{report_id}",
        template="report_not_ready.html",
        fr_ids=["FR10-DETAIL-01"],
        must_have_selectors=[".card", ".card-title"],
        forbidden_content=["Traceback", "undefined"],
        expected_api_calls=["/reports/{report_id}"],
        seed_scenario="report_not_ready",
        notes="Displayed when dependencies are not ready.",
    ),
    PageExpectation(
        page_id="report_loading",
        route="/report/{stock_code}",
        template="report_loading.html",
        fr_ids=["LEGACY-REPORT-04"],
        must_have_selectors=[".progress-bar", "#msg", "#status", "#attempt"],
        forbidden_content=["Traceback", "undefined"],
        expected_api_calls=["/report/{stock_code}/status"],
        seed_scenario="report_loading",
        notes="Legacy compatibility loading page before redirecting to canonical report detail.",
    ),
    PageExpectation(
        page_id="report_redirect",
        route="/report",
        template="redirect",
        fr_ids=["LEGACY-REPORT-04"],
        must_have_selectors=["html"],
        expected_api_calls=["/report/{stock_code}"],
        notes="Legacy redirect endpoint.",
    ),
    PageExpectation(
        page_id="report_status",
        route="/report/{stock_code}/status",
        template="json",
        fr_ids=["LEGACY-REPORT-02"],
        must_have_selectors=["body"],
        expected_api_calls=["/report/{stock_code}/status"],
        notes="Legacy compatibility status endpoint that must still return JSON.",
    ),
    PageExpectation(
        page_id="demo_index_redirect",
        route="/demo",
        template="redirect",
        fr_ids=["LEGACY-REPORT-03"],
        must_have_selectors=["html"],
        expected_api_calls=["/report/{stock_code}"],
        notes="Demo index page redirects to latest report.",
    ),
    PageExpectation(
        page_id="demo_report_redirect",
        route="/demo/report",
        template="redirect",
        fr_ids=["LEGACY-REPORT-03"],
        must_have_selectors=["html"],
        expected_api_calls=["/report/{stock_code}"],
        notes="Demo compatibility redirect endpoint.",
    ),
    PageExpectation(
        page_id="demo_report_stock_redirect",
        route="/demo/report/{stock_code}",
        template="redirect",
        fr_ids=["LEGACY-REPORT-01"],
        must_have_selectors=["html"],
        expected_api_calls=["/report/{stock_code}"],
        notes="Demo compatibility redirect endpoint with stock code path.",
    ),
    PageExpectation(
        page_id="demo_report_status",
        route="/demo/report/{stock_code}/status",
        template="json",
        fr_ids=["LEGACY-REPORT-02"],
        must_have_selectors=["body"],
        expected_api_calls=["/demo/report/{stock_code}/status"],
        notes="Demo compatibility status endpoint that must return JSON.",
    ),
    PageExpectation(
        page_id="report_cn_redirect",
        route="/report/实时研报/{stock_code}",
        template="redirect",
        fr_ids=["LEGACY-REPORT-01"],
        must_have_selectors=["html"],
        expected_api_calls=["/report/{stock_code}"],
        notes="Chinese compatibility redirect endpoint.",
    ),
]


def get_expectations_by_fr(fr_feature_id: str) -> list[PageExpectation]:
    return [p for p in PAGE_EXPECTATIONS if fr_feature_id in p.fr_ids]


def get_expectation_by_page(page_id: str) -> PageExpectation | None:
    for item in PAGE_EXPECTATIONS:
        if item.page_id == page_id:
            return item
    return None


def get_all_page_ids() -> list[str]:
    return [p.page_id for p in PAGE_EXPECTATIONS]


def get_frontend_fr_ids() -> set[str]:
    result: set[str] = set()
    for item in PAGE_EXPECTATIONS:
        result.update(item.fr_ids)
    return result
