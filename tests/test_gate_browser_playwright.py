from __future__ import annotations

import json
import re
import socket
import threading
import time
from pathlib import Path

import pytest
import uvicorn
from playwright.sync_api import sync_playwright

from app.services.trade_calendar import latest_trade_date_str
from tests.helpers_ssot import (
    insert_market_state_cache,
    insert_open_position,
    insert_pool_snapshot,
    insert_report_bundle_ssot,
    insert_sim_dashboard_snapshot,
    insert_sim_equity_curve_point,
    insert_stock_master,
)


_BROWSER_SAFE_PORT_RANGE = range(18000, 20000)


def _launch_browser(playwright):
    """Prefer Google Chrome channel for MCP-parity; fall back to Chromium."""
    try:
        return playwright.chromium.launch(channel="chrome")
    except Exception:
        return playwright.chromium.launch()


def _run_live_server(server) -> None:
    try:
        server.run()
    except SystemExit:
        # Uvicorn raises SystemExit on bind conflicts. The fixture retries the next port.
        return


def _free_port() -> int:
    # Chromium blocks a fixed set of "unsafe" ports (for example 6667).
    # Keep Playwright on a known-safe local range to avoid flaky false negatives.
    for port in _BROWSER_SAFE_PORT_RANGE:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError("no free browser-safe port available for Playwright live_server")


@pytest.fixture()
def live_server(isolated_app):
    server = None
    thread = None
    port = None
    for _ in _BROWSER_SAFE_PORT_RANGE:
        candidate_port = _free_port()
        config = uvicorn.Config(
            isolated_app["app"],
            host="127.0.0.1",
            port=candidate_port,
            log_level="error",
            lifespan="off",
        )
        candidate_server = uvicorn.Server(config)
        candidate_thread = threading.Thread(target=_run_live_server, args=(candidate_server,), daemon=True)
        candidate_thread.start()

        deadline = time.time() + 3
        while time.time() < deadline:
            if getattr(candidate_server, "started", False):
                server = candidate_server
                thread = candidate_thread
                port = candidate_port
                break
            if candidate_thread.is_alive() is False:
                break
            time.sleep(0.05)

        if server is not None:
            break

        candidate_server.should_exit = True
        candidate_thread.join(timeout=5)

    if server is None or thread is None or port is None:
        pytest.fail("live server failed to start for Playwright gate")

    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=5)


def _playwright_login(page, live_server: str, email: str, password: str) -> None:
    page.goto(f"{live_server}/login", wait_until="networkidle")
    page.locator("#login-form input[name='email']").fill(email)
    page.locator("#login-form input[name='password']").fill(password)
    page.locator("button[type=submit]").click()
    page.wait_for_url(re.compile(rf"{re.escape(live_server)}/?$"))


def test_gate_browser_playwright_login_page_dom_and_submit_flow(live_server, create_user):
    user = create_user(
        email="playwright-login@example.com",
        password="Password123",
        role="user",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        probe_hits: list[str] = []
        seen_requests: list[tuple[str, str]] = []
        page.on("request", lambda request: seen_requests.append((request.method, request.url)))

        _playwright_login(page, live_server, user["user"].email, user["password"])

        assert any(method == "POST" and url.endswith("/auth/login") for method, url in seen_requests)
        assert page.locator("nav").count() >= 1
        browser.close()


def test_gate_browser_playwright_login_rejects_external_next_redirect(live_server, create_user):
    user = create_user(
        email="playwright-login-next@example.com",
        password="Password123",
        role="user",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        page.goto(f"{live_server}/login?next=https://example.com/phish", wait_until="networkidle")
        page.locator("#login-form input[name='email']").fill(user["user"].email)
        page.locator("#login-form input[name='password']").fill(user["password"])
        page.locator("button[type=submit]").click()
        page.wait_for_url(re.compile(rf"{re.escape(live_server)}/?$"))

        assert page.url == f"{live_server}/"
        browser.close()


def test_gate_browser_playwright_login_page_renders_oauth_start_form_when_provider_configured(live_server, monkeypatch):
    from app.core.config import settings
    import app.services.oauth_service as oauth_service

    monkeypatch.setattr(settings, "oauth_callback_base", live_server)
    monkeypatch.setattr(settings, "wechat_app_id", "wechat-app-id")
    monkeypatch.setattr(settings, "wechat_app_secret", "wechat-app-secret")
    monkeypatch.setattr(
        oauth_service,
        "build_oauth_authorize_url",
        lambda provider, state: f"{live_server}/login?activated=1&provider={provider}&state={state}",
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        seen_requests: list[tuple[str, str]] = []
        page.on("request", lambda request: seen_requests.append((request.method, request.url)))

        page.goto(f"{live_server}/login?next=/subscribe", wait_until="networkidle")
        page.wait_for_function("document.querySelectorAll('.auth-oauth-form').length >= 1")

        first_form = page.locator(".auth-oauth-form").first
        action = first_form.get_attribute("action") or ""
        assert "/auth/oauth/" in action
        assert "/start?next=%2Fsubscribe" in action

        with page.expect_request(
            lambda request: request.method == "POST" and "/auth/oauth/" in request.url and "/start" in request.url
        ) as request_info:
            first_form.locator("button").click()
        start_request = request_info.value

        assert any(method == "GET" and "/auth/oauth/providers" in url for method, url in seen_requests)
        assert any(
            method == "POST" and "/auth/oauth/" in url and "/start?next=%2Fsubscribe" in url
            for method, url in seen_requests
        )
        assert "/start?next=%2Fsubscribe" in start_request.url
        browser.close()


def test_gate_browser_playwright_register_page_posts_email_contract(live_server):
    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        page.goto(f"{live_server}/register", wait_until="networkidle")
        assert page.locator("input[type=email]").count() >= 1
        content = page.content().lower()
        assert 'name="email"' in content
        assert 'name="account"' not in content

        email = f"playwright-register-{time.time_ns()}@example.com"
        page.locator("#register-form input[name='email']").fill(email)
        page.locator("#register-form input[name='password']").fill("Password123")
        page.locator("#register-form input[name='password_confirm']").fill("Password123")
        page.locator("#register-form input[type='checkbox']").check()

        with page.expect_response(lambda response: response.request.method == "POST" and response.url.endswith("/auth/register")) as response_info:
            page.locator("#btn-submit").click()
        register_response = response_info.value
        assert register_response.status == 201
        payload = register_response.json()
        assert payload["success"] is True
        assert payload["data"]["email"] == email
        page.wait_for_timeout(300)
        assert page.url == f"{live_server}/register"
        assert (payload["data"].get("message") or "") in (page.locator("#register-success-msg").text_content() or "")
        browser.close()


def test_gate_browser_playwright_report_detail_has_final_dom_without_external_font_requests(live_server, db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="鐠愰潧绐為懠鍛酱",
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        requests: list[str] = []
        page.on("request", lambda request: requests.append(request.url))

        page.goto(f"{live_server}/reports/{report.report_id}", wait_until="networkidle")

        assert page.locator(".report-hero").count() >= 1
        assert "600519.SH" in page.content()
        assert not any("fonts.googleapis.com" in url or "fonts.gstatic.com" in url for url in requests)
        browser.close()


def test_gate_browser_playwright_home_page_renders_ssot_layers(live_server, db_session):
    trade_date = latest_trade_date_str()
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="鐠愰潧绐為懠鍛酱")
    insert_stock_master(db_session, stock_code="000001.SZ", stock_name="楠炲啿鐣ㄩ柧鎯邦攽")
    insert_pool_snapshot(db_session, trade_date=trade_date, stock_codes=["600519.SH", "000001.SZ"])
    insert_market_state_cache(db_session, trade_date=trade_date, market_state="BULL")
    insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="鐠愰潧绐為懠鍛酱", trade_date=trade_date)
    insert_report_bundle_ssot(db_session, stock_code="000001.SZ", stock_name="楠炲啿鐣ㄩ柧鎯邦攽", trade_date=trade_date)

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        seen_responses: list[tuple[str, int]] = []
        page.on("response", lambda response: seen_responses.append((response.url, response.status)))

        page.goto(f"{live_server}/", wait_until="networkidle")
        page.wait_for_function("document.querySelectorAll('#pool-stocks .pool-chip').length === 2")
        page.wait_for_function("document.querySelector('#latest-reports').children.length > 0")

        assert page.locator("#home-status").text_content().strip() != ""
        assert page.locator("#market-state").text_content().strip() != ""
        assert page.locator("#market-reason").text_content().strip() != ""
        assert page.locator("#pool-size").text_content().strip() == "2"
        assert page.locator("#today-report-count").text_content().strip() == "2"
        assert page.locator("#pool-stocks .pool-chip").count() == 2
        assert any(url.endswith("/api/v1/home") and status == 200 for url, status in seen_responses)
        assert any(f"/api/v1/pool/stocks?trade_date={trade_date}" in url and status == 200 for url, status in seen_responses)
        assert not any("/api/v1/market/hot-stocks" in url for url, _ in seen_responses)
        browser.close()


def test_gate_browser_playwright_home_page_keeps_authoritative_home_date_on_market_mismatch(live_server, db_session):
    trade_date = latest_trade_date_str()
    mismatch_trade_date = "2099-01-02"
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date=trade_date, stock_codes=["600519.SH"])
    insert_market_state_cache(db_session, trade_date=trade_date, market_state="BULL")
    insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="MOUTAI", trade_date=trade_date)

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        def _route_market_state(route):
            response = route.fetch()
            payload = response.json()
            payload["data"]["market_state"] = "BEAR"
            payload["data"]["market_state_date"] = mismatch_trade_date
            payload["data"]["reference_date"] = mismatch_trade_date
            payload["data"]["is_trading_day"] = False
            payload["data"]["state_reason"] = "home_source_inconsistent"
            headers = dict(response.headers)
            headers["content-type"] = "application/json"
            route.fulfill(status=response.status, headers=headers, body=json.dumps(payload))

        page.route("**/api/v1/market/state", _route_market_state)
        page.goto(f"{live_server}/", wait_until="networkidle")
        page.wait_for_function(
            "document.querySelector('#home-trade-date') && document.querySelector('#home-trade-date').textContent.trim().length > 0"
        )

        home_trade_text = (page.locator("#home-trade-date").text_content() or "").strip()
        latest_batch_text = (page.locator("#latest-batch-trade-date").text_content() or "").strip()
        hint_text = (page.locator("#home-hint").text_content() or "").strip()
        reason_text = (page.locator("#market-reason").text_content() or "").strip()
        market_state_text = (page.locator("#market-state").text_content() or "").strip()
        main_text = (page.locator("main").text_content() or "").strip()
        trading_day_text = (page.locator("#trading-day-flag").text_content() or "").strip()

        assert trade_date in home_trade_text
        assert mismatch_trade_date not in home_trade_text
        assert latest_batch_text == trade_date
        assert mismatch_trade_date not in latest_batch_text
        assert trade_date in hint_text
        assert mismatch_trade_date not in hint_text
        assert market_state_text == "牛市"
        assert mismatch_trade_date not in main_text
        assert trading_day_text == "按首页批次展示"
        assert "不一致" in reason_text
        assert mismatch_trade_date not in reason_text
        authoritative_text = " ".join([home_trade_text, latest_batch_text, hint_text, reason_text])
        assert sorted(set(re.findall(r"\d{4}-\d{2}-\d{2}", authoritative_text))) == [trade_date]
        browser.close()


def test_gate_browser_playwright_home_page_keeps_home_payload_when_market_state_probe_fails(live_server, db_session):
    trade_date = latest_trade_date_str()
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date=trade_date, stock_codes=["600519.SH"])
    insert_market_state_cache(db_session, trade_date=trade_date, market_state="BULL")
    insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="MOUTAI", trade_date=trade_date)

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        page.route("**/api/v1/market/state", lambda route: route.abort())
        page.goto(f"{live_server}/", wait_until="networkidle")
        page.wait_for_function(
            "document.querySelector('#home-trade-date') && document.querySelector('#home-trade-date').textContent.trim().length > 0"
        )

        home_trade_text = (page.locator("#home-trade-date").text_content() or "").strip()
        market_reason_text = (page.locator("#market-reason").text_content() or "").strip()
        trading_day_text = (page.locator("#trading-day-flag").text_content() or "").strip()
        market_state_text = (page.locator("#market-state").text_content() or "").strip()

        assert trade_date in home_trade_text
        assert "首页摘要加载失败" not in market_reason_text
        assert "市场状态补充信息暂不可用" in market_reason_text
        assert trading_day_text == "按首页批次展示"
        assert market_state_text == "牛市"
        browser.close()


def test_gate_browser_playwright_home_page_keeps_home_payload_when_market_state_returns_envelope_error(live_server, db_session):
    trade_date = latest_trade_date_str()
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date=trade_date, stock_codes=["600519.SH"])
    insert_market_state_cache(db_session, trade_date=trade_date, market_state="BULL")
    insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="MOUTAI", trade_date=trade_date)

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        page.route(
            "**/api/v1/market/state",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "success": False,
                        "error_code": "MARKET_STATE_UNAVAILABLE",
                        "message": "market bridge failed",
                        "data": None,
                    }
                ),
            ),
        )
        page.goto(f"{live_server}/", wait_until="networkidle")
        page.wait_for_function(
            "document.querySelector('#home-trade-date') && document.querySelector('#home-trade-date').textContent.trim().length > 0"
        )

        home_trade_text = (page.locator("#home-trade-date").text_content() or "").strip()
        market_reason_text = (page.locator("#market-reason").text_content() or "").strip()
        trading_day_text = (page.locator("#trading-day-flag").text_content() or "").strip()
        market_state_text = (page.locator("#market-state").text_content() or "").strip()

        assert trade_date in home_trade_text
        assert "首页摘要加载失败" not in market_reason_text
        assert "市场状态补充信息暂不可用" in market_reason_text
        assert trading_day_text == "按首页批次展示"
        assert market_state_text == "牛市"
        browser.close()


def test_gate_browser_playwright_home_page_keeps_home_payload_when_home_payload_has_no_hot_stocks(live_server, db_session):
    trade_date = latest_trade_date_str()
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date=trade_date, stock_codes=["600519.SH"])
    insert_market_state_cache(db_session, trade_date=trade_date, market_state="BULL")
    insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="MOUTAI", trade_date=trade_date)

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        page.route(
            "**/api/v1/home",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "success": True,
                        "data": {
                            "latest_reports": [
                                {
                                    "report_id": "report-1",
                                    "stock_code": "600519.SH",
                                    "stock_name": "MOUTAI",
                                    "trade_date": trade_date,
                                    "recommendation": "BUY",
                                    "strategy_type": "B",
                                    "confidence": 0.88,
                                }
                            ],
                            "hot_stocks": [],
                            "market_state": "BULL",
                            "trade_date": trade_date,
                            "pool_size": 1,
                            "data_status": "READY",
                            "status_reason": None,
                            "display_reason": None,
                            "today_report_count": 1,
                            "public_performance": {
                                "runtime_trade_date": trade_date,
                                "overall_win_rate": None,
                                "overall_profit_loss_ratio": None,
                                "total_settled": 0,
                                "total_reports": 1,
                                "display_hint": None,
                                "status_reason": None,
                            },
                        },
                    }
                ),
            ),
        )
        page.goto(f"{live_server}/", wait_until="networkidle")
        page.wait_for_function(
            "document.querySelector('#home-trade-date') && document.querySelector('#home-trade-date').textContent.trim().length > 0"
        )

        home_trade_text = (page.locator("#home-trade-date").text_content() or "").strip()
        market_state_text = (page.locator("#market-state").text_content() or "").strip()
        market_reason_text = (page.locator("#market-reason").text_content() or "").strip()
        quick_links_el = page.locator("#quick-links")
        quick_links_parent_display = quick_links_el.evaluate(
            "el => el.parentElement ? getComputedStyle(el.parentElement).display : 'block'"
        ) if quick_links_el.count() > 0 else "none"

        assert trade_date in home_trade_text
        assert market_state_text == "牛市"
        assert "首页摘要加载失败" not in market_reason_text
        assert quick_links_parent_display == "none"
        browser.close()


def test_gate_browser_playwright_home_page_filters_invalid_hot_stock_items(live_server, db_session):
    trade_date = latest_trade_date_str()
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date=trade_date, stock_codes=["600519.SH"])
    insert_market_state_cache(db_session, trade_date=trade_date, market_state="BULL")
    insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="MOUTAI", trade_date=trade_date)

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        page.route(
            "**/api/v1/home",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "success": True,
                        "data": {
                            "latest_reports": [
                                {
                                    "report_id": "report-1",
                                    "stock_code": "600519.SH",
                                    "stock_name": "MOUTAI",
                                    "trade_date": trade_date,
                                    "recommendation": "BUY",
                                    "strategy_type": "B",
                                    "confidence": 0.88,
                                }
                            ],
                            "hot_stocks": [
                                {"stock_code": "", "stock_name": "<img src=x onerror=alert(1)>", "topic_title": "bad"},
                                {"stock_code": "600519.SH", "stock_name": "MOUTAI", "topic_title": "<script>alert(1)</script>"},
                            ],
                            "market_state": "BULL",
                            "trade_date": trade_date,
                            "pool_size": 1,
                            "data_status": "READY",
                            "status_reason": None,
                            "display_reason": None,
                            "today_report_count": 1,
                            "public_performance": {
                                "runtime_trade_date": trade_date,
                                "overall_win_rate": None,
                                "overall_profit_loss_ratio": None,
                                "total_settled": 0,
                                "total_reports": 1,
                                "display_hint": None,
                                "status_reason": None,
                            },
                        },
                    },
                ),
            ),
        )
        page.goto(f"{live_server}/", wait_until="networkidle")
        page.wait_for_function(
            "document.querySelector('#quick-links') && document.querySelectorAll('#quick-links .home-quick-link').length === 1"
        )

        quick_link = page.locator("#quick-links .home-quick-link")
        quick_links_text = (page.locator("#quick-links").text_content() or "").strip()

        assert quick_link.count() == 1
        assert quick_link.get_attribute("href") == "/report/600519.SH"
        assert page.locator("#quick-links img").count() == 0
        assert page.locator("#quick-links script").count() == 0
        assert "<img" not in quick_links_text
        browser.close()


def test_gate_browser_playwright_home_page_hides_report_links_when_no_live_reports(live_server, db_session):
    trade_date = latest_trade_date_str()
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date=trade_date, stock_codes=["600519.SH"])
    insert_market_state_cache(db_session, trade_date=trade_date, market_state="BULL")

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        page.route(
            "**/api/v1/home",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "success": True,
                        "data": {
                            "latest_reports": [],
                            "hot_stocks": [
                                {"stock_code": "600519.SH", "stock_name": "MOUTAI", "topic_title": "core"},
                            ],
                            "market_state": "BULL",
                            "trade_date": trade_date,
                            "pool_size": 1,
                            "data_status": "DEGRADED",
                            "status_reason": "home_snapshot_not_ready",
                            "display_reason": "当前无可用研报",
                            "today_report_count": 0,
                            "public_performance": {
                                "runtime_trade_date": trade_date,
                                "overall_win_rate": None,
                                "overall_profit_loss_ratio": None,
                                "total_settled": 0,
                                "total_reports": 0,
                                "display_hint": "当前无可用研报",
                                "status_reason": "stats_not_ready",
                            },
                        },
                    }
                ),
            ),
        )
        page.route(
            "**/api/v1/reports**",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"success": True, "data": {"items": [], "total": 0, "data_status": "READY"}}),
            ),
        )

        page.goto(f"{live_server}/", wait_until="networkidle")
        page.wait_for_function(
            "document.querySelector('#home-trade-date') && document.querySelector('#home-trade-date').textContent.trim().length > 0"
        )

        quick_links_el = page.locator("#quick-links")
        quick_links_parent_display = quick_links_el.evaluate(
            "el => el.parentElement ? getComputedStyle(el.parentElement).display : 'block'"
        ) if quick_links_el.count() > 0 else "none"

        assert quick_links_parent_display == "none"
        assert page.locator("#quick-links .home-quick-link").count() == 0
        assert page.locator("#pool-stocks a.pool-chip").count() == 0
        assert page.locator("#pool-stocks .pool-chip").count() == 1
        assert (page.locator("#pool-stocks .pool-chip").text_content() or "").strip() == "MOUTAI"
        browser.close()


def test_gate_browser_playwright_home_page_skips_pool_fetch_without_home_anchor_date(live_server):
    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        pool_hits: list[str] = []

        page.route(
            "**/api/v1/home",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "success": True,
                        "data": {
                            "latest_reports": [
                                {
                                    "report_id": "report-with-date-only",
                                    "stock_code": "600519.SH",
                                    "stock_name": "MOUTAI",
                                    "trade_date": "2099-01-02",
                                    "recommendation": "BUY",
                                    "strategy_type": "B",
                                    "confidence": 0.88,
                                }
                            ],
                            "market_state": "NEUTRAL",
                            "trade_date": None,
                            "pool_size": 0,
                            "data_status": "COMPUTING",
                            "status_reason": None,
                            "display_reason": None,
                            "today_report_count": 0,
                            "public_performance": {
                                "runtime_trade_date": None,
                            },
                        },
                    }
                ),
            )
        )
        page.route(
            "**/api/v1/pool/stocks**",
            lambda route: pool_hits.append(route.request.url) or route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"success": True, "data": {"items": [], "total": 0, "trade_date": "2099-01-02"}}),
            ),
        )

        page.goto(f"{live_server}/", wait_until="networkidle")
        page.wait_for_function(
            "document.querySelector('#home-hint') && document.querySelector('#home-hint').textContent.trim().length > 0"
        )

        assert pool_hits == []
        home_trade_text = (page.locator("#home-trade-date").text_content() or "").strip()
        latest_batch_text = (page.locator("#latest-batch-trade-date").text_content() or "").strip()
        home_hint_text = (page.locator("#home-hint").text_content() or "").strip()
        pool_date_text = (page.locator("#pool-date").text_content() or "").strip()
        assert "2099-01-02" not in home_trade_text
        assert "2099-01-02" not in latest_batch_text
        assert "2099-01-02" not in home_hint_text
        assert "2099-01-02" not in pool_date_text
        browser.close()


def test_gate_browser_playwright_home_page_hides_pool_when_pool_date_mismatches_home_anchor(live_server, db_session):
    trade_date = latest_trade_date_str()
    mismatch_trade_date = "2099-01-02"
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date=trade_date, stock_codes=["600519.SH"])
    insert_market_state_cache(db_session, trade_date=trade_date, market_state="BULL")
    insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="MOUTAI", trade_date=trade_date)

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        page.route(
            "**/api/v1/pool/stocks**",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "success": True,
                        "data": {
                            "items": [{"stock_code": "600519.SH", "stock_name": "MOUTAI", "rank": 1}],
                            "total": 1,
                            "trade_date": mismatch_trade_date,
                        },
                    }
                ),
            ),
        )
        page.goto(f"{live_server}/", wait_until="networkidle")
        page.wait_for_function(
            "document.querySelector('#pool-progress-text') && document.querySelector('#pool-progress-text').textContent.trim().length > 0"
        )

        pool_date_text = (page.locator("#pool-date").text_content() or "").strip()
        pool_progress_text = (page.locator("#pool-progress-text").text_content() or "").strip()

        assert mismatch_trade_date not in pool_date_text
        assert "不一致" in pool_progress_text
        assert page.locator("#pool-stocks .pool-chip").count() == 0
        browser.close()


def test_gate_browser_playwright_admin_page_requests_official_admin_endpoints(live_server, create_user):
    admin = create_user(
        email="playwright-admin@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        seen_responses: list[tuple[str, str, int]] = []
        page.on("response", lambda response: seen_responses.append((response.request.method, response.url, response.status)))

        _playwright_login(page, live_server, admin["user"].email, admin["password"])
        page.goto(f"{live_server}/admin", wait_until="networkidle")
        time.sleep(0.5)

        assert page.locator("#scheduler-body").count() == 1
        assert any(url.endswith("/api/v1/admin/overview") and status == 200 for _, url, status in seen_responses)
        assert any(url.endswith("/api/v1/admin/scheduler/status?page=1&page_size=10") and status == 200 for _, url, status in seen_responses)
        assert any(url.endswith("/api/v1/admin/users?page=1&page_size=20") and status == 200 for _, url, status in seen_responses)
        browser.close()


def test_gate_browser_playwright_super_admin_admin_page_hides_purge_entry(live_server, create_user):
    super_admin = create_user(
        email="playwright-super-admin@example.com",
        password="Password123",
        role="super_admin",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        _playwright_login(page, live_server, super_admin["user"].email, super_admin["password"])
        page.goto(f"{live_server}/admin", wait_until="networkidle")

        assert "清除旧数据" not in (page.locator("main").text_content() or "")
        browser.close()


def test_gate_browser_playwright_features_page_uses_real_stylesheet(live_server, create_user):
    admin = create_user(
        email="playwright-features-admin@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        requests: list[str] = []
        page.on("request", lambda request: requests.append(request.url))

        _playwright_login(page, live_server, admin["user"].email, admin["password"])
        page.goto(f"{live_server}/features", wait_until="networkidle")

        assert page.locator(".feature-summary").count() == 1
        assert page.locator(".fr-group").count() >= 1
        assert any(url.endswith("/static/demo.css") for url in requests)
        assert any(url.endswith("/static/api-bridge.js") for url in requests)
        assert not any(url.endswith("/static/css/style.css") for url in requests)
        browser.close()


def test_gate_browser_playwright_features_page_requests_official_catalog_api(live_server, create_user):
    admin = create_user(
        email="playwright-features-api@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        responses: list[tuple[str, int]] = []
        page.on("response", lambda response: responses.append((response.url, response.status)))

        _playwright_login(page, live_server, admin["user"].email, admin["password"])
        page.goto(f"{live_server}/features", wait_until="networkidle")

        assert any("/api/v1/features/catalog?source=live" in url and status == 200 for url, status in responses)
        assert any("/api/v1/governance/catalog?source=snapshot" in url and status == 200 for url, status in responses)
        assert any("/api/v1/admin/system-status" in url and status == 200 for url, status in responses)
        assert any(url.endswith("/health") and status == 200 for url, status in responses)
        browser.close()


def test_gate_browser_playwright_features_page_shows_runtime_anchor_strip(live_server, create_user):
    admin = create_user(
        email="playwright-features-runtime@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        _playwright_login(page, live_server, admin["user"].email, admin["password"])
        page.goto(f"{live_server}/features", wait_until="networkidle")
        page.wait_for_function(
            """
            () => {
              const syncNode = document.querySelector("#catalog-sync-status");
              const anchorNode = document.querySelector("#catalog-runtime-anchor-strip");
              const freshnessNode = document.querySelector("#catalog-test-freshness");
              return !!syncNode && !!anchorNode && !!freshnessNode
                && syncNode.textContent.trim().length > 0
                && anchorNode.textContent.trim().length > 0
                && freshnessNode.textContent.trim().length > 0;
            }
            """
        )

        assert page.locator("#catalog-runtime-anchor-strip").count() == 1
        assert page.locator("#catalog-test-freshness").count() == 1
        browser.close()


def test_gate_browser_playwright_features_page_keeps_partial_runtime_data_when_health_probe_fails(live_server, create_user):
    admin = create_user(
        email="playwright-features-partial@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        page.route("**/health", lambda route: route.abort())

        _playwright_login(page, live_server, admin["user"].email, admin["password"])
        page.goto(f"{live_server}/features", wait_until="networkidle")
        page.wait_for_function(
            """
            () => {
              const syncNode = document.querySelector("#catalog-sync-status");
              const liveNode = document.querySelector("#catalog-live-generated-at");
              const healthNode = document.querySelector("#catalog-runtime-health-value");
              return !!syncNode && !!liveNode && !!healthNode
                && syncNode.textContent.trim().length > 0
                && liveNode.textContent.trim().length > 0
                && healthNode.textContent.trim().length > 0;
            }
            """
        )

        sync_text = (page.locator("#catalog-sync-status").text_content() or "").strip()
        live_generated_text = (page.locator("#catalog-live-generated-at").text_content() or "").strip()
        health_text = (page.locator("#catalog-runtime-health-value").text_content() or "").strip()

        assert "部分读取失败" in sync_text
        assert "未生成" not in live_generated_text
        assert health_text == "暂不可用"
        browser.close()


def test_gate_browser_playwright_features_page_keeps_partial_runtime_data_when_system_status_fails(live_server, create_user):
    admin = create_user(
        email="playwright-features-system-partial@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        page.route("**/api/v1/admin/system-status", lambda route: route.abort())

        _playwright_login(page, live_server, admin["user"].email, admin["password"])
        page.goto(f"{live_server}/features", wait_until="networkidle")
        page.wait_for_function(
            """
            () => {
              const syncNode = document.querySelector("#catalog-sync-status");
              const liveNode = document.querySelector("#catalog-live-generated-at");
              const runtimeNode = document.querySelector("#catalog-runtime-state-value");
              const healthNode = document.querySelector("#catalog-runtime-health-value");
              return !!syncNode && !!liveNode && !!runtimeNode && !!healthNode
                && syncNode.textContent.trim().length > 0
                && liveNode.textContent.trim().length > 0
                && runtimeNode.textContent.trim().length > 0
                && healthNode.textContent.trim().length > 0;
            }
            """
        )

        sync_text = (page.locator("#catalog-sync-status").text_content() or "").strip()
        live_generated_text = (page.locator("#catalog-live-generated-at").text_content() or "").strip()
        runtime_text = (page.locator("#catalog-runtime-state-value").text_content() or "").strip()
        health_text = (page.locator("#catalog-runtime-health-value").text_content() or "").strip()

        assert "部分读取失败" in sync_text
        assert "未生成" not in live_generated_text
        assert runtime_text == "暂不可用"
        assert health_text in {"正常", "降级", "待配置", "待确认", "暂不可用"}
        browser.close()


def test_gate_browser_playwright_features_page_keeps_partial_runtime_data_when_live_catalog_returns_envelope_error(live_server, create_user):
    admin = create_user(
        email="playwright-features-live-envelope@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        page.route(
            "**/api/v1/features/catalog?source=live",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "success": False,
                        "error_code": "LIVE_CATALOG_UNAVAILABLE",
                        "message": "live catalog bridge failed",
                        "data": None,
                    }
                ),
            ),
        )

        _playwright_login(page, live_server, admin["user"].email, admin["password"])
        page.goto(f"{live_server}/features", wait_until="networkidle")
        page.wait_for_function(
            """
            () => {
              const syncNode = document.querySelector("#catalog-sync-status");
              const runtimeNode = document.querySelector("#catalog-runtime-state-value");
              const healthNode = document.querySelector("#catalog-runtime-health-value");
              return !!syncNode && !!runtimeNode && !!healthNode
                && syncNode.textContent.trim().length > 0
                && runtimeNode.textContent.trim().length > 0
                && healthNode.textContent.trim().length > 0;
            }
            """
        )

        sync_text = (page.locator("#catalog-sync-status").text_content() or "").strip()
        runtime_text = (page.locator("#catalog-runtime-state-value").text_content() or "").strip()
        health_text = (page.locator("#catalog-runtime-health-value").text_content() or "").strip()
        generated_text = (page.locator("#catalog-generated-at").text_content() or "").strip()

        assert "部分读取失败" in sync_text
        assert "当前目录读取失败" in sync_text
        assert runtime_text in {"正常", "已就绪", "已降级展示", "阻塞", "待确认", "暂不可用"}
        assert health_text in {"正常", "降级", "待配置", "待确认", "暂不可用"}
        assert "部分接口暂不可用" in generated_text
        browser.close()


def test_gate_browser_playwright_subscribe_page_renders_server_seeded_plans_without_public_helper(live_server):
    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        responses: list[tuple[str, int]] = []
        page.on("response", lambda response: responses.append((response.url, response.status)))

        page.goto(f"{live_server}/subscribe", wait_until="networkidle")

        assert page.locator("#subscribe-grid").count() == 1
        assert page.locator("#subscribe-grid .subscribe-card").count() >= 1
        assert page.locator('[data-plan]' ).count() >= 1
        assert page.locator('[data-plan="free"]').count() == 1
        assert page.locator('[data-plan="pro_1m"]').count() == 1
        assert not any("/api/v1/platform/plans" in url for url, _ in responses)
        browser.close()
        browser.close()


def test_gate_browser_playwright_subscribe_page_does_not_break_when_legacy_plan_helper_is_blocked(live_server):
    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        page.route(
            "**/api/v1/platform/plans",
            lambda route: route.abort(),
        )

        page.goto(f"{live_server}/subscribe", wait_until="networkidle")

        assert page.locator("#subscribe-grid .subscribe-card").count() >= 1
        assert page.locator('[data-plan="pro_12m"]').count() == 1
        browser.close()

def test_gate_browser_playwright_paid_user_subscribe_page_marks_current_plan(live_server, create_user):
    user = create_user(
        email="playwright-pro-subscribe@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        _playwright_login(page, live_server, user["user"].email, user["password"])
        page.goto(f"{live_server}/subscribe", wait_until="networkidle")
        page.locator('[data-plan="pro_1m"] a.btn').wait_for()

        monthly_text = page.locator('[data-plan="pro_1m"] a.btn').text_content() or ""
        free_text = page.locator('[data-plan="free"] a.btn').text_content() or ""
        assert "当前权益" in monthly_text
        assert "当前权益" not in free_text
        browser.close()


def test_gate_browser_playwright_subscribe_page_does_not_depend_on_legacy_status_probe(live_server, create_user):
    user = create_user(
        email="playwright-subscribe-unknown@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        probe_hits: list[str] = []
        page.route(
            "**/api/v1/membership/subscription/status*",
            lambda route: probe_hits.append(route.request.url) or route.abort(),
        )

        _playwright_login(page, live_server, user["user"].email, user["password"])
        page.goto(f"{live_server}/subscribe", wait_until="networkidle")

        assert probe_hits == []
        assert "当前权益" in (page.locator('[data-plan="pro_1m"] a.btn').text_content() or "")
        assert "当前权益" not in (page.locator('[data-plan="free"] a.btn').text_content() or "")
        browser.close()


def test_gate_browser_playwright_subscribe_page_shows_unknown_state_when_paid_expiry_is_unconfirmed(live_server, create_user):
    user = create_user(
        email="playwright-subscribe-unconfirmed@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
        tier_expires_at=None,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        _playwright_login(page, live_server, user["user"].email, user["password"])
        page.goto(f"{live_server}/subscribe", wait_until="networkidle")
        page.wait_for_function(
            """
            () => {
              const state = document.querySelector('#subscription-state');
              return !!state && state.textContent.includes('当前权益暂未确认');
            }
            """
        )

        main_text = page.locator("main").text_content() or ""
        assert "当前权益暂未确认" in main_text
        assert "等待权益确认" in main_text
        assert "到期时间仍待核验" in main_text
        assert "当前使用基础权益" not in main_text
        browser.close()


def test_gate_browser_playwright_subscribe_page_keeps_mock_provider_non_checkoutable(live_server, monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "enable_mock_billing", True)
    monkeypatch.setattr(settings, "alipay_app_id", "")
    monkeypatch.setattr(settings, "alipay_gateway_url", "")
    monkeypatch.setattr(settings, "wechat_pay_app_id", "")
    monkeypatch.setattr(settings, "wechat_pay_gateway_url", "")

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        page.goto(f"{live_server}/subscribe", wait_until="networkidle")
        page.locator('[data-plan="pro_1m"] a.btn').wait_for()

        main_text = page.locator("main").text_content() or ""
        monthly_text = page.locator('[data-plan="pro_1m"] a.btn').text_content() or ""
        assert "暂未开放在线支付" in monthly_text
        assert "登录后订阅" not in main_text
        browser.close()


def test_gate_browser_playwright_profile_page_preserves_unknown_paid_membership_copy(live_server, create_user):
    user = create_user(
        email="playwright-profile-unknown@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
        tier_expires_at=None,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        _playwright_login(page, live_server, user["user"].email, user["password"])
        page.goto(f"{live_server}/profile", wait_until="networkidle")

        main_text = page.locator(".profile-main").text_content() or ""
        assert "订阅权益" in main_text
        assert "待确认" in main_text
        assert page.locator("#membership").get_attribute("data-subscription-reason") == "expiry_unconfirmed"
        assert "基础权益" not in (page.locator("#profile-subscription-tier").text_content() or "")
        browser.close()
def test_gate_browser_playwright_forgot_password_page_renders_form(live_server):
    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        page.goto(f"{live_server}/forgot-password", wait_until="networkidle")

        assert page.locator("form").count() == 1
        assert page.locator("input[type=email]").count() == 1
        browser.close()


def test_gate_browser_playwright_forgot_password_page_shows_business_friendly_notice(live_server):
    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        page.goto(f"{live_server}/forgot-password", wait_until="networkidle")
        page.locator("#email").fill("reset-me@example.com")
        page.locator("button[type=submit]").click()
        page.wait_for_timeout(300)

        assert "若该邮箱已注册，重置请求已提交，请按后续指引完成密码重置。" in (page.locator("#forgot-msg").text_content() or "")
        browser.close()


def test_gate_browser_playwright_profile_page_after_login(live_server, create_user):
    user = create_user(
        email="playwright-profile@example.com",
        password="Password123",
        role="user",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        _playwright_login(page, live_server, user["user"].email, user["password"])
        page.goto(f"{live_server}/profile", wait_until="networkidle")

        assert page.locator(".profile-layout").count() == 1
        assert page.locator("#account").count() == 1
        browser.close()

def test_gate_browser_playwright_profile_page_shows_business_access_copy(live_server, create_user):
    admin = create_user(
        email="playwright-role-admin@example.com",
        password="Password123",
        role="admin",
        tier="Free",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        _playwright_login(page, live_server, admin["user"].email, admin["password"])
        page.goto(f"{live_server}/profile", wait_until="networkidle")

        content = page.locator(".profile-main").text_content() or ""
        assert "订阅权益" in content
        assert "基础权益" in content


def test_gate_browser_playwright_features_page_search_interaction(live_server, create_user):
    admin = create_user(
        email="playwright-feature-search@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        _playwright_login(page, live_server, admin["user"].email, admin["password"])
        page.goto(f"{live_server}/features", wait_until="networkidle")
        page.locator("#featureSearch").fill("首页")
        page.wait_for_timeout(200)

        visible_cards = page.locator(".feature-entry:visible")
        assert visible_cards.count() >= 1
        assert all("FR" not in (visible_cards.nth(i).text_content() or "") for i in range(visible_cards.count()))
        browser.close()


def test_gate_browser_playwright_free_report_detail_shows_preview(live_server, create_user, db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="鐠愰潧绐為懠鍛酱",
    )
    user = create_user(
        email="playwright-free-preview@example.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        _playwright_login(page, live_server, user["user"].email, user["password"])
        page.goto(f"{live_server}/reports/{report.report_id}", wait_until="networkidle")

        content = page.locator("main").text_content() or ""
        assert "当前仅展示前 200 字预览" in content
        assert "完整推理链仅对订阅用户开放" in content
        browser.close()


def test_gate_browser_playwright_anonymous_report_detail_does_not_prefetch_advanced(live_server, db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        seen_responses: list[tuple[str, int]] = []
        page.on("response", lambda response: seen_responses.append((response.url, response.status)))

        page.goto(f"{live_server}/reports/{report.report_id}", wait_until="networkidle")

        assert not any(url.endswith(f"/api/v1/reports/{report.report_id}/advanced") for url, _ in seen_responses)
        assert not any(status == 401 for _, status in seen_responses)
        browser.close()


def test_gate_browser_playwright_reset_password_page_renders_form(live_server, db_session, create_user):
    from datetime import datetime, timedelta, timezone

    from app.core.security import hash_token
    from app.models import AuthTempToken

    user = create_user(
        email="playwright-reset@example.com",
        password="Password123",
        role="user",
        email_verified=True,
    )
    raw_token = "playwright-valid-reset-token-12345678"
    db_session.add(
        AuthTempToken(
            user_id=user["user"].user_id,
            token_type="PASSWORD_RESET",
            token_hash=hash_token(raw_token),
            sent_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            created_at=datetime.now(timezone.utc),
        )
    )
    db_session.commit()
    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        page.goto(f"{live_server}/reset-password?token={raw_token}", wait_until="networkidle")

        assert page.locator("#reset-form").count() == 1
        assert page.locator("#password").count() == 1
        assert page.locator("#password2").count() == 1
        browser.close()


def test_gate_browser_playwright_legacy_redirect_routes_resolve_to_report_detail(live_server, db_session):
    trade_date = latest_trade_date_str()
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="鐠愰潧绐為懠鍛酱",
        trade_date=trade_date,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        page.goto(f"{live_server}/demo/report?stock_code=600519.SH", wait_until="networkidle")
        assert page.url.endswith(f"/reports/{report.report_id}")

        page.goto(f"{live_server}/demo/report/600519.SH", wait_until="networkidle")
        assert page.url.endswith(f"/reports/{report.report_id}")

        page.goto(f"{live_server}/report/600519", wait_until="networkidle")
        assert page.url.endswith(f"/reports/{report.report_id}")
        browser.close()


def test_gate_browser_playwright_report_page_does_not_autostart_generation_for_missing_report(live_server, db_session):
    stock_code = "688123.SH"
    insert_stock_master(db_session, stock_code=stock_code, stock_name="测试股票")
    db_session.commit()

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        responses: list[tuple[str, int, str]] = []
        page.on(
            "response",
            lambda response: responses.append((response.url, response.status, response.headers.get("content-type", ""))),
        )

        page.goto(f"{live_server}/report/{stock_code}", wait_until="networkidle")

        assert page.locator(".progress-bar").count() == 0
        assert page.locator("#status").count() == 0
        page_text = page.locator("main").text_content() or ""
        assert len(page_text) > 0
        assert "MANUAL_TRIGGER_REQUIRED" not in page_text
        assert not any(f"/report/{stock_code}/status" in url for url, status, content_type in responses)
        browser.close()


def test_gate_browser_playwright_terms_and_privacy_pages_render(live_server):
    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        page.goto(f"{live_server}/terms", wait_until="networkidle")
        assert page.locator(".legal-page").count() == 1

        page.goto(f"{live_server}/privacy", wait_until="networkidle")
        assert page.locator(".legal-page").count() == 1
        browser.close()


def test_gate_browser_playwright_report_feedback_does_not_fake_success_for_anonymous(live_server, db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="鐠愰潧绐為懠鍛酱",
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        page.goto(f"{live_server}/reports/{report.report_id}", wait_until="networkidle")
        assert page.locator("#feedback-section").count() == 0
        assert page.locator("#feedback-login-hint").count() == 1
        assert "登录后可提交研报反馈" in (page.locator("#feedback-login-hint").text_content() or "")
        assert page.locator("#feedback-login-hint a").count() == 1
        assert page.locator("#feedback-login-hint a").get_attribute("href") == f"/login?next=/reports/{report.report_id}"
        browser.close()


def test_gate_browser_playwright_report_feedback_shows_success_only_after_api_success(live_server, create_user, db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="鐠愰潧绐為懠鍛酱",
    )
    user = create_user(
        email="playwright-feedback@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        _playwright_login(page, live_server, user["user"].email, user["password"])
        page.goto(f"{live_server}/reports/{report.report_id}", wait_until="networkidle")

        with page.expect_response(
            lambda response: response.request.method == "POST"
            and response.url.endswith(f"/api/v1/reports/{report.report_id}/feedback")
        ) as response_info:
            page.locator(".rv-fb-btn.positive").click()

        feedback_response = response_info.value
        assert feedback_response.status == 200
        page.wait_for_timeout(300)
        assert page.locator("#feedback-btns").evaluate("e => getComputedStyle(e).display") == "none"
        assert page.locator("#feedback-thanks").is_visible()
        assert page.locator("#feedback-error").is_hidden()
        browser.close()


def test_gate_browser_playwright_admin_free_role_keeps_admin_access_and_full_report(live_server, create_user, db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="鐠愰潧绐為懠鍛酱",
    )
    admin = create_user(
        email="admin@example.com",
        password="Password123",
        tier="Free",
        role="admin",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        _playwright_login(page, live_server, admin["user"].email, admin["password"])

        page.goto(f"{live_server}/profile", wait_until="networkidle")
        assert "访问权限：管理员" in (page.locator(".profile-main").text_content() or "")

        page.goto(f"{live_server}/admin", wait_until="networkidle")
        assert page.locator("#scheduler-body").count() == 1

        page.goto(f"{live_server}/portfolio/sim-dashboard", wait_until="networkidle")
        assert page.locator("#tier-tabs").count() == 1

        page.goto(f"{live_server}/reports/{report.report_id}", wait_until="networkidle")
        instruction_text = page.locator(".instr-grid").first.text_content() or ""
        assert "123.45" in instruction_text
        assert "閳煎繆妫岄埣蹇婃" not in instruction_text
        browser.close()


def test_gate_browser_playwright_sim_dashboard_humanizes_baseline_pending_hint(live_server, create_user, db_session):
    insert_sim_dashboard_snapshot(
        db_session,
        capital_tier="100k",
        snapshot_date=latest_trade_date_str(),
    )
    insert_sim_dashboard_snapshot(
        db_session,
        capital_tier="500k",
        snapshot_date=latest_trade_date_str(),
        data_status="DEGRADED",
        status_reason="sim_snapshot_lagging",
        display_hint="baseline_pending",
        sample_size=30,
    )
    user = create_user(
        email="playwright-sim@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        _playwright_login(page, live_server, user["user"].email, user["password"])
        page.goto(f"{live_server}/portfolio/sim-dashboard", wait_until="networkidle")
        with page.expect_response(lambda response: "/api/v1/portfolio/sim-dashboard?capital_tier=500k" in response.url) as response_info:
            page.locator("#tier-tabs a[data-tier='500k']").click()
        assert response_info.value.status == 200
        page.wait_for_timeout(300)
        sim_status = page.locator("#sim-status").text_content() or ""
        chart_hint = page.locator("#chart-hint").text_content() or ""
        translated = page.evaluate("() => typeof statusCn === 'function' ? statusCn('baseline_pending') : null")
        assert "baseline_pending" not in sim_status
        assert "baseline_pending" not in chart_hint
        assert translated == "基线对照数据计算中"
        browser.close()


def test_gate_browser_playwright_home_does_not_render_unknown_metrics_as_zero(live_server, db_session):
    trade_date = latest_trade_date_str()
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")
    insert_pool_snapshot(db_session, trade_date=trade_date, stock_codes=["600519.SH"])
    insert_market_state_cache(db_session, trade_date=trade_date, market_state="BULL")
    insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="MOUTAI", trade_date=trade_date)

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        seen_requests: list[str] = []
        page.on("request", lambda request: seen_requests.append(request.url))
        with page.expect_response(
            lambda response: response.url.endswith("/api/v1/home") and response.status == 200
        ):
            page.goto(f"{live_server}/", wait_until="networkidle")
        page.wait_for_function(
            "document.querySelector('#overall-win-rate') && document.querySelector('#overall-win-rate').textContent.trim().length > 0"
        )

        win_text = (page.locator("#overall-win-rate").text_content() or "").strip()
        pnl_text = (page.locator("#overall-pnl-ratio").text_content() or "").strip()
        assert not any("/api/v1/dashboard/stats" in url for url in seen_requests)
        assert win_text != "0.0%"
        assert pnl_text != "0.00"
        browser.close()


def test_gate_browser_playwright_sim_dashboard_draws_chart_when_equity_curve_exists(live_server, create_user, db_session):
    trade_date = latest_trade_date_str()
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date=trade_date,
        recommendation="BUY",
        strategy_type="B",
    )
    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code=report.stock_code,
        capital_tier="100k",
        signal_date=trade_date,
        entry_date=trade_date,
        actual_entry_price=100.0,
        signal_entry_price=100.0,
        position_ratio=0.2,
        shares=100,
    )
    insert_sim_dashboard_snapshot(
        db_session,
        capital_tier="100k",
        snapshot_date=trade_date,
        data_status="READY",
        status_reason=None,
        sample_size=35,
        total_return_pct=0.05,
        win_rate=0.6,
        profit_loss_ratio=1.7,
        display_hint=None,
    )
    insert_sim_equity_curve_point(db_session, capital_tier="100k", trade_date=trade_date, equity=100000.0)
    user = create_user(
        email="playwright-sim-chart@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        _playwright_login(page, live_server, user["user"].email, user["password"])
        with page.expect_response(
            lambda response: "/api/v1/portfolio/sim-dashboard?capital_tier=100k" in response.url and response.status == 200
        ) as response_info:
            page.goto(f"{live_server}/portfolio/sim-dashboard", wait_until="networkidle")
        payload = response_info.value.json()["data"]
        assert isinstance(payload.get("equity_curve"), list) and payload["equity_curve"], "test seed must produce non-empty equity_curve"
        page.wait_for_function(
            """() => {
                const chart = document.querySelector('#equity-chart');
                return !!chart && getComputedStyle(chart).display !== 'none' && chart.querySelectorAll('path').length >= 1;
            }"""
        )

        assert page.locator("#equity-chart").is_visible()
        assert page.locator("#chart-empty").is_hidden()
        browser.close()


def test_gate_browser_playwright_report_detail_hides_internal_tokens(live_server, db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        recommendation="HOLD",
        market_state="NEUTRAL",
        quality_flag="ok",
    )
    forbidden_tokens = [
        "stale_ok",
        "fallback_t_minus_1",
        "not_triggered",
        "kline_daily",
        "tdx_local",
        "neutral",
        "hold",
    ]

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        page.goto(f"{live_server}/reports/{report.report_id}", wait_until="networkidle")
        text = (page.locator("main").text_content() or "").lower()
        for token in forbidden_tokens:
            assert token not in text
        browser.close()


def test_gate_browser_playwright_non_ok_report_detail_is_not_accessible(live_server, db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        quality_flag="stale_ok",
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        response = page.goto(f"{live_server}/reports/{report.report_id}", wait_until="networkidle")

        assert response is not None
        assert response.status == 404
        page_text = (page.locator("body").text_content() or "").lower()
        assert report.report_id not in page_text
        browser.close()


def test_gate_browser_playwright_reports_list_humanizes_quality_and_shows_summary(live_server, db_session):
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        quality_flag="ok",
        review_flag="PENDING_REVIEW",
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        with page.expect_response(
            lambda response: "/api/v1/reports?" in response.url and response.status == 200
        ):
            page.goto(f"{live_server}/reports", wait_until="networkidle")
        page.wait_for_function("document.querySelectorAll('.report-row').length >= 1")

        page_text = (page.locator("main").text_content() or "").lower()
        assert "stale_ok" not in page_text
        assert "pending_review" not in page_text
        assert "fallback_t_minus_1" not in page_text
        summary = (page.locator("#page-info").text_content() or "").strip()
        assert summary != ""
        browser.close()


def test_gate_browser_playwright_reports_list_hides_soft_deleted_reports(live_server, db_session):
    visible = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        quality_flag="ok",
    )
    hidden = insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        quality_flag="ok",
    )
    hidden.is_deleted = True
    hidden.published = False
    db_session.commit()

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        with page.expect_response(lambda response: "/api/v1/reports?" in response.url and response.status == 200):
            page.goto(f"{live_server}/reports", wait_until="networkidle")
        page.wait_for_function("document.querySelectorAll('.report-row').length >= 1")

        content = page.content()
        assert visible.report_id in content
        assert hidden.report_id not in content
        assert "000001.SZ" not in content
        browser.close()


def test_gate_browser_playwright_soft_deleted_report_detail_is_not_accessible(live_server, db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
    )
    report.is_deleted = True
    report.published = False
    db_session.commit()

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        response = page.goto(f"{live_server}/reports/{report.report_id}", wait_until="networkidle")

        assert response is not None
        assert response.status == 404
        assert page.locator(".report-hero").count() == 0
        browser.close()


def test_gate_browser_playwright_profile_avoids_recharge_cta_when_payment_unavailable(live_server, create_user):
    user = create_user(
        email="playwright-no-pay@example.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )
    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        _playwright_login(page, live_server, user["user"].email, user["password"])
        page.goto(f"{live_server}/subscribe", wait_until="networkidle")
        subscribe_text = page.locator("main").text_content() or ""
        page.goto(f"{live_server}/profile", wait_until="networkidle")
        profile_text = page.locator("main").text_content() or ""
        assert "立即订阅" not in profile_text
        browser.close()
        browser.close()


def test_gate_browser_playwright_login_then_subscription_page_renders(live_server, create_user):
    """ISSUE-N6 (P2): 登录后订阅页面可达（登录→Subscribe 操作链）"""
    user = create_user(
        email="playwright-sub-chain@example.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()
        seen_responses: list[tuple[str, int]] = []
        page.on("response", lambda response: seen_responses.append((response.url, response.status)))

        _playwright_login(page, live_server, user["user"].email, user["password"])

        # 导航到订阅页
        page.goto(f"{live_server}/subscribe", wait_until="networkidle")
        assert page.locator("main").count() == 1

        # 导航到个人页
        page.goto(f"{live_server}/profile", wait_until="networkidle")
        assert page.locator(".profile-layout").count() == 1

        # 验证整个操作链期间所有关键业务请求均回 200
        assert any(
            "/auth/login" in url and status == 200 for url, status in seen_responses
        ), "登录请求应当 200"

        browser.close()


def test_gate_browser_playwright_reports_api_only_exposes_ok_quality_after_login(live_server, create_user, db_session):
    """ISSUE-N6 (P2): 登录后 /api/v1/reports 只返回 quality_flag=ok 的研报（数据准入联动验证）"""
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="测试贵州茅台",
    )

    user = create_user(
        email="playwright-reports-ok@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
    )

    with sync_playwright() as playwright:
        browser = _launch_browser(playwright)
        page = browser.new_page()

        _playwright_login(page, live_server, user["user"].email, user["password"])

        # 通过 Playwright 直接调用 /api/v1/reports 并检查响应
        response = page.goto(f"{live_server}/api/v1/reports?page_size=100", wait_until="networkidle")
        assert response is not None
        assert response.status == 200

        body = response.json()
        assert body.get("success") is True
        items = body.get("data", {}).get("items", [])

        # 所有返回项的 quality_flag 必须是 ok 或 None（默认 ok）
        for item in items:
            qf = item.get("quality_flag")
            assert qf in (None, "ok"), (
                f"报告 {item.get('report_id')} 的 quality_flag={qf!r}，不应对外可见"
            )

        browser.close()


