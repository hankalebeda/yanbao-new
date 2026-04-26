#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx
from playwright.sync_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

BASE_URL = "http://127.0.0.1:8099"
DEFAULT_DB_PATH = Path("data/app.db")
AUTH_COOKIE = "access_token"
RUN_VERSION = "v711"
DEFAULT_TIMEOUT_MS = 15_000
SAFE_PAUSE_MS = 900

DEFAULT_ROLE_CREDENTIAL_ENV = {
    "admin": ("BROWSER_AUDIT_ADMIN_EMAIL", "BROWSER_AUDIT_ADMIN_PASSWORD", "Free", "admin"),
    "pro": ("BROWSER_AUDIT_PRO_EMAIL", "BROWSER_AUDIT_PRO_PASSWORD", "Pro", "user"),
    "free": ("BROWSER_AUDIT_FREE_EMAIL", "BROWSER_AUDIT_FREE_PASSWORD", "Free", "user"),
}


def utc_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_space(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_price_text(value: Any) -> str:
    return normalize_space(value).replace("￥", "¥")


def normalize_int_text(value: Any) -> str:
    return re.sub(r"[^\d-]", "", normalize_space(value))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="True browser audit with Playwright + DB/API reconciliation")
    parser.add_argument("--base-url", default=BASE_URL)
    parser.add_argument("--run-label", default="round1")
    parser.add_argument("--headed", action="store_true", help="Run Chromium in headed mode")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--allow-live-db", action="store_true", help="Explicitly allow reading the live runtime DB path")
    parser.add_argument("--allow-destructive-auth-flows", action="store_true", help="Run register / forgot-password flows that mutate runtime state")
    return parser.parse_args()


def load_role_credentials() -> dict[str, dict[str, str]]:
    creds: dict[str, dict[str, str]] = {}
    missing: list[str] = []
    for role, (email_key, password_key, membership_tier, role_name) in DEFAULT_ROLE_CREDENTIAL_ENV.items():
        email = (os.getenv(email_key) or "").strip()
        password = (os.getenv(password_key) or "").strip()
        if not email or not password:
            missing.append(f"{email_key}/{password_key}")
            continue
        creds[role] = {
            "email": email,
            "password": password,
            "membership_tier": membership_tier,
            "role": role_name,
        }
    if missing:
        raise RuntimeError("Missing browser audit credentials: " + ", ".join(missing))
    return creds


class AuditRun:
    def __init__(self, base_url: str, run_label: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.run_label = run_label
        today = date.today().isoformat()
        self.root_dir = Path("docs/_temp")
        self.run_dir = self.root_dir / f"{today}_browser_audit_{RUN_VERSION}_{run_label}"
        self.screenshot_dir = self.run_dir / "screenshots"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.screenshot_dir.mkdir(parents=True, exist_ok=True)
        self.started_at = utc_now()
        self.issues: list[dict[str, Any]] = []
        self.non_defects: list[dict[str, Any]] = []
        self.page_results: list[dict[str, Any]] = []
        self.cross_checks: list[dict[str, Any]] = []
        self.summary: dict[str, Any] = {}

    def add_issue(
        self,
        *,
        issue_id: str,
        severity: str,
        module: str,
        page: str,
        description: str,
        expected: Any = None,
        actual: Any = None,
        root_cause: str = "query/formatting defect",
        evidence: str | None = None,
    ) -> None:
        self.issues.append(
            {
                "issue_id": issue_id,
                "severity": severity,
                "module": module,
                "page": page,
                "description": description,
                "expected": expected,
                "actual": actual,
                "root_cause": root_cause,
                "evidence": evidence,
            }
        )

    def add_non_defect(
        self,
        *,
        item_id: str,
        module: str,
        page: str,
        description: str,
        reason: str,
        evidence: str | None = None,
    ) -> None:
        self.non_defects.append(
            {
                "item_id": item_id,
                "module": module,
                "page": page,
                "description": description,
                "reason": reason,
                "evidence": evidence,
            }
        )

    def add_cross_check(
        self,
        *,
        check_id: str,
        metric: str,
        status: str,
        expected: Any,
        actual: Any,
        source_chain: str,
        root_cause: str = "",
    ) -> None:
        self.cross_checks.append(
            {
                "check_id": check_id,
                "metric": metric,
                "status": status,
                "expected": expected,
                "actual": actual,
                "source_chain": source_chain,
                "root_cause": root_cause,
            }
        )

    def screenshot(self, page: Page, name: str) -> str:
        target = self.screenshot_dir / f"{name}.png"
        page.screenshot(path=str(target), full_page=True)
        return str(target).replace("\\", "/")

    def write_reports(self) -> tuple[Path, Path]:
        self.summary = {
            "started_at": self.started_at,
            "finished_at": utc_now(),
            "issue_count": len(self.issues),
            "non_defect_count": len(self.non_defects),
            "page_count": len(self.page_results),
            "cross_check_count": len(self.cross_checks),
        }
        payload = {
            "version": RUN_VERSION,
            "base_url": self.base_url,
            "run_label": self.run_label,
            "summary": self.summary,
            "issues": self.issues,
            "non_defects": self.non_defects,
            "cross_checks": self.cross_checks,
            "pages": self.page_results,
        }
        json_path = self.run_dir / "summary.json"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        lines = [
            f"# 浏览器真巡检报告 {RUN_VERSION} / {self.run_label}",
            "",
            f"- 运行地址: `{self.base_url}`",
            f"- 开始时间: `{self.started_at}`",
            f"- 结束时间: `{self.summary['finished_at']}`",
            f"- 页面数: `{len(self.page_results)}`",
            f"- 缺陷数: `{len(self.issues)}`",
            f"- 非缺陷说明数: `{len(self.non_defects)}`",
            f"- 对账项数: `{len(self.cross_checks)}`",
            "",
            "## 发现的问题",
        ]
        if self.issues:
            for item in self.issues:
                lines.extend(
                    [
                        f"- `{item['issue_id']}` [{item['severity']}] {item['page']} / {item['module']}: {item['description']}",
                        f"  期望: `{item['expected']}` | 实际: `{item['actual']}` | 根因分类: `{item['root_cause']}`",
                    ]
                )
                if item.get("evidence"):
                    lines.append(f"  证据: `{item['evidence']}`")
        else:
            lines.append("- 本轮未发现新的页面级代码缺陷。")

        lines.extend(["", "## 非缺陷 / 口径说明"])
        if self.non_defects:
            for item in self.non_defects:
                lines.append(
                    f"- `{item['item_id']}` {item['page']} / {item['module']}: {item['description']} | 归因: `{item['reason']}`"
                )
        else:
            lines.append("- 本轮未记录新的非缺陷说明。")

        lines.extend(["", "## 跨页对账"])
        if self.cross_checks:
            for item in self.cross_checks:
                lines.append(
                    f"- `{item['check_id']}` {item['metric']}: `{item['status']}` | 期望 `{item['expected']}` | 实际 `{item['actual']}` | 链路 `{item['source_chain']}`"
                )
        else:
            lines.append("- 本轮未执行对账。")

        lines.extend(["", "## 页面记录"])
        for page_result in self.page_results:
            lines.append(
                f"- `{page_result['role']}` {page_result['page']} => status `{page_result['status']}` | actions `{len(page_result['actions'])}` | console `{len(page_result['console_errors'])}` | network `{len(page_result['request_failures'])}` | screenshot `{page_result.get('screenshot', '')}`"
            )

        md_path = self.run_dir / "summary.md"
        md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return json_path, md_path


def api_login(client: httpx.Client, *, base_url: str, email: str, password: str) -> tuple[str, dict[str, Any]]:
    response = client.post(f"{base_url}/auth/login", json={"email": email, "password": password})
    response.raise_for_status()
    body = response.json()
    data = body.get("data", {})
    return str(data["access_token"]), data


def api_get(client: httpx.Client, base_url: str, path: str, *, token: str | None = None) -> tuple[dict[str, Any], httpx.Response]:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    response = client.get(f"{base_url}{path}", headers=headers)
    response.raise_for_status()
    body = response.json()
    return body.get("data", body), response


def latest_trade_date_from_runtime() -> str:
    try:
        from app.services.trade_calendar import latest_trade_date_str

        return latest_trade_date_str()
    except Exception:
        return date.today().isoformat()


def get_db_truth(report_id: str, db_path: Path) -> dict[str, Any]:
    from app.core.db import SessionLocal
    from app.services.stock_pool import get_public_pool_view

    session = SessionLocal()
    try:
        pool_view = get_public_pool_view(session)
        latest_pool_date = pool_view.task.trade_date.isoformat() if pool_view else None
        pool_size = len(pool_view.core_rows) if pool_view else 0
    finally:
        session.close()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    latest_trade_date = latest_trade_date_from_runtime()
    today_reports = cur.execute(
        "SELECT COUNT(*) FROM report WHERE trade_date = ? AND is_deleted = 0",
        (latest_trade_date,),
    ).fetchone()[0]
    today_buy_signals = cur.execute(
        "SELECT COUNT(*) FROM report WHERE trade_date = ? AND recommendation = 'BUY' AND confidence >= 0.65 AND is_deleted = 0",
        (latest_trade_date,),
    ).fetchone()[0]
    pending_review = cur.execute(
        "SELECT COUNT(*) FROM report WHERE review_flag = 'PENDING_REVIEW' AND is_deleted = 0"
    ).fetchone()[0]
    latest_kline_date = cur.execute("SELECT MAX(trade_date) FROM kline_daily").fetchone()[0]
    latest_market_state_date = cur.execute("SELECT MAX(trade_date) FROM market_state_cache").fetchone()[0]
    report_row = cur.execute(
        """
        SELECT report_id, stock_code, stock_name_snapshot AS stock_name, trade_date, recommendation, confidence, strategy_type, quality_flag
        FROM report
        WHERE report_id = ?
        """,
        (report_id,),
    ).fetchone()
    instruction_row = cur.execute(
        """
        SELECT signal_entry_price, atr_pct, atr_multiplier, stop_loss, target_price, stop_loss_calc_mode
        FROM instruction_card
        WHERE report_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (report_id,),
    ).fetchone()
    conn.close()
    return {
        "latest_trade_date": latest_trade_date,
        "latest_pool_date": latest_pool_date,
        "pool_size": pool_size,
        "today_reports": today_reports,
        "today_buy_signals": today_buy_signals,
        "pending_review": pending_review,
        "latest_kline_date": latest_kline_date,
        "latest_market_state_date": latest_market_state_date,
        "report": dict(report_row) if report_row else None,
        "instruction_card": dict(instruction_row) if instruction_row else None,
    }


def attach_page_watchers(page: Page, page_result: dict[str, Any]) -> None:
    page_result["console_errors"] = []
    page_result["request_failures"] = []
    page_result["page_errors"] = []
    page_result["dialogs"] = []

    def on_console(msg: Any) -> None:
        if msg.type == "error":
            page_result["console_errors"].append(normalize_space(msg.text))

    def on_request_failed(request: Any) -> None:
        page_result["request_failures"].append(
            {
                "method": request.method,
                "url": request.url,
                "failure": request.failure,
            }
        )

    def on_page_error(exc: Any) -> None:
        page_result["page_errors"].append(normalize_space(str(exc)))

    def on_dialog(dialog: Any) -> None:
        page_result["dialogs"].append(normalize_space(dialog.message))
        dialog.dismiss()

    page.on("console", on_console)
    page.on("requestfailed", on_request_failed)
    page.on("pageerror", on_page_error)
    page.on("dialog", on_dialog)


def settle_page(page: Page) -> None:
    try:
        page.wait_for_load_state("networkidle", timeout=DEFAULT_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        pass
    page.wait_for_timeout(SAFE_PAUSE_MS)


def open_page(context: BrowserContext, run: AuditRun, *, role: str, path: str, label: str) -> tuple[Page, dict[str, Any]]:
    page_result = {
        "role": role,
        "page": path,
        "label": label,
        "status": "unknown",
        "actions": [],
        "extracted": {},
    }
    page = context.new_page()
    page.set_default_timeout(DEFAULT_TIMEOUT_MS)
    attach_page_watchers(page, page_result)
    response = page.goto(f"{run.base_url}{path}", wait_until="domcontentloaded")
    settle_page(page)
    page_result["status"] = response.status if response is not None else "no_response"
    return page, page_result


def action(page_result: dict[str, Any], label: str, ok: bool, detail: str = "") -> None:
    page_result["actions"].append({"label": label, "ok": ok, "detail": detail})


def text_or_empty(page: Page, selector: str) -> str:
    locator = page.locator(selector)
    if locator.count() == 0:
        return ""
    return normalize_space(locator.first.text_content())


def expect_equal(
    run: AuditRun,
    *,
    issue_id: str,
    severity: str,
    module: str,
    page: str,
    expected: Any,
    actual: Any,
    description: str,
    root_cause: str = "query/formatting defect",
    evidence: str | None = None,
) -> None:
    if expected != actual:
        run.add_issue(
            issue_id=issue_id,
            severity=severity,
            module=module,
            page=page,
            description=description,
            expected=expected,
            actual=actual,
            root_cause=root_cause,
            evidence=evidence,
        )


def expect_true(
    run: AuditRun,
    *,
    condition: bool,
    issue_id: str,
    severity: str,
    module: str,
    page: str,
    description: str,
    expected: Any,
    actual: Any,
    root_cause: str = "query/formatting defect",
    evidence: str | None = None,
) -> None:
    if not condition:
        run.add_issue(
            issue_id=issue_id,
            severity=severity,
            module=module,
            page=page,
            description=description,
            expected=expected,
            actual=actual,
            root_cause=root_cause,
            evidence=evidence,
        )


def setup_role_context(browser: Any, run: AuditRun, role: str, token: str) -> BrowserContext:
    context = browser.new_context(ignore_https_errors=True)
    page, page_result = open_page(context, run, role=role, path="/login", label=f"{role}-login")
    creds = run.summary["role_creds"][role]
    page.fill("#login-email", creds["email"])
    page.fill("#login-pwd", creds["password"])
    page.click("button[type='submit']")
    settle_page(page)
    ui_login_ok = "/login" not in page.url
    action(page_result, "submit-login-form", ui_login_ok, f"url={page.url}")
    if not ui_login_ok:
        run.add_issue(
            issue_id=f"LOGIN-{role.upper()}-UI",
            severity="P0",
            module="auth",
            page="/login",
            description=f"{role} UI login failed; fallback cookie injected for the remaining audit",
            expected="redirect away from /login",
            actual=page.url,
            root_cause="query/formatting defect",
        )
        context.add_cookies([{"name": AUTH_COOKIE, "value": token, "url": run.base_url, "path": "/"}])
    page_result["screenshot"] = run.screenshot(page, f"{role}_login")
    run.page_results.append(page_result)
    page.close()
    return context


def audit_home(page: Page, page_result: dict[str, Any], run: AuditRun, report_seed: dict[str, Any], api_home: dict[str, Any], api_dash: dict[str, Any]) -> None:
    page.wait_for_selector("#pool-size")
    settle_page(page)
    extracted = {
        "market_state": text_or_empty(page, "#market-state"),
        "pool_size": text_or_empty(page, "#pool-size"),
        "today_report_count": text_or_empty(page, "#today-report-count"),
        "total_reports": text_or_empty(page, "#total-reports"),
        "total_settled": text_or_empty(page, "#total-settled"),
    }
    page_result["extracted"] = extracted
    action(page_result, "load-home", True, json.dumps(extracted, ensure_ascii=False))

    expect_equal(
        run,
        issue_id="HOME-POOL-SIZE",
        severity="P0",
        module="home",
        page="/",
        description="Home pool_size should match /api/v1/home",
        expected=str(api_home.get("pool_size", 0)),
        actual=extracted["pool_size"],
        evidence=page_result.get("screenshot"),
    )
    expect_equal(
        run,
        issue_id="HOME-TODAY-REPORTS",
        severity="P1",
        module="home",
        page="/",
        description="Home today_report_count should match /api/v1/home",
        expected=str(api_home.get("today_report_count", 0)),
        actual=extracted["today_report_count"],
        evidence=page_result.get("screenshot"),
    )
    expect_equal(
        run,
        issue_id="HOME-TOTAL-REPORTS",
        severity="P1",
        module="home",
        page="/",
        description="Home 30-day total_reports should match /api/v1/dashboard/stats",
        expected=normalize_int_text(api_dash.get("total_reports", 0)),
        actual=normalize_int_text(extracted["total_reports"]),
        evidence=page_result.get("screenshot"),
    )

    page.fill("#hero-stock-code", report_seed["stock_code"])
    page.click("#hero-form button[type='submit']")
    settle_page(page)
    action(page_result, "hero-search", "/report/" in page.url or "/reports/" in page.url, page.url)
    page.go_back()
    settle_page(page)

    if page.locator("#latest-reports a.signal-card").count():
        page.locator("#latest-reports a.signal-card").first.click()
        settle_page(page)
        action(page_result, "open-latest-report-card", "/reports/" in page.url, page.url)
        page.go_back()
        settle_page(page)

    page_result["screenshot"] = run.screenshot(page, "anon_home")


def audit_reports_list(page: Page, page_result: dict[str, Any], run: AuditRun, report_seed: dict[str, Any]) -> None:
    page.wait_for_selector("#apply-filters")
    page.fill("#filter-code", report_seed["stock_code"])
    page.click("#apply-filters")
    settle_page(page)
    list_text = text_or_empty(page, "#reports-list")
    action(page_result, "filter-by-stock-code", report_seed["stock_code"] in list_text, list_text[:120])
    expect_true(
        run,
        condition=report_seed["stock_code"] in list_text or report_seed["stock_name"] in list_text,
        issue_id="REPORTS-FILTER-CODE",
        severity="P1",
        module="reports_list",
        page="/reports",
        description="Filtered reports list should contain the target stock",
        expected=f"contains {report_seed['stock_code']}",
        actual=list_text[:160],
        evidence=page_result.get("screenshot"),
    )

    if page.locator("#reports-list .report-row a").count():
        page.locator("#reports-list .report-row a").first.click()
        settle_page(page)
        action(page_result, "open-first-report", "/reports/" in page.url, page.url)
        page.go_back()
        settle_page(page)

    next_button = page.locator("#next-page").first
    prev_button = page.locator("#prev-page").first
    if next_button.count() and next_button.is_visible() and next_button.is_enabled():
        next_button.click()
        settle_page(page)
        action(page_result, "next-page", True, text_or_empty(page, "#page-info"))
        if prev_button.count() and prev_button.is_visible() and prev_button.is_enabled():
            prev_button.click()
            settle_page(page)
            action(page_result, "prev-page", True, text_or_empty(page, "#page-info"))

    page_result["extracted"] = {"page_info": text_or_empty(page, "#page-info")}
    page_result["screenshot"] = run.screenshot(page, "anon_reports")


def audit_report_detail(
    page: Page,
    page_result: dict[str, Any],
    run: AuditRun,
    *,
    role: str,
    report_id: str,
    api_detail: dict[str, Any],
) -> None:
    page.wait_for_selector(".report-hero")
    settle_page(page)
    card_text = normalize_space(page.locator(".instr-grid").first.text_content())
    page_result["extracted"] = {
        "title": text_or_empty(page, ".rv-hero-main h1"),
        "instruction_card": card_text,
        "term_context_atr": text_or_empty(page, "[data-tc-key='ATR'] .tc-val"),
    }
    action(page_result, "load-report-detail", True, card_text[:180])

    atr_ratio = api_detail.get("instruction_card", {}).get("atr_pct")
    if atr_ratio is not None:
        expected_atr = f"{float(atr_ratio) * 100:.2f}%"
        expect_true(
            run,
            condition=expected_atr in card_text,
            issue_id=f"REPORT-ATR-{role.upper()}",
            severity="P0",
            module="report_detail",
            page=f"/reports/{report_id}",
            description="Report detail ATR display should use 0-1 ratio -> percent conversion",
            expected=expected_atr,
            actual=card_text,
            evidence=page_result.get("screenshot"),
        )

    stop_loss_mode = api_detail.get("instruction_card", {}).get("stop_loss_calc_mode")
    expected_mode_map = {
        "atr_multiplier": "ATR 倍数",
        "fixed_92pct_fallback": "92% 兜底止损",
    }
    if stop_loss_mode in expected_mode_map:
        expect_true(
            run,
            condition=expected_mode_map[stop_loss_mode] in card_text,
            issue_id=f"REPORT-SLM-{role.upper()}",
            severity="P1",
            module="report_detail",
            page=f"/reports/{report_id}",
            description="Report detail stop loss mode label should be user-friendly",
            expected=expected_mode_map[stop_loss_mode],
            actual=card_text,
            evidence=page_result.get("screenshot"),
        )

    if role in {"anon", "free"}:
        expect_true(
            run,
            condition="●●●●" in card_text,
            issue_id=f"REPORT-MASK-{role.upper()}",
            severity="P0",
            module="report_detail",
            page=f"/reports/{report_id}",
            description="Anonymous/Free report detail should mask price-sensitive instruction fields",
            expected="masked dots",
            actual=card_text,
            root_cause="permission trimming",
            evidence=page_result.get("screenshot"),
        )
    else:
        signal_entry = api_detail.get("instruction_card", {}).get("signal_entry_price")
        if signal_entry is not None:
            expect_true(
                run,
                condition=f"{float(signal_entry):.2f}" in card_text,
                issue_id=f"REPORT-PRICE-{role.upper()}",
                severity="P1",
                module="report_detail",
                page=f"/reports/{report_id}",
                description="Paid/admin report detail should show unmasked signal price",
                expected=f"{float(signal_entry):.2f}",
                actual=card_text,
                evidence=page_result.get("screenshot"),
            )

    if page.locator("[data-tc-key='ATR'] .tc-term").count():
        page.hover("[data-tc-key='ATR'] .tc-term")
        action(page_result, "hover-atr-tooltip", True, text_or_empty(page, "[data-tc-key='ATR'] .tc-tooltip"))

    if page.locator(".rv-hero-meta a").count():
        page.locator(".rv-hero-meta a").first.click()
        settle_page(page)
        action(page_result, "open-latest-version-link", "/report/" in page.url or "/reports/" in page.url, page.url)
        page.go_back()
        settle_page(page)

    page_result["screenshot"] = run.screenshot(page, f"{role}_report_detail")


def audit_dashboard(page: Page, page_result: dict[str, Any], run: AuditRun, api_dash: dict[str, Any]) -> None:
    page.wait_for_selector("#window-tabs")
    for window in ("1", "7", "14", "30", "60"):
        page.click(f".db-window-tab[data-window='{window}']")
        settle_page(page)
        action(page_result, f"switch-window-{window}", True, text_or_empty(page, "#date-range"))

    extracted = {
        "total_reports": text_or_empty(page, "#total-reports"),
        "total_settled": text_or_empty(page, "#total-settled"),
        "overall_win_rate": text_or_empty(page, "#overall-win-rate"),
        "status_text": text_or_empty(page, "#dashboard-status"),
        "warning_text": text_or_empty(page, "#dashboard-warning"),
    }
    page_result["extracted"] = extracted
    expect_equal(
        run,
        issue_id="DASHBOARD-TOTAL-REPORTS",
        severity="P1",
        module="dashboard",
        page="/dashboard",
        description="Dashboard total_reports should match /api/v1/dashboard/stats?window_days=30",
        expected=str(api_dash.get("total_reports", 0)),
        actual=extracted["total_reports"],
        evidence=page_result.get("screenshot"),
    )
    expect_equal(
        run,
        issue_id="DASHBOARD-TOTAL-SETTLED",
        severity="P1",
        module="dashboard",
        page="/dashboard",
        description="Dashboard total_settled should match /api/v1/dashboard/stats?window_days=30",
        expected=str(api_dash.get("total_settled", 0)),
        actual=extracted["total_settled"],
        evidence=page_result.get("screenshot"),
    )
    if api_dash.get("signal_validity_warning"):
        expect_true(
            run,
            condition=bool(extracted["warning_text"]),
            issue_id="DASHBOARD-SIGNAL-WARNING",
            severity="P1",
            module="dashboard",
            page="/dashboard",
            description="Dashboard should render the top-level signal_validity_warning banner when API warns",
            expected="non-empty warning banner",
            actual=extracted["warning_text"] or "<empty>",
            evidence=page_result.get("screenshot"),
        )
    page_result["screenshot"] = run.screenshot(page, "anon_dashboard")


def audit_subscribe(page: Page, page_result: dict[str, Any], run: AuditRun, api_plans: list[dict[str, Any]]) -> None:
    page.wait_for_selector("#subscribe-grid")
    settle_page(page)
    page.locator("summary").first.click()
    action(page_result, "toggle-plan-compare", True)
    if page.locator("summary").count() > 1:
        page.locator("summary").nth(1).click()
        action(page_result, "toggle-faq", True)

    page_text = normalize_price_text(page.locator("#subscribe-grid").text_content())
    for plan in api_plans:
        normalized_price = normalize_price_text(plan.get("price_display"))
        expect_true(
            run,
            condition=normalized_price in page_text,
            issue_id=f"SUBSCRIBE-{plan.get('code', 'unknown').upper()}",
            severity="P1",
            module="subscribe",
            page="/subscribe",
            description="Subscribe page should show the same plan price as /api/v1/platform/plans",
            expected=normalized_price,
            actual=page_text,
            evidence=page_result.get("screenshot"),
        )

    page_result["extracted"] = {"plans_text": page_text[:280]}
    page_result["screenshot"] = run.screenshot(page, "anon_subscribe")


def audit_terms_or_privacy(page: Page, page_result: dict[str, Any], run: AuditRun, *, back_target: str) -> None:
    page.wait_for_selector("main h1")
    action(page_result, "load-legal-page", True, text_or_empty(page, "main h1"))
    if page.locator("a.btn-outline").count():
        page.locator("a.btn-outline").first.click()
        settle_page(page)
        action(page_result, "click-return-register", back_target in page.url, page.url)
    page_result["screenshot"] = run.screenshot(page, f"anon_{page_result['page'].strip('/').replace('-', '_')}")


def audit_profile(page: Page, page_result: dict[str, Any], run: AuditRun, expected_tier: str) -> None:
    page.wait_for_selector("#account")
    page.click("a[href='#membership']")
    action(page_result, "jump-membership", True, page.url)
    page.click("a[href='#feedback']")
    action(page_result, "jump-feedback", True, page.url)
    profile_text = normalize_space(page.locator(".profile-main").text_content())
    expect_true(
        run,
        condition=expected_tier in profile_text,
        issue_id=f"PROFILE-TIER-{expected_tier.upper()}",
        severity="P2",
        module="profile",
        page="/profile",
        description="Profile page should expose the user's current membership tier text",
        expected=expected_tier,
        actual=profile_text,
        evidence=page_result.get("screenshot"),
    )
    page_result["extracted"] = {"profile_text": profile_text[:260]}
    page_result["screenshot"] = run.screenshot(page, f"{expected_tier.lower()}_profile")


def audit_sim_dashboard(
    page: Page,
    page_result: dict[str, Any],
    run: AuditRun,
    *,
    role: str,
    api_by_tier: dict[str, dict[str, Any]],
) -> None:
    if role == "free":
        banner_text = normalize_space(page.locator("main").text_content())
        expect_true(
            run,
            condition="仅对付费会员和管理员开放" in banner_text,
            issue_id="SIM-FREE-PAYWALL",
            severity="P0",
            module="sim_dashboard",
            page="/portfolio/sim-dashboard",
            description="Free user should see the expected membership paywall on sim dashboard",
            expected="付费会员和管理员开放",
            actual=banner_text,
            root_cause="permission trimming",
            evidence=page_result.get("screenshot"),
        )
        page_result["extracted"] = {"banner": banner_text[:220]}
        page_result["screenshot"] = run.screenshot(page, "free_sim_dashboard")
        return

    page.wait_for_selector("#tier-tabs")
    for tier, api_payload in api_by_tier.items():
        if page.locator(f"#tier-tabs a[data-tier='{tier}']").count():
            page.click(f"#tier-tabs a[data-tier='{tier}']")
            settle_page(page)
            metrics_text = normalize_space(page.locator("main").text_content())
            expected_return = f"{float(api_payload.get('total_return_pct') or 0) * 100:.1f}%"
            expect_true(
                run,
                condition=expected_return in metrics_text or "样本不足" in metrics_text,
                issue_id=f"SIM-RETURN-{role.upper()}-{tier.upper()}",
                severity="P1",
                module="sim_dashboard",
                page="/portfolio/sim-dashboard",
                description="Sim dashboard should display the tier return from /api/v1/portfolio/sim-dashboard",
                expected=expected_return,
                actual=metrics_text[:220],
                evidence=page_result.get("screenshot"),
            )
            action(page_result, f"switch-tier-{tier}", True, metrics_text[:160])

    if page.locator("#open-positions a").count():
        page.locator("#open-positions a").first.click()
        settle_page(page)
        action(page_result, "open-position-report", "/reports/" in page.url, page.url)
        page.go_back()
        settle_page(page)

    page_result["extracted"] = {
        "metric_return": text_or_empty(page, "#metric-return"),
        "metric_win_rate": text_or_empty(page, "#metric-win-rate"),
    }
    page_result["screenshot"] = run.screenshot(page, f"{role}_sim_dashboard")


def audit_admin(page: Page, page_result: dict[str, Any], run: AuditRun, db_truth: dict[str, Any], api_overview: dict[str, Any]) -> None:
    page.wait_for_selector("#overview-pool-size")
    settle_page(page)
    extracted = {
        "pool_size": text_or_empty(page, "#overview-pool-size"),
        "today_reports": text_or_empty(page, "#overview-today-reports"),
        "today_buy_signals": text_or_empty(page, "#overview-buy-signals"),
        "pending_review": text_or_empty(page, "#overview-pending-review"),
        "latest_kline_date": text_or_empty(page, "#overview-kline-date"),
        "latest_market_state_date": text_or_empty(page, "#overview-ms-date"),
    }
    page_result["extracted"] = extracted

    page.click("#nav-scheduler")
    settle_page(page)
    action(page_result, "switch-scheduler", True, normalize_space(page.locator("#section-scheduler").text_content())[:120])
    page.click("#nav-review")
    settle_page(page)
    action(page_result, "switch-review", True, normalize_space(page.locator("#section-review").text_content())[:120])
    page.click("#nav-users")
    settle_page(page)
    action(page_result, "switch-users", True, normalize_space(page.locator("#section-users").text_content())[:120])
    page.click("#nav-overview")
    settle_page(page)
    action(page_result, "switch-overview", True)
    page.click("#section-overview .action-btn-ghost")
    settle_page(page)
    action(page_result, "refresh-overview", True)

    expect_equal(
        run,
        issue_id="ADMIN-POOL-SIZE",
        severity="P0",
        module="admin_overview",
        page="/admin",
        description="Admin overview pool_size should match DB truth and /api/v1/admin/overview",
        expected=str(db_truth["pool_size"]),
        actual=extracted["pool_size"],
        evidence=page_result.get("screenshot"),
    )
    expect_equal(
        run,
        issue_id="ADMIN-TODAY-REPORTS",
        severity="P1",
        module="admin_overview",
        page="/admin",
        description="Admin overview today_reports should match DB truth",
        expected=str(db_truth["today_reports"]),
        actual=extracted["today_reports"],
        evidence=page_result.get("screenshot"),
    )
    expect_equal(
        run,
        issue_id="ADMIN-BUY-SIGNALS",
        severity="P1",
        module="admin_overview",
        page="/admin",
        description="Admin overview today_buy_signals should match DB truth",
        expected=str(db_truth["today_buy_signals"]),
        actual=extracted["today_buy_signals"],
        evidence=page_result.get("screenshot"),
    )
    expect_equal(
        run,
        issue_id="ADMIN-KLINE-DATE",
        severity="P1",
        module="admin_overview",
        page="/admin",
        description="Admin overview latest_kline_date should match DB truth",
        expected=str(db_truth["latest_kline_date"]),
        actual=extracted["latest_kline_date"],
        evidence=page_result.get("screenshot"),
    )
    expect_equal(
        run,
        issue_id="ADMIN-MARKET-STATE-DATE",
        severity="P1",
        module="admin_overview",
        page="/admin",
        description="Admin overview latest_market_state_date should match DB truth",
        expected=str(db_truth["latest_market_state_date"]),
        actual=extracted["latest_market_state_date"],
        evidence=page_result.get("screenshot"),
    )
    expect_equal(
        run,
        issue_id="ADMIN-API-PAGE-POOL",
        severity="P1",
        module="admin_overview",
        page="/admin",
        description="Admin page pool_size should match /api/v1/admin/overview",
        expected=str(api_overview.get("pool_size", 0)),
        actual=extracted["pool_size"],
        evidence=page_result.get("screenshot"),
    )
    page_result["screenshot"] = run.screenshot(page, "admin_overview")


def audit_admin_forbidden(page: Page, page_result: dict[str, Any], run: AuditRun) -> None:
    main_text = normalize_space(page.locator("body").text_content())
    page_result["allow_console_errors"] = True
    expect_true(
        run,
        condition=page_result["status"] == 403 or "403" in main_text or "去登录" in main_text,
        issue_id="ADMIN-PRO-FORBIDDEN",
        severity="P0",
        module="rbac",
        page="/admin",
        description="Non-admin paid user should not access /admin",
        expected="403 or forbidden page",
        actual=f"status={page_result['status']} text={main_text[:120]}",
        root_cause="permission trimming",
        evidence=page_result.get("screenshot"),
    )
    page_result["screenshot"] = run.screenshot(page, "pro_admin_forbidden")


def audit_anonymous_redirect(page: Page, page_result: dict[str, Any], run: AuditRun, *, path: str) -> None:
    current = page.url
    expect_true(
        run,
        condition="/login" in current or page_result["status"] in (302, 307),
        issue_id=f"ANON-REDIRECT-{path.strip('/').upper()}",
        severity="P0",
        module="rbac",
        page=path,
        description="Anonymous protected route should redirect to /login",
        expected="/login redirect",
        actual=current,
        root_cause="permission trimming",
        evidence=page_result.get("screenshot"),
    )
    page_result["screenshot"] = run.screenshot(page, f"anon_{path.strip('/').replace('/', '_')}")


def audit_register(page: Page, page_result: dict[str, Any], run: AuditRun, email: str, password: str) -> None:
    page.wait_for_selector("#register-form")
    page.fill("#reg-email", email)
    page.fill("#reg-pwd", password)
    page.fill("#reg-pwd2", password)
    page.check("#agree-tos")
    page.click("#btn-submit")
    settle_page(page)
    form_text = normalize_space(page.locator("body").text_content())
    action(page_result, "submit-register", "注册成功" in form_text or "/login" in page.url, page.url)
    expect_true(
        run,
        condition="注册成功" in form_text or "/login" in page.url,
        issue_id="REGISTER-SUBMIT",
        severity="P1",
        module="auth",
        page="/register",
        description="Register page should submit successfully with a unique email",
        expected="success message or redirect to /login",
        actual=form_text[:200],
        evidence=page_result.get("screenshot"),
    )
    page_result["screenshot"] = run.screenshot(page, "anon_register")


def audit_forgot_password(page: Page, page_result: dict[str, Any], run: AuditRun, email: str) -> None:
    page.wait_for_selector("#forgot-form")
    page.fill("#email", email)
    page.click("button[type='submit']")
    settle_page(page)
    message = text_or_empty(page, "#forgot-msg")
    action(page_result, "submit-forgot-password", bool(message), message)
    expect_true(
        run,
        condition="若该邮箱已注册" in message,
        issue_id="FORGOT-PASSWORD-SUBMIT",
        severity="P1",
        module="auth",
        page="/forgot-password",
        description="Forgot password page should show the generic success message",
        expected="若该邮箱已注册，将收到重置链接。",
        actual=message,
        evidence=page_result.get("screenshot"),
    )
    page_result["screenshot"] = run.screenshot(page, "anon_forgot_password")


def record_page_result(run: AuditRun, page_result: dict[str, Any], page: Page) -> None:
    if not page_result.get("screenshot"):
        page_result["screenshot"] = run.screenshot(page, page_result["label"].replace("/", "_"))
    allow_console_errors = bool(page_result.get("allow_console_errors"))
    if (not allow_console_errors) and (page_result.get("console_errors") or page_result.get("request_failures") or page_result.get("page_errors")):
        run.add_issue(
            issue_id=f"PAGE-RUNTIME-{page_result['label'].upper()}",
            severity="P2",
            module="browser_runtime",
            page=page_result["page"],
            description="Page emitted console/page/network errors during browser audit",
            expected="no console/page/request failures",
            actual={
                "console_errors": page_result.get("console_errors", []),
                "page_errors": page_result.get("page_errors", []),
                "request_failures": page_result.get("request_failures", []),
            },
            root_cause="query/formatting defect",
            evidence=page_result["screenshot"],
        )
    run.page_results.append(page_result)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path)
    if db_path.resolve() == DEFAULT_DB_PATH.resolve() and not args.allow_live_db:
        raise RuntimeError("Refusing to read the live runtime DB without --allow-live-db")
    run = AuditRun(base_url=args.base_url, run_label=args.run_label)
    role_creds = load_role_credentials()
    run.summary["role_creds"] = role_creds
    temp_email = f"browser.audit.{args.run_label}.{int(datetime.now().timestamp())}@test.com"
    temp_password = secrets.token_urlsafe(12)

    with httpx.Client(timeout=30, follow_redirects=True) as client:
        tokens: dict[str, str] = {}
        for role, creds in role_creds.items():
            token, _profile = api_login(client, base_url=args.base_url, email=creds["email"], password=creds["password"])
            tokens[role] = token

        reports_list, _ = api_get(client, args.base_url, "/api/v1/reports?page=1&page_size=3", token=tokens["pro"])
        items = reports_list.get("items", [])
        if not items:
            print("No reports available for audit", file=sys.stderr)
            return 1
        seed = dict(items[0])
        report_id = seed["report_id"]

        db_truth = get_db_truth(report_id, db_path)
        api_home, _ = api_get(client, args.base_url, "/api/v1/home")
        api_dash_30, _ = api_get(client, args.base_url, "/api/v1/dashboard/stats?window_days=30")
        api_plans_response, _ = api_get(client, args.base_url, "/api/v1/platform/plans")
        api_plans = api_plans_response.get("plans", [])
        api_admin_overview, _ = api_get(client, args.base_url, "/api/v1/admin/overview", token=tokens["admin"])
        api_detail_by_role = {
            "anon": api_get(client, args.base_url, f"/api/v1/reports/{report_id}")[0],
            "free": api_get(client, args.base_url, f"/api/v1/reports/{report_id}", token=tokens["free"])[0],
            "pro": api_get(client, args.base_url, f"/api/v1/reports/{report_id}", token=tokens["pro"])[0],
            "admin": api_get(client, args.base_url, f"/api/v1/reports/{report_id}", token=tokens["admin"])[0],
        }
        sim_api_by_role = {
            "admin": {
                tier: api_get(client, args.base_url, f"/api/v1/portfolio/sim-dashboard?capital_tier={tier}", token=tokens["admin"])[0]
                for tier in ("10k", "100k", "500k")
            },
            "pro": {
                tier: api_get(client, args.base_url, f"/api/v1/portfolio/sim-dashboard?capital_tier={tier}", token=tokens["pro"])[0]
                for tier in ("10k", "100k", "500k")
            },
        }

        run.add_cross_check(
            check_id="X-POOL-SIZE-DB-API",
            metric="pool_size",
            status="ok" if db_truth["pool_size"] == api_home.get("pool_size") == api_admin_overview.get("pool_size") else "mismatch",
            expected=db_truth["pool_size"],
            actual={"home": api_home.get("pool_size"), "admin": api_admin_overview.get("pool_size")},
            source_chain="DB -> /api/v1/home -> /api/v1/admin/overview",
            root_cause="query/formatting defect",
        )
        run.add_cross_check(
            check_id="X-TODAY-BUY-SIGNALS-DB-API",
            metric="today_buy_signals",
            status="ok" if db_truth["today_buy_signals"] == api_admin_overview.get("today_buy_signals") else "mismatch",
            expected=db_truth["today_buy_signals"],
            actual=api_admin_overview.get("today_buy_signals"),
            source_chain="DB -> /api/v1/admin/overview",
            root_cause="query/formatting defect",
        )

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=not args.headed)
            anon_context = browser.new_context(ignore_https_errors=True)
            admin_context = setup_role_context(browser, run, "admin", tokens["admin"])
            pro_context = setup_role_context(browser, run, "pro", tokens["pro"])
            free_context = setup_role_context(browser, run, "free", tokens["free"])

            page, result = open_page(anon_context, run, role="anon", path="/", label="anon-home")
            audit_home(page, result, run, seed, api_home, api_dash_30)
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(anon_context, run, role="anon", path="/reports", label="anon-reports")
            audit_reports_list(page, result, run, seed)
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(anon_context, run, role="anon", path="/dashboard", label="anon-dashboard")
            audit_dashboard(page, result, run, api_dash_30)
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(anon_context, run, role="anon", path="/subscribe", label="anon-subscribe")
            audit_subscribe(page, result, run, api_plans)
            record_page_result(run, result, page)
            page.close()

            if args.allow_destructive_auth_flows:
                page, result = open_page(anon_context, run, role="anon", path="/register", label="anon-register")
                audit_register(page, result, run, temp_email, temp_password)
                record_page_result(run, result, page)
                page.close()

                page, result = open_page(anon_context, run, role="anon", path="/forgot-password", label="anon-forgot-password")
                audit_forgot_password(page, result, run, temp_email)
                record_page_result(run, result, page)
                page.close()

            page, result = open_page(anon_context, run, role="anon", path="/terms", label="anon-terms")
            audit_terms_or_privacy(page, result, run, back_target="/register")
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(anon_context, run, role="anon", path="/privacy", label="anon-privacy")
            audit_terms_or_privacy(page, result, run, back_target="/register")
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(anon_context, run, role="anon", path="/profile", label="anon-profile-redirect")
            audit_anonymous_redirect(page, result, run, path="/profile")
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(anon_context, run, role="anon", path=f"/reports/{report_id}", label="anon-report-detail")
            audit_report_detail(page, result, run, role="anon", report_id=report_id, api_detail=api_detail_by_role["anon"])
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(pro_context, run, role="pro", path=f"/reports/{report_id}", label="pro-report-detail")
            audit_report_detail(page, result, run, role="pro", report_id=report_id, api_detail=api_detail_by_role["pro"])
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(free_context, run, role="free", path=f"/reports/{report_id}", label="free-report-detail")
            audit_report_detail(page, result, run, role="free", report_id=report_id, api_detail=api_detail_by_role["free"])
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(pro_context, run, role="pro", path="/portfolio/sim-dashboard", label="pro-sim-dashboard")
            audit_sim_dashboard(page, result, run, role="pro", api_by_tier=sim_api_by_role["pro"])
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(admin_context, run, role="admin", path="/portfolio/sim-dashboard", label="admin-sim-dashboard")
            audit_sim_dashboard(page, result, run, role="admin", api_by_tier=sim_api_by_role["admin"])
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(free_context, run, role="free", path="/portfolio/sim-dashboard", label="free-sim-dashboard")
            audit_sim_dashboard(page, result, run, role="free", api_by_tier={})
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(pro_context, run, role="pro", path="/profile", label="pro-profile")
            audit_profile(page, result, run, expected_tier="Pro")
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(admin_context, run, role="admin", path="/profile", label="admin-profile")
            audit_profile(page, result, run, expected_tier=role_creds["admin"].get("membership_tier", "Free"))
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(admin_context, run, role="admin", path="/admin", label="admin-page")
            audit_admin(page, result, run, db_truth, api_admin_overview)
            record_page_result(run, result, page)
            page.close()

            page, result = open_page(pro_context, run, role="pro", path="/admin", label="pro-admin-forbidden")
            audit_admin_forbidden(page, result, run)
            record_page_result(run, result, page)
            page.close()

            anon_context.close()
            admin_context.close()
            pro_context.close()
            free_context.close()
            browser.close()

        run.add_non_defect(
            item_id="ND-ADMIN-DESTRUCTIVE-ACTIONS",
            module="admin",
            page="/admin",
            description="Skipped destructive admin buttons (purge / DAG retrigger / report mutation) during browser sweep",
            reason="needs cross-FR linked change and carries real data mutation risk",
        )
        if not args.allow_destructive_auth_flows:
            run.add_non_defect(
                item_id="ND-DESTRUCTIVE-AUTH-FLOWS-SKIPPED",
                module="auth",
                page="/register",
                description="Skipped register / forgot-password flows to keep the browser audit read-mostly by default",
                reason="requires --allow-destructive-auth-flows",
            )

    json_path, md_path = run.write_reports()
    print(f"browser_audit_json={json_path}")
    print(f"browser_audit_md={md_path}")
    print(f"browser_audit_issues={len(run.issues)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
