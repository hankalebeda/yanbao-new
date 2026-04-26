#!/usr/bin/env python3
"""Comprehensive admin browser test for the FastAPI web application."""
import io
import json
import sys
import os
import re
import traceback
from collections import defaultdict

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

import httpx

BASE = "http://127.0.0.1:8000"
EMAIL = "test_browser_admin@test.com"
PASSWORD = "Admin123456"
TIMEOUT = 30

# Collect cross-page data for consistency checks
cross_data = defaultdict(dict)
all_issues = []


def log(msg):
    try:
        print(str(msg), flush=True)
    except UnicodeEncodeError:
        print(str(msg).encode("utf-8", errors="replace").decode("utf-8", errors="replace"), flush=True)


def sep(title):
    print(f"\n{'='*80}", flush=True)
    print(f"  {title}", flush=True)
    print(f"{'='*80}", flush=True)


def subsep(title):
    print(f"\n{'-'*60}", flush=True)
    print(f"  {title}", flush=True)
    print(f"{'-'*60}", flush=True)


def record_issue(location, severity, description):
    issue = {"location": location, "severity": severity, "description": description}
    all_issues.append(issue)
    marker = {"critical": "[CRITICAL]", "warning": "[WARNING]", "info": "[INFO]"}.get(severity, "[?]")
    log(f"  {marker} {description}")


def check_html_page(client, path, page_name, token=None):
    """Fetch an HTML page and analyze it."""
    subsep(f"PAGE: {page_name} ({path})")
    headers = {"Accept": "text/html"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        r = client.get(f"{BASE}{path}", headers=headers, follow_redirects=False)
    except Exception as e:
        record_issue(page_name, "critical", f"Connection error: {e}")
        return None

    log(f"  Status: {r.status_code}")

    # Handle redirects
    if r.status_code in (301, 302, 303, 307, 308):
        loc = r.headers.get("location", "N/A")
        log(f"  Redirect -> {loc}")
        record_issue(page_name, "info", f"Redirects to {loc}")
        try:
            r2 = client.get(f"{BASE}{path}", headers=headers, follow_redirects=True)
            log(f"  Final status after redirect: {r2.status_code}")
            r = r2
        except Exception as e:
            record_issue(page_name, "warning", f"Error following redirect: {e}")
            return r

    if r.status_code >= 400:
        record_issue(page_name, "critical", f"HTTP {r.status_code} error")
        body_preview = r.text[:500] if r.text else "(empty)"
        log(f"  Body preview: {body_preview}")
        return r

    html = r.text
    html_len = len(html)
    log(f"  Content length: {html_len} chars")

    if html_len < 100:
        record_issue(page_name, "warning", f"Very short HTML content ({html_len} chars)")

    # Check for common error patterns in HTML
    error_patterns = [
        (r"500 Internal Server Error", "500 error text in page"),
        (r"404 Not Found", "404 error text in page"),
        (r"Traceback \(most recent call last\)", "Python traceback visible in page"),
        (r"Internal Server Error", "Internal server error text"),
    ]
    for pat, desc in error_patterns:
        matches = re.findall(pat, html, re.IGNORECASE)
        if matches:
            record_issue(page_name, "warning", f"{desc} ({len(matches)} occurrences)")

    # Check title
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.DOTALL)
    if title_match:
        log(f"  Page title: {title_match.group(1).strip()}")
    else:
        record_issue(page_name, "warning", "No <title> tag found")

    # Language stats
    chinese_count = len(re.findall(r'[\u4e00-\u9fff]', html))
    english_words = len(re.findall(r'\b[a-zA-Z]{3,}\b', html))
    log(f"  Chinese chars: {chinese_count}, English words: {english_words}")

    # Check links
    links = re.findall(r'href="([^"]+)"', html)
    internal_links = [l for l in links if l.startswith('/') and not l.startswith('//')]
    log(f"  Internal links found: {len(internal_links)}")
    if internal_links:
        log(f"  Sample links: {internal_links[:10]}")

    # Look for fetch/API calls in JavaScript to detect frontend-backend coupling
    api_calls = re.findall(r"""(?:fetch|axios\.get|axios\.post|api[Bb]ridge\.[a-z]+)\s*\(\s*[`'"]([^`'"]+)""", html)
    if api_calls:
        log(f"  JS API calls found: {api_calls[:10]}")

    # Check for broken images
    img_srcs = re.findall(r'<img[^>]+src=["\']([^"\']+)["\']', html)
    if img_srcs:
        log(f"  Image sources: {img_srcs[:5]}")

    # Check for JavaScript errors / undefined / NaN visible in text (not in JS code)
    # Look at text content within visible elements
    visible_text_chunks = re.findall(r'>([^<]{3,})<', html)
    for chunk in visible_text_chunks:
        chunk_stripped = chunk.strip()
        if chunk_stripped in ('undefined', 'NaN', 'null', 'None'):
            record_issue(page_name, "warning", f"Visible text shows '{chunk_stripped}'")
        if "error" in chunk_stripped.lower() and len(chunk_stripped) < 200:
            if not any(skip in chunk_stripped.lower() for skip in ("error-", "error_", "haserror", "if error")):
                pass  # too many false positives, skip

    return r


def check_api(client, path, api_name, token, params=None):
    """Fetch an API endpoint and analyze the response."""
    subsep(f"API: {api_name} ({path})")
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }
    try:
        r = client.get(f"{BASE}{path}", headers=headers, params=params, follow_redirects=True)
    except Exception as e:
        record_issue(api_name, "critical", f"Connection error: {e}")
        return None

    log(f"  Status: {r.status_code}")
    ct = r.headers.get("content-type", "")
    log(f"  Content-Type: {ct}")

    if r.status_code >= 400:
        record_issue(api_name, "critical", f"HTTP {r.status_code} error")
        try:
            body = r.json()
            log(f"  Error response: {json.dumps(body, ensure_ascii=False, indent=2)[:800]}")
        except Exception:
            log(f"  Error body: {r.text[:500]}")
        return r

    if "json" not in ct and "text" not in ct:
        record_issue(api_name, "warning", f"Unexpected Content-Type: {ct}")
        log(f"  Body preview: {r.text[:300]}")
        return r

    try:
        data = r.json()
    except Exception as e:
        record_issue(api_name, "critical", f"Invalid JSON response: {e}")
        log(f"  Body preview: {r.text[:300]}")
        return r

    # Pretty print response (truncated)
    data_str = json.dumps(data, ensure_ascii=False, indent=2)
    if len(data_str) > 4000:
        log(f"  Response (truncated to 4000 chars):\n{data_str[:4000]}...")
    else:
        log(f"  Response:\n{data_str}")

    # Check for envelope structure
    if isinstance(data, dict):
        if "ok" in data:
            if not data.get("ok"):
                error_msg = data.get("error") or data.get("message") or data.get("detail", "unknown")
                record_issue(api_name, "critical", f"API returned ok=false: {error_msg}")
        if "data" in data and data["data"] is None:
            record_issue(api_name, "warning", "API returned data=null")

    return r


def analyze_api_data(data, api_name):
    """Deep analysis of API response data for specific issues."""
    if not isinstance(data, dict):
        return
    payload = data.get("data", data)
    if payload is None:
        return

    cross_data[api_name] = payload

    if isinstance(payload, dict):
        # Market state
        if "market_state" in payload:
            ms = payload["market_state"]
            cross_data["_market_state"][api_name] = ms
            log(f"  >> market_state = {ms}")
        # Pool size
        if "pool_size" in payload:
            ps = payload["pool_size"]
            cross_data["_pool_size"][api_name] = ps
            log(f"  >> pool_size = {ps}")
        if "pool" in payload and isinstance(payload["pool"], list):
            cross_data["_pool_size"][api_name + "_list_len"] = len(payload["pool"])
            log(f"  >> pool list length = {len(payload['pool'])}")
        # Stocks
        if "stocks" in payload and isinstance(payload["stocks"], list):
            log(f"  >> stocks list length = {len(payload['stocks'])}")
            for s in payload["stocks"][:3]:
                if isinstance(s, dict):
                    log(f"     stock: {s.get('stock_code','?')} {s.get('stock_name','?')} score={s.get('score','?')}")
        # Report counts
        for key in ("report_count", "total_reports", "total", "count"):
            if key in payload:
                cross_data["_report_count"][f"{api_name}.{key}"] = payload[key]
                log(f"  >> {key} = {payload[key]}")
        # Dates
        for key in ("trade_date", "latest_trade_date", "date", "updated_at", "last_run"):
            if key in payload:
                cross_data["_dates"][f"{api_name}.{key}"] = payload[key]
                log(f"  >> {key} = {payload[key]}")
        # Counts / stats
        for key in ("total_users", "active_users", "total_reports", "total_stocks"):
            if key in payload:
                log(f"  >> {key} = {payload[key]}")
        # Sim dashboard specific
        for key in ("equity", "cash", "initial_capital", "total_pnl", "total_return"):
            if key in payload:
                log(f"  >> {key} = {payload[key]}")
        # Positions
        if "positions" in payload and isinstance(payload["positions"], list):
            log(f"  >> positions count = {len(payload['positions'])}")
            for p in payload["positions"][:3]:
                if isinstance(p, dict):
                    log(f"     pos: {p.get('stock_code','?')} qty={p.get('quantity','?')} cost={p.get('cost_price','?')} current={p.get('current_price','?')}")
        # Items / list
        if "items" in payload and isinstance(payload["items"], list):
            log(f"  >> items count = {len(payload['items'])}")
            cross_data["_report_count"][f"{api_name}.items_len"] = len(payload["items"])
        if "reports" in payload and isinstance(payload["reports"], list):
            log(f"  >> reports count = {len(payload['reports'])}")


def main():
    sep("ADMIN BROWSER TEST - COMPREHENSIVE")
    log(f"Target: {BASE}")
    log(f"Email: {EMAIL}")

    client = httpx.Client(timeout=TIMEOUT, follow_redirects=False)

    # ─── Step 1: Login ───
    sep("STEP 1: LOGIN")
    # Auth router has no prefix, so path is /auth/login
    try:
        r = client.post(
            f"{BASE}/auth/login",
            json={"email": EMAIL, "password": PASSWORD},
            headers={"Accept": "application/json"},
        )
    except Exception as e:
        log(f"FATAL: Cannot connect to {BASE}: {e}")
        sys.exit(1)

    log(f"Login status: {r.status_code}")

    if r.status_code != 200:
        log(f"Login failed: {r.text[:500]}")
        log("Trying to register first...")
        reg = client.post(
            f"{BASE}/auth/register",
            json={"email": EMAIL, "password": PASSWORD},
        )
        log(f"Register status: {reg.status_code}")
        log(f"Register body: {reg.text[:500]}")
        # Retry login
        r = client.post(
            f"{BASE}/auth/login",
            json={"email": EMAIL, "password": PASSWORD},
        )
        log(f"Login retry status: {r.status_code}")

    if r.status_code != 200:
        log(f"FATAL: Cannot login. Response: {r.text[:500]}")
        sys.exit(1)

    login_data = r.json()
    log(f"Login response: {json.dumps(login_data, ensure_ascii=False, indent=2)}")

    # Extract token
    token = None
    if isinstance(login_data, dict):
        token = login_data.get("access_token")
        if not token and "data" in login_data:
            inner = login_data["data"]
            if isinstance(inner, dict):
                token = inner.get("access_token")

    if not token:
        log("FATAL: No access_token in login response")
        sys.exit(1)

    log(f"Token extracted: {token[:40]}...")

    # Check cookies
    set_cookie_headers = [v for k, v in r.headers.multi_items() if k.lower() == "set-cookie"]
    log(f"Set-Cookie headers ({len(set_cookie_headers)}): {set_cookie_headers}")
    for k, v in r.cookies.items():
        client.cookies.set(k, v)
        log(f"  Cookie set: {k}={v[:40]}...")

    # ─── Step 2: Verify auth ───
    sep("STEP 2: VERIFY AUTH (GET /auth/me)")
    me_r = check_api(client, "/auth/me", "auth/me", token)
    if me_r and me_r.status_code == 200:
        me_data = me_r.json()
        inner = me_data.get("data", me_data)
        if isinstance(inner, dict):
            log(f"  User ID: {inner.get('user_id', 'N/A')}")
            log(f"  Email: {inner.get('email', 'N/A')}")
            log(f"  Role: {inner.get('role', 'N/A')}")
            log(f"  Tier: {inner.get('tier', 'N/A')}")
            role = inner.get("role", "")
            if role != "admin":
                record_issue("auth/me", "critical", f"User role is '{role}', expected 'admin'")
            cross_data["_user"] = inner

    # ─── Step 3: HTML Pages ───
    sep("STEP 3: HTML PAGES")
    pages = [
        ("/", "Homepage"),
        ("/login", "Login Page"),
        ("/register", "Register Page"),
        ("/reports", "Reports List"),
        ("/dashboard", "Dashboard"),
        ("/profile", "Profile"),
        ("/admin", "Admin Panel"),
        ("/subscribe", "Subscribe"),
        ("/features", "Features Page"),
        ("/forgot-password", "Forgot Password"),
        ("/terms", "Terms of Service"),
        ("/privacy", "Privacy Policy"),
    ]
    for path, name in pages:
        try:
            check_html_page(client, path, name, token)
        except Exception as e:
            record_issue(name, "critical", f"Exception: {e}")
            traceback.print_exc()

    # ─── Step 4: API Endpoints ───
    sep("STEP 4: API ENDPOINTS")

    # Health check
    r = check_api(client, "/health", "health", token)
    if r and r.status_code == 200:
        try:
            analyze_api_data(r.json(), "health")
        except Exception:
            pass

    # Home (business router prefix /api/v1)
    r = check_api(client, "/api/v1/home", "home", token)
    if r and r.status_code == 200:
        try:
            analyze_api_data(r.json(), "home")
        except Exception:
            pass

    # Reports list
    r = check_api(client, "/api/v1/reports", "reports_list", token, params={"page": 1, "page_size": 5})
    first_report_id = None
    if r and r.status_code == 200:
        try:
            rdata = r.json()
            analyze_api_data(rdata, "reports_list")
            inner = rdata.get("data", rdata)
            items = None
            if isinstance(inner, dict):
                items = inner.get("items") or inner.get("reports") or inner.get("data")
            elif isinstance(inner, list):
                items = inner
            if items and len(items) > 0:
                first = items[0]
                first_report_id = first.get("report_id")
                log(f"  >> First report: id={first_report_id}")
                log(f"     stock: {first.get('stock_code', '?')} - {first.get('stock_name', '?')}")
                log(f"     date: {first.get('trade_date', '?')}")
                log(f"     recommendation: {first.get('recommendation', '?')}")
                log(f"     confidence: {first.get('confidence', '?')}")
                log(f"     run_mode: {first.get('run_mode', '?')}")
                required_fields = ["report_id", "stock_code", "trade_date"]
                for rf in required_fields:
                    if not first.get(rf):
                        record_issue("reports_list", "warning", f"First report missing '{rf}'")
            else:
                record_issue("reports_list", "warning", "No reports found in list")
        except Exception as e:
            record_issue("reports_list", "warning", f"Error parsing: {e}")

    # Report detail
    if first_report_id:
        r = check_api(client, f"/api/v1/reports/{first_report_id}", "report_detail", token)
        if r and r.status_code == 200:
            try:
                analyze_api_data(r.json(), "report_detail")
            except Exception:
                pass

        r = check_api(client, f"/api/v1/reports/{first_report_id}/advanced", "report_advanced", token)
        if r and r.status_code == 200:
            try:
                analyze_api_data(r.json(), "report_advanced")
            except Exception:
                pass
    else:
        record_issue("report_detail", "warning", "Skipped - no report ID available")
        record_issue("report_advanced", "warning", "Skipped - no report ID available")

    # Dashboard stats
    r = check_api(client, "/api/v1/dashboard/stats", "dashboard_stats", token, params={"window_days": 30})
    if r and r.status_code == 200:
        try:
            analyze_api_data(r.json(), "dashboard_stats")
        except Exception:
            pass

    # Pool stocks
    r = check_api(client, "/api/v1/pool/stocks", "pool_stocks", token)
    if r and r.status_code == 200:
        try:
            analyze_api_data(r.json(), "pool_stocks")
        except Exception:
            pass

    # Sim dashboards
    for tier in ("100k", "500k"):
        r = check_api(client, "/api/v1/portfolio/sim-dashboard", f"sim_{tier}", token,
                       params={"capital_tier": tier})
        if r and r.status_code == 200:
            try:
                data = r.json()
                analyze_api_data(data, f"sim_{tier}")
                inner = data.get("data", data)
                if isinstance(inner, dict):
                    equity = inner.get("equity")
                    cash = inner.get("cash")
                    init_cap = inner.get("initial_capital")
                    positions = inner.get("positions", [])
                    n_pos = len(positions) if isinstance(positions, list) else "?"
                    log(f"  >> {tier}: equity={equity}, cash={cash}, init_capital={init_cap}, positions={n_pos}")
                    if equity is not None and cash is not None:
                        if isinstance(equity, (int, float)) and isinstance(cash, (int, float)):
                            if cash > equity and equity > 0:
                                record_issue(f"sim_{tier}", "warning", f"Cash ({cash}) > Equity ({equity})")
                    if init_cap is not None and equity is not None:
                        if isinstance(init_cap, (int, float)) and isinstance(equity, (int, float)):
                            ret = (equity - init_cap) / init_cap * 100 if init_cap > 0 else 0
                            stated_ret = inner.get("total_return")
                            log(f"  >> {tier}: computed_return={ret:.2f}%, stated_return={stated_ret}")
            except Exception:
                pass

    # Admin endpoints (prefix /api/v1/admin)
    r = check_api(client, "/api/v1/admin/overview", "admin_overview", token)
    if r and r.status_code == 200:
        try:
            analyze_api_data(r.json(), "admin_overview")
        except Exception:
            pass

    r = check_api(client, "/api/v1/admin/reports", "admin_reports", token, params={"page": 1})
    if r and r.status_code == 200:
        try:
            analyze_api_data(r.json(), "admin_reports")
        except Exception:
            pass

    r = check_api(client, "/api/v1/admin/scheduler/status", "admin_scheduler", token)
    if r and r.status_code == 200:
        try:
            analyze_api_data(r.json(), "admin_scheduler")
        except Exception:
            pass

    r = check_api(client, "/api/v1/admin/users", "admin_users", token, params={"page": 1})
    if r and r.status_code == 200:
        try:
            analyze_api_data(r.json(), "admin_users")
        except Exception:
            pass

    # Platform endpoints
    r = check_api(client, "/api/v1/platform/config", "platform_config", token)
    if r and r.status_code == 200:
        try:
            analyze_api_data(r.json(), "platform_config")
        except Exception:
            pass

    r = check_api(client, "/api/v1/platform/plans", "platform_plans", token)
    if r and r.status_code == 200:
        try:
            analyze_api_data(r.json(), "platform_plans")
        except Exception:
            pass

    r = check_api(client, "/api/v1/platform/summary", "platform_summary", token)
    if r and r.status_code == 200:
        try:
            analyze_api_data(r.json(), "platform_summary")
        except Exception:
            pass

    # Market state
    r = check_api(client, "/api/v1/market/state", "market_state", token)
    if r and r.status_code == 200:
        try:
            analyze_api_data(r.json(), "market_state")
        except Exception:
            pass

    # ─── Step 5: Cross-data consistency ───
    sep("STEP 5: CROSS-DATA CONSISTENCY CHECKS")

    # Market state
    ms_values = cross_data.get("_market_state", {})
    if ms_values:
        unique_ms = set(str(v) for v in ms_values.values())
        log(f"Market state values: {dict(ms_values)}")
        if len(unique_ms) > 1:
            record_issue("cross-check", "critical", f"Market state inconsistency across APIs: {dict(ms_values)}")
        else:
            log(f"  [OK] Market state consistent: {unique_ms.pop()}")

    # Pool size
    ps_values = cross_data.get("_pool_size", {})
    if ps_values:
        log(f"Pool size values: {dict(ps_values)}")
        unique_ps = set(str(v) for v in ps_values.values())
        if len(unique_ps) > 1:
            record_issue("cross-check", "warning", f"Pool size inconsistency: {dict(ps_values)}")

    # Report counts
    rc_values = cross_data.get("_report_count", {})
    if rc_values:
        log(f"Report count values: {dict(rc_values)}")

    # Dates
    date_values = cross_data.get("_dates", {})
    if date_values:
        log(f"Date values: {dict(date_values)}")

    # ─── Step 6: Summary ───
    sep("FINAL SUMMARY")
    critical = [i for i in all_issues if i["severity"] == "critical"]
    warnings = [i for i in all_issues if i["severity"] == "warning"]
    infos = [i for i in all_issues if i["severity"] == "info"]

    log(f"\nTotal issues found: {len(all_issues)}")
    log(f"  CRITICAL: {len(critical)}")
    log(f"  WARNING:  {len(warnings)}")
    log(f"  INFO:     {len(infos)}")

    if critical:
        log("\n--- CRITICAL ISSUES ---")
        for i, issue in enumerate(critical, 1):
            log(f"  {i}. [{issue['location']}] {issue['description']}")

    if warnings:
        log("\n--- WARNINGS ---")
        for i, issue in enumerate(warnings, 1):
            log(f"  {i}. [{issue['location']}] {issue['description']}")

    if infos:
        log("\n--- INFO ---")
        for i, issue in enumerate(infos, 1):
            log(f"  {i}. [{issue['location']}] {issue['description']}")

    client.close()
    log("\n=== TEST COMPLETE ===")


if __name__ == "__main__":
    main()
