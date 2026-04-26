"""v7.9 browser verification — comprehensive test of all pages and API endpoints
with admin, Pro, Free, and anonymous roles."""
import httpx
import json
import sys
from typing import Any

BASE = "http://127.0.0.1:8099"

# Use existing known test accounts
USERS = {
    "admin": {"email": "admin@example.com", "password": "Qwer1234.."},
    "pro": {"email": "v79_pro@test.com", "password": "TestPro123!"},
    "free": {"email": "v79_free@test.com", "password": "TestFree123!"},
}

results: list[dict[str, Any]] = []


def log(category: str, status: str, msg: str, detail: str = ""):
    entry = {"category": category, "status": status, "msg": msg}
    if detail:
        entry["detail"] = detail[:300]
    results.append(entry)
    icon = "✅" if status == "PASS" else "❌" if status == "FAIL" else "⚠️"
    print(f"  {icon} [{category}] {msg}" + (f" | {detail[:120]}" if detail else ""))


def login(client: httpx.Client, role: str) -> str | None:
    """Login and return JWT token."""
    creds = USERS[role]
    resp = client.post(f"{BASE}/auth/login",
                       json={"email": creds["email"], "password": creds["password"]},
                       follow_redirects=False)
    if resp.status_code == 200:
        try:
            body = resp.json()
            data = body.get("data", body)
            token = data.get("access_token")
            if token:
                return token
        except Exception:
            pass
        # Try cookie
        token = resp.cookies.get("access_token")
        if token:
            return token
    return None


def api_get(client: httpx.Client, path: str, token: str | None = None) -> httpx.Response:
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return client.get(f"{BASE}{path}", headers=headers, follow_redirects=True)


def check_json(resp: httpx.Response, path: str, role: str, required_keys: list[str] | None = None) -> dict | list | None:
    if resp.status_code != 200:
        log(f"API-{role}", "FAIL", f"{path} returned {resp.status_code}")
        return None
    try:
        body = resp.json()
    except Exception:
        log(f"API-{role}", "FAIL", f"{path} not JSON")
        return None
    # Unwrap envelope if present
    data = body
    if isinstance(body, dict) and "data" in body and "success" in body:
        data = body["data"]
    if required_keys and isinstance(data, dict):
        missing = [k for k in required_keys if k not in data]
        if missing:
            log(f"API-{role}", "FAIL", f"{path} missing keys: {missing}", f"got: {list(data.keys())[:15]}")
            return data
    log(f"API-{role}", "PASS", f"{path} OK")
    return data


def check_html(client: httpx.Client, path: str, role: str, token: str | None = None, expect_status: int = 200):
    cookies = {}
    if token:
        cookies["access_token"] = token
    resp = client.get(f"{BASE}{path}", cookies=cookies, follow_redirects=False)
    if expect_status == 302:
        if resp.status_code in (302, 303, 307):
            log(f"HTML-{role}", "PASS", f"{path} redirects as expected → {resp.headers.get('location','?')}")
        else:
            log(f"HTML-{role}", "FAIL", f"{path} expected redirect, got {resp.status_code}")
        return resp
    if resp.status_code != expect_status:
        log(f"HTML-{role}", "FAIL", f"{path} expected {expect_status}, got {resp.status_code}")
    else:
        log(f"HTML-{role}", "PASS", f"{path} → {resp.status_code}")
    return resp


def main():
    print("=" * 70)
    print("V7.9 COMPREHENSIVE BROWSER VERIFICATION")
    print("=" * 70)

    with httpx.Client(timeout=30) as client:
        # ── 1. Login all roles ──
        print("\n── 1. Authentication ──")
        tokens: dict[str, str | None] = {}
        for role in ("admin", "pro", "free"):
            tok = login(client, role)
            tokens[role] = tok
            if tok:
                log("AUTH", "PASS", f"{role} login OK")
            else:
                log("AUTH", "FAIL", f"{role} login FAILED")

        # ── 2. Public HTML pages ──
        print("\n── 2. Public HTML Pages (anonymous) ──")
        for path in ("/", "/subscribe", "/login", "/reports"):
            check_html(client, path, "anon")

        # ── 3. Protected HTML pages - anonymous should redirect ──
        print("\n── 3. Protected Pages (anonymous → redirect) ──")
        for path in ("/admin", "/portfolio/sim-dashboard"):
            check_html(client, path, "anon", expect_status=302)

        # ── 4. Protected HTML pages - admin ──
        print("\n── 4. Admin HTML Pages ──")
        for path in ("/admin", "/portfolio/sim-dashboard", "/reports"):
            check_html(client, path, "admin", token=tokens.get("admin"))

        # ── 5. Public API endpoints ──
        print("\n── 5. Public API Endpoints ──")
        # Home
        home = check_json(api_get(client, "/api/v1/home"), "/api/v1/home", "public",
                         ["pool_size", "latest_reports", "market_overview"])
        if home:
            pool_size = home.get("pool_size")
            reports = home.get("latest_reports", [])
            print(f"     pool_size={pool_size}, latest_reports={len(reports)}")
            if reports:
                r0 = reports[0]
                print(f"     first report: {r0.get('stock_code')} rec={r0.get('recommendation')} conf={r0.get('confidence')}")

        # Pool stocks
        pool = check_json(api_get(client, "/api/v1/pool/stocks"), "/api/v1/pool/stocks", "public",
                         ["total", "trade_date", "stocks"])
        if pool:
            total = pool.get("total", 0)
            trade_date = pool.get("trade_date")
            print(f"     total={total}, trade_date={trade_date}")
            if total == 0:
                log("DATA", "FAIL", "pool/stocks returned 0 stocks")

        # Subscribe plans
        plans = check_json(api_get(client, "/api/v1/platform/plans"), "/api/v1/platform/plans", "public")
        if plans and isinstance(plans, dict):
            plan_data = plans.get("data", plans)
            plan_list = plan_data.get("plans", []) if isinstance(plan_data, dict) else plan_data
            if isinstance(plan_list, list):
                for p in plan_list:
                    name = p.get("name") or p.get("tier")
                    price = p.get("price_monthly") or p.get("price")
                    print(f"     Plan: {name} → {price}")
            elif isinstance(plan_data, list):
                for p in plan_data:
                    name = p.get("name") or p.get("tier")
                    price = p.get("price_monthly") or p.get("price")
                    print(f"     Plan: {name} → {price}")

        # ── 6. Report detail API ──
        print("\n── 6. Report Detail API ──")
        for role, tok in tokens.items():
            # Get a report ID from home
            home_resp = api_get(client, "/api/v1/home", tok)
            try:
                home_data = home_resp.json()
                reports_list = home_data.get("latest_reports", [])
                if reports_list:
                    rid = reports_list[0].get("report_id")
                    detail = check_json(
                        api_get(client, f"/api/v1/reports/{rid}", tok),
                        f"/api/v1/reports/<id>",
                        role,
                        ["report_id", "instruction_card", "capital_game_summary"],
                    )
                    if detail:
                        ic = detail.get("instruction_card", {})
                        cap = detail.get("capital_game_summary", {})
                        print(f"     [{role}] entry_price={ic.get('signal_entry_price')}, "
                              f"headline={cap.get('headline','N/A')[:50]}")
                        # Check headline doesn't have English "missing"
                        headline = cap.get("headline") or ""
                        if "missing" in headline.lower():
                            log(f"DATA-{role}", "FAIL",
                                "capital headline contains English 'missing'",
                                headline)
                        # Check term_context
                        tc = detail.get("term_context", {})
                        if tc.get("ATR"):
                            log(f"DATA-{role}", "PASS", f"term_context.ATR present: {str(tc.get('ATR'))[:60]}")
                        else:
                            log(f"DATA-{role}", "WARN", "term_context.ATR missing")
            except Exception as e:
                log(f"REPORT-{role}", "FAIL", f"Report detail error: {e}")

        # ── 7. Dashboard/Stats API ──
        print("\n── 7. Dashboard Stats API ──")
        for window in (7, 14, 30, 60):
            resp = api_get(client, f"/api/v1/dashboard/stats?window_days={window}", tokens.get("admin"))
            data = check_json(resp, f"/api/v1/dashboard/stats?window_days={window}", "admin",
                            ["window_days", "total_reports", "total_settled", "overall_win_rate",
                             "baseline_random", "baseline_ma_cross", "signal_validity_warning"])
            if data:
                print(f"     {window}d: reports={data.get('total_reports')}, "
                      f"settled={data.get('total_settled')}, "
                      f"wr={data.get('overall_win_rate')}, "
                      f"signal_warning={data.get('signal_validity_warning')}")

        # ── 8. Admin API ──
        print("\n── 8. Admin API ──")
        admin_resp = api_get(client, "/api/v1/admin/overview", tokens.get("admin"))
        admin_data = check_json(admin_resp, "/api/v1/admin/overview", "admin",
                               ["pool_size", "active_users", "report_generation"])
        if admin_data:
            admin_pool = admin_data.get("pool_size")
            home_pool = home.get("pool_size") if home else None
            print(f"     admin pool_size={admin_pool}, home pool_size={home_pool}")
            if admin_pool != home_pool:
                log("CONSISTENCY", "FAIL",
                    f"pool_size mismatch: admin={admin_pool} vs home={home_pool}")
            else:
                log("CONSISTENCY", "PASS",
                    f"pool_size consistent: admin={admin_pool} == home={home_pool}")

        # Admin RBAC: free user should get 403
        print("\n── 9. RBAC Checks ──")
        free_admin = api_get(client, "/api/v1/admin/overview", tokens.get("free"))
        if free_admin.status_code == 403:
            log("RBAC", "PASS", "Free → admin/overview = 403")
        else:
            log("RBAC", "FAIL", f"Free → admin/overview = {free_admin.status_code}")

        # Sim dashboard API: free should get 403
        free_sim = api_get(client, "/api/v1/portfolio/sim-dashboard?capital_tier=500k", tokens.get("free"))
        if free_sim.status_code == 403:
            log("RBAC", "PASS", "Free → sim-dashboard = 403")
        else:
            log("RBAC", "FAIL", f"Free → sim-dashboard = {free_sim.status_code}")

        # ── 10. Advanced area ──
        print("\n── 10. Advanced Area API ──")
        for role in ("admin", "pro", "free"):
            tok = tokens.get(role)
            home_resp = api_get(client, "/api/v1/home", tok)
            try:
                home_data = home_resp.json()
                reports_list = home_data.get("latest_reports", [])
                if reports_list:
                    rid = reports_list[0].get("report_id")
                    adv_resp = api_get(client, f"/api/v1/reports/{rid}/advanced", tok)
                    adv = check_json(adv_resp, f"/api/v1/reports/<id>/advanced", role)
                    if adv:
                        rc = adv.get("reasoning_chain")
                        is_trunc = adv.get("is_truncated")
                        print(f"     [{role}] reasoning_chain len={len(rc or '')}, is_truncated={is_trunc}")
            except Exception as e:
                log(f"ADVANCED-{role}", "FAIL", str(e))

        # ── 11. Sim Dashboard API ──
        print("\n── 11. Sim Dashboard API ──")
        for tier in ("500k", "200k", "100k"):
            resp = api_get(client, f"/api/v1/portfolio/sim-dashboard?capital_tier={tier}", tokens.get("admin"))
            data = check_json(resp, f"/api/v1/portfolio/sim-dashboard?capital_tier={tier}", "admin")
            if data:
                stats = data.get("performance_stats", {})
                print(f"     [{tier}] total={stats.get('total_reports')}, "
                      f"settled={stats.get('total_settled')}, "
                      f"wr={stats.get('overall_win_rate')}")

    # ── Summary ──
    print("\n" + "=" * 70)
    passes = sum(1 for r in results if r["status"] == "PASS")
    fails = sum(1 for r in results if r["status"] == "FAIL")
    warns = sum(1 for r in results if r["status"] == "WARN")
    print(f"SUMMARY: {passes} PASS, {fails} FAIL, {warns} WARN (total {len(results)})")
    if fails:
        print("\nFAILURES:")
        for r in results:
            if r["status"] == "FAIL":
                print(f"  ❌ [{r['category']}] {r['msg']}" + (f" | {r.get('detail','')}" if r.get('detail') else ""))
    if warns:
        print("\nWARNINGS:")
        for r in results:
            if r["status"] == "WARN":
                print(f"  ⚠️ [{r['category']}] {r['msg']}")

    # Save results
    with open("output/v79_browser_test.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nResults saved to output/v79_browser_test.json")

    return 0 if fails == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
