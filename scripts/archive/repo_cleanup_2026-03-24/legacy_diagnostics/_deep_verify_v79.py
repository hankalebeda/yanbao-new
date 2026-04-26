"""Deep verification of report detail APIs and HTML rendering for v7.9"""
import httpx
import json

BASE = "http://127.0.0.1:8099"

def login(client, email, password):
    r = client.post(f"{BASE}/auth/login", json={"email": email, "password": password})
    return r.json()["data"]["access_token"]

with httpx.Client(timeout=15) as c:
    admin_tok = login(c, "admin@example.com", "Qwer1234..")
    pro_tok = login(c, "v79_pro@test.com", "TestPro123!")
    free_tok = login(c, "v79_free@test.com", "TestFree123!")

    # Get a report ID
    home = c.get(f"{BASE}/api/v1/home").json()["data"]
    rid = home["latest_reports"][0]["report_id"]
    print(f"Report: {rid}\n")

    # Check report detail with different roles
    for role, tok in [("admin", admin_tok), ("pro", pro_tok), ("free", free_tok)]:
        headers = {"Authorization": f"Bearer {tok}"}
        r = c.get(f"{BASE}/api/v1/reports/{rid}", headers=headers)
        d = r.json()["data"]
        ic = d.get("instruction_card", {})
        cap = d.get("capital_game_summary", {})
        tc = d.get("term_context", {})
        
        print(f"=== {role.upper()} ===")
        print(f"  instruction_card.signal_entry_price: {ic.get('signal_entry_price')}")
        print(f"  instruction_card.stop_loss: {ic.get('stop_loss')}")
        print(f"  instruction_card.target_price: {ic.get('target_price')}")
        print(f"  instruction_card.atr_pct: {ic.get('atr_pct')}")
        print(f"  capital_game_summary.headline: {cap.get('headline')}")
        print(f"  capital_game_summary.northbound.status: {cap.get('northbound', {}).get('status') if cap.get('northbound') else 'None'}")
        print(f"  capital_game_summary.etf_flow.status: {cap.get('etf_flow', {}).get('status') if cap.get('etf_flow') else 'None'}")
        print(f"  term_context.ATR: {tc.get('ATR', 'N/A')[:80]}")
        print(f"  degraded_banner: {d.get('degraded_banner')}")
        print(f"  quality_flag: {d.get('quality_flag')}")
        print()

    # Check advanced area
    print("=== ADVANCED AREA ===")
    for role, tok in [("admin", admin_tok), ("pro", pro_tok), ("free", free_tok)]:
        headers = {"Authorization": f"Bearer {tok}"}
        r = c.get(f"{BASE}/api/v1/reports/{rid}/advanced", headers=headers)
        d = r.json()["data"]
        print(f"  [{role}] is_truncated={d.get('is_truncated')}, "
              f"reasoning_chain_len={len(d.get('reasoning_chain','') or '')}, "
              f"used_data_lineage={len(d.get('used_data_lineage', []))}")

    # Check sim dashboard for admin
    print("\n=== SIM DASHBOARD (admin) ===")
    for tier in ("500k", "100k", "10k"):
        r = c.get(f"{BASE}/api/v1/portfolio/sim-dashboard?capital_tier={tier}",
                  headers={"Authorization": f"Bearer {admin_tok}"})
        if r.status_code == 200:
            d = r.json()["data"]
            ps = d.get("performance_stats", {})
            print(f"  {tier}: total_reports={ps.get('total_reports')}, "
                  f"total_settled={ps.get('total_settled')}, "
                  f"wr={ps.get('overall_win_rate')}, "
                  f"signal_warning={ps.get('signal_validity_warning')}")
            eq = d.get("equity_curve", [])
            print(f"  {tier}: equity_curve points={len(eq)}")
            op = d.get("open_positions", [])
            print(f"  {tier}: open_positions={len(op)}")
        else:
            print(f"  {tier}: {r.status_code}")

    # Check pool/stocks
    print("\n=== POOL/STOCKS ===")
    r = c.get(f"{BASE}/api/v1/pool/stocks")
    d = r.json()["data"]
    print(f"  total: {d.get('total')}, trade_date: {d.get('trade_date')}, items: {len(d.get('items', []))}")
    if d.get("items"):
        item0 = d["items"][0]
        print(f"  first: {item0.get('stock_code')} {item0.get('stock_name', 'N/A')}")

    print("\n=== DASHBOARD STATS ===")
    r = c.get(f"{BASE}/api/v1/dashboard/stats?window_days=30")
    d = r.json()["data"]
    print(
        "  total_reports={total_reports}, total_settled={total_settled}, "
        "status={data_status}, reason={status_reason}, hint={display_hint}, warning={signal_validity_warning}".format(
            **d
        )
    )
    for strategy in ("A", "B", "C"):
        row = (d.get("by_strategy_type") or {}).get(strategy, {})
        print(
            f"  {strategy}: sample={row.get('sample_size')}, "
            f"coverage={row.get('coverage_pct')}, warning={row.get('signal_validity_warning')}, "
            f"hint={row.get('display_hint')}"
        )

    # Check platform/plans
    print("\n=== PLATFORM/PLANS ===")
    r = c.get(f"{BASE}/api/v1/platform/plans")
    plans = r.json()["data"]
    for p in (plans if isinstance(plans, list) else plans.get("plans", [])):
        print(f"  {p.get('tier', p.get('name'))}: price_monthly={p.get('price_monthly')}, "
              f"price_quarterly={p.get('price_quarterly')}, price_yearly={p.get('price_yearly')}")

    # HTML pages with Pro token
    print("\n=== HTML PAGES (Pro user) ===")
    cookies = {"access_token": pro_tok}
    for path in ("/", "/reports", "/dashboard", f"/reports/{rid}", "/subscribe", "/profile"):
        r = c.get(f"{BASE}{path}", cookies=cookies, follow_redirects=False)
        print(f"  {path}: {r.status_code}")
