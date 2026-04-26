"""Phase 5+6: 全面验证脚本"""
import httpx

import os
BASE = os.environ.get("VALIDATE_BASE_URL", "http://127.0.0.1:8000")

def check(name, method, path, expected_status=200, check_fn=None):
    try:
        r = httpx.request(method, f"{BASE}{path}", timeout=15, follow_redirects=True)
        ok = r.status_code == expected_status
        extra = ""
        if check_fn and ok:
            try:
                extra = check_fn(r)
            except Exception as e:
                extra = f"CHECK_FAIL: {e}"
                ok = False
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {name:40s}  HTTP {r.status_code}  {extra}")
        return ok
    except Exception as e:
        print(f"  [FAIL] {name:40s}  ERROR: {e}")
        return False

def main():
    results = []

    print("=" * 70)
    print("Phase 5: 端点验证")
    print("=" * 70)

    # Health
    results.append(check("Health", "GET", "/health",
        check_fn=lambda r: f"status={r.json()['data']['status']}"))

    # Home page
    results.append(check("Home Page", "GET", "/",
        check_fn=lambda r: f"len={len(r.text)}"))

    # Reports list
    results.append(check("Reports List", "GET", "/api/v1/reports",
        check_fn=lambda r: f"total={r.json()['data']['total']}"))

    # Report detail
    results.append(check("Report Detail (first)", "GET", "/api/v1/reports",
        check_fn=lambda r: _check_report_detail(r)))

    # Dashboard
    results.append(check("Dashboard Stats", "GET", "/api/v1/dashboard/stats",
        check_fn=lambda r: f"total_reports={r.json()['data']['total_reports']}"))

    # Admin login page
    results.append(check("Admin Page", "GET", "/admin",
        check_fn=lambda r: f"len={len(r.text)}"))

    # Market state API (current SSOT endpoint)
    results.append(check("Market State API", "GET", "/api/v1/market/state",
        check_fn=lambda r: f"state={r.json().get('data',{}).get('market_state','?')}"))

    # Home payload API
    results.append(check("Home Payload API", "GET", "/api/v1/home",
        check_fn=lambda r: f"status={r.json().get('data',{}).get('data_status','?')}"))

    print(f"\n{'=' * 70}")
    print(f"Phase 6: 10 股报告详情验证")
    print(f"{'=' * 70}")

    # Get all 10 reports
    try:
        r = httpx.get(f"{BASE}/api/v1/reports?page_size=100", timeout=10)
        items = r.json()["data"]["items"]
        for item in items:
            rid = item.get("report_id", "?")
            code = item.get("stock_code", "?")
            name = item.get("stock_name_snapshot", "?")
            rec = item.get("recommendation", "?")
            conf = item.get("confidence", "?")
            quality = item.get("quality_flag", "?")
            print(f"  {code:12s} {name:10s}  rec={rec:6s}  conf={conf}  quality={quality}  id={rid[:8]}")

            # Get detail
            dr = httpx.get(f"{BASE}/api/v1/reports/{rid}", timeout=10)
            if dr.status_code == 200:
                detail = dr.json().get("data", {})
                conclusion = detail.get("conclusion_text", "")[:60]
                citations = len(detail.get("citations", []))
                instructions = len(detail.get("instruction_cards", []))
                print(f"    conclusion={conclusion}...")
                print(f"    citations={citations}, instructions={instructions}")
                results.append(True)
            else:
                print(f"    detail FAIL: {dr.status_code}")
                results.append(False)

            # Get advanced
            ar = httpx.get(f"{BASE}/api/v1/reports/{rid}/advanced", timeout=10)
            if ar.status_code == 200:
                adv = ar.json().get("data", {})
                reasoning = (adv.get("reasoning_chain_md") or "")[:60]
                data_used = len(adv.get("data_used", []))
                print(f"    advanced: reasoning={reasoning}...")
                print(f"    data_used_count={data_used}")
                results.append(True)
            else:
                print(f"    advanced FAIL: {ar.status_code}")
                results.append(False)
    except Exception as e:
        print(f"  ERROR: {e}")
        results.append(False)

    passed = sum(1 for r in results if r)
    total = len(results)
    print(f"\n{'=' * 70}")
    print(f"总结: {passed}/{total} 通过")
    print(f"{'=' * 70}")


def _check_report_detail(r):
    items = r.json()["data"]["items"]
    if not items:
        raise AssertionError("no items")
    rid = items[0]["report_id"]
    r2 = httpx.get(f"{BASE}/api/v1/reports/{rid}", timeout=10)
    if r2.status_code != 200:
        raise AssertionError(f"detail status={r2.status_code}")
    d = r2.json()["data"]
    return f"id={rid[:8]} rec={d.get('recommendation')}"


if __name__ == "__main__":
    main()
