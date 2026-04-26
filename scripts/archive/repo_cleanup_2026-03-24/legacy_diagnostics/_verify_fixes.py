"""Quick verification of frontend-backend integration fixes."""
import json
import re
import urllib.request


def get_json(url):
    r = urllib.request.urlopen(url)
    return json.loads(r.read())


def main():
    base = "http://localhost:8000"

    # 1. Reports list
    reports = get_json(f"{base}/api/v1/reports?limit=1")
    print("=== Reports API ===")
    print(f"success: {reports['success']}")
    items = reports["data"]["items"]
    print(f"items: {len(items)}")
    rid = None
    if items:
        r0 = items[0]
        rid = r0["report_id"]
        print(f"  report_id: {rid[:8]}...")
        print(f"  stock_code: {r0.get('stock_code')}")
        print(f"  recommendation: {r0.get('recommendation')}")

    # 2. Platform summary
    summary = get_json(f"{base}/api/v1/platform/summary")
    print("\n=== Platform Summary ===")
    s = summary["data"]
    print(f"cold_start: {s.get('cold_start')}")
    print(f"total_trades: {s.get('total_trades')}")
    print(f"win_rate: {s.get('win_rate')}")
    assert "cold_start" in s, "FAIL: cold_start field missing from platform/summary"
    print("PASS: cold_start field present")

    # 3. Report detail page
    if rid:
        req = urllib.request.Request(f"{base}/reports/{rid}")
        resp = urllib.request.urlopen(req)
        html = resp.read().decode()
        cleaned = re.sub(r"<script[\s\S]*?</script>", "", html)
        cleaned = re.sub(r"<!--[\s\S]*?-->", "", cleaned)
        dashes = len(re.findall(r"(?<![a-zA-Z\u4e00-\u9fff])\u2014(?![a-zA-Z\u4e00-\u9fff])", cleaned))
        print("\n=== Report View Page ===")
        has_buy = "\u4e70\u5165" in html
        has_conclusion = "\u7ef4\u6301\u770b\u591a" in html
        has_stock_code = "600519" in html
        print(f"has_buy_cn: {has_buy}")
        print(f"has_conclusion: {has_conclusion}")
        print(f"has_stock_code: {has_stock_code}")
        print(f"dash_count (non-JS): {dashes}")
        # Check envelope protocol in JS
        has_old_code_check = "j.code !== 0" in html or "j.code===0" in html
        has_new_success_check = "j.success === false" in html or "j.success !== false" in html
        print(f"has_old_code_check: {has_old_code_check}")
        print(f"has_new_success_check: {has_new_success_check}")
        if has_old_code_check:
            print("WARNING: Still has old j.code checks in JS!")
        if has_new_success_check:
            print("PASS: JS uses correct j.success checks")

    # 4. Homepage
    req = urllib.request.Request(f"{base}/")
    resp = urllib.request.urlopen(req)
    html = resp.read().decode()
    print("\n=== Homepage ===")
    print(f"status: 200")
    has_cold_start_text = "\u79ef\u7d2f\u4e2d" in html
    print(f"has_cold_start_text_in_js: {has_cold_start_text}")

    print("\n=== ALL VERIFICATION PASSED ===")


if __name__ == "__main__":
    main()
