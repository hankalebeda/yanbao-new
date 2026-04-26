"""v24 硬闸端到端验证：用内部 API 请求生成 1 篇研报，预期 422 REPORT_DATA_INCOMPLETE。"""
import httpx, json
TOKEN = "kestra-internal-20260327"
URL = "http://127.0.0.1:8000/api/v1/internal/reports/generate-batch"

payload = {
    "stock_codes": ["600519.SH"],
    "trade_date": "2026-04-16",
    "force": True,
    "skip_pool_check": False,
    "cleanup_incomplete_before_batch": True,
}
with httpx.Client(trust_env=False, timeout=180.0) as c:
    r = c.post(URL, headers={"X-Internal-Token": TOKEN, "Content-Type": "application/json"}, json=payload)
    print("HTTP", r.status_code)
    try:
        body = r.json()
        print(json.dumps(body, ensure_ascii=False, indent=2)[:2500])
    except Exception:
        print(r.text[:2500])
