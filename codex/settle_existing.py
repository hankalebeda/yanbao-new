"""
触发已发布报告的 1d 结算（仅对有出场日K线数据的股票）
- 000858.SZ / 002594.SZ: 报告日 2026-04-16, 出场日 2026-04-17 ✅
- 688002.SH:             报告日 2026-04-21, 出场日 2026-04-22 ✅
- 600519.SH:             报告日 2026-04-21, 出场日 2026-04-22 ✗ (无K线)
"""
import json
import time
import requests

BASE = "http://127.0.0.1:8010"
TOKEN = "kestra-internal-20260327"
HEADERS = {"X-Internal-Token": TOKEN, "Content-Type": "application/json"}
NO_PROXY = {"http": None, "https": None}

TO_SETTLE = [
    ("000858.SZ", "2026-04-16", 1),
    ("002594.SZ", "2026-04-16", 1),
    ("688002.SH", "2026-04-21", 1),
]


def trigger_settlement(stock_code: str, trade_date: str, window_days: int) -> dict:
    payload = {
        "trade_date": trade_date,
        "window_days": window_days,
        "target_scope": "stock_code",
        "target_stock_code": stock_code,
        "force": True,
    }
    resp = requests.post(
        f"{BASE}/api/v1/internal/settlement/run",
        headers=HEADERS,
        json=payload,
        timeout=120,
        proxies=NO_PROXY,
    )
    return resp.json()


def main():
    print("=== Triggering settlement for existing published reports ===\n")
    for stock_code, trade_date, window_days in TO_SETTLE:
        print(f"Settling {stock_code} {trade_date} window_days={window_days}...")
        try:
            result = trigger_settlement(stock_code, trade_date, window_days)
            status = result.get("data", {}).get("status") or result.get("data", {}).get("task_status")
            task_id = result.get("data", {}).get("task_id") or result.get("data", {}).get("id")
            print(f"  -> status={status} task_id={task_id}")
            if result.get("error"):
                print(f"  -> ERROR: {result['error']}")
        except Exception as e:
            print(f"  -> EXCEPTION: {e}")
        time.sleep(2)  # brief pause between calls

    print("\n=== Checking dashboard stats after settlement ===")
    try:
        r = requests.get(f"{BASE}/api/v1/dashboard/stats?window_days=7", timeout=30, proxies=NO_PROXY)
        data = r.json().get("data", {})
        print(f"  total_reports={data.get('total_reports')}")
        print(f"  total_settled={data.get('total_settled')}")
        print(f"  overall_win_rate={data.get('overall_win_rate')}")
        print(f"  data_status={data.get('data_status')}")
        print(f"  status_reason={data.get('status_reason')}")
    except Exception as e:
        print(f"  Dashboard check failed: {e}")


if __name__ == "__main__":
    main()
