#!/usr/bin/env python3
"""Round 1/2 验证：请求列表 API、predictions/stats、单条报告详情与 report_data_usage。"""
import json
import sys

def main():
    import urllib.request
    url_base = "http://127.0.0.1:8000"
    # List
    with urllib.request.urlopen(f"{url_base}/api/v1/reports?page=1&page_size=5", timeout=10) as f:
        list_data = json.loads(f.read().decode())
    data = list_data.get("data", {})
    items = data.get("items", [])
    total = data.get("total", 0)
    print("LIST_OK", "total=", total, "items=", len(items))
    if items:
        first = items[0]
        print("first_report_id", first.get("report_id"))
        print("first_stock", first.get("stock_code"), first.get("stock_name"))
    # Stats
    with urllib.request.urlopen(f"{url_base}/api/v1/predictions/stats", timeout=10) as f:
        stats_data = json.loads(f.read().decode())
    s = stats_data.get("data", {})
    print("STATS_OK")
    print("accuracy", s.get("accuracy"))
    print("total_judged", s.get("total_judged"))
    print("by_window", json.dumps(s.get("by_window", [])[:3], ensure_ascii=False))
    # Detail API root fields + report_data_usage (Round 2)
    if items:
        rid = items[0].get("report_id")
        with urllib.request.urlopen(f"{url_base}/api/v1/reports/{rid}", timeout=10) as f:
            detail = json.loads(f.read().decode())
        d = detail.get("data", {})
        root_ok = all(d.get(k) is not None for k in ["report_id", "stock_code"])
        print("DETAIL_ROOT_OK", root_ok)
        usage = d.get("report_data_usage") or {}
        sources = usage.get("sources") or []
        print("REPORT_DATA_USAGE_SOURCES", len(sources))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERR", str(e), file=sys.stderr)
        sys.exit(1)
