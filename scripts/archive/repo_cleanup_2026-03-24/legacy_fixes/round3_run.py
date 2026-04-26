#!/usr/bin/env python3
"""Round 3 自动化：验证 + 可选预测结算 + 再次验证。"""
import json
import sys
import urllib.request
import urllib.error

URL_BASE = "http://127.0.0.1:8000"


def get(path, timeout=10):
    req = urllib.request.Request(URL_BASE + path, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as f:
        return json.loads(f.read().decode())


def post_json(path, body, timeout=15):
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        URL_BASE + path,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as f:
        return json.loads(f.read().decode())


def main():
    print("=== Round3 执行 ===\n1) 初始验证")
    j = get("/api/v1/reports?page=1&page_size=3")
    items = (j.get("data") or {}).get("items") or []
    if not items:
        print("LIST_EMPTY 无报告，跳过结算")
        return
    rid = items[0].get("report_id")
    code = items[0].get("stock_code")
    print("LIST_OK report_id=%s stock_code=%s" % (rid, code))

    j = get("/api/v1/predictions/stats")
    d = j.get("data") or {}
    print("STATS_INIT total_judged=%s accuracy=%s" % (d.get("total_judged"), d.get("accuracy")))

    print("\n2) 预测结算（单报告、窗口 1,7）")
    try:
        r = post_json(
            "/api/v1/predictions/settle",
            {"report_id": rid, "stock_code": code, "windows": [1, 7]},
            timeout=30,
        )
        if r.get("code") == 0:
            print("SETTLE_OK count=%s" % (r.get("data") or {}).get("count", 0))
        else:
            print("SETTLE_API_ERR", r.get("message", r))
    except urllib.error.HTTPError as e:
        print("SETTLE_HTTP_ERR", e.code, e.reason)
    except Exception as e:
        print("SETTLE_ERR", str(e))

    print("\n3) 再次验证")
    j = get("/api/v1/predictions/stats")
    d = j.get("data") or {}
    print("STATS_AFTER total_judged=%s accuracy=%s" % (d.get("total_judged"), d.get("accuracy")))
    if d.get("by_window"):
        for w in d["by_window"][:3]:
            print("  window=%s samples=%s accuracy=%s" % (w.get("window_days"), w.get("samples"), w.get("accuracy")))
    print("\n=== Round3 完成 ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("FATAL", str(e), file=sys.stderr)
        sys.exit(1)
