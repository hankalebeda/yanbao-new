"""Regenerate 3 reports via API."""
import requests
import json
import time
import os

# Bypass proxy for localhost
os.environ["NO_PROXY"] = "localhost,127.0.0.1"
os.environ["no_proxy"] = "localhost,127.0.0.1"

session = requests.Session()
session.trust_env = False  # Ignore system proxy

BASE = "http://localhost:8010/api/v1"
stocks = ["600519.SH", "002594.SZ", "000858.SZ"]

for stock in stocks:
    print(f"Generating report for {stock}...")
    resp = session.post(
        f"{BASE}/reports/generate",
        json={"stock_code": stock, "source": "real", "trade_date": "2026-04-16"},
        timeout=300,
    )
    print(f"  Status: {resp.status_code}")
    data = resp.json()
    if data.get("success"):
        r = data["data"]
        print(f"  report_id: {r.get('report_id', '?')}")
        print(f"  recommendation: {r.get('recommendation', '?')}")
        print(f"  llm_level: {r.get('llm_fallback_level', '?')}")
        print(f"  quality_flag: {r.get('quality_flag', '?')}")
        print(f"  published: {r.get('publish_status', '?')}")
    else:
        print(f"  ERROR: {json.dumps(data, ensure_ascii=False)[:300]}")
    print()
    time.sleep(2)

print("All done!")
