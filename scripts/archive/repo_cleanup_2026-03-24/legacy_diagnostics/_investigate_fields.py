#!/usr/bin/env python3
"""调查研报详情API返回字段"""
import httpx, json, sys
sys.stdout.reconfigure(encoding='utf-8')

BASE = "http://127.0.0.1:8099"
r = httpx.post(BASE + "/auth/login", json={"email": "admin@example.com", "password": "Qwer1234.."})
token = r.json()["data"]["access_token"]
hdrs = {"Authorization": f"Bearer {token}"}

r2 = httpx.get(BASE + "/api/v1/reports?page=1", headers=hdrs)
rpt_id = r2.json()["data"]["items"][0]["report_id"]
r3 = httpx.get(f"{BASE}/api/v1/reports/{rpt_id}", headers=hdrs)
data = r3.json()["data"]
print("Report fields:", list(data.keys()))
print("citations[0] fields:", list(data["citations"][0].keys()) if data.get("citations") else "empty")
print("analysis_steps:", data.get("analysis_steps", "NOT PRESENT"))
print("stock_name:", data.get("stock_name", "NOT PRESENT"))
print("industry:", data.get("industry", "NOT PRESENT"))

# 高级区
r4 = httpx.get(f"{BASE}/api/v1/reports/{rpt_id}/advanced", headers=hdrs)
adv = r4.json().get("data", {})
print("\nAdvanced fields:", list(adv.keys()))

# Market state
r5 = httpx.get(f"{BASE}/api/v1/market/state", headers=hdrs)
mkt = r5.json().get("data", {})
print("\nMarket state fields:", list(mkt.keys()))
print("Market state data:", mkt)
