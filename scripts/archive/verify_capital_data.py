import asyncio
import httpx
from datetime import datetime
import json

# Eastmoney API for LHB (Dragon Tiger List)
# Note: This is a REVERSE ENGINEERED API endpoint, use with caution.
# It fetches "Daily Billboard Details"

async def fetch_lhb_data(date_str: str = None):
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
        
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_DAILY_BILLBOARD_DETAILSV2",
        "columns": "ALL",
        "filter": f"(TRADE_DATE='{date_str}')",
        "pageNumber": 1,
        "pageSize": 500, 
        "sortTypes": "-1",
        "sortColumns": "TRADE_DATE",
        "source": "WEB",
        "client": "WEB",
    }
    
    print(f"Fetching LHB data for {date_str}...")
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url, params=params, timeout=10.0)
            resp.raise_for_status()
            data = resp.json()
            if data.get("result") and data["result"].get("data"):
                items = data["result"]["data"]
                print(f"Success! Found {len(items)} LHB records.")
                # Print first 3
                for item in items[:3]:
                    code = item.get("SECURITY_CODE")
                    name = item.get("SECURITY_NAME_ABBR")
                    reason = item.get("EXPLANATION")
                    net_buy = item.get("NET_BUY_AMT")
                    print(f"- {code} {name}: {reason} (Net Buy: {net_buy})")
                return items
            else:
                print("No LHB data found (market might be closed or API changed).")
                return []
        except Exception as e:
            print(f"Error fetching LHB: {e}")
            return []

async def fetch_northbound_flow():
    # Northbound money flow (overall)
    url = "https://push2.eastmoney.com/api/qt/kamt/get"
    params = {
        "fields1": "f1,f2,f3,f4",
        "fields2": "f51,f52,f53,f54", # f51=Northbound Net Inflow? Need to verify
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "cb": "jQuery1830635293294332215_1587889874837", # dummy callback
        "_": "1587889875150"
    }
    print("\nFetching Northbound Fund Flow (Overall)...")
    async with httpx.AsyncClient() as client:
        try:
             # Just a test request to see if we can reach Eastmoney push API
             resp = await client.get(url, params=params, timeout=10.0)
             if resp.status_code == 200:
                 print("Success! Northbound API unreachable (or returns specific format). This requires more specific parsing.")
                 # Real implementation would parse the jQuery wrapper or use a cleaner JSON API
                 # For now, just confirming network connectivity to Eastmoney push service.
             else:
                 print(f"Failed with status {resp.status_code}")
        except Exception as e:
            print(f"Error fetching Northbound: {e}")

if __name__ == "__main__":
    # Test LHB
    asyncio.run(fetch_lhb_data())
    # Test Northbound
    asyncio.run(fetch_northbound_flow())
