import httpx, json
r = httpx.get("http://127.0.0.1:8099/api/v1/pool/stocks")
d = r.json()["data"]
print(f"total={d['total']}, trade_date={d['trade_date']}")
items = d.get("items", [])
if items:
    print(f"items[0]={json.dumps(items[0], ensure_ascii=False)[:200]}")
else:
    print("NO ITEMS")
