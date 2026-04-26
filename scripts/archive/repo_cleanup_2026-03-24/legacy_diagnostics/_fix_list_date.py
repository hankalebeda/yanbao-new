"""一次性脚本：从东方财富批量拉取上市日期，补全 stock_master.list_date"""
import httpx
import sqlite3
from datetime import datetime, date
import time

DB = "data/app.db"

def fetch_page(pn: int, pz: int = 5000):
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": pn, "pz": pz, "po": 1, "np": 1, "fltt": 2, "invt": 2,
        "fid": "f3",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
        "fields": "f12,f14,f26",  # f12=代码, f14=名称, f26=上市日期(YYYYMMDD int)
    }
    r = httpx.get(url, params=params, timeout=30, verify=False)
    data = r.json()
    total = data.get("data", {}).get("total", 0)
    items = data.get("data", {}).get("diff", [])
    return total, items

def code_to_full(code6: str) -> str:
    if code6.startswith(("6", "9")):
        return f"{code6}.SH"
    elif code6.startswith(("4", "8")):
        return f"{code6}.BJ"
    else:
        return f"{code6}.SZ"

def main():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # 拉取所有页
    all_items = []
    pn = 1
    while True:
        total, items = fetch_page(pn, 5000)
        all_items.extend(items)
        print(f"page {pn}: got {len(items)}, total so far {len(all_items)}/{total}")
        if len(all_items) >= total or len(items) == 0:
            break
        pn += 1
        time.sleep(0.3)

    print(f"\nTotal fetched: {len(all_items)}")

    updated = 0
    now = datetime.now().isoformat()
    for item in all_items:
        code6 = str(item.get("f12", "")).zfill(6)
        list_date_int = item.get("f26")  # e.g. 19910403 or "-"
        if not list_date_int or list_date_int == "-" or not isinstance(list_date_int, (int, float)):
            continue
        try:
            ld = date(int(str(int(list_date_int))[:4]),
                      int(str(int(list_date_int))[4:6]),
                      int(str(int(list_date_int))[6:8]))
        except (ValueError, IndexError):
            continue

        full_code = code_to_full(code6)
        cur.execute(
            "UPDATE stock_master SET list_date=?, updated_at=? WHERE stock_code=? AND list_date IS NULL",
            (ld.isoformat(), now, full_code)
        )
        updated += cur.rowcount

    conn.commit()
    r = cur.execute("SELECT COUNT(*) FROM stock_master WHERE list_date IS NULL").fetchone()
    print(f"Updated list_date: {updated}")
    print(f"Still NULL list_date: {r[0]}")
    conn.close()

if __name__ == "__main__":
    main()
