"""
batch generate - 18 stocks with correct trade_dates
Group1: trade_date=2026-04-21 (688002.SH, 600519.SH)
Group2: trade_date=2026-04-16 (16 stocks)
"""
import requests
import json
import time

BASE = "http://127.0.0.1:8010"
TOKEN = "kestra-internal-20260327"
HEADERS = {"X-Internal-Token": TOKEN, "Content-Type": "application/json"}
NO_PROXY = {"http": None, "https": None}

BATCHES = [
    {
        "trade_date": "2026-04-21",
        "stock_codes": ["688002.SH", "600519.SH"],
        "label": "Round-A (2026-04-21)"
    },
    {
        "trade_date": "2026-04-16",
        "stock_codes": [
            "000858.SZ", "002594.SZ", "000001.SZ", "000002.SZ",
            "000069.SZ", "000100.SZ", "000157.SZ", "000301.SZ"
        ],
        "label": "Round-B1 (2026-04-16)"
    },
    {
        "trade_date": "2026-04-16",
        "stock_codes": [
            "000333.SZ", "000400.SZ", "000423.SZ", "000425.SZ",
            "000519.SZ", "000538.SZ", "000568.SZ", "000596.SZ"
        ],
        "label": "Round-B2 (2026-04-16)"
    },
]

total_ok = 0
total_fail = 0

for batch in BATCHES:
    label = batch["label"]
    trade_date = batch["trade_date"]
    stocks = batch["stock_codes"]
    print(f"\n=== {label} | {len(stocks)} stocks ===")

    payload = {
        "stock_codes": stocks,
        "trade_date": trade_date,
        "force": True,
        "skip_pool_check": True,
        "cleanup_incomplete_before_batch": False,
    }

    try:
        resp = requests.post(
            f"{BASE}/api/v1/internal/reports/generate-batch",
            headers=HEADERS,
            json=payload,
            timeout=600,
            proxies=NO_PROXY,
        )
        if resp.status_code in (200, 202):
            data = resp.json().get("data", {})
            succeeded = data.get("succeeded", 0)
            failed = data.get("failed", 0)
            elapsed = data.get("elapsed_s", 0)
            print(f"  total={data.get('total')}, succeeded={succeeded}, failed={failed}, elapsed={elapsed:.1f}s")
            total_ok += succeeded
            total_fail += failed

            for detail in (data.get("details") or []):
                sc = detail.get("stock_code", "?")
                status = detail.get("status", "?")
                r = detail.get("result") or {}
                ec = detail.get("error_code", "")
                if r:
                    rec = r.get("recommendation", "?")
                    conf = r.get("confidence", 0)
                    qf = r.get("quality_flag", "?")
                    pub = r.get("published", False)
                    strategy = r.get("strategy_type", "?")
                    fallback = r.get("llm_fallback_level", "?")
                    print(f"    {sc} [{status}]: {rec} conf={conf} quality={qf} pub={pub} strategy={strategy} llm={fallback}")
                else:
                    print(f"    {sc} [{status}]: error_code={ec}")
        else:
            print(f"  HTTP {resp.status_code}: {resp.text[:400]}")
    except Exception as e:
        print(f"  EXCEPTION: {e}")

    if batch != BATCHES[-1]:
        print("  Waiting 3s...")
        time.sleep(3)

print(f"\n=== FINAL SUMMARY ===")
print(f"Total succeeded: {total_ok}")
print(f"Total failed: {total_fail}")

# DB check
import sqlite3
db = sqlite3.connect(r'd:\yanbao-new\data\app.db')
c = db.cursor()
c.execute('SELECT COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND published=1')
pub = c.fetchone()[0]
c.execute('SELECT trade_date, COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND published=1 GROUP BY trade_date ORDER BY trade_date DESC')
dist = c.fetchall()
c.execute('SELECT COUNT(*) FROM report WHERE is_deleted=0 OR is_deleted IS NULL')
alive = c.fetchone()[0]
db.close()
print(f"\nDB: alive={alive}, published={pub}")
print(f"Published by date: {dist}")
