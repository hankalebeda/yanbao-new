"""Regenerate reports on 2026-04-03 (which has K-line coverage of 947 stocks).

Use existing pool (2026-04-03 has task core=200). Force rebuild may give variety
of recommendations (BUY/HOLD/SELL).
"""
import json
import time
import urllib.request
from sqlalchemy import text
from app import models  # noqa: F401
from app.core.db import SessionLocal

INTERNAL_TOKEN = "phase1-audit-token-20260417"
BASE = "http://127.0.0.1:8000"
TRADE_DATE = "2026-04-03"
BATCH = 20
MAX_STOCKS = 40

db = SessionLocal()
try:
    pool_rows = db.execute(text("""
        SELECT s.stock_code
        FROM stock_pool_snapshot s
        JOIN stock_pool_refresh_task t ON s.refresh_task_id = t.task_id
        WHERE t.trade_date = :td AND s.pool_role = 'core'
          AND EXISTS (SELECT 1 FROM kline_daily k WHERE k.stock_code = s.stock_code AND k.trade_date = :td)
        ORDER BY s.rank_no ASC
        LIMIT :n
    """), {"td": TRADE_DATE, "n": MAX_STOCKS}).fetchall()
    codes = [r[0] for r in pool_rows]
    print(f"pool stocks with kline on {TRADE_DATE}: {len(codes)}")
    print("first 5:", codes[:5])
finally:
    db.close()

if not codes:
    raise SystemExit("no codes")

opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
urllib.request.install_opener(opener)

def call_batch(codes_batch, idx):
    payload = {
        "stock_codes": codes_batch,
        "trade_date": TRADE_DATE,
        "force": True,
        "cleanup_incomplete_before_batch": False,
        "max_concurrent": 6,
    }
    req = urllib.request.Request(
        f"{BASE}/api/v1/internal/reports/generate-batch",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "X-Internal-Token": INTERNAL_TOKEN},
        method="POST",
    )
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=3600) as r:
            body = json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = {"http_error": e.code, "body": e.read().decode()[:500]}
    dt = time.time() - t0
    d = body.get("data") or {}
    print(f"[batch {idx}] n={len(codes_batch)} elapsed={dt:.1f}s total={d.get('total')} ok={d.get('succeeded')} fail={d.get('failed')}")
    fails = [x for x in (d.get("details") or []) if x.get("status") == "error"]
    if fails:
        by_ec = {}
        for x in fails:
            by_ec[x.get("error_code")] = by_ec.get(x.get("error_code"), 0) + 1
        print(f"  failures: {by_ec}")
    return body

all_results = []
for i in range(0, len(codes), BATCH):
    chunk = codes[i:i+BATCH]
    all_results.append(call_batch(chunk, i // BATCH + 1))

with open("output/phase2_batch_20260403.json", "w", encoding="utf-8") as f:
    json.dump(all_results, f, ensure_ascii=False, indent=2)
print("Saved output/phase2_batch_20260403.json")
