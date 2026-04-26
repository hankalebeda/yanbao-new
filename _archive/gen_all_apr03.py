"""Generate reports for all stocks with data on 2026-04-03"""
import sys, urllib.request as u, json, os, time
sys.path.insert(0, '.')
os.environ.pop('http_proxy', None)
u.install_opener(u.build_opener(u.ProxyHandler({})))
base = 'http://127.0.0.1:8010'
hdr = {'X-Internal-Token': 'kestra-internal-20260327', 'Content-Type': 'application/json'}

# Get stocks with data usage on 2026-04-03
from app.core.db import SessionLocal
from sqlalchemy import text
db = SessionLocal()

# Get all stocks with complete data for 2026-04-03
stocks_with_data = db.execute(text("""
    SELECT DISTINCT stock_code FROM report_data_usage 
    WHERE trade_date='2026-04-03'
    GROUP BY stock_code
    HAVING COUNT(DISTINCT dataset_name) >= 5
""")).fetchall()
all_stocks = [r[0] for r in stocks_with_data]
print(f"Stocks with data for 2026-04-03: {len(all_stocks)}")

# Get already-generated ok reports
existing = db.execute(text("""
    SELECT stock_code FROM report WHERE is_deleted=0 AND quality_flag='ok'
""")).fetchall()
existing_stocks = {r[0] for r in existing}
print(f"Already have ok reports: {len(existing_stocks)}: {list(existing_stocks)[:5]}")

# New stocks to generate
new_stocks = [s for s in all_stocks if s not in existing_stocks]
print(f"Stocks to generate: {len(new_stocks)}")

db.close()

# Generate in batches of 30
BATCH_SIZE = 30
total_ok = 0
total_fail = 0
for i in range(0, len(new_stocks), BATCH_SIZE):
    batch = new_stocks[i:i+BATCH_SIZE]
    print(f"\nBatch {i//BATCH_SIZE + 1}: {len(batch)} stocks ({i+1}-{i+len(batch)} of {len(new_stocks)})...")
    payload = json.dumps({
        'stock_codes': batch,
        'trade_date': '2026-04-03',
        'force': False,
        'skip_pool_check': True,
        'cleanup_incomplete_before_batch': True
    }).encode()
    req = u.Request(base + '/api/v1/internal/reports/generate-batch', data=payload, headers=hdr, method='POST')
    t = time.time()
    resp = json.loads(u.urlopen(req, timeout=300).read())
    elapsed = time.time() - t
    data = resp.get('data', {})
    ok = data.get('succeeded', 0)
    fail = data.get('total', 0) - ok
    total_ok += ok
    total_fail += fail
    print(f"  Result: {ok}/{data.get('total')} ok ({elapsed:.0f}s)")
    if fail > 0:
        for d in data.get('details', []):
            if d['status'] != 'ok':
                print(f"  FAIL: {d['stock_code']}: {d.get('error_code','')}")

print(f"\n=== FINAL: {total_ok} ok, {total_fail} failed out of {len(new_stocks)} total")
