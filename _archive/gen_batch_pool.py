"""Generate reports for pool stocks on best available date"""
import urllib.request as u, json, os, time
os.environ.pop('http_proxy', None)
u.install_opener(u.build_opener(u.ProxyHandler({})))
base = 'http://127.0.0.1:8010'
hdr_int = {'X-Internal-Token': 'kestra-internal-20260327', 'Content-Type': 'application/json'}

# Get pool stocks
resp = u.urlopen(base + '/api/v1/pool/stocks?page=1&page_size=50', timeout=10)
pool_data = json.loads(resp.read()).get('data', {})
pool_stocks = [x['stock_code'] for x in pool_data.get('items', [])]
pool_date = pool_data.get('trade_date', '2026-04-03')
print(f"Pool date: {pool_date}, stocks: {len(pool_stocks)}")
print(f"Sample: {pool_stocks[:10]}")

# Get existing ok reports to skip
resp2 = u.urlopen(base + '/api/v1/reports?page=1&page_size=100', timeout=10)
existing_data = json.loads(resp2.read()).get('data', {})
existing_stocks = {x['stock_code'] for x in existing_data.get('items', [])}
print(f"Existing ok reports: {len(existing_stocks)} stocks: {list(existing_stocks)[:5]}")

# Filter to new stocks
new_stocks = [s for s in pool_stocks if s not in existing_stocks]
print(f"New stocks to generate: {len(new_stocks)}")

# Generate batch (first 20 for testing)
BATCH_SIZE = 20
target_stocks = new_stocks[:BATCH_SIZE]
if not target_stocks:
    print("No new stocks to generate!")
else:
    print(f"\nGenerating {len(target_stocks)} reports for {pool_date}...")
    payload = json.dumps({
        'stock_codes': target_stocks,
        'trade_date': pool_date,
        'force': False,  # Don't force regenerate if already exists
        'skip_pool_check': True,
        'cleanup_incomplete_before_batch': False
    }).encode()
    req = u.Request(base + '/api/v1/internal/reports/generate-batch', data=payload, headers=hdr_int, method='POST')
    t = time.time()
    resp3 = json.loads(u.urlopen(req, timeout=300).read())
    elapsed = time.time() - t
    data = resp3.get('data', {})
    print(f"\nResults ({elapsed:.0f}s): {data.get('succeeded')}/{data.get('total')} succeeded")
    for d in data.get('details', []):
        status = d['status']
        err = d.get('error_code', '')
        if status != 'ok':
            print(f"  FAIL {d['stock_code']}: {status} {err}")
        else:
            print(f"  OK   {d['stock_code']}")
