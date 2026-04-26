"""Cleanup incomplete tasks then generate reports"""
import urllib.request as u, json, os, time
os.environ.pop('http_proxy', None)
u.install_opener(u.build_opener(u.ProxyHandler({})))
base = 'http://127.0.0.1:8010'
hdr = {'X-Internal-Token': 'kestra-internal-20260327', 'Content-Type': 'application/json'}

# Step 1: Cleanup all incomplete tasks
print("=== Cleanup incomplete tasks...")
req = u.Request(base + '/api/v1/internal/reports/cleanup-incomplete-all', 
                data=json.dumps({}).encode(), headers=hdr, method='POST')
resp = json.loads(u.urlopen(req, timeout=30).read())
print(json.dumps(resp, ensure_ascii=False, indent=2)[:500])

time.sleep(2)

# Step 2: Try generation for multiple dates
test_stocks = ['601888.SH', '000333.SZ', '600036.SH']
for trade_date in ['2026-04-14', '2026-04-15', '2026-04-10', '2026-04-03']:
    payload = json.dumps({
        'stock_codes': test_stocks[:2], 
        'trade_date': trade_date, 
        'force': True, 
        'skip_pool_check': True,
        'cleanup_incomplete_before_batch': True
    }).encode()
    req = u.Request(base + '/api/v1/internal/reports/generate-batch', data=payload, headers=hdr, method='POST')
    t = time.time()
    resp = json.loads(u.urlopen(req, timeout=180).read())
    elapsed = time.time() - t
    data = resp.get('data', {})
    print(f"\n=== trade_date={trade_date} ({elapsed:.0f}s): {data.get('succeeded')}/{data.get('total')}")
    for d in data.get('details', []):
        print(f"  {d['stock_code']}: {d['status']} {d.get('error_code', '')}")
    if data.get('succeeded', 0) > 0:
        print("WORKING DATE FOUND!")
        break
