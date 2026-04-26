"""Test report generation for 2026-04-14 which has good coverage"""
import urllib.request as u, json, os, time
os.environ.pop('http_proxy', None)
u.install_opener(u.build_opener(u.ProxyHandler({})))
base = 'http://127.0.0.1:8010'
hdr = {'X-Internal-Token': 'kestra-internal-20260327', 'Content-Type': 'application/json'}

# First get pool stocks for 2026-04-14 by checking pool snapshot
# Try 2026-04-14 (COMPLETED refresh task, 702 kline stocks)
test_stocks = ['601888.SH', '000333.SZ', '600036.SH', '000858.SZ', '002594.SZ']
for trade_date in ['2026-04-14', '2026-04-15', '2026-04-10']:
    payload = json.dumps({
        'stock_codes': test_stocks[:3], 
        'trade_date': trade_date, 
        'force': True, 
        'skip_pool_check': True
    }).encode()
    req = u.Request(base + '/api/v1/internal/reports/generate-batch', data=payload, headers=hdr, method='POST')
    t = time.time()
    resp = json.loads(u.urlopen(req, timeout=180).read())
    elapsed = time.time() - t
    data = resp.get('data', {})
    print(f"\n=== trade_date={trade_date} ({elapsed:.0f}s): {data.get('succeeded')}/{data.get('total')}")
    for d in data.get('details', []):
        print(f"  {d['stock_code']}: {d['status']} {d.get('error_code', '')[:50]}")
    if data.get('succeeded', 0) > 0:
        print("WORKING DATE FOUND!")
        break
