"""Try single stock generation to diagnose CONCURRENT_CONFLICT"""
import sys, urllib.request as u, json, os
sys.path.insert(0, '.')
os.environ.pop('http_proxy', None)
u.install_opener(u.build_opener(u.ProxyHandler({})))
base = 'http://127.0.0.1:8010'
hdr = {'X-Internal-Token': 'kestra-internal-20260327', 'Content-Type': 'application/json'}

# Try single stock 000002.SZ (no active task)
def call_batch(stocks, force=True):
    payload = json.dumps({
        'stock_codes': stocks,
        'trade_date': '2026-04-03',
        'force': force,
        'skip_pool_check': True,
        'cleanup_incomplete_before_batch': False
    }).encode()
    req = u.Request(base + '/api/v1/internal/reports/generate-batch', data=payload, headers=hdr, method='POST')
    resp = json.loads(u.urlopen(req, timeout=60).read())
    return resp.get('data', {})

print("Testing 000002.SZ (no active task) with force=True...")
result = call_batch(['000002.SZ'], force=True)
print(f"Result: {result}")

print("\nTesting 000001.SZ (Completed task) with force=True...")
result2 = call_batch(['000001.SZ'], force=True)
print(f"Result: {result2}")
