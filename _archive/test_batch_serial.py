import urllib.request as u, json, os
os.environ.pop('http_proxy', None)
u.install_opener(u.build_opener(u.ProxyHandler({})))
base = 'http://127.0.0.1:8010'
hdr = {'X-Internal-Token': 'kestra-internal-20260327', 'Content-Type': 'application/json'}

stocks = ['000001.SZ','000069.SZ','000100.SZ','000157.SZ','000301.SZ']
payload = json.dumps({
    'stock_codes': stocks,
    'trade_date': '2026-04-03',
    'force': True,
    'skip_pool_check': True,
    'cleanup_incomplete_before_batch': True,
    'max_concurrent': 1,
}).encode()
req = u.Request(base + '/api/v1/internal/reports/generate-batch', data=payload, headers=hdr, method='POST')
resp = json.loads(u.urlopen(req, timeout=400).read())
data = resp.get('data', {})
print('succeeded:', data.get('succeeded'), 'failed:', data.get('failed'), 'elapsed:', data.get('elapsed_s'), 'max_concurrent:', data.get('max_concurrent'))
for d in data.get('details', []):
    print(d['stock_code'], d['status'], d.get('error_code'))
