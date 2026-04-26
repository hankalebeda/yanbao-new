import urllib.request as u, json, os
os.environ.pop('http_proxy', None)
u.install_opener(u.build_opener(u.ProxyHandler({})))
base = 'http://127.0.0.1:8010'
hdr = {'X-Internal-Token': 'kestra-internal-20260327', 'Content-Type': 'application/json'}

stocks = ['000001.SZ','000002.SZ','000333.SZ','600519.SH','601888.SH']
payload = json.dumps({
    'stock_codes': stocks,
    'trade_date': '2026-04-03',
    'force': True,
    'skip_pool_check': True,
    'cleanup_incomplete_before_batch': True,
}).encode()
req = u.Request(base + '/api/v1/internal/reports/generate-batch', data=payload, headers=hdr, method='POST')
resp = json.loads(u.urlopen(req, timeout=300).read())
print(json.dumps(resp.get('data', {}), ensure_ascii=False, indent=2))
