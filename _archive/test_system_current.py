"""系统功能点全量测试 - 2026-04-16"""
import urllib.request as u, json, os, sys
os.environ.pop('http_proxy', None)
u.install_opener(u.build_opener(u.ProxyHandler({})))

base = 'http://127.0.0.1:8010'
results = []

def test(label, path, method='GET', body=None, headers=None, expected_status=200):
    h = headers or {}
    try:
        data = json.dumps(body).encode() if body else None
        req = u.Request(base + path, data=data, headers=h, method=method)
        resp = u.urlopen(req, timeout=15)
        rdata = json.loads(resp.read())
        ok = rdata.get('success', resp.status == 200)
        results.append((resp.status, 'PASS', label, str(rdata)[:100]))
        return rdata
    except Exception as e:
        results.append((0, 'FAIL', label, str(e)[:100]))
        return None

# == Section 1: Health & Infrastructure ==
test('S1-01 health aggregated', '/health')
test('S1-02 health v1', '/api/v1/health')
test('S1-03 platform config', '/api/v1/platform/config')
test('S1-04 platform summary', '/api/v1/platform/summary')

# == Section 2: Reports ==
rlist_data = test('S2-01 reports list', '/api/v1/reports?page=1&page_size=5')
test('S2-02 reports featured', '/api/v1/reports/featured?page_size=3')

report_id = None
if rlist_data:
    items = rlist_data.get('data', {}).get('items', [])
    if items:
        report_id = items[0]['report_id']
        stock_code = items[0].get('stock_code', '')
        test('S2-03 report detail', f'/api/v1/reports/{report_id}')
        test('S2-04 report advanced', f'/api/v1/reports/{report_id}/advanced')
        test('S2-05 report feedback get', f'/api/v1/reports/{report_id}/feedback')

# == Section 3: Stocks ==
test('S3-01 stocks autocomplete ASCII', '/api/v1/stocks/autocomplete?q=600585')
test('S3-02 stocks list', '/api/v1/stocks?page=1&page_size=5')
test('S3-03 pool stocks', '/api/v1/pool/stocks?page=1&page_size=5')

# == Section 4: Market ==
test('S4-01 hot stocks', '/api/v1/market/hot-stocks')
test('S4-02 market state', '/api/v1/market/state')
test('S4-03 market overview', '/api/v1/market-overview')

# == Section 5: Home/Dashboard ==
test('S5-01 home', '/api/v1/home')
test('S5-02 dashboard stats (internal)', '/api/v1/dashboard/stats', headers={'X-Internal-Token': 'kestra-internal-20260327'})

# == Section 6: SIM (needs auth – expect 401) ==
test('S6-01 sim account summary (no-auth)', '/api/v1/sim/account/summary')
test('S6-02 sim positions (no-auth)', '/api/v1/sim/positions')
test('S6-03 portfolio sim-dashboard', '/api/v1/portfolio/sim-dashboard')

# == Section 7: Predictions ==
test('S7-01 predictions stats', '/api/v1/predictions/stats')

# == Section 8: User ==
test('S8-01 user favorites (no-auth expect 401)', '/api/v1/user/favorites')

# == Section 9: Admin (internal) ==
test('S9-01 admin overview', '/api/v1/admin/overview', headers={'X-Internal-Token': 'kestra-internal-20260327'})
test('S9-02 internal hotspot health', '/api/v1/internal/hotspot/health', headers={'X-Internal-Token': 'kestra-internal-20260327'})
test('S9-03 internal llm health', '/api/v1/internal/llm/health', headers={'X-Internal-Token': 'kestra-internal-20260327'})
test('S9-04 internal source fallback', '/api/v1/internal/source/fallback-status', headers={'X-Internal-Token': 'kestra-internal-20260327'})
test('S9-05 internal metrics summary', '/api/v1/internal/metrics/summary', headers={'X-Internal-Token': 'kestra-internal-20260327'})

# == Summary ==
print("\n" + "="*60)
passed = sum(1 for s, r, l, e in results if r == 'PASS')
failed = sum(1 for s, r, l, e in results if r == 'FAIL')
print(f"TOTAL: {passed} PASS / {failed} FAIL")
print("="*60)
for status, res, label, detail in results:
    icon = '✅' if res == 'PASS' else '❌'
    print(f"  {icon} [{status}] {label}: {detail[:80]}")
