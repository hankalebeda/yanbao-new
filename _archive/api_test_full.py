"""Full authenticated API test - admin + internal"""
import urllib.request, json

BASE = 'http://127.0.0.1:8010'
CRON_TOKEN = 'kestra-internal-20260327'

def http(method, path, data=None, headers=None):
    url = f'{BASE}{path}'
    try:
        if data:
            req = urllib.request.Request(url, data=json.dumps(data).encode(), method=method)
            req.add_header('Content-Type', 'application/json')
        else:
            req = urllib.request.Request(url, method=method)
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode('utf-8', errors='replace')
            return resp.status, json.loads(body) if body.strip()[:1] in '{[' else body
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace') if e.fp else ''
        try: return e.code, json.loads(body)
        except: return e.code, body
    except Exception as e:
        return 0, str(e)

# Login as admin
s, r = http('POST', '/auth/login', {'email': 'audit_admin99@test.com', 'password': 'AuditAdmin123!'})
token = r.get('data', {}).get('access_token') if isinstance(r, dict) else None
if not token:
    print(f'Login failed: [{s}]')
    exit(1)
print(f'Admin login OK')

admin_h = {'Authorization': f'Bearer {token}'}
internal_h = {'X-Internal-Token': CRON_TOKEN}

# ===== ADMIN ENDPOINTS =====
print('\n=== ADMIN ENDPOINTS (as admin) ===')
admin_eps = [
    ('GET', '/api/v1/admin/overview'),
    ('GET', '/api/v1/admin/system-status'),
    ('GET', '/api/v1/admin/users'),
    ('GET', '/api/v1/admin/reports?page=1&page_size=2'),
    ('GET', '/api/v1/admin/cookie-sessions'),
    ('GET', '/api/v1/admin/scheduler/status'),
]
for method, path in admin_eps:
    s, r = http(method, path, headers=admin_h)
    body = json.dumps(r, ensure_ascii=False)[:200] if isinstance(r, dict) else str(r)[:200]
    ok = 'OK' if s == 200 else f'FAIL({s})'
    print(f'{ok} {path}')
    if s == 200 and isinstance(r, dict):
        data = r.get('data', r)
        if isinstance(data, dict):
            keys = list(data.keys())[:10]
            print(f'  keys: {keys}')

# ===== INTERNAL ENDPOINTS =====
print('\n=== INTERNAL ENDPOINTS (cron token) ===')
internal_eps = [
    ('GET', '/api/v1/internal/llm/health'),
    ('GET', '/api/v1/internal/llm/version'),
    ('GET', '/api/v1/internal/source/fallback-status'),
    ('GET', '/api/v1/internal/hotspot/health'),
    ('GET', '/api/v1/internal/metrics/summary'),
    ('GET', '/api/v1/internal/runtime/gates'),
    ('GET', '/api/v1/internal/audit/context'),
]
for method, path in internal_eps:
    s, r = http(method, path, headers=internal_h)
    body = json.dumps(r, ensure_ascii=False)[:200] if isinstance(r, dict) else str(r)[:200]
    ok = 'OK' if s == 200 else f'FAIL({s})'
    print(f'{ok} {path}')
    if s == 200 and isinstance(r, dict):
        data = r.get('data', r)
        if isinstance(data, dict):
            keys = list(data.keys())[:10]
            print(f'  keys: {keys}')

# ===== FEATURES / GOVERNANCE =====
print('\n=== FEATURES/GOVERNANCE (admin) ===')
for path in ['/api/v1/features/catalog', '/api/v1/governance/catalog']:
    s, r = http('GET', path, headers=admin_h)
    ok = 'OK' if s == 200 else f'FAIL({s})'
    count = len(r.get('data', {}).get('features', [])) if isinstance(r, dict) and r.get('data') else 0
    print(f'{ok} {path} (items: {count})')

# ===== REPORT DETAIL + ADVANCED =====
print('\n=== REPORT DETAIL + ADVANCED ===')
s, r = http('GET', '/api/v1/reports?page=1&page_size=5')
if isinstance(r, dict) and r.get('data', {}).get('items'):
    for item in r['data']['items'][:3]:
        rid = item['report_id']
        sc = item.get('stock_code', '?')
        
        # Detail
        s2, r2 = http('GET', f'/api/v1/reports/{rid}')
        if isinstance(r2, dict) and r2.get('data'):
            d = r2['data']
            print(f'\nReport {sc} ({rid[:8]}...):')
            print(f'  conclusion_text: {"YES" if d.get("conclusion_text") else "NO"}')
            print(f'  reasoning_chain_md: {"YES" if d.get("reasoning_chain_md") else "NO"} ({len(str(d.get("reasoning_chain_md","")))} chars)')
            print(f'  recommendation: {d.get("recommendation", "NONE")}')
            print(f'  stock_name: {d.get("stock_name_snapshot", "NONE")}')
            print(f'  quality_flag: {d.get("quality_flag", "NONE")}')
            print(f'  llm_fallback: {d.get("llm_fallback_level", "NONE")}')
        
        # Advanced (with admin auth)
        s3, r3 = http('GET', f'/api/v1/reports/{rid}/advanced', headers=admin_h)
        if isinstance(r3, dict) and r3.get('data'):
            d3 = r3['data']
            for f in ['reasoning_chain', 'prior_stats', 'risk_audit', 'used_data_lineage']:
                val = d3.get(f)
                has = bool(val)
                preview = str(val)[:60] if val else 'NONE'
                print(f'  adv.{f}: {"YES" if has else "NO"} — {preview}')

# ===== SIM ENDPOINTS =====
print('\n=== SIM ENDPOINTS (admin) ===')
for path in ['/api/v1/sim/positions', '/api/v1/sim/account/summary', '/api/v1/portfolio/sim-dashboard', '/api/v1/sim/account/snapshots']:
    s, r = http('GET', path, headers=admin_h)
    body = json.dumps(r, ensure_ascii=False)[:150] if isinstance(r, dict) else str(r)[:150]
    ok = 'OK' if s == 200 else f'FAIL({s})'
    print(f'{ok} {path}')
    if s == 200 and isinstance(r, dict):
        data = r.get('data', r)
        if isinstance(data, dict):
            print(f'  keys: {list(data.keys())[:8]}')
        elif isinstance(data, list):
            print(f'  items: {len(data)}')

# ===== PLATFORM SUMMARY DEEP CHECK =====
print('\n=== PLATFORM SUMMARY ===')
s, r = http('GET', '/api/v1/platform/summary')
if isinstance(r, dict) and r.get('data'):
    d = r['data']
    for k, v in d.items():
        print(f'  {k}: {v}')

# ===== HEALTH CHECK =====
print('\n=== HEALTH ===')
s, r = http('GET', '/health')
if isinstance(r, dict) and r.get('data'):
    d = r['data']
    for k, v in d.items():
        print(f'  {k}: {v}')

print('\n=== DONE ===')
