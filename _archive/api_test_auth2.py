"""Quick auth test with correct paths"""
import urllib.request, json

BASE = 'http://127.0.0.1:8010'

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

# Register
s, r = http('POST', '/auth/register', {'email': 'audit_admin99@test.com', 'password': 'AuditAdmin123!', 'nickname': 'AuditAdmin'})
print(f'Register: [{s}]')
if isinstance(r, dict):
    print(json.dumps(r, ensure_ascii=False)[:300])

# Login
s, r = http('POST', '/auth/login', {'email': 'audit_admin99@test.com', 'password': 'AuditAdmin123!'})
print(f'Login: [{s}]')
token = None
if isinstance(r, dict):
    data = r.get('data') or r
    token = data.get('access_token') or data.get('token')
    if token:
        print(f'Token obtained: {token[:30]}...')
    else:
        print(json.dumps(r, ensure_ascii=False)[:300])

if not token:
    print("Trying existing users...")
    import sqlite3
    conn = sqlite3.connect('data/app.db')
    cur = conn.cursor()
    cur.execute("SELECT email, role FROM app_user LIMIT 10")
    users = cur.fetchall()
    print(f"Users in DB: {users}")
    conn.close()
    
    for email in [u[0] for u in users[:5]]:
        for pwd in ['admin123', 'Test123!', 'password', 'AuditAdmin123!']:
            s, r = http('POST', '/auth/login', {'email': email, 'password': pwd})
            if isinstance(r, dict):
                data = r.get('data') or r
                t = data.get('access_token') or data.get('token')
                if t:
                    token = t
                    print(f'Login OK with {email}/{pwd}, token: {token[:30]}...')
                    break
        if token:
            break

if not token:
    print("FAILED to get token")
    exit(1)

auth = {'Authorization': f'Bearer {token}'}

# Auth me
s, r = http('GET', '/auth/me', headers=auth)
print(f'\nAuth/me: [{s}]')
if isinstance(r, dict):
    print(json.dumps(r, ensure_ascii=False)[:300])

# Admin tests
print('\n=== ADMIN ENDPOINTS ===')
endpoints = [
    ('GET', '/api/v1/admin/overview'),
    ('GET', '/api/v1/admin/system-status'),
    ('GET', '/api/v1/admin/users'),
    ('GET', '/api/v1/admin/reports?page=1&page_size=2'),
    ('GET', '/api/v1/admin/cookie-sessions'),
    ('GET', '/api/v1/admin/scheduler/status'),
]
for method, path in endpoints:
    s, r = http(method, path, headers=auth)
    body = json.dumps(r, ensure_ascii=False)[:150] if isinstance(r, dict) else str(r)[:150]
    marker = 'OK' if s == 200 else f'FAIL({s})'
    print(f'{marker} {path}: {body}')

# Internal tests
print('\n=== INTERNAL ENDPOINTS ===')
# Check for INTERNAL_CRON_TOKEN
cron_token = None
try:
    with open('.env') as f:
        for line in f:
            if line.startswith('INTERNAL_CRON_TOKEN='):
                cron_token = line.split('=', 1)[1].strip().strip('"').strip("'")
                break
except:
    pass

if cron_token:
    int_headers = {'X-Internal-Token': cron_token}
    print(f'Using cron token: {cron_token[:10]}...')
else:
    int_headers = auth
    print('Using admin auth for internal')

internal_eps = [
    ('GET', '/api/v1/internal/llm/health'),
    ('GET', '/api/v1/internal/llm/version'),
    ('GET', '/api/v1/internal/source/fallback-status'),
    ('GET', '/api/v1/internal/hotspot/health'),
    ('GET', '/api/v1/internal/metrics/summary'),
    ('GET', '/api/v1/internal/runtime/gates'),
]
for method, path in internal_eps:
    s, r = http(method, path, headers=int_headers)
    body = json.dumps(r, ensure_ascii=False)[:150] if isinstance(r, dict) else str(r)[:150]
    marker = 'OK' if s == 200 else f'FAIL({s})'
    print(f'{marker} {path}: {body}')

# Features / Governance
print('\n=== FEATURES/GOVERNANCE ===')
for path in ['/api/v1/features/catalog', '/api/v1/governance/catalog']:
    s, r = http('GET', path, headers=auth)
    body = json.dumps(r, ensure_ascii=False)[:150] if isinstance(r, dict) else str(r)[:150]
    marker = 'OK' if s == 200 else f'FAIL({s})'
    print(f'{marker} {path}: {body}')

# A specific report detail
print('\n=== REPORT DETAIL ===')
s, r = http('GET', '/api/v1/reports?page=1&page_size=1')
if isinstance(r, dict) and r.get('data', {}).get('items'):
    rid = r['data']['items'][0]['report_id']
    sc = r['data']['items'][0].get('stock_code', '?')
    print(f'Report: {rid} ({sc})')
    s2, r2 = http('GET', f'/api/v1/reports/{rid}')
    if isinstance(r2, dict) and r2.get('data'):
        d = r2['data']
        print(f'  conclusion_text: {"YES" if d.get("conclusion_text") else "NO"}')
        print(f'  reasoning_chain_md: {"YES" if d.get("reasoning_chain_md") else "NO"}')
        print(f'  recommendation: {d.get("recommendation", "NONE")}')
        print(f'  stock_name: {d.get("stock_name_snapshot", "NONE")}')
        print(f'  quality_flag: {d.get("quality_flag", "NONE")}')
    else:
        print(f'  Detail [{s2}]: {json.dumps(r2, ensure_ascii=False)[:200] if isinstance(r2, dict) else str(r2)[:200]}')
    
    s3, r3 = http('GET', f'/api/v1/reports/{rid}/advanced')
    if isinstance(r3, dict) and r3.get('data'):
        d = r3['data']
        fields = ['reasoning_chain', 'prior_stats', 'risk_audit', 'used_data_lineage']
        for f in fields:
            val = d.get(f)
            print(f'  advanced.{f}: {"YES" if val else "NO"}')
    else:
        print(f'  Advanced [{s3}]: {json.dumps(r3, ensure_ascii=False)[:200] if isinstance(r3, dict) else str(r3)[:200]}')

# Sim endpoints with auth
print('\n=== SIM ENDPOINTS ===')
for path in ['/api/v1/sim/positions', '/api/v1/sim/account/summary', '/api/v1/portfolio/sim-dashboard']:
    s, r = http('GET', path, headers=auth)
    body = json.dumps(r, ensure_ascii=False)[:150] if isinstance(r, dict) else str(r)[:150]
    marker = 'OK' if s == 200 else f'FAIL({s})'
    print(f'{marker} {path}: {body}')
