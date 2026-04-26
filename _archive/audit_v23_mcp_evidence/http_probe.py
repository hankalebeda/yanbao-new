import json, os, urllib.request, urllib.error, ssl

os.environ['NO_PROXY'] = '*'
BASE = 'http://127.0.0.1:8000'
endpoints = [
    ('GET', '/api/v1/health'),
    ('GET', '/api/v1/home'),
    ('GET', '/api/v1/reports?limit=5'),
    ('GET', '/api/v1/dashboard/stats?window_days=1'),
    ('GET', '/api/v1/dashboard/stats?window_days=7'),
    ('GET', '/api/v1/dashboard/stats?window_days=30'),
    ('GET', '/api/v1/dashboard/stats?window_days=60'),
    ('GET', '/api/v1/market/hotspots'),
    ('GET', '/api/v1/search?q=601888'),
    ('GET', '/api/v1/favorites'),
    ('GET', '/api/v1/stock/recent-prices?code=000001.SZ'),
    ('GET', '/openapi.json'),
    ('GET', '/'),
    ('GET', '/reports'),
    ('GET', '/dashboard'),
    ('GET', '/subscribe'),
    ('GET', '/login'),
    ('GET', '/register'),
    ('GET', '/privacy'),
    ('GET', '/terms'),
    ('GET', '/admin'),
    ('GET', '/profile'),
    ('GET', '/portfolio/sim-dashboard'),
    ('GET', '/forgot-password'),
    ('GET', '/api/v1/features/catalog'),
]

out = []
for m, p in endpoints:
    url = BASE + p
    req = urllib.request.Request(url, method=m)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read(2000)
            out.append({'url': p, 'status': r.status, 'len': len(body), 'snippet': body[:200].decode('utf-8', 'replace')})
    except urllib.error.HTTPError as e:
        body = e.read(500)
        out.append({'url': p, 'status': e.code, 'err': str(e), 'snippet': body[:200].decode('utf-8', 'replace')})
    except Exception as e:
        out.append({'url': p, 'status': 'EXC', 'err': str(e)})

print(json.dumps(out, ensure_ascii=False, indent=2))
