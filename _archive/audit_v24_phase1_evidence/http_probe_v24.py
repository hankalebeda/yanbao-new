"""V24 修正后的 HTTP 探针 — 修正 v23 探针的 4 条路径错误"""
import json, os, urllib.request, urllib.error

os.environ['NO_PROXY'] = '*'
BASE = 'http://127.0.0.1:8000'
endpoints = [
    ('GET', '/api/v1/health'),
    ('GET', '/api/v1/home'),
    ('GET', '/api/v1/reports?limit=5'),
    ('GET', '/api/v1/dashboard/stats?window_days=1'),
    ('GET', '/api/v1/dashboard/stats?window_days=7'),
    ('GET', '/api/v1/dashboard/stats?window_days=30'),
    # 修正: v23 误写为 /api/v1/market/hotspots（不存在），真实路由是 /api/v1/hot-stocks
    ('GET', '/api/v1/hot-stocks'),
    ('GET', '/api/v1/market-overview'),
    # 修正: v23 误写为 /api/v1/search（不存在），真实路由是 /api/v1/stocks/autocomplete
    ('GET', '/api/v1/stocks/autocomplete?q=601888'),
    ('GET', '/api/v1/stocks?limit=5'),
    # 修正: v23 误写为 /api/v1/favorites（不存在），真实路由是 /api/v1/user/favorites（需登录）
    ('GET', '/api/v1/user/favorites'),
    # 注: /api/v1/stock/recent-prices 确实不存在，此端点为 v23 虚构，移除
    ('GET', '/api/v1/reports/featured'),
    ('GET', '/openapi.json'),
    ('GET', '/'),
    ('GET', '/reports'),
    ('GET', '/dashboard'),
    ('GET', '/subscribe'),
    ('GET', '/login'),
    ('GET', '/register'),
    ('GET', '/admin'),
    ('GET', '/profile'),
    ('GET', '/portfolio/sim-dashboard'),
]

out = []
for m, p in endpoints:
    url = BASE + p
    try:
        req = urllib.request.Request(url, method=m)
        with urllib.request.urlopen(req, timeout=10) as resp:
            out.append({'method': m, 'path': p, 'status': resp.status})
    except urllib.error.HTTPError as e:
        out.append({'method': m, 'path': p, 'status': e.code, 'err': str(e)})
    except Exception as e:
        out.append({'method': m, 'path': p, 'status': 'EXC', 'err': str(e)[:120]})

with open('_archive/audit_v24_phase1_evidence/http_probe_v24.json', 'w', encoding='utf-8') as f:
    json.dump(out, f, ensure_ascii=False, indent=2)

print(f'Total: {len(out)}')
for r in out:
    s = r['status']
    marker = 'OK' if isinstance(s, int) and s < 400 else 'ERR'
    print(f'  {marker:3} {s:<5} {r["method"]:5} {r["path"]}')
