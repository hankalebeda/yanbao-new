"""MCP 首页 vs admin 今日研报口径差异实证"""
import json, os, urllib.request

os.environ['NO_PROXY'] = '*'

def get_json(url, cookie=None):
    req = urllib.request.Request(url)
    if cookie:
        req.add_header('Cookie', cookie)
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, json.loads(r.read().decode('utf-8'))

s, home = get_json('http://127.0.0.1:8000/api/v1/home')
s, stats1 = get_json('http://127.0.0.1:8000/api/v1/dashboard/stats?window_days=1')
s, stats7 = get_json('http://127.0.0.1:8000/api/v1/dashboard/stats?window_days=7')

data = home.get('data', {})
print('=== /api/v1/home ===')
print(f"  latest_reports count: {len(data.get('latest_reports', []))}")
print(f"  data_status: {data.get('data_status')}")
print(f"  current_trade_date: {data.get('current_trade_date')}")
print(f"  stock_pool_count: {data.get('stock_pool_count')}")
print(f"  today_reports: {data.get('today_reports')}")
print()
print('=== /api/v1/dashboard/stats?window_days=1 ===')
print(json.dumps(stats1.get('data', {}), ensure_ascii=False, indent=2)[:500])
print()
print('=== /api/v1/dashboard/stats?window_days=7 ===')
print(json.dumps(stats7.get('data', {}), ensure_ascii=False, indent=2)[:500])
