"""Round 2 DB + HTTP snapshot"""
import sqlite3, json, urllib.request, urllib.error

conn = sqlite3.connect('data/app.db')
c = conn.cursor()
queries = [
    ('total_reports', 'SELECT COUNT(*) FROM report'),
    ('alive_reports', 'SELECT COUNT(*) FROM report WHERE is_deleted=0'),
    ('published_alive', "SELECT COUNT(*) FROM report WHERE is_deleted=0 AND published=1"),
    ('ok_published', "SELECT COUNT(*) FROM report WHERE is_deleted=0 AND published=1 AND quality_flag='ok'"),
    ('buy_published', "SELECT COUNT(*) FROM report WHERE is_deleted=0 AND published=1 AND recommendation='BUY'"),
    ('hold_published', "SELECT COUNT(*) FROM report WHERE is_deleted=0 AND published=1 AND recommendation='HOLD'"),
    ('settle_total', 'SELECT COUNT(*) FROM settlement_result'),
    ('settle_valid', 'SELECT COUNT(*) FROM settlement_result WHERE is_misclassified=0'),
    ('kline_stocks', 'SELECT COUNT(DISTINCT stock_code) FROM kline_daily'),
    ('kline_rows', 'SELECT COUNT(*) FROM kline_daily'),
    ('kline_latest', 'SELECT MAX(trade_date) FROM kline_daily'),
    ('sms_count', 'SELECT COUNT(*) FROM strategy_metric_snapshot'),
    ('sms_latest_day', 'SELECT MAX(snapshot_date) FROM strategy_metric_snapshot'),
    ('msc_latest', 'SELECT MAX(trade_date) FROM market_state_cache'),
    ('hotspot_count', 'SELECT COUNT(*) FROM hotspot_raw'),
    ('pool_tasks', 'SELECT COUNT(*) FROM pool_task'),
    ('pool_tasks_latest', 'SELECT MAX(task_date) FROM pool_task'),
]
print('=== DB SNAPSHOT ===')
for k, q in queries:
    try:
        v = c.execute(q).fetchone()[0]
    except Exception as e:
        v = f'ERR: {e}'
    print(f'  {k}: {v}')

print()
print('=== ALIVE REPORTS ===')
rows = c.execute(
    "SELECT report_id, stock_code, trade_date, recommendation, quality_flag, published, created_at "
    "FROM report WHERE is_deleted=0 ORDER BY created_at DESC"
).fetchall()
for r in rows:
    print(f'  {r}')

print()
print('=== SETTLEMENT_RESULT ===')
rows = c.execute(
    'SELECT report_id, signal_date, window_days, net_return_pct, is_misclassified, settlement_status '
    'FROM settlement_result'
).fetchall()
for r in rows:
    print(f'  {r}')

print()
print('=== STRATEGY_METRIC_SNAPSHOT (latest 5) ===')
rows = c.execute(
    'SELECT snapshot_date, window_days, strategy_type, sample_size, win_rate, profit_loss_ratio '
    'FROM strategy_metric_snapshot ORDER BY snapshot_date DESC, window_days LIMIT 15'
).fetchall()
for r in rows:
    print(f'  {r}')

conn.close()

print()
print('=== HTTP PROBE ===')
base = 'http://127.0.0.1:8010'
endpoints = [
    '/api/v1/health',
    '/api/v1/home',
    '/api/v1/market/state',
    '/api/v1/reports',
    '/api/v1/dashboard/stats?window_days=1',
    '/api/v1/dashboard/stats?window_days=7',
    '/api/v1/dashboard/stats?window_days=30',
    '/api/v1/pool/latest',
    '/api/v1/platform/config',
]
for ep in endpoints:
    url = base + ep
    try:
        resp = urllib.request.urlopen(url, timeout=5)
        body = resp.read().decode('utf-8', errors='replace')
        data = json.loads(body)
        print(f'  {ep}: {resp.status} | {str(data)[:200]}')
    except urllib.error.HTTPError as e:
        print(f'  {ep}: HTTP {e.code}')
    except Exception as e:
        print(f'  {ep}: ERR {e}')
