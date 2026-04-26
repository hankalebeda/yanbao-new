import json
import sqlite3

DATES = ['2026-04-07','2026-04-08','2026-04-09','2026-04-10','2026-04-13','2026-04-14','2026-04-15','2026-04-16']

with open('_archive/audit_v24_phase1_evidence/core_pool.json', encoding='utf-8') as f:
    pool = json.load(f)['core_stocks']
pool_set = set(pool)

conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

print('=== Missing kline_daily ok in core pool ===')
all_missing = {}
for d in DATES:
    cur.execute('''
    SELECT DISTINCT stock_code
    FROM report_data_usage
    WHERE trade_date=? AND dataset_name='kline_daily' AND status='ok'
    ''', (d,))
    have = {r[0] for r in cur.fetchall()} & pool_set
    miss = sorted(pool_set - have)
    all_missing[d] = miss
    print(d, len(miss), miss[:10])

# intersection
common = set(pool)
for d in DATES:
    common &= set(all_missing[d])
print('\nCommon missing across all dates:', len(common), sorted(common))

print('\n=== Missing alive reports in core pool ===')
for d in DATES:
    cur.execute('''
    SELECT DISTINCT stock_code FROM report WHERE trade_date=? AND COALESCE(is_deleted,0)=0
    ''', (d,))
    have = {r[0] for r in cur.fetchall()} & pool_set
    miss = sorted(pool_set - have)
    print(d, len(miss), miss[:10])

print('\n=== stock_pool_refresh_task by date ===')
cur.execute('''
SELECT trade_date, status, COUNT(*)
FROM stock_pool_refresh_task
WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16'
GROUP BY trade_date, status
ORDER BY trade_date, status
''')
for r in cur.fetchall():
    print(r)

print('\n=== market_state_cache by date ===')
cur.execute('''
SELECT trade_date, cache_status, reference_date, market_state_degraded, kline_batch_id, hotspot_batch_id
FROM market_state_cache
WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16'
ORDER BY trade_date
''')
for r in cur.fetchall():
    print(r)

print('\n=== failed tasks details (2026-04-07~16) ===')
cur.execute('''
SELECT trade_date, stock_code, status, status_reason
FROM report_generation_task
WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' AND status IN ('Failed')
ORDER BY trade_date, stock_code
''')
for r in cur.fetchall():
    print(r)

conn.close()
