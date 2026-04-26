import json
import sqlite3
from pathlib import Path

DB = Path('data/app.db')
PIPE = Path('_archive/audit_v24_phase1_evidence/pipeline_result.json')
CORE = Path('_archive/audit_v24_phase1_evidence/core_pool.json')
DATES = ['2026-04-07','2026-04-08','2026-04-09','2026-04-10','2026-04-13','2026-04-14','2026-04-15','2026-04-16']

print('='*70)
print('PIPELINE RESULT')
print('='*70)
if PIPE.exists():
    try:
        print(PIPE.read_text(encoding='utf-8'))
    except Exception as e:
        print(f'Cannot read pipeline_result.json: {e}')
else:
    print('pipeline_result.json not found')

conn = sqlite3.connect(DB)
cur = conn.cursor()

print('\n' + '='*70)
print('TABLES')
print('='*70)
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
all_tables = [r[0] for r in cur.fetchall()]
keywords = ('report','task','usage','batch','cache','error')
rel_tables = [t for t in all_tables if any(k in t.lower() for k in keywords)]
print('related tables:', rel_tables)

for t in rel_tables:
    print(f'\n[{t}]')
    try:
        cur.execute(f'PRAGMA table_info({t})')
        cols = cur.fetchall()
        print(', '.join([f"{c[1]}:{c[2]}" for c in cols]))
    except Exception as e:
        print('PRAGMA failed:', e)

print('\n' + '='*70)
print('REPORT COUNTS BY DATE')
print('='*70)
cur.execute('''
SELECT trade_date,
       COUNT(*) AS total,
       SUM(CASE WHEN COALESCE(is_deleted,0)=0 THEN 1 ELSE 0 END) AS alive,
       SUM(CASE WHEN COALESCE(is_deleted,0)=1 THEN 1 ELSE 0 END) AS deleted
FROM report
WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16'
GROUP BY trade_date
ORDER BY trade_date
''')
for r in cur.fetchall():
    print(r)

print('\n' + '='*70)
print('TASK STATUS DISTRIBUTION (report_generation_task)')
print('='*70)
if 'report_generation_task' in all_tables:
    cur.execute('''
    SELECT trade_date, status, COUNT(*)
    FROM report_generation_task
    WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16'
    GROUP BY trade_date, status
    ORDER BY trade_date, status
    ''')
    for r in cur.fetchall():
        print(r)

    # Try common error fields safely
    cur.execute('PRAGMA table_info(report_generation_task)')
    task_cols = {c[1] for c in cur.fetchall()}
    err_col = None
    for c in ('status_reason','error_code','failure_reason','error_message'):
        if c in task_cols:
            err_col = c
            break
    if err_col:
        print(f'\nTop reasons by {err_col}:')
        cur.execute(f'''
        SELECT {err_col}, COUNT(*)
        FROM report_generation_task
        WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16'
        GROUP BY {err_col}
        ORDER BY COUNT(*) DESC
        LIMIT 20
        ''')
        for r in cur.fetchall():
            print(r)

    wanted = [c for c in ['task_id','trade_date','stock_code','status','status_reason','error_code','retry_count','created_at','updated_at'] if c in task_cols]
    if wanted:
        print('\nRecent 50 tasks:')
        cols = ','.join(wanted)
        order_col = 'updated_at' if 'updated_at' in task_cols else ('created_at' if 'created_at' in task_cols else 'rowid')
        cur.execute(f'SELECT {cols} FROM report_generation_task ORDER BY {order_col} DESC LIMIT 50')
        for r in cur.fetchall():
            print(r)
else:
    print('report_generation_task not exists')

print('\n' + '='*70)
print('REPORT_DATA_USAGE STATUS DISTRIBUTION')
print('='*70)
cur.execute('''
SELECT trade_date, dataset_name, status, COUNT(*)
FROM report_data_usage
WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16'
GROUP BY trade_date, dataset_name, status
ORDER BY trade_date, dataset_name, status
''')
for r in cur.fetchall():
    print(r)

print('\nKLINE COVERAGE (distinct stocks with ok):')
cur.execute('''
SELECT trade_date, COUNT(DISTINCT stock_code)
FROM report_data_usage
WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16'
  AND dataset_name='kline_daily' AND status='ok'
GROUP BY trade_date
ORDER BY trade_date
''')
for r in cur.fetchall():
    print(r)

print('\n' + '='*70)
print('DATA_BATCH HEALTH')
print('='*70)
if 'data_batch' in all_tables:
    cur.execute('PRAGMA table_info(data_batch)')
    batch_cols = {c[1] for c in cur.fetchall()}
    status_col = 'batch_status' if 'batch_status' in batch_cols else ('status' if 'status' in batch_cols else None)
    if status_col:
        cur.execute(f'''
        SELECT trade_date, source_name, {status_col}, COUNT(*)
        FROM data_batch
        WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16'
        GROUP BY trade_date, source_name, {status_col}
        ORDER BY trade_date, source_name, {status_col}
        ''')
        for r in cur.fetchall():
            print(r)

if 'data_batch_error' in all_tables:
    print('\nData batch error top 20:')
    cur.execute('PRAGMA table_info(data_batch_error)')
    err_cols = [c[1] for c in cur.fetchall()]
    msg_col = 'error_message' if 'error_message' in err_cols else (err_cols[0] if err_cols else None)
    if msg_col:
        cur.execute(f'''SELECT {msg_col}, COUNT(*) FROM data_batch_error GROUP BY {msg_col} ORDER BY COUNT(*) DESC LIMIT 20''')
        for r in cur.fetchall():
            print(r)

print('\n' + '='*70)
print('CORE POOL MISSING ALIVE REPORTS PER DATE')
print('='*70)
if CORE.exists():
    pool = json.loads(CORE.read_text(encoding='utf-8')).get('core_stocks', [])
    if pool:
        q = ','.join(['?'] * len(pool))
        for d in DATES:
            cur.execute(f'''
            SELECT COUNT(*) FROM (
                SELECT sm.stock_code
                FROM stock_master sm
                WHERE sm.stock_code IN ({q})
            ) p
            WHERE p.stock_code NOT IN (
                SELECT r.stock_code
                FROM report r
                WHERE r.trade_date=? AND COALESCE(r.is_deleted,0)=0
            )
            ''', [*pool, d])
            miss = cur.fetchone()[0]
            print(d, miss)

conn.close()
