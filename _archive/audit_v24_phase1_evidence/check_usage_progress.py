"""Check what usage records were created by the pipeline"""
import sqlite3, json
c = sqlite3.connect('data/app.db')
cur = c.cursor()

with open('_archive/audit_v24_phase1_evidence/core_pool.json') as f:
    pool = json.load(f)['core_stocks']
pool_str = ','.join([f"'{s}'" for s in pool])

target_dates = ['2026-04-07','2026-04-08','2026-04-09','2026-04-10',
                '2026-04-13','2026-04-14','2026-04-15','2026-04-16']

print('=== Usage records per date per dataset (core pool stocks) ===')
for d in target_dates:
    cur.execute(f"""SELECT dataset_name, source_name, status, count(*) 
    FROM report_data_usage WHERE trade_date='{d}' AND stock_code IN ({pool_str})
    GROUP BY dataset_name, source_name, status ORDER BY dataset_name""")
    rows = cur.fetchall()
    if rows:
        print(f'\n{d}:')
        for r in rows:
            print(f'  {r[0]}/{r[1]}/{r[2]}: {r[3]}')
    else:
        print(f'{d}: NO DATA')

# Check if northbound/etf/hotspot were created for 2026-04-08 etc
print('\n=== Summary: how many dates have northbound ok? ===')
cur.execute(f"""SELECT trade_date, count(DISTINCT stock_code) FROM report_data_usage 
WHERE dataset_name='northbound_summary' AND status='ok' AND stock_code IN ({pool_str})
GROUP BY trade_date ORDER BY trade_date DESC LIMIT 15""")
for r in cur.fetchall():
    print(f'  {r[0]}: {r[1]} stocks with northbound ok')
