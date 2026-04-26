"""Check kline sources for target dates"""
import sqlite3, json
c = sqlite3.connect('data/app.db')
cur = c.cursor()

with open('_archive/audit_v24_phase1_evidence/core_pool.json') as f:
    pool = json.load(f)['core_stocks']

pool_str = ','.join([f"'{s}'" for s in pool])
cur.execute(f"""SELECT trade_date, stock_code, source_batch_id FROM kline_daily 
WHERE trade_date>='2026-04-07' AND stock_code IN ({pool_str}) ORDER BY trade_date""")
rows = cur.fetchall()
print(f'Total kline records for core pool after 2026-04-07: {len(rows)}')

batch_ids = set(r[2] for r in rows)
print('Batch IDs:')
for bid in batch_ids:
    cur.execute('SELECT source_name, batch_scope, quality_flag, records_total FROM data_batch WHERE batch_id=?', (bid,))
    b = cur.fetchone()
    print(f'  {bid[:12]}: {b}')

print('\nSample records:')
for r in rows[:15]:
    print(f'  {r[0]} {r[1]}')
