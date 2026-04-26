"""Check kline backfill errors"""
import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()

# The failed kline backfill batch
batch_id = '86c4e214-51b7-41f2-ab00-9bd18cd7a3f6'
cur.execute("""SELECT error_code, error_message, count(*) 
FROM data_batch_error WHERE batch_id=? GROUP BY error_code, error_message""", (batch_id,))
print('Error summary:')
for r in cur.fetchall():
    print(f'  {r[2]}x: [{r[0]}] {r[1]}')

# Sample of failed stocks
cur.execute("""SELECT stock_code, error_message FROM data_batch_error WHERE batch_id=? LIMIT 10""", (batch_id,))
print('\nSample failed stocks:')
for r in cur.fetchall():
    print(f'  {r[0]}: {r[1]}')

# Check: which of the 200 core pool stocks have kline on 2026-04-07?
import json
with open('_archive/audit_v24_phase1_evidence/core_pool.json') as f:
    pool = json.load(f)['core_stocks']

placeholders = ','.join(['?' for _ in pool])
cur.execute(f"SELECT count(*) FROM kline_daily WHERE trade_date='2026-04-07' AND stock_code IN ({placeholders})", pool)
print(f'\nCore pool stocks with kline on 2026-04-07: {cur.fetchone()[0]}/200')

# Also check how many have report_data_usage with kline ok
cur.execute(f"SELECT count(DISTINCT stock_code) FROM report_data_usage WHERE trade_date='2026-04-07' AND dataset_name='kline_daily' AND status='ok' AND stock_code IN ({placeholders})", pool)
print(f'Core pool stocks with kline_usage ok on 2026-04-07: {cur.fetchone()[0]}/200')
