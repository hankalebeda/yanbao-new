"""Check kline state for 2026-04-07: kline in DB vs usage records"""
import sqlite3, json
c = sqlite3.connect('data/app.db')
cur = c.cursor()

with open('_archive/audit_v24_phase1_evidence/core_pool.json') as f:
    pool = json.load(f)['core_stocks']

pool_str = ','.join([f"'{s}'" for s in pool])

# Stocks with kline records in kline_daily table for 2026-04-07
cur.execute(f"""SELECT stock_code FROM kline_daily WHERE trade_date='2026-04-07' 
AND stock_code IN ({pool_str}) ORDER BY stock_code""")
kline_in_db = set(r[0] for r in cur.fetchall())
print(f'Core pool stocks with kline_daily record for 2026-04-07: {len(kline_in_db)}')

# Stocks with report_data_usage kline ok
cur.execute(f"""SELECT DISTINCT stock_code FROM report_data_usage WHERE trade_date='2026-04-07' 
AND dataset_name='kline_daily' AND status='ok' AND stock_code IN ({pool_str})""")
usage_ok = set(r[0] for r in cur.fetchall())
print(f'Core pool stocks with kline usage ok for 2026-04-07: {len(usage_ok)}')

# Check overlaps
only_in_db = kline_in_db - usage_ok
only_in_usage = usage_ok - kline_in_db
in_both = kline_in_db & usage_ok
print(f'In DB but not usage: {len(only_in_db)}: {sorted(only_in_db)[:5]}')
print(f'In usage but not DB: {len(only_in_usage)}')
print(f'In both: {len(in_both)}')

# Check other dates too
for d in ['2026-04-08', '2026-04-09', '2026-04-10', '2026-04-13', '2026-04-14', '2026-04-15', '2026-04-16']:
    cur.execute(f"""SELECT count(*) FROM kline_daily WHERE trade_date='{d}' AND stock_code IN ({pool_str})""")
    kdb = cur.fetchone()[0]
    cur.execute(f"""SELECT count(DISTINCT stock_code) FROM report_data_usage WHERE trade_date='{d}' 
    AND dataset_name='kline_daily' AND status='ok' AND stock_code IN ({pool_str})""")
    uok = cur.fetchone()[0]
    print(f'{d}: kline_db={kdb}, usage_ok={uok}')
