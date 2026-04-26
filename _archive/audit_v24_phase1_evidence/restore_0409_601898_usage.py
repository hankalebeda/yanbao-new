"""Restore report_data_usage rows for 601898.SH 04-09 from the backup,
so that re-generation can find used_data."""
import json
import sqlite3
from pathlib import Path

BK = Path('_archive/backup_20260418')
DB = Path('data/app.db')

# find latest backup
files = sorted(BK.glob('report_data_usage_rows_*.jsonl'))
assert files, 'no backup found'
src = files[-1]
print('Using backup', src)

c = sqlite3.connect(str(DB))
c.row_factory = sqlite3.Row

# read backup, filter to target
restore_rows = []
with src.open('r', encoding='utf-8') as f:
    for line in f:
        row = json.loads(line)
        if row.get('stock_code') == '601898.SH' and str(row.get('trade_date')).startswith('2026-04-09'):
            restore_rows.append(row)
print(f'rows to restore: {len(restore_rows)}')

if not restore_rows:
    raise SystemExit('nothing to restore')

# get target columns from current table
cols = [r[1] for r in c.execute('PRAGMA table_info(report_data_usage)')]
print('table cols:', cols)

c.execute('BEGIN')
try:
    for row in restore_rows:
        keys = [k for k in cols if k in row]
        vals = [row[k] for k in keys]
        sql = f"INSERT OR REPLACE INTO report_data_usage ({','.join(keys)}) VALUES ({','.join(['?']*len(keys))})"
        c.execute(sql, vals)
    c.execute('COMMIT')
    print(f'restored {len(restore_rows)} rows')
except Exception:
    c.execute('ROLLBACK')
    raise

n = c.execute("SELECT COUNT(*) FROM report_data_usage WHERE stock_code='601898.SH' AND trade_date='2026-04-09'").fetchone()[0]
print('verify count:', n)
c.close()
