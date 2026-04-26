"""Check error tables and recent kline backfill errors"""
import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%error%'")
print('Error tables:', cur.fetchall())

cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%batch%'")
print('Batch tables:', cur.fetchall())

# Most recent batch for 2026-04-07
cur.execute("""SELECT batch_id, source_name, batch_scope, batch_status, quality_flag, 
records_total, records_success, records_failed, status_reason
FROM data_batch WHERE trade_date='2026-04-07' ORDER BY created_at DESC LIMIT 5""")
for r in cur.fetchall():
    print(r)

# Check data_batch_error table
cur.execute("""SELECT name FROM sqlite_master WHERE type='table'""")
tables = [r[0] for r in cur.fetchall()]
print('\nAll tables:', sorted(tables))
