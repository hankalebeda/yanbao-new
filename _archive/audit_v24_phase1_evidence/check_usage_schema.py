"""Check report_data_usage schema and existing ok records for 2026-04-10, 2026-04-14"""
import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()

# Schema
print('=== report_data_usage table schema ===')
cur.execute("SELECT sql FROM sqlite_master WHERE name='report_data_usage'")
print(cur.fetchone()[0])

# What datasets/status exist for 2026-04-10?
print('\n=== 2026-04-10 dataset summary ===')
cur.execute("""SELECT dataset_name, source_name, status, count(*) 
FROM report_data_usage WHERE trade_date='2026-04-10'
GROUP BY dataset_name, source_name, status ORDER BY dataset_name""")
for r in cur.fetchall():
    print(r)

# What datasets/status exist for 2026-04-14?
print('\n=== 2026-04-14 dataset summary ===')
cur.execute("""SELECT dataset_name, source_name, status, count(*) 
FROM report_data_usage WHERE trade_date='2026-04-14'
GROUP BY dataset_name, source_name, status ORDER BY dataset_name""")
for r in cur.fetchall():
    print(r)

# Check if batch_id is nullable
print('\n=== report_data_usage nullable batch_id? ===')
cur.execute("SELECT count(*) FROM report_data_usage WHERE batch_id IS NULL")
print('rows with null batch_id:', cur.fetchone()[0])
