import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

# Check report_data_usage column names
cur.execute('PRAGMA table_info(report_data_usage)')
cols = [r[1] for r in cur.fetchall()]
print('report_data_usage columns:', cols)

# Check dates using actual column names
date_col = 'trade_date' if 'trade_date' in cols else cols[1]
stock_col = 'stock_code' if 'stock_code' in cols else cols[2]
print(f'\nUsing: date_col={date_col}, stock_col={stock_col}')
cur.execute(f'SELECT {date_col}, COUNT(DISTINCT {stock_col}), COUNT(*) FROM report_data_usage GROUP BY {date_col} ORDER BY {date_col} DESC LIMIT 20')
for r in cur.fetchall():
    print(f'  {r}')

# Check all tables that might have per-date data
print('\n=== 全量表行数检查 ===')
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
all_tables = [r[0] for r in cur.fetchall()]
print(f'Total tables: {len(all_tables)}')
for t in all_tables:
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{t}"')
        cnt = cur.fetchone()[0]
        print(f'  {t}: {cnt}')
    except Exception as e:
        print(f'  {t}: ERROR {e}')

conn.close()
