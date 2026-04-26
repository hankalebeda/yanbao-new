import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

# Get column names
cur.execute('PRAGMA table_info(report)')
cols = [r[1] for r in cur.fetchall()]
print('report columns:', cols)

# Report overview
cur.execute('SELECT * FROM report ORDER BY created_at DESC LIMIT 5')
rows = cur.fetchall()
print(f'\n=== 研报总计示例 (最新5条) ===')
for r in rows:
    row_dict = dict(zip(cols, r))
    print({k: str(v)[:60] if v else v for k, v in row_dict.items()})

# Count by date/status
date_col = 'trade_date' if 'trade_date' in cols else 'created_at'
print(f'\n=== 按日期/类型统计 ===')
cur.execute(f'''SELECT substr({date_col},1,10) as dt, strategy_type, published, llm_fallback_level, COUNT(*)
FROM report GROUP BY substr({date_col},1,10), strategy_type, published, llm_fallback_level
ORDER BY dt DESC''')
for r in cur.fetchall():
    print(r)

# Total count
cur.execute('SELECT COUNT(*) FROM report')
print(f'\n总研报数: {cur.fetchone()[0]}')

conn.close()
