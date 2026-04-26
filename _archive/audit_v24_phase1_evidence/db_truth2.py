import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()
cur.execute('PRAGMA table_info(stock_pool)')
cols = [r[1] for r in cur.fetchall()]
print('stock_pool cols:', cols)
try:
    cur.execute('SELECT COUNT(*) FROM stock_pool')
    print('total:', cur.fetchone())
    cur.execute('SELECT * FROM stock_pool LIMIT 2')
    print('sample:', cur.fetchall())
except Exception as e:
    print('error:', e)
cur.execute("""SELECT dataset_name, MAX(trade_date), COUNT(*) FROM report_data_usage 
WHERE trade_date >= '2026-04-10' GROUP BY dataset_name""")
print('=== usage since 04-10 ===')
for r in cur.fetchall():
    print(r)
# latest kline stats
cur.execute("""SELECT trade_date, COUNT(DISTINCT stock_code) as stocks 
FROM kline_daily WHERE trade_date >= '2026-04-10' GROUP BY trade_date ORDER BY trade_date DESC""")
print('=== kline coverage since 04-10 ===')
for r in cur.fetchall():
    print(r)
# settlement coverage
cur.execute("SELECT COUNT(*), SUM(CASE WHEN is_misclassified=0 THEN 1 ELSE 0 END) FROM settlement_result")
print('=== settlement ===', cur.fetchone())
