import sqlite3
c = sqlite3.connect('data/app.db')
c.row_factory = sqlite3.Row
print('-- kline_daily cols --')
cols = [r[1] for r in c.execute('PRAGMA table_info(kline_daily)')]
print(' ', cols)
print()
print('-- kline_daily 601898.SH 04-01~04-16 --')
for r in c.execute("SELECT * FROM kline_daily WHERE stock_code='601898.SH' AND trade_date BETWEEN '2026-04-01' AND '2026-04-16' ORDER BY trade_date"):
    print(' ', dict(r))
print()
print('-- report_generation_task cols --')
for row in c.execute('PRAGMA table_info(report_generation_task)'):
    print(' ', row[1])
print()
print('-- task 04-09 601898.SH --')
for r in c.execute("SELECT * FROM report_generation_task WHERE trade_date='2026-04-09' AND stock_code='601898.SH'"):
    print(' ', dict(r))
