import sqlite3
conn = sqlite3.connect('data/app.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print('=== pool stocks 2026-04-16 (top 10) ===')
cur.execute(
    'SELECT s.stock_code, s.pool_role FROM stock_pool_snapshot s'
    ' WHERE s.trade_date=? ORDER BY s.rank_no LIMIT 10',
    ('2026-04-16',)
)
for r in cur.fetchall():
    print(dict(r))

print()
print('=== new capital datasets any date ===')
new_ds = ('main_force_flow', 'dragon_tiger_list', 'margin_financing', 'stock_profile')
placeholders = ','.join(['?' for _ in new_ds])
cur.execute(
    f'SELECT dataset_name, source_name, trade_date, COUNT(*) cnt'
    f' FROM report_data_usage WHERE dataset_name IN ({placeholders})'
    f' GROUP BY dataset_name, source_name, trade_date ORDER BY trade_date DESC LIMIT 20',
    new_ds
)
for r in cur.fetchall():
    print(dict(r))

print()
print('=== sample kline for 2026-04-16 ===')
cur.execute(
    'SELECT stock_code, trade_date, close, ma5, ma20 FROM kline_daily WHERE trade_date=? LIMIT 5',
    ('2026-04-16',)
)
for r in cur.fetchall():
    print(dict(r))

print()
print('=== usage rows for one stock 2026-04-16 ===')
cur.execute(
    'SELECT stock_code FROM stock_pool_snapshot WHERE trade_date=? ORDER BY rank_no LIMIT 1',
    ('2026-04-16',)
)
row = cur.fetchone()
if row:
    sc = row['stock_code']
    print(f'Stock: {sc}')
    cur.execute(
        'SELECT dataset_name, source_name, status, substr(status_reason,1,80) as reason'
        ' FROM report_data_usage WHERE stock_code=? AND trade_date=? ORDER BY dataset_name',
        (sc, '2026-04-16')
    )
    for r in cur.fetchall():
        print(dict(r))

conn.close()
