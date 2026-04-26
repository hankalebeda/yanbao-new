import sqlite3, sys
sys.path.insert(0, 'd:/yanbao-new')
conn = sqlite3.connect('data/app.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

stocks = ['000858.SZ', '002594.SZ', '600519.SH']
trade_date = '2026-04-16'

print('=== pool membership check ===')
for sc in stocks:
    cur.execute('SELECT stock_code, pool_role FROM stock_pool_snapshot WHERE stock_code=? AND trade_date=?', (sc, trade_date))
    r = cur.fetchone()
    print(f'{sc}: {dict(r) if r else "NOT IN POOL"}')

print()
print('=== kline data for stocks ===')
for sc in stocks:
    cur.execute('SELECT stock_code, close, ma5, ma20, atr_pct FROM kline_daily WHERE stock_code=? AND trade_date=?', (sc, trade_date))
    r = cur.fetchone()
    print(f'{sc}: {dict(r) if r else "NO KLINE"}')

print()
print('=== complete dataset coverage per stock ===')
required = ['kline_daily', 'hotspot_top50', 'northbound_summary', 'etf_flow_summary', 'market_state_input',
            'main_force_flow', 'dragon_tiger_list', 'margin_financing', 'stock_profile']

for sc in stocks:
    cur.execute(
        'SELECT dataset_name, status FROM report_data_usage WHERE stock_code=? AND trade_date=? ORDER BY dataset_name',
        (sc, trade_date)
    )
    rows = {r['dataset_name']: r['status'] for r in cur.fetchall()}
    print(f'\n{sc}:')
    for ds in required:
        status = rows.get(ds, 'MISSING')
        print(f'  {ds}: {status}')

conn.close()
