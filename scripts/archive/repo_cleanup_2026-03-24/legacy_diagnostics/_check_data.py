import sqlite3
conn = sqlite3.connect('data/app.db')
checks = [
    ('stock_master', "SELECT COUNT(*) FROM stock_master"),
    ('kline_daily', "SELECT COUNT(*) FROM kline_daily"),
    ('core_pool', "SELECT COUNT(*) FROM stock_pool_snapshot WHERE pool_role='core'"),
    ('standby_pool', "SELECT COUNT(*) FROM stock_pool_snapshot WHERE pool_role='standby'"),
    ('hotspot', "SELECT COUNT(*) FROM market_hotspot_item"),
    ('market_state', "SELECT COUNT(*) FROM market_state_snapshot"),
    ('report_data_usage', "SELECT COUNT(*) FROM report_data_usage"),
    ('data_batch', "SELECT COUNT(*) FROM data_batch"),
]
for name, sql in checks:
    try:
        r = conn.execute(sql).fetchone()
        print(f'{name:25s}: {r[0]}')
    except Exception as e:
        print(f'{name:25s}: ERROR {e}')
conn.close()
