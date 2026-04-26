import sqlite3
c = sqlite3.connect('data/app.db')
c.row_factory = sqlite3.Row
print('=== kline tables ===')
for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'kline%' ORDER BY name"):
    print(' ', r['name'])
for tbl in ('kline_daily', 'kline_history', 'kline_qfq', 'kline'):
    try:
        n = c.execute(f"SELECT COUNT(*) FROM {tbl} WHERE stock_code='601898.SH' AND trade_date BETWEEN '2026-04-01' AND '2026-04-16'").fetchone()[0]
        print(f'  {tbl}: {n}')
    except Exception as e:
        print(f'  {tbl}: ERR {e}')
print()
print('=== data_batch 04-09 ===')
for r in c.execute("SELECT batch_id, source_name, batch_status, records_total, records_success, records_failed FROM data_batch WHERE trade_date='2026-04-09' ORDER BY source_name"):
    print(' ', dict(r))
print()
print('=== market_state_cache 04-09 ===')
for r in c.execute("SELECT trade_date, market_state, kline_batch_id FROM market_state_cache WHERE trade_date='2026-04-09'"):
    print(' ', dict(r))
print()
print('=== report_generation_task 04-09 601898.SH ===')
for r in c.execute("SELECT task_id, status, snapshot_kline_batch_id, snapshot_hotspot_batch_id FROM report_generation_task WHERE trade_date='2026-04-09' AND stock_code='601898.SH'"):
    print(' ', dict(r))
