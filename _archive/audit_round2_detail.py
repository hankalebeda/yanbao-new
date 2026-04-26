import sqlite3
c = sqlite3.connect('data/app.db').cursor()

# Pool related tables
tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
pool_tables = [t for t in tables if 'pool' in t.lower() or 'task' in t.lower() or 'generation' in t.lower()]
print('Pool/task tables:', pool_tables)

# Check stock_pool_refresh_task
try:
    rows = c.execute('SELECT trade_date, status, core_pool_size FROM stock_pool_refresh_task ORDER BY trade_date DESC LIMIT 5').fetchall()
    print('stock_pool_refresh_task:', rows)
except Exception as e:
    print('stock_pool_refresh_task:', e)

# Check report_generation_task
try:
    rows = c.execute("SELECT trade_date, status, COUNT(*) as cnt FROM report_generation_task GROUP BY trade_date, status ORDER BY trade_date DESC LIMIT 10").fetchall()
    print('report_generation_task:', rows)
except Exception as e:
    print('report_generation_task:', e)

# settlement_result details
rows = c.execute('SELECT report_id, signal_date, exit_trade_date, window_days, net_return_pct FROM settlement_result').fetchall()
print('settlement_result:', rows)

# strategy_metric_snapshot - future dates
rows = c.execute("SELECT snapshot_date, window_days, strategy_type, sample_size, data_status FROM strategy_metric_snapshot WHERE snapshot_date > '2026-04-25' ORDER BY snapshot_date DESC LIMIT 10").fetchall()
print('SMS future dates (>2026-04-25):', rows)

# baseline_metric_snapshot 
rows = c.execute("SELECT snapshot_date, COUNT(*) FROM baseline_metric_snapshot GROUP BY snapshot_date ORDER BY snapshot_date DESC LIMIT 5").fetchall()
print('baseline_metric_snapshot:', rows)
