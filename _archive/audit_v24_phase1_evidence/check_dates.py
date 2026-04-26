"""Check trading dates and pool coverage for target dates"""
import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()

dates = ['2026-04-07','2026-04-08','2026-04-09','2026-04-10',
         '2026-04-11','2026-04-13','2026-04-14','2026-04-15','2026-04-16']

print('=== Market state cache + report counts per date ===')
for d in dates:
    cur.execute("SELECT market_state FROM market_state_cache WHERE trade_date=?", (d,))
    ms = cur.fetchone()
    cur.execute("SELECT count(*) FROM report WHERE trade_date=? AND is_deleted=0", (d,))
    rep = cur.fetchone()
    cur.execute("SELECT count(DISTINCT stock_code) FROM report_data_usage WHERE trade_date=? AND dataset_name='kline_daily' AND status='ok'", (d,))
    kline_ok = cur.fetchone()
    print(f"  {d}: market_state={'yes' if ms else 'no'}, alive_reports={rep[0]}, kline_ok_stocks={kline_ok[0]}")

print('\n=== Stock pool coverage for target dates ===')
for d in dates:
    cur.execute("""SELECT count(DISTINCT stock_code) FROM stock_pool_snapshot 
    WHERE trade_date<=? ORDER BY trade_date DESC LIMIT 200""", (d,))
    # Better: get most recent pool before this date
    cur.execute("""SELECT count(DISTINCT sps.stock_code) FROM stock_pool_snapshot sps
    JOIN stock_pool_refresh_task sprt ON sprt.refresh_task_id = sps.refresh_task_id
    WHERE sprt.trade_date <= ? ORDER BY sprt.trade_date DESC""", (d,))
    pool = cur.fetchone()
    print(f"  {d}: accessible_pool={pool[0] if pool else 0}")
