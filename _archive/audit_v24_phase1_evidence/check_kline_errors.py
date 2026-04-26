"""Check batch errors for kline backfill on 2026-04-07"""
import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()

# Most recent kline backfill batch
cur.execute("""SELECT batch_id, batch_status, quality_flag, records_total, records_success, records_failed
FROM data_batch WHERE trade_date='2026-04-07' AND source_name='eastmoney' AND batch_scope='backfill_missing'
ORDER BY created_at DESC LIMIT 3""")
batches = cur.fetchall()
print('Kline backfill batches for 2026-04-07:', batches)

if batches:
    batch_id = batches[0][0]
    # Get sample errors
    cur.execute("""SELECT stock_code, error_code, error_message, error_stage 
    FROM batch_error_log WHERE batch_id=? LIMIT 10""", (batch_id,))
    errors = cur.fetchall()
    print(f'\nSample errors for batch {batch_id[:8]}:')
    error_msgs = {}
    for e in errors:
        msg = e[2]
        error_msgs[msg] = error_msgs.get(msg, 0) + 1
    
    # Count by error type
    cur.execute("""SELECT error_message, count(*) FROM batch_error_log WHERE batch_id=? GROUP BY error_message""", (batch_id,))
    for r in cur.fetchall():
        print(f'  {r[1]}x: {r[0]}')

# Check if there's a kline record for any stock on 2026-04-07
cur.execute("""SELECT stock_code, trade_date FROM kline_daily WHERE trade_date='2026-04-07' LIMIT 5""")
print('\nExisting kline records for 2026-04-07:', cur.fetchall())
