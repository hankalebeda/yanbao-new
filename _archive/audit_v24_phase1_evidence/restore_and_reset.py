"""Restore accidentally deleted BEAR_MARKET_FILTERED reports on 04-08 (legitimate
business-logic degrade); keep LLM_FALLBACK deletions; reset Processing tasks.
"""
import sqlite3

c = sqlite3.connect('data/app.db')
c.execute('PRAGMA busy_timeout=60000')
cur = c.cursor()

n = cur.execute(
    "UPDATE report SET is_deleted=0, deleted_at=NULL "
    "WHERE trade_date='2026-04-08' AND is_deleted=1 AND status_reason='BEAR_MARKET_FILTERED'"
).rowcount
print(f'restored BEAR_MARKET_FILTERED reports: {n}')

m = cur.execute(
    "UPDATE report_generation_task SET status='Expired', updated_at=CURRENT_TIMESTAMP "
    "WHERE status='Processing' AND trade_date BETWEEN '2026-04-10' AND '2026-04-16'"
).rowcount
print(f'reset stuck Processing tasks: {m}')

c.commit()
print()
print('=== final state ===')
for r in cur.execute(
    "SELECT trade_date, COUNT(*) FROM report WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' AND is_deleted=0 GROUP BY trade_date ORDER BY trade_date"
):
    print(' ', r)

print('=== quality dist ===')
for r in cur.execute(
    "SELECT trade_date, quality_flag, COUNT(*) FROM report WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' AND is_deleted=0 GROUP BY trade_date, quality_flag ORDER BY trade_date"
):
    print(' ', r)

c.close()
