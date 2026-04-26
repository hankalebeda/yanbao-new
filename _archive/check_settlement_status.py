import sqlite3

conn = sqlite3.connect('data/app.db')
c = conn.cursor()

c.execute("SELECT quality_flag, COUNT(1) FROM report WHERE published=1 AND is_deleted=0 GROUP BY quality_flag")
print('Reports by quality_flag:')
for r in c.fetchall():
    print(' ', r)

c.execute("SELECT COUNT(1) FROM settlement_result")
print('Total settlement_result:', c.fetchone()[0])

c.execute("SELECT COUNT(DISTINCT stock_code) FROM settlement_result")
print('Distinct stocks settled:', c.fetchone()[0])

c.execute("SELECT stock_code, COUNT(1) cnt FROM settlement_result GROUP BY stock_code ORDER BY cnt DESC LIMIT 5")
print('Top stocks by settlement count:')
for r in c.fetchall():
    print(' ', r)

# Check if stale_ok reports have signal_entry_price and trade_date
c.execute("SELECT COUNT(1) FROM report WHERE published=1 AND is_deleted=0 AND quality_flag='stale_ok' AND signal_entry_price IS NOT NULL AND trade_date IS NOT NULL")
print('stale_ok with entry price + trade_date:', c.fetchone()[0])

conn.close()
