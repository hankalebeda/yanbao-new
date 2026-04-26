import sqlite3

conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

rows = cur.execute(
    """
    SELECT r.trade_date,
           COUNT(*) AS reports,
           SUM(CASE WHEN EXISTS (
               SELECT 1 FROM settlement_result sr
               WHERE sr.report_id = r.report_id
           ) THEN 1 ELSE 0 END) AS settled
    FROM report r
    WHERE r.published = 1 AND r.trade_date IS NOT NULL
    GROUP BY r.trade_date
    ORDER BY r.trade_date ASC
    """
).fetchall()

print('trade_date,reports,settled,coverage')
for td, rp, st in rows:
    cov = (st / rp * 100.0) if rp else 0.0
    print(f'{td},{rp},{st},{cov:.1f}%')

print('\nTOTAL_REPORTS=', cur.execute('SELECT COUNT(*) FROM report WHERE published=1').fetchone()[0])
print('TOTAL_SETTLED=', cur.execute('SELECT COUNT(*) FROM settlement_result').fetchone()[0])

conn.close()
