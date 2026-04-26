import sqlite3

conn = sqlite3.connect('data/app.db')
c = conn.cursor()

c.execute(
    """
    SELECT u.stock_code, MAX(u.trade_date) latest_trade_date
    FROM report_data_usage u
    WHERE NOT EXISTS (
      SELECT 1 FROM report r
      WHERE r.stock_code = u.stock_code
        AND r.published = 1
        AND r.is_deleted = 0
    )
    GROUP BY u.stock_code
    ORDER BY latest_trade_date DESC, u.stock_code
    LIMIT 30
    """
)
rows = [r[0] for r in c.fetchall()]
print('candidates=', len(rows))
print('sample=', rows[:20])

conn.close()
