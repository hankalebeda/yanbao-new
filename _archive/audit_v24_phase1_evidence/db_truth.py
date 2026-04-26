import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()

def q(label, sql):
    print(f'=== {label} ===')
    cur.execute(sql)
    for r in cur.fetchall():
        print(r)

q('hotspot_raw cols', "PRAGMA table_info(hotspot_raw)")
q('hotspot_normalized cols', "PRAGMA table_info(hotspot_normalized)")
q('hotspot_top50 cols', "PRAGMA table_info(hotspot_top50)")
q('hotspot_raw count', "SELECT COUNT(*) FROM hotspot_raw")
q('hotspot_normalized count', "SELECT COUNT(*) FROM hotspot_normalized")
q('hotspot_top50 count', "SELECT COUNT(*) FROM hotspot_top50")
q('kline_daily coverage latest 5 days', """
  SELECT trade_date, COUNT(DISTINCT stock_code) FROM kline_daily
  WHERE trade_date >= '2026-04-10'
  GROUP BY trade_date ORDER BY trade_date DESC
""")
q('report by trade_date (latest 10)', """
  SELECT trade_date, COUNT(*),
         SUM(CASE WHEN is_deleted=1 THEN 1 ELSE 0 END) AS del,
         SUM(CASE WHEN is_deleted=0 OR is_deleted IS NULL THEN 1 ELSE 0 END) AS alive
  FROM report GROUP BY trade_date ORDER BY trade_date DESC LIMIT 10
""")
q('report quality_flag distribution alive', """
  SELECT quality_flag, COUNT(*) FROM report
  WHERE is_deleted=0 OR is_deleted IS NULL GROUP BY quality_flag
""")
q('stock_pool active', "SELECT COUNT(*) FROM stock_pool WHERE is_active=1")
q('settlement_result latest', """
  SELECT is_misclassified, exit_reason, COUNT(*) FROM settlement_result
  GROUP BY is_misclassified, exit_reason
""")
