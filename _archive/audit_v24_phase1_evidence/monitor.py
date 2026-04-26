import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()
cur.execute("SELECT trade_date, count(*) FROM report WHERE is_deleted=0 AND trade_date>='2026-04-07' GROUP BY trade_date ORDER BY trade_date")
print("生成完成的研报数 (alive, not deleted):")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")
cur.execute("SELECT count(*) FROM report WHERE is_deleted=0 AND trade_date>='2026-04-07'")
print(f"Total alive: {cur.fetchone()[0]}")
