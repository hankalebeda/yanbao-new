import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()
cur.execute("SELECT sql FROM sqlite_master WHERE name='kline_daily'")
print(cur.fetchone()[0])
cur.execute("SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='kline_daily'")
for r in cur.fetchall():
    print(r[0])
conn.close()
