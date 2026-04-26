import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
print('=== 所有表 ===')
for t in tables:
    try:
        cur.execute('SELECT COUNT(*) FROM "' + t + '"')
        cnt = cur.fetchone()[0]
        print(t + ': ' + str(cnt))
    except Exception as e:
        print(t + ': ERROR ' + str(e))
conn.close()
