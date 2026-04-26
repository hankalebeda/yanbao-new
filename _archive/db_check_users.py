import sqlite3
conn = sqlite3.connect("data/app.db")
cur = conn.cursor()
cur.execute("SELECT user_id, email, role, tier FROM app_user ORDER BY role DESC LIMIT 10")
for r in cur.fetchall():
    print(r)
conn.close()
