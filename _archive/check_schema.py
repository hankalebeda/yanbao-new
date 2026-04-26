import sqlite3
conn = sqlite3.connect('data/app.db')
c = conn.cursor()
c.execute("PRAGMA table_info(report)")
cols = c.fetchall()
print("report table columns:")
for col in cols:
    print(f"  {col}")
conn.close()
