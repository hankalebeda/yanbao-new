import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

# Check all tables
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
all_tables = [r[0] for r in cur.fetchall()]
print("All tables:")
for t in all_tables:
    cur.execute(f'SELECT COUNT(*) FROM "{t}"')
    cnt = cur.fetchone()[0]
    print(f"  {t}: {cnt} rows")

conn.close()
