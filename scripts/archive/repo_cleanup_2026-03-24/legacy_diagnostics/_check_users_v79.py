import sqlite3
c = sqlite3.connect("data/app.db")
# Find user table name
tables = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%user%'").fetchall()
print("User tables:", tables)
for t in tables:
    cols = c.execute(f"PRAGMA table_info({t[0]})").fetchall()
    print(f"\n{t[0]} columns: {[col[1] for col in cols]}")
    rows = c.execute(f"SELECT * FROM {t[0]}").fetchall()
    for r in rows:
        print(r)
c.close()
