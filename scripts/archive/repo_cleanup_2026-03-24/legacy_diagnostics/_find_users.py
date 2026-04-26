import sqlite3
c = sqlite3.connect("data/app.db")
tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%user%'").fetchall()]
print("User tables:", tables)
for t in tables:
    cols = [x[1] for x in c.execute(f"PRAGMA table_info({t})").fetchall()]
    print(f"  {t}: {cols}")
    rows = c.execute(f"SELECT * FROM {t} LIMIT 5").fetchall()
    for r in rows:
        print(f"    {list(r)}")
# Also check 'user' table
for name in ["user", "users", "account"]:
    try:
        cnt = c.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"\n{name} table has {cnt} rows")
        cols = [x[1] for x in c.execute(f"PRAGMA table_info({name})").fetchall()]
        print(f"  cols: {cols}")
        rows = c.execute(f"SELECT * FROM {name} LIMIT 3").fetchall()
        for r in rows:
            print(f"    {list(r)}")
    except:
        pass
c.close()
