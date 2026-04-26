import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
print('=== pool/task/stock tables ===')
for t in tables:
    if any(k in t for k in ['pool', 'task', 'stock']):
        print(t)
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print('  count:', cur.fetchone()[0])

print('\n=== stock_pool_refresh_task sample ===')
try:
    cur.execute("SELECT * FROM stock_pool_refresh_task ORDER BY created_at DESC LIMIT 3")
    rows = cur.fetchall()
    cur.execute("PRAGMA table_info(stock_pool_refresh_task)")
    cols = [r[1] for r in cur.fetchall()]
    print('cols:', cols)
    for row in rows:
        print(row[:8])
except Exception as e:
    print('error:', e)

print('\n=== stock_master sample ===')
try:
    cur.execute("SELECT COUNT(*) FROM stock_master")
    print('count:', cur.fetchone()[0])
    cur.execute("SELECT stock_code, stock_name FROM stock_master LIMIT 5")
    print(cur.fetchall())
except Exception as e:
    print('error:', e)
