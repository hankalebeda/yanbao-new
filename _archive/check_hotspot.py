import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

for t in ['market_hotspot_item', 'market_hotspot_item_source']:
    print(f'=== {t} ===')
    cur.execute(f'PRAGMA table_info("{t}")')
    cols = cur.fetchall()
    for c in cols:
        print(c)
    cur.execute(f'SELECT * FROM "{t}" LIMIT 3')
    for r in cur.fetchall():
        print('ROW:', r)
    print()

conn.close()
