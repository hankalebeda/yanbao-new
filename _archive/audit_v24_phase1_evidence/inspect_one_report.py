import sqlite3
c = sqlite3.connect('data/app.db')
c.row_factory = sqlite3.Row
r = c.execute("SELECT * FROM report WHERE is_deleted=0 AND quality_flag='ok' AND trade_date='2026-04-16' LIMIT 1").fetchone()
for k in r.keys():
    v = r[k]
    if v is None:
        print(f'  {k}: NULL')
    elif isinstance(v, (int, float)):
        print(f'  {k}: {v}')
    else:
        sv = str(v)
        print(f'  {k}: ({len(sv)} chars) {sv[:200]!r}')
