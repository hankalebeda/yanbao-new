import sqlite3
c = sqlite3.connect('data/app.db')
print('=== report counts by date ===')
for r in c.execute("SELECT trade_date, COUNT(*) FROM report WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' AND is_deleted=0 GROUP BY trade_date ORDER BY trade_date"):
    print(r)
print('=== quality_flag dist (04-08..04-16) ===')
for r in c.execute("SELECT trade_date, quality_flag, COUNT(*) FROM report WHERE trade_date BETWEEN '2026-04-08' AND '2026-04-16' AND is_deleted=0 GROUP BY trade_date, quality_flag ORDER BY trade_date, quality_flag"):
    print(r)
print('=== llm_fallback_level dist ===')
for r in c.execute("SELECT llm_fallback_level, COUNT(*) FROM report WHERE trade_date BETWEEN '2026-04-08' AND '2026-04-16' AND is_deleted=0 GROUP BY llm_fallback_level"):
    print(r)
