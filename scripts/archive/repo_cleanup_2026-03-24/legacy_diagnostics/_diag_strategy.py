import sqlite3
conn = sqlite3.connect('data/app.db')
c = conn.cursor()
rows = c.execute(
    "SELECT r.stock_code, r.strategy_type, r.trade_date, r.llm_fallback_level "
    "FROM report r "
    "WHERE r.is_deleted = 0 AND r.stock_code IN ('002261.SZ','002470.SZ','002506.SZ','601868.SH','600722.SH') "
    "ORDER BY r.stock_code, r.trade_date"
).fetchall()
print('Reports for hotspot-linked stocks:')
for r in rows: print(r)

print()
rows = c.execute(
    "SELECT stock_code, trade_date, recommendation, confidence, llm_fallback_level "
    "FROM report WHERE strategy_type = 'C' AND is_deleted=0"
).fetchall()
print('C-type reports:', rows)

# Check timing: when were hotspot links created vs reports generated?
print()
rows = c.execute(
    "SELECT created_at FROM market_hotspot_item_stock_link ORDER BY created_at ASC LIMIT 1"
).fetchone()
print(f'First hotspot link created: {rows}')

rows = c.execute(
    "SELECT MIN(created_at), MAX(created_at) FROM report WHERE trade_date='2026-03-12' AND is_deleted=0"
).fetchone()
print(f'3/12 report creation window: {rows}')

conn.close()
