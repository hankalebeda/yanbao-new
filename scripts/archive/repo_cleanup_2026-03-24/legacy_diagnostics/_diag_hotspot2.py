"""Check hotspot/pool overlap."""
import sqlite3
conn = sqlite3.connect('data/app.db')
c = conn.cursor()

rows = c.execute(
    "SELECT stock_code FROM stock_pool_snapshot WHERE pool_role = 'core' ORDER BY stock_code LIMIT 20"
).fetchall()
print('=== Core pool stock codes (sample) ===')
for r in rows:
    print(r[0])

rows = c.execute("""
    SELECT l.stock_code, m.topic_title, m.news_event_type
    FROM market_hotspot_item_stock_link l
    JOIN market_hotspot_item m ON m.hotspot_item_id = l.hotspot_item_id
    WHERE l.stock_code IN (
        SELECT stock_code FROM stock_pool_snapshot WHERE pool_role = 'core'
    )
""").fetchall()
print(f'\nHotspot items linked to core pool stocks: {len(rows)}')
for r in rows:
    print(r)

# Check what ALL the hotspot stock link codes are
rows = c.execute("SELECT DISTINCT stock_code FROM market_hotspot_item_stock_link").fetchall()
print(f'\nAll stock codes in hotspot links:')
for r in rows:
    print(r[0])

conn.close()
