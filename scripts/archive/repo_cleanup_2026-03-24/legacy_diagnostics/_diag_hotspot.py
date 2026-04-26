"""Diagnose hotspot data and stock links."""
import sqlite3
conn = sqlite3.connect('data/app.db')
c = conn.cursor()

print('=== Hotspot Item columns ===')
for r in c.execute('PRAGMA table_info(market_hotspot_item)').fetchall():
    print(r[1], r[2])

print('\n=== Hotspot StockLink columns ===')
for r in c.execute('PRAGMA table_info(market_hotspot_item_stock_link)').fetchall():
    print(r[1], r[2])

print('\n=== Hotspot items with news_event_type (latest 10) ===')
rows = c.execute("""
    SELECT hotspot_item_id, topic_title, news_event_type, merged_rank 
    FROM market_hotspot_item 
    WHERE news_event_type IS NOT NULL AND news_event_type != ''
    ORDER BY fetch_time DESC LIMIT 10
""").fetchall()
for r in rows:
    print(r)
total_with_type = c.execute(
    "SELECT COUNT(*) FROM market_hotspot_item WHERE news_event_type IS NOT NULL AND news_event_type != ''"
).fetchone()[0]
print(f'Total with news_event_type: {total_with_type}')

print('\n=== Stock Links (all) ===')
rows = c.execute('SELECT * FROM market_hotspot_item_stock_link LIMIT 20').fetchall()
for r in rows:
    print(r)
total_links = c.execute('SELECT COUNT(*) FROM market_hotspot_item_stock_link').fetchone()[0]
print(f'Total stock links: {total_links}')

print('\n=== Hotspot items sample (latest 5 with stock codes) ===')
rows = c.execute("""
    SELECT m.hotspot_item_id, m.topic_title, m.news_event_type, l.stock_code
    FROM market_hotspot_item m
    LEFT JOIN market_hotspot_item_stock_link l ON m.hotspot_item_id = l.hotspot_item_id
    WHERE l.stock_code IS NOT NULL
    LIMIT 10
""").fetchall()
for r in rows:
    print(r)

conn.close()
