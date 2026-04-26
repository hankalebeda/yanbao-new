"""Check hotspot batches and usage for 2026-04-03"""
import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()

# What hotspot batches exist for 2026-04-03?
print('=== Hotspot data_batch for 2026-04-03 ===')
cur.execute("""SELECT batch_id, source_name, batch_scope, batch_status, quality_flag, 
records_total, records_success 
FROM data_batch WHERE trade_date='2026-04-03' AND source_name LIKE '%hotspot%' 
OR (trade_date='2026-04-03' AND batch_scope LIKE '%hotspot%')
ORDER BY source_name""")
for r in cur.fetchall():
    print(r)

# How many hotspot items?
print('\n=== Hotspot items for 2026-04-03 batch ===')
cur.execute("""SELECT db.source_name, count(hi.hotspot_item_id) 
FROM data_batch db 
JOIN market_hotspot_item hi ON hi.batch_id = db.batch_id
WHERE db.trade_date='2026-04-03'
GROUP BY db.source_name ORDER BY db.source_name""")
for r in cur.fetchall():
    print(r)

# How many unique stocks linked?
print('\n=== Unique stocks in hotspot links for 2026-04-03 ===')
cur.execute("""SELECT count(DISTINCT hisl.stock_code)
FROM data_batch db 
JOIN market_hotspot_item hi ON hi.batch_id = db.batch_id
JOIN market_hotspot_item_stock_link hisl ON hisl.hotspot_item_id = hi.hotspot_item_id
WHERE db.trade_date='2026-04-03'""")
print('unique stocks:', cur.fetchone()[0])

# Check data_batch for hotspot merged
print('\n=== data_batch hotspot_merged 2026-04-03 ===')
cur.execute("""SELECT batch_id, source_name, batch_scope, quality_flag, records_total
FROM data_batch WHERE trade_date='2026-04-03' AND batch_scope LIKE '%merged%'""")
for r in cur.fetchall():
    print(r)
    
# Does data_batch have "eastmoney" source for hotspot on 2026-04-03?
print('\n=== data_batch all sources 2026-04-03 ===')
cur.execute("""SELECT source_name, batch_scope, quality_flag, records_total, batch_status
FROM data_batch WHERE trade_date='2026-04-03' ORDER BY source_name""")
for r in cur.fetchall():
    print(r)
