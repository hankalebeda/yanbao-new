"""Check market_state_cache and data_batch references for recent dates"""
import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()

print('=== market_state_cache with batch refs ===')
cur.execute("""SELECT trade_date, market_state, kline_batch_id, hotspot_batch_id
FROM market_state_cache ORDER BY trade_date DESC LIMIT 12""")
rows = cur.fetchall()
for r in rows:
    kline_bid = r[2]
    hotspot_bid = r[3]
    
    # Check if these batches exist
    kline_exists = cur.execute("SELECT 1 FROM data_batch WHERE batch_id=? LIMIT 1", (kline_bid,)).fetchone() if kline_bid else None
    hotspot_exists = cur.execute("SELECT 1 FROM data_batch WHERE batch_id=? LIMIT 1", (hotspot_bid,)).fetchone() if hotspot_bid else None
    
    print(f"  {r[0]} {r[1]} kline_batch={'exists' if kline_exists else 'MISSING' if kline_bid else 'null'} hotspot_batch={'exists' if hotspot_exists else 'MISSING' if hotspot_bid else 'null'}")
