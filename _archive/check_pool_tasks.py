"""Check what dates have pool refresh tasks"""
import sys
sys.path.insert(0, '.')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()

print("=== Pool refresh tasks by date:")
r = db.execute(text("PRAGMA table_info(stock_pool_refresh_task)")).fetchall()
cols = [c[1] for c in r]
print("Columns:", cols)

r2 = db.execute(text("SELECT trade_date, status, COUNT(*) FROM stock_pool_refresh_task GROUP BY trade_date, status ORDER BY trade_date DESC LIMIT 15")).fetchall()
for row in r2: print(' ', row)

print("\n=== Market state cache by date:")
r3 = db.execute(text("PRAGMA table_info(market_state_cache)")).fetchall()
print("Columns:", [c[1] for c in r3])
r4 = db.execute(text("SELECT trade_date, market_state, kline_batch_id, hotspot_batch_id FROM market_state_cache ORDER BY trade_date DESC LIMIT 10")).fetchall()
for row in r4: print(' ', row)

db.close()
