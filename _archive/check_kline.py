"""Check kline data coverage and try to backfill"""
import sys
sys.path.insert(0, '.')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()

print("=== Kline coverage by recent date:")
r = db.execute(text("SELECT trade_date, COUNT(DISTINCT stock_code) FROM kline_daily WHERE trade_date >= date('now', '-10 days') GROUP BY trade_date ORDER BY trade_date DESC")).fetchall()
for row in r: print(' ', row)

print("\n=== Total kline stocks:")
total = db.execute(text("SELECT COUNT(DISTINCT stock_code) FROM kline_daily")).scalar()
print(' Total distinct stocks:', total)

# What stocks are in pool vs kline for 2026-04-14
print("\n=== Pool stocks for 2026-04-14:")
r2 = db.execute(text("SELECT COUNT(*) FROM stock_pool_snapshot WHERE trade_date='2026-04-14'")).scalar()
print(' Pool total:', r2)

print("\n=== Kline for 2026-04-14:")
r3 = db.execute(text("SELECT COUNT(DISTINCT stock_code) FROM kline_daily WHERE trade_date='2026-04-14'")).scalar()
print(' Kline count:', r3)

# Pool stocks that have kline for 2026-04-14
r4 = db.execute(text("""
    SELECT COUNT(*) FROM stock_pool_snapshot p 
    JOIN kline_daily k ON k.stock_code=p.stock_code AND k.trade_date=p.trade_date
    WHERE p.trade_date='2026-04-14'
""")).scalar()
print(' Pool stocks with kline:', r4)

db.close()
