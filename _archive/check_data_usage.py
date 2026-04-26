"""Check report_data_usage coverage by trade_date"""
import sys
sys.path.insert(0, '.')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()

print("=== report_data_usage by trade_date:")
r = db.execute(text("SELECT trade_date, COUNT(DISTINCT stock_code) as stocks, COUNT(DISTINCT dataset_name) as datasets FROM report_data_usage GROUP BY trade_date ORDER BY trade_date DESC LIMIT 15")).fetchall()
for row in r: print(' ', row)

print("\n=== report_data_usage for key stocks on 2026-04-14:")
r2 = db.execute(text("SELECT dataset_name, status, source_name FROM report_data_usage WHERE stock_code='601888.SH' AND trade_date='2026-04-14'")).fetchall()
for row in r2: print(' ', row)

print("\n=== resolve_refresh_task check - pool snapshots by date:")
r3 = db.execute(text("SELECT trade_date, status FROM stock_pool_refresh_task WHERE trade_date >= '2026-04-10' ORDER BY trade_date DESC")).fetchall()
for row in r3: print(' ', row)

print("\n=== Pool snapshot items count by date:")
try:
    r4 = db.execute(text("SELECT pss.trade_date, COUNT(*) FROM pool_snapshot_item psi JOIN pool_snapshot pss ON pss.id=psi.pool_snapshot_id GROUP BY pss.trade_date ORDER BY pss.trade_date DESC LIMIT 10")).fetchall()
    for row in r4: print(' ', row)
except Exception as e:
    print(f"  Error: {e}")
    # Try alternative schema
    r4b = db.execute(text("PRAGMA table_info(stock_pool_snapshot)")).fetchall()
    print("stock_pool_snapshot cols:", [c[1] for c in r4b])

db.close()
