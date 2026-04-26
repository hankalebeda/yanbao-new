"""Analyze where report data comes from"""
import sys
sys.path.insert(0, '.')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()

# Get one ok report
report_id = db.execute(text("SELECT report_id, stock_code, trade_date FROM report WHERE is_deleted=0 AND quality_flag='ok' LIMIT 1")).fetchone()
print(f"Analyzing report: {report_id}")

if report_id:
    rid = report_id[0]
    r = db.execute(text("""
        SELECT u.dataset_name, u.status, u.status_reason, u.source_name
        FROM report_data_usage u 
        JOIN report_data_usage_link l ON l.usage_id=u.usage_id 
        WHERE l.report_id=:rid
    """), {"rid": rid}).fetchall()
    for row in r:
        print(f"  {row}")

# Check market_hotspot_item table
print("\n=== market_hotspot_item sample:")
r2 = db.execute(text("PRAGMA table_info(market_hotspot_item)")).fetchall()
for row in r2: print(' ', row)
r2b = db.execute(text("SELECT COUNT(*) FROM market_hotspot_item")).scalar()
print('  Total count:', r2b)

# Check if there's a data_fetch service 
print("\n=== data_batch table:")
r3 = db.execute(text("SELECT batch_date, batch_type, status, COUNT(*) FROM data_batch GROUP BY batch_date, batch_type, status ORDER BY batch_date DESC LIMIT 20")).fetchall()
for row in r3: print(' ', row)

db.close()
