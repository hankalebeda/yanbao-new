"""Check report states for 2026-04-03 stocks"""
import sys
sys.path.insert(0, '.')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()

print("=== Reports for 2026-04-03:")
r = db.execute(text("""
    SELECT quality_flag, published, is_deleted, COUNT(*) 
    FROM report 
    WHERE trade_date='2026-04-03'
    GROUP BY quality_flag, published, is_deleted
    ORDER BY quality_flag, is_deleted
""")).fetchall()
for row in r: print(' ', row)

print("\n=== Processing tasks for last 2 hours:")
r2 = db.execute(text("""
    SELECT status, stock_code, trade_date, status_reason, updated_at
    FROM report_generation_task
    WHERE status='Processing'
    ORDER BY updated_at DESC
    LIMIT 20
""")).fetchall()
for row in r2: print(' ', row)

print("\n=== Most recent task errors:")
r3 = db.execute(text("""
    SELECT status, status_reason, COUNT(*) 
    FROM report_generation_task
    WHERE created_at > datetime('now', '-2 hours')
    GROUP BY status, status_reason
""")).fetchall()
for row in r3: print(' ', row)

db.close()
