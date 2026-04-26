"""Check task states for 2026-04-03"""
import sys
sys.path.insert(0, '.')
from app.core.db import SessionLocal
from sqlalchemy import text
db = SessionLocal()
r = db.execute(text("""
    SELECT status, COUNT(*) FROM report_generation_task 
    WHERE trade_date='2026-04-03' AND superseded_at IS NULL
    GROUP BY status
""")).fetchall()
print("Active tasks for 2026-04-03:", r)

r2 = db.execute(text("""
    SELECT status, COUNT(*) FROM report_generation_task 
    WHERE trade_date='2026-04-03'
    GROUP BY status
""")).fetchall()
print("All tasks for 2026-04-03:", r2)

# check 000001.SZ specificity
r3 = db.execute(text("""
    SELECT task_id, status, superseded_at, generation_seq FROM report_generation_task 
    WHERE trade_date='2026-04-03' AND stock_code='000001.SZ'
    ORDER BY generation_seq DESC LIMIT 5
""")).fetchall()
print("000001.SZ tasks:", r3)
db.close()
