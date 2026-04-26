"""Check all active (non-superseded) task states"""
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
print("Active tasks 2026-04-03 (superseded_at IS NULL):", r)

r2 = db.execute(text("""
    SELECT status, COUNT(*) FROM report_generation_task 
    WHERE trade_date='2026-04-03'
    GROUP BY status
""")).fetchall()
print("All tasks 2026-04-03:", r2)

# Check stocks where status=Processing (stuck)
r3 = db.execute(text("""
    SELECT stock_code, status, created_at, updated_at FROM report_generation_task 
    WHERE trade_date='2026-04-03' AND superseded_at IS NULL AND status='Processing'
    ORDER BY updated_at DESC LIMIT 10
""")).fetchall()
print("Processing tasks (stuck):", r3[:5])
print(f"Total Processing: {len(r3)}")

# Actually count ALL
r4 = db.execute(text("""
    SELECT COUNT(*) FROM report_generation_task 
    WHERE trade_date='2026-04-03' AND superseded_at IS NULL AND status='Processing'
""")).scalar()
print(f"Total Processing tasks (stuck): {r4}")

db.close()
