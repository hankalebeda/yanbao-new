"""Diagnose CONCURRENT_CONFLICT for specific stocks"""
import sys
sys.path.insert(0, '.')
from app.core.db import SessionLocal
from sqlalchemy import text
db = SessionLocal()

# List all active tasks
r = db.execute(text("""
    SELECT stock_code, status FROM report_generation_task 
    WHERE trade_date='2026-04-03' AND superseded_at IS NULL
    ORDER BY stock_code
""")).fetchall()
print(f"Total active tasks: {len(r)}")
for row in r:
    print(f"  {row[0]}: {row[1]}")

# List stocks with NO active task
r2 = db.execute(text("""
    SELECT DISTINCT stock_code FROM report_data_usage 
    WHERE trade_date='2026-04-03'
    GROUP BY stock_code HAVING COUNT(DISTINCT dataset_name) >= 5
""")).fetchall()
all_data_stocks = {s[0] for s in r2}
active_stocks = {row[0] for row in r}
no_task_stocks = all_data_stocks - active_stocks
print(f"\nStocks with data but NO active task: {len(no_task_stocks)}")
for s in sorted(no_task_stocks)[:20]:
    print(f"  {s}")
db.close()
