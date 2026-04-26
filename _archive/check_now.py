"""Check state after test - did generation happen?"""
import sys, urllib.request as u, json, os, time
sys.path.insert(0, '.')
os.environ.pop('http_proxy', None)
u.install_opener(u.build_opener(u.ProxyHandler({})))
base = 'http://127.0.0.1:8010'
hdr = {'X-Internal-Token': 'kestra-internal-20260327', 'Content-Type': 'application/json'}

from app.core.db import SessionLocal
from sqlalchemy import text
db = SessionLocal()

# Check recent tasks
r = db.execute(text("""
    SELECT stock_code, status, created_at, updated_at FROM report_generation_task 
    WHERE trade_date='2026-04-03' 
    ORDER BY created_at DESC LIMIT 10
""")).fetchall()
print("Recent tasks:")
for row in r: print(f"  {row}")

# Check current ok reports
r2 = db.execute(text("SELECT COUNT(*), quality_flag FROM report WHERE is_deleted=0 GROUP BY quality_flag")).fetchall()
print("\nCurrent ok reports:", r2)

# Check 000002.SZ task status
r3 = db.execute(text("""
    SELECT task_id, status, created_at FROM report_generation_task 
    WHERE trade_date='2026-04-03' AND stock_code='000002.SZ'
    ORDER BY created_at DESC LIMIT 3
""")).fetchall()
print("\n000002.SZ tasks:", r3)

db.close()
