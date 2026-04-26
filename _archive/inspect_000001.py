import sys
sys.path.insert(0, '.')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()
print('report rows:')
rows = db.execute(text("""
SELECT report_id, trade_date, is_deleted, quality_flag, idempotency_key, superseded_by_report_id, created_at
FROM report
WHERE stock_code='000001.SZ' AND trade_date='2026-04-03'
ORDER BY created_at DESC
LIMIT 10
""")).fetchall()
for r in rows:
    print(r)

print('\nactive task rows:')
rows2 = db.execute(text("""
SELECT task_id, generation_seq, status, retry_count, superseded_at, created_at, updated_at
FROM report_generation_task
WHERE stock_code='000001.SZ' AND trade_date='2026-04-03'
ORDER BY generation_seq DESC, created_at DESC
LIMIT 10
""")).fetchall()
for r in rows2:
    print(r)

db.close()
