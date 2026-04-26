import sys
sys.path.insert(0,'.')
from app.core.db import SessionLocal
from sqlalchemy import text

db=SessionLocal()
rows=db.execute(text("""
SELECT task_id,generation_seq,status,retry_count,superseded_at,created_at,updated_at
FROM report_generation_task
WHERE stock_code='000001.SZ' AND trade_date='2026-04-03'
ORDER BY generation_seq DESC,created_at DESC
LIMIT 8
""")).fetchall()
for r in rows:
    print(r)
db.close()
