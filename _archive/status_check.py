"""Quick status check"""
import sys
sys.path.insert(0, '.')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()
print("OK reports:", db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0 AND quality_flag='ok'")).scalar())
print("Processing tasks:", db.execute(text("SELECT COUNT(*) FROM report_generation_task WHERE status='Processing'")).scalar())
print("Recent tasks:", db.execute(text("SELECT status, COUNT(*) FROM report_generation_task WHERE updated_at > datetime('now', '-60 minutes') GROUP BY status")).fetchall())
db.close()
