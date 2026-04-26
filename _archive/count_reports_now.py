import sys
sys.path.insert(0,'.')
from app.core.db import SessionLocal
from sqlalchemy import text

db=SessionLocal()
print('visible_reports', db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0")).scalar())
print('visible_ok', db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0 AND quality_flag='ok' ")).scalar())
print('visible_degraded', db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=0 AND quality_flag='degraded' ")).scalar())
db.close()
