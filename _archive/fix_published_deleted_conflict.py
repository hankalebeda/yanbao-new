import sys
sys.path.insert(0, '.')
from sqlalchemy import text
from app.core.db import SessionLocal

db = SessionLocal()
try:
    before = db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=1 AND COALESCE(published,0)=1")).scalar()
    db.execute(text("UPDATE report SET published=0 WHERE is_deleted=1 AND COALESCE(published,0)=1"))
    db.commit()
    after = db.execute(text("SELECT COUNT(*) FROM report WHERE is_deleted=1 AND COALESCE(published,0)=1")).scalar()
    print({"before": before, "after": after})
finally:
    db.close()
