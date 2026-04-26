"""Mass-publish the existing 204 ok reports so UI surfaces real content."""
from datetime import datetime, timezone
from sqlalchemy import text
from app import models  # noqa: F401
from app.core.db import SessionLocal

db = SessionLocal()
try:
    now = datetime.now(timezone.utc)
    eligible = db.execute(text("""
        SELECT report_id FROM report 
        WHERE is_deleted = 0 
          AND quality_flag = 'ok'
          AND published = 0
    """)).fetchall()
    ids = [r[0] for r in eligible]
    print(f"publishing {len(ids)} reports...")
    if ids:
        updated = db.execute(text("""
            UPDATE report
            SET published = 1,
                publish_status = 'PUBLISHED',
                published_at = COALESCE(published_at, :now),
                updated_at = :now
            WHERE report_id IN (SELECT value FROM json_each(:ids))
        """), {"now": now, "ids": __import__("json").dumps(ids)}).rowcount
        db.commit()
        print(f"updated rows: {updated}")

    print("after:")
    for r in db.execute(text("SELECT published, COUNT(*) FROM report WHERE is_deleted=0 GROUP BY published")).fetchall():
        print(" ", r)
finally:
    db.close()
