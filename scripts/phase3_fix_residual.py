import sys
sys.path.insert(0, r'D:\yanbao-new')
import app.models
from datetime import datetime, timezone
from uuid import uuid4
from sqlalchemy import text
from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_report_ssot

db = SessionLocal()
now = datetime.now(timezone.utc).replace(tzinfo=None)
db.execute(
    text("UPDATE report SET is_deleted=1, deleted_at=:now, updated_at=:now WHERE report_id=:rid"),
    {"now": now, "rid": "d68e555c-df1f-4dec-bd6f-5d4286097eef"},
)
db.commit()
print("soft-deleted residual")
try:
    r = generate_report_ssot(
        db,
        stock_code="601169.SH",
        trade_date="2026-04-03",
        idempotency_key="phase3-fix-" + uuid4().hex[:8],
        force_same_day_rebuild=True,
    )
    print("ok", r.get("report_id"), r.get("quality_flag"), r.get("recommendation"))
except Exception as e:
    print("fail", type(e).__name__, str(e)[:300])
db.close()
