import os
import sys
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.services.settlement_ssot import submit_settlement_task

os.environ["SETTLEMENT_INLINE_EXECUTION"] = "true"

engine = create_engine("sqlite:///data/app.db", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)

with SessionLocal() as db:
    before_sr = db.execute(text("select count(*) from settlement_result")).scalar_one()
    dates = [
        r[0]
        for r in db.execute(
            text(
                """
                select distinct trade_date
                from report
                where published = 1 and trade_date is not null
                order by trade_date asc
                limit 80
                """
            )
        ).fetchall()
    ]

    submitted = 0
    conflicts = 0
    errors = 0
    for trade_date in dates:
        for window_days in (7, 14, 30):
            try:
                submit_settlement_task(
                    db,
                    trade_date=str(trade_date),
                    window_days=window_days,
                    target_scope="all",
                    force=False,
                    request_id=f"v15-expand-old-{trade_date}-{window_days}",
                    requested_by_user_id=None,
                    run_inline=None,
                )
                db.commit()
                submitted += 1
            except Exception as exc:
                db.rollback()
                msg = str(exc)
                if "CONCURRENT_CONFLICT" in msg:
                    conflicts += 1
                else:
                    errors += 1

    after_sr = db.execute(text("select count(*) from settlement_result")).scalar_one()

print("old_dates_considered=", len(dates))
print("tasks_submitted=", submitted)
print("conflicts=", conflicts)
print("errors=", errors)
print("settlement_result:", before_sr, "->", after_sr)
