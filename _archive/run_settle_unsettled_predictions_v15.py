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
    before = db.execute(text("select count(*) from settlement_result")).scalar_one()
    rows = db.execute(
        text(
            """
            SELECT po.report_id, po.window_days, po.stock_code
            FROM prediction_outcome po
            WHERE NOT EXISTS (
                SELECT 1 FROM settlement_result sr
                WHERE sr.report_id = po.report_id
            )
            ORDER BY po.report_id
            """
        )
    ).fetchall()

    submitted = 0
    failed = 0
    for report_id, window_days, stock_code in rows:
        try:
            submit_settlement_task(
                db,
                trade_date=str(db.execute(text("SELECT trade_date FROM report WHERE report_id=:rid"), {"rid": report_id}).scalar_one()),
                window_days=int(window_days or 7),
                target_scope="report",
                target_report_id=str(report_id),
                target_stock_code=str(stock_code) if stock_code else None,
                force=True,
                request_id=f"v15-report-settle-{report_id}",
                requested_by_user_id=None,
                run_inline=None,
            )
            db.commit()
            submitted += 1
        except Exception:
            db.rollback()
            failed += 1

    after = db.execute(text("select count(*) from settlement_result")).scalar_one()

print("unsettled_reports=", len(rows))
print("submitted=", submitted)
print("failed=", failed)
print("settlement_result:", before, "->", after)
