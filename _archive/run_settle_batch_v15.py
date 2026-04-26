import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.services.settlement_ssot import submit_settlement_batch

os.environ["SETTLEMENT_INLINE_EXECUTION"] = "true"

engine = create_engine("sqlite:///data/app.db", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)

with SessionLocal() as db:
    before_sr = db.execute(text("select count(*) from settlement_result")).scalar_one()
    before_sp = db.execute(text("select count(*) from sim_position")).scalar_one()
    before_sa = db.execute(text("select count(*) from sim_account")).scalar_one()
    latest_trade = db.execute(
        text("select max(trade_date) from report where trade_date is not null")
    ).scalar_one()

    print("latest_trade_date=", latest_trade)
    accepted = submit_settlement_batch(
        db,
        trade_date=str(latest_trade),
        force=True,
        request_id="v15-batch-settle-20260414",
        requested_by_user_id=None,
        window_days_list=(7, 14, 30),
        target_scope="all",
        run_inline=None,
    )
    db.commit()

    after_sr = db.execute(text("select count(*) from settlement_result")).scalar_one()
    after_sp = db.execute(text("select count(*) from sim_position")).scalar_one()
    after_sa = db.execute(text("select count(*) from sim_account")).scalar_one()

print("accepted_tasks=", len(accepted))
print("settlement_result:", before_sr, "->", after_sr)
print("sim_position:", before_sp, "->", after_sp)
print("sim_account:", before_sa, "->", after_sa)
