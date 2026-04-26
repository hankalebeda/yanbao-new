"""Trigger pool refresh + verify, then batch generate reports via service layer."""
import traceback
from app import models  # ensure ORM metadata populated
from app.core.db import SessionLocal
from app.services.stock_pool import refresh_stock_pool, PoolColdStartError, PoolRefreshConflict

TRADE_DATES = ["2026-04-16", "2026-04-15", "2026-04-14"]

db = SessionLocal()
try:
    for td in TRADE_DATES:
        try:
            r = refresh_stock_pool(db, trade_date=td, force_rebuild=True, request_id=f"phase2-{td}")
            db.commit()
            print(f"[{td}] status={r['status']} core={r['core_pool_size']} standby={r['standby_pool_size']} reason={r.get('status_reason')}")
        except PoolColdStartError as e:
            db.rollback()
            print(f"[{td}] ColdStart: {e}")
        except PoolRefreshConflict as e:
            db.rollback()
            print(f"[{td}] Conflict: {e}")
        except Exception as e:
            db.rollback()
            print(f"[{td}] ERR: {e}")
            traceback.print_exc()
finally:
    db.close()
