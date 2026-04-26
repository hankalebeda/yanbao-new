import sys, os
sys.path.insert(0, r'd:\yanbao-new')
os.chdir(r'd:\yanbao-new')
os.environ['MOCK_LLM'] = 'true'
os.environ['ENABLE_SCHEDULER'] = 'false'
from datetime import date
from app.core.db import SessionLocal
from app.services.settlement_ssot import rebuild_fr07_snapshot

db = SessionLocal()
try:
    # Rebuild strategy_metric_snapshot for 2026-04-21 w=1 (matches runtime_trade_date)
    result = rebuild_fr07_snapshot(db, trade_day=date(2026,4,21), window_days=1, purge_invalid=False)
    db.commit()
    print(f"rebuild 2026-04-21 w=1: {result}")
except Exception as e:
    db.rollback()
    print(f"ERROR: {e}")
    import traceback; traceback.print_exc()
finally:
    db.close()
