import sys, os
sys.path.insert(0, r'd:\yanbao-new')
os.chdir(r'd:\yanbao-new')
os.environ['MOCK_LLM'] = 'true'
os.environ['ENABLE_SCHEDULER'] = 'false'
os.environ['SETTLEMENT_INLINE_EXECUTION'] = 'true'

from app.core.db import SessionLocal
from app.services.settlement_ssot import submit_settlement_task

db = SessionLocal()
try:
    print("Running settlement trade_date=2026-04-22 window_days=1...")
    result = submit_settlement_task(
        db,
        trade_date='2026-04-22',
        window_days=1,
        target_scope='all',
        force=True,
        requested_by_user_id='system',
        run_inline=True,
    )
    db.commit()
    print(f"Result: {result}")
except Exception as e:
    db.rollback()
    print(f"ERROR: {e}")
    import traceback; traceback.print_exc()
finally:
    db.close()
print("Done!")
