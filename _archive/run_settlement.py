import sys
import os

# Use absolute path and ensure it's in sys.path
BASE_DIR = r"D:\yanbao-new"
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

os.environ['MOCK_LLM'] = 'true'
os.environ['ENABLE_SCHEDULER'] = 'false'
os.environ['SETTLEMENT_INLINE_EXECUTION'] = 'true'
os.chdir(BASE_DIR)

from app.core.db import SessionLocal
from app.services.settlement_ssot import submit_settlement_task

db = SessionLocal()

try:
    # Run settlement for 2026-04-21 (signal_date for 688001.SH and 688002.SH reports)
    print("Running settlement for 2026-04-21 (window_days=1)...")
    result1 = submit_settlement_task(
        db,
        trade_date='2026-04-21',
        window_days=1,
        target_scope='all',
        force=True,
        requested_by_user_id='system',
        run_inline=True,
    )
    print(f"Result 1: {result1}")
    
    # Run settlement for 2026-04-16 (signal_date for 000858.SZ, 002594.SZ, 600519.SH)
    print("Running settlement for 2026-04-16 (window_days=1)...")
    result2 = submit_settlement_task(
        db,
        trade_date='2026-04-16',
        window_days=1,
        target_scope='all',
        force=True,
        requested_by_user_id='system',
        run_inline=True,
    )
    print(f"Result 2: {result2}")
    
    db.commit()
    print("Done!")
    
except Exception as e:
    db.rollback()
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
finally:
    db.close()
