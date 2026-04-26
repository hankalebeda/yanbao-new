import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

os.environ.setdefault('MOCK_LLM', 'true')
os.environ.setdefault('ENABLE_SCHEDULER', 'false')
os.environ.setdefault('SETTLEMENT_INLINE_EXECUTION', 'true')

from app.core.db import SessionLocal
from app.services.settlement_ssot import submit_settlement_task

TARGET_DATE = '2026-04-06'
WINDOW_DAYS = 7


def main():
    db = SessionLocal()
    try:
        result = submit_settlement_task(
            db,
            trade_date=TARGET_DATE,
            window_days=WINDOW_DAYS,
            target_scope='all',
            force=True,
            run_inline=True,
        )
        db.commit()
        print('status=', result.get('status'))
        print('task_id=', result.get('task_id'))
    finally:
        db.close()


if __name__ == '__main__':
    main()
