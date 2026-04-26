import os
import sys
import sqlite3
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

os.environ.setdefault('MOCK_LLM', 'true')
os.environ.setdefault('ENABLE_SCHEDULER', 'false')
os.environ.setdefault('SETTLEMENT_INLINE_EXECUTION', 'true')

from app.core.db import SessionLocal
from app.services.settlement_ssot import submit_settlement_task

DATES = [
    '2026-03-27', '2026-03-30', '2026-03-31', '2026-04-01',
    '2026-04-02', '2026-04-03', '2026-04-06', '2026-04-07',
    '2026-04-10', '2026-04-14'
]


def count_settlement():
    conn = sqlite3.connect(str(ROOT / 'data' / 'app.db'))
    c = conn.cursor()
    c.execute('SELECT COUNT(1) FROM settlement_result')
    total = c.fetchone()[0]
    c.execute('SELECT COUNT(DISTINCT stock_code) FROM settlement_result')
    stocks = c.fetchone()[0]
    conn.close()
    return total, stocks


best = None
for d in DATES:
    db = SessionLocal()
    try:
        submit_settlement_task(
            db,
            trade_date=d,
            window_days=7,
            target_scope='all',
            force=True,
            run_inline=True,
        )
        db.commit()
    finally:
        db.close()
    total, stocks = count_settlement()
    print(f'{d}: total={total}, stocks={stocks}')
    if best is None or total > best[1]:
        best = (d, total, stocks)

print('best=', best)
