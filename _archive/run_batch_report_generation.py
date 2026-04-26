import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_reports_batch

os.environ.setdefault('MOCK_LLM', 'false')
os.environ.setdefault('ENABLE_SCHEDULER', 'false')

DB_PATH = ROOT / 'data' / 'app.db'


def pick_candidates(limit: int = 20):
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute(
        """
        SELECT DISTINCT k.stock_code
        FROM kline_daily k
        WHERE NOT EXISTS (
          SELECT 1 FROM report r
          WHERE r.stock_code = k.stock_code AND r.published = 1 AND r.is_deleted = 0
        )
        ORDER BY k.stock_code
        LIMIT ?
        """,
        (limit,),
    )
    rows = [r[0] for r in c.fetchall()]
    conn.close()
    return rows


def main():
    codes = pick_candidates(limit=20)
    print(f'candidate_stock_codes={len(codes)}')
    if not codes:
        print('no candidates')
        return
    print('sample=', ','.join(codes[:10]))

    # Candidate stocks mostly have latest K-line at 2026-04-03 in current local dataset.
    # Fix trade_day to an actually available date to avoid DEPENDENCY_NOT_READY.
    result = generate_reports_batch(
        SessionLocal,
        stock_codes=codes,
        trade_date='2026-04-03',
        skip_pool_check=True,
        force_same_day_rebuild=False,
    )
    print('batch_result_total=', result.get('total'))
    print('batch_result_succeeded=', result.get('succeeded'))
    print('batch_result_failed=', result.get('failed'))
    print('batch_elapsed_s=', result.get('elapsed_s'))

    failed = [x for x in result.get('details', []) if x.get('status') != 'ok']
    print('failed_examples=', failed[:5])


if __name__ == '__main__':
    main()
