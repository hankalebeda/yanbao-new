import os
import sys
import sqlite3
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_reports_batch

os.environ.setdefault('MOCK_LLM', 'false')
os.environ.setdefault('ENABLE_SCHEDULER', 'false')

conn = sqlite3.connect(str(ROOT / 'data' / 'app.db'))
c = conn.cursor()
c.execute(
    """
    SELECT u.stock_code, MAX(u.trade_date) latest_trade_date
    FROM report_data_usage u
    WHERE NOT EXISTS (
      SELECT 1 FROM report r
      WHERE r.stock_code = u.stock_code
        AND r.published = 1
        AND r.is_deleted = 0
    )
    GROUP BY u.stock_code
    ORDER BY latest_trade_date DESC, u.stock_code
    LIMIT 10
    """
)
rows = c.fetchall()
conn.close()

print('candidate_rows=', rows)
if not rows:
    print('no candidates')
    raise SystemExit(0)

trade_date = rows[0][1]
codes = [r[0] for r in rows]

res = generate_reports_batch(
    SessionLocal,
    stock_codes=codes,
    trade_date=trade_date,
    skip_pool_check=True,
    force_same_day_rebuild=False,
)
print('result_total=', res.get('total'))
print('result_succeeded=', res.get('succeeded'))
print('result_failed=', res.get('failed'))
print('elapsed_s=', res.get('elapsed_s'))
print('details=', res.get('details'))
