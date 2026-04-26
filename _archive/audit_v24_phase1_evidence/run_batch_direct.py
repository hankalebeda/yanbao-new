import json
import sqlite3
import sys

sys.path.insert(0, 'd:/yanbao-new')

from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_reports_batch

DATES = ['2026-04-08','2026-04-09','2026-04-10','2026-04-13','2026-04-14','2026-04-15','2026-04-16']

with open('_archive/audit_v24_phase1_evidence/ready_stocks.json', encoding='utf-8') as f:
    ready = json.load(f)

c = sqlite3.connect('data/app.db')
cur = c.cursor()

print('=== Dry small run (10 each date) ===')
for td in DATES:
    stocks = ready.get(td, [])
    if not stocks:
        continue
    cur.execute("SELECT stock_code FROM report WHERE trade_date=? AND is_deleted=0", (td,))
    existing = {r[0] for r in cur.fetchall()}
    pending = [s for s in stocks if s not in existing]
    test_codes = pending[:10]
    if not test_codes:
        print(td, 'no pending')
        continue
    res = generate_reports_batch(
        db_factory=SessionLocal,
        stock_codes=test_codes,
        trade_date=td,
        skip_pool_check=True,
        force_same_day_rebuild=True,
        max_concurrent_override=2,
    )
    print(td, 'total', res.get('total'), 'ok', res.get('succeeded'), 'fail', res.get('failed'))
    # print top 3 errors for visibility
    errs = [d for d in res.get('details', []) if d.get('status') != 'ok']
    if errs:
        print('  sample errors:', errs[:3])

c.close()
