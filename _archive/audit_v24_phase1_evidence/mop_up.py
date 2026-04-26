"""Mop up CONCURRENT_CONFLICT residuals: identify dates with <197 reports, find
missing stocks, force-rebuild via service call with force_same_day_rebuild=True.
"""
import json
import sqlite3
import sys
sys.path.insert(0, 'd:/yanbao-new')

from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_reports_batch

with open('_archive/audit_v24_phase1_evidence/ready_stocks.json', encoding='utf-8') as f:
    ready = json.load(f)

c = sqlite3.connect('data/app.db')
cur = c.cursor()

# Reset any Processing tasks first
n = cur.execute(
    "UPDATE report_generation_task SET status='Expired', updated_at=CURRENT_TIMESTAMP "
    "WHERE status='Processing' AND trade_date BETWEEN '2026-04-08' AND '2026-04-16'"
).rowcount
c.commit()
print(f'reset {n} stuck Processing tasks')

for td in ['2026-04-09', '2026-04-10', '2026-04-14', '2026-04-15']:
    stocks = ready.get(td, [])
    cur.execute("SELECT stock_code FROM report WHERE trade_date=? AND is_deleted=0", (td,))
    existing = {r[0] for r in cur.fetchall()}
    missing = [s for s in stocks if s not in existing]
    if not missing:
        print(f'{td}: complete (existing={len(existing)})')
        continue
    print(f'{td}: re-attempting {len(missing)} missing: {missing}')
    res = generate_reports_batch(
        db_factory=SessionLocal,
        stock_codes=missing,
        trade_date=td,
        skip_pool_check=True,
        force_same_day_rebuild=True,
        max_concurrent_override=2,
    )
    print(f'  -> ok={res.get("succeeded")} fail={res.get("failed")}')
    for d in (res.get('details') or []):
        if d.get('status') != 'ok':
            print(f'    err {d.get("stock_code")}: {d.get("error_code")} {d.get("error_message","")[:120]}')

c.close()
