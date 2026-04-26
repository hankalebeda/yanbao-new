"""Cleanup + regen the lone LLM_FALLBACK degraded report (04-07 601390.SH)."""
import json
import sqlite3
from datetime import datetime
from pathlib import Path
import time
import httpx

DB = Path('data/app.db')
BK = Path('_archive/backup_20260418')
ts = datetime.now().strftime('%Y%m%d_%H%M%S')

c = sqlite3.connect(str(DB))
c.row_factory = sqlite3.Row

target = c.execute("SELECT * FROM report WHERE is_deleted=0 AND trade_date='2026-04-07' AND stock_code='601390.SH' AND quality_flag='degraded'").fetchone()
if not target:
    print('no target')
    raise SystemExit(0)
rid = target['report_id']
print('target report_id=', rid)

# backup
bk_file = BK / f'extra_lone_degraded_{ts}.json'
bk_file.write_text(json.dumps({k: target[k] for k in target.keys()}, ensure_ascii=False, default=str, indent=2), encoding='utf-8')
print('backup→', bk_file)

# delete report (keep usage rows so regen can read them)
c.execute('BEGIN')
try:
    c.execute("DELETE FROM report WHERE report_id=?", (rid,))
    c.execute('COMMIT')
    print('deleted report row')
except Exception:
    c.execute('ROLLBACK')
    raise

# regen
URL = 'http://127.0.0.1:8000/api/v1/internal/reports/generate-batch'
TOKEN = 'kestra-internal-20260327'
payload = {
    'trade_date': '2026-04-07',
    'stock_codes': ['601390.SH'],
    'force': True,
    'skip_pool_check': True,
    'cleanup_incomplete_before_batch': False,
}
print('POST', URL, payload)
r = httpx.post(URL, json=payload, headers={'X-Internal-Token': TOKEN}, timeout=180, trust_env=False)
print('status', r.status_code)
print(r.text[:400])

time.sleep(3)
print('-- post check --')
c2 = sqlite3.connect(str(DB))
c2.row_factory = sqlite3.Row
for row in c2.execute("SELECT report_id, recommendation, confidence, quality_flag, status_reason, LENGTH(reasoning_chain_md) AS rc, LENGTH(conclusion_text) AS ct FROM report WHERE trade_date='2026-04-07' AND stock_code='601390.SH' AND is_deleted=0"):
    print(' ', dict(row))
