"""Phase D-mini: regenerate 1 deleted report (04-09 601898.SH)."""
import json
import time
import httpx

URL = 'http://127.0.0.1:8000/api/v1/internal/reports/generate-batch'
TOKEN = 'kestra-internal-20260327'

payload = {
    'trade_date': '2026-04-09',
    'stock_codes': ['601898.SH'],
    'force': True,
    'skip_pool_check': True,
    'cleanup_incomplete_before_batch': False,
}
print('POST', URL, payload)
r = httpx.post(URL, json=payload, headers={'X-Internal-Token': TOKEN}, timeout=180, trust_env=False)
print('status', r.status_code)
print(r.text[:600])

if r.status_code == 202:
    body = r.json()
    print('batch_id?', body)
    # poll using internal task status if we have a task id; else just sleep+inspect db
    time.sleep(60)
    import sqlite3
    c = sqlite3.connect('data/app.db')
    c.row_factory = sqlite3.Row
    for row in c.execute("SELECT report_id, recommendation, confidence, quality_flag, LENGTH(reasoning_chain_md) AS rc, LENGTH(conclusion_text) AS ct, status_reason FROM report WHERE trade_date='2026-04-09' AND stock_code='601898.SH' AND is_deleted=0 ORDER BY created_at DESC LIMIT 3"):
        print(' ', dict(row))
