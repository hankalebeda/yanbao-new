"""Phase B: backup + physical delete of placeholder reports.

Targets:
  T1: status_reason='BEAR_MARKET_FILTERED' (placeholder text "bear market filtered")
  T2: trade_date='2026-04-09' AND stock_code='601898.SH' (RC="推理链待补充")

Action:
  1. Dump full report rows + report_data_usage rows for these report_ids to JSONL backup.
  2. DELETE FROM report_data_usage WHERE (stock_code,trade_date) of targeted reports.
     Keep report_generation_task untouched (will be reused by force_same_day_rebuild for T2).
  3. DELETE FROM report WHERE report_id IN (...).
"""
import json
import sqlite3
from datetime import datetime
from pathlib import Path

DB = Path('data/app.db')
BK_DIR = Path('_archive/backup_20260418')
BK_DIR.mkdir(parents=True, exist_ok=True)
ts = datetime.now().strftime('%Y%m%d_%H%M%S')

c = sqlite3.connect(str(DB))
c.row_factory = sqlite3.Row

# 1. select targets
sql_targets = (
    "SELECT report_id, stock_code, trade_date, status_reason, quality_flag, "
    "LENGTH(reasoning_chain_md) AS rc_len "
    "FROM report WHERE is_deleted=0 AND ("
    "  status_reason='BEAR_MARKET_FILTERED'"
    "  OR (trade_date='2026-04-09' AND stock_code='601898.SH')"
    ")"
)
targets = [dict(r) for r in c.execute(sql_targets).fetchall()]
print(f'Targets: {len(targets)}')

if not targets:
    print('No targets. exit.')
    raise SystemExit(0)

ids = [t['report_id'] for t in targets]
pairs = [(t['stock_code'], t['trade_date']) for t in targets]

# 2. backup full rows
bk_reports = BK_DIR / f'report_rows_{ts}.jsonl'
with bk_reports.open('w', encoding='utf-8') as f:
    placeholders = ','.join('?' * len(ids))
    for r in c.execute(f"SELECT * FROM report WHERE report_id IN ({placeholders})", ids):
        f.write(json.dumps(dict(r), ensure_ascii=False, default=str) + '\n')
print(f'Backed up reports → {bk_reports}')

bk_usage = BK_DIR / f'report_data_usage_rows_{ts}.jsonl'
n_usage = 0
with bk_usage.open('w', encoding='utf-8') as f:
    for code, date in pairs:
        for r in c.execute("SELECT * FROM report_data_usage WHERE stock_code=? AND trade_date=?", (code, date)):
            f.write(json.dumps(dict(r), ensure_ascii=False, default=str) + '\n')
            n_usage += 1
print(f'Backed up {n_usage} report_data_usage rows → {bk_usage}')

bk_index = BK_DIR / f'index_{ts}.json'
bk_index.write_text(json.dumps({
    'ts': ts,
    'count': len(targets),
    'usage_rows': n_usage,
    'targets': targets,
}, ensure_ascii=False, indent=2), encoding='utf-8')
print(f'Index → {bk_index}')

# 3. delete (transaction)
print('--- DELETE phase ---')
c.execute('BEGIN')
try:
    placeholders = ','.join('?' * len(ids))
    cur = c.execute(f"DELETE FROM report WHERE report_id IN ({placeholders})", ids)
    n_del_report = cur.rowcount
    n_del_usage = 0
    for code, date in pairs:
        cur = c.execute("DELETE FROM report_data_usage WHERE stock_code=? AND trade_date=?", (code, date))
        n_del_usage += cur.rowcount
    c.execute('COMMIT')
    print(f'Deleted: report={n_del_report}, report_data_usage={n_del_usage}')
except Exception:
    c.execute('ROLLBACK')
    raise

# 4. verify
print('--- post-delete verify ---')
remain = c.execute("SELECT COUNT(*) FROM report WHERE is_deleted=0 AND status_reason='BEAR_MARKET_FILTERED'").fetchone()[0]
print(f'  remaining BEAR-placeholder reports: {remain}')
remain2 = c.execute("SELECT COUNT(*) FROM report WHERE is_deleted=0 AND trade_date='2026-04-09' AND stock_code='601898.SH'").fetchone()[0]
print(f'  remaining 601898.SH on 04-09 (need 0): {remain2}')

# new totals
totals = c.execute("SELECT trade_date, COUNT(*) FROM report WHERE is_deleted=0 AND trade_date BETWEEN '2026-04-07' AND '2026-04-16' GROUP BY trade_date ORDER BY trade_date").fetchall()
for t in totals:
    print(f'  {t[0]}: {t[1]}')

c.close()
