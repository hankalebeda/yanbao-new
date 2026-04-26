"""Soft-delete all reports with llm_fallback_level='failed' OR quality_flag='degraded'
within 2026-04-08..2026-04-16, so backfill re-creates them with fresh LLM calls.
04-07 untouched (already OK baseline).
"""
import sqlite3
import time

c = sqlite3.connect('data/app.db')
c.execute('PRAGMA busy_timeout=60000')
cur = c.cursor()

cur.execute(
    "SELECT report_id, trade_date, stock_code, quality_flag, llm_fallback_level FROM report "
    "WHERE trade_date BETWEEN '2026-04-08' AND '2026-04-16' AND is_deleted=0 "
    "AND (llm_fallback_level='failed' OR quality_flag='degraded')"
)
rows = cur.fetchall()
print(f'targets to soft-delete: {len(rows)}')
flag_counts = {}
for _, td, _, qf, llf in rows:
    k = (td, qf, llf)
    flag_counts[k] = flag_counts.get(k, 0) + 1
for k, v in sorted(flag_counts.items()):
    print(' ', k, '->', v)

from datetime import datetime, timezone
now_iso = datetime.now(timezone.utc).isoformat()
for rid, *_ in rows:
    cur.execute(
        "UPDATE report SET is_deleted=1, deleted_at=? WHERE report_id=?",
        (now_iso, rid),
    )
c.commit()

print('after cleanup:')
for r in cur.execute(
    "SELECT trade_date, COUNT(*) FROM report WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' AND is_deleted=0 GROUP BY trade_date ORDER BY trade_date"
):
    print(' ', r)

c.close()
