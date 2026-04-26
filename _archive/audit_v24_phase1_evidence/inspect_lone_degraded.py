import sqlite3
c = sqlite3.connect('data/app.db')
c.row_factory = sqlite3.Row
print('-- the lone degraded alive report --')
for r in c.execute("SELECT trade_date, stock_code, recommendation, confidence, quality_flag, status_reason, llm_fallback_level FROM report WHERE is_deleted=0 AND quality_flag='degraded'"):
    print(' ', dict(r))
print()
print('-- 04-08 alive distinct ct/rc lengths --')
for r in c.execute("SELECT recommendation, confidence, status_reason, LENGTH(conclusion_text) AS ct, LENGTH(reasoning_chain_md) AS rc, COUNT(*) AS n FROM report WHERE is_deleted=0 AND trade_date='2026-04-08' GROUP BY recommendation, confidence, status_reason, ct, rc"):
    print(' ', dict(r))
