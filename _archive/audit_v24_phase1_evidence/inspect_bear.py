import sqlite3
c = sqlite3.connect('data/app.db')
c.row_factory = sqlite3.Row
sql = """SELECT report_id, stock_code, recommendation, confidence, status_reason, quality_flag,
         LENGTH(conclusion_text) AS ct_len, LENGTH(reasoning_chain_md) AS rc_len,
         conclusion_text, reasoning_chain_md
  FROM report
  WHERE is_deleted=0 AND trade_date='2026-04-08'
  ORDER BY status_reason, stock_code LIMIT 5"""
for row in c.execute(sql).fetchall():
    print('---', row['stock_code'], 'qf=', row['quality_flag'], 'sr=', row['status_reason'], 'rec=', row['recommendation'], '---')
    print(f'  ct_len={row["ct_len"]} rc_len={row["rc_len"]} conf={row["confidence"]}')
    print(f'  CT: {(row["conclusion_text"] or "")[:200]!r}')
    print(f'  RC: {(row["reasoning_chain_md"] or "")[:200]!r}')
print()
print('--- status_reason dist on 04-08 ---')
for r in c.execute("SELECT status_reason, quality_flag, COUNT(*) AS n FROM report WHERE is_deleted=0 AND trade_date='2026-04-08' GROUP BY status_reason, quality_flag").fetchall():
    print(' ', dict(r))
print()
print('--- 04-09 the one bad report ---')
for r in c.execute("SELECT stock_code, status_reason, quality_flag, llm_fallback_level, LENGTH(reasoning_chain_md) AS rc_len, LENGTH(conclusion_text) AS ct_len FROM report WHERE is_deleted=0 AND trade_date='2026-04-09' AND (LENGTH(reasoning_chain_md)<50 OR reasoning_chain_md IS NULL)").fetchall():
    print(' ', dict(r))
