"""List the 2 LLM_FALLBACK reports for retry."""
import sqlite3
c = sqlite3.connect("data/app.db")
rows = c.execute(
    """
    SELECT report_id, stock_code, trade_date, idempotency_key, status_reason,
           quality_flag, published, llm_fallback_level, created_at
    FROM report
    WHERE is_deleted=0 AND (status_reason='LLM_FALLBACK' OR quality_flag<>'ok')
    """
).fetchall()
for r in rows:
    print(r)
