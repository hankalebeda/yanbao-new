import sqlite3
c = sqlite3.connect('data/app.db')
print('--- failure_category on 04-08/09 (just soft-deleted) ---')
for r in c.execute(
    "SELECT trade_date, failure_category, substr(status_reason,1,100), COUNT(*) "
    "FROM report WHERE trade_date BETWEEN '2026-04-08' AND '2026-04-09' AND is_deleted=1 "
    "GROUP BY trade_date, failure_category, status_reason ORDER BY COUNT(*) DESC LIMIT 20"
):
    print(r)

print('--- 04-10 sample existing ok ---')
for r in c.execute(
    "SELECT report_id, stock_code, llm_fallback_level, quality_flag, created_at "
    "FROM report WHERE trade_date='2026-04-10' AND is_deleted=0 ORDER BY created_at LIMIT 3"
):
    print(r)

print('--- generation_task status ---')
for r in c.execute(
    "SELECT trade_date, status, COUNT(*) FROM report_generation_task "
    "WHERE trade_date BETWEEN '2026-04-08' AND '2026-04-16' "
    "GROUP BY trade_date, status ORDER BY trade_date, status"
):
    print(r)
