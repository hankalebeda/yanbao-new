import sqlite3
c = sqlite3.connect('data/app.db').cursor()

# 600519 report details
row = c.execute(
    "SELECT report_id, stock_code, trade_date, recommendation, quality_flag, "
    "published, conclusion_text, status_reason FROM report "
    "WHERE stock_code='600519.SH' AND trade_date='2026-04-21' AND is_deleted=0"
).fetchone()
print('600519.SH 2026-04-21:', row)

# All tasks 2026-04-21
rows = c.execute(
    "SELECT task_id, stock_code, status FROM report_generation_task "
    "WHERE trade_date='2026-04-21' ORDER BY created_at"
).fetchall()
print()
print('Tasks 2026-04-21:')
for r in rows:
    print(' ', r)

# 688199.SH report details
row2 = c.execute(
    "SELECT report_id, stock_code, quality_flag, published, conclusion_text "
    "FROM report WHERE stock_code='688199.SH' AND is_deleted=0"
).fetchone()
print()
print('688199.SH alive report:', row2)
