"""
任务1: 硬删除所有旧研报及关联数据
- 不允许软删除（is_deleted=1）
- 删除所有 report 及其关联的 report_citation, report_data_usage_link,
  report_feedback, report_idempotency, report_generation_task
- 同时清理 settlement_result, prediction_outcome 中依赖研报的关联
"""
import sqlite3

DB_PATH = 'data/app.db'

conn = sqlite3.connect(DB_PATH)
conn.execute('PRAGMA foreign_keys = OFF')  # Disable FK enforcement for bulk delete
cur = conn.cursor()

print('=== 删除前统计 ===')
for t in ['report', 'report_citation', 'report_data_usage_link', 'report_feedback',
          'report_idempotency', 'report_generation_task']:
    cur.execute(f'SELECT COUNT(*) FROM "{t}"')
    print(f'  {t}: {cur.fetchone()[0]}')

# Also check settlement_result and prediction_outcome references to report
cur.execute('PRAGMA table_info(settlement_result)')
sr_cols = [r[1] for r in cur.fetchall()]
print(f'\n  settlement_result columns: {sr_cols}')

cur.execute('PRAGMA table_info(prediction_outcome)')
po_cols = [r[1] for r in cur.fetchall()]
print(f'  prediction_outcome columns: {po_cols}')

print('\n=== 执行删除 ===')

# 1. Delete report_citation (child of report)
cur.execute('DELETE FROM report_citation')
print(f'  report_citation: deleted {cur.rowcount}')

# 2. Delete report_data_usage_link (child of report)
cur.execute('DELETE FROM report_data_usage_link')
print(f'  report_data_usage_link: deleted {cur.rowcount}')

# 3. Delete report_feedback (child of report)
cur.execute('DELETE FROM report_feedback')
print(f'  report_feedback: deleted {cur.rowcount}')

# 4. Delete report_idempotency (child of report)
cur.execute('DELETE FROM report_idempotency')
print(f'  report_idempotency: deleted {cur.rowcount}')

# 5. Delete settlement_result if it has report FK
if 'report_id' in sr_cols:
    cur.execute('DELETE FROM settlement_result')
    print(f'  settlement_result (report FK): deleted {cur.rowcount}')

# 6. Delete prediction_outcome if it has report FK  
if 'report_id' in po_cols:
    cur.execute('DELETE FROM prediction_outcome WHERE report_id IS NOT NULL')
    print(f'  prediction_outcome (report FK): deleted {cur.rowcount}')

# 7. Delete all report records (including soft-deleted)
cur.execute('DELETE FROM report')
print(f'  report: deleted {cur.rowcount}')

# 8. Delete report_generation_task
cur.execute('DELETE FROM report_generation_task')
print(f'  report_generation_task: deleted {cur.rowcount}')

conn.commit()

print('\n=== 删除后验证 ===')
for t in ['report', 'report_citation', 'report_data_usage_link', 'report_feedback',
          'report_idempotency', 'report_generation_task', 'settlement_result', 'prediction_outcome']:
    cur.execute(f'SELECT COUNT(*) FROM "{t}"')
    print(f'  {t}: {cur.fetchone()[0]}')

conn.execute('PRAGMA foreign_keys = ON')
conn.close()
print('\n✅ 硬删除完成')
