import sqlite3
conn = sqlite3.connect('data/app.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print('=== stocks with new capital data on 2026-04-16 ===')
new_ds = ('main_force_flow', 'dragon_tiger_list', 'margin_financing', 'stock_profile')
phs = ','.join(['?' for _ in new_ds])
cur.execute(
    f'SELECT stock_code, dataset_name, status, substr(status_reason,1,120) reason'
    f' FROM report_data_usage WHERE dataset_name IN ({phs}) AND trade_date="2026-04-16"',
    new_ds
)
for r in cur.fetchall():
    print(dict(r))

print()
print('=== check which stock has ALL 4 new datasets ===')
cur.execute(
    f'SELECT stock_code, COUNT(DISTINCT dataset_name) cnt FROM report_data_usage'
    f' WHERE dataset_name IN ({phs}) AND trade_date="2026-04-16" AND status="ok"'
    f' GROUP BY stock_code HAVING cnt=4',
    new_ds
)
for r in cur.fetchall():
    print(dict(r))

print()
print('=== LLM config ===')
import sys
sys.path.insert(0, 'd:/yanbao-new')
from app.core.config import settings
print(f'mock_llm={settings.mock_llm}')
print(f'llm_audit_enabled={settings.llm_audit_enabled}')

conn.close()
