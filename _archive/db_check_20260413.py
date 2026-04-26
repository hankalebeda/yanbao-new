"""Quick DB diagnostic script - check schema alignment and empty tables."""
import sqlite3

conn = sqlite3.connect('data/app.db')

# 1. settlement_result columns
cursor = conn.execute('PRAGMA table_info(settlement_result)')
cols = cursor.fetchall()
print('=== settlement_result DB columns ===')
for c in cols:
    print(f'  {c[1]:30s} {c[2]:15s} nullable={c[3]==0} default={c[4]}')
print(f'\nTotal DB columns: {len(cols)}')

# 2. ORM column names (from models.py definition)
orm_cols = [
    'settlement_result_id', 'report_id', 'stock_code', 'signal_date',
    'window_days', 'strategy_type', 'settlement_status', 'quality_flag',
    'entry_trade_date', 'exit_trade_date', 'shares', 'buy_price', 'sell_price',
    'buy_commission', 'sell_commission', 'stamp_duty',
    'buy_slippage_cost', 'sell_slippage_cost',
    'gross_return_pct', 'net_return_pct', 'display_hint',
    'settlement_id', 'trade_date', 'is_misclassified', 'exit_reason',
    'settled_at', 'created_at', 'updated_at',
]
db_col_names = {c[1] for c in cols}
orm_set = set(orm_cols)
print(f'\n=== Schema Diff ===')
print(f'In ORM but not in DB: {orm_set - db_col_names}')
print(f'In DB but not in ORM: {db_col_names - orm_set}')

# 3. Check all tables
cursor2 = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cursor2.fetchall()]
empty = []
nonempty = []
for t in tables:
    cnt = conn.execute(f'SELECT COUNT(*) FROM [{t}]').fetchone()[0]
    if cnt == 0:
        empty.append(t)
    else:
        nonempty.append((t, cnt))

print(f'\n=== Empty tables ({len(empty)}/{len(tables)}) ===')
for t in empty:
    print(f'  {t}')

print(f'\n=== Non-empty tables ===')
for t, cnt in sorted(nonempty, key=lambda x: -x[1]):
    print(f'  {t:40s} {cnt:>8,}')

conn.close()
