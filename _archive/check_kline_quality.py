import sqlite3
conn = sqlite3.connect('data/app.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

stocks = ['000858.SZ', '002594.SZ', '600519.SH']
for sc in stocks:
    cur.execute(
        'SELECT trade_date, open, high, low, close, volume, source_batch_id'
        ' FROM kline_daily WHERE stock_code=? ORDER BY trade_date DESC LIMIT 5',
        (sc,)
    )
    rows = cur.fetchall()
    print(f'\n{sc}:')
    for r in rows:
        print(f'  {dict(r)}')

print()
for sc in stocks:
    cur.execute('SELECT stock_code, stock_name FROM stock_master WHERE stock_code=?', (sc,))
    r = cur.fetchone()
    print(dict(r) if r else f'{sc}: NOT FOUND')

# Check what the LLM config looks like
import sys
sys.path.insert(0, 'd:/yanbao-new')
from app.core.config import settings
print(f'\nLLM Settings:')
print(f'  mock_llm={settings.mock_llm}')
print(f'  llm_audit_enabled={settings.llm_audit_enabled}')
# Check any AI API config
for attr in dir(settings):
    if 'llm' in attr.lower() or 'api' in attr.lower() or 'codex' in attr.lower():
        try:
            val = getattr(settings, attr)
            if isinstance(val, str) and len(str(val)) < 100:
                print(f'  {attr}={val}')
        except:
            pass

conn.close()
