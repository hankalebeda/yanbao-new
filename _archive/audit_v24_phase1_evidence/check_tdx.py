"""Check TDX local data availability"""
import sys, os
sys.path.insert(0, 'd:/yanbao-new')

# Check if TDX data collection service exists
from app.services.market_data import _fetch_tdx_quote
print('TDX function found:', _fetch_tdx_quote)

# Try fetching a TDX quote
result = _fetch_tdx_quote('601888.SH')
print('TDX quote for 601888.SH:', result)

# Check mootdx
try:
    from mootdx.quotes import Quotes
    client = Quotes.factory(market='std')
    print('mootdx available')
except Exception as e:
    print('mootdx error:', e)
    
# Check the tdx kline path
import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()

# Check tdx_local kline usage records
cur.execute("""SELECT trade_date, count(*) FROM report_data_usage 
WHERE source_name='tdx_local' AND dataset_name='kline_daily'
GROUP BY trade_date ORDER BY trade_date DESC LIMIT 10""")
print('\nTDX local kline usage per date:')
for r in cur.fetchall():
    print(f'  {r[0]}: {r[1]} stocks')
