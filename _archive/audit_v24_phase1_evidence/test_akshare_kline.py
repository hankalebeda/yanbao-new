"""Test AkShare for historical kline data"""
import sys, os
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'
sys.path.insert(0, 'd:/yanbao-new')

try:
    import akshare as ak
    
    # Fetch daily kline for 600000 from 2026-04-01 to 2026-04-17
    print('Testing AkShare stock_zh_a_hist for 600000.SH...')
    df = ak.stock_zh_a_hist(symbol='600000', period='daily', start_date='20260401', end_date='20260417', adjust='qfq')
    print(f'Got {len(df)} rows')
    if len(df) > 0:
        print('Columns:', list(df.columns))
        print('Last 5 rows:')
        print(df.tail(5)[['日期','开盘','收盘','最高','最低']].to_string())
except Exception as e:
    import traceback
    traceback.print_exc()

print('\n---')
try:
    import akshare as ak
    print('Testing AkShare for 000001.SZ...')
    df2 = ak.stock_zh_a_hist(symbol='000001', period='daily', start_date='20260401', end_date='20260417', adjust='qfq')
    print(f'Got {len(df2)} rows')
    if len(df2) > 0:
        print('Last 3 rows:')
        print(df2.tail(3)[['日期','开盘','收盘','最高','最低']].to_string())
except Exception as e:
    print(f'Error: {e}')
