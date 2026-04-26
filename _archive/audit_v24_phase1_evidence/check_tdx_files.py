"""Check TDX local data files and try to read kline"""
import os, sys
sys.path.insert(0, 'd:/yanbao-new')

tdx_path = r'C:\new_tdx'

# Check vipdoc directory
vipdoc = os.path.join(tdx_path, 'vipdoc')
if os.path.exists(vipdoc):
    print('vipdoc exists')
    for item in os.listdir(vipdoc)[:10]:
        print(f'  {item}')
else:
    print('vipdoc NOT found')
    # Check what's in new_tdx
    print('Contents of', tdx_path)
    for item in os.listdir(tdx_path):
        print(f'  {item}')

# Try mootdx reader
try:
    from mootdx.reader import Reader
    reader = Reader.factory(market='std', tdxdir=tdx_path)
    print('\nmootdx Reader created')
    
    # Try reading daily kline for 000001
    klines = reader.daily(symbol='000001')
    print(f'000001.SZ klines: {len(klines) if klines is not None else None} rows')
    if klines is not None and len(klines) > 0:
        print('Last 3 rows:')
        if hasattr(klines, 'tail'):
            print(klines.tail(3).to_string())
        else:
            print(klines[-3:])
except Exception as e:
    import traceback
    traceback.print_exc()
