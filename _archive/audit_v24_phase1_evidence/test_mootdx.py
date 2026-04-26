"""Test mootdx historical kline fetch"""
import sys, os
sys.path.insert(0, 'd:/yanbao-new')

try:
    from mootdx.quotes import Quotes
    client = Quotes.factory(market='std')
    
    # Fetch kline for 600000 (SH) - note: mootdx uses different code format
    # SH stocks: just the 6-digit code; SZ stocks: just the 6-digit code
    # Market code: 0=SZ, 1=SH
    
    # Test SH stock (600000.SH = market 1, code 600000)
    result = client.bars(symbol='600000', frequency=9, market=1, offset=0, count=20)
    print('mootdx 600000.SH kline:')
    if result is not None and len(result) > 0:
        print(f'  Got {len(result)} rows')
        print('  Last row:', result.iloc[-1].to_dict() if hasattr(result, 'iloc') else result[-1])
    else:
        print('  No data returned')
    
    # Test SZ stock (000001.SZ = market 0)
    result2 = client.bars(symbol='000001', frequency=9, market=0, offset=0, count=20)
    print('\nmootdx 000001.SZ kline:')
    if result2 is not None and len(result2) > 0:
        print(f'  Got {len(result2)} rows')
        last = result2.iloc[-1].to_dict() if hasattr(result2, 'iloc') else result2[-1]
        print('  Last row:', last)
    else:
        print('  No data returned')
        
except Exception as e:
    import traceback
    traceback.print_exc()
