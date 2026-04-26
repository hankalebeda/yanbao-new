"""Check mootdx configuration and server connection"""
import sys
sys.path.insert(0, 'd:/yanbao-new')

try:
    from mootdx.quotes import Quotes
    
    # Try different markets
    print('Trying local TDX server...')
    try:
        client = Quotes.factory(market='std', timeout=3)
        print('Factory created with std')
        
        # Check connection
        print('Testing ping...')
        
        # Get server list
        print('Getting server info...')
    except Exception as e:
        print(f'Error: {e}')
    
    # Check if there's a TDX data directory
    import os
    tdx_paths = [
        r'C:\TDX',
        r'C:\Program Files\TDX',
        r'C:\通达信',
        r'D:\TDX',
        r'C:\new_tdx',
    ]
    for p in tdx_paths:
        if os.path.exists(p):
            print(f'TDX found at: {p}')
            # List contents
            for item in os.listdir(p)[:10]:
                print(f'  {item}')
            break
    else:
        print('TDX directory not found in common locations')
    
    # Check mootdx config
    from mootdx.utils import get_config_path
    cfg_path = get_config_path('server.cfg')
    print(f'mootdx config path: {cfg_path}')
    if os.path.exists(cfg_path):
        with open(cfg_path) as f:
            print(f'Config: {f.read()[:200]}')
            
except Exception as e:
    import traceback
    traceback.print_exc()
