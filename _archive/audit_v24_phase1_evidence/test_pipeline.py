"""Test akshare and ingest pipeline for one stock/date"""
import os
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

import sys
sys.path.insert(0, 'd:/yanbao-new')

# Test ETF flow
print('=== Testing fetch_etf_flow_summary_global ===')
try:
    from app.services.etf_flow_data import fetch_etf_flow_summary_global
    result = fetch_etf_flow_summary_global('2026-04-14')
    print(f'ETF flow result: status={result.get("status")}, keys={list(result.keys())[:6]}')
except Exception as e:
    print(f'ETF flow error: {e}')

# Test northbound (per-stock)
print('\n=== Testing northbound_data.fetch_northbound_summary ===')
try:
    from app.services.northbound_data import fetch_northbound_summary
    result = fetch_northbound_summary('601888.SH')
    if result:
        print(f'Northbound result: status={result.get("status")}, keys={list(result.keys())[:6]}')
    else:
        print('Northbound: returned None')
except Exception as e:
    print(f'Northbound error: {e}')

# Check HOTSPOT_SOURCE_PRIORITY
print('\n=== Check HOTSPOT_SOURCE_PRIORITY ===')
try:
    from app.services.multisource_ingest import HOTSPOT_SOURCE_PRIORITY
    print(f'Hotspot sources: {HOTSPOT_SOURCE_PRIORITY}')
except Exception as e:
    print(f'Error: {e}')

# Check market_state_cache for recent dates
print('\n=== Check market_state_cache ===')
import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()
cur.execute("""SELECT trade_date, market_state, reference_date FROM market_state_cache 
ORDER BY trade_date DESC LIMIT 10""")
for r in cur.fetchall():
    print(r)
