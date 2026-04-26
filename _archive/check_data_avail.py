"""Check data availability by date"""
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()

print('=== Market state:')
r = db.execute(text('SELECT trade_date, market_state, market_state_degraded FROM market_state_daily WHERE trade_date >= "2026-04-01" ORDER BY trade_date DESC LIMIT 15')).fetchall()
for row in r: print(' ', row)

print('\n=== Hotspot data by date:')
r2 = db.execute(text('SELECT trade_date, COUNT(*) FROM hotspot_raw GROUP BY trade_date ORDER BY trade_date DESC LIMIT 15')).fetchall()
for row in r2: print(' ', row)

print('\n=== Northbound by date:')
r3 = db.execute(text('SELECT trade_date, COUNT(*) FROM northbound_flow_daily GROUP BY trade_date ORDER BY trade_date DESC LIMIT 15')).fetchall()
for row in r3: print(' ', row)

print('\n=== ETF flow by date:')
try:
    r4 = db.execute(text('SELECT trade_date, COUNT(*) FROM etf_flow_summary GROUP BY trade_date ORDER BY trade_date DESC LIMIT 10')).fetchall()
    for row in r4: print(' ', row)
except Exception as e:
    print(f'  Error: {e}')

db.close()
