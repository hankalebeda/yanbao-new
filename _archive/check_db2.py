"""DB diagnostic - check tables and data availability"""
import sys
sys.path.insert(0, '.')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()

print('=== TABLES IN DB:')
r = db.execute(text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")).fetchall()
tables = [n[0] for n in r]
for t in tables:
    print(' ', t)

print('\n=== HOTSPOT DATA by date:')
r = db.execute(text("SELECT date(fetch_time) as dt, COUNT(*) FROM hotspot_raw GROUP BY dt ORDER BY dt DESC LIMIT 10")).fetchall()
for row in r: print(' ', row)

print('\n=== REPORT DATA USAGE by dataset_name:')
r = db.execute(text("SELECT dataset_name, status, COUNT(*) FROM report_data_usage GROUP BY dataset_name, status ORDER BY dataset_name")).fetchall()
for row in r: print(' ', row)

# What does a successful report need?
print('\n=== Checking 2026-04-03 data sources (working date):')
r = db.execute(text("""
    SELECT u.dataset_name, u.status, COUNT(*) 
    FROM report r
    JOIN report_data_usage_link l ON l.report_id=r.report_id
    JOIN report_data_usage u ON u.usage_id=l.usage_id
    WHERE r.quality_flag='ok' AND r.is_deleted=0
    GROUP BY u.dataset_name, u.status
""")).fetchall()
for row in r: print(' ', row)

db.close()
