"""Check idempotency table"""
import sys
sys.path.insert(0, '.')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()

print("=== Idempotency records for 2026-04-03:")
r = db.execute(text("""
    SELECT stock_code, trade_date, status, expires_at, report_id
    FROM report_idempotency
    WHERE trade_date='2026-04-03'
    ORDER BY created_at DESC
    LIMIT 30
""")).fetchall()
for row in r: print(' ', row)

print("\n=== Total idempotency by status:")
r2 = db.execute(text("SELECT status, COUNT(*) FROM report_idempotency GROUP BY status")).fetchall()
for row in r2: print(' ', row)

# What does CONCURRENT_CONFLICT mean in the code
# Likely: status='Processing' and not expired
print("\n=== Processing idempotency records:")
r3 = db.execute(text("""
    SELECT stock_code, trade_date, status, expires_at
    FROM report_idempotency
    WHERE status='Processing'
    LIMIT 20
""")).fetchall()
for row in r3: print(' ', row)

db.close()
