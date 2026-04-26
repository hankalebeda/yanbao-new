"""Simple DB exploration"""
import sys
sys.path.insert(0, '.')
from app.core.db import SessionLocal
from sqlalchemy import text

db = SessionLocal()

# First, show what data usage the ok reports have
print("=== OK report data usage:")
rid = db.execute(text("SELECT report_id FROM report WHERE is_deleted=0 AND quality_flag='ok' LIMIT 1")).scalar()
print(f"Report: {rid}")
r = db.execute(text("SELECT u.dataset_name, u.status, u.status_reason FROM report_data_usage u JOIN report_data_usage_link l ON l.usage_id=u.usage_id WHERE l.report_id=:rid"), {"rid": rid}).fetchall()
for row in r: print(' ', row)

# Why 2026-04-06 fails - check data_source_circuit_state
print("\n=== Data source circuit state:")
r2 = db.execute(text("PRAGMA table_info(data_source_circuit_state)")).fetchall()
for c in r2[:5]: print(' ', c)
r3 = db.execute(text("SELECT source_name, state, last_updated FROM data_source_circuit_state LIMIT 10")).fetchall()
for row in r3: print(' ', row)

# Check report_generation_task for recent failed stock
print("\n=== Recent generation task failures:")
r4 = db.execute(text("SELECT stock_code, trade_date, status, status_reason FROM report_generation_task WHERE trade_date >= '2026-04-06' ORDER BY created_at DESC LIMIT 15")).fetchall()
for row in r4: print(' ', row)

db.close()
