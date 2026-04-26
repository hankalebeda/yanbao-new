"""Emit missing DAG event for fr06 so fr07 can proceed."""
import sqlite3
import uuid
import datetime

db = sqlite3.connect("data/app.db")

# Check existing events
rows = db.execute(
    "SELECT event_name, trade_date FROM dag_event WHERE trade_date='2026-03-16'"
).fetchall()
print("Existing events for 2026-03-16:")
for r in rows:
    print(f"  {r}")

# Check if FR06_BATCH_COMPLETED exists
exists = db.execute(
    "SELECT COUNT(*) FROM dag_event "
    "WHERE event_name='FR06_BATCH_COMPLETED' AND trade_date='2026-03-16'"
).fetchone()[0]

if not exists:
    # Get the fr06 task_run_id
    fr06 = db.execute(
        "SELECT task_run_id FROM scheduler_task_run "
        "WHERE task_name='fr06_report_gen' AND trade_date='2026-03-16' AND status='SUCCESS'"
    ).fetchone()
    producer_id = fr06[0] if fr06 else None
    
    event_id = str(uuid.uuid4())
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    db.execute(
        "INSERT INTO dag_event (dag_event_id, event_key, event_name, trade_date, "
        "producer_task_run_id, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (event_id, "FR06_BATCH_COMPLETED:2026-03-16", "FR06_BATCH_COMPLETED",
         "2026-03-16", producer_id, "{}", now)
    )
    db.commit()
    print(f"Emitted FR06_BATCH_COMPLETED event: {event_id}")
else:
    print("FR06_BATCH_COMPLETED already exists")

db.close()
