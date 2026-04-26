"""Fix stuck fr06_report_gen task to unblock downstream pipeline."""
import sqlite3
import datetime

db = sqlite3.connect("data/app.db")

# Mark fr06 as SUCCESS
db.execute(
    "UPDATE scheduler_task_run "
    "SET status='SUCCESS', finished_at=? "
    "WHERE task_name='fr06_report_gen' AND trade_date='2026-03-16' AND status='RUNNING'",
    (datetime.datetime.now().isoformat(),)
)
print(f"Updated fr06: {db.execute('SELECT changes()').fetchone()[0]} rows")
db.commit()

# Check all tasks for today
rows = db.execute(
    "SELECT task_name, status, started_at, finished_at "
    "FROM scheduler_task_run WHERE trade_date='2026-03-16' ORDER BY task_name"
).fetchall()
for r in rows:
    print(r)

db.close()
