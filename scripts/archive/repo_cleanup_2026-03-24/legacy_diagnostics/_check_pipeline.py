"""Check pipeline task statuses."""
import sqlite3
db = sqlite3.connect("data/app.db")
rows = db.execute(
    "SELECT task_name, status, started_at, finished_at "
    "FROM scheduler_task_run WHERE trade_date='2026-03-16' ORDER BY task_name"
).fetchall()
for r in rows:
    print(r)

# Also check new settlements
cnt = db.execute("SELECT COUNT(*) FROM settlement_result WHERE signal_date='2026-03-16'").fetchone()[0]
print(f"\nSettlements for 2026-03-16: {cnt}")
total = db.execute("SELECT COUNT(*) FROM settlement_result").fetchone()[0]
print(f"Total settlements: {total}")

db.close()
