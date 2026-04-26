"""Check scheduler runs and metric snapshots."""
import sqlite3
db = sqlite3.connect("data/app.db")

print("=== SCHEDULER TASKS ON 2026-03-16 ===")
rows = db.execute(
    "SELECT task_name, status, started_at, finished_at "
    "FROM scheduler_task_run WHERE trade_date='2026-03-16' ORDER BY started_at DESC"
).fetchall()
for r in rows:
    print(r)

print("\n=== STRATEGY METRIC SNAPSHOT ===")
rows = db.execute(
    "SELECT snapshot_date, window_days, strategy_type, sample_size, win_rate, data_status "
    "FROM strategy_metric_snapshot ORDER BY snapshot_date DESC, window_days LIMIT 20"
).fetchall()
for r in rows:
    print(r)

print("\n=== BASELINE METRIC SNAPSHOT ===")
rows = db.execute(
    "SELECT snapshot_date, window_days, baseline_type, sample_size, win_rate "
    "FROM baseline_metric_snapshot ORDER BY snapshot_date DESC LIMIT 10"
).fetchall()
for r in rows:
    print(r)

print("\n=== SIM ACCOUNT SNAPSHOT ===")
rows = db.execute("SELECT * FROM sim_account_snapshot ORDER BY snapshot_date DESC LIMIT 5").fetchall()
for r in rows:
    print(r)

print("\n=== EQUITY CURVE ===")
rows = db.execute("SELECT * FROM equity_curve_point ORDER BY trade_date DESC LIMIT 5").fetchall()
for r in rows:
    print(r)

print("\n=== DATA USAGE SUMMARY ===")
rows = db.execute(
    "SELECT status, COUNT(*) FROM report_data_usage GROUP BY status"
).fetchall()
for r in rows:
    print(r)

db.close()
