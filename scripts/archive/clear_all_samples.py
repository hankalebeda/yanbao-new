"""Delete all historical prediction outcomes and reset reports to force fresh generation."""
import sqlite3

conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

# Delete prediction outcomes
cur.execute("DELETE FROM prediction_outcome")
print(f"Deleted prediction_outcome rows: {cur.rowcount}")

# Delete all reports so fresh ones can be generated
cur.execute("DELETE FROM report")
print(f"Deleted report rows: {cur.rowcount}")

# Delete idempotency keys to allow fresh generation
cur.execute("DELETE FROM report_idempotency")
print(f"Deleted report_idempotency rows: {cur.rowcount}")

# Delete model run logs
cur.execute("DELETE FROM model_run_log")
print(f"Deleted model_run_log rows: {cur.rowcount}")

conn.commit()
conn.close()
print("\nAll historical data cleared. New reports will be generated fresh.")
