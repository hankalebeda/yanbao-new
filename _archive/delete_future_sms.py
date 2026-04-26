import sqlite3
conn = sqlite3.connect('data/app.db')
c = conn.cursor()

# Delete future SMS contamination (2026-05-10 w=7 rows)
c.execute("DELETE FROM strategy_metric_snapshot WHERE snapshot_date > '2026-04-22'")
print('strategy_metric_snapshot deleted:', c.rowcount)

c.execute("DELETE FROM baseline_metric_snapshot WHERE snapshot_date > '2026-04-22'")
print('baseline_metric_snapshot deleted:', c.rowcount)

conn.commit()

# Verify remaining
rows = c.execute("SELECT snapshot_date, window_days, strategy_type, sample_size FROM strategy_metric_snapshot ORDER BY snapshot_date DESC").fetchall()
print('Remaining strategy snapshots:', rows)
rows = c.execute("SELECT snapshot_date, window_days, COUNT(*) FROM baseline_metric_snapshot GROUP BY snapshot_date, window_days ORDER BY snapshot_date DESC").fetchall()
print('Remaining baseline snapshots:', rows)
conn.close()
