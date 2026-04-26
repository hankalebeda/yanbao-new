"""DB audit script for v15 - get all tables with row counts."""
import sqlite3

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
print(f"Total tables: {len(tables)}")
print()
print(f"{'Table':<45} {'Rows':>8}")
print("-" * 55)

empty = []
nonempty = []
for t in tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM [{t}]")
        cnt = cur.fetchone()[0]
        if cnt == 0:
            empty.append(t)
        else:
            nonempty.append((t, cnt))
        print(f"{t:<45} {cnt:>8}")
    except Exception as e:
        print(f"{t:<45} ERROR: {e}")

print()
print(f"Non-empty: {len(nonempty)}, Empty: {len(empty)}")
print(f"\nEmpty tables ({len(empty)}):")
for t in empty:
    print(f"  - {t}")

# Key metrics
print("\n=== Key Metrics ===")
queries = [
    ("Reports (published)", "SELECT COUNT(*) FROM report WHERE published = 1"),
    ("Reports (total)", "SELECT COUNT(*) FROM report"),
    ("Settlement results", "SELECT COUNT(*) FROM settlement_result"),
    ("Prediction outcomes", "SELECT COUNT(*) FROM prediction_outcome"),
    ("Kline stocks", "SELECT COUNT(DISTINCT stock_code) FROM kline_daily"),
    ("Kline rows", "SELECT COUNT(*) FROM kline_daily"),
    ("Users", "SELECT COUNT(*) FROM app_user"),
    ("Cookie sessions", "SELECT COUNT(*) FROM cookie_session"),
    ("Hotspot items", "SELECT COUNT(*) FROM market_hotspot_item"),
    ("Hotspot raw", "SELECT COUNT(*) FROM hotspot_raw"),
    ("Hotspot normalized", "SELECT COUNT(*) FROM hotspot_normalized"),
    ("Hotspot top50", "SELECT COUNT(*) FROM hotspot_top50"),
    ("Notifications", "SELECT COUNT(*) FROM notification"),
    ("Sim positions", "SELECT COUNT(*) FROM sim_position"),
    ("Sim accounts", "SELECT COUNT(*) FROM sim_account"),
    ("Sim dashboard", "SELECT COUNT(*) FROM sim_dashboard_snapshot"),
    ("Business events", "SELECT COUNT(*) FROM business_event"),
    ("Stock score", "SELECT COUNT(*) FROM stock_score"),
]
for label, sql in queries:
    try:
        cur.execute(sql)
        val = cur.fetchone()[0]
        print(f"  {label}: {val}")
    except Exception as e:
        print(f"  {label}: ERROR - {e}")

conn.close()
