"""Quick DB state check - safe to delete after use."""
import sqlite3, os

db_path = "data/app.db"
print(f"DB exists: {os.path.exists(db_path)}, size: {os.path.getsize(db_path)/1024:.1f} KB")

conn = sqlite3.connect(db_path)
cur = conn.cursor()

tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
print(f"\nTotal tables: {len(tables)}")
for t in tables:
    count = cur.execute(f"SELECT COUNT(*) FROM [{t[0]}]").fetchone()[0]
    print(f"  {t[0]}: {count} rows")

# Check key data
print("\n--- Key Data Checks ---")
try:
    reports = cur.execute("SELECT id, ts_code, signal_type, confidence, quality_flag, created_at FROM report ORDER BY created_at DESC LIMIT 5").fetchall()
    print(f"\nLatest reports ({len(reports)}):")
    for r in reports:
        print(f"  id={r[0]}, stock={r[1]}, signal={r[2]}, conf={r[3]}, quality={r[4]}, time={r[5]}")
except Exception as e:
    print(f"Report query error: {e}")

try:
    users = cur.execute("SELECT id, email, role FROM user LIMIT 5").fetchall()
    print(f"\nUsers ({len(users)}):")
    for u in users:
        print(f"  id={u[0]}, email={u[1]}, role={u[2]}")
except Exception as e:
    print(f"User query error: {e}")

try:
    klines = cur.execute("SELECT COUNT(*) FROM kline_daily").fetchone()[0]
    stocks = cur.execute("SELECT COUNT(DISTINCT ts_code) FROM kline_daily").fetchone()[0]
    print(f"\nKline data: {klines} rows, {stocks} stocks")
except Exception as e:
    print(f"Kline query error: {e}")

try:
    pool = cur.execute("SELECT COUNT(*) FROM stock_pool_snapshot").fetchone()[0]
    print(f"Stock pool snapshots: {pool}")
except Exception as e:
    print(f"Pool query error: {e}")

try:
    sim_acc = cur.execute("SELECT capital_tier, total_equity, cash_balance FROM sim_account").fetchall()
    print(f"\nSim accounts ({len(sim_acc)}):")
    for a in sim_acc:
        print(f"  tier={a[0]}, equity={a[1]}, cash={a[2]}")
except Exception as e:
    print(f"Sim account error: {e}")

conn.close()
