"""Quick DB inspection for bug root causes."""
import sqlite3
db = sqlite3.connect("data/app.db")
db.row_factory = sqlite3.Row

# 1. BUY recommendations today
print("=== BUY RECOMMENDATIONS FOR 2026-03-16 ===")
rows = db.execute(
    "SELECT recommendation, COUNT(*) as cnt FROM report "
    "WHERE trade_date='2026-03-16' AND is_deleted=0 GROUP BY recommendation"
).fetchall()
for r in rows:
    print(dict(r))

# 2. business_event table
print("\n=== BUSINESS_EVENT ===")
rows = db.execute("SELECT * FROM business_event LIMIT 10").fetchall()
for r in rows:
    print(dict(r))

# 3. Holding days
print("\n=== SIM POSITIONS (OPEN) ===")
rows = db.execute(
    "SELECT position_id, stock_code, signal_date, entry_date, holding_days, "
    "net_return_pct, position_status, capital_tier FROM sim_position "
    "WHERE position_status='OPEN' LIMIT 5"
).fetchall()
for r in rows:
    print(dict(r))

# 4. Settlement accuracy across windows
print("\n=== SETTLEMENT RESULTS ===")
rows = db.execute(
    "SELECT window_days, signal_date, COUNT(*) as cnt, "
    "AVG(CASE WHEN net_return_pct > 0 THEN 1.0 ELSE 0.0 END) as win_rate "
    "FROM settlement_result GROUP BY window_days, signal_date ORDER BY signal_date DESC, window_days"
).fetchall()
for r in rows:
    print(dict(r))
total = db.execute("SELECT COUNT(*) FROM settlement_result").fetchone()[0]
print(f"Total settlements: {total}")
unique_dates = db.execute("SELECT DISTINCT signal_date FROM settlement_result").fetchall()
print(f"Unique signal dates: {[r[0] for r in unique_dates]}")

# 5. Strategy distribution in reports
print("\n=== STRATEGY DISTRIBUTION ===")
rows = db.execute(
    "SELECT strategy_type, trade_date, COUNT(*) as cnt FROM report "
    "WHERE is_deleted=0 GROUP BY strategy_type, trade_date ORDER BY trade_date DESC, strategy_type"
).fetchall()
for r in rows:
    print(dict(r))

# 6. LLM call log
print("\n=== LLM CALL LOG ===")
cols = db.execute("PRAGMA table_info(llm_call_log)").fetchall()
print("Columns:", [c[1] for c in cols])
cnt = db.execute("SELECT COUNT(*) FROM llm_call_log").fetchone()[0]
print(f"Total rows: {cnt}")
if cnt > 0:
    rows = db.execute("SELECT * FROM llm_call_log LIMIT 3").fetchall()
    for r in rows:
        print(dict(r))

# 7. Check pool_size from different sources
print("\n=== POOL SIZE COMPARISON ===")
# From refresh task
task = db.execute(
    "SELECT trade_date, core_pool_size, pool_version FROM stock_pool_refresh_task "
    "ORDER BY trade_date DESC LIMIT 1"
).fetchone()
print(f"Refresh task: {dict(task) if task else 'NONE'}")
# From snapshot
snap = db.execute(
    "SELECT trade_date, COUNT(*) as cnt FROM stock_pool_snapshot "
    "WHERE pool_role='core' GROUP BY trade_date ORDER BY trade_date DESC LIMIT 3"
).fetchall()
for r in snap:
    print(f"Snapshot: {dict(r)}")

db.close()
