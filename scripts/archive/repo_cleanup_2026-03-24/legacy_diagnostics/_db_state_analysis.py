"""Database state analysis script."""
import sqlite3
import json

conn = sqlite3.connect('data/app.db')
conn.row_factory = sqlite3.Row

# Check tables
tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
print(f'Tables: {len(tables)}')
for t in tables:
    count = conn.execute(f'SELECT COUNT(*) FROM [{t}]').fetchone()[0]
    if count > 0:
        print(f'  {t}: {count} rows')

print()

# Check today's reports
today_reports = conn.execute("SELECT COUNT(*) FROM report WHERE trade_date >= '2026-03-20'").fetchone()[0]
print(f"Today reports (>=2026-03-20): {today_reports}")

# Check yesterday reports
yesterday_reports = conn.execute("SELECT COUNT(*) FROM report WHERE trade_date = '2026-03-19'").fetchone()[0]
print(f"Yesterday reports (2026-03-19): {yesterday_reports}")

# Check all report dates
dates = conn.execute("SELECT trade_date, COUNT(*) as cnt FROM report GROUP BY trade_date ORDER BY trade_date DESC LIMIT 10").fetchall()
print("Report dates:")
for d in dates:
    print(f"  {d[0]}: {d[1]} reports")

# Check report recommendations distribution
recs = conn.execute("SELECT recommendation, COUNT(*) as cnt FROM report GROUP BY recommendation ORDER BY cnt DESC").fetchall()
print("\nRecommendation distribution:")
for r in recs:
    print(f"  {r[0]}: {r[1]}")

# Check strategy types
strats = conn.execute("SELECT strategy_type, COUNT(*) as cnt FROM report GROUP BY strategy_type ORDER BY cnt DESC").fetchall()
print("\nStrategy type distribution:")
for s in strats:
    print(f"  {s[0]}: {s[1]}")

# Check settlement
settled = conn.execute("SELECT COUNT(*) FROM settlement_result").fetchone()[0]
print(f"\nSettlement results: {settled}")

# Check settlement outcomes
outcomes = conn.execute("SELECT settlement_status, COUNT(*) as cnt FROM settlement_result GROUP BY settlement_status ORDER BY cnt DESC").fetchall()
print("Settlement statuses:")
for o in outcomes:
    print(f"  {o[0]}: {o[1]}")

# Check win/loss distribution
wins = conn.execute("SELECT COUNT(*) FROM settlement_result WHERE net_return_pct > 0").fetchone()[0]
losses = conn.execute("SELECT COUNT(*) FROM settlement_result WHERE net_return_pct <= 0 AND net_return_pct IS NOT NULL AND settlement_status='SETTLED'").fetchone()[0]
print(f"Wins (net_return_pct > 0): {wins}")
print(f"Losses (net_return_pct <= 0 and settled): {losses}")

# Check avg returns
avg_return = conn.execute("SELECT AVG(net_return_pct) FROM settlement_result WHERE settlement_status='SETTLED'").fetchone()[0]
print(f"Avg net return (settled): {avg_return}")

# Check strategy metric snapshots
snapshots = conn.execute("SELECT snapshot_date, window_days, COUNT(*) FROM strategy_metric_snapshot GROUP BY snapshot_date, window_days ORDER BY snapshot_date DESC LIMIT 15").fetchall()
print("\nStrategy metric snapshots:")
for s in snapshots:
    print(f"  date={s[0]} window={s[1]}d count={s[2]}")

# Check users
users = conn.execute("SELECT user_id, email, role, tier, email_verified, created_at FROM app_user ORDER BY created_at DESC LIMIT 10").fetchall()
print(f"\nUsers ({len(users)}):")
for u in users:
    print(f"  id={u[0][:8]}... email={u[1]} role={u[2]} tier={u[3]} verified={u[4]}")

# Check stock pool
pool = conn.execute("SELECT trade_date, pool_version, core_pool_size, status, status_reason FROM stock_pool_refresh_task ORDER BY created_at DESC LIMIT 5").fetchall()
print("\nStock pool tasks:")
for p in pool:
    print(f"  date={p[0]} v={p[1]} size={p[2]} status={p[3]} reason={p[4]}")

# Check hot stocks
hots = conn.execute("SELECT COUNT(*) FROM market_hotspot_item").fetchone()[0]
print(f"\nHot stock items: {hots}")

# Check hot stock sources
hsources = conn.execute("SELECT source, COUNT(*) as cnt FROM market_hotspot_item_source GROUP BY source ORDER BY cnt DESC LIMIT 5").fetchall()
print("Hot stock sources:")
for h in hsources:
    print(f"  {h[0]}: {h[1]}")

# Check sim accounts
sims = conn.execute("SELECT capital_tier, initial_capital, current_equity FROM sim_account LIMIT 5").fetchall()
print(f"\nSim accounts ({len(sims)}):")
for s in sims:
    print(f"  tier={s[0]} initial={s[1]} equity={s[2]}")

# Check sim positions
pos_count = conn.execute("SELECT COUNT(*) FROM sim_position").fetchone()[0]
pos_status = conn.execute("SELECT status, COUNT(*) as cnt FROM sim_position GROUP BY status ORDER BY cnt DESC").fetchall()
print(f"\nSim positions: {pos_count}")
for p in pos_status:
    print(f"  {p[0]}: {p[1]}")

# Check business events
events = conn.execute("SELECT event_type, COUNT(*) as cnt FROM business_event GROUP BY event_type ORDER BY cnt DESC LIMIT 10").fetchall()
print(f"\nBusiness events:")
for e in events:
    print(f"  {e[0]}: {e[1]}")

# Check data sources
batches = conn.execute("SELECT source_name, status, COUNT(*) FROM data_batch GROUP BY source_name, status ORDER BY source_name LIMIT 20").fetchall()
print(f"\nData batches:")
for b in batches:
    print(f"  {b[0]} [{b[1]}]: {b[2]}")

# Check kline coverage 
kline_count = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM kline_daily").fetchone()[0]
kline_dates = conn.execute("SELECT MIN(trade_date), MAX(trade_date) FROM kline_daily").fetchone()
print(f"\nKline daily: {kline_count} stocks, date range: {kline_dates[0]} ~ {kline_dates[1]}")

# Check scheduler tasks
tasks = conn.execute("SELECT task_name, status, COUNT(*) as cnt FROM scheduler_task_run GROUP BY task_name, status ORDER BY task_name LIMIT 20").fetchall()
print(f"\nScheduler tasks:")
for t in tasks:
    print(f"  {t[0]} [{t[1]}]: {t[2]}")

# Check if there are reports without instruction cards
reports_no_card = conn.execute("""
    SELECT COUNT(*) FROM report r 
    WHERE NOT EXISTS (SELECT 1 FROM instruction_card ic WHERE ic.report_id = r.id)
    AND r.recommendation = 'BUY'
""").fetchone()[0]
print(f"\nBUY reports without instruction cards: {reports_no_card}")

# Check reports without data usage
reports_no_usage = conn.execute("""
    SELECT COUNT(*) FROM report r 
    WHERE NOT EXISTS (SELECT 1 FROM report_data_usage rdu WHERE rdu.report_id = r.id)
""").fetchone()[0]
total_reports = conn.execute("SELECT COUNT(*) FROM report").fetchone()[0]
print(f"Reports without data usage: {reports_no_usage} / {total_reports}")

# Check billing orders
orders = conn.execute("SELECT COUNT(*) FROM billing_order").fetchone()[0]
print(f"\nBilling orders: {orders}")

# Check feedback
feedback = conn.execute("SELECT COUNT(*) FROM report_feedback").fetchone()[0]
print(f"Report feedback: {feedback}")

# Check DAG events
dag = conn.execute("SELECT event_type, status, COUNT(*) FROM dag_event GROUP BY event_type, status ORDER BY event_type LIMIT 20").fetchall()
print(f"\nDAG events:")
for d in dag:
    print(f"  {d[0]} [{d[1]}]: {d[2]}")

# Check circuit breaker state
circuit = conn.execute("SELECT source_name, is_open, failure_count, last_failure_at FROM data_source_circuit_state").fetchall()
print(f"\nCircuit breaker state:")
for c in circuit:
    print(f"  {c[0]}: open={c[1]} failures={c[2]} last_failure={c[3]}")

conn.close()
print("\nDatabase analysis complete.")
