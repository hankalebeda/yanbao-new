"""DB audit for v13 progress table."""
import sqlite3

db = sqlite3.connect("data/app.db")
c = db.cursor()

# 1. Table row counts
print("=== TABLE ROW COUNTS ===")
tables = c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
total_tables = len(tables)
has_data = 0
empty_tables = []
for (t,) in tables:
    try:
        cnt = c.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
        if cnt > 0:
            has_data += 1
            print(f"  {t}: {cnt:,}")
        else:
            empty_tables.append(t)
    except Exception as e:
        empty_tables.append(f"{t}(ERR:{e})")

print(f"\nTotal tables: {total_tables}")
print(f"With data: {has_data}")
print(f"Empty: {len(empty_tables)}")
print(f"Empty tables: {empty_tables}")

# 2. Report quality
print("\n=== REPORT QUALITY ===")
for row in c.execute("SELECT quality_flag, COUNT(*) FROM report GROUP BY quality_flag ORDER BY COUNT(*) DESC"):
    print(f"  {row[0]}: {row[1]}")

# 3. LLM fallback
print("\n=== LLM FALLBACK ===")
for row in c.execute("SELECT llm_fallback_level, COUNT(*) FROM report GROUP BY llm_fallback_level ORDER BY COUNT(*) DESC"):
    print(f"  {row[0]}: {row[1]}")

# 4. Key metrics
print("\n=== KEY METRICS ===")
report_total = c.execute("SELECT COUNT(*) FROM report").fetchone()[0]
published = c.execute("SELECT COUNT(*) FROM report WHERE published=1").fetchone()[0]
settlement = c.execute("SELECT COUNT(*) FROM settlement_result").fetchone()[0]
prediction = c.execute("SELECT COUNT(*) FROM prediction_outcome").fetchone()[0]
users = c.execute("SELECT COUNT(*) FROM app_user").fetchone()[0]
kline = c.execute("SELECT COUNT(*) FROM kline_daily").fetchone()[0]
kline_stocks = c.execute("SELECT COUNT(DISTINCT stock_code) FROM kline_daily").fetchone()[0]
stock_master = c.execute("SELECT COUNT(*) FROM stock_master").fetchone()[0]
instr_card = c.execute("SELECT COUNT(*) FROM instruction_card").fetchone()[0]
rdu = c.execute("SELECT COUNT(*) FROM report_data_usage").fetchone()[0]

print(f"  Reports: {report_total}")
print(f"  Published: {published} ({published/max(report_total,1)*100:.1f}%)")
print(f"  Settlement: {settlement}")
print(f"  Prediction: {prediction}")
print(f"  Users: {users}")
print(f"  KLine records: {kline:,}")
print(f"  KLine stocks: {kline_stocks}")
print(f"  Stock master: {stock_master:,}")
print(f"  Instruction cards: {instr_card:,}")
print(f"  Report data usage: {rdu:,}")

try:
    hotspot = c.execute("SELECT COUNT(*) FROM market_hotspot_item").fetchone()[0]
    print(f"  Hotspot items: {hotspot}")
except:
    print("  Hotspot items: table not found")

# 5. Settlement stats
print("\n=== SETTLEMENT STATS ===")
try:
    for row in c.execute("SELECT outcome, COUNT(*) FROM prediction_outcome GROUP BY outcome ORDER BY COUNT(*) DESC"):
        print(f"  {row[0]}: {row[1]}")
    total_pred = c.execute("SELECT COUNT(*) FROM prediction_outcome WHERE outcome IS NOT NULL").fetchone()[0]
    wins = c.execute("SELECT COUNT(*) FROM prediction_outcome WHERE outcome = 'win'").fetchone()[0]
    if total_pred > 0:
        print(f"  Win rate: {wins/total_pred*100:.2f}%")
except Exception as e:
    print(f"  Error: {e}")

# 6. Data freshness
print("\n=== DATA FRESHNESS ===")
try:
    print(f"  Latest report: {c.execute('SELECT MAX(created_at) FROM report').fetchone()[0]}")
    print(f"  Latest kline: {c.execute('SELECT MAX(trade_date) FROM kline_daily').fetchone()[0]}")
    print(f"  Latest settlement: {c.execute('SELECT MAX(created_at) FROM settlement_result').fetchone()[0]}")
    print(f"  Latest pool refresh: {c.execute('SELECT MAX(refreshed_at) FROM stock_pool_snapshot').fetchone()[0]}")
except Exception as e:
    print(f"  Error: {e}")

# 7. Report publish status
print("\n=== REPORT PUBLISH STATUS ===")
for row in c.execute("SELECT published, COUNT(*) FROM report GROUP BY published"):
    print(f"  published={row[0]}: {row[1]}")

# 8. Report generation tasks
print("\n=== GENERATION TASKS ===")
try:
    for row in c.execute("SELECT status, COUNT(*) FROM report_generation_task GROUP BY status ORDER BY COUNT(*) DESC"):
        print(f"  {row[0]}: {row[1]}")
except Exception as e:
    print(f"  Error: {e}")

# 9. Audit log
print("\n=== AUDIT LOG ===")
try:
    audit_count = c.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
    print(f"  audit_log records: {audit_count}")
except Exception as e:
    print(f"  Error: {e}")

# 10. Cookie sessions
print("\n=== COOKIE SESSIONS ===")
try:
    cookie_count = c.execute("SELECT COUNT(*) FROM cookie_session").fetchone()[0]
    print(f"  cookie_session records: {cookie_count}")
except Exception as e:
    print(f"  Error: {e}")

# 11. Stock score
print("\n=== STOCK SCORE ===")
try:
    score_count = c.execute("SELECT COUNT(*) FROM stock_score").fetchone()[0]
    print(f"  stock_score records: {score_count}")
except Exception as e:
    print(f"  Error: {e}")

# 12. Scheduler tasks
print("\n=== SCHEDULER TASKS ===")
try:
    for row in c.execute("SELECT status, COUNT(*) FROM scheduler_task GROUP BY status ORDER BY COUNT(*) DESC"):
        print(f"  {row[0]}: {row[1]}")
except Exception as e:
    print(f"  Error: {e}")

# 13. Notification
print("\n=== NOTIFICATIONS ===")
try:
    notif_count = c.execute("SELECT COUNT(*) FROM notification").fetchone()[0]
    print(f"  notification records: {notif_count}")
except Exception as e:
    print(f"  Error: {e}")

db.close()
