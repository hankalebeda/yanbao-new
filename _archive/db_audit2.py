"""Supplementary DB queries for v13."""
import sqlite3

db = sqlite3.connect("data/app.db")
c = db.cursor()

# 1. prediction_outcome columns
print("=== PREDICTION_OUTCOME SCHEMA ===")
for col in c.execute("PRAGMA table_info(prediction_outcome)"):
    print(f"  {col}")

# 2. Win rate via is_correct
print("\n=== PREDICTION WIN RATE ===")
for row in c.execute("SELECT is_correct, COUNT(*) FROM prediction_outcome GROUP BY is_correct"):
    print(f"  is_correct={row[0]}: {row[1]}")

total = c.execute("SELECT COUNT(*) FROM prediction_outcome WHERE is_correct IS NOT NULL").fetchone()[0]
wins = c.execute("SELECT COUNT(*) FROM prediction_outcome WHERE is_correct=1").fetchone()[0]
if total > 0:
    print(f"  Win rate: {wins}/{total} = {wins/total*100:.2f}%")

# 3. Settlement status distribution
print("\n=== SETTLEMENT STATUS ===")
for row in c.execute("SELECT settlement_status, COUNT(*) FROM settlement_result GROUP BY settlement_status"):
    print(f"  {row[0]}: {row[1]}")

# 5. Settlement outcome via signal_outcome
print("\n=== SETTLEMENT OUTCOMES ===")
try:
    for row in c.execute("SELECT signal_outcome, COUNT(*) FROM settlement_result GROUP BY signal_outcome ORDER BY COUNT(*) DESC"):
        print(f"  signal_outcome={row[0]}: {row[1]}")
except Exception as e:
    print(f"  Error: {e}")
print("--- settlement_result schema ---")
for col in c.execute("PRAGMA table_info(settlement_result)"):
    print(f"  {col}")

# 6. Settlement win rate via net_return_pct
print("\n=== SETTLEMENT WIN RATE ===")
try:
    pos = c.execute("SELECT COUNT(*) FROM settlement_result WHERE CAST(net_return_pct AS FLOAT) > 0").fetchone()[0]
    neg = c.execute("SELECT COUNT(*) FROM settlement_result WHERE CAST(net_return_pct AS FLOAT) <= 0").fetchone()[0]
    null_r = c.execute("SELECT COUNT(*) FROM settlement_result WHERE net_return_pct IS NULL").fetchone()[0]
    print(f"  Positive: {pos}, Negative/zero: {neg}, NULL: {null_r}")
    if pos + neg > 0:
        print(f"  Win rate (return>0): {pos}/{pos+neg} = {pos/(pos+neg)*100:.2f}%")
    avg = c.execute("SELECT AVG(CAST(net_return_pct AS FLOAT)) FROM settlement_result WHERE net_return_pct IS NOT NULL").fetchone()[0]
    print(f"  Avg net return: {avg}")
    # exit_reason distribution
    for row in c.execute("SELECT exit_reason, COUNT(*) FROM settlement_result GROUP BY exit_reason ORDER BY COUNT(*) DESC"):
        print(f"  exit_reason={row[0]}: {row[1]}")
    # quality_flag
    for row in c.execute("SELECT quality_flag, COUNT(*) FROM settlement_result GROUP BY quality_flag ORDER BY COUNT(*) DESC"):
        print(f"  quality_flag={row[0]}: {row[1]}")
except Exception as e:
    print(f"  Error: {e}")

# 6. stock_pool_refresh_task columns
print("\n=== STOCK_POOL_REFRESH_TASK SCHEMA ===")
for col in c.execute("PRAGMA table_info(stock_pool_refresh_task)"):
    print(f"  {col}")

# 7. Latest pool refresh
print("\n=== POOL REFRESH ===")
for col in c.execute("SELECT * FROM stock_pool_refresh_task ORDER BY rowid DESC LIMIT 1"):
    print(f"  {col}")

# 8. Report date distribution
print("\n=== REPORT DATE RANGE ===")
print(f"  Earliest: {c.execute('SELECT MIN(created_at) FROM report').fetchone()[0]}")
print(f"  Latest: {c.execute('SELECT MAX(created_at) FROM report').fetchone()[0]}")
print(f"  Reports by trade_date (top 10):")
for row in c.execute("SELECT trade_date, COUNT(*) FROM report GROUP BY trade_date ORDER BY trade_date DESC LIMIT 10"):
    print(f"    {row[0]}: {row[1]}")

# 9. Market state cache freshness  
print("\n=== MARKET STATE FRESHNESS ===")
try:
    for col in c.execute("PRAGMA table_info(market_state_cache)"):
        print(f"  {col}")
    cnt = c.execute("SELECT COUNT(*) FROM market_state_cache").fetchone()[0]
    print(f"  Total rows: {cnt}")
    for row in c.execute("SELECT cache_status, COUNT(*) FROM market_state_cache GROUP BY cache_status"):
        print(f"  cache_status={row[0]}: {row[1]}")
    for row in c.execute("SELECT market_state, COUNT(*) FROM market_state_cache GROUP BY market_state"):
        print(f"  market_state={row[0]}: {row[1]}")
except Exception as e:
    print(f"  Error: {e}")

# 10. DAG events
print("\n=== DAG EVENTS ===")
for row in c.execute("SELECT event_key, COUNT(*) FROM dag_event GROUP BY event_key"):
    print(f"  {row[0]}: {row[1]}")

# 11. Actual empty tables list (more precise)
print("\n=== EMPTY TABLES DETAIL ===")
tables = c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
for (t,) in tables:
    try:
        cnt = c.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
        if cnt == 0:
            print(f"  EMPTY: {t}")
    except:
        pass

# 12. Report content analysis
print("\n=== REPORT CONTENT ===")
with_json = c.execute("SELECT COUNT(*) FROM report WHERE content_json IS NOT NULL").fetchone()[0]
with_conclusion = c.execute("SELECT COUNT(*) FROM report WHERE conclusion_text IS NOT NULL").fetchone()[0]
print(f"  With content_json: {with_json}")
print(f"  With conclusion_text: {with_conclusion}")

# 13. LLM model usage
print("\n=== MODEL USAGE ===")
try:
    for row in c.execute("SELECT model_name, COUNT(*) FROM model_run_log GROUP BY model_name ORDER BY COUNT(*) DESC"):
        print(f"  {row[0]}: {row[1]}")
except:
    print("  model_run_log query failed")

# 14. Net return stats
print("\n=== NET RETURN STATS ===")
try:
    avg_return = c.execute("SELECT AVG(CAST(net_return_pct AS FLOAT)) FROM settlement_result WHERE net_return_pct IS NOT NULL").fetchone()[0]
    print(f"  Avg net return: {avg_return}")
    pos = c.execute("SELECT COUNT(*) FROM settlement_result WHERE CAST(net_return_pct AS FLOAT) > 0").fetchone()[0]
    neg = c.execute("SELECT COUNT(*) FROM settlement_result WHERE CAST(net_return_pct AS FLOAT) <= 0").fetchone()[0]
    print(f"  Positive returns: {pos}, Negative/zero: {neg}")
except Exception as e:
    print(f"  Error: {e}")

db.close()
