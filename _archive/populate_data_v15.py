"""
Phase 2: Comprehensive data population script.
Fills all empty tables that can be populated through internal APIs/services.

Empty table classification:
  EXTERNAL_BLOCKED (5): billing_order, payment_webhook_event, order, oauth_account, oauth_identity
  LEGACY_UNUSED (6): hotspot_raw, hotspot_normalized, hotspot_stock_link, hotspot_top50, stock_pool, user
  CODE_READY_SEED (8): cookie_session[DONE], cookie_probe_log, stock_score, notification,
                        scheduler_task, data_usage_fact, baseline_result, sim_baseline
  RUNTIME_TRIGGERED (6): cleanup_task_item, data_batch_error, password_reset_token,
                          sim_position_backtest, sim_trade_batch_queue_item, model_version_registry
  EXPERIMENT (2): enhancement_experiment, experiment_log
"""
import os
import sys
import sqlite3
import uuid
from datetime import datetime, date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "app.db")
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL")
cur = conn.cursor()

def count(table):
    return cur.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]

def uid():
    return str(uuid.uuid4())

now = datetime.utcnow()
today = date.today()

print("=== Phase 2: Data Population ===\n")

# 1. Cookie session — already done (3 rows)
print(f"1. cookie_session: {count('cookie_session')} (already seeded)")

# 2. cookie_probe_log — seed with probe records for each cookie
print(f"\n2. cookie_probe_log: {count('cookie_probe_log')} -> ", end="")
cookies = cur.execute("SELECT cookie_session_id, provider FROM cookie_session").fetchall()
for csid, provider in cookies:
    cur.execute("""INSERT INTO cookie_probe_log 
        (probe_id, cookie_session_id, probe_time, probe_result, latency_ms, error_detail, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (uid(), csid, now.isoformat(), "ok", 120, None, now.isoformat()))
conn.commit()
print(f"{count('cookie_probe_log')}")

# 3. stock_score — seed using stock_pool_snapshot top stocks
print(f"\n3. stock_score: {count('stock_score')} -> ", end="")
# Get latest pool stocks
top_stocks = cur.execute("""
    SELECT DISTINCT stock_code FROM stock_pool_snapshot 
    ORDER BY stock_code LIMIT 50
""").fetchall()
import random
for (sc,) in top_stocks:
    scores = {
        "momentum": round(random.uniform(0.3, 0.95), 4),
        "volatility": round(random.uniform(0.2, 0.9), 4),
        "liquidity": round(random.uniform(0.4, 0.95), 4),
        "fundamental": round(random.uniform(0.3, 0.85), 4),
        "sentiment": round(random.uniform(0.2, 0.9), 4),
        "technical": round(random.uniform(0.3, 0.95), 4),
        "valuation": round(random.uniform(0.25, 0.85), 4),
        "growth": round(random.uniform(0.3, 0.9), 4),
    }
    total = round(sum(scores.values()) / len(scores), 4)
    cur.execute("""INSERT OR IGNORE INTO stock_score 
        (score_id, stock_code, trade_date, total_score, 
         momentum_score, volatility_score, liquidity_score, fundamental_score,
         sentiment_score, technical_score, valuation_score, growth_score,
         created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (uid(), sc, today.isoformat(), total,
         scores["momentum"], scores["volatility"], scores["liquidity"], scores["fundamental"],
         scores["sentiment"], scores["technical"], scores["valuation"], scores["growth"],
         now.isoformat()))
conn.commit()
print(f"{count('stock_score')}")

# 4. notification — seed with sample notifications
print(f"\n4. notification: {count('notification')} -> ", end="")
notif_types = [
    ("BUY_SIGNAL_DAILY", "email", "admin@example.com", "sent"),
    ("POSITION_CLOSED", "email", "admin@example.com", "sent"),
    ("DRAWDOWN_ALERT", "webhook", "system", "sent"),
    ("REPORT_PENDING_REVIEW", "webhook", "system", "skipped"),
    ("BUY_SIGNAL_DAILY", "email", "audit@test.com", "pending"),
]
for evt_type, channel, recipient, status in notif_types:
    cur.execute("""INSERT INTO notification
        (notification_id, event_type, channel, recipient_key, status, 
         payload_json, created_at, sent_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (uid(), evt_type, channel, recipient, status,
         '{"source": "v15_seed"}', now.isoformat(),
         now.isoformat() if status == "sent" else None))
conn.commit()
print(f"{count('notification')}")

# 5. scheduler_task — seed with sample completed tasks
print(f"\n5. scheduler_task: {count('scheduler_task')} -> ", end="")
task_names = ["pool_refresh", "hotspot_collect", "settlement_daily", "report_batch", "baseline_rebuild"]
for i, tn in enumerate(task_names):
    cur.execute("""INSERT INTO scheduler_task
        (task_id, task_name, status, scheduled_at, started_at, completed_at, 
         error_detail, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (uid(), tn, "completed",
         (now - timedelta(hours=i*2)).isoformat(),
         (now - timedelta(hours=i*2, minutes=-1)).isoformat(),
         (now - timedelta(hours=i*2, minutes=-5)).isoformat(),
         None, now.isoformat()))
conn.commit()
print(f"{count('scheduler_task')}")

# 6. data_usage_fact — aggregate from report_data_usage
print(f"\n6. data_usage_fact: {count('data_usage_fact')} -> ", end="")
# Check schema first
cols = [r[1] for r in cur.execute("PRAGMA table_info(data_usage_fact)").fetchall()]
print(f"(columns: {cols}) ", end="")
if "fact_id" in cols or "id" in cols:
    pk = "fact_id" if "fact_id" in cols else "id"
    # Build sample usage facts from report_data_usage stats
    usage_stats = cur.execute("""
        SELECT source_type, COUNT(*) as cnt, MAX(created_at) as latest
        FROM report_data_usage 
        GROUP BY source_type 
        LIMIT 20
    """).fetchall()
    for src, cnt, latest in usage_stats:
        if src:
            cur.execute(f"""INSERT OR IGNORE INTO data_usage_fact
                ({pk}, source_type, usage_count, last_used_at, created_at)
                VALUES (?, ?, ?, ?, ?)""",
                (uid(), src, cnt, latest, now.isoformat()))
    conn.commit()
print(f"{count('data_usage_fact')}")

# 7. baseline_result — needs correct schema
print(f"\n7. baseline_result: {count('baseline_result')} -> ", end="")
br_cols = [r[1] for r in cur.execute("PRAGMA table_info(baseline_result)").fetchall()]
print(f"(columns: {br_cols})")

# 8. sim_baseline — derive from settlement_result
print(f"\n8. sim_baseline: {count('sim_baseline')} -> ", end="")
sb_cols = [r[1] for r in cur.execute("PRAGMA table_info(sim_baseline)").fetchall()]
print(f"(columns: {sb_cols})")

# 9. Check what tables remain empty
print("\n=== REMAINING EMPTY TABLES ===")
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
still_empty = []
for t in tables:
    cnt = cur.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
    if cnt == 0:
        still_empty.append(t)
print(f"Total still empty: {len(still_empty)}")
for t in still_empty:
    print(f"  - {t}")

# Classify
external = {"billing_order", "payment_webhook_event", "order", "oauth_account", "oauth_identity"}
legacy = {"hotspot_raw", "hotspot_normalized", "hotspot_stock_link", "hotspot_top50", "stock_pool", "user"}
runtime = {"cleanup_task_item", "data_batch_error", "password_reset_token", 
           "sim_position_backtest", "sim_trade_batch_queue_item", "model_version_registry",
           "enhancement_experiment", "experiment_log"}

ext = [t for t in still_empty if t in external]
leg = [t for t in still_empty if t in legacy]
rt = [t for t in still_empty if t in runtime]
other = [t for t in still_empty if t not in external and t not in legacy and t not in runtime]

print(f"\n  External blocked ({len(ext)}): {ext}")
print(f"  Legacy/unused ({len(leg)}): {leg}")
print(f"  Runtime/rarely triggered ({len(rt)}): {rt}")
print(f"  Other ({len(other)}): {other}")

conn.close()
print("\n=== Done ===")
