"""分析其他系统级问题 - 不涉及研报生成"""
import sqlite3, os, json
from pathlib import Path

c = sqlite3.connect('data/app.db')
c.row_factory = sqlite3.Row
cur = c.cursor()

print("="*70)
print("系统级问题分析 (非研报)")
print("="*70)

# ISSUE-1: 遗留软删除研报
print("\n[ISSUE-1] 软删除研报数量 (需物理清理):")
cur.execute("SELECT count(*) FROM report WHERE is_deleted=1")
print(f"  is_deleted=1: {cur.fetchone()[0]} 条")

# ISSUE-2: 过期或stale的data_batch
print("\n[ISSUE-2] data_batch 状态分布:")
cur.execute("""SELECT source_name, batch_status, count(*) FROM data_batch 
    GROUP BY source_name, batch_status ORDER BY source_name, batch_status""")
for r in cur.fetchall(): print(f"  {r[0]:20s} {r[1]:20s}: {r[2]}")

# ISSUE-3: 缺 kline 的 3 只股票
print("\n[ISSUE-3] 缺kline的3只股票:")
with open('_archive/audit_v24_phase1_evidence/core_pool.json') as f:
    core = json.load(f)['core_stocks']
pool_str = ",".join([f"'{s}'" for s in core])
cur.execute(f"""SELECT DISTINCT stock_code FROM report_data_usage 
    WHERE trade_date='2026-04-07' AND dataset_name='kline_daily' AND status='ok' 
    AND stock_code IN ({pool_str})""")
has_kline = set(r[0] for r in cur.fetchall())
missing = [s for s in core if s not in has_kline]
print(f"  missing: {missing}")

# ISSUE-4: stock_master 状态
print("\n[ISSUE-4] stock_master 对缺失股票的状态:")
for s in missing:
    cur.execute("SELECT stock_code, stock_name, is_st, is_suspended, is_delisted, list_date FROM stock_master WHERE stock_code=?", (s,))
    r = cur.fetchone()
    if r: print(f"  {dict(r)}")
    else: print(f"  {s}: NOT IN stock_master")

# ISSUE-5: report_data_usage 非 ok 状态
print("\n[ISSUE-5] report_data_usage 状态分布 (2026-04-01+):")
cur.execute("""SELECT status, count(*) FROM report_data_usage 
    WHERE trade_date>='2026-04-01' GROUP BY status""")
for r in cur.fetchall(): print(f"  {r[0]}: {r[1]}")

# ISSUE-6: market_state_input
print("\n[ISSUE-6] market_state 覆盖:")
cur.execute("""SELECT state_date, market_state, degraded, reason FROM market_state_daily 
    WHERE state_date>='2026-04-07' ORDER BY state_date""")
for r in cur.fetchall(): print(f"  {dict(r)}")

# ISSUE-7: 孤儿 report_data_usage (无研报引用)
print("\n[ISSUE-7] report_data_usage 孤儿(无link引用):")
cur.execute("""SELECT count(*) FROM report_data_usage rdu
    WHERE trade_date>='2026-04-07' 
    AND NOT EXISTS (SELECT 1 FROM report_data_usage_link l WHERE l.usage_id=rdu.usage_id)""")
orphan = cur.fetchone()[0]
print(f"  orphan usage: {orphan}")

# ISSUE-8: user 表状态
print("\n[ISSUE-8] user 账号:")
cur.execute("SELECT count(*) FROM user")
print(f"  total users: {cur.fetchone()[0]}")
cur.execute("SELECT count(*) FROM user WHERE is_active=1")
print(f"  active: {cur.fetchone()[0]}")

# ISSUE-9: generation_task 状态
print("\n[ISSUE-9] generation_task 统计:")
cur.execute("""SELECT task_status, count(*) FROM generation_task 
    WHERE created_at>=date('now','-1 day') GROUP BY task_status""")
for r in cur.fetchall(): print(f"  {r[0]}: {r[1]}")

# ISSUE-10: data_batch_error
print("\n[ISSUE-10] data_batch_error 分布 (近24h):")
cur.execute("""SELECT error_type, count(*) FROM data_batch_error 
    WHERE created_at>=datetime('now','-1 day') GROUP BY error_type ORDER BY count(*) DESC""")
errs = cur.fetchall()
for r in errs[:10]: print(f"  {r[0]}: {r[1]}")
print(f"  total error types: {len(errs)}")

# ISSUE-11: settlement (日终) 覆盖
print("\n[ISSUE-11] settlement_daily 状态:")
cur.execute("""SELECT trade_date, status, count(*) FROM settlement_daily 
    WHERE trade_date>='2026-04-07' GROUP BY trade_date, status ORDER BY trade_date""")
for r in cur.fetchall(): print(f"  {r[0]} {r[1]}: {r[2]}")

# ISSUE-12: tables with no rows
print("\n[ISSUE-12] 主要表行数:")
tables = ['report','report_data_usage','kline_daily','stock_master',
          'data_batch','generation_task','settlement_daily','hotspot_top50_snapshot',
          'etf_flow_summary_snapshot','northbound_summary_snapshot','market_state_daily']
for t in tables:
    try:
        cur.execute(f"SELECT count(*) FROM {t}")
        n = cur.fetchone()[0]
        print(f"  {t}: {n}")
    except Exception as e:
        print(f"  {t}: ERR {e}")

c.close()
