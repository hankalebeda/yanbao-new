"""
Phase 2+3: 诊断数据覆盖 + 批量生成研报
1. 获取200股核心池
2. 检查各日期kline覆盖情况
3. 输出可生成日期与受阻日期
"""
import sqlite3
import json
from datetime import date, timedelta

c = sqlite3.connect('data/app.db')
cur = c.cursor()

# 获取最新的200股核心池（从stock_pool_refresh_task的FALLBACK任务）
print('=== 最新200股核心池来源 ===')
cur.execute("""
SELECT task_id, trade_date, status, pool_version, fallback_from, core_pool_size, standby_pool_size
FROM stock_pool_refresh_task
WHERE status IN ('COMPLETED', 'FALLBACK') AND core_pool_size >= 100
ORDER BY trade_date DESC LIMIT 5
""")
tasks = cur.fetchall()
for t in tasks:
    print(t)

# 获取最近的FALLBACK任务中的200股
print('\n=== 从stock_pool_snapshot获取200股核心池 ===')
# stock_pool_snapshot stores one row per stock per task
cur.execute("""
SELECT task_id FROM stock_pool_refresh_task
WHERE status IN ('COMPLETED', 'FALLBACK') AND core_pool_size >= 100
ORDER BY trade_date DESC LIMIT 1
""")
latest_task = cur.fetchone()
if latest_task:
    task_id = latest_task[0]
    print(f'Using task_id: {task_id}')
    cur.execute("PRAGMA table_info(stock_pool_snapshot)")
    snap_cols = [r[1] for r in cur.fetchall()]
    print('snapshot cols:', snap_cols)
    cur.execute("SELECT COUNT(*) FROM stock_pool_snapshot WHERE task_id = ?", (task_id,))
    print('snapshot count for this task:', cur.fetchone()[0])
    cur.execute("SELECT stock_code FROM stock_pool_snapshot WHERE task_id = ? LIMIT 5", (task_id,))
    print('sample stocks:', cur.fetchall())
else:
    print('No suitable pool task found')

# 检查各日期kline覆盖
print('\n=== kline_daily 各日期覆盖（2026-04-01 至 2026-04-16）===')
cur.execute("""
SELECT trade_date, COUNT(DISTINCT stock_code) as stocks
FROM kline_daily
WHERE trade_date >= '2026-04-01'
GROUP BY trade_date ORDER BY trade_date DESC
""")
kline_dates = cur.fetchall()
for d, s in kline_dates:
    bar = '█' * min(50, s // 14)
    print(f"  {d}: {s:5d} stocks  {bar}")

# 检查report_data_usage中hotspot/northbound/etf_flow最近覆盖
print('\n=== report_data_usage 最近各dataset覆盖 ===')
cur.execute("""
SELECT dataset_name, trade_date, COUNT(*) as cnt
FROM report_data_usage
WHERE trade_date >= '2026-04-01'
GROUP BY dataset_name, trade_date
ORDER BY dataset_name, trade_date DESC
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} = {r[2]}")

# 检查market_state_cache
print('\n=== market_state_cache 最近 ===')
cur.execute("SELECT trade_date, market_state FROM market_state_cache WHERE trade_date >= '2026-04-01' ORDER BY trade_date DESC LIMIT 10")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")

# 查看cleanup_task内容
print('\n=== cleanup_task 最近4条 ===')
cur.execute("PRAGMA table_info(cleanup_task)")
ctcols = [r[1] for r in cur.fetchall()]
print('cols:', ctcols)
cur.execute("SELECT * FROM cleanup_task ORDER BY created_at DESC LIMIT 4")
for r in cur.fetchall():
    print(r)
