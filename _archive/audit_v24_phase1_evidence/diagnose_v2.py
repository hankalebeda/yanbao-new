"""Phase 2+3: 诊断数据覆盖 + 提取核心股票池 + 批量生成计划"""
import sqlite3
import json
from datetime import date, timedelta

c = sqlite3.connect('data/app.db')
cur = c.cursor()

# 获取最新任务
print('=== 最新200股核心池来源 ===')
cur.execute("""
SELECT task_id, trade_date, status, pool_version, fallback_from, core_pool_size
FROM stock_pool_refresh_task
WHERE status IN ('COMPLETED', 'FALLBACK') AND core_pool_size >= 100
ORDER BY trade_date DESC LIMIT 5
""")
tasks = cur.fetchall()
for t in tasks:
    print(t)

latest_task_id = tasks[0][0] if tasks else None
print(f'\nUsing task_id: {latest_task_id}')

# 获取200股
cur.execute("""
SELECT stock_code FROM stock_pool_snapshot
WHERE refresh_task_id = ? AND pool_role = 'core'
ORDER BY rank_no
""", (latest_task_id,))
core_stocks = [r[0] for r in cur.fetchall()]
print(f'Core pool stocks: {len(core_stocks)}')
print(f'Sample: {core_stocks[:10]}')

# 检查kline覆盖
print('\n=== kline_daily 各日期覆盖（2026-04-01 至 2026-04-16）===')
cur.execute("""
SELECT trade_date, COUNT(DISTINCT stock_code) as stocks
FROM kline_daily WHERE trade_date >= '2026-04-01'
GROUP BY trade_date ORDER BY trade_date DESC
""")
kline_dates = cur.fetchall()
for d, s in kline_dates:
    print(f"  {d}: {s:5d} stocks")

# 检查200核心股各日期kline覆盖
if core_stocks:
    print('\n=== 200核心股各日期kline覆盖 ===')
    cur.execute("""
    SELECT trade_date, COUNT(DISTINCT stock_code) as covered
    FROM kline_daily
    WHERE trade_date >= '2026-04-01'
      AND stock_code IN ({})
    GROUP BY trade_date ORDER BY trade_date DESC
    """.format(','.join('?' * len(core_stocks))), core_stocks)
    pool_kline = cur.fetchall()
    for d, covered in pool_kline:
        pct = 100.0 * covered / len(core_stocks)
        status = '✅' if pct >= 90 else ('⚠️' if pct >= 50 else '❌')
        print(f"  {d}: {covered:3d}/{len(core_stocks)} = {pct:.0f}%  {status}")

# 输出JSON供生成脚本使用
out = {
    'core_stocks': core_stocks,
    'pool_size': len(core_stocks),
    'task_id': latest_task_id,
}
with open('_archive/audit_v24_phase1_evidence/core_pool.json', 'w') as f:
    json.dump(out, f, ensure_ascii=False, indent=2)
print(f'\nSaved to _archive/audit_v24_phase1_evidence/core_pool.json')

# 检查report_data_usage hotspot/northbound/etf_flow
print('\n=== report_data_usage 各dataset最近覆盖 ===')
cur.execute("""
SELECT dataset_name, trade_date, COUNT(*) as cnt, SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END) as ok_cnt
FROM report_data_usage
WHERE trade_date >= '2026-04-01'
GROUP BY dataset_name, trade_date
ORDER BY dataset_name, trade_date DESC
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} cnt={r[2]} ok={r[3]}")

# cleanup_task分析
print('\n=== cleanup_task ===')
cur.execute("PRAGMA table_info(cleanup_task)")
ctcols = [r[1] for r in cur.fetchall()]
print('cols:', ctcols)
cur.execute("SELECT * FROM cleanup_task ORDER BY created_at DESC LIMIT 4")
for r in cur.fetchall():
    print(r)
