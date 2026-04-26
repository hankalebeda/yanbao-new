"""最终数据缺口检查"""
import sqlite3
from datetime import date, timedelta

conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

print("=" * 60)
print("最终数据缺口检查")
print("=" * 60)

# 1. kline_daily coverage
print("\n1. kline_daily 最近交易日覆盖:")
cur.execute("SELECT trade_date, COUNT(*) FROM kline_daily WHERE trade_date >= '2026-04-15' GROUP BY trade_date ORDER BY trade_date")
for r in cur.fetchall():
    print(f"   {r[0]}: {r[1]} 只")

# 2. stock_pool_snapshot coverage
print("\n2. stock_pool_snapshot 覆盖:")
cur.execute("""
    SELECT sps.trade_date, t.status, COUNT(sps.stock_code)
    FROM stock_pool_snapshot sps
    JOIN stock_pool_refresh_task t ON t.task_id = sps.refresh_task_id
    WHERE sps.trade_date >= '2026-04-15'
    GROUP BY sps.trade_date, t.status
    ORDER BY sps.trade_date
""")
for r in cur.fetchall():
    print(f"   {r[0]}: {r[1]}, {r[2]} 只")

# 3. market_state_cache
print("\n3. market_state_cache 覆盖:")
cur.execute("SELECT trade_date, market_state, cache_status FROM market_state_cache WHERE trade_date >= '2026-04-15' ORDER BY trade_date")
for r in cur.fetchall():
    print(f"   {r[0]}: {r[1]}, {r[2]}")

# 4. market_hotspot_item
print("\n4. market_hotspot_item 热点数据:")
cur.execute("SELECT COUNT(*), MAX(fetch_time) FROM market_hotspot_item WHERE fetch_time > datetime('now', '-48 hours')")
r = cur.fetchone()
print(f"   最近48小时内: {r[0]} 条, 最新={r[1]}")
cur.execute("SELECT COUNT(*) FROM market_hotspot_item_stock_link WHERE hotspot_item_id IN (SELECT hotspot_item_id FROM market_hotspot_item WHERE fetch_time > datetime('now', '-48 hours'))")
print(f"   关联股票链接: {cur.fetchone()[0]} 条")

# 5. data_batch recent success
print("\n5. data_batch 最近数据批次 (>= 2026-04-20):")
cur.execute("""
    SELECT source_name, MAX(trade_date), 
           SUM(CASE WHEN batch_status='SUCCESS' THEN 1 ELSE 0 END),
           SUM(CASE WHEN batch_status='FAILED' THEN 1 ELSE 0 END)
    FROM data_batch 
    WHERE trade_date >= '2026-04-20'
    GROUP BY source_name
    ORDER BY MAX(trade_date) DESC, source_name
""")
for r in cur.fetchall():
    print(f"   {r[0]}: last={r[1]}, success={r[2]}, failed={r[3]}")

# 6. Summary of report_data_usage for recent dates
print("\n6. report_data_usage 覆盖情况:")
cur.execute("""
    SELECT trade_date, COUNT(DISTINCT stock_code), COUNT(DISTINCT dataset_name)
    FROM report_data_usage
    WHERE trade_date >= '2026-04-15'
    GROUP BY trade_date
    ORDER BY trade_date
""")
for r in cur.fetchall():
    print(f"   {r[0]}: {r[1]} 只股票, {r[2]} 个数据集")

# 7. capital_cache files
import os
cache_dir = 'data/capital_cache'
if os.path.exists(cache_dir):
    files = os.listdir(cache_dir)
    print(f"\n7. capital_cache 文件: {len(files)} 个")
    # Check latest files
    files_sorted = sorted(files)[-5:]
    print(f"   最新5个: {files_sorted}")
else:
    print("\n7. capital_cache: 目录不存在")

conn.close()
print("\n" + "=" * 60)
print("检查完成")
