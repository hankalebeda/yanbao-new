import sqlite3
from datetime import datetime, date, timedelta

conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

today = date.today().isoformat()
print(f'当前日期: {today}')
print()

# 1. stock_pool 状态
print('=== stock_pool ===')
cur.execute('SELECT COUNT(*) FROM stock_pool')
print('总数: ' + str(cur.fetchone()[0]))

# 2. stock_pool_snapshot 最新日期
print()
print('=== stock_pool_snapshot 最新状态 ===')
cur.execute('SELECT trade_date, COUNT(*) as cnt FROM stock_pool_snapshot GROUP BY trade_date ORDER BY trade_date DESC LIMIT 10')
for r in cur.fetchall():
    print(f'  {r[0]}: {r[1]} 条')

# 3. kline_daily 最新状态
print()
print('=== kline_daily 最新日期 ===')
cur.execute('SELECT trade_date, COUNT(*) as cnt FROM kline_daily GROUP BY trade_date ORDER BY trade_date DESC LIMIT 10')
for r in cur.fetchall():
    print(f'  {r[0]}: {r[1]} 条')

# 4. hotspot相关表
print()
print('=== hotspot 表状态 ===')
for t in ['hotspot_raw', 'hotspot_normalized', 'hotspot_top50', 'hotspot_stock_link',
          'market_hotspot_item', 'market_hotspot_item_source', 'market_hotspot_item_stock_link']:
    cur.execute('SELECT COUNT(*) FROM "' + t + '"')
    cnt = cur.fetchone()[0]
    print(f'  {t}: {cnt}')

# 5. report_data_usage 缺口分析
print()
print('=== report_data_usage 数据集状态（最近30天）===')
cutoff = (date.today() - timedelta(days=30)).isoformat()
cur.execute("""
    SELECT dataset_name, status, COUNT(*) as cnt 
    FROM report_data_usage 
    WHERE created_at >= ? 
    GROUP BY dataset_name, status 
    ORDER BY dataset_name, status
""", (cutoff,))
for r in cur.fetchall():
    print(f'  {r[0]} | {r[1]}: {r[2]}')

conn.close()
