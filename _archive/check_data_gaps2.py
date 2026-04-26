import sqlite3
from datetime import datetime, date, timedelta

conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

today = date.today().isoformat()
print(f'当前日期: {today}')

# 最近交易日的kline覆盖情况
print()
print('=== 近期 kline_daily 覆盖的股票 ===')
for d in ['2026-04-24', '2026-04-23', '2026-04-22', '2026-04-21', '2026-04-17', '2026-04-16']:
    cur.execute("SELECT stock_code FROM kline_daily WHERE trade_date=?", (d,))
    codes = [r[0] for r in cur.fetchall()]
    print(f'  {d}: {len(codes)} 只 - {codes[:20]}')

# 哪些股票 stock_pool_snapshot 有 2026-04-24 数据
print()
print('=== stock_pool_snapshot 2026-04-24 覆盖情况 ===')
cur.execute("SELECT stock_code, created_at FROM stock_pool_snapshot WHERE trade_date='2026-04-24'")
rows = cur.fetchall()
for r in rows:
    print(f'  {r[0]}: {r[1]}')

# stock_pool_snapshot 2026-04-16 有多少
print()
print('=== stock_pool_snapshot 2026-04-16 覆盖 (前20) ===')
cur.execute("SELECT stock_code FROM stock_pool_snapshot WHERE trade_date='2026-04-16' LIMIT 20")
codes = [r[0] for r in cur.fetchall()]
print(codes)

# market_hotspot_item 最新情况
print()
print('=== market_hotspot_item 最新 ===')
cur.execute("SELECT fetch_date, COUNT(*) FROM market_hotspot_item GROUP BY fetch_date ORDER BY fetch_date DESC LIMIT 10")
for r in cur.fetchall():
    print(f'  {r[0]}: {r[1]}')

# report_data_usage missing 的具体情况
print()
print('=== report_data_usage missing 明细（最近7天）===')
cutoff7 = (date.today() - timedelta(days=7)).isoformat()
cur.execute("""
    SELECT rdu.stock_code, rdu.dataset_name, rdu.status, rdu.trade_date, rdu.created_at
    FROM report_data_usage rdu
    WHERE rdu.status = 'missing' AND rdu.created_at >= ?
    ORDER BY rdu.created_at DESC
    LIMIT 50
""", (cutoff7,))
for r in cur.fetchall():
    print(f'  {r[0]} | {r[1]} | {r[2]} | trade:{r[3]} | created:{r[4]}')

# 了解 stock_profile 缺口
print()
print('=== stock_profile 状态 ===')
cur.execute("""
    SELECT rdu.stock_code, rdu.status, rdu.trade_date, rdu.created_at
    FROM report_data_usage rdu
    WHERE rdu.dataset_name = 'stock_profile'
    ORDER BY rdu.created_at DESC
    LIMIT 20
""")
for r in cur.fetchall():
    print(f'  {r[0]} | {r[1]} | trade:{r[2]} | {r[3]}')

conn.close()
