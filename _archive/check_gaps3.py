import sqlite3
from datetime import date, timedelta

conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

# stock_profile missing 的股票
print('=== stock_profile missing 的具体股票 ===')
cur.execute("""
    SELECT rdu.stock_code, rdu.trade_date, rdu.created_at, rdu.status_reason
    FROM report_data_usage rdu
    WHERE rdu.dataset_name = 'stock_profile' AND rdu.status = 'missing'
    ORDER BY rdu.created_at DESC
""")
for r in cur.fetchall():
    print(r)

# margin_financing missing
print()
print('=== margin_financing missing ===')
cur.execute("""
    SELECT rdu.stock_code, rdu.trade_date, rdu.created_at
    FROM report_data_usage rdu
    WHERE rdu.dataset_name = 'margin_financing' AND rdu.status = 'missing'
    ORDER BY rdu.created_at DESC
""")
for r in cur.fetchall():
    print(r)

# northbound_summary missing
print()
print('=== northbound_summary missing ===')
cur.execute("""
    SELECT rdu.stock_code, rdu.trade_date, rdu.created_at
    FROM report_data_usage rdu
    WHERE rdu.dataset_name = 'northbound_summary' AND rdu.status = 'missing'
    ORDER BY rdu.created_at DESC
""")
for r in cur.fetchall():
    print(r)

# etf_flow_summary missing
print()
print('=== etf_flow_summary missing ===')
cur.execute("""
    SELECT rdu.stock_code, rdu.trade_date, rdu.created_at
    FROM report_data_usage rdu
    WHERE rdu.dataset_name = 'etf_flow_summary' AND rdu.status = 'missing'
    ORDER BY rdu.created_at DESC
""")
for r in cur.fetchall():
    print(r)

# 检查最新的 kline_daily 股票中，哪些股票在 2026-04-16 有数据但在 2026-04-24 没有
print()
print('=== 2026-04-16有数据但2026-04-24缺失的股票（取前30）===')
cur.execute("SELECT DISTINCT stock_code FROM kline_daily WHERE trade_date='2026-04-16' LIMIT 30")
stocks_0416 = set(r[0] for r in cur.fetchall())
cur.execute("SELECT DISTINCT stock_code FROM kline_daily WHERE trade_date='2026-04-24'")
stocks_0424 = set(r[0] for r in cur.fetchall())
missing_in_0424 = stocks_0416 - stocks_0424
print(f'0416有数据但0424没有: {len(missing_in_0424)} 只')
print(list(missing_in_0424)[:20])

# 2026-04-24 stock_pool_snapshot 的 8 只股票
print()
print('=== 2026-04-24 stock_pool_snapshot ===')
cur.execute("SELECT stock_code FROM stock_pool_snapshot WHERE trade_date='2026-04-24'")
pool0424 = [r[0] for r in cur.fetchall()]
print(pool0424)

# 检查这8只股票是否在 kline_daily 有数据
print()
print('=== 这8只股票在 kline_daily 最新数据 ===')
for sc in pool0424:
    cur.execute("SELECT trade_date FROM kline_daily WHERE stock_code=? ORDER BY trade_date DESC LIMIT 3", (sc,))
    dates = [r[0] for r in cur.fetchall()]
    print(f'  {sc}: {dates}')

conn.close()
