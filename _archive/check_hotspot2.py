import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

# hotspot_top50 结构
print('=== hotspot_top50 结构 ===')
cur.execute('PRAGMA table_info("hotspot_top50")')
for c in cur.fetchall():
    print(c)

# market_hotspot_item 最近日期
print()
print('=== market_hotspot_item 最近10条 ===')
cur.execute('SELECT fetch_time, source_name, merged_rank, topic_title, quality_flag FROM market_hotspot_item ORDER BY fetch_time DESC LIMIT 10')
for r in cur.fetchall():
    print(r)

# market_state_cache 最新状态
print()
print('=== market_state_cache 最新 ===')
cur.execute('PRAGMA table_info("market_state_cache")')
cols = [c[1] for c in cur.fetchall()]
print('columns:', cols)
cur.execute('SELECT * FROM market_state_cache ORDER BY created_at DESC LIMIT 5')
for r in cur.fetchall():
    print(r)

# data_batch_error 最新错误类型
print()
print('=== data_batch_error 最近错误类型统计 ===')
cur.execute("""
    SELECT error_type, COUNT(*) as cnt 
    FROM data_batch_error 
    GROUP BY error_type 
    ORDER BY cnt DESC 
    LIMIT 20
""")
for r in cur.fetchall():
    print(f'  {r[0]}: {r[1]}')

conn.close()
