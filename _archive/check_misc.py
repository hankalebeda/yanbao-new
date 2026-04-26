import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

# data_batch_error 结构
print('=== data_batch_error 结构 ===')
cur.execute('PRAGMA table_info("data_batch_error")')
for c in cur.fetchall():
    print(c)

# data_batch_error 最新错误
print()
print('=== data_batch_error 最新10条 ===')
cur.execute('SELECT * FROM data_batch_error ORDER BY rowid DESC LIMIT 10')
for r in cur.fetchall():
    print(r)

# 了解stock_master的数量 - 核心股票池
print()
print('=== stock_master 样本 ===')
cur.execute('SELECT COUNT(*) FROM stock_master')
print('total:', cur.fetchone()[0])
cur.execute('SELECT stock_code, stock_name, exchange FROM stock_master LIMIT 20')
for r in cur.fetchall():
    print(r)

# 了解 stock_score 最新状态
print()
print('=== stock_score 最新 ===')
cur.execute('PRAGMA table_info("stock_score")')
cols = [c[1] for c in cur.fetchall()]
print('columns:', cols)
cur.execute('SELECT * FROM stock_score ORDER BY rowid DESC LIMIT 5')
for r in cur.fetchall():
    print(r)

conn.close()
