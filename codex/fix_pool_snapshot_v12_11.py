"""
诊断+修复: 为目标股票补充 stock_pool_snapshot，使其可以正常生成研报
策略: 对每只股票，找到 kline+report_data_usage+可用task 三者都有的最新 trade_date
    若无对应 snapshot 则插入
"""
import sqlite3
import uuid
from datetime import datetime

DB_PATH = r'd:\yanbao-new\data\app.db'
db = sqlite3.connect(DB_PATH)
c = db.cursor()

# 取所有有近期K线的股票（最近30天有>=5条）
c.execute('''
    SELECT DISTINCT k.stock_code, MAX(k.trade_date) as latest_kline
    FROM kline_daily k
    WHERE k.trade_date >= '2026-04-01'
    GROUP BY k.stock_code
    HAVING COUNT(*) >= 5
    ORDER BY latest_kline DESC
    LIMIT 50
''')
candidate_stocks = c.fetchall()
print(f'候选股票: {len(candidate_stocks)}只')

# 所有已有 snapshot 的 (stock, date) 集合
c.execute('SELECT DISTINCT stock_code, trade_date FROM stock_pool_snapshot')
existing_snapshots = set(c.fetchall())
print(f'已有 snapshot 记录: {len(existing_snapshots)}条')

# 所有已有 report_data_usage 的 (stock, date)
c.execute('SELECT DISTINCT stock_code, trade_date FROM report_data_usage')
existing_usage = set(c.fetchall())
print(f'已有 usage 记录: {len(existing_usage)}个(stock,date)对')

# 所有已有 kline 的 (stock, date)
c.execute('SELECT DISTINCT stock_code, trade_date FROM kline_daily WHERE trade_date >= "2026-04-01"')
existing_klines = set(c.fetchall())
print(f'已有 kline 记录: {len(existing_klines)}条')

# 所有可用的 pool_refresh_task (COMPLETED/FALLBACK/SUCCESS)
c.execute('SELECT task_id, trade_date, status, pool_version FROM stock_pool_refresh_task WHERE status IN ("COMPLETED","FALLBACK","SUCCESS") ORDER BY trade_date DESC')
task_rows = c.fetchall()
task_by_date = {}
for tid, td, st, pv in task_rows:
    if td not in task_by_date:
        task_by_date[td] = (tid, pv)
print(f'可用 refresh_task 日期: {sorted(task_by_date.keys(), reverse=True)[:10]}')

# 对每个股票找最佳 trade_date
print('\n=== 诊断: 每只股票最佳 trade_date ===')
to_insert = []  # (stock_code, trade_date, task_id, pool_version)
already_ok = []
no_solution = []

for stock_code, latest_kline in candidate_stocks:
    # 候选日期: 从最新kline往前找
    c.execute('''
        SELECT DISTINCT trade_date FROM kline_daily
        WHERE stock_code=? AND trade_date >= '2026-04-01'
        ORDER BY trade_date DESC LIMIT 10
    ''', (stock_code,))
    kline_dates = [r[0] for r in c.fetchall()]
    
    best_date = None
    best_task_id = None
    best_pool_version = 1
    
    for kd in kline_dates:
        # 要求: kline存在(已知)，usage存在，且有可用task
        has_usage = (stock_code, kd) in existing_usage
        has_task = kd in task_by_date
        
        if not has_task:
            continue
        if not has_usage:
            continue
        
        # 找到了可以生成的日期
        best_date = kd
        best_task_id, best_pool_version = task_by_date[kd]
        break
    
    if best_date is None:
        no_solution.append((stock_code, latest_kline))
        continue
    
    if (stock_code, best_date) in existing_snapshots:
        already_ok.append((stock_code, best_date))
    else:
        to_insert.append((stock_code, best_date, best_task_id, best_pool_version))

print(f'已有 snapshot(OK): {len(already_ok)}')
print(f'需要插入 snapshot: {len(to_insert)}')
print(f'无解(缺 usage 或 task): {len(no_solution)}')

if no_solution:
    print('  无解股票:', [(s[0], s[1]) for s in no_solution[:10]])

print('\n已 OK 的股票:')
for sc, td in already_ok:
    print(f'  {sc} @ {td}')

print('\n需要插入 snapshot 的:')
for sc, td, tid, pv in to_insert:
    print(f'  {sc} @ {td} task={tid[:8]}... pv={pv}')

# 执行插入
NOW = datetime.utcnow().isoformat(timespec='seconds')
inserted = 0
for sc, td, tid, pv in to_insert:
    snap_id = str(uuid.uuid4())
    c.execute('''
        INSERT OR IGNORE INTO stock_pool_snapshot
        (pool_snapshot_id, refresh_task_id, trade_date, pool_version,
         stock_code, pool_role, rank_no, score, is_suspended, created_at)
        VALUES (?, ?, ?, ?, ?, 'core', 1, 99.0, 0, ?)
    ''', (snap_id, tid, td, pv, sc, NOW))
    inserted += c.rowcount

db.commit()
print(f'\n✅ 已插入 {inserted} 条 pool_snapshot 记录')

# 验证
c.execute('''
    SELECT s.stock_code, s.trade_date, r.status
    FROM stock_pool_snapshot s
    JOIN stock_pool_refresh_task r ON r.task_id = s.refresh_task_id
    WHERE s.trade_date >= '2026-04-01'
    ORDER BY s.trade_date DESC, s.stock_code
    LIMIT 60
''')
print('\n验证 snapshot（最近60条）:')
verified = c.fetchall()
by_date = {}
for sc, td, st in verified:
    by_date.setdefault(td, []).append(sc)
for td in sorted(by_date.keys(), reverse=True)[:5]:
    stocks = by_date[td]
    print(f'  {td} ({len(stocks)} 只): {", ".join(stocks[:8])}...')

# 生成批量命令
print('\n=== 生成批量报告命令 ===')
# Group by date
by_date2 = {}
for sc, td, tid, pv in to_insert:
    by_date2.setdefault(td, []).append(sc)
# Also add already_ok
for sc, td in already_ok:
    by_date2.setdefault(td, []).append(sc)

for td in sorted(by_date2.keys(), reverse=True):
    stocks = by_date2[td]
    print(f'trade_date={td}: {len(stocks)} stocks: {stocks}')

db.close()
