"""
修复 2026-04-08 ~ 2026-04-15 的 stock_pool_snapshot 缺口：
- 2026-04-08, 2026-04-09: 完全缺失 pool task
- 2026-04-10: COMPLETED 但只有 3 股
- 2026-04-13: COMPLETED 但只有 35 股
- 2026-04-14, 2026-04-15: COMPLETED 但只有 6 股

均重建为 FALLBACK（candidates < 200），继承最近有效 pool。
"""
import sqlite3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = 'data/app.db'

# Dates that have bad/missing pool tasks
BAD_DATES = ['2026-04-10', '2026-04-13', '2026-04-14', '2026-04-15']
MISSING_DATES = ['2026-04-08', '2026-04-09']
ALL_DATES = sorted(BAD_DATES + MISSING_DATES)

print("=== Step 1: 检查当前状态 ===")
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Check current pool tasks for these dates
cur.execute("""
    SELECT t.trade_date, t.status, t.core_pool_size, COUNT(s.stock_code) as snap_count
    FROM stock_pool_refresh_task t
    LEFT JOIN stock_pool_snapshot s ON s.refresh_task_id = t.task_id
    WHERE t.trade_date BETWEEN '2026-04-08' AND '2026-04-15'
    GROUP BY t.task_id, t.trade_date, t.status
    ORDER BY t.trade_date
""")
rows = cur.fetchall()
print(f"当前 2026-04-08 to 2026-04-15 pool tasks:")
for r in rows:
    print(f"  {r}")

# Check kline coverage for these dates
print("\nkline_daily 覆盖情况:")
for dt in ALL_DATES:
    cur.execute("SELECT COUNT(*) FROM kline_daily WHERE trade_date = ?", (dt,))
    cnt = cur.fetchone()[0]
    print(f"  {dt}: {cnt} 只")

# Check candidate count
print("\n候选股票数量 (经过所有过滤):")
for dt in ALL_DATES:
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT k.stock_code
            FROM kline_daily k
            JOIN stock_master s ON s.stock_code = k.stock_code
            WHERE k.trade_date = ?
            AND s.circulating_shares IS NOT NULL AND s.circulating_shares > 0
            AND k.close > 0
            AND k.amount > 0
            AND k.close * s.circulating_shares >= 5000000000
            AND k.amount >= 30000000
            AND s.list_date <= date(?, '-365 days')
            AND (s.is_st IS NULL OR s.is_st = 0)
            AND (s.is_delisted IS NULL OR s.is_delisted = 0)
        )
    """, (dt, dt))
    cnt = cur.fetchone()[0]
    print(f"  {dt}: {cnt} 只候选")

conn.close()

print("\n=== Step 2: 清理 BAD_DATES 的错误 task/snapshot/score ===")
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

for dt in BAD_DATES:
    # Delete stock_score
    cur.execute("DELETE FROM stock_score WHERE pool_date = ?", (dt,))
    del_score = cur.rowcount
    if del_score > 0:
        print(f"  {dt}: deleted {del_score} stock_score rows")

    # Find and delete snapshots + tasks
    cur.execute("SELECT task_id, status, core_pool_size FROM stock_pool_refresh_task WHERE trade_date = ?", (dt,))
    tasks = cur.fetchall()
    if tasks:
        for task in tasks:
            task_id = task[0]
            cur.execute("DELETE FROM stock_pool_snapshot WHERE refresh_task_id = ?", (task_id,))
            del_snap = cur.rowcount
            print(f"  {dt}: deleted {del_snap} snapshot rows for task {task_id[:8]}...")
        cur.execute("DELETE FROM stock_pool_refresh_task WHERE trade_date = ?", (dt,))
        print(f"  {dt}: deleted {cur.rowcount} task rows")
    else:
        print(f"  {dt}: no task found (expected for BAD_DATES, please re-check)")

conn.commit()
conn.close()

print("\n=== Step 3: 重建所有 6 个日期的 stock_pool_snapshot ===")
from app.core.db import SessionLocal, Base, engine
import app.models
Base.metadata.create_all(bind=engine)
from app.services.stock_pool import refresh_stock_pool
from datetime import date

rebuild_dates = [
    date(2026, 4, 8),
    date(2026, 4, 9),
    date(2026, 4, 10),
    date(2026, 4, 13),
    date(2026, 4, 14),
    date(2026, 4, 15),
]

summary = {}
for target_date in rebuild_dates:
    print(f"\n  重建 {target_date}...")
    db = SessionLocal()
    try:
        result = refresh_stock_pool(db, target_date, force_rebuild=True)
        status = result.get('status', 'UNKNOWN')
        core_size = result.get('core_pool_size', 0)
        standby_size = result.get('standby_pool_size', 0)
        reason = result.get('status_reason', 'N/A')
        fallback_from = result.get('fallback_from', None)
        summary[str(target_date)] = f"{status} (core={core_size}, standby={standby_size}, reason={reason}, fallback_from={fallback_from})"
        print(f"    Status: {status}, Core: {core_size}, Standby: {standby_size}, Reason: {reason}")
    except Exception as e:
        import traceback
        summary[str(target_date)] = f"FAILED - {e}"
        print(f"    ERROR: {e}")
        traceback.print_exc()
    finally:
        db.close()

print("\n=== 汇总 ===")
for dt, result in summary.items():
    print(f"  {dt}: {result}")

print("\n=== Step 4: 最终验证 ===")
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("""
    SELECT t.trade_date, t.status, t.core_pool_size, t.standby_pool_size, COUNT(s.stock_code) as snap_count
    FROM stock_pool_refresh_task t
    LEFT JOIN stock_pool_snapshot s ON s.refresh_task_id = t.task_id
    WHERE t.trade_date BETWEEN '2026-04-07' AND '2026-04-16'
    GROUP BY t.task_id, t.trade_date, t.status
    ORDER BY t.trade_date
""")
for r in cur.fetchall():
    print(f"  {r}")
conn.close()
