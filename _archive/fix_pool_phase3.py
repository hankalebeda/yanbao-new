"""
完整修复脚本：
1. 用腾讯实时行情获取流通市值，计算并更新 stock_master.circulating_shares
2. 清理 2026-04-17 到 2026-04-24 的错误 task/snapshot 记录
3. 重新运行 refresh_stock_pool 重建 stock_pool_snapshot
"""
import sqlite3
import httpx
import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = 'data/app.db'

def tencent_sym(stock_code: str) -> str:
    code, exchange = stock_code.split('.')
    if exchange == 'SH':
        return f'sh{code}'
    else:
        return f'sz{code}'

def from_tencent_sym(sym: str) -> str:
    if sym.startswith('sh'):
        return f'{sym[2:]}.SH'
    else:
        return f'{sym[2:]}.SZ'

def fetch_rt_batch(sym_list: list) -> dict:
    """Fetch real-time quote, extract circulating_shares"""
    query = ','.join(sym_list)
    url = f'https://qt.gtimg.cn/q={query}'
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.qq.com'}
    with httpx.Client(timeout=20, trust_env=False) as c:
        r = c.get(url, headers=headers)
    result = {}
    for line in r.text.split('\n'):
        line = line.strip()
        if not line or '~' not in line:
            continue
        try:
            key_part, val_part = line.split('=', 1)
            sym = key_part.strip().replace('v_', '')
            val = val_part.strip().strip('"').strip(';')
            fields = val.split('~')
            if len(fields) < 45:
                continue
            close_price = float(fields[3]) if fields[3] else 0
            circ_mktcap_billion = float(fields[44]) if len(fields) > 44 and fields[44] else 0
            if close_price > 0 and circ_mktcap_billion > 0:
                # circulating_shares = 流通市值(亿元) * 1e8 / 收盘价(元)
                circ_shares = circ_mktcap_billion * 1e8 / close_price
                result[sym] = int(circ_shares)
        except Exception:
            pass
    return result

# ===== Step 1: Get all stocks needing circulating_shares =====
print("=== Step 1: 获取需要更新 circulating_shares 的股票 ===")
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Get stocks in kline_daily for target dates but without circulating_shares in stock_master
cur.execute("""
    SELECT DISTINCT k.stock_code
    FROM kline_daily k
    JOIN stock_master s ON s.stock_code = k.stock_code
    WHERE k.trade_date >= '2026-04-17'
    AND (s.circulating_shares IS NULL OR s.circulating_shares = 0)
""")
missing_shares_codes = [r[0] for r in cur.fetchall()]
print(f"需要补 circulating_shares 的股票: {len(missing_shares_codes)}")

# Also get all stock_master stocks with NULL circulating_shares
cur.execute("SELECT COUNT(*) FROM stock_master WHERE circulating_shares IS NULL OR circulating_shares = 0")
total_missing = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM stock_master WHERE circulating_shares IS NOT NULL AND circulating_shares > 0")
total_ok = cur.fetchone()[0]
print(f"stock_master circulating_shares: {total_ok} 有值, {total_missing} 为NULL")

conn.close()

# ===== Step 2: Fetch from Tencent in batches =====
print(f"\n=== Step 2: 从腾讯实时行情获取 circulating_shares ===")
syms = [tencent_sym(c) for c in missing_shares_codes]
batch_size = 100
all_results = {}

for i in range(0, len(syms), batch_size):
    batch = syms[i:i+batch_size]
    print(f"  Fetching batch {i//batch_size + 1}/{(len(syms)+batch_size-1)//batch_size}: {len(batch)} stocks...")
    try:
        res = fetch_rt_batch(batch)
        for sym, shares in res.items():
            stock_code = from_tencent_sym(sym)
            all_results[stock_code] = shares
        print(f"    Got {len(res)} results")
    except Exception as e:
        print(f"    ERROR: {e}")
    if i + batch_size < len(syms):
        time.sleep(0.5)

print(f"\n  共获取 {len(all_results)} 只股票的 circulating_shares")

# ===== Step 3: Update stock_master =====
print(f"\n=== Step 3: 更新 stock_master.circulating_shares ===")
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
updated = 0
for stock_code, shares in all_results.items():
    if shares > 0:
        cur.execute(
            "UPDATE stock_master SET circulating_shares = ? WHERE stock_code = ?",
            (shares, stock_code)
        )
        if cur.rowcount > 0:
            updated += 1
conn.commit()
print(f"  更新了 {updated} 只股票的 circulating_shares")

# Verify
cur.execute("SELECT COUNT(*) FROM stock_master WHERE circulating_shares IS NOT NULL AND circulating_shares > 0")
total_ok_after = cur.fetchone()[0]
print(f"  更新后有 circulating_shares 的股票: {total_ok_after}")
conn.close()

# ===== Step 4: Clean up bad task entries =====
print(f"\n=== Step 4: 清理错误的 task/snapshot 记录 ===")
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

target_dates = ['2026-04-24', '2026-04-23', '2026-04-22', '2026-04-21', '2026-04-20', '2026-04-17']

for dt in target_dates:
    # Delete stock_score entries for this date
    cur.execute("DELETE FROM stock_score WHERE pool_date = ?", (dt,))
    del_score = cur.rowcount
    if del_score > 0:
        print(f"  {dt}: deleted {del_score} stock_score rows")

    # Check existing task
    cur.execute("SELECT task_id, status, core_pool_size FROM stock_pool_refresh_task WHERE trade_date = ?", (dt,))
    tasks = cur.fetchall()
    if tasks:
        print(f"  {dt}: 找到 {len(tasks)} 个 task - {tasks}")
        # Delete snapshot entries for these task_ids
        for task in tasks:
            task_id = task[0]
            cur.execute("DELETE FROM stock_pool_snapshot WHERE refresh_task_id = ?", (task_id,))
            del_snap = cur.rowcount
            print(f"    deleted {del_snap} snapshot rows for task {task_id[:8]}...")
        # Delete the tasks
        cur.execute("DELETE FROM stock_pool_refresh_task WHERE trade_date = ?", (dt,))
        print(f"    deleted {cur.rowcount} task rows for {dt}")

conn.commit()
conn.close()

# ===== Step 5: Verify candidate count before rebuilding =====
print(f"\n=== Step 5: 验证候选股票数量 ===")
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
for dt in ['2026-04-23', '2026-04-22', '2026-04-21', '2026-04-20', '2026-04-17']:
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
    print(f"  {dt}: {cnt} 只股票通过全部过滤")
conn.close()

# ===== Step 6: Rebuild stock_pool_snapshot =====
print(f"\n=== Step 6: 重建 stock_pool_snapshot ===")
from app.core.db import SessionLocal, Base, engine
import app.models
Base.metadata.create_all(bind=engine)
from app.services.stock_pool import refresh_stock_pool
from datetime import date

rebuild_dates = [
    date(2026, 4, 17),
    date(2026, 4, 20),
    date(2026, 4, 21),
    date(2026, 4, 22),
    date(2026, 4, 23),
    date(2026, 4, 24),
]

summary = {}
for target_date in rebuild_dates:
    print(f"\n  重建 {target_date}...")
    db = SessionLocal()
    try:
        result = refresh_stock_pool(db, target_date, force_rebuild=True)
        # refresh_stock_pool returns a dict
        status = result.get('status', 'UNKNOWN')
        core_size = result.get('core_pool_size', 0)
        standby_size = result.get('standby_pool_size', 0)
        reason = result.get('status_reason', 'N/A')
        summary[str(target_date)] = f"{status} (core={core_size}, standby={standby_size}, reason={reason})"
        print(f"    Status: {status}, Core: {core_size}, Standby: {standby_size}, Reason: {reason}")
    except Exception as e:
        summary[str(target_date)] = f"FAILED - {e}"
        print(f"    ERROR: {e}")
    finally:
        db.close()

print(f"\n=== 汇总 ===")
for dt, result in summary.items():
    print(f"  {dt}: {result}")
