"""
Phase 2 综合修复：
1) 核查并剔除 3 只退市/停牌股 (002013.SZ / 600837.SH / 601989.SH) —— 从 core_pool.json、stock_pool_refresh_task.core_pool 等入口删除
2) 物理清理所有 is_deleted=1 研报 及其 citation / usage_link / instruction_card
3) 将历史 eastmoney empty_history 的 usage 降级记录按规则补写 status_reason='source_blocked_eastmoney_kline'（不修改，仅审计报告输出）
4) 输出清理后的状态快照
"""
from __future__ import annotations
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import time

DB = Path('data/app.db')
CORE_POOL = Path('_archive/audit_v24_phase1_evidence/core_pool.json')
DEAD_STOCKS = ['002013.SZ', '600837.SH', '601989.SH']

now_iso = datetime.now(timezone.utc).isoformat(timespec='seconds')
print(f'[run] {now_iso}')

import time

conn = sqlite3.connect(DB, timeout=120, isolation_level=None)
conn.execute('PRAGMA busy_timeout=120000')
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA foreign_keys=ON')
cur = conn.cursor()


def exec_retry(sql: str, params=(), *, attempts: int = 10, sleep_sec: float = 3.0) -> bool:
    """Execute SQL with retry on transient sqlite write lock."""
    for i in range(attempts):
        try:
            cur.execute(sql, params)
            return True
        except sqlite3.OperationalError as e:
            if 'locked' not in str(e).lower() and 'busy' not in str(e).lower():
                raise
            print(f'  [retry {i+1}/{attempts}] {e}; sleep {sleep_sec}s')
            time.sleep(sleep_sec)
    return False


def exec_retry(sql: str, params=(), *, attempts: int = 10, sleep_sec: float = 3.0):
    for i in range(attempts):
        try:
            cur.execute(sql, params)
            return
        except sqlite3.OperationalError as e:
            if 'locked' not in str(e).lower() and 'busy' not in str(e).lower():
                raise
            print(f'  [retry {i+1}/{attempts}] {e}; sleep {sleep_sec}s')
            time.sleep(sleep_sec)
    raise RuntimeError(f'failed after {attempts} retries: {sql[:80]}')


def commit_retry(attempts: int = 10, sleep_sec: float = 3.0):
    for i in range(attempts):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if 'locked' not in str(e).lower() and 'busy' not in str(e).lower():
                raise
            print(f'  [commit-retry {i+1}/{attempts}] {e}; sleep {sleep_sec}s')
            time.sleep(sleep_sec)
    raise RuntimeError('commit failed')

# --- 1) 确认 3 只退市股没有任何 kline_daily 记录 ---
print('\n[1] 核查 DEAD_STOCKS 无 kline_daily 数据')
for s in DEAD_STOCKS:
    cur.execute("SELECT COUNT(*), MAX(trade_date) FROM kline_daily WHERE stock_code=?", (s,))
    c, last = cur.fetchone()
    print(f'  {s}: rows={c}, last_trade_date={last}')

# --- 2) 从 core_pool.json 剔除 ---
print('\n[2] 更新 core_pool.json')
payload = json.loads(CORE_POOL.read_text(encoding='utf-8'))
before = len(payload.get('core_stocks', []))
payload['core_stocks'] = [s for s in payload['core_stocks'] if s not in DEAD_STOCKS]
payload.setdefault('excluded_stocks', [])
for s in DEAD_STOCKS:
    if s not in payload['excluded_stocks']:
        payload['excluded_stocks'].append({'stock_code': s, 'reason': 'suspended_no_kline_window', 'excluded_at': now_iso})
payload['pool_size'] = len(payload['core_stocks'])
CORE_POOL.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')
print(f'  core_stocks: {before} -> {payload["pool_size"]}')

# --- 3) 同步到 stock_pool_refresh_task.evicted_stocks_json（仅加标记，不破坏 pool_version） ---
print('\n[3] 标记 stock_pool_refresh_task 退市')
cur.execute("SELECT task_id, trade_date, evicted_stocks_json FROM stock_pool_refresh_task WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16'")
rows = cur.fetchall()
for task_id, td, evicted in rows:
    try:
        lst = json.loads(evicted) if evicted else []
    except Exception:
        lst = []
    if not isinstance(lst, list):
        lst = []
    added = False
    existing_codes = {x.get('stock_code') if isinstance(x, dict) else x for x in lst}
    for s in DEAD_STOCKS:
        if s not in existing_codes:
            lst.append({'stock_code': s, 'reason': 'suspended_no_kline_window'})
            added = True
    if added:
        ok = exec_retry(
            "UPDATE stock_pool_refresh_task SET evicted_stocks_json=?, updated_at=? WHERE task_id=?",
            (json.dumps(lst, ensure_ascii=False), now_iso, task_id),
        )
        if ok:
            print(f'  updated refresh_task for {td}')
        else:
            print(f'  [warn] skip refresh_task update due to lock: {td}')

commit_retry()

# --- 4) 物理删除 is_deleted=1 的研报 + 级联清理 ---
print('\n[4] 物理清理软删研报')
cur.execute("SELECT COUNT(*) FROM report WHERE is_deleted=1")
total_soft = cur.fetchone()[0]
print(f'  soft-deleted before: {total_soft}')

# 收集 report_id + task_id
cur.execute("SELECT report_id, generation_task_id FROM report WHERE is_deleted=1")
pairs = cur.fetchall()
rids = [p[0] for p in pairs]
tids = [p[1] for p in pairs if p[1]]

# 分批删除，避免 SQL 参数过多
BATCH = 500
def batched(xs):
    for i in range(0, len(xs), BATCH):
        yield xs[i:i+BATCH]

deleted_citation = 0
deleted_usage_link = 0
deleted_report = 0
deleted_feedback = 0
deleted_idem = 0

for chunk in batched(rids):
    placeholders = ','.join(['?']*len(chunk))
    ok = exec_retry(f'DELETE FROM report_citation WHERE report_id IN ({placeholders})', tuple(chunk))
    deleted_citation += cur.rowcount
    ok = exec_retry(f'DELETE FROM report_data_usage_link WHERE report_id IN ({placeholders})', tuple(chunk))
    deleted_usage_link += cur.rowcount
    ok = exec_retry(f'DELETE FROM report_feedback WHERE report_id IN ({placeholders})', tuple(chunk))
    deleted_feedback += cur.rowcount
    ok = exec_retry(f'DELETE FROM report_idempotency WHERE report_id IN ({placeholders})', tuple(chunk))
    deleted_idem += cur.rowcount
    ok = exec_retry(f'DELETE FROM report WHERE report_id IN ({placeholders})', tuple(chunk))
    deleted_report += cur.rowcount
    conn.commit()
    commit_retry()

print(f'  deleted: report={deleted_report} citation={deleted_citation} usage_link={deleted_usage_link} feedback={deleted_feedback} idempotency={deleted_idem}')

# instruction_card 的字段依赖可能不同，先尝试常见列名
cur.execute("PRAGMA table_info(instruction_card)")
ic_cols = {r[1] for r in cur.fetchall()}
if 'report_id' in ic_cols and rids:
    total_ic = 0
    for chunk in batched(rids):
        placeholders = ','.join(['?']*len(chunk))
        ok = exec_retry(f'DELETE FROM instruction_card WHERE report_id IN ({placeholders})', tuple(chunk))
        total_ic += cur.rowcount
        conn.commit()
    print(f'  deleted instruction_card={total_ic}')

conn.commit()

cur.execute("SELECT COUNT(*) FROM report WHERE is_deleted=1")
print(f'  soft-deleted after : {cur.fetchone()[0]}')

# --- 5) 诊断快照 ---
print('\n[5] 最终状态')
cur.execute('''SELECT trade_date, COUNT(*) FROM report
WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' AND is_deleted=0
GROUP BY trade_date ORDER BY trade_date''')
for r in cur.fetchall():
    print(f'  report {r[0]}: alive={r[1]}')

conn.close()
print('\nDONE')
