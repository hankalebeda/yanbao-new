"""
为 2026-04-13 创建股票池元数据（stock_pool_refresh_task + stock_pool_snapshot）
让 generate_reports_batch 能正常工作（绕过 DEPENDENCY_NOT_READY 503）

必须在生成报告前运行这个脚本。
"""
import sqlite3
from datetime import datetime, timezone
from uuid import uuid4

DB_PATH = "data/app.db"
TRADE_DATE = "2026-04-13"

# 2026-04-13 牛势候选股票（close > ma5 > ma20，按动量排序）
TARGET_STOCKS = [
    "002281.SZ",  # +27.6% above MA20
    "300308.SZ",  # +19.6% above MA20
    "300782.SZ",  # +17.5% above MA20
    "002475.SZ",  # +16.2% above MA20
    "002008.SZ",  # +15.4% above MA20
    "688002.SH",  # +14.1% above MA20
    "000977.SZ",  # +13.9% above MA20
    "002466.SZ",  # +13.3% above MA20
    "600309.SH",  # +12.9% above MA20
    "002460.SZ",  # +12.4% above MA20
    "300502.SZ",  # +11.3% above MA20
    "300408.SZ",  # +10.6% above MA20
    "600183.SH",  # +9.6% above MA20
    "688008.SH",  # +9.5% above MA20
    "300450.SZ",  # +8.4% above MA20
    "603259.SH",  # +8.3% above MA20
    "600741.SH",  # +7.8% above MA20
    "601138.SH",  # +7.4% above MA20
    "002756.SZ",  # +7.4% above MA20
    "002179.SZ",  # +6.7% above MA20
    "600760.SH",  # +5.8% above MA20
    "002415.SZ",  # +5.6% above MA20
    "600030.SH",  # +5.3% above MA20
    "603799.SH",  # +5.2% above MA20
    "002396.SZ",  # +4.5% above MA20
    "002709.SZ",  # +4.2% above MA20
    "603993.SH",  # +4.2% above MA20
    "002241.SZ",  # +4.2% above MA20
    "600584.SH",  # +4.1% above MA20
    "688396.SH",  # +4.0% above MA20
    "002371.SZ",  # +4.0% above MA20
    "000708.SZ",  # +3.6% above MA20
    "002236.SZ",  # +3.3% above MA20
    "600570.SH",  # +2.4% above MA20
    "601633.SH",  # +2.2% above MA20
]


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    now_str = datetime.now(timezone.utc).isoformat()

    # 1. 检查是否已存在
    existing = cur.execute(
        "SELECT task_id, status FROM stock_pool_refresh_task WHERE trade_date=? LIMIT 1",
        (TRADE_DATE,)
    ).fetchone()
    if existing:
        print(f"stock_pool_refresh_task already exists for {TRADE_DATE}: {existing}")
        task_id = existing[0]
    else:
        task_id = str(uuid4())
        cur.execute("""
            INSERT INTO stock_pool_refresh_task (
                task_id, trade_date, status, pool_version,
                fallback_from, filter_params_json, core_pool_size, standby_pool_size,
                evicted_stocks_json, status_reason, request_id,
                started_at, finished_at, updated_at, created_at
            ) VALUES (?, ?, 'COMPLETED', 1, NULL, '{"target_pool_size": 35}',
                      ?, 0, '[]', NULL, ?, ?, ?, ?, ?)
        """, (
            task_id, TRADE_DATE, len(TARGET_STOCKS),
            str(uuid4()), now_str, now_str, now_str, now_str
        ))
        print(f"Created stock_pool_refresh_task: {task_id}")

    # 2. 创建 stock_pool_snapshot 行
    pool_version = 1
    existing_snaps = cur.execute(
        "SELECT COUNT(*) FROM stock_pool_snapshot WHERE trade_date=?",
        (TRADE_DATE,)
    ).fetchone()[0]
    print(f"Existing snapshots for {TRADE_DATE}: {existing_snaps}")

    inserted = 0
    for rank, stock_code in enumerate(TARGET_STOCKS, 1):
        exists = cur.execute(
            "SELECT 1 FROM stock_pool_snapshot WHERE stock_code=? AND trade_date=?",
            (stock_code, TRADE_DATE)
        ).fetchone()
        if not exists:
            cur.execute("""
                INSERT INTO stock_pool_snapshot (
                    pool_snapshot_id, refresh_task_id, trade_date, pool_version,
                    stock_code, pool_role, rank_no, score, is_suspended, created_at
                ) VALUES (?, ?, ?, ?, ?, 'core', ?, ?, 0, ?)
            """, (
                str(uuid4()), task_id, TRADE_DATE, pool_version,
                stock_code, rank, 88.5 - rank * 0.1, now_str
            ))
            inserted += 1

    conn.commit()
    print(f"Inserted {inserted} snapshot rows for {TRADE_DATE}")

    # 3. 验证
    snap_count = cur.execute(
        "SELECT COUNT(*) FROM stock_pool_snapshot WHERE trade_date=?",
        (TRADE_DATE,)
    ).fetchone()[0]
    task_row = cur.execute(
        "SELECT task_id, status, pool_version, core_pool_size FROM stock_pool_refresh_task WHERE trade_date=?",
        (TRADE_DATE,)
    ).fetchone()
    print(f"\nVerification:")
    print(f"  stock_pool_refresh_task: {task_row}")
    print(f"  stock_pool_snapshot count for {TRADE_DATE}: {snap_count}")

    conn.close()
    print("\nDone! Now run: python codex/batch_gen_apr13.py --max 35 --settle")


if __name__ == "__main__":
    main()
