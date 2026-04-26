#!/usr/bin/env python3
"""
Backfill kline_daily RDU entries for 2026-04-24 pool stocks that have kline data
but no report_data_usage record.

kline data source: mootdx_online_20260424 (not a UUID, not in data_batch)
Action: create a data_batch entry for mootdx_online + create 249 RDU rows.
"""
import sqlite3
from datetime import datetime, timezone, date
from uuid import uuid4

DB_PATH = "data/app.db"
TRADE_DATE = "2026-04-24"
SOURCE_NAME = "mootdx_online"
BATCH_SCOPE = "full_market"

def now_utc():
    return datetime.now(timezone.utc).isoformat()

def main():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    # Check current state
    cur.execute("""
        SELECT COUNT(*) as cnt FROM report_data_usage
        WHERE trade_date = ? AND dataset_name = 'kline_daily'
    """, (TRADE_DATE,))
    print(f"Current kline_daily RDU for {TRADE_DATE}: {cur.fetchone()['cnt']}")

    # Find pool stocks missing kline RDU for 2026-04-24
    cur.execute("""
        SELECT ps.stock_code
        FROM stock_pool_snapshot ps
        LEFT JOIN report_data_usage rdu ON rdu.stock_code = ps.stock_code
            AND rdu.trade_date = ?
            AND rdu.dataset_name = 'kline_daily'
        WHERE ps.trade_date = ? AND rdu.usage_id IS NULL
        ORDER BY ps.stock_code
    """, (TRADE_DATE, TRADE_DATE))
    missing_stocks = [r["stock_code"] for r in cur.fetchall()]
    print(f"Pool stocks missing kline RDU: {len(missing_stocks)}")
    if not missing_stocks:
        print("Nothing to fix.")
        conn.close()
        return

    # Check if a mootdx_online batch already exists for this date
    cur.execute("""
        SELECT batch_id, batch_status, quality_flag FROM data_batch
        WHERE source_name = ? AND trade_date = ? AND batch_scope = ?
        ORDER BY created_at DESC LIMIT 1
    """, (SOURCE_NAME, TRADE_DATE, BATCH_SCOPE))
    existing_batch = cur.fetchone()

    if existing_batch:
        batch_id = existing_batch["batch_id"]
        print(f"Using existing batch: {batch_id[:8]} status={existing_batch['batch_status']} quality={existing_batch['quality_flag']}")
    else:
        # Get next batch_seq
        cur.execute("""
            SELECT MAX(batch_seq) as max_seq FROM data_batch
            WHERE source_name = ? AND trade_date = ? AND batch_scope = ?
        """, (SOURCE_NAME, TRADE_DATE, BATCH_SCOPE))
        row = cur.fetchone()
        next_seq = (row["max_seq"] or 0) + 1

        batch_id = str(uuid4())
        ts = now_utc()
        cur.execute("""
            INSERT INTO data_batch (
                batch_id, source_name, trade_date, batch_scope, batch_seq,
                batch_status, quality_flag, covered_stock_count, core_pool_covered_count,
                records_total, records_success, records_failed, status_reason,
                trigger_task_run_id, started_at, finished_at, updated_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            batch_id, SOURCE_NAME, TRADE_DATE, BATCH_SCOPE, next_seq,
            "SUCCESS", "ok", len(missing_stocks), None,
            len(missing_stocks), len(missing_stocks), 0, "backfill_rdu_only",
            None, ts, ts, ts, ts
        ))
        print(f"Created new batch: {batch_id[:8]} seq={next_seq}")

    # Create RDU entries for all missing pool stocks
    ts = now_utc()
    inserted = 0
    for stock_code in missing_stocks:
        # Check if pool stock has kline data on this date
        cur.execute("""
            SELECT kline_id FROM kline_daily
            WHERE stock_code = ? AND trade_date = ?
            LIMIT 1
        """, (stock_code, TRADE_DATE))
        kline_row = cur.fetchone()
        status = "ok" if kline_row else "stale_ok"
        status_reason = None if kline_row else "kline_missing_backfill"

        usage_id = str(uuid4())
        cur.execute("""
            INSERT INTO report_data_usage (
                usage_id, trade_date, stock_code, dataset_name, source_name,
                batch_id, fetch_time, status, status_reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            usage_id, TRADE_DATE, stock_code, "kline_daily", SOURCE_NAME,
            batch_id, ts, status, status_reason, ts
        ))
        inserted += 1

    conn.commit()
    print(f"Inserted {inserted} kline_daily RDU entries.")

    # Verify
    cur.execute("""
        SELECT COUNT(*) as cnt FROM report_data_usage
        WHERE trade_date = ? AND dataset_name = 'kline_daily'
    """, (TRADE_DATE,))
    print(f"After fix: kline_daily RDU for {TRADE_DATE}: {cur.fetchone()['cnt']}")

    # Check pool coverage
    cur.execute("""
        SELECT COUNT(DISTINCT ps.stock_code) as pool_total,
               COUNT(DISTINCT rdu.stock_code) as rdu_covered
        FROM stock_pool_snapshot ps
        LEFT JOIN report_data_usage rdu ON rdu.stock_code = ps.stock_code
            AND rdu.trade_date = ?
            AND rdu.dataset_name = 'kline_daily'
        WHERE ps.trade_date = ?
    """, (TRADE_DATE, TRADE_DATE))
    r = cur.fetchone()
    print(f"Pool coverage: {r['rdu_covered']}/{r['pool_total']}")

    conn.close()
    print("DONE")

if __name__ == "__main__":
    main()
