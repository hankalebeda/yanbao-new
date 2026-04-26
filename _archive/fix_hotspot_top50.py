#!/usr/bin/env python3
"""
Fix hotspot_top50 data gaps for 2026-04-24:
1. Populate hotspot_top50 table from market_hotspot_item (weibo, 50 records)
2. Create report_data_usage entries for:
   - Pool stocks linked to hotspot items -> status='stale_ok'
   - Pool stocks NOT linked to hotspot items -> status='missing'
"""
import sqlite3
from datetime import datetime, timezone
from uuid import uuid4

DB_PATH = "data/app.db"
TRADE_DATE = "2026-04-24"


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def main():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    # --- Step 0: Check current state ---
    cur.execute("SELECT COUNT(*) as cnt FROM hotspot_top50 WHERE trade_date = ?", (TRADE_DATE,))
    print(f"hotspot_top50 rows for {TRADE_DATE}: {cur.fetchone()['cnt']}")

    cur.execute("""
        SELECT COUNT(*) as cnt FROM report_data_usage
        WHERE trade_date = ? AND dataset_name = 'hotspot_top50'
    """, (TRADE_DATE,))
    print(f"hotspot_top50 RDU for {TRADE_DATE}: {cur.fetchone()['cnt']}")

    # --- Step 1: Get market_hotspot_items for 2026-04-24 ---
    cur.execute("""
        SELECT mi.hotspot_item_id, mi.batch_id, mi.source_name, mi.merged_rank, mi.source_rank,
               mi.topic_title, mi.news_event_type, mi.source_url, mi.fetch_time,
               mi.quality_flag, mi.created_at
        FROM market_hotspot_item mi
        WHERE DATE(mi.created_at) = ?
        ORDER BY mi.merged_rank ASC, mi.source_rank ASC
    """, (TRADE_DATE,))
    hotspot_items = [dict(r) for r in cur.fetchall()]
    print(f"\nmarket_hotspot_item for {TRADE_DATE}: {len(hotspot_items)} items")

    if not hotspot_items:
        print("No hotspot items found for this date. Cannot fix hotspot_top50.")
        conn.close()
        return

    # Show first few
    for item in hotspot_items[:3]:
        print(f"  rank={item['merged_rank']} source={item['source_name']} title={item['topic_title'][:30]}")

    # --- Step 2: Get the weibo batch ID (primary source) ---
    # Use the batch_id from the first item as the reference
    # Also find the best existing merged batch or create new one
    weibo_batch_id = hotspot_items[0]["batch_id"]
    primary_source = hotspot_items[0]["source_name"]
    print(f"\nPrimary source batch: {weibo_batch_id[:8]} source={primary_source}")

    # Check if there's already a successful hotspot_merged batch for this date
    cur.execute("""
        SELECT batch_id, source_name, batch_status, quality_flag
        FROM data_batch
        WHERE trade_date = ? AND batch_scope = 'hotspot_merged'
        ORDER BY created_at DESC LIMIT 1
    """, (TRADE_DATE,))
    merged_batch_row = cur.fetchone()

    if merged_batch_row and merged_batch_row["batch_status"] != "FAILED":
        merged_batch_id = merged_batch_row["batch_id"]
        print(f"Using existing merged batch: {merged_batch_id[:8]} status={merged_batch_row['batch_status']}")
    else:
        # Create a new merged batch
        cur.execute("""
            SELECT MAX(batch_seq) as max_seq FROM data_batch
            WHERE source_name = ? AND trade_date = ? AND batch_scope = 'hotspot_merged'
        """, (primary_source, TRADE_DATE))
        row = cur.fetchone()
        next_seq = (row["max_seq"] or 0) + 1

        merged_batch_id = str(uuid4())
        ts = now_utc()
        cur.execute("""
            INSERT INTO data_batch (
                batch_id, source_name, trade_date, batch_scope, batch_seq,
                batch_status, quality_flag, covered_stock_count, core_pool_covered_count,
                records_total, records_success, records_failed, status_reason,
                trigger_task_run_id, started_at, finished_at, updated_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            merged_batch_id, primary_source, TRADE_DATE, "hotspot_merged", next_seq,
            "SUCCESS", "stale_ok", None, None,
            len(hotspot_items), len(hotspot_items), 0, "backfill_from_market_hotspot_item",
            None, ts, ts, ts, ts
        ))
        print(f"Created new merged batch: {merged_batch_id[:8]} seq={next_seq}")

    # --- Step 3: Populate hotspot_top50 table ---
    # Check if already populated
    cur.execute("SELECT COUNT(*) as cnt FROM hotspot_top50 WHERE trade_date = ? AND batch_id = ?",
                (TRADE_DATE, merged_batch_id))
    already_in_top50 = cur.fetchone()["cnt"]

    if already_in_top50 > 0:
        print(f"\nhotspot_top50 already has {already_in_top50} rows for this batch, skipping insert.")
    else:
        ts = now_utc()
        top50_inserted = 0
        for item in hotspot_items[:50]:
            rank = item["merged_rank"] if item["merged_rank"] else (top50_inserted + 1)
            cur.execute("""
                INSERT OR IGNORE INTO hotspot_top50 (
                    hotspot_id, trade_date, rank, topic_title, source_name,
                    source_url, fetch_time, quality_flag, batch_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(uuid4()), TRADE_DATE, str(rank),
                item["topic_title"] or "",
                item["source_name"] or "weibo",
                item["source_url"] or "",
                item["fetch_time"] or ts,
                item["quality_flag"] or "stale_ok",
                merged_batch_id,
                ts
            ))
            top50_inserted += 1
        print(f"\nInserted {top50_inserted} rows into hotspot_top50.")

    # --- Step 4: Get pool stocks for 2026-04-24 ---
    cur.execute("""
        SELECT DISTINCT ps.stock_code
        FROM stock_pool_snapshot ps
        WHERE ps.trade_date = ?
        ORDER BY ps.stock_code
    """, (TRADE_DATE,))
    pool_stocks = {r["stock_code"] for r in cur.fetchall()}
    print(f"\nPool stocks for {TRADE_DATE}: {len(pool_stocks)}")

    # --- Step 5: Get stocks already linked to hotspot items ---
    item_ids = [item["hotspot_item_id"] for item in hotspot_items]
    if item_ids:
        placeholders = ",".join("?" for _ in item_ids)
        cur.execute(f"""
            SELECT DISTINCT sl.stock_code
            FROM market_hotspot_item_stock_link sl
            WHERE sl.hotspot_item_id IN ({placeholders})
        """, item_ids)
        hotspot_linked_stocks = {r["stock_code"] for r in cur.fetchall()}
    else:
        hotspot_linked_stocks = set()

    pool_linked = pool_stocks & hotspot_linked_stocks
    pool_not_linked = pool_stocks - hotspot_linked_stocks
    print(f"Pool stocks linked to hotspot items: {len(pool_linked)}")
    print(f"Pool stocks NOT linked to hotspot items: {len(pool_not_linked)}")

    # --- Step 6: Check existing RDU entries ---
    cur.execute("""
        SELECT stock_code FROM report_data_usage
        WHERE trade_date = ? AND dataset_name = 'hotspot_top50'
    """, (TRADE_DATE,))
    existing_rdu_stocks = {r["stock_code"] for r in cur.fetchall()}
    print(f"Existing hotspot_top50 RDU entries: {len(existing_rdu_stocks)}")

    # --- Step 7: Create RDU entries ---
    ts = now_utc()
    inserted_ok = 0
    inserted_missing = 0

    # For stocks linked to hotspot items -> stale_ok
    for stock_code in sorted(pool_linked - existing_rdu_stocks):
        cur.execute("""
            INSERT INTO report_data_usage (
                usage_id, trade_date, stock_code, dataset_name, source_name,
                batch_id, fetch_time, status, status_reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(uuid4()), TRADE_DATE, stock_code, "hotspot_top50", primary_source,
            merged_batch_id, ts, "stale_ok", "hotspot_source_partial", ts
        ))
        inserted_ok += 1

    # For stocks NOT linked to hotspot items -> missing
    for stock_code in sorted(pool_not_linked - existing_rdu_stocks):
        cur.execute("""
            INSERT INTO report_data_usage (
                usage_id, trade_date, stock_code, dataset_name, source_name,
                batch_id, fetch_time, status, status_reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(uuid4()), TRADE_DATE, stock_code, "hotspot_top50", primary_source,
            merged_batch_id, ts, "missing", "no_hotspot_link", ts
        ))
        inserted_missing += 1

    conn.commit()
    print(f"\nInserted RDU entries: {inserted_ok} stale_ok + {inserted_missing} missing = {inserted_ok + inserted_missing} total")

    # --- Verify ---
    cur.execute("SELECT COUNT(*) as cnt FROM hotspot_top50 WHERE trade_date = ?", (TRADE_DATE,))
    print(f"\nVerification:")
    print(f"  hotspot_top50 table rows: {cur.fetchone()['cnt']}")

    cur.execute("""
        SELECT status, COUNT(*) as cnt FROM report_data_usage
        WHERE trade_date = ? AND dataset_name = 'hotspot_top50'
        GROUP BY status
    """, (TRADE_DATE,))
    for r in cur.fetchall():
        print(f"  hotspot_top50 RDU status={r['status']} count={r['cnt']}")

    cur.execute("""
        SELECT COUNT(DISTINCT ps.stock_code) as pool_total,
               COUNT(DISTINCT rdu.stock_code) as rdu_covered
        FROM stock_pool_snapshot ps
        LEFT JOIN report_data_usage rdu ON rdu.stock_code = ps.stock_code
            AND rdu.trade_date = ?
            AND rdu.dataset_name = 'hotspot_top50'
        WHERE ps.trade_date = ?
    """, (TRADE_DATE, TRADE_DATE))
    r = cur.fetchone()
    print(f"  Pool coverage: {r['rdu_covered']}/{r['pool_total']}")

    conn.close()
    print("DONE")


if __name__ == "__main__":
    main()
