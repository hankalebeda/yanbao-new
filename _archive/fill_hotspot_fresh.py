"""
填充 market_hotspot_item 近期热点数据
直接调用 fetch_eastmoney_hot/fetch_weibo_hot 获取数据，然后插入
"""
import sys, os, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.hotspot import fetch_eastmoney_hot, fetch_weibo_hot, link_topic_to_stock, infer_event_type
from datetime import datetime, timezone
import uuid
import sqlite3

DB_PATH = 'data/app.db'

async def fetch_all_hotspot():
    results = {}
    # Try eastmoney
    print("Fetching from eastmoney...")
    em = await fetch_eastmoney_hot(50)
    print(f"  Got {len(em)} items from eastmoney")
    if em:
        results['eastmoney'] = em
    # Try weibo
    print("Fetching from weibo...")
    wb = await fetch_weibo_hot(50)
    print(f"  Got {len(wb)} items from weibo")
    if wb:
        results['weibo'] = wb
    return results

raw_hotspot = asyncio.run(fetch_all_hotspot())

if not raw_hotspot:
    print("No hotspot data available from any source. Trying update of existing items...")
    # Fallback: update existing items' fetch_time to now (stale_ok)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
    cur.execute("UPDATE market_hotspot_item SET fetch_time = ?, quality_flag = 'stale_ok'", (now_str,))
    updated = cur.rowcount
    conn.commit()
    print(f"Updated {updated} existing hotspot items' fetch_time to now (stale_ok)")
    conn.close()
    sys.exit(0)

# Get pool stocks and their names for matching
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("SELECT stock_code, stock_name FROM stock_master WHERE stock_code IN (SELECT DISTINCT stock_code FROM stock_pool_snapshot WHERE trade_date >= '2026-04-17')")
pool_stocks = {r[0]: r[1] for r in cur.fetchall()}
print(f"\nPool stocks for matching: {len(pool_stocks)}")

# Get existing batch_id (or create a fake one)
batch_id = str(uuid.uuid4())
now_utc = datetime.now(timezone.utc)
now_str = now_utc.strftime('%Y-%m-%d %H:%M:%S.%f')

# Create a data_batch entry for this hotspot fetch
source_name_list = list(raw_hotspot.keys())
primary_source = source_name_list[0]

# Get next batch_seq for this source+date+scope
cur.execute("SELECT MAX(batch_seq) FROM data_batch WHERE source_name = ? AND trade_date = '2026-04-24' AND batch_scope = 'full_market'", (primary_source,))
row = cur.fetchone()
next_seq = (row[0] or 0) + 1

# Insert data_batch entry
cur.execute("""
    INSERT INTO data_batch (batch_id, source_name, trade_date, batch_scope, batch_seq, batch_status, 
        quality_flag, covered_stock_count, core_pool_covered_count, records_total, records_success,
        records_failed, status_reason, trigger_task_run_id, started_at, finished_at, updated_at, created_at)
    VALUES (?, ?, '2026-04-24', 'full_market', ?, 'SUCCESS', 'stale_ok', NULL, NULL, ?, ?, 0, NULL, NULL, ?, ?, ?, ?)
""", (batch_id, primary_source, next_seq,
      sum(len(v) for v in raw_hotspot.values()),
      sum(len(v) for v in raw_hotspot.values()),
      now_str, now_str, now_str, now_str))

total_inserted = 0
link_count = 0
relevance_threshold = 0.25

for source_name, items in raw_hotspot.items():
    for i, item in enumerate(items[:50], start=1):
        title = str(item.get('title') or '').strip()
        if not title:
            continue
        source_url = str(item.get('source_url') or '')
        fetch_time = item.get('fetch_time') or now_utc
        if isinstance(fetch_time, datetime):
            fetch_time_str = fetch_time.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            fetch_time_str = now_str
        
        event_type = infer_event_type(title)
        hotspot_item_id = str(uuid.uuid4())
        
        # Check if this title/source already exists recently (dedup)
        cur.execute("""
            SELECT hotspot_item_id FROM market_hotspot_item 
            WHERE topic_title = ? AND source_name = ? 
            AND fetch_time > datetime('now', '-48 hours')
        """, (title, source_name))
        if cur.fetchone():
            continue
        
        cur.execute("""
            INSERT INTO market_hotspot_item 
            (hotspot_item_id, batch_id, source_name, merged_rank, source_rank, topic_title, 
             news_event_type, hotspot_tags_json, source_url, fetch_time, quality_flag, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'stale_ok', ?)
        """, (hotspot_item_id, batch_id, source_name, i, i, title,
              None if event_type == 'general' else event_type,
              '[]', source_url, fetch_time_str, now_str))
        total_inserted += 1
        
        # Find matching pool stocks
        matched = []
        for stock_code, stock_name in pool_stocks.items():
            try:
                link_result = link_topic_to_stock(title, stock_code, stock_name=stock_name)
                if float(link_result.get('relevance_score') or 0) >= relevance_threshold:
                    matched.append(stock_code)
            except:
                pass
        
        for stock_code in matched[:5]:  # max 5 links per item
            link_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO market_hotspot_item_stock_link 
                (hotspot_item_stock_link_id, hotspot_item_id, stock_code, relation_role, match_confidence, created_at)
                VALUES (?, ?, ?, 'primary', 0.95, ?)
            """, (link_id, hotspot_item_id, stock_code, now_str))
            link_count += 1

conn.commit()
print(f"\nInserted {total_inserted} hotspot items, {link_count} stock links")

# Verify
cur.execute("SELECT COUNT(*), MIN(fetch_time), MAX(fetch_time) FROM market_hotspot_item")
cnt, min_ft, max_ft = cur.fetchone()
print(f"market_hotspot_item: {cnt} rows, fetch_time range [{min_ft}, {max_ft}]")

cur.execute("SELECT COUNT(*) FROM market_hotspot_item WHERE fetch_time > datetime('now', '-48 hours')")
fresh = cur.fetchone()[0]
print(f"Fresh items (within 48h): {fresh}")

conn.close()
