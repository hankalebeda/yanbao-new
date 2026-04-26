#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect("data/app.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

# market_hotspot_item
cur.execute("SELECT COUNT(*) as cnt, MAX(created_at) as latest FROM market_hotspot_item")
r = cur.fetchone()
print("market_hotspot_item: cnt=", r["cnt"], "latest=", r["latest"])

cur.execute("SELECT source_name, COUNT(*) as cnt, MAX(created_at) as latest FROM market_hotspot_item GROUP BY source_name ORDER BY latest DESC LIMIT 10")
for r in cur.fetchall():
    print("  source=", r["source_name"], "cnt=", r["cnt"], "latest=", r["latest"])

# market_hotspot_item_source
cur.execute("PRAGMA table_info(market_hotspot_item_source)")
cols = [r["name"] for r in cur.fetchall()]
print("\nmarket_hotspot_item_source cols:", cols)
cur.execute("SELECT COUNT(*) FROM market_hotspot_item_source")
print("  count:", cur.fetchone()[0])

# hotspot_raw
cur.execute("PRAGMA table_info(hotspot_raw)")
cols = [r["name"] for r in cur.fetchall()]
print("\nhotspot_raw cols:", cols)
cur.execute("SELECT COUNT(*) FROM hotspot_raw")
print("  count:", cur.fetchone()[0])

# batch details for weibo/douyin/etc on 2026-04-24
cur.execute("""
    SELECT batch_id, source_name, batch_scope, batch_status, quality_flag, records_success, records_failed, created_at
    FROM data_batch 
    WHERE source_name IN ('weibo', 'douyin', 'eastmoney', 'baidu_hot', 'xueqiu', 'cls', 'kuaishou') 
    AND trade_date = '2026-04-24'
    ORDER BY created_at DESC LIMIT 10
""")
print("\nHotspot batches for 2026-04-24:")
for r in cur.fetchall():
    print("  ", r["batch_id"][:8], r["source_name"], r["batch_scope"], r["batch_status"], r["quality_flag"], "success=", r["records_success"])

# What is in market_hotspot_item_stock_link 
cur.execute("SELECT COUNT(*) FROM market_hotspot_item_stock_link")
print("\nmarket_hotspot_item_stock_link count:", cur.fetchone()[0])

# What about hotspot_stock_link
cur.execute("SELECT COUNT(*) FROM hotspot_stock_link")
print("hotspot_stock_link count:", cur.fetchone()[0])

# Check data_usage_fact 
cur.execute("SELECT COUNT(*), MAX(fact_date) FROM data_usage_fact")
r = cur.fetchone()
print("\ndata_usage_fact count:", r[0], "latest:", r[1])

# Check hotspot-related report_data_usage to understand what happened on 2026-04-16
cur.execute("""
    SELECT stock_code, status, source_name, trade_date
    FROM report_data_usage
    WHERE dataset_name = 'hotspot_top50' AND status = 'ok'
    ORDER BY trade_date DESC, stock_code
    LIMIT 10
""")
print("\nHotspot_top50 ok records in RDU:")
for r in cur.fetchall():
    print("  ", r["stock_code"], r["status"], r["source_name"], r["trade_date"])

# What batch_id were those using
cur.execute("""
    SELECT rdu.batch_id, d.source_name, d.batch_status, d.quality_flag
    FROM report_data_usage rdu
    LEFT JOIN data_batch d ON d.batch_id = rdu.batch_id
    WHERE rdu.dataset_name = 'hotspot_top50' AND rdu.status = 'ok'
    GROUP BY rdu.batch_id
    LIMIT 5
""")
print("\nHotspot_top50 ok batch details:")
for r in cur.fetchall():
    print("  batch=", r["batch_id"][:8] if r["batch_id"] else "None", "source=", r["source_name"], "status=", r["batch_status"])

conn.close()
print("\nDONE")
