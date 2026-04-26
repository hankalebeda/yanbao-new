#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()

cur.execute("SELECT stock_code, stock_name, is_delisted, list_date FROM stock_master WHERE is_delisted=0")
stocks = cur.fetchall()
print("ACTIVE:", len(stocks), [s[0] for s in stocks])

cur.execute("SELECT COUNT(DISTINCT stock_code),MAX(trade_date) FROM kline_daily")
print("KLINE:", cur.fetchone())

cur.execute("SELECT trade_date,COUNT(*) FROM stock_pool_snapshot GROUP BY trade_date ORDER BY trade_date DESC LIMIT 3")
for r in cur.fetchall():
    print("POOL_SNAP:", r)

cur.execute("SELECT DISTINCT dataset_name FROM report_data_usage ORDER BY dataset_name")
print("DATASETS:", [r[0] for r in cur.fetchall()])

cur.execute("SELECT stock_code,dataset_name,status,MAX(trade_date) FROM report_data_usage GROUP BY stock_code,dataset_name ORDER BY stock_code,dataset_name")
for r in cur.fetchall():
    print("RDU:", r)

cur.execute("SELECT MAX(trade_date),COUNT(*) FROM hotspot_top50")
print("HOTSPOT_TOP50:", cur.fetchone())

cur.execute("SELECT platform,COUNT(*),MAX(fetch_time) FROM hotspot_raw GROUP BY platform")
for r in cur.fetchall():
    print("HOTSPOT_RAW:", r)

cur.execute("SELECT COUNT(*),MAX(score_date) FROM stock_score")
print("STOCK_SCORE:", cur.fetchone())

cur.execute("SELECT id,batch_seq,status,created_at FROM data_batch ORDER BY id DESC LIMIT 5")
for r in cur.fetchall():
    print("BATCH:", r)

conn.close()
print("DONE")
