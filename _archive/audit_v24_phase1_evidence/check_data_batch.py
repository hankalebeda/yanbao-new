import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()
# 检查2026-04-03的数据来源批次
print('=== data_batch for 2026-04-03 ===')
cur.execute("""SELECT source_name, batch_scope, batch_status, quality_flag, 
covered_stock_count, records_success, status_reason
FROM data_batch WHERE trade_date = '2026-04-03' ORDER BY created_at LIMIT 20""")
for r in cur.fetchall():
    print(r)

print('\n=== report_data_usage for 2026-04-03 (group) ===')
cur.execute("""SELECT dataset_name, source_name, status, COUNT(*) as cnt
FROM report_data_usage WHERE trade_date = '2026-04-03' 
GROUP BY dataset_name, source_name, status ORDER BY dataset_name""")
for r in cur.fetchall():
    print(r)

# 查看northbound使用的source
print('\n=== northbound/etf_flow source_name for 2026-04-03 sample ===')
cur.execute("""SELECT dataset_name, source_name, status, status_reason FROM report_data_usage 
WHERE trade_date='2026-04-03' AND dataset_name IN ('northbound_summary', 'etf_flow_summary') LIMIT 3""")
for r in cur.fetchall():
    print(r)
