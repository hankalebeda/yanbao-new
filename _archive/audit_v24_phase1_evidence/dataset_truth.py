import sqlite3
c = sqlite3.connect('data/app.db')
cur = c.cursor()
print('=== by dataset x status ===')
cur.execute("SELECT dataset_name, status, COUNT(*) FROM report_data_usage GROUP BY dataset_name, status ORDER BY dataset_name, status")
for r in cur.fetchall():
    print(r)
print('=== latest per dataset ===')
cur.execute("SELECT dataset_name, MAX(trade_date), COUNT(*) FROM report_data_usage GROUP BY dataset_name")
for r in cur.fetchall():
    print(r)
print('=== schema check ===')
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('northbound_summary','etf_flow_summary','hotspot_top50','hotspot_raw','hotspot_normalized','kline_daily','market_state_cache','market_state_input','report_data_usage','market_data_batch')")
for r in cur.fetchall():
    print(r)
print('=== report_data_usage columns ===')
cur.execute("PRAGMA table_info(report_data_usage)")
for r in cur.fetchall():
    print(r)
