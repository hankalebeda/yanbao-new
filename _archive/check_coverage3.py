import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

cur.execute("SELECT source_name, trade_date, batch_status, records_total FROM data_batch WHERE source_name IN ('northbound', 'etf_flow', 'akshare_hsgt_hist', 'akshare_fund_etf_fund_daily') ORDER BY trade_date DESC LIMIT 20")
print("northbound/etf batch entries:")
for r in cur.fetchall():
    print(r)

print()
cur.execute("SELECT trade_date, dataset_name, COUNT(*), status FROM report_data_usage WHERE trade_date >= '2026-04-17' GROUP BY trade_date, dataset_name, status ORDER BY trade_date DESC, dataset_name")
print("report_data_usage >= 2026-04-17:")
for r in cur.fetchall():
    print(r)

# Check instruction_card recent dates
cur.execute("PRAGMA table_info(instruction_card)")
ic_cols = [r[1] for r in cur.fetchall()]
print()
print("instruction_card cols:", ic_cols[:10])

conn.close()
