import sqlite3

conn = sqlite3.connect('data/app.db')
c = conn.cursor()

# Check K-line stats
c.execute('SELECT COUNT(DISTINCT stock_code), COUNT(*) FROM kline_daily')
stocks, records = c.fetchone()
print(f"K-line: {stocks} stocks, {records} total records")

# Check extension batch
c.execute("SELECT COUNT(*) FROM kline_daily WHERE source_batch_id='kline_extension_batch_001'")
ext_batch = c.fetchone()[0]
print(f"Extension batch (kline_extension_batch_001): {ext_batch} records")

# Verify data quality - check some sample records
c.execute("SELECT stock_code, trade_date, open, close FROM kline_daily WHERE source_batch_id='kline_extension_batch_001' LIMIT 5")
samples = c.fetchall()
print(f"\nSample records from extension batch:")
for stock, date, open_p, close_p in samples:
    print(f"  {stock} {date}: open={open_p}, close={close_p}")

conn.close()
print("\n✓ Verification complete")
