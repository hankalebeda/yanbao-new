import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()

print('=== 当前系统状态 ===')

# N1/N5: 可见研报
cur.execute('SELECT quality_flag, COUNT(*) FROM report WHERE published=1 AND is_deleted=0 GROUP BY quality_flag')
print('可见研报质量分布:', cur.fetchall())

# N2: Settlement
cur.execute('SELECT COUNT(DISTINCT report_id) FROM settlement_result')
print('settlement 覆盖 report_ids:', cur.fetchone()[0])
cur.execute('SELECT COUNT(*) FROM settlement_result')
print('settlement_result 总记录:', cur.fetchone()[0])

# N3: K线
cur.execute('SELECT COUNT(DISTINCT stock_code), COUNT(*) FROM kline_daily')
r = cur.fetchone()
print('kline: %d 股票 / 5197, %d 条记录' % (r[0], r[1]))

# N4: report_data_usage
cur.execute("SELECT COUNT(*) FROM report_data_usage WHERE status IN ('missing','degraded','stale_ok')")
non_ok = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM report_data_usage')
total = cur.fetchone()[0]
pct = non_ok / total * 100 if total else 0
print('report_data_usage 非ok: %d/%d = %.1f%%' % (non_ok, total, pct))

# N4 breakdown
cur.execute("SELECT dataset_name, status, COUNT(*) as cnt FROM report_data_usage WHERE status IN ('missing','degraded','stale_ok') GROUP BY dataset_name, status ORDER BY cnt DESC LIMIT 15")
print('\nN4 非ok breakdown (top15):')
for row in cur.fetchall():
    print('  %s | %s | %d' % (row[0], row[1], row[2]))

conn.close()
print('\nDone.')
