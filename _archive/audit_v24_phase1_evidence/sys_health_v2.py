import sqlite3

c = sqlite3.connect('data/app.db')

print('=' * 60)
print('1) data_batch per source status for 04-07..04-16')
print('=' * 60)
for r in c.execute(
    "SELECT source_name, batch_status, COUNT(*) FROM data_batch "
    "WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' "
    "GROUP BY source_name, batch_status ORDER BY source_name, batch_status"
):
    print(' ', r)

print()
print('=' * 60)
print('2) data_batch_error recent top categories (created >= 2026-04-07)')
print('=' * 60)
for r in c.execute(
    "SELECT error_code, error_stage, COUNT(*) FROM data_batch_error "
    "WHERE created_at >= '2026-04-07' "
    "GROUP BY error_code, error_stage ORDER BY COUNT(*) DESC LIMIT 20"
):
    print(' ', r)

print()
print('=' * 60)
print('3) refresh_task status per date')
print('=' * 60)
for r in c.execute(
    "SELECT trade_date, status, COUNT(*) FROM stock_pool_refresh_task "
    "WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' "
    "GROUP BY trade_date, status ORDER BY trade_date, status"
):
    print(' ', r)

print()
print('=' * 60)
print('4) report_data_usage non-ok per date/source')
print('=' * 60)
for r in c.execute(
    "SELECT trade_date, source_name, status, COUNT(*) FROM report_data_usage "
    "WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' AND status != 'ok' "
    "GROUP BY trade_date, source_name, status ORDER BY trade_date LIMIT 50"
):
    print(' ', r)

print()
print('=' * 60)
print('5) failure_category on live reports (04-07..04-16)')
print('=' * 60)
for r in c.execute(
    "SELECT trade_date, failure_category, COUNT(*) FROM report "
    "WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' AND is_deleted=0 "
    "GROUP BY trade_date, failure_category ORDER BY trade_date"
):
    print(' ', r)

print()
print('=' * 60)
print('6) market_state_cache NULL batch ids')
print('=' * 60)
for r in c.execute(
    "SELECT trade_date, kline_batch_id, hotspot_batch_id FROM market_state_cache "
    "WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' "
    "AND (kline_batch_id IS NULL OR hotspot_batch_id IS NULL) "
    "ORDER BY trade_date"
):
    print(' ', r)

print()
print('=' * 60)
print('7) recommendation distribution on live reports')
print('=' * 60)
for r in c.execute(
    "SELECT trade_date, recommendation, COUNT(*) FROM report "
    "WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' AND is_deleted=0 "
    "GROUP BY trade_date, recommendation ORDER BY trade_date"
):
    print(' ', r)

c.close()
