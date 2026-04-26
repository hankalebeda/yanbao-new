import sqlite3
c = sqlite3.connect('data/app.db')
print('--- market_state_cache 04-07..04-16 ---')
cols = [r[1] for r in c.execute('PRAGMA table_info(market_state_cache)').fetchall()]
print('cols:', cols)
for r in c.execute(
    "SELECT * FROM market_state_cache WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' ORDER BY trade_date"
):
    print(dict(zip(cols, r)))

print()
print('--- stocks first 5 with BEAR_MARKET_FILTERED on 04-08 ---')
for r in c.execute(
    "SELECT stock_code, market_state, market_state_reference_date, market_state_degraded, "
    "substr(market_state_reason_snapshot,1,200) "
    "FROM report WHERE trade_date='2026-04-08' AND is_deleted=1 AND status_reason='BEAR_MARKET_FILTERED' LIMIT 5"
):
    print(r)

print()
print('--- 04-09 ok sample market_state ---')
for r in c.execute(
    "SELECT stock_code, market_state, market_state_reference_date, market_state_degraded "
    "FROM report WHERE trade_date='2026-04-09' AND is_deleted=0 LIMIT 3"
):
    print(r)
