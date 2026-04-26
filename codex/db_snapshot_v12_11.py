"""DB snapshot for v12.11 audit"""
import sqlite3, json

db = sqlite3.connect(r'd:\yanbao-new\data\app.db')
c = db.cursor()

# report snapshot
c.execute('SELECT COUNT(*) FROM report')
total = c.fetchone()[0]
c.execute('SELECT COUNT(*) FROM report WHERE is_deleted=0 OR is_deleted IS NULL')
alive = c.fetchone()[0]
c.execute('SELECT COUNT(*) FROM report WHERE is_deleted=1')
deleted = c.fetchone()[0]
c.execute('SELECT COUNT(*) FROM report WHERE published=1 AND (is_deleted=0 OR is_deleted IS NULL)')
pub_alive = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM report WHERE quality_flag='ok' AND (is_deleted=0 OR is_deleted IS NULL)")
ok_alive = c.fetchone()[0]

# trade_date distribution
c.execute('SELECT trade_date, COUNT(*) FROM report WHERE is_deleted=0 OR is_deleted IS NULL GROUP BY trade_date ORDER BY trade_date DESC LIMIT 10')
td_dist = c.fetchall()

# field missing analysis
c.execute('SELECT COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND (conclusion_text IS NULL OR LENGTH(conclusion_text)<120)')
missing_conclusion = c.fetchone()[0]

c.execute('SELECT COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND (reasoning_chain_md IS NULL OR LENGTH(reasoning_chain_md)<200)')
missing_reasoning = c.fetchone()[0]

c.execute('SELECT COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND market_state IS NULL')
null_ms = c.fetchone()[0]

c.execute("SELECT COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND quality_flag!='ok'")
bad_quality = c.fetchone()[0]

# content_json empty / indicators missing
c.execute('SELECT COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND (content_json IS NULL OR content_json="{}" OR content_json="")')
empty_cj = c.fetchone()[0]

# recommendation distribution
c.execute('SELECT recommendation, COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) GROUP BY recommendation')
rec_dist = c.fetchall()

# strategy_type distribution
c.execute('SELECT strategy_type, COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) GROUP BY strategy_type')
st_dist = c.fetchall()

print('=== REPORT SNAPSHOT ===')
print(f'total={total}, alive={alive}, deleted={deleted}, published_alive={pub_alive}, ok_alive={ok_alive}')
print(f'missing_conclusion(<120)={missing_conclusion}')
print(f'missing_reasoning(<200)={missing_reasoning}')
print(f'null_market_state={null_ms}')
print(f'bad_quality_flag(!=ok)={bad_quality}')
print(f'empty_content_json={empty_cj}')
print(f'trade_date distribution: {td_dist}')
print(f'recommendation distribution: {rec_dist}')
print(f'strategy_type distribution: {st_dist}')

# settlement
c.execute('SELECT COUNT(*) FROM settlement_result')
st_total = c.fetchone()[0]
c.execute('SELECT COUNT(*) FROM settlement_result WHERE is_misclassified=0')
st_ok = c.fetchone()[0]
c.execute('SELECT COUNT(*) FROM settlement_result WHERE is_misclassified=1')
st_bad = c.fetchone()[0]
print(f'=== SETTLEMENT: total={st_total}, ok={st_ok}, misclassified={st_bad} ===')

# kline
try:
    c.execute('SELECT COUNT(DISTINCT stock_code) FROM kline_daily')
    kl_stocks = c.fetchone()[0]
    c.execute('SELECT MAX(trade_date) FROM kline_daily')
    kl_max = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM kline_daily')
    kl_rows = c.fetchone()[0]
    print(f'=== KLINE: unique_stocks={kl_stocks}, latest_date={kl_max}, total_rows={kl_rows} ===')
except Exception as e:
    print(f'kline error: {e}')

# hotspot
try:
    c.execute('SELECT COUNT(*) FROM hotspot_raw')
    hs_raw = c.fetchone()[0]
    print(f'=== HOTSPOT: hotspot_raw={hs_raw} ===')
except Exception as e:
    print(f'hotspot error: {e}')

# market_state_cache
try:
    c.execute('SELECT COUNT(*), MAX(trade_date) FROM market_state_cache')
    mc = c.fetchone()
    print(f'=== MARKET_STATE_CACHE: count={mc[0]}, latest={mc[1]} ===')
except Exception as e:
    print(f'msc error: {e}')

# data_usage_fact
try:
    c.execute('SELECT COUNT(*) FROM data_usage_fact')
    duf = c.fetchone()[0]
    print(f'=== DATA_USAGE_FACT: count={duf} ===')
except Exception as e:
    print(f'data_usage_fact: {e}')

# report_data_usage (legacy)
try:
    c.execute('SELECT COUNT(*) FROM report_data_usage')
    rdu = c.fetchone()[0]
    print(f'=== REPORT_DATA_USAGE (legacy): count={rdu} ===')
except Exception as e:
    print(f'report_data_usage: {e}')

# stock_master
try:
    c.execute('SELECT COUNT(*) FROM stock_master')
    sm = c.fetchone()[0]
    print(f'=== STOCK_MASTER: count={sm} ===')
except Exception as e:
    print(f'stock_master: {e}')

# candidate pool (pool_task)
try:
    c.execute('SELECT COUNT(DISTINCT stock_code) FROM pool_task WHERE status="active" OR status="standby"')
    pool = c.fetchone()[0]
    print(f'=== POOL_TASK (active/standby unique stocks): count={pool} ===')
except Exception as e:
    print(f'pool_task: {e}')

# Latest 5 reports details
print('=== LATEST 5 ALIVE REPORTS ===')
c.execute("""SELECT stock_code, trade_date, recommendation, quality_flag, published, strategy_type,
    LENGTH(conclusion_text) as cl, LENGTH(reasoning_chain_md) as rl
    FROM report WHERE is_deleted=0 OR is_deleted IS NULL
    ORDER BY created_at DESC LIMIT 5""")
for row in c.fetchall():
    print(row)

db.close()
print('=== DONE ===')
