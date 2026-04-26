import sqlite3, json
c = sqlite3.connect('data/app.db')
cur = c.cursor()
def q(s):
    cur.execute(s); return cur.fetchall()
out = {}
out['report_total'] = q('SELECT COUNT(*) FROM report')[0][0]
out['report_alive'] = q("SELECT COUNT(*) FROM report WHERE is_deleted=0 OR is_deleted IS NULL")[0][0]
out['report_deleted'] = q("SELECT COUNT(*) FROM report WHERE is_deleted=1")[0][0]
out['report_published'] = q("SELECT COUNT(*) FROM report WHERE published=1 AND (is_deleted=0 OR is_deleted IS NULL)")[0][0]
out['quality_ok'] = q("SELECT COUNT(*) FROM report WHERE quality_flag='ok' AND (is_deleted=0 OR is_deleted IS NULL)")[0][0]
out['by_date'] = q("SELECT trade_date,COUNT(*) FROM report WHERE is_deleted=0 OR is_deleted IS NULL GROUP BY trade_date ORDER BY trade_date DESC LIMIT 15")
out['by_quality'] = q("SELECT quality_flag,COUNT(*) FROM report WHERE is_deleted=0 OR is_deleted IS NULL GROUP BY quality_flag")
out['by_recommendation'] = q("SELECT recommendation,COUNT(*) FROM report WHERE is_deleted=0 OR is_deleted IS NULL GROUP BY recommendation")
out['kline_stocks'] = q("SELECT COUNT(DISTINCT stock_code) FROM kline_daily")[0][0]
out['kline_dates'] = q("SELECT COUNT(DISTINCT trade_date) FROM kline_daily")[0][0]
out['kline_latest'] = q("SELECT MAX(trade_date) FROM kline_daily")[0][0]
out['hotspot_raw'] = q("SELECT COUNT(*) FROM hotspot_raw")[0][0]
out['hotspot_top50'] = q("SELECT COUNT(*) FROM hotspot_top50")[0][0]
out['settlement_total'] = q("SELECT COUNT(*) FROM settlement_result")[0][0]
try:
    out['settlement_misclassified'] = q("SELECT COUNT(*) FROM settlement_result WHERE is_misclassified=1")[0][0]
    out['settlement_good'] = q("SELECT COUNT(*) FROM settlement_result WHERE is_misclassified=0")[0][0]
except Exception as e:
    out['settlement_err'] = str(e)
out['market_state'] = q("SELECT MIN(trade_date),MAX(trade_date),COUNT(*) FROM market_state_cache")[0]
for t in ('northbound_summary','etf_flow_summary','report_data_usage'):
    try:
        out[t] = q(f'SELECT COUNT(*) FROM {t}')[0][0]
    except Exception as e:
        out[t] = f'ERR: {e}'
try:
    out['task_status'] = q("SELECT stage, status, COUNT(*), MAX(updated_at) FROM pipeline_task_status GROUP BY stage, status")
except Exception as e:
    out['task_status_err'] = str(e)
print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
