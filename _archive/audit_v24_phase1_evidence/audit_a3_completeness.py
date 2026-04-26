"""Phase A3: data completeness audit.
READ-ONLY against data/app.db. Outputs JSON + human summary.
"""
import json
import sqlite3
from datetime import datetime
from pathlib import Path

DB = Path('data/app.db')
OUT_JSON = Path('_archive/audit_v24_phase1_evidence/audit_a3_completeness.json')

c = sqlite3.connect(str(DB))
c.row_factory = sqlite3.Row

result: dict = {'ts': datetime.now().isoformat(), 'db': str(DB.absolute())}


def q(sql: str, params: tuple = ()):
    return [dict(r) for r in c.execute(sql, params).fetchall()]


# 1. report counts
result['totals'] = q("SELECT COUNT(*) AS total, SUM(CASE WHEN is_deleted=0 THEN 1 ELSE 0 END) AS alive, SUM(CASE WHEN is_deleted=1 THEN 1 ELSE 0 END) AS deleted FROM report")[0]

# 2. quality_flag dist (alive)
result['quality_flag_dist'] = q("SELECT quality_flag, COUNT(*) AS cnt FROM report WHERE is_deleted=0 GROUP BY quality_flag ORDER BY cnt DESC")

# 3. llm_fallback_level
result['llm_fallback_dist'] = q("SELECT llm_fallback_level, COUNT(*) AS cnt FROM report WHERE is_deleted=0 GROUP BY llm_fallback_level ORDER BY cnt DESC")

# 4. failure_category
result['failure_category_dist'] = q("SELECT failure_category, COUNT(*) AS cnt FROM report WHERE is_deleted=0 GROUP BY failure_category ORDER BY cnt DESC")

# 5. status_reason for degraded reports (top buckets)
result['degraded_status_reason'] = q("SELECT status_reason, COUNT(*) AS cnt FROM report WHERE is_deleted=0 AND quality_flag='degraded' GROUP BY status_reason ORDER BY cnt DESC LIMIT 20")

# 6. content_json missing (per trade_date)
result['content_json_missing'] = q(
    "SELECT trade_date, COUNT(*) AS missing FROM report WHERE is_deleted=0 AND (content_json IS NULL OR content_json='' OR content_json='{}') "
    "GROUP BY trade_date ORDER BY trade_date"
)
result['content_json_missing_total'] = q(
    "SELECT COUNT(*) AS n FROM report WHERE is_deleted=0 AND (content_json IS NULL OR content_json='' OR content_json='{}')"
)[0]['n']

# 7. conclusion_text empty/short
result['conclusion_text_short'] = q(
    "SELECT trade_date, COUNT(*) AS n FROM report WHERE is_deleted=0 AND (conclusion_text IS NULL OR LENGTH(TRIM(conclusion_text))<50) GROUP BY trade_date ORDER BY trade_date"
)
result['conclusion_text_short_total'] = q(
    "SELECT COUNT(*) AS n FROM report WHERE is_deleted=0 AND (conclusion_text IS NULL OR LENGTH(TRIM(conclusion_text))<50)"
)[0]['n']

# 8. reasoning_chain_md empty
result['reasoning_chain_empty'] = q(
    "SELECT trade_date, COUNT(*) AS n FROM report WHERE is_deleted=0 AND (reasoning_chain_md IS NULL OR LENGTH(TRIM(reasoning_chain_md))<50) GROUP BY trade_date ORDER BY trade_date"
)
result['reasoning_chain_empty_total'] = q(
    "SELECT COUNT(*) AS n FROM report WHERE is_deleted=0 AND (reasoning_chain_md IS NULL OR LENGTH(TRIM(reasoning_chain_md))<50)"
)[0]['n']

# 9. confidence NULL
result['confidence_null'] = q(
    "SELECT trade_date, COUNT(*) AS n FROM report WHERE is_deleted=0 AND confidence IS NULL GROUP BY trade_date ORDER BY trade_date"
)
result['confidence_null_total'] = q("SELECT COUNT(*) AS n FROM report WHERE is_deleted=0 AND confidence IS NULL")[0]['n']

# 10. report_data_usage coverage by source per date
result['data_usage_by_source'] = q(
    "SELECT trade_date, source_name, COUNT(DISTINCT stock_code) AS stocks_with_usage "
    "FROM report_data_usage WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' "
    "GROUP BY trade_date, source_name ORDER BY trade_date, source_name"
)

# 11. reports without ANY usage row
result['reports_without_usage'] = q(
    "SELECT r.trade_date, COUNT(*) AS missing FROM report r "
    "WHERE r.is_deleted=0 AND r.trade_date BETWEEN '2026-04-07' AND '2026-04-16' "
    "AND NOT EXISTS (SELECT 1 FROM report_data_usage u WHERE u.stock_code=r.stock_code AND u.trade_date=r.trade_date) "
    "GROUP BY r.trade_date ORDER BY r.trade_date"
)

# 12. reports missing specific critical sources (kline / market_state)
for src in ('kline_qfq_history', 'market_state', 'tencent_kline', 'kline'):
    result[f'reports_missing_{src}'] = q(
        f"SELECT r.trade_date, COUNT(*) AS missing FROM report r "
        f"WHERE r.is_deleted=0 AND r.trade_date BETWEEN '2026-04-07' AND '2026-04-16' "
        f"AND NOT EXISTS (SELECT 1 FROM report_data_usage u WHERE u.stock_code=r.stock_code AND u.trade_date=r.trade_date AND u.source_name='{src}') "
        f"GROUP BY r.trade_date ORDER BY r.trade_date"
    )

# 13. distinct source_name values
result['distinct_source_names'] = [r['source_name'] for r in c.execute(
    "SELECT DISTINCT source_name FROM report_data_usage WHERE trade_date >= '2026-04-07' ORDER BY source_name"
).fetchall()]

# 14. failed/partial data_batch & top errors
result['data_batch_status'] = q("SELECT source_name, batch_status, COUNT(*) AS n FROM data_batch WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' GROUP BY source_name, batch_status ORDER BY source_name")
result['data_batch_error_top'] = q("SELECT error_code, error_stage, COUNT(*) AS n FROM data_batch_error WHERE created_at >= '2026-04-07' GROUP BY error_code, error_stage ORDER BY n DESC LIMIT 10")

# 15. settlement coverage
try:
    result['settlement_count'] = q("SELECT COUNT(*) AS n FROM settlement")[0]['n']
    result['settlement_misclassified'] = q("SELECT COUNT(*) AS n FROM settlement WHERE COALESCE(is_misclassified,0)=1")[0]['n']
except sqlite3.OperationalError as e:
    result['settlement_error'] = str(e)

# 16. tasks not Completed
result['task_status_dist'] = q(
    "SELECT trade_date, status, COUNT(*) AS n FROM report_generation_task "
    "WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' "
    "GROUP BY trade_date, status ORDER BY trade_date, status"
)

# 17. market_state_cache anomalies
result['market_state_cache'] = q(
    "SELECT trade_date, market_state, kline_batch_id, hotspot_batch_id, a_type_pct, b_type_pct, c_type_pct "
    "FROM market_state_cache WHERE trade_date BETWEEN '2026-04-07' AND '2026-04-16' ORDER BY trade_date"
)

OUT_JSON.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')

# human summary
print('=' * 70)
print('A3 DATA COMPLETENESS AUDIT — SUMMARY')
print('=' * 70)
print(f"DB: {result['db']}")
print(f"reports total={result['totals']['total']} alive={result['totals']['alive']} deleted={result['totals']['deleted']}")
print()
print('quality_flag dist (alive):')
for r in result['quality_flag_dist']:
    print(f"  {r['quality_flag']}: {r['cnt']}")
print()
print('llm_fallback_level dist:')
for r in result['llm_fallback_dist']:
    print(f"  {r['llm_fallback_level']}: {r['cnt']}")
print()
print(f"content_json missing: {result['content_json_missing_total']}")
for r in result['content_json_missing']:
    print(f"  {r['trade_date']}: {r['missing']}")
print()
print(f"conclusion_text short(<50): {result['conclusion_text_short_total']}")
for r in result['conclusion_text_short']:
    print(f"  {r['trade_date']}: {r['n']}")
print()
print(f"reasoning_chain empty: {result['reasoning_chain_empty_total']}")
for r in result['reasoning_chain_empty']:
    print(f"  {r['trade_date']}: {r['n']}")
print()
print(f"confidence NULL: {result['confidence_null_total']}")
for r in result['confidence_null']:
    print(f"  {r['trade_date']}: {r['n']}")
print()
print('distinct source_names in usage (>=04-07):')
print('  ', result['distinct_source_names'])
print()
print('reports_without_usage:')
for r in result['reports_without_usage']:
    print(f"  {r['trade_date']}: {r['missing']}")
print()
print('data_batch_error top:')
for r in result['data_batch_error_top']:
    print(f"  {r['error_code']} / {r['error_stage']}: {r['n']}")
print()
if 'settlement_count' in result:
    print(f"settlement: total={result['settlement_count']} misclassified={result['settlement_misclassified']}")
print()
print('JSON written to', OUT_JSON)

c.close()
