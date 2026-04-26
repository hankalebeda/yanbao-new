import sqlite3
c = sqlite3.connect('data/app.db')
c.row_factory = sqlite3.Row
r = c.execute("SELECT * FROM report WHERE is_deleted=0 AND quality_flag='degraded' LIMIT 1").fetchone()
print('stock=', r['stock_code'], 'date=', r['trade_date'])
print('ct_len=', len(r['conclusion_text'] or ''), 'rc_len=', len(r['reasoning_chain_md'] or ''))
print('llm_fallback_level=', r['llm_fallback_level'])
print('status_reason=', r['status_reason'])
print('rec=', r['recommendation'], 'conf=', r['confidence'])
print('CT:', repr((r['conclusion_text'] or '')[:300]))
print('RC:', repr((r['reasoning_chain_md'] or '')[:400]))
