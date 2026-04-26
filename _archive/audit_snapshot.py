#!/usr/bin/env python3
import sqlite3, json

conn = sqlite3.connect('data/app.db')
conn.row_factory = sqlite3.Row
c = conn.cursor()

q = {}
q['report_total'] = c.execute('select count(*) from report').fetchone()[0]
q['report_visible'] = c.execute("select count(*) from report where published=1 and is_deleted=0").fetchone()[0]
q['report_visible_ok'] = c.execute("select count(*) from report where published=1 and is_deleted=0 and lower(coalesce(quality_flag,'ok'))='ok'").fetchone()[0]
q['report_visible_non_ok'] = c.execute("select count(*) from report where published=1 and is_deleted=0 and lower(coalesce(quality_flag,'ok'))<>'ok'").fetchone()[0]
q['stock_master_total'] = c.execute('select count(*) from stock_master').fetchone()[0]
q['kline_stock_covered'] = c.execute('select count(distinct stock_code) from kline_daily').fetchone()[0]
q['kline_rows'] = c.execute('select count(*) from kline_daily').fetchone()[0]
q['settlement_distinct_reports'] = c.execute('select count(distinct report_id) from settlement_result').fetchone()[0]
q['settlement_rows'] = c.execute('select count(*) from settlement_result').fetchone()[0]
q['quality_breakdown'] = [dict(r) for r in c.execute("select lower(coalesce(quality_flag,'ok')) as quality_flag, count(*) as cnt from report where published=1 and is_deleted=0 group by lower(coalesce(quality_flag,'ok')) order by cnt desc")]

conn.close()
print(json.dumps(q, ensure_ascii=False, indent=2))
