import sqlite3
conn = sqlite3.connect('data/app.db')
c = conn.cursor()

queries = [
    ("report_total", "SELECT COUNT(1) FROM report"),
    ("report_published", "SELECT COUNT(1) FROM report WHERE published=1 AND is_deleted=0"),
    ("report_ok", "SELECT COUNT(1) FROM report WHERE published=1 AND is_deleted=0 AND quality_flag='ok'"),
    ("report_stale_ok", "SELECT COUNT(1) FROM report WHERE published=1 AND is_deleted=0 AND quality_flag='stale_ok'"),
    ("report_null_conclusion", "SELECT COUNT(1) FROM report WHERE conclusion_text IS NULL"),
    ("settlement_total", "SELECT COUNT(1) FROM settlement_result"),
    ("settlement_misclassified", "SELECT COUNT(1) FROM settlement_result WHERE is_misclassified=1"),
    ("settlement_quality_ok", "SELECT COUNT(1) FROM settlement_result WHERE quality_flag='ok'"),
    ("settlement_quality_stale", "SELECT COUNT(1) FROM settlement_result WHERE quality_flag='stale_ok'"),
    ("settlement_distinct_stocks", "SELECT COUNT(DISTINCT stock_code) FROM settlement_result"),
    ("kline_total", "SELECT COUNT(1) FROM kline_daily"),
    ("kline_distinct_stocks", "SELECT COUNT(DISTINCT stock_code) FROM kline_daily"),
    ("notification_total", "SELECT COUNT(1) FROM notification"),
    ("stock_score_total", "SELECT COUNT(1) FROM stock_score"),
    ("cookie_session_total", "SELECT COUNT(1) FROM cookie_session"),
]

for name, sql in queries:
    c.execute(sql)
    print(f"{name}={c.fetchone()[0]}")

conn.close()
