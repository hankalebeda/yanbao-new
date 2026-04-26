"""Diagnose DEPENDENCY_NOT_READY for report generation."""
from sqlalchemy import text, create_engine
from datetime import date

e = create_engine("sqlite:///./data/app.db")
with e.connect() as c:
    r = c.execute(text("SELECT MAX(trade_date) as max_td, COUNT(DISTINCT trade_date) as dates FROM kline_daily")).fetchone()
    print(f"Kline max trade_date={r.max_td} distinct_dates={r.dates}")

    r = c.execute(text("SELECT trade_date, market_state FROM market_state_cache ORDER BY trade_date DESC LIMIT 3")).fetchall()
    print(f"Market states: {[(x.trade_date, x.market_state) for x in r]}")

    r = c.execute(text("SELECT trade_date, COUNT(*) as cnt FROM report_data_usage WHERE stock_code='688702.SH' GROUP BY trade_date")).fetchall()
    print(f"Data usage dates for 688702.SH: {[(x.trade_date, x.cnt) for x in r]}")

    # Check today's kline for 688702
    today = str(date.today())
    r = c.execute(text("SELECT COUNT(*) as cnt FROM kline_daily WHERE trade_date = :td"), {"td": today}).fetchone()
    print(f"Kline rows for today ({today}): {r.cnt}")

    r = c.execute(text("SELECT COUNT(*) as cnt FROM kline_daily WHERE trade_date = '2026-03-10'")).fetchone()
    print(f"Kline rows for 2026-03-10: {r.cnt}")

    # Check report_data_usage for today
    r = c.execute(text("SELECT COUNT(*) as cnt FROM report_data_usage WHERE trade_date = :td"), {"td": today}).fetchone()
    print(f"Data usage rows for today ({today}): {r.cnt}")

    r = c.execute(text("SELECT COUNT(*) as cnt FROM report_data_usage WHERE trade_date = '2026-03-10'")).fetchone()
    print(f"Data usage rows for 2026-03-10: {r.cnt}")

    # What stock pool returns
    print(f"\nToday: {today}")
