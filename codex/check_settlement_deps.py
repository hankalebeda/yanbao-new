"""Check settlement dependencies for 2026-04-13 BUY reports."""
import sqlite3

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()

# Check if BUY reports have instruction_cards
rows = cur.execute(
    "SELECT r.stock_code, r.report_id, r.recommendation, r.published, r.trade_date,"
    "       i.signal_entry_price, i.stop_loss, i.target_price"
    " FROM report r"
    " LEFT JOIN instruction_card i ON i.report_id = r.report_id"
    " WHERE r.trade_date='2026-04-13' AND r.recommendation='BUY' AND r.published=1"
    " LIMIT 5"
).fetchall()
print("BUY reports with instruction_card join (sample):")
for r in rows:
    print(" ", r)

# Check instruction_card count
cnt = cur.execute(
    "SELECT COUNT(*) FROM report r"
    " JOIN instruction_card i ON i.report_id = r.report_id"
    " WHERE r.trade_date='2026-04-13' AND r.recommendation='BUY'"
    " AND r.published=1 AND r.is_deleted=0"
).fetchone()[0]
print(f"\nBUY reports WITH instruction_card JOIN: {cnt}")

# Check total published BUY for 2026-04-13
total = cur.execute(
    "SELECT COUNT(*) FROM report"
    " WHERE trade_date='2026-04-13' AND recommendation='BUY'"
    " AND published=1 AND is_deleted=0"
).fetchone()[0]
print(f"Total published BUY for 2026-04-13: {total}")

# K-line data for 2026-04-14 (exit price day for 1d window)
kl14 = cur.execute(
    "SELECT COUNT(*) FROM kline_daily WHERE trade_date='2026-04-14'"
).fetchone()[0]
print(f"\nK-line entries for 2026-04-14: {kl14}")

# FR07_ELIGIBLE_REPORTS clauses - check what they might be
# These likely filter by quality_flag, is_deleted, published
rows2 = cur.execute(
    "SELECT quality_flag, published, is_deleted, COUNT(*) as cnt"
    " FROM report"
    " WHERE trade_date='2026-04-13' AND recommendation='BUY'"
    " GROUP BY quality_flag, published, is_deleted"
).fetchall()
print("\nReport quality breakdown for 2026-04-13 BUY:")
for r in rows2:
    print(" ", r)

# The settlement uses r.trade_date < :trade_date
# So settlement for 2026-04-13 won't include reports from 2026-04-13
# Settlement SHOULD be for a date > 2026-04-13
# What's the correct settlement date? Check what the script sends
print("\n=== ANALYSIS ===")
print("Settlement condition: r.trade_date < :trade_date")
print("Reports are on 2026-04-13 -> need settlement trade_date > 2026-04-13")
print("i.e., settlement_date = 2026-04-14 (or 2026-04-21 etc.)")

conn.close()
