"""Temporary script to check frontend data issues."""
import sqlite3

conn = sqlite3.connect("data/app.db")
conn.row_factory = sqlite3.Row
c = conn.cursor()

print("=== ATR check for report 7a33dd2b ===")
rows = c.execute(
    "SELECT report_id, atr_pct, atr_multiplier, signal_entry_price, stop_loss, stop_loss_calc_mode "
    "FROM instruction_card WHERE report_id LIKE '7a33dd2b%'"
).fetchall()
for r in rows:
    d = dict(r)
    print(d)
    if d.get("atr_pct"):
        print(f"  -> atr_pct raw = {d['atr_pct']}, displayed as {d['atr_pct']*100:.2f}%")

print("\n=== Settlement vs Report counts ===")
settled = c.execute(
    "SELECT COUNT(*) FROM settlement_result WHERE settlement_status = 'settled'"
).fetchone()[0]
reports = c.execute(
    "SELECT COUNT(*) FROM report WHERE is_deleted = 0 AND published = 1"
).fetchone()[0]
latest = c.execute(
    "SELECT MAX(trade_date) FROM report WHERE is_deleted = 0 AND published = 1"
).fetchone()[0]
print(f"Total settled: {settled}")
print(f"Total reports: {reports}")
print(f"Latest trade_date: {latest}")

# Check window counts
from datetime import date, timedelta
if latest:
    latest_date = date.fromisoformat(latest)
    window_start = (latest_date - timedelta(days=29)).isoformat()
    win_settled = c.execute(
        "SELECT COUNT(*) FROM settlement_result WHERE signal_date BETWEEN ? AND ? AND settlement_status = 'settled'",
        (window_start, latest),
    ).fetchone()[0]
    win_reports = c.execute(
        "SELECT COUNT(*) FROM report WHERE is_deleted = 0 AND published = 1 AND trade_date BETWEEN ? AND ?",
        (window_start, latest),
    ).fetchone()[0]
    print(f"\n30-day window ({window_start} ~ {latest}):")
    print(f"  Reports in window: {win_reports}")
    print(f"  Settled in window: {win_settled}")

print("\n=== ATR distribution (all reports) ===")
rows = c.execute(
    "SELECT atr_pct FROM instruction_card WHERE atr_pct IS NOT NULL ORDER BY atr_pct DESC LIMIT 20"
).fetchall()
for r in rows:
    print(f"  atr_pct = {r[0]}, displayed as {r[0]*100:.2f}%")

print("\n=== Pool size check ===")
# Check daily_stock_pool
try:
    rows = c.execute(
        "SELECT trade_date, COUNT(*) as cnt FROM daily_stock_pool GROUP BY trade_date ORDER BY trade_date DESC LIMIT 5"
    ).fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]} stocks")
except Exception as e:
    print(f"  daily_stock_pool table error: {e}")

print("\n=== Dashboard API simulation ===")
# Call the actual API
import urllib.request
import json
try:
    resp = urllib.request.urlopen("http://127.0.0.1:8099/api/v1/dashboard/stats?window_days=30")
    data = json.loads(resp.read())
    d = data.get("data", data)
    print(f"  total_reports: {d.get('total_reports')}")
    print(f"  total_settled: {d.get('total_settled')}")
    print(f"  baseline_random: {d.get('baseline_random')}")
    print(f"  baseline_ma_cross: {d.get('baseline_ma_cross')}")
    print(f"  data_status: {d.get('data_status')}")
    print(f"  display_hint: {d.get('display_hint')}")
except Exception as e:
    print(f"  API error: {e}")

conn.close()
