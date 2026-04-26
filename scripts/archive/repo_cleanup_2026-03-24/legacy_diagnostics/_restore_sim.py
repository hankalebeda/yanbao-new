"""Restore original sim_account + equity values, then re-run fix with correct cash debit."""
import sqlite3

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()

# Restore original equity curve points
originals = [
    ("10k", "2026-03-11", 100050.82, 91810.82, 8240),
    ("10k", "2026-03-12", 99902.82, 91810.82, 8092),
    ("100k", "2026-03-11", 2056694, 1000000, 1056694),
    ("100k", "2026-03-12", 2027132, 1000000, 1027132),
    ("500k", "2026-03-11", 13203745.18, 3499875.18, 9703870),
    ("500k", "2026-03-12", 12944943.18, 3499875.18, 9445068),
]
for tier, td, eq, cash, mv in originals:
    cur.execute(
        "UPDATE sim_equity_curve_point SET equity=?, cash_available=?, position_market_value=? "
        "WHERE capital_tier=? AND trade_date=?",
        (eq, cash, mv, tier, td)
    )

# Restore original sim_account
cur.execute("UPDATE sim_account SET initial_cash=100000, cash_available=91810.82, "
            "total_asset=99902.82, peak_total_asset=100050.82, max_drawdown_pct=-0.001479 "
            "WHERE capital_tier='10k'")
cur.execute("UPDATE sim_account SET initial_cash=1000000, cash_available=1000000, "
            "total_asset=2027132, peak_total_asset=2056694, max_drawdown_pct=-0.014374 "
            "WHERE capital_tier='100k'")
cur.execute("UPDATE sim_account SET initial_cash=5000000, cash_available=3499875.18, "
            "total_asset=12944943.18, peak_total_asset=13203745.18, max_drawdown_pct=-0.019601 "
            "WHERE capital_tier='500k'")

# Restore dashboard snapshots
cur.execute("UPDATE sim_dashboard_snapshot SET total_return_pct=-0.000972, max_drawdown_pct=-0.001479 "
            "WHERE capital_tier='10k'")
cur.execute("UPDATE sim_dashboard_snapshot SET total_return_pct=1.027132, max_drawdown_pct=-0.014374 "
            "WHERE capital_tier='100k'")
cur.execute("UPDATE sim_dashboard_snapshot SET total_return_pct=1.588989, max_drawdown_pct=-0.019601 "
            "WHERE capital_tier='500k'")

conn.commit()
conn.close()
print("Restored all original values")
