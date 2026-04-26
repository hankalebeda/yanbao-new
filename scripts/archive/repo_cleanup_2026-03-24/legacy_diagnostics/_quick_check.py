import sqlite3
c = sqlite3.connect("data/app.db").cursor()
r = c.execute(
    "SELECT signal_entry_price, atr_pct, stop_loss, target_price FROM instruction_card WHERE report_id = '7a33dd2b-dc31-453b-92a3-c41216091a8f'"
).fetchone()
print(f"entry={r[0]}, atr={r[1]}%, sl={r[2]}, tp={r[3]}")
