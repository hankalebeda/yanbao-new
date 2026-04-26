"""
Fix stop_loss and target_price in instruction_card table.

The bug: atr_pct was stored as percentage (e.g. 7.93 = 7.93%) 
but used as ratio in calculation:
  stop_loss = entry * (1 - atr_pct * multiplier)  # WRONG
  stop_loss = entry * (1 - atr_pct/100 * multiplier)  # CORRECT

This script recalculates all instruction_card stop_loss and target_price 
using the correct formula.
"""
import sqlite3

conn = sqlite3.connect("data/app.db")
conn.row_factory = sqlite3.Row
c = conn.cursor()

rows = c.execute("""
    SELECT instruction_card_id, report_id, signal_entry_price, atr_pct, atr_multiplier, 
           stop_loss, target_price, stop_loss_calc_mode
    FROM instruction_card
    WHERE atr_pct IS NOT NULL AND atr_pct > 0
      AND stop_loss_calc_mode = 'atr_multiplier'
""").fetchall()

print(f"Total instruction_card rows to fix: {len(rows)}")

fixed = 0
skipped = 0
for row in rows:
    entry = row["signal_entry_price"]
    atr_pct = row["atr_pct"]
    mult = row["atr_multiplier"] or 2.0
    old_sl = row["stop_loss"]
    
    if entry is None or entry <= 0:
        skipped += 1
        continue
    
    # Correct calculation: atr_pct is percentage, divide by 100
    atr_ratio = atr_pct / 100.0
    new_sl = round(entry * (1 - atr_ratio * mult), 4)
    
    # FR06-LLM-09: stop_loss >= entry → logic_inversion_fallback
    if new_sl >= entry:
        new_tp = None
        new_mode = "logic_inversion_fallback"
    else:
        new_tp = round(entry + (entry - new_sl) * 1.5, 4)
        new_mode = "atr_multiplier"
    
    c.execute("""
        UPDATE instruction_card
        SET stop_loss = ?, target_price = ?, stop_loss_calc_mode = ?
        WHERE instruction_card_id = ?
    """, (new_sl, new_tp, new_mode, row["instruction_card_id"]))
    fixed += 1
    
    if fixed <= 3:
        print(f"  Example: entry={entry}, atr_pct={atr_pct}%, mult={mult}")
        print(f"    old_sl={old_sl} -> new_sl={new_sl}, new_tp={new_tp}")

conn.commit()
print(f"\nFixed: {fixed}, Skipped: {skipped}")

# Verify
sample = c.execute("""
    SELECT signal_entry_price, atr_pct, stop_loss, target_price 
    FROM instruction_card 
    WHERE stop_loss_calc_mode = 'atr_multiplier' 
    LIMIT 5
""").fetchall()
print("\n=== Verification ===")
for r in sample:
    ratio = (r["signal_entry_price"] - r["stop_loss"]) / r["signal_entry_price"] * 100 if r["signal_entry_price"] > 0 else 0
    print(f"  entry={r['signal_entry_price']}, atr={r['atr_pct']}%, sl={r['stop_loss']}, tp={r['target_price']}, sl_pct={ratio:.1f}%")

conn.close()
