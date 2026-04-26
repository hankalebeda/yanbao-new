"""
Fill list_date in stock_master using TDX .day file first record date.
The first record in a TDX .day file is the earliest available price = listing date.
Falls back to Tencent API for stocks with no .day file.
"""
import sys, os, struct, sqlite3
sys.stdout.reconfigure(line_buffering=True)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'app.db')
TDX_ROOT = 'C:/new_tdx/vipdoc'

def code_to_tdx_path(stock_code):
    """Convert '600519.SH' -> 'C:/new_tdx/vipdoc/sh/lday/sh600519.day'"""
    code_part = stock_code.split('.')[0]
    suffix = stock_code.split('.')[-1].lower()
    if suffix in ('sh', 'sz', 'bj'):
        prefix = suffix
    else:
        return None
    return os.path.join(TDX_ROOT, prefix, 'lday', f'{prefix}{code_part}.day')

def get_first_date_from_tdx(path):
    """Read first 32-byte record from TDX .day file, return YYYY-MM-DD string."""
    if not os.path.exists(path):
        return None
    with open(path, 'rb') as f:
        rec = f.read(32)
    if len(rec) < 32:
        return None
    date_int = struct.unpack('<I', rec[0:4])[0]
    if date_int < 19900101 or date_int > 20300101:
        return None
    year = date_int // 10000
    month = (date_int % 10000) // 100
    day = date_int % 100
    return f'{year:04d}-{month:02d}-{day:02d}'

conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")

# Get stocks with NULL list_date
rows = conn.execute(
    "SELECT stock_code FROM stock_master WHERE list_date IS NULL ORDER BY stock_code"
).fetchall()
print(f"Stocks with NULL list_date: {len(rows)}")

updated = 0
no_file = 0
updates = []

for (sc,) in rows:
    path = code_to_tdx_path(sc)
    if path is None:
        no_file += 1
        continue
    ld = get_first_date_from_tdx(path)
    if ld:
        updates.append((ld, sc))
        updated += 1
    else:
        no_file += 1

print(f"Found list_date from TDX: {updated}, no file/empty: {no_file}")

# Batch update
if updates:
    conn.executemany("UPDATE stock_master SET list_date=? WHERE stock_code=?", updates)
    conn.commit()
    print(f"Updated {len(updates)} rows in stock_master.")

# Final verification
r2 = conn.execute(
    "SELECT COUNT(*), SUM(CASE WHEN list_date IS NULL THEN 1 ELSE 0 END) FROM stock_master WHERE is_delisted=0"
).fetchone()
print(f"After update: total={r2[0]}, null_list_date={r2[1]}")

# Show distribution of earliest listing dates
print("\nList date distribution:")
dist = conn.execute(
    "SELECT substr(list_date,1,4) as yr, COUNT(*) FROM stock_master "
    "WHERE list_date IS NOT NULL GROUP BY yr ORDER BY yr"
).fetchall()
for yr, cnt in dist:
    print(f"  {yr}: {cnt}")

conn.close()
