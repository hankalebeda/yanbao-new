"""Check industry data and company overview."""
import sqlite3, json

conn = sqlite3.connect("data/app.db")
conn.row_factory = sqlite3.Row
c = conn.cursor()

print("=== stock_master ===")
r = c.execute("SELECT stock_code, stock_name, industry FROM stock_master WHERE stock_code = '603629.SH'").fetchone()
print(dict(r) if r else "NOT FOUND")

print("\n=== company_overview from report ===")
r2 = c.execute("SELECT report_id, company_overview FROM report WHERE report_id LIKE '7a33dd2b%'").fetchone()
if r2:
    ov = r2["company_overview"]
    if ov:
        try:
            d = json.loads(ov)
            print(json.dumps(d, ensure_ascii=False, indent=2)[:500])
        except:
            print(ov[:300])
    else:
        print("company_overview is NULL")

print("\n=== industry values ===")
rows = c.execute("SELECT COUNT(*), industry FROM stock_master GROUP BY industry ORDER BY COUNT(*) DESC LIMIT 10").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]}")

conn.close()
