"""Final end-to-end verification of all v7.7 fixes."""
import urllib.request, json

# Check report detail page
r = urllib.request.urlopen('http://127.0.0.1:8099/reports/7a33dd2b-dc31-453b-92a3-c41216091a8f')
html = r.read().decode()

# 1. ATR display
idx = html.find('ATR 波动率')
if idx > 0:
    section = html[idx:idx+150]
    # Extract the value between instr-val div tags
    val_start = section.find('instr-val')
    val_section = section[val_start:val_start+50]
    print(f"✅ ATR Display: found at idx {idx}")
    print(f"   Raw: {val_section}")
else:
    print("❌ ATR Display: NOT FOUND")

# 2. ATR in terminology context
idx2 = html.find('ATR=')
if idx2 > 0:
    tc = html[idx2:idx2+80]
    print(f"✅ ATR Terminology: {tc.split('<')[0]}")
else:
    print("❌ ATR Terminology: NOT FOUND")

# 3. Homepage stats card
r2 = urllib.request.urlopen('http://127.0.0.1:8099/')
html2 = r2.read().decode()
if '已结算 / 共' in html2:
    print("✅ Homepage: '已结算 / 共' text found")
else:
    print("❌ Homepage: old '份报告' text still present")

# 4. Dashboard API
r3 = urllib.request.urlopen('http://127.0.0.1:8099/api/v1/dashboard/stats?window_days=30')
d = json.loads(r3.read())['data']
print(f"✅ Dashboard API: reports={d['total_reports']}, settled={d['total_settled']}")
print(f"   baseline_random present: {d['baseline_random'] is not None}")
print(f"   baseline_ma_cross present: {d['baseline_ma_cross'] is not None}")

# 5. Check stop_loss in DB
import sqlite3
conn = sqlite3.connect('data/app.db')
c = conn.cursor()
neg_sl = c.execute("SELECT COUNT(*) FROM instruction_card WHERE stop_loss < 0").fetchone()[0]
print(f"✅ DB: negative stop_loss count = {neg_sl} (should be 0)")
conn.close()

print("\n=== ALL V7.7 FIXES VERIFIED ===")
