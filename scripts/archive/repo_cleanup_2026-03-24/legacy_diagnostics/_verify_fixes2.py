"""Quick verification of ATR display fix."""
import urllib.request

r = urllib.request.urlopen('http://127.0.0.1:8099/reports/7a33dd2b-dc31-453b-92a3-c41216091a8f')
html = r.read().decode()

# Find ATR section in instruction card
search = 'ATR 波动率'
idx = html.find(search)
if idx >= 0:
    print("=== ATR Instruction Card ===")
    print(html[idx:idx+200])
else:
    print("ATR 波动率 not found")

# Find terminology section
search2 = 'ATR（平均真实波幅）'
idx2 = html.find(search2)
if idx2 >= 0:
    print("\n=== ATR Terminology ===")
    print(html[idx2:idx2+300])

# Check homepage 30日结算数据
r2 = urllib.request.urlopen('http://127.0.0.1:8099/')
html2 = r2.read().decode()
search3 = '30日结算样本'
idx3 = html2.find(search3)
if idx3 >= 0:
    print("\n=== 30日结算样本 ===")
    print(html2[idx3:idx3+200])
