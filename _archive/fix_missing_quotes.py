"""Fix missing closing quotes on Chinese strings."""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    t = f.read()

# Fix: '总览接口不可用; -> '总览接口不可用';
t = t.replace("'总览接口不可用;", "'总览接口不可用';")
print("Fixed 总览接口不可用")

# Scan for other broken strings: '中文; pattern (missing closing quote)
# Look for '(non-ASCII chars); where the string isn't closed
pattern = re.compile(r"= '([^']*[\u4e00-\u9fff][^']*);$", re.MULTILINE)
for m in pattern.finditer(t):
    pos = m.start()
    line_no = t[:pos].count("\n") + 1
    if 1559 <= line_no <= 2153 or 603 <= line_no <= 1200:
        print(f"  Broken quote line {line_no}: ...= '{m.group(1)[:30]};")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(t)
print("Saved.")
