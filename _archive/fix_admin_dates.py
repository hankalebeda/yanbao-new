"""Fix remaining source date labels and other garbled strings."""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

# Fix: '首页池日期, -> '首页池日期',
text = text.replace("'首页池日期,", "'首页池日期',")
print("Fixed 首页池日期 missing quote")

# Fix: 鏈€鏂板凡鍙戝竷鐮旀姤 -> 最新已发布研报
# Read the exact codepoints
garbled_report = ""
for i, line in enumerate(text.split("\n")):
    if "鏈€鏂板凡鍙戝竷" in line:
        m = re.search(r"'(鏈€鏂板凡鍙戝竷[^']*)'", line)
        if m:
            garbled_report = m.group(1)
            print(f"  Latest report garbled: {repr(garbled_report)}")
            break

if garbled_report:
    text = text.replace(garbled_report, "最新已发布研报")
    print(f"  Fixed 最新已发布研报 ({text.count('最新已发布研报')} occurrences)")

# Also fix: alert('已 + action ... -> already fixed above, check
# And check: '璇锋眰澶辫触锛?' = 请求失败！
text = text.replace("'璇锋眰澶辫触锛?", "'请求失败！'")

# Now scan for ALL remaining single-quoted strings in script block (1560-2153) 
# that have garbled chars or broken syntax
lines = text.split("\n")
issues = 0
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    # Check for any remaining garbled patterns
    has_pua = any("\ue000" <= ch <= "\uf8ff" for ch in line)
    has_broken_quote = bool(re.search(r"'[^']*[\u4e00-\u9fff]\?[,;}\)]", line))
    # Check for high-range CJK-like garbled chars (0x9000-0x9FFF range commonly garbled)
    for m in re.finditer(r"'([^']*)'", line):
        s = m.group(1)
        if any(0x9200 <= ord(c) <= 0x9500 for c in s):
            # Likely garbled
            if not any(c in s for c in "运行成功失败等待跳过锁已占用正常降级计算未知"):
                issues += 1
                print(f"  Suspect garbled line {i+1}: {repr(s[:50])}")
    if has_pua:
        issues += 1
        print(f"  PUA line {i+1}: {line.strip()[:80]}")
    if has_broken_quote:
        issues += 1
        print(f"  Broken quote line {i+1}: {line.strip()[:80]}")

print(f"\nRemaining potential issues: {issues}")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Saved.")
