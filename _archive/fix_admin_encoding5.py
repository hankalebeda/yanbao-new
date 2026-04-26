"""Final targeted fix for admin.html garbled strings."""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

count = 0

# Fix RUNNING entry: '杩愯涓?' -> '运行中'  (note: ? is literal question mark)
old = "\u674d\u8fd0\u6da8\u6d93?"  # 杩愯涓?
c = text.count(old)
if c:
    text = text.replace(old, "\u8fd0\u884c\u4e2d")  # 运行中
    count += c
    print(f"Fixed '杩愯涓?' -> '运行中' ({c}x)")

# Fix any remaining: RUNNING entry may now read 'RUNNING':'运行中,'  (missing closing quote)
c = text.count("'RUNNING':'运行中,")
if c:
    text = text.replace("'RUNNING':'运行中,", "'RUNNING':'运行中',")
    count += c
    print(f"Fixed RUNNING quote ({c}x)")

# Fix UNKNOWN: 鏈煡 -> 未知
old_unknown = "\u93c8\u7164"  # 鏈煡
c = text.count(old_unknown)
if c:
    text = text.replace(old_unknown, "\u672a\u77e5")  # 未知
    count += c
    print(f"Fixed '鏈煡' -> '未知' ({c}x)")

# Fix 寰呭瀹? -> 待审定  (from earlier list)
old_dai = "\u5f85\u5ba1\u5b9a?"
c = text.count(old_dai)
if c:
    text = text.replace(old_dai, "待审定")
    count += c

# Fix 寰呭瀹＄爺鎶? -> 待审定研报
old_dai2 = "\u5f85\u5ba1\u5b9a\uff04\u7237\u93b6?"
c = text.count(old_dai2)
if c:
    text = text.replace(old_dai2, "待审定研报")
    count += c

# Fix 婧愮姸鎬佹甯? -> 源状态正常
old_source = "\u5a67\u6e2e\u59e8\u6028\u6057\u0e1a\u5e34?"  
c = text.count(old_source)
if c:
    text = text.replace(old_source, "源状态正常")
    count += c

# Check and report on remaining garbled lines
print(f"\nTotal fixes applied: {count}")

# Show STATUS_CN lines
for i, line in enumerate(text.split("\n"), 1):
    if "STATUS_CN" in line and "var " in line:
        print(f"\nLine {i}: {line.strip()[:200]}")

# Count remaining question marks adjacent to Chinese
issues = re.findall(r'[\u4e00-\u9fff]+\?', text)
unique = set(issues)
print(f"\nRemaining Chinese+? patterns: {len(unique)} unique, {len(issues)} total")
for s in sorted(unique)[:10]:
    print(f"  '{s}'")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text)
print("\nSaved.")
