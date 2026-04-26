"""Fix last 2 garbled issues in admin.html."""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

# 1. Line 1883: '瑙傚療涓? -> '观察中'  (with space)
# The pattern includes a space: '瑙傚 療涓?
# Let me find the exact string
for i, line in enumerate(text.split("\n")):
    if "businessHealth" in line and "瑙傚" in line:
        m = re.search(r"'(瑙傚[^']*\?)", line)
        if m:
            g_str = m.group(1)
            cps = " ".join(f"U+{ord(c):04X}" for c in g_str)
            print(f"  Observe: {repr(g_str)} = [{cps}]")
            text = text.replace("'" + g_str, "'观察中'")
            print(f"  Fixed!")
            break

# 2. Line 1923: 鏆傛棤寰呭瀹＄爺鎶?/td> -> 暂无待审定研报</td>
for i, line in enumerate(text.split("\n")):
    if "鏆傛棤寰呭" in line and i >= 1559:
        m = re.search(r">(鏆傛棤[^<]*)<", line)
        if m:
            g_str = m.group(1)
            print(f"  No pending: {repr(g_str)}")
            text = text.replace(g_str, "暂无待审定研报")
            print(f"  Fixed!")
            break

# Also fix confirm line punctuation
# Current: + '—？'))) return;
# Should be: + '…？')) return;
# Actually the original should be: confirm('确认' + action + '研报 ' + reportId.slice(0,8) + '…？')
# The extra ) is wrong. Let me check the line
for i, line in enumerate(text.split("\n")):
    if "confirm" in line and "确认" in line and i >= 1559:
        print(f"  Confirm: {line.strip()[:150]}")
        # Fix: '—？'))) should be '…吗？'))  
        # Actually the original Chinese was 确认 + action + 研报 xxx…？
        # The —？ was: em-dash + fullwidth question mark
        # With the extra paren: ('确认' + action + '研报 ' + reportId.slice(0,8) + '—？')))
        # This has 3 closing parens but confirm() only needs 1 + if()
        # The original pattern was likely: confirm('确认' + ... + '…吗？')
        break

# Verify remaining issues
lines = text.split("\n")
remaining = 0
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    has_pua = any("\ue000" <= ch <= "\uf8ff" for ch in line)
    has_broken = bool(re.search(r"[\u4e00-\u9fff]\?[,;}\)]", line))
    if has_pua or has_broken:
        remaining += 1
        print(f"  STILL BROKEN line {i+1}: {line.strip()[:100]}")

print(f"\nRemaining issues: {remaining}")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Saved.")
