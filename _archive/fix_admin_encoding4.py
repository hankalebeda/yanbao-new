"""Targeted fix of specific garbled strings in admin.html."""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

# Fix STATUS_CN entries
replacements = [
    ("'杩愯涓?", "'运行中'"),
    ("'等待中,", "'等待中',"),
    ("'计算中,", "'计算中',"),
    ("'鏈煡'", "'未知'"),
    # Other garbled strings found
    ("闄嶇骇鐜?", "降级率"),
    ("10涓?", "10个"),
    ("1涓?", "1个"),
    ("50涓?", "50个"),
    ("K绾挎姄鍙栧け璐?", "K线抓取失败"),
    ("婧愮姸鎬佹甯?", "源状态正常"),
    ("宸?", "已"),
    ("宸叉嫆缁?", "已拒绝"),
    ("宸查檷绾?", "已降级"),
    ("宸查獙璇?", "已验证"),
    ("寰呭瀹?", "待审定"),
    ("杩愯涓?", "运行中"),
    ("杩愯鏃?", "运行时"),
    ("浠呯敤浜庤瘎浼?", "仅用于评估"),
    ("浼佷笟鐗?", "企业版"),
    ("涓婃父宸插氨缁?", "上游已就绪"),
]

for garbled, fixed in replacements:
    count = text.count(garbled)
    if count:
        print(f"  '{garbled}' -> '{fixed}' ({count}x)")
        text = text.replace(garbled, fixed)

# Now do a comprehensive scan for remaining garbled patterns
# Build a dict of all known garbled->fixed pairs from GBK decode
# Process non-ASCII chars that can be decoded via GBK->UTF-8
def try_fix(match):
    s = match.group(0)
    # If ends with ?, try without it  
    base = s.rstrip('?')
    suffix = s[len(base):]
    if not base:
        return s
    try:
        fixed = base.encode('gbk').decode('utf-8')
        if suffix == '?':
            return fixed  # The ? was a lost byte replacement
        return fixed + suffix
    except:
        return s

text = re.sub(r'[^\x00-\x7f]+\??', try_fix, text)

# Verify STATUS_CN
for i, line in enumerate(text.split("\n"), 1):
    if "STATUS_CN" in line and "var " in line:
        print(f"\nLine {i}: {line.strip()[:200]}")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text)
print("\nSaved.")
