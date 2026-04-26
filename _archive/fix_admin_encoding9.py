"""Fix all remaining garbled Chinese that breaks JS syntax in admin.html."""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

# Fix reasonCn: '鈥?' should be '—' (em-dash)
# The garbled '鈥?' is from the GBK corruption of '—'
old = "return '鈥?"
new = "return '—'"
c = text.count(old)
print(f"reasonCn fix: {c}")
text = text.replace(old, new)

# Now let's find ALL remaining lines with PUA characters (U+E000-U+F8FF)
# or Chinese followed by ? that would break JS string syntax
lines = text.split("\n")
for i, line in enumerate(lines, 1):
    has_pua = any("\ue000" <= ch <= "\uf8ff" for ch in line)
    # Check for garbled pattern: Chinese char followed by ? inside JS string context
    has_broken_str = bool(re.search(r"'[^']*[\u4e00-\u9fff]\?[^']*[,;}]", line))
    if has_pua or has_broken_str:
        stripped = line.strip()[:120]
        if "<script" not in stripped and "<!--" not in stripped:
            print(f"  Line {i}: {stripped}")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text)
print("\nSaved.")
