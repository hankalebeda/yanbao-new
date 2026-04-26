"""Show remaining garbled strings in admin.html."""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

# Find strings in single quotes that contain Chinese + ?
# The pattern is: 'some_garbled_chinese?' where ? terminates the garbled text
issues = re.findall(r"'([^']{0,40}[\u4e00-\u9fff][^']*\?)", text)
unique = sorted(set(issues))
print(f"Total unique garbled-with-? patterns: {len(unique)}")
for s in unique[:20]:
    print(f"  '{s}'")

# Also look at the STATUS_CN line
for i, line in enumerate(text.split("\n"), 1):
    if "STATUS_CN" in line:
        print(f"\nLine {i}: {line[:200]}")
