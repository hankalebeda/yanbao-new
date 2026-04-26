"""Fix strategy-dist and all remaining broken JS strings."""

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    t = f.read()

# Fix the strategy-dist line
t = t.replace(
    "document.getElementById('overview-strategy-dist').textContent = '\u2014'/ \u2014/ \u2014;",
    "document.getElementById('overview-strategy-dist').textContent = '\u2014 / \u2014 / \u2014';"
)
print("Fixed strategy-dist")

# Fix the report-progress line: '— —' pattern
# Should be '— / —'
t = t.replace(
    ".textContent = '\u2014 \u2014';",
    ".textContent = '\u2014 / \u2014';"
)

# Now do a thorough scan: find ALL lines in the script block where there are
# unmatched single quotes (odd number of ' characters, accounting for escaped ones)
import re
lines = t.split("\n")
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    # Skip empty lines and comments
    stripped = line.strip()
    if not stripped or stripped.startswith("//") or stripped.startswith("/*"):
        continue
    # Count single quotes (simple heuristic - not perfect for all JS)
    # Check for common broken patterns
    # Pattern 1: Chinese text; (no closing quote)
    if re.search(r"= '[\u4e00-\u9fff\u2014\u2026\uff01\uff1f !/.]+;$", stripped):
        print(f"  Broken line {i+1}: {stripped[:80]}")
    # Pattern 2: '中文) without closing quote
    if re.search(r"'[\u4e00-\u9fff\u2014\u2026]+[,;)\]]", stripped) and "'" not in stripped.split("'")[1:2]:
        pass  # Too many false positives

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(t)
print("Saved.")
