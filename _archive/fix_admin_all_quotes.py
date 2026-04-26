"""
Find and fix ALL unclosed single-quoted strings in the script block.
Strategy: For each line in the script block, find all '...' patterns.
If a ' is opened but not closed before a ), ;, or , check if it needs fixing.
"""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    t = f.read()

lines = t.split("\n")

# Focus on script block lines 1560-2153
fixes = []
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    if not line.strip():
        continue
    
    # Find all opening single quotes and check if properly closed
    # Skip lines that are comments
    stripped = line.strip()
    if stripped.startswith("//") or stripped.startswith("/*"):
        continue
    
    # Look for pattern: 'Chinese_text followed by ) or ; or , without closing '
    # This regex finds '(content) where content contains Chinese and isn't closed
    for m in re.finditer(r"'([^'\n]*[\u4e00-\u9fff\uff01\uff1f\u2014\u2026][^'\n]*)([);,\]])", line):
        content = m.group(1)
        delimiter = m.group(2)
        # Check: is this a broken string? The content shouldn't end with just text+delimiter
        # A properly formed string would have closing ' before the delimiter
        pos = m.start()
        # Get the full context
        before = line[max(0, pos-5):pos]
        after = line[m.end():m.end()+5]
        
        # This is a real issue if:
        # - There's Chinese text right before the delimiter
        # - The delimiter is ), ;, or ]
        if re.search(r"[\u4e00-\u9fff\uff01\uff1f\u2014\u2026]$", content):
            fixes.append((i, content, delimiter))
            print(f"  Line {i+1}: '{content[:40]}' missing closing quote before '{delimiter}'")

print(f"\nTotal broken strings found: {len(fixes)}")

# Apply fixes: insert closing ' before the delimiter
for line_idx, content, delimiter in fixes:
    line = lines[line_idx]
    old = "'" + content + delimiter
    new = "'" + content + "'" + delimiter
    if old in line:
        lines[line_idx] = line.replace(old, new, 1)  # Fix first occurrence

t = "\n".join(lines)

# Write and re-check
with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(t)

# Verify
remaining = 0
lines2 = t.split("\n")
for i in range(1559, min(2153, len(lines2))):
    line = lines2[i]
    stripped = line.strip()
    if not stripped or stripped.startswith("//"):
        continue
    for m in re.finditer(r"'([^'\n]*[\u4e00-\u9fff\uff01\uff1f\u2014\u2026][^'\n]*)([);,\]])", line):
        content = m.group(1)
        if re.search(r"[\u4e00-\u9fff\uff01\uff1f\u2014\u2026]$", content):
            remaining += 1

print(f"Remaining broken: {remaining}")
print("Saved.")
