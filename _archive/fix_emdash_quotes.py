"""Fix all em-dash missing closing quotes."""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    t = f.read()

EM = "\u2014"  # —
Q = "'"

# Find all instances of '— not followed by '
# These are broken string literals where the garbled ? was the closing quote
# Pattern: '—X where X is not '
pattern = re.compile(Q + EM + r"([^" + Q + r"])")

def fix(m):
    return Q + EM + Q + m.group(1)

count = len(pattern.findall(t))
t2 = pattern.sub(fix, t)
print(f"Fixed {count} em-dash missing quotes")

# Also fix '— at end of string (before newline)
t2 = t2.replace(Q + EM + "\n", Q + EM + Q + "\n")

# Verify: no more '— without closing quote
remaining = len(pattern.findall(t2))
print(f"Remaining: {remaining}")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(t2)
print("Saved.")
