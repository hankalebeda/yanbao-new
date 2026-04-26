"""Fix all remaining unclosed quotes - broader pattern."""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    t = f.read()

# Fix specific known issues first
t = t.replace("'状态未知));", "'状态未知'));")
print("Fixed 状态未知")

# Now do a comprehensive search: find any '(Chinese text) that isn't closed
# Before ), ;, ,, ], or + (string concatenation)
lines = t.split("\n")
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    stripped = line.strip()
    if not stripped or stripped.startswith("//"):
        continue
    
    # Find all single-quoted strings
    in_quote = False
    quote_start = -1
    j = 0
    while j < len(line):
        ch = line[j]
        if ch == "'" and not in_quote:
            in_quote = True
            quote_start = j
        elif ch == "'" and in_quote:
            in_quote = False
        elif ch == "\\" and in_quote:
            j += 1  # skip escaped char
        j += 1
    
    if in_quote:
        # Quote wasn't closed on this line
        content = line[quote_start+1:]
        if any("\u4e00" <= c <= "\u9fff" for c in content):
            print(f"  Unclosed quote line {i+1}: '{content[:40]}...'")
            # Try to fix by finding the most likely close point
            # Look for the first ) ; , or ] after Chinese text
            for k in range(quote_start+1, len(line)):
                if line[k] in ");,]" and k > quote_start + 1:
                    # Insert closing quote
                    lines[i] = line[:k] + "'" + line[k:]
                    print(f"    -> Inserted ' at position {k}")
                    break

t = "\n".join(lines)

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(t)
print("Saved.")
