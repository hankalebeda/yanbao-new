"""
Comprehensive scan for ALL broken JS strings in admin.html script block.
Strategy: Parse each line for single-quoted strings. If a quote opens but
doesn't close before the end of a reasonable string boundary, it's broken.
"""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    t = f.read()

lines = t.split("\n")

# We'll collect all issues first, then fix them
issues = []

# Script block is roughly lines 1560-2153 (0-indexed: 1559-2152)
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    stripped = line.strip()
    if not stripped or stripped.startswith("//") or stripped.startswith("/*") or stripped.startswith("*"):
        continue
    
    # Manual single-quote parser
    j = 0
    while j < len(line):
        if line[j] == "'":
            # Found opening quote
            start = j
            j += 1
            # Scan for closing quote
            closed = False
            while j < len(line):
                if line[j] == "\\":
                    j += 2
                    continue
                if line[j] == "'":
                    closed = True
                    break
                j += 1
            
            if not closed:
                content = line[start+1:]
                # This is an unclosed string on this line
                # Check if it contains Chinese (our typical corruption pattern)
                if any("\u4e00" <= c <= "\u9fff" or c in "\uff01\uff1f\u2014" for c in content):
                    issues.append((i+1, start, content[:60]))
                    print(f"  UNCLOSED Line {i+1} col {start}: '{content[:60]}'")
        j += 1

print(f"\nTotal unclosed Chinese strings: {len(issues)}")

# Now let's also find the specific patterns where ' CHINESE : ' or ' CHINESE ) or similar
# Ternary: condition ? 'strA' : 'strB' -- if strA is unclosed, it eats the : and strB
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    # Pattern: '(Chinese chars)(space)(colon/question-mark  space)(single-quote)
    # This means the Chinese string wasn't closed
    matches = re.findall(r"'([\u4e00-\u9fff\uff01\uff1f\u2014\u2026·]+)\s+([?:+])\s+'", line)
    for content, op in matches:
        # This is: 'Chinese OP 'otherString - missing close quote
        old = f"'{content} {op} '"
        new = f"'{content}' {op} '"
        if old in line:
            lines[i] = lines[i].replace(old, new, 1)
            print(f"  FIXED ternary Line {i+1}: '{content}' {op}")

# Also fix: 'Chinese + (  and  'Chinese)  patterns
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    # Pattern: Chinese character followed by ) or )) or ); without closing quote
    for m in re.finditer(r"'([^']*[\u4e00-\u9fff\uff01\uff1f\u2014])(\)+)", line):
        full = m.group(0)
        content = m.group(1)
        parens = m.group(2)
        fixed = f"'{content}'{parens}"
        if full != fixed:
            lines[i] = lines[i].replace(full, fixed, 1)
            print(f"  FIXED paren Line {i+1}: '{content}'")

t = "\n".join(lines)
with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(t)
print("\nSaved.")
