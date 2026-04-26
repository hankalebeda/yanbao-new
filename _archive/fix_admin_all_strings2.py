"""
Comprehensive: find ALL broken string patterns in the second script block.
"""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    t = f.read()

lines = t.split("\n")
count = 0

# Pass 1: Fix 'Chinese + ( → 'Chinese' + (
# Also 'Chinese ： + ( → 'Chinese：' + (
for i in range(len(lines)):
    line = lines[i]
    # Pattern: single-quote, then text with Chinese, then space+plus 
    # but NO closing single-quote before the +
    while True:
        m = re.search(r"'([^'\n]*[\u4e00-\u9fff\uff01\uff1f\uff1a\u2014\u2026！？：]) \+ ", line)
        if m:
            content = m.group(1)
            old = f"'{content} + "
            new = f"'{content}' + "
            line = line.replace(old, new, 1)
            lines[i] = line
            count += 1
            print(f"  Fixed concat Line {i+1}: '{content[:30]}' +")
        else:
            break

# Pass 2: Fix 'Chinese : 'other → 'Chinese' : 'other  (ternary)
for i in range(len(lines)):
    line = lines[i]
    while True:
        m = re.search(r"'([^'\n]*[\u4e00-\u9fff\uff01\uff1f\uff1a\u2014]) : '", line)
        if m:
            content = m.group(1)
            old = f"'{content} : '"
            new = f"'{content}' : '"
            line = line.replace(old, new, 1)
            lines[i] = line
            count += 1
            print(f"  Fixed ternary Line {i+1}: '{content[:30]}' :")
        else:
            break

# Pass 3: Fix 'Chinese) or 'Chinese)) → 'Chinese') etc.
for i in range(len(lines)):
    line = lines[i]
    while True:
        m = re.search(r"'([^'\n]*[\u4e00-\u9fff\uff01\uff1f\u2014])(\)+)", line)
        if m:
            content = m.group(1)
            parens = m.group(2)
            old = f"'{content}{parens}"
            new = f"'{content}'{parens}"
            line = line.replace(old, new, 1)
            lines[i] = line
            count += 1
            print(f"  Fixed paren Line {i+1}: '{content[:30]}'{parens}")
        else:
            break

# Pass 4: Fix 'Chinese; → 'Chinese';
for i in range(len(lines)):
    line = lines[i]
    while True:
        m = re.search(r"'([^'\n]*[\u4e00-\u9fff\uff01\uff1f\u2014]);", line)
        if m:
            content = m.group(1)
            old = f"'{content};"
            new = f"'{content}';"
            line = line.replace(old, new, 1)
            lines[i] = line
            count += 1
            print(f"  Fixed semicolon Line {i+1}: '{content[:30]}';")
        else:
            break

# Pass 5: Fix 'Chinese } → 'Chinese' }
for i in range(len(lines)):
    line = lines[i]
    while True:
        m = re.search(r"'([^'\n]*[\u4e00-\u9fff\uff01\uff1f\u2014]) }", line)
        if m:
            content = m.group(1)
            old = f"'{content} }}"
            new = f"'{content}' }}"
            line = line.replace(old, new, 1)
            lines[i] = line
            count += 1
            print(f"  Fixed brace Line {i+1}: '{content[:30]}' }}")
        else:
            break

# Pass 6: 'Chinese, → 'Chinese',
for i in range(len(lines)):
    line = lines[i]
    while True:
        m = re.search(r"'([^'\n]*[\u4e00-\u9fff\uff01\uff1f\u2014]),", line)
        if m:
            content = m.group(1)
            old = f"'{content},"
            new = f"'{content}',"
            line = line.replace(old, new, 1)
            lines[i] = line
            count += 1
            print(f"  Fixed comma Line {i+1}: '{content[:30]}',")
        else:
            break

# Pass 7: 'Chinese] → 'Chinese']
for i in range(len(lines)):
    line = lines[i]
    while True:
        m = re.search(r"'([^'\n]*[\u4e00-\u9fff\uff01\uff1f\u2014])\]", line)
        if m:
            content = m.group(1)
            old = f"'{content}]"
            new = f"'{content}']"
            line = line.replace(old, new, 1)
            lines[i] = line
            count += 1
            print(f"  Fixed bracket Line {i+1}: '{content[:30]}']")
        else:
            break

print(f"\nTotal fixes: {count}")
t = "\n".join(lines)
with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(t)
print("Saved.")
