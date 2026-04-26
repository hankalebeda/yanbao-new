"""Aggressive fix of garbled Chinese in admin.html.

The corruption pattern: original UTF-8 bytes were read as GBK by some tool,
then saved as UTF-8. Some GBK characters are 2 bytes, so a 3-byte UTF-8 char
might become garbled when adjacent bytes happen to form valid GBK sequences.

When the last byte of a UTF-8 sequence doesn't form a valid GBK pair with the 
next byte, it gets replaced with '?' (U+003F).

Strategy: Find all non-ASCII sequences (including trailing '?'), encode as GBK,
decode as UTF-8. If that fails, try without the trailing '?'.
"""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

# Pattern: non-ASCII chars optionally followed by ? (the replacement char)
# We process greedily to capture full garbled sequences
def fix_garbled(match):
    s = match.group(0)
    # Try 1: encode full segment as GBK, decode as UTF-8
    try:
        fixed = s.encode('gbk').decode('utf-8')
        return fixed
    except:
        pass
    # Try 2: strip trailing ? and try again
    if s.endswith('?'):
        try:
            fixed = s[:-1].encode('gbk').decode('utf-8')
            return fixed
        except:
            pass
    # Try 3: remove all embedded ? between non-ASCII chars and try
    cleaned = s.replace('?', '')
    if cleaned:
        try:
            fixed = cleaned.encode('gbk').decode('utf-8')
            return fixed
        except:
            pass
    return s

# Match: one or more non-ASCII chars, optionally followed by ?
# We need to also match patterns like '涓?' where ? follows Chinese
text_fixed = re.sub(r'[^\x00-\x7f]+\??', fix_garbled, text)

# Count remaining garbled
remaining = re.findall(r"'[^']*[\u4e00-\u9fff][^']*\?[^']*'", text_fixed)
print(f"Remaining Chinese+? issues: {len(remaining)}")

# Also check for common garbled patterns
garbled_check = re.findall(r'[鑲＄エ姹犲鏂鏁鐮妯浜鐢閫娲惧浼憳杩囬甯傚鐘闃畻缁]+', text_fixed)
if garbled_check:
    unique = set(garbled_check)
    print(f"Potential remaining garbled segments: {len(unique)}")
    for s in sorted(unique)[:10]:
        print(f"  '{s}'")
else:
    print("No obvious garbled patterns remaining")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text_fixed)
print("Saved.")
