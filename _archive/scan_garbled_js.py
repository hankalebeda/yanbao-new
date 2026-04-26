"""
Comprehensive fix of ALL garbled JS strings in admin.html lines 1560-2153
(the second/last inline script block, which the tests extract).

Strategy: Read each line, detect PUA chars or garbled patterns,
replace using a comprehensive mapping built from Unicode codepoints.
"""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    lines = f.readlines()

# Build mapping: garbled string (using exact Unicode codepoints) -> correct Chinese
# Each entry is (garbled_codepoints_as_string, correct_string)
MAPPINGS = {}

def g(*codepoints):
    """Build a string from Unicode codepoints."""
    return "".join(chr(cp) for cp in codepoints)

# Common PUA-containing patterns found in the file
# '鈥?' (U+9225 U+E046?) -> '—' (em-dash) - already fixed by encoding9
# But let me add the codepoint version too

# From debug output, key garbled sequences:
# 涓撲笟鐗? = U+6D93 U+6492 U+4E1F U+7266 U+003F => 专业版
# But some are already fixed. Let me focus on what's STILL garbled.

# I'll process lines 1560-2153 and fix them systematically.
# Rather than individual char mapping, let me use regex to find garbled 
# single-quoted strings and replace them with known correct values.

# First, let me extract all single-quoted strings from lines 1560-2153
# and identify which ones are garbled
script_start = 1559  # 0-indexed line 1560
script_end = 2152    # 0-indexed line 2153

garbled_strings = {}
for i in range(script_start, min(script_end + 1, len(lines))):
    line = lines[i]
    # Find all single-quoted string contents
    for m in re.finditer(r"'([^']*)'", line):
        s = m.group(1)
        has_pua = any("\ue000" <= ch <= "\uf8ff" for ch in s)
        has_garbled = bool(re.search(r'[\u4e00-\u9fff]\?', s))
        # Check for known garbled chars (high Unicode from GBK corruption)
        has_corrupt = any(ord(ch) > 0x9fff and ch not in '﹀€' for ch in s)
        if has_pua or has_garbled or has_corrupt:
            key = repr(s)
            if key not in garbled_strings:
                garbled_strings[key] = (i+1, s)

print(f"Found {len(garbled_strings)} unique garbled strings in script block")
for key, (line_no, s) in sorted(garbled_strings.items(), key=lambda x: x[1][0]):
    codepoints = " ".join(f"U+{ord(c):04X}" for c in s)
    print(f"  Line {line_no}: {repr(s[:40])} = [{codepoints[:100]}]")
