"""Debug REVIEW_CN exact chars."""
with open("app/web/templates/admin.html", encoding="utf-8") as f:
    lines = f.readlines()
line = lines[623]  # 0-indexed line 624
idx = line.find("REVIEW_CN")
segment = line[idx:idx+120]
print(repr(segment))
# Show codepoints for garbled parts
for ch in segment:
    if ord(ch) > 0x7f:
        print(f"  '{ch}' U+{ord(ch):04X}", end="")
print()
