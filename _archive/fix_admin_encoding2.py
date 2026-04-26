"""Fix remaining garbled Chinese in admin.html."""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

# Manual replacements for known garbled strings
replacements = {
    "杩愯涓?": "运行中",
    "绛夊緟涓?": "等待中", 
    "璁＄畻涓?": "计算中",
    "鏈煡": "未知",
    # Check for other remaining garbled patterns
}

for garbled, fixed in replacements.items():
    count = text.count(garbled)
    if count:
        print(f"Replacing '{garbled}' -> '{fixed}' ({count} occurrences)")
        text = text.replace(garbled, fixed)

# Now let's scan for any remaining garbled sequences
# Garbled chars typically involve: 鑲 鏂 鐮 妯 鐢 娲 浼 甯 etc.
GARBLED_CHARS = set("姣忔棩娴佹按绾鑲＄エ姹犲埛鏂鏁版嵁閲囬泦甯傚満鐘舵€鐮旀姤鐢熸垚缁撶畻妯℃嫙寤轰粨浜ゆ槗閫氱煡娲惧彂浜嬩欢瀵硅处浼氬憳杩囨湡娓呯悊寮€鐩樹环瀹氭椂")

remaining_garbled = []
for i, line in enumerate(text.split("\n"), 1):
    has_garbled = any(c in GARBLED_CHARS for c in line)
    if has_garbled:
        # Check if this segment can be fixed
        matches = re.findall(r'[^\x00-\x7f]+', line)
        for m in matches:
            try:
                fixed = m.encode('gbk').decode('utf-8')
                if fixed != m:
                    remaining_garbled.append((i, m, fixed))
            except:
                pass

if remaining_garbled:
    print(f"\nFound {len(remaining_garbled)} remaining garbled segments:")
    for lineno, garbled, fixed in remaining_garbled[:20]:
        print(f"  Line {lineno}: '{garbled}' -> '{fixed}'")
        text = text.replace(garbled, fixed)

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text)

# Final verification
with open("app/web/templates/admin.html", encoding="utf-8") as f:
    verify = f.read()

# Check for remaining ? in Chinese context
issues = re.findall(r"'[^']*[\u4e00-\u9fff][^']*\?[^']*'", verify)
if issues:
    print(f"\nRemaining issues with ? in Chinese strings: {len(issues)}")
    for iss in issues[:5]:
        print(f"  {iss}")
else:
    print("\nNo remaining Chinese+? issues found")
