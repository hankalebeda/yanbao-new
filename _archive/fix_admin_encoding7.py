"""Fix garbled Chinese in admin.html using exact Unicode codepoints."""

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

# Build exact garbled strings from codepoints we identified
# RUNNING: 杩(U+6769) 愯(U+612F) U+E511 涓(U+6D93) ?(U+003F)
running_garbled = "\u6769\u612F\uE511\u6D93?"
# UNKNOWN: 鏈(U+93C8) U+E046 煡(U+7161)
unknown_garbled = "\u93C8\uE046\u7161"
# startup: 鍚(U+935A) U+E21A 姩(U+59E9) 瑙(U+7459) ﹀(U+FE40) 彂(U+5F42)
startup_garbled = "\u935A\uE21A\u59E9\u7459\uFE40\u5F42"
# user: 鏅(U+93C5) U+E1C0 €(U+20AC) 氱(U+6C31) 敤(U+6564) 鎴(U+93B4) ?(U+003F)
user_garbled = "\u93C5\uE1C0\u20AC\u6C31\u6564\u93B4?"

# Verify these exist in text
print(f"RUNNING garbled found: {text.count(running_garbled)}")
print(f"UNKNOWN garbled found: {text.count(unknown_garbled)}")
print(f"startup garbled found: {text.count(startup_garbled)}")
print(f"user garbled found: {text.count(user_garbled)}")

# Apply replacements
# STATUS_CN RUNNING: 'RUNNING':'杩愯涓?,' -> 'RUNNING':'运行中',
text = text.replace("'" + running_garbled + ",", "'运行中',")
# PUBLIC_STATUS_CN UNKNOWN: 'UNKNOWN':'鏈煡' -> 'UNKNOWN':'未知'
text = text.replace("'" + unknown_garbled + "'", "'未知'")
# TRIGGER_CN startup: 'startup':'鍚姩瑙﹀彂' -> 'startup':'启动触发'
text = text.replace("'" + startup_garbled + "'", "'启动触发'")
# ROLE_CN user: 'user':'鏅€氱敤鎴?} -> 'user':'普通用户'}
text = text.replace("'" + user_garbled + "}", "'普通用户'}")
# statusCn fallback: || '鏈煡' -> || '未知'
text = text.replace("'" + unknown_garbled + "'", "'未知'")

# Verify
for i, line in enumerate(text.split("\n"), 1):
    if "STATUS_CN" in line and "var " in line:
        print(f"\nLine {i}: {line.strip()[:200]}")
    if "ROLE_CN" in line and "var " in line:
        print(f"Line {i}: {line.strip()[:200]}")
    if "TRIGGER_CN" in line and "var " in line:
        print(f"Line {i}: {line.strip()[:200]}")
    if "statusCn" in line and "function" in line:
        print(f"Line {i}: {line.strip()[:200]}")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text)
print("\nSaved.")
