"""Fix all remaining garbled Chinese in admin.html using exact string replacements."""

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

# Dictionary of exact garbled -> correct replacements
replacements = {
    # STATUS_CN: RUNNING entry (missing closing quote)
    "'RUNNING':'杩愯涓?,": "'RUNNING':'运行中',",
    # PUBLIC_STATUS_CN: UNKNOWN entry 
    "'UNKNOWN':'鏈煡'": "'UNKNOWN':'未知'",
    # statusCn fallback
    "|| '鏈煡'": "|| '未知'",
    # TRIGGER_CN entries
    "'startup':'鍚姩瑙﹀彂'": "'startup':'启动触发'",
    # ROLE_CN entries
    "'admin':'绠＄悊鍛?,": "'admin':'管理员',",
    "'super_admin':'瓒呯骇绠＄悊鍛?,": "'super_admin':'超级管理员',",
    "'user':'鏅€氱敤鎴?}": "'user':'普通用户'}",
}

total = 0
for garbled, fixed in replacements.items():
    c = text.count(garbled)
    if c:
        text = text.replace(garbled, fixed)
        total += c
        print(f"  {repr(garbled)} -> {repr(fixed)} ({c}x)")
    else:
        print(f"  NOT FOUND: {repr(garbled)}")

print(f"\nTotal replacements: {total}")

# Now do a broader scan for common garbled patterns
# These are complete word replacements
word_replacements = {
    "鎬昏鎺ュ彛鍔犺浇澶辫触锛?": "总接口加载失败！",
    "鎬昏鎺ュ彛鍔犺浇澶辫触": "总接口加载失败",
    "鍙ｅ緞璇存槑锛氫粖鏃ョ爺鎶?": "口径说明：今日研报",
    "棣栭〉姹犳棩鏈?": "首页池日期",
    "娴佹按绾块樁娈佃繘搴?": "流水线阶段进度",
    "寰呭瀹＄爺鎶?": "待审定研报",
    "寰呭瀹?": "待审定",
    "宸茬敓鎴?": "已生成",
    "杩愯鏃?": "运行时",
    "杩愯涓?": "运行中",
    "鍚姩瑙﹀彂": "启动触发",
    "绠＄悊鍛?": "管理员",
    "瓒呯骇绠＄悊鍛?": "超级管理员",
    "鏅€氱敤鎴?": "普通用户",
    "鏈煡": "未知",
    "姹犳棩鏈?": "池日期",
    "佸師鍥?": "原因",
    "佹湭鐭?": "未知",
    "偂绁?": "股票",
    "彂婧?": "发源",
    "彛涓嶅彲鐢?": "口不可用",
    "彛鍔犺浇澶辫触锛?": "口加载失败！",
}

for garbled, fixed in word_replacements.items():
    c = text.count(garbled)
    if c:
        text = text.replace(garbled, fixed)
        total += c
        print(f"  word: '{garbled}' -> '{fixed}' ({c}x)")

# Verify critical lines
for i, line in enumerate(text.split("\n"), 1):
    if "STATUS_CN" in line and "var " in line:
        print(f"\nLine {i}: {line.strip()[:200]}")
    if "ROLE_CN" in line and "var " in line:
        print(f"Line {i}: {line.strip()[:200]}")
    if "TRIGGER_CN" in line and "var " in line:
        print(f"Line {i}: {line.strip()[:200]}")

# Count remaining
import re
issues = re.findall(r'[\u4e00-\u9fff]+\?', text)
print(f"\nRemaining Chinese+? patterns: {len(set(issues))} unique, {len(issues)} total")
for s in sorted(set(issues))[:10]:
    print(f"  '{s}'")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text)
print("\nSaved.")
