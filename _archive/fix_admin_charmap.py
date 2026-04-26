"""
Comprehensive: find ALL garbled Chinese fragments in admin.html and fix them.
Strategy: any non-ASCII char sequence that looks like GBK-corrupted UTF-8,
try to decode it back. Also build explicit mappings for common garbled words.
"""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

# Known character-level mappings from the GBK corruption
# These are common garbled -> correct mappings for this specific corruption type
CHAR_FIXES = {
    "鎺ュ": "接",
    "鎺": "接",
    "鎺ュ彛": "接口",
    "鐘舵€?": "状态",
    "鐘舵€": "状态",
    "閿欒": "错误",
    "娴佹按绾?": "流水线",
    "娴佹按绾": "流水线",
    "褰?": "录",
    "涓嶅彲鐢?": "不可用",
    "鍔犺浇澶辫触": "加载失败",
    "鎬昏": "总览",
    "鏃犻敊璇?": "无错误",
    "鏃犲鐞嗗嚱鏁?": "无处理函数",
    "绠＄悊鍛?": "管理员",
    "瓒呯骇绠＄悊鍛?": "超级管理员",
    "鏅€氱敤鎴?": "普通用户",
    "鏈煡": "未知",
    "鏈€杩?": "最近",
    "澶╂殏鏃犺皟搴﹁": "天暂无调度",
    "璋冨害": "调度",
    "鎽樿": "摘要",
    "鈿?": "⚠",
    "鈥?": "—",
    "锛?": "！",
    "锛?": "：",
    "銆?": "。",
    "鐮旀姤銆?": "研报。",
    "鐮旀姤": "研报",
    "鑷攣淇": "自锁修",
    "娓呯悊": "清理",
    "閲嶈瘯娆℃暟宸茬敤灏?": "重试次数已用尽",
    "鍏ュ彛": "入口",
    "瀵瑰": "对外",
    "寮€鏀?": "开放",
    "鍙ｅ緞": "口径",
    "璇存槑": "说明",
    "浠婃棩": "今日",
    "鐮旀姤": "研报",
    "鏆傛棤": "暂无",
    "寰呭瀹?": "待审定",
    "璇锋眰": "请求",
    "杩愯": "运行",
    "棩鏈?": "日期",
    "闃舵": "阶段",
    "瀹屾垚鏃堕棿": "完成时间",
    "寮€濮嬫椂闂?": "开始时间",
    "妯℃嫙": "模拟",
    "蹇収": "快照",
}

# Apply longest-first replacements
sorted_fixes = sorted(CHAR_FIXES.items(), key=lambda x: -len(x[0]))
total = 0
for garbled, correct in sorted_fixes:
    c = text.count(garbled)
    if c > 0:
        text = text.replace(garbled, correct)
        total += c
        print(f"  '{garbled}' -> '{correct}' ({c}x)")

print(f"\nTotal char-level fixes: {total}")

# Now try to fix any remaining non-ASCII sequences via GBK roundtrip
def try_gbk_roundtrip(match):
    s = match.group(0)
    base = s.rstrip("?")
    if not base:
        return s
    try:
        decoded = base.encode("gbk").decode("utf-8")
        # Only accept if result is reasonable Chinese
        if all("\u4e00" <= c <= "\u9fff" or c in "，。！？：；" for c in decoded):
            return decoded + ("" if s == base else "")  # drop trailing ?
        return s
    except:
        return s

text = re.sub(r"[^\x00-\x7f]+\??", try_gbk_roundtrip, text)

# Final check: any remaining garbled in script block
lines = text.split("\n")
issues = 0
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    has_pua = any("\ue000" <= ch <= "\uf8ff" for ch in line)
    if has_pua:
        issues += 1
        pua_chars = [f"U+{ord(c):04X}" for c in line if "\ue000" <= c <= "\uf8ff"]
        print(f"  PUA line {i+1}: {', '.join(pua_chars)}")
    # Check for garbled high-range chars that aren't normal CJK
    for m in re.finditer(r"'([^']+)'", line):
        s = m.group(1)
        has_corrupt = any(0x9200 <= ord(c) <= 0x9500 for c in s)
        if has_corrupt:
            bad = [f"U+{ord(c):04X}" for c in s if 0x9200 <= ord(c) <= 0x9500]
            issues += 1
            print(f"  Garbled line {i+1}: chars {', '.join(bad[:5])} in '{s[:30]}'")

print(f"\nRemaining issues in script block: {issues}")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Saved.")
