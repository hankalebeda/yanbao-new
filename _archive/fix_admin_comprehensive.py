"""
Comprehensive fix of ALL garbled Chinese in admin.html inline JS.
Uses exact Unicode codepoints to match garbled text.
"""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

def g(*cps):
    """Build string from codepoints."""
    return "".join(chr(c) for c in cps)

# Comprehensive garbled -> correct mapping
# Each tuple: (garbled_string_built_from_codepoints, correct_chinese)
REPLACEMENTS = [
    # Multi-char PUA sequences (longer matches first to avoid partial replacements)
    # 统计快照
    (g(0x7F01, 0x71BB, 0xE178, 0x8E47, 0xE0A4, 0x53CE), "统计快照"),
    # 模拟快照
    (g(0x59AF, 0x2103, 0x5AD9, 0x8E47, 0xE0A4, 0x53CE), "模拟快照"),
    # 未知错误 (full)
    (g(0x93C8, 0xE046, 0x7161, 0x95BF, 0x6B12, 0xE1E4), "未知错误"),
    # 未知错误 (shorter variant without trailing PUA)
    (g(0x93C8, 0xE046, 0x7161, 0x95BF, 0x6B12), "未知错误"),
    # 服务可用
    (g(0x93C8, 0x5D85, 0x59DF, 0x9359, 0xE21C, 0x6564), "服务可用"),
    # 运行健康
    (g(0x6769, 0x612F, 0xE511, 0x934B, 0x30E5, 0x608D), "运行健康"),
    # 准确率 (鍑嗙'鐜?)
    (g(0x9351, 0x55D9, 0x2018, 0x941C, 0x003F), "准确率"),
    # 个数据源 ( 涓暟鎹簮)
    (g(0x6D93, 0xE045, 0x669F, 0x93B9, 0xE1BD, 0x7C2E), "个数据源"),
    # 总览加载失败！with ⚠ prefix (鈿?鎬昏鍔犺浇澶辫触锛?)
    (g(0x923F, 0x003F, 0x0020, 0x93AC, 0x660F, 0xE74D, 0x9354, 0x72BA, 0x6D47, 0x6FB6, 0x8FAB, 0x89E6, 0x951B, 0x003F), "⚠ 总览加载失败！"),
    # 总览加载失败！(鎬昏鍔犺浇澶辫触锛?)
    (g(0x93AC, 0x660F, 0xE74D, 0x9354, 0x72BA, 0x6D47, 0x6FB6, 0x8FAB, 0x89E6, 0x951B, 0x003F), "总览加载失败！"),
    # 总览接口： (鎬昏鎺ュ彛锛?)
    (g(0x93AC, 0x660F, 0xE74D, 0x93BA, 0x30E5, 0x5F5B, 0x951B, 0x003F), "总览接口："),
    # 总览接口不可用 - already partially fixed: 鎬昏鎺ュ口不可用
    # 总览接口加载失败！ - check 鎬昏鎺ュ口加载失败 
    # 总览不可用 (鎬昏涓嶅彲鐢?)
    (g(0x93AC, 0x660F, 0xE74D), "总览"),
    # 操作失败！(鎿嶄綔澶辫触锛?)
    (g(0x93BF, 0x5D84, 0x7D94, 0x6FB6, 0x8FAB, 0x89E6, 0x951B, 0x003F), "操作失败！"),
    # 调度接口： (璋冨害鎺ュ彛锛?)
    (g(0x748B, 0x51A8, 0x5BB3, 0x93BA, 0x30E5, 0x5F5B, 0x951B, 0x003F), "调度接口："),
    # 用户接口： (鐢ㄦ埛鎺ュ彛锛?)
    (g(0x9422, 0x3126, 0x57DB, 0x93BA, 0x30E5, 0x5F5B, 0x951B, 0x003F), "用户接口："),
    # 复审接口：(澶嶅鎺ュ彛锛?)
    (g(0x6FB6, 0x5D85, 0xE178, 0x93BA, 0x30E5, 0x5F5B, 0x951B, 0x003F), "复审接口："),
    # 系统健康接口：(绯荤粺鍋ュ悍鎺ュ彛锛?)
    (g(0x7EEF, 0x8364, 0x7CBA, 0x934B, 0x30E5, 0x608D, 0x93BA, 0x30E5, 0x5F5B, 0x951B, 0x003F), "系统健康接口："),
    # 调度摘要加载失败 (璋冨害鎽樿鍔犺浇澶辫触)
    (g(0x748B, 0x51A8, 0x5BB3, 0x93BD, 0x6A3F, 0xE6E6, 0x9354, 0x72BA, 0x6D47, 0x6FB6, 0x8FAB, 0x89E6), "调度摘要加载失败"),
    # ⚠ (鈿?)
    (g(0x923F, 0x003F), "⚠"),
    # — (鈥?)
    (g(0x9225, 0x003F), "—"),
    # 确认 (纭)  
    (g(0x7EAD, 0xE1BF, 0xE17B), "确认"),
    # 存在异常源 + ?
    (g(0x701B, 0x6A3A, 0x6E6A, 0x5BEE, 0x509A, 0x7236, 0x5A67, 0x003F), "存在异常源"),
    # 加载失败！ (standalone occurrences of this pattern)
    (g(0x9354, 0x72BA, 0x6D47, 0x6FB6, 0x8FAB, 0x89E6, 0x951B, 0x003F), "加载失败！"),
    (g(0x9354, 0x72BA, 0x6D47, 0x6FB6, 0x8FAB, 0x89E6), "加载失败"),
    # 观察中 (瑙傚療涓?)
    (g(0x7459, 0x50AC, 0x7642, 0x6D93, 0x003F), "观察中"),
    # Also check for this variant without ?
    (g(0x7459, 0x50AC, 0x7642, 0x6D93), "观察中"),
    # 接口 / 接口: variants
    (g(0x93BA, 0x30E5, 0x5F5B), "接口"),
    # 无数据 (鏃犳暟鎹?)  
    (g(0x93C8, 0xE046, 0x7161), "未知"),
    # 研报。(鐮旀姤銆?)
    (g(0x9422, 0x65FC, 0x59E4, 0x94AD, 0x003F), "研报。"),
    # Try simpler fragments
    # 请求失败 (璇锋眰澶辫触)
    (g(0x748B, 0x951B), "锛"),
]

# Apply replacements 
total = 0
for garbled, correct in REPLACEMENTS:
    c = text.count(garbled)
    if c:
        text = text.replace(garbled, correct)
        total += c
        if len(garbled) > 1:
            print(f"  {repr(garbled[:15])} -> '{correct}' ({c}x)")

print(f"\nTotal replacements: {total}")

# Now scan for any remaining PUA characters in script blocks (lines 1560-2153)
lines = text.split("\n")
remaining_issues = 0
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    has_pua = any("\ue000" <= ch <= "\uf8ff" for ch in line)
    if has_pua:
        remaining_issues += 1
        # Show the PUA chars
        pua_chars = [(j, ch, f"U+{ord(ch):04X}") for j, ch in enumerate(line) if "\ue000" <= ch <= "\uf8ff"]
        stripped = line.strip()[:80]
        print(f"  Still broken line {i+1}: {stripped}")
        for pos, ch, code in pua_chars:
            context = line[max(0,pos-3):pos+4]
            print(f"    PUA {code} in context: {repr(context)}")

# Also check for Chinese+? broken quotes in script block
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    # Pattern: Chinese char(s) followed by ? then , or ; (missing closing quote)
    if re.search(r"[\u4e00-\u9fff]\?[,;}\)]", line):
        remaining_issues += 1
        stripped = line.strip()[:100]
        print(f"  Broken quote line {i+1}: {stripped}")

print(f"\nRemaining issues in script block: {remaining_issues}")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Saved.")
