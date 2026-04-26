"""Fix remaining 18 garbled issues in admin.html script block."""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

def g(*cps):
    return "".join(chr(c) for c in cps)

# Remaining specific fixes using exact codepoints/patterns:

# 1. Line 1693: ['杩愯鏃?, ... ] = '运行时' (with PUA U+E511)
# Full garbled: 杩愯\ue511鏃?
text = text.replace(g(0x6769, 0x612F, 0xE511, 0x93C8), "运行时")  # partial fix first
# But the ? after is also part of it
old_runtime = g(0x6769, 0x612F, 0xE511) + "鏃?"
text = text.replace(old_runtime, "运行时'")
# Actually, the full pattern in the source is: ['杩愯\ue511鏃?,
# which should be: ['运行时',
# The garbled is: 杩愯(U+6769 U+612F) + PUA(U+E511) + 鏃(U+93C8) followed by ?,
# So: g(0x6769, 0x612F, 0xE511, 0x93C8, 0x003F)
rt_garbled = g(0x6769, 0x612F, 0xE511, 0x93C8, 0x003F)
c = text.count(rt_garbled)
print(f"运行时: {c}")
# We need '运行时', so the garbled '杩愯鏃? should become 运行时'
# In context: ['杩愯鏃?, --> ['运行时',
text = text.replace("'" + rt_garbled + ",", "'运行时',")

# 2. Lines 1669, 1671: '鏃犳暟鎹? -> '无数据' 
# 鏃犳暟鎹 = U+93C8 ... let me just use the text
nd_garbled = g(0x93C8, 0xE046)  # this was already replaced to 未知
# Actually these are different chars. Let me read the actual line
# Lines 1669: || '鏃犳暟鎹?;
# The 鏃犳暟鎹 pattern:
# 鏃(U+93C8) 犳(?) 暟(U+669F) 鎹(U+93B9) ?(U+003F)
# Let me find it by searching for a unique text pattern
lines = text.split("\n")
for i, line in enumerate(lines):
    if "overview-kline-date" in line and "鏃" in line:
        # Get the garbled part between quotes
        m = re.search(r"'([^']*鏃[^']*)\?", line)
        if m:
            garbled = m.group(1) + "?"
            cps = " ".join(f"U+{ord(c):04X}" for c in garbled)
            print(f"  Line {i+1} kline: {cps}")

# Let me do it differently - just replace in context
text = text.replace("|| '鏃犳暟鎹?;", "|| '无数据';")

# Check if that worked
c = text.count("鏃犳暟鎹?")
print(f"Remaining 鏃犳暟鎹?: {c}")

# 3. Line 1883: '瑙傚療涓? -> '观察中'
# 瑙傚(U+7459 U+50AC) 療(U+7642) 涓(U+6D93) ? 
obs_garbled = g(0x7459, 0x50AC, 0x7642, 0x6D93, 0x003F)
c = text.count(obs_garbled)
print(f"观察中 garbled: {c}")
text = text.replace("'" + obs_garbled, "'观察中'")

# 4. Line 1885: '婧愮姸鎬佹甯? -> '源状态正常'
# 婧愮姸鎬佹\ue11c甯?
src_garbled_parts = []
for i, line in enumerate(text.split("\n")):
    if "婧愮姸鎬佹" in line and i >= 1559:
        m = re.search(r"'(婧愮姸鎬佹[^']*\?)", line)
        if m:
            g_str = m.group(1)
            cps = " ".join(f"U+{ord(c):04X}" for c in g_str)
            print(f"  Source normal: {cps}")
            text = text.replace("'" + g_str, "'源状态正常'")
            break

# 5. Line 1923: 寰呭\ue632瀹＄爺... -> 待审定研报
# Find and replace
for i, line in enumerate(text.split("\n")):
    if "寰呭" in line and i >= 1559 and "colspan" in line:
        m = re.search(r">(寰呭[^<]*)<", line)
        if m:
            g_str = m.group(1)
            print(f"  Pending review: {repr(g_str)}")
            text = text.replace(g_str, "暂无待审定研报")
            break

# 6. Line 1939: '鏈\ue048獙璇?' -> '未验证'
for i, line in enumerate(text.split("\n")):
    if "font-family:monospace" in line and "鏈" in line and i >= 1559:
        m = re.search(r"'(鏈[^']*\?)", line)
        if m:
            g_str = m.group(1)
            print(f"  Unverified: {repr(g_str)}, len={len(g_str)}")
            text = text.replace("'" + g_str, "'未验证'")
            break

# 7. Line 1993: '鏈\ue044綍鍏? -> '未录入'
for i, line in enumerate(text.split("\n")):
    if "login_source" in line and "鏈" in line and i >= 1559:
        m = re.search(r"'(鏈[^']*\?)", line)
        if m:
            g_str = m.group(1)
            print(f"  Not recorded: {repr(g_str)}")
            text = text.replace("'" + g_str, "'未录入'")
            break

# 8. Line 2091: '寰呭\ue632瀹＄爺鎶? -> '待审定研报'
for i, line in enumerate(text.split("\n")):
    if "renderTableError" in line and "寰呭" in line and i >= 1559:
        m = re.search(r"'(寰呭[^']*\?)", line)
        if m:
            g_str = m.group(1)
            print(f"  Pending report: {repr(g_str)}")
            text = text.replace("'" + g_str, "'待审定研报'")
            break

# 9. Line 2127: '—锛?) -> '…吗？')
# The line: if (!reportId || !confirm('确认' + action + '研报 ' + reportId.slice(0,8) + '—锛?)) return;
# 锛? should be ？ (fullwidth question mark) so the full is '—？')
text = text.replace("'—锛?)", "'—？'))")
# Actually the pattern is: + '—锛?) meaning it's: ...+ '—？') where ?) is actually just ?))
# Let me re-check
for i, line in enumerate(text.split("\n")):
    if "confirm" in line and "reportId" in line and i >= 1559:
        print(f"  Confirm line {i+1}: {line.strip()[:150]}")
        break

# 10. Line 2145: alert('已 + action + '鐮旀姤銆?) -> alert('已' + action + '研报。')
# The garbled: '已 lacks closing quote, and 鐮旀姤銆? = 研报。
text = text.replace("alert('已 + action + '鐮旀姤銆?)", "alert('已' + action + '研报。')")

# 11. HTML string patterns in JS (lines 1743, 1809):
# 浠婃棩鏆傛棤娴佹按绾胯\ue187褰? -> 今日暂无流水线记录
# 鏈€杩?7 澶╂殏鏃犺皟搴﹁\ue187褰? -> 最近7天暂无调度记录
# These are in HTML template strings, need to find the garbled portion
for i, line in enumerate(text.split("\n")):
    if "浠婃棩" in line and i >= 1559:
        m = re.search(r">(浠婃棩[^<]*)<", line)
        if m:
            g_str = m.group(1)
            print(f"  Today no pipeline: {repr(g_str)}")
            text = text.replace(g_str, "今日暂无流水线记录")

for i, line in enumerate(text.split("\n")):
    if "鏈€杩" in line and "7" in line and i >= 1559:
        m = re.search(r">(鏈€杩[^<]*)<", line)
        if m:
            g_str = m.group(1)
            print(f"  Last 7 days: {repr(g_str)}")
            text = text.replace(g_str, "最近7天暂无调度记录")

# 12. Line 1785: Already partly fixed. Check
# 13. Line 1789: 总览不可用 - check 
for i, line in enumerate(text.split("\n")):
    if "position-mini-grid" in line and "总览" in line and i >= 1559:
        m = re.search(r">(总览[^<]*)<", line)
        if m:
            print(f"  Total unavailable: {repr(m.group(1))}")

# Check remaining issues
lines = text.split("\n")
remaining = 0
for i in range(1559, min(2153, len(lines))):
    line = lines[i]
    has_pua = any("\ue000" <= ch <= "\uf8ff" for ch in line)
    has_broken = bool(re.search(r"[\u4e00-\u9fff]\?[,;}\)]", line))
    if has_pua or has_broken:
        remaining += 1
        print(f"  REMAINING line {i+1}: {line.strip()[:100]}")

print(f"\nRemaining issues: {remaining}")

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Saved.")
