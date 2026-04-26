"""Fix encoding corruption in admin.html — TASK_CN and other garbled Chinese."""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

# Find all garbled TASK_CN values and fix them
# The corruption is: original UTF-8 bytes interpreted as GBK, then saved as UTF-8
# To reverse: encode current text as GBK to get original UTF-8 bytes, then decode as UTF-8

# The problem is that some values have a trailing '?' which is a replacement char from the 
# GBK decode that lost a byte. We need to handle these specially.

# Known TASK_CN mapping:
TASK_CN_CORRECT = {
    'daily_pipeline': '每日流水线',
    'fr01_stock_pool': '股票池刷新',
    'fr04_data_collect': '数据采集',
    'fr05_market_state': '市场状态',
    'fr06_report_gen': '研报生成',
    'fr07_settlement': '结算',
    'fr08_sim_positioning': '模拟建仓',
    'fr08_sim_trade': '模拟交易',
    'fr13_notification': '通知派发',
    'fr13_event_notify': '事件通知',
    'billing_poller': '支付对账',
    'tier_expiry_sweep': '会员过期清理',
    'market_state': '市场状态',
    'sim_open_price': '模拟开盘价',
    'sim_settle': '模拟结算',
    'tier9_report': '研报定时',
}

# Build the correct TASK_CN line
entries = ",".join(f"'{k}':'{v}'" for k, v in TASK_CN_CORRECT.items())
correct_task_cn = "    var TASK_CN = {" + entries + "};"

# Replace all TASK_CN lines (there are 2 occurrences)
pattern = re.compile(r"^\s*var TASK_CN = \{.*?\};", re.MULTILINE)
matches = list(pattern.finditer(text))
print(f"Found {len(matches)} TASK_CN lines")
for m in matches:
    print(f"  at pos {m.start()}: {m.group()[:80]}...")

text = pattern.sub(correct_task_cn, text)

# Also fix taskCn default: '鏈煡' should be '未知'  
text = text.replace("'鏈煡'", "'未知'")

# Fix other known garbled Chinese strings in the file
# Let's try a character-level approach for remaining non-ASCII text
def try_fix_segment(match):
    s = match.group(0)
    try:
        fixed = s.encode('gbk').decode('utf-8')
        return fixed
    except:
        return s

# Apply to remaining content
text = re.sub(r'[^\x00-\x7f]+', try_fix_segment, text)

with open("app/web/templates/admin.html", "w", encoding="utf-8") as f:
    f.write(text)

# Verify
with open("app/web/templates/admin.html", encoding="utf-8") as f:
    verify = f.read()
idx = verify.find("TASK_CN")
print("Fixed TASK_CN:", verify[idx:idx+300])
