"""
Comprehensive fix for ALL garbled Chinese text in admin.html.
Fixes both HTML template section and duplicated JS inline HTML.
"""
import re

with open("app/web/templates/admin.html", encoding="utf-8") as f:
    text = f.read()

original_len = len(text)

# ============================================================
# PHASE 1: Fix garbled text in HTML template section (lines 1-590)
# ============================================================

# Line 11: Title tag - broken closing tag
text = text.replace(
    "管理后台 - A鑲＄爺鎶ュ钩鍙?/title>",
    "管理后台 - A股研报平台</title>"
)

# Line 131: Comment
text = text.replace("<!-- 绉诲姩绔?Tab -->", "<!-- 移动端 Tab -->")

# Line 143: Mobile tab
text = text.replace(">澶嶅</button>", ">复审</button>")
# Broader form with garbled + broken closing tag
text = text.replace(">澶嶅<", ">复审<")

# Line 163: Comment
text = text.replace("<!-- 渚ц竟鏍?-->", "<!-- 侧边栏 -->")

# Line 187: Nav button  
text = text.replace("绯荤粺总览</button>", "系统总览</button>")
text = text.replace("绯荤粺总览</h1>", "系统总览</h1>")

# Line 197: Nav button - broken closing tag
text = text.replace("寰呭瀹＄爺鎶?/button>", "待审定研报</button>")

# Line 199: User overview nav - garbled
# Try different garbled forms based on PUA analysis
text = text.replace("鐢ㄦ埛姒傝</button>", "用户概览</button>")

# Line 207: Running note label
text = text.replace("杩愯顢戣鏄?", "运行说明")
text = text.replace(">运行说明</span>", ">运行说明</span>")

# Line 211: Description about DAG
# This is a long garbled string - replace the entire content
text = text.replace(
    "绠＄悊椤典笉鍐嶆彁渚?DAG 琛ヨ窇入口锛涘闇€琛ヨ窇锛岃浣跨敤鏂扮殑鍐呴儴杩愮淮娴佺▼骞朵互调度状态佷负鍑嗐€?",
    "管理页不再提供 DAG 补跑入口；如需补跑，请使用新的内部运维流程并以调度状态为准。"
)

# Line 219: Return to home - may already be correct
text = text.replace("杩斿洖棣栭〉", "返回首页")

# Line 221: Stats dashboard link  
text = text.replace("缁熻鐪嬫澘</a>", "统计看板</a>")

# Line 253: KPI label - broken closing div
text = text.replace("鏍稿績姹犺妯?/div>", "核心池规模</div>")

# Line 257: KPI sub - broken closing div  
text = text.replace("鍙股票/div>", "只股票</div>")
# alternative form
text = text.replace("鍙\u80a1\u7968/div>", "只股票</div>")

# Line 267: KPI sub
text = text.replace("鍚?BUY/HOLD/SELL", "含 BUY/HOLD/SELL")
text = text.replace("閸?BUY/HOLD/SELL", "含 BUY/HOLD/SELL")

# Line 283: KPI label - broken
text = text.replace("寰呭瀹?/div>", "待审定</div>")

# Line 287: KPI sub
text = text.replace("浠芥姤鍛婇渶瀹℃壒", "份报告需审批")

# Line 297: KPI label - broken  
text = text.replace("鏈€鏂癒绾挎日期/div>", "最新K线日期</div>")
text = text.replace("鏈€鏂癒绾挎棩鏈?/div>", "最新K线日期</div>")

# Line 307: KPI label - broken
text = text.replace("鏈€鏂板競鍦虹姸鎬?/div>", "最新市场状态</div>")

# Line 317: KPI sub
text = text.replace("已生成姹犳€绘暟", "已生成/池总数")

# Line 339: Section title
text = text.replace("椤甸潰鍙ｅ緞閿氱偣", "页面口径锚点")

# Line 341, 351, 371, 387, 399, 445, 485, 529: Loading text - broken
text = text.replace("鍔犺浇涓?..", "加载中...")
text = text.replace("閸旂姾娴囨稉?..", "加载中...")

# Line 361: Section title - broken closing tag
text = text.replace("流水线阶段进度/h2>", "流水线阶段进度</h2>")

# Line 367: Table headers - broken closing tags
text = text.replace("<th>阶段</th><th>状态/th><th>开始时间/th>", "<th>阶段</th><th>状态</th><th>开始时间</th>")
text = text.replace(">状态/th>", ">状态</th>")
text = text.replace(">开始时间/th>", ">开始时间</th>")

# Garbled table header variants
text = text.replace("闃舵</th>", "阶段</th>")

# Line 383: Section title
text = text.replace("鎸佷粨姒傝</h2>", "持仓概览</h2>")

# Line 395: Section title - broken closing tag
text = text.replace("鏈€杩戜换鍔℃憳瑕?/h2>", "最近任务摘要</h2>")

# Line 425: Scheduler description
text = text.replace(
    "璇ラ〉鍙睍绀烘渶杩戣皟搴︾粨鏋滀笌错误鍘熷洜锛屽凡閫€浼戠殑浜哄伐琛ヨ窇入口涓嶅啀对外寮€鏀俱€?",
    "该页只展示最近调度结果与错误原因，已退休的人工补跑入口不再对外开放。"
)

# Line 439: Scheduler table headers - broken
text = text.replace("浠诲姟鍚?/th>", "任务名</th>")
text = text.replace("浜ゆ槗鏃?/th>", "交易日</th>")
text = text.replace("瑙﹀发源/th>", "触发源</th>")
text = text.replace("鐟欙箑鍙戞簮/th>", "触发源</th>")

# Line 459: Comment
text = text.replace("<!-- 寰呭瀹＄爺鎶?-->", "<!-- 待审定研报 -->")

# Line 465: Section title - broken
text = text.replace("寰呭瀹＄爺鎶?/h1>", "待审定研报</h1>")

# Line 479: Review table headers - broken
text = text.replace("澶嶅状态/th>", "复审状态</th>")
text = text.replace("婢跺秴</th>", "复审</th>")  

# Line 521: Users table headers
text = text.replace("<th>閭</th>", "<th>邮箱</th>")
text = text.replace("閭楠岃瘉</th>", "邮箱验证</th>")

# Session table headers (line ~550)
text = text.replace("鏈€杩戝埛鏂?/th>", "最近刷新</th>")

# Session loading text
text = text.replace(
    "切换到本页时加载 Cookie 浼氳瘽鍋ュ悍状态/td>",
    "切换到本页时加载 Cookie 会话健康状态</td>"
)

# ============================================================
# PHASE 2: Fix garbled PUA lines in both HTML and JS sections
# Using exact regex patterns for PUA character sequences
# ============================================================

def fix_pua_line(text, garbled, correct):
    """Replace garbled text that may contain PUA chars."""
    count = text.count(garbled)
    if count > 0:
        text = text.replace(garbled, correct)
    return text

# Fix various garbled patterns found via PUA analysis
# Mobile tab: 复审
text = re.sub(r"婢跺秴[\ue600-\uf8ff]?", "复审", text)

# 待审定研报 variants with PUA
text = re.sub(r"瀵板懎[\ue600-\uf8ff]鐎癸紕鐖洪幎[\ue600-\uf8ff]?", "待审定研报", text)
text = re.sub(r"瀵板懎[\ue600-\uf8ff]?", "待审", text)
text = re.sub(r"寰呭[\ue600-\uf8ff]瀹＄爺鎶[\ue600-\uf8ff]?", "待审定研报", text)

# 用户概览 with PUA
text = re.sub(r"鐢ㄦ埛姒傝[\ue600-\uf8ff]", "用户概览", text)

# 运行说明 with PUA
text = re.sub(r"杩愯[\ue600-\uf8ff]鏄[\ue600-\uf8ff]?", "运行说明", text)

# 统计看板 with PUA  
text = re.sub(r"缂佺喕[\ue600-\uf8ff]閻婢[\ue600-\uf8ff]?", "统计看板", text)
text = re.sub(r"缂佺喕[\ue600-\uf8ff]?闅忕窗瀣緲", "统计看板", text)

# 核心池规模 with PUA
text = re.sub(r"閺嶇绺惧Ч鐘[\ue600-\uf8ff]濡[\ue600-\uf8ff]?", "核心池规模", text)
text = re.sub(r"閺嶇绺惧Ч鐘侯潐濡[\ue600-\uf8ff]?", "核心池规模", text)

# 只股票 with PUA
text = re.sub(r"閸橽[\ue600-\uf8ff]鑲＄エ", "只股票", text)
text = re.sub(r"閸欘亣鑲＄エ", "只股票", text)

# 待审定 (standalone) with PUA
text = re.sub(r"瀵板懎[\ue600-\uf8ff]鐎?", "待审定", text)

# 阶段 with PUA
text = re.sub(r"闃舵[\ue600-\uf8ff]", "阶段", text)

# 错误 with PUA
text = re.sub(r"閿欒[\ue600-\uf8ff]", "错误", text)

# 持仓概览 with PUA
text = re.sub(r"閹镐椒绮ㄥ鍌[\ue600-\uf8ff][\ue600-\uf8ff]?", "持仓概览", text)
text = re.sub(r"閹镐椒绮ㄥ鍌濐潔", "持仓概览", text)

# 邮箱 and variants with PUA
text = re.sub(r"闁喚[\ue600-\uf8ff][\ue600-\uf8ff]?", "邮箱", text)
text = re.sub(r"闁喚顔堟宀冪槈", "邮箱验证", text)  
text = re.sub(r"闁喚顔[\ue600-\uf8ff]?", "邮箱", text)

# Long description in JS sections about DAG
text = re.sub(
    r"鐠囥儵銆夐崣[\ue600-\uf8ff]?鐫嶇粈鐑樻付鏉╂垼鐨熸惔锔剧波閺嬫粈绗岄敊璇[\ue600-\uf8ff]閸樼喎娲滈敍灞藉嚒闁偓娴兼垹娈戞禍鍝勪紣鐞涖儴绐囧叆鍙ｆ稉宥呭晙瀵瑰[\ue600-\uf8ff]瀵偓閺€淇扁偓[\ue600-\uf8ff]?",
    "该页只展示最近调度结果与错误原因，已退休的人工补跑入口不再对外开放。",
    text
)
# Simpler fallback
text = re.sub(
    r"鐠囥儵銆夐崣.{30,80}淇扁偓.?",
    "该页只展示最近调度结果与错误原因，已退休的人工补跑入口不再对外开放。",
    text
)

# Long description in HTML about DAG (another variant)
text = re.sub(
    r"缁狅紕鎮婃い鍏哥瑝閸愬秵褰佹笟.{1,3}DAG 鐞涖儴绐囧叆鍙ｉ敍娑.{1,10}闂団偓鐞涖儴绐囬敍宀.{1,10}娴ｈ法鏁ら弬鎵畱閸愬懘鍎存潻鎰樊濞翠胶鈻奸獮鏈典簰璋冨害鐘舵€佷椒璐熼崙鍡愨偓.?",
    "管理页不再提供 DAG 补跑入口；如需补跑，请使用新的内部运维流程并以调度状态为准。",
    text
)

# ============================================================
# PHASE 3: Fix remaining garbled text in JS duplicate HTML
# (lines ~1100-2150 have the same HTML duplicated in JS)
# ============================================================

# These are the same patterns but appearing in JS string context
# Most should already be handled by the global replacements above

# Additional JS-specific patterns:
# 研报ID
text = text.replace("鐮旀姤ID", "研报ID")
# 股票代码
text = text.replace("鑲＄エ浠ｇ爜", "股票代码")
# 交易日 (in table headers)
text = text.replace("娴溿倖妲楅弮?/th>", "交易日</th>")
text = text.replace("娴溿倖妲楅弮", "交易日")
# 复审状态
text = text.replace("婢跺秴顓哥姸鎬?th>", "复审状态</th>")
text = re.sub(r"婢跺秴[\ue600-\uf8ff]?哥姸鎬[\ue600-\uf8ff]?", "复审状态", text)
# 数据质量
text = text.replace("鏁版嵁璐ㄩ噺", "数据质量")
# 创建时间
text = text.replace("鍒涘缓鏃堕棿", "创建时间")
# 操作
text = text.replace("鎿嶄綔", "操作")
# 用户ID
text = text.replace("鐢ㄦ埛ID", "用户ID")
# 角色
text = text.replace("瑙掕壊", "角色")
# 会员等级
text = text.replace("浼氬憳绛夌骇", "会员等级")
# 完成时间
text = text.replace("瀹屾垚鏃堕棿", "完成时间")
# 时间窗口
text = text.replace("鏃堕棿绐楀彛", "时间窗口")
# 时长
text = text.replace("鏃堕暱", "时长")
# 重试
text = text.replace("閲嶈瘯", "重试")
# 原因
text = text.replace("鍘熷洜", "原因")
# 任务名
text = text.replace("浠诲姟鍚", "任务名")
# 任务
text = text.replace("浠诲姟", "任务")
# 交易日 (standalone)
text = text.replace("浜ゆ槗鏃", "交易日")
# 触发源
text = text.replace("瑙﹀发源", "触发源")
text = text.replace("鐟欙箑鍙戞簮", "触发源")
# 结束时间
text = text.replace("缁撴潫鏃堕棿", "结束时间")
text = text.replace("缁撴潫鏃?棿", "结束时间")
# 状态 (standalone - careful not to over-replace)
text = text.replace("鐘舵€?th>", "状态</th>")
text = text.replace("鐘舵€?", "状态")
# 开始时间 
text = text.replace("寮€濮嬫椂闂?th>", "开始时间</th>")
text = text.replace("寮€濮嬫椂闂?", "开始时间")
# 最近刷新
text = text.replace("鏈€杩戝埛鏂?", "最近刷新")
# 会话健康状态
text = text.replace("浼氳瘽鍋ュ悍状态", "会话健康状态")
text = text.replace("浼氳瘽鍋ュ悍鐘舵€?", "会话健康状态") 
# 加载中
text = text.replace("鍔犺浇涓?", "加载中")
text = text.replace("閸旂姾娴囨稉?", "加载中")
# 页面口径锚点 (if still remaining)
text = text.replace("椤甸潰鍙ｅ緞閿氱偣", "页面口径锚点")
# 最新K线日期
text = text.replace("鏈€鏂癒绾挎棩鏈?", "最新K线日期")
text = text.replace("閺堚偓閺傜檼缁炬寧鏃ユ湡", "最新K线日期")
# 最新市场状态
text = text.replace("鏈€鏂板競鍦虹姸鎬?", "最新市场状态")
text = text.replace("閺堚偓閺傛澘绔堕崷铏瑰Ц閹?", "最新市场状态")
# 管理后台 (title context)
text = text.replace("绠＄悊鍚庡彴", "管理后台")
# 已生成/池总数
text = text.replace("已生成姹犳€绘暟", "已生成/池总数")

# ============================================================
# PHASE 4: Fix remaining garbled text in JS logic section
# ============================================================

# Return home
text = text.replace("杩斿洖棣栭〉", "返回首页")

# 系统总览
text = text.replace("缁崵绮烘€昏", "系统总览")
text = text.replace("绯荤粺总览", "系统总览")

# 核心池规模 (already handled above, but add fallback)
text = text.replace("鏍稿績姹犺妯?", "核心池规模")
text = text.replace("鏍稿績姹犺妯", "核心池规模")

# 含 BUY
text = text.replace("鍚?BUY", "含 BUY")
text = text.replace("閸?BUY", "含 BUY")

# 待审定 (sub label)
text = text.replace("寰呭瀹?", "待审定")
text = text.replace("瀵板懎", "待审")

# 份报告需审批 
text = text.replace("浠芥姤鍛婇渶瀹℃壒", "份报告需审批")

# 持仓概览
text = text.replace("鎸佷粨姒傝", "持仓概览")

# 最近任务摘要
text = text.replace("鏈€杩戜换鍔℃憳瑕?", "最近任务摘要")
text = text.replace("閺堚偓鏉╂垳鎹㈤崝鈩冩喅鐟?", "最近任务摘要")

# 流水线阶段进度
text = text.replace("娴佹按绾块樁娈佃繘搴?", "流水线阶段进度")

# ============================================================
# PHASE 5: Fix JS inline strings (alert messages, labels, etc.)
# ============================================================

# 无数据
text = text.replace("鏃犳暟鎹?", "无数据")
text = text.replace("鏃犳暟鎹", "无数据")

# 总览接口不可用
text = text.replace("鎬昏鎺ュ彛涓嶅彲鐢?", "总览接口不可用")
text = text.replace("鎬昏鎺ュ口涓嶅彲鐢?", "总览接口不可用")
text = text.replace("鎬昏涓嶅彲鐢?", "总览不可用")
text = text.replace("总览涓嶅彲鐢?", "总览不可用")

# 总览接口加载失败！
text = text.replace("鎬昏鎺ュ彛鍔犺浇澶辫触锛?", "总览接口加载失败！")
text = text.replace("鎬昏鎺ュ口鍔犺浇澶辫触锛?", "总览接口加载失败！")
text = text.replace("鎬昏鎺ュ口加载失败", "总览接口加载失败")

# 总览加载失败！
text = text.replace("鎬昏鍔犺浇澶辫触锛?", "总览加载失败！")

# 暂无来源日期信息
text = text.replace("鏆傛棤鏉ユ簮鏃ユ湡淇℃伅", "暂无来源日期信息")

# 当前各项页面日期锚点未完全对齐。
text = text.replace("褰撳墠鍚勯」椤甸潰鏃ユ湡閿氱偣鏈畬鍏ㄥ榻愩€?", "当前各项页面日期锚点未完全对齐。")

# 口径说明：
text = text.replace("鍙ｅ緞璇存槑锛?", "口径说明：")
text = text.replace("鍙ｅ緞璇存槑", "口径说明")

# 今日研报=当日全部已发布研报 
text = text.replace("浠婃棩鐮旀姤褰撴棩鍏ㄩ儴宸插彂甯冪爺鎶?", "今日研报=当日全部已发布研报")
text = text.replace("浠婃棩鐮旀姤=褰撴棩鍏ㄩ儴宸插彂甯冪爺鎶?", "今日研报=当日全部已发布研报")

# 今日买入信号=
text = text.replace("浠婃棩涔板叆淇″彿=", "今日买入信号=")

# 当日 BUY 中 confidence≥0.65
text = text.replace("褰撴棩 BUY 涓?confidence≥0.65", "当日 BUY 中 confidence≥0.65")

# 暂无持仓数据
text = text.replace("鏆傛棤鎸佷粨鏁版嵁", "暂无持仓数据")

# 今日暂无流水线记录
text = text.replace("浠婃棩鏆傛棤娴佹按绾胯褰?", "今日暂无流水线记录")
text = re.sub(r"浠婃棩鏆傛棤娴佹按绾胯[\ue600-\uf8ff]褰[\ue600-\uf8ff]?", "今日暂无流水线记录", text)

# 暂无任务记录
text = text.replace("鏆傛棤浠诲姟璁板綍", "暂无任务记录")

# 活跃持仓
text = text.replace("娲昏穬鎸佷粨", "活跃持仓")

# 最近7天暂无调度记录
text = text.replace("鏈€杩?7 澶╂殏鏃犺皟搴﹁褰?", "最近7天暂无调度记录")
text = re.sub(r"鏈€杩?7\s*澶╂殏鏃犺皟搴﹁[\ue600-\uf8ff]褰[\ue600-\uf8ff]?", "最近7天暂无调度记录", text)

# 加载失败(various)
text = text.replace("鍔犺浇澶辫触", "加载失败")
text = text.replace("加载澶辫触", "加载失败")

# 调度摘要加载失败  
text = text.replace("璋冨害鎽樿鍔犺浇澶辫触", "调度摘要加载失败")

# 暂无待审定研报
text = text.replace("鏆傛棤寰呭瀹＄爺鎶?", "暂无待审定研报")
text = text.replace("鏆傛棤寰呭瀹＄", "暂无待审定")

# 暂无用户数据
text = text.replace("鏆傛棤鐢ㄦ埛鏁版嵁", "暂无用户数据")

# 暂无 Cookie 会话记录
text = text.replace("鏆傛棤 Cookie 浼氳瘽璁板綍", "暂无 Cookie 会话记录")

# 已验证/未验证
text = text.replace("宸查獙璇?", "已验证")
text = text.replace("鏈獙璇?", "未验证")
text = re.sub(r"鏈[\ue600-\uf8ff]獙璇[\ue600-\uf8ff]?", "未验证", text)

# 未录入
text = re.sub(r"鏈[\ue600-\uf8ff]綍鍏[\ue600-\uf8ff]?", "未录入", text)
text = text.replace("鏈綍鍏?", "未录入")

# 编辑
text = text.replace("缂栬緫", "编辑")

# 来源
text = text.replace(">鏉ユ簮<", ">来源<")

# 服务降级/服务可用/状态未知
text = text.replace("鏈嶅姟闄嶇骇", "服务降级")
text = text.replace("鏈嶅姟鍙敤", "服务可用")
text = text.replace("鐘舵€佹湭鐭?", "状态未知")
text = re.sub(r"鐘舵€佹湭鐭[\ue600-\uf8ff]?", "状态未知", text)

# 观察中
text = text.replace("瑙傚療涓?", "观察中")
text = re.sub(r"瑙傚療涓[\ue600-\uf8ff]?", "观察中", text)

# 存在异常源/源状态正常
text = text.replace("婧愮姸鎬佹甯?", "源状态正常")
text = re.sub(r"婧愮姸鎬佹[\ue600-\uf8ff]甯[\ue600-\uf8ff]?", "源状态正常", text)
text = text.replace("瀛樺湪寮傚父婧?", "存在异常源")
text = re.sub(r"瀛樺湪寮傚父婧[\ue600-\uf8ff]?", "存在异常源", text)

# 运行健康
text = text.replace("杩愯鍋ュ悍", "运行健康")
text = re.sub(r"杩愯[\ue600-\uf8ff]鍋ュ悍", "运行健康", text)

# 业务健康
text = text.replace("涓氬姟鍋ュ悍", "业务健康")

# 数据质量 (standalone)
text = text.replace("鏁版嵁璐ㄩ噺", "数据质量")

# 预测样本
text = text.replace("棰勬祴鏍锋湰", "预测样本")

# 仅用于评估
text = text.replace("浠呯敤浜庤瘎浼?", "仅用于评估")
text = text.replace("浠呯敤浜庤瘎浼", "仅用于评估")

# 准确率/降级率
text = text.replace("鍑嗙'鐜?", "准确率")
text = re.sub(r"鍑嗙[\ue600-\uf8ff]鐜[\ue600-\uf8ff]?", "准确率", text)
text = text.replace("闄嶇骇鐜?", "降级率")

# 个数据源
text = text.replace("涓暟鎹簮", "个数据源")
text = re.sub(r"涓[\ue600-\uf8ff]暟鎹[\ue600-\uf8ff]簮", "个数据源", text)

# runtime_state=
# This should be fine as-is

# 总览 (standalone JS)
text = text.replace("鎬昏", "总览")

# 系统健康 
text = text.replace("绯荤粺鍋ュ悍", "系统健康")
text = text.replace("绯荤粺鍋ュ悍鎺ュ彛", "系统健康接口")

# 调度接口
text = text.replace("璋冨害鎺ュ彛", "调度接口")

# 用户接口
text = text.replace("鐢ㄦ埛鎺ュ彛", "用户接口")

# 复审接口
text = text.replace("澶嶅鎺ュ彛", "复审接口")
text = re.sub(r"澶嶅[\ue600-\uf8ff]鎺ュ彛", "复审接口", text)

# 操作失败！
text = text.replace("鎿嶄綔澶辫触锛?", "操作失败！")

# 请求失败！
text = text.replace("璇锋眰澶辫触锛?", "请求失败！")

# 确认 (for confirm dialog)
text = re.sub(r"纭[\ue600-\uf8ff]{0,2}", "确认", text)
text = text.replace("纭", "确认")

# 研报 (standalone)
text = text.replace("鐮旀姤", "研报")

# ⚠ symbol
text = text.replace("鈿?", "⚠")
text = text.replace("鈿", "⚠")

# — (em dash)
text = text.replace("鈥?", "—")

# 锛 (fullwidth semicolon/separators)
text = text.replace("锛?", "！")

# % 完成
text = text.replace("% 瀹屾垚", "% 完成")
text = text.replace("%瀹屾垚", "% 完成")

# 已/已+garbled → patterns for actions
text = text.replace("宸?", "已")

# Cookie session label fixes
text = text.replace("Cookie 浼氳瘽", "Cookie 会话")

# ============================================================
# PHASE 6: Fix broken HTML closing tags globally
# Pattern: Chinese text followed by /tagname> instead of </tagname>
# ============================================================

# Fix patterns where < was consumed by garbled chars
# These should have been caught by specific fixes above, but let's catch remaining ones
text = re.sub(r'([^<\s])/title>', r'\1</title>', text)
text = re.sub(r'([^<\s])/button>', r'\1</button>', text)
text = re.sub(r'([^<\s])/h1>', r'\1</h1>', text)
text = re.sub(r'([^<\s])/h2>', r'\1</h2>', text)
text = re.sub(r'([^<\s])/div>', r'\1</div>', text)
text = re.sub(r'([^<\s])/th>', r'\1</th>', text)
text = re.sub(r'([^<\s])/td>', r'\1</td>', text)
text = re.sub(r'([^<\s])/a>', r'\1</a>', text)
text = re.sub(r'([^<\s])/span>', r'\1</span>', text)

# ============================================================
# PHASE 7: Clean up any remaining double-garbled artifacts
# ============================================================

# Fix common patterns where garbled text creates broken HTML structure
# e.g. "状态</th>" where "状态" appears without the opening <th>

# Ensure all remaining single-char garbled PUA are removed
text = re.sub(r'[\ue000-\uf8ff]', '', text)

# Save
with open("app/web/templates/admin.html", "w", encoding="utf-8", newline='\n') as f:
    f.write(text)

new_len = len(text)
print(f"Original: {original_len} chars")
print(f"Fixed: {new_len} chars")
print(f"Delta: {new_len - original_len}")

# Verify: count remaining garbled indicators
remaining_garbled = 0
for i, line in enumerate(text.split('\n'), 1):
    # Check for common garbled CJK patterns
    has_suspicious = False
    for c in line:
        cp = ord(c)
        if cp in range(0x9300, 0x9500) or cp in range(0xE000, 0xF900):
            has_suspicious = True
            break
    if has_suspicious:
        remaining_garbled += 1
        if remaining_garbled <= 20:
            print(f"  Remaining garbled L{i}: {line.strip()[:100]}")

print(f"\nRemaining garbled lines: {remaining_garbled}")
