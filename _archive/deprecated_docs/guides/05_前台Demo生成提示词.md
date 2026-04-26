# 前台 Demo 生成提示词

> **用途**：在正式写业务代码前，先按文档生成一个**静态/半静态前台 Demo**，让你直观看到系统长什么样、有哪些模块、每个模块展示哪些数据，便于提前验证体验和布局，避免代码写完再返工。
>
> **用法**：将下方「完整提示词」复制到 AI 对话（如 Cursor、Claude 等），并附上本项目的 `docs/core/07_系统目标与范围整合.md`、`docs/core/11_UI_设计规范.md` 等文档引用。

---

## 一、完整提示词（可直接复制使用）

```
【任务】根据项目文档，生成一个 A 股个股研报平台的**前台 Demo**，纯前端展示，使用 Mock 数据，不需要后端接口。

【目标】
1. 让你在写正式代码前，就能看到系统长什么样
2. 清晰展示每个页面的模块结构、布局和每个模块的数据内容
3. 方便提前验证：布局是否合理、信息是否完整、体验是否达标

【约束】
- 纯 HTML + CSS + 少量 JS（无需框架，可单文件或多页面）
- 所有数据用 Mock，可硬编码在 HTML 或 .js 中
- 不连接真实 API，不依赖后端
- 页面可点击跳转（首页→列表→详情→看板等）
- **页面自动化适配浏览器**：375px～桌面端无横向溢出，关键信息在小屏下可读
- 设计规范遵循 docs/core/11_UI_设计规范.md（Design Tokens、配色、组件规范）
- **专业名词旁附解释**：使用专业名词（如 MA20、ATR、量能比、年化 Alpha、盈亏比等）时，在其附近用小字给出简短解释，保持美观

【CSS 基础规范（demo.css 必须严格遵守）】

## A. 间距变量（不可遗漏任何一个）
```css
--space-1: 4px;
--space-2: 8px;
--space-3: 12px;
--space-4: 16px;
--space-5: 20px;    /* ← 必须定义，否则 margin/padding 塌陷 */
--space-6: 24px;
--space-8: 32px;
--space-10: 40px;   /* ← 必须定义，Hero 区与页脚依赖此值 */
--space-12: 48px;
```

## B. 字体栈与混排规范
```css
/* 数字/英文标题（金融数字需等宽、清晰）*/
--font-display: 'Inter', 'Outfit', system-ui, sans-serif;

/* 正文（中英混排，系统字体优先，避免 Web Font 与中文 fallback 视觉割裂）*/
--font-body: 'Inter', -apple-system, BlinkMacSystemFont,
             'PingFang SC', 'Hiragino Sans GB',
             'Microsoft YaHei UI', 'Microsoft YaHei', sans-serif;
```

**中英混排必须遵守的 4 条规则：**
1. `letter-spacing` 只对英文/数字使用（负值在中文上会压字间距），中文元素一律设 `letter-spacing: normal` 或 `0`
2. 中文正文 `line-height` 不低于 **1.75**；英文标题/数字可用 1.2～1.4
3. 中文字重上限建议 **600**（bold 在中文宋体/黑体会过重模糊）；数字/英文标题可用 700
4. `font-family` 在 `body` 统一设置 `var(--font-body)`，Outfit/Inter 只在数字展示类元素（`.metric-card .val`, `.platform-stats .num`, `nav-brand`, `.card-title` 等）用 `var(--font-display)`

## C. 字号层级（完整定义，不可有未引用的变量）
```css
--fs-2xs:  12px;   /* 徽章、极小标注 */
--fs-xs:   13px;   /* 标签、辅助文字 */
--fs-sm:   14px;   /* 次级、说明、表格 */
--fs-base: 15px;   /* 正文 */
--fs-lg:   16px;   /* 副标题、强调 */
--fs-xl:   18px;   /* 二级标题 */
--fs-2xl:  22px;   /* 一级标题 */
--fs-3xl:  28px;   /* 大数字（指标卡） */
```

## D. 全局标题层级（body 之后立即定义，避免各页 inline 补丁）
```css
body { font-family: var(--font-body); font-size: var(--fs-base); line-height: 1.75; }
h1 { font-family: var(--font-display); font-size: var(--fs-2xl); font-weight: 700; line-height: 1.25; letter-spacing: -0.01em; margin: 0 0 var(--space-4); }
h2 { font-family: var(--font-display); font-size: var(--fs-xl); font-weight: 600; line-height: 1.3; letter-spacing: 0; margin: 0 0 var(--space-3); }
h3 { font-family: var(--font-display); font-size: var(--fs-lg); font-weight: 600; line-height: 1.4; letter-spacing: 0; margin: 0 0 var(--space-3); }
p  { margin: 0 0 var(--space-3); }
```

## E. 卡片标题（.card-title）统一用 h3，不再 inline 覆盖
```css
.card-title { font-family: var(--font-display); font-size: var(--fs-lg); font-weight: 600; color: var(--ink); letter-spacing: 0; line-height: 1.4; margin-bottom: var(--space-4); }
```

## F. 导航 Brand 字体
```css
.nav-brand { font-family: var(--font-display); font-weight: 700; font-size: 1rem; letter-spacing: -0.01em; color: var(--ink); }
```

## G. 数字指标卡值
```css
.metric-card .val, .platform-stats .num { font-family: var(--font-display); font-size: var(--fs-3xl); font-weight: 700; letter-spacing: -0.02em; line-height: 1.1; }
```
> 仅在**纯数字区域**用 `letter-spacing: -0.02em`，含汉字的元素不得使用负 letter-spacing。

## H. 表格与列表行高
```css
.data-table td, .data-table th { line-height: 1.5; }   /* 表格行内容较密，1.5 足够 */
.report-row, .signal-card { line-height: 1.7; }        /* 卡片内有中文，1.7 */
```

## I. 响应式断点补全（640px 仅覆盖移动，需补 768px 平板）
```css
@media (max-width: 768px) {
  .subscribe-grid { grid-template-columns: repeat(2, 1fr); }
  .metrics-grid   { grid-template-columns: repeat(2, 1fr); }
}
@media (max-width: 640px) {
  .subscribe-grid { grid-template-columns: 1fr; }
  .metrics-grid   { grid-template-columns: 1fr 1fr; }
  .strategy-grid  { grid-template-columns: 1fr; }
  .profile-layout, .admin-layout { grid-template-columns: 1fr; }
  .filter-bar     { flex-direction: column; align-items: stretch; }
  .hero-search    { flex-direction: column; }
  h1 { font-size: var(--fs-xl); }
}
```

【必须包含的页面与模块】

## 1. 首页 index.html
- **顶部导航**：首页 | 研报列表 | 统计看板 | 模拟收益 | 登录 | 注册，居中排列；不设「关于与免责」链接；小屏可折叠为汉堡菜单
- 市场状态横幅：今日市场（BULL/NEUTRAL/BEAR）、沪深300近20日涨跌幅、量能比、MA20
- Hero 区：平台标语、股票代码输入框（跳转研报）
- 今日强信号列表：最多 6 条，每条含：股票代码+名称、信号类型徽章(A/B/C)、结论(买入/观望/卖出)、目标价、止损价、有效截止日
- 平台简介 + 数据统计：已追踪信号数、模拟胜率、运行天数
- **页脚风险提示与免责**：内容居中；用折叠区或小字号灰色卡片，默认一句「投资有风险，本平台仅供参考」+ [展开完整声明]，展开后显示完整免责文案（居中）

## 2. 研报列表页 reports_list.html
- 筛选栏：股票代码/名称、日期范围、结论(全部/买入/观望/卖出)、信号类型(全部/A/B/C)、持仓状态、排序
- 列表卡片：每条含 [代码+名称] [信号类型徽章] [结论徽章] 置信度、T日收盘价、目标价、止损价、持仓状态徽章、日期、[查看研报] 链接

## 3. 研报详情页 report_view.html（三层渐进式）
- **第一层（首屏）**：Hero（公司名、代码、行业、收盘价、涨跌幅、结论、置信度进度条、市场状态）、持仓追踪状态徽章、一句话结论、实操指令卡（含入场价区间、止损、目标价1/2、仓位建议、付费模糊化示例）
- **第二层**：未来趋势预测表(1/7/14/30/60日)、标的概况、操作建议、结论依据（证据卡片）、机会与风险、资金面（含数据来源标签）
- **第三层**：本股历史信号追踪表、AI推理全过程（可折叠）、所用数据清单

## 4. 统计看板 dashboard.html
- 方向命中率（监控用，标注口径）
- 四维度绩效：真实胜率、盈亏比、年化Alpha、样本数、覆盖率
- Tab 切换：方向命中率 / 模拟收益（可跳转 sim-dashboard）

## 5. 模拟收益看板 sim_dashboard.html
- 回撤状态横幅（REDUCE/HALT 时显示）
- 核心指标卡：累计收益、年化Alpha、最大回撤、胜率（按 A/B/C 分策略）
- 净值曲线图占位区（可画简单 SVG 折线示意）
- 分策略绩效卡片：A类/B类/C类 各显示胜率、盈亏比、笔数
- 交易明细表：股票、类型、开仓日、平仓日、净盈亏、研报链接
- 冷启动提示：样本<30 时显示"积累中 N/30 笔"

## 6. 登录页 login.html
- 简化导航（仅 Logo，可链回首页）
- **账号密码**：邮箱 或 手机号、密码、[忘记密码]（链接至 forgot_password.html）、[登录] 按钮（暂不支持短信验证码）
- 底部：还没有账号？[立即注册]
- **第三方登录**：QQ登录、微信登录 放在 [立即注册] **下方**，两个独立整行按钮，文字默认可见
- **企业微信联系方式占位**：预留展示区（如「商务合作：企业微信 [预留]」，运营方后续填写）
- 风险提示文案（投资有风险，本平台仅供参考）
- **页面自动化适配浏览器**：375px～桌面端无横向溢出
- 详见 docs/core/11_UI_设计规范.md §7.1

## 7. 注册页 register.html
- 注册卡片居中：邮箱/手机号、密码、确认密码、同意协议复选框、[立即注册]
- 底部：已有账号？[去登录]
- 详见 11 文档 §7.2

## 8. 忘记密码页 forgot_password.html
- 简化导航（仅 Logo）
- 卡片居中：标题「找回密码」、邮箱或手机号输入框、[发送重置链接] 按钮（Demo 可占位）
- 底部：想起密码？[返回登录]
- 风险提示小字

## 9. 用户后台 / 个人中心 profile.html（见 11 §7.4）
- 侧栏或 Tab：账户信息 | 会员与订阅 | 我的反馈
- **账户信息**：昵称、邮箱、注册时间
- **会员与订阅**：当前套餐（免费/月会员/年会员）、到期时间、[去充值/续费] 按钮
- **我的反馈**：近期研报反馈记录（可选 Mock 2～3 条）
- 导航：首页 | 研报列表 | 个人中心 | 退出登录

## 10. 管理员后台 admin.html（见 11 §7.5）
- 侧栏：概览 | 股票池 | 任务状态 | 用户统计（Mock）
- **概览**：今日研报数、待处理任务、系统健康简况
- **股票池**：Tier-1/Tier-2 列表占位（可 Mock 若干股票代码）
- **任务状态**：近期待跑/已完成任务占位
- **用户统计**：注册用户数、付费用户数、日活占位
- 导航区分：管理员入口（可从首页底部或单独链接进入，Demo 中可固定展示）

## 11. 充值 / 订阅页 subscribe.html
- 套餐三栏对比：免费 ¥0 | 月会员 ¥39/月 ⭐推荐 | 年会员 ¥299/年
- 每栏含权益说明、[当前方案]/[立即订阅]/[年度优惠] 按钮
- 功能对比详表、常见问题 FAQ（可折叠）
- 退款政策（7 天无理由）
- 详见 11 文档 §7.3

【导航与入口约定】
- **导航**：首页 | 研报列表 | 统计看板 | 模拟收益 | 登录 | 注册，整体居中；不设「关于与免责」入口，免责内容放页脚
- 未登录：导航显示 [登录] [注册]；点击付费内容可引导至登录页；登录页 [忘记密码] 跳转 forgot_password.html
- 已登录：导航显示 [用户头像/昵称] 下拉→个人中心、退出；个人中心内可进入充值页
- 管理员：额外入口 [管理后台]（可从首页底部小字链接或 profile 进入）

【Mock 数据要求】
- 至少 3 只股票的示例数据（如贵州茅台 600519、宁德时代 300750、比亚迪 002594）
- 研报结论有 BUY/SELL/HOLD 三种
- 信号类型有 A/B/C 三类
- 持仓状态有：持有中、已止盈、已止损、已过期
- 实操指令卡中付费内容用 ¥**.** 或模糊块展示
- 用户后台、管理员后台、充值页：用 Mock 用户数据（如 免费用户/月会员、到期时间 2026-03-24）

【布局与美观约定】
- **风险提示与免责**：全站页脚，内容居中；用折叠区或小字号灰色，默认折叠，点击「展开完整声明」再显示
- **导航**：6 项（首页、研报列表、统计看板、模拟收益、登录、注册）整体居中

【可选补充】
- 404/500 错误页（见 11 文档 §7.6）：用于演示错误态与返回首页入口

【输出】
- 生成可直接在浏览器打开的 HTML 文件（可多页，用相对路径跳转）
- 在页面顶部或 README 中说明：这是 Demo，数据为 Mock，仅供体验验证
```

---

## 二、精简版提示词（仅要点）

若上下文有限，可用此精简版：

```
按 docs/core/07_系统目标与范围整合.md §4 和 docs/core/11_UI_设计规范.md，生成 A 股研报平台的静态前台 Demo（纯 HTML+CSS，Mock 数据）。

包含 11 个页面：① 首页 ② 研报列表 ③ 研报详情 ④ 统计看板 ⑤ 模拟收益看板 ⑥ 登录 ⑦ 注册 ⑧ 忘记密码 ⑨ 用户后台 ⑩ 管理员后台 ⑪ 充值/订阅页。

每个模块需展示该模块应有的数据字段（用 Mock），设计规范用 11 文档的 Design Tokens；导航 6 项居中（首页、研报列表、统计看板、模拟收益、登录、注册），无「关于与免责」链接；页脚免责内容居中、折叠区；登录页 [忘记密码] 跳 forgot_password.html；页面 375px～桌面端自适应。

demo.css 排版强制要求：
① 必须定义 --space-5: 20px 和 --space-10: 40px（否则部分组件间距塌陷）
② body line-height 设 1.75（中文可读性）
③ letter-spacing 负值只用于纯数字区域，含中文的元素一律 letter-spacing: 0
④ 中文字重上限 600（700 在系统黑体下会模糊）
⑤ h1/h2/h3 在全局统一定义，不要在每个 HTML 里 inline style 打补丁
```

---

## 三、分步提示词（逐页生成）

若希望逐页生成、逐步调整，可按以下顺序执行：

### Step 0：先生成 demo.css（其他所有页面依赖此文件）
```
生成 demo.css，作为所有页面的共用样式表。必须包含：

1. Design Tokens（CSS 变量）：
   - 完整间距变量 --space-1 到 --space-12（不能缺 --space-5: 20px 和 --space-10: 40px）
   - 完整字号变量 --fs-2xs(12px) 到 --fs-3xl(28px)
   - 颜色变量：背景/文字/品牌色/语义色（buy/sell/hold/up/down）/信号类型(A/B/C)/市场状态(bull/neutral/bear)
   - 圆角/阴影/过渡变量

2. 字体规范（中英混排关键）：
   --font-display: 'Inter', 'Outfit', system-ui, sans-serif;   /* 数字/英文标题 */
   --font-body: 'Inter', -apple-system, BlinkMacSystemFont, 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei UI', 'Microsoft YaHei', sans-serif;

3. 全局重置与标题层级：
   body { font-family: var(--font-body); font-size: 15px; line-height: 1.75; }
   h1 { font-size: 22px; font-weight: 700; line-height: 1.25; letter-spacing: -0.01em; }
   h2 { font-size: 18px; font-weight: 600; line-height: 1.3; letter-spacing: 0; }
   h3 { font-size: 16px; font-weight: 600; line-height: 1.4; letter-spacing: 0; }
   （中文字重上限 600，不能对含中文的标签设负 letter-spacing）

4. 所有组件类：导航/市场横幅/Hero/徽章/卡片/信号卡/研报行/实操指令卡/表格/折叠区/Tab/指标卡/策略卡/净值曲线占位/页脚/登录表单/个人中心布局/管理员布局/订阅套餐卡/空状态/Demo横幅

5. 响应式：640px 移动端 + 768px 平板两个断点
```

### Step 1：首页
```
按 docs/core/11_UI_设计规范.md §4.1，生成首页 index.html。

导航：首页|研报列表|统计看板|模拟收益|登录|注册，居中排列，不设「关于与免责」链接。
页脚风险提示：内容居中，折叠区或小字灰色，默认一句+[展开完整声明]。

含市场状态横幅、Hero、今日强信号(6条 Mock)、平台统计。用 demo.css 样式类，不要 inline style 覆盖全局标题字体/字号/行高。
```

### Step 2：研报列表
```
按 11 文档 §4.2，生成研报列表页 reports_list.html。含：7 种筛选栏、列表卡片（含信号类型徽章、结论徽章、持仓状态），每条可点击跳转详情。Mock 至少 5 条研报。不要 inline style 覆盖全局排版变量。
```

### Step 3：研报详情
```
按 11 文档 §4.3，生成研报详情页 report_view.html。三层：Hero+实操指令卡+持仓追踪徽章 → 未来趋势表+标的概况+结论依据+资金面 → 历史信号追踪+AI推理+数据清单。Mock 贵州茅台 600519 的完整研报数据。
```

### Step 4：统计看板
```
按 11 文档 §4.4，生成统计看板 dashboard.html。含四维度绩效、方向命中率、Tab 切换，Mock 核心数字。
```

### Step 5：模拟收益看板
```
按 11 文档 §4.5，生成模拟收益看板 sim_dashboard.html。含指标卡、净值曲线占位、分策略卡片、交易明细表、冷启动提示。Mock 数据。
```

### Step 6：登录、注册、忘记密码
```
按 11 文档 §7.1、§7.2，生成 login.html、register.html、forgot_password.html。

登录页：① 账号密码（邮箱或手机号+密码）；② [忘记密码] 链接至 forgot_password.html；③ 还没有账号？[立即注册]；④ 第三方登录（QQ、微信）在 [立即注册] 下方；⑤ 企业微信占位；⑥ 375px～桌面端自适应。

注册：邮箱/密码/确认密码、同意协议、去登录。卡片居中最大宽度 400px。

忘记密码页：标题「找回密码」、邮箱或手机号输入、[发送重置链接] 占位、[返回登录]。
```

### Step 7：用户后台
```
生成 profile.html 用户个人中心。含 Tab：账户信息、会员与订阅、我的反馈。会员区显示当前套餐与到期时间、[去充值] 入口。导航含个人中心、退出登录。
```

### Step 8：管理员后台
```
生成 admin.html 管理员后台。侧栏：概览、股票池、任务状态、用户统计。各模块用 Mock 占位数据。导航含管理后台入口。
```

### Step 9：充值/订阅页
```
按 11 文档 §7.3，生成 subscribe.html。三栏套餐（免费/月会员/年会员）、权益对比、[立即订阅] 按钮、FAQ 可折叠。
```

---

## 四、现有 Demo 排版修复提示词

若已有现成的 demo.css 和 HTML 文件，需要**只修复字体和排版问题**而不重新生成，使用此提示词：

```
对 demo/ 目录下现有的 demo.css 进行排版修复，不改动颜色/配色，只修复以下 6 类问题：

1. **补全缺失间距变量**：在 :root 中添加 --space-5: 20px 和 --space-10: 40px（当前缺失，导致 .market-banner padding 和 .site-footer margin-top 塌陷）

2. **正文行高**：将 body line-height 从 1.6 改为 1.75（中文正文可读性要求）

3. **中文 letter-spacing 修复**：
   - .nav-brand 的 letter-spacing: -0.02em → 改为 letter-spacing: -0.01em（品牌名有英文可保留微小负值）
   - .card-title 的 letter-spacing: -0.01em → 改为 letter-spacing: 0（卡片标题多含中文）
   - 所有含中文的卡片/段落/标题 letter-spacing 设为 0 或 normal；只有 .metric-card .val（纯数字）保留 -0.02em

4. **全局标题层级统一**：在 body 之后添加以下规则（避免各页 inline style 打补丁）：
   h1 { font-family: var(--font-display); font-size: var(--fs-2xl, 22px); font-weight: 700; line-height: 1.25; letter-spacing: -0.01em; margin: 0 0 var(--space-4); }
   h2 { font-family: var(--font-display); font-size: var(--fs-xl, 18px); font-weight: 600; line-height: 1.3; letter-spacing: 0; margin: 0 0 var(--space-3); }
   h3 { font-family: var(--font-display); font-size: var(--fs-lg, 16px); font-weight: 600; line-height: 1.4; letter-spacing: 0; margin: 0 0 var(--space-3); }
   （注意：中文标题 font-weight 上限 600，不用 700）

5. **表格行高**：.data-table td, .data-table th 添加 line-height: 1.5

6. **卡片/列表行高**：.signal-card, .report-row 添加 line-height: 1.7（中文内容适配）

同时检查各 HTML 文件中是否有 inline style 直接覆盖了字体、行高或 letter-spacing，若有则删除，改用 CSS 类统一管理。
```

---

## 五、验证清单（生成后自检）

生成 Demo 后，可对照此清单确认是否满足需求：

| 检查项 | 说明 |
|--------|------|
| 11 个页面齐全 | 首页、列表、详情、看板、模拟收益、登录、注册、忘记密码、用户后台、管理员后台、充值页 |
| 导航可跳转 | 各页导航链接有效；未登录/已登录/管理员入口区分 |
| 模块与文档一致 | 每个模块的数据字段与 07/11 文档描述一致 |
| 徽章与配色 | 信号类型(A/B/C)、结论(买/卖/观望)、持仓状态、市场状态 使用规范配色 |
| 实操指令卡 | 含入场价、止损、目标价、仓位建议，免费用户看到模糊化 |
| 高级区 | 研报详情有「AI推理全过程」「所用数据清单」等可折叠区 |
| 空状态 | 至少有一个空状态示例（如今日强信号为空时的提示）|
| 账号与商业化 | 登录、注册、用户后台、管理后台、充值页 可跳转且展示 Mock 数据 |
| 登录布局 | 账号密码 → 立即注册 → QQ/微信登录（在下方）→ 企业微信占位 |
| 页面自适应 | 375px～桌面端无横向溢出，实操指令卡/登录按钮小屏竖排 |
| 导航 | 6 项（首页、研报列表、统计看板、模拟收益、登录、注册）居中，无「关于与免责」链接 |
| 页脚免责 | 内容居中，折叠区或小字灰色，可「展开完整声明」 |
| **排版一致性** | 无 inline style 覆盖全局字体/行高；--space-5 和 --space-10 已定义；中文元素无负 letter-spacing |
| **字体混排** | 数字/英文标题用 display 字体，正文用 body 字体；中文 font-weight ≤ 600 |
| **行高** | body line-height ≥ 1.75；表格 ≥ 1.5；数字指标卡可用 1.1 |

---

## 六、与正式开发的关系

- **Demo 阶段**：静态/Mock，用于验证信息架构、布局和字段是否合理
- **正式开发**：用 FastAPI + Jinja2 模板，接真实 API，替换 Mock 为动态数据
- **建议**：Demo 验证通过后，可将 Demo 的 HTML 结构作为 `app/web/templates/` 的参考实现，减少返工
