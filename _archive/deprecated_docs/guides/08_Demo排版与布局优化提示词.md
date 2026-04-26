# Demo 排版与布局优化提示词

> **用途**：针对 demo 目录下现有页面的排版、布局、留白、导航栏进行专项优化。  
> **前提**：已存在 demo/index.html、dashboard.html、reports_list.html、login.html、register.html、sim_dashboard.html 及 demo.css。  
> **目标**：解决左半边信息堆积、右半边空白过多、卡片网格布局不合理等问题，并区分业务页与登录/注册页的导航栏。

---

## 一、完整提示词（可直接复制使用）

```
【任务】对 A 股研报平台 Demo 的排版与布局做专项优化，使页面更美观、留白均衡、信息分布合理。必须调用 frontend-design、responsive-design、visual-design-foundations 三个 skill。

【待优化文件】
- demo/index.html
- demo/dashboard.html
- demo/reports_list.html
- demo/sim_dashboard.html
- demo/login.html
- demo/register.html
- demo/demo.css

【优化要点（必须全部落实）】

一、导航栏区分
1. **业务页**（index、reports_list、dashboard、sim_dashboard）：使用完整导航栏
   - 品牌 + 首页 | 研报列表 | 统计看板 | 模拟收益 | 登录 | 注册
   - 保留现有 site-nav 结构和样式
2. **登录/注册页**（login.html、register.html）：使用**简化导航栏**
   - 仅保留：品牌 Logo/名称 + 返回首页链接（或「← 返回」）
   - 不展示：研报列表、看板、模拟收益等业务入口
   - 可新增 class：.nav-auth 或 .site-nav--auth，样式更简洁（如无边框、透明背景、居中品牌）
   - 目的：登录/注册页专注身份操作，减少干扰

二、整体布局与留白
1. **左右失衡**：当前内容集中在左侧，右侧大片空白
   - 将 .container 的 max-width 合理利用，或增加主内容区与侧边的视觉平衡
   - 若使用单列布局，可适度拉宽 max-width 或增加卡片内边距，使内容更居中、两侧留白对称
   - 列表、筛选栏、表格等可考虑 max-width 略大（如 960px～1200px），避免过窄
2. **间距调整**：适当缩小过大留白，增大过密区域
   - 使用 Design Tokens（--space-4～--space-8）统一调整 section 间距、卡片内边距
   - 页面标题与首块内容、卡片与卡片之间的间距应协调，避免某处特别空或特别挤

三、卡片网格布局（重点）
1. **4 个方框**（如 platform-stats、metrics-grid 方向命中率 4 卡、sim_dashboard 4 指标卡）
   - 必须：4 个方框一行排满，或 2×2 网格
   - 禁止：出现 3 个一行、第 4 个单独换行，或 3+1 导致右侧大片空白
   - 实现建议：`grid-template-columns: repeat(4, 1fr)` 或 `repeat(2, 1fr)`；小屏可 2×2 或 2 列
2. **3 个方框**（如 strategy-grid 的 A/B/C 三策略、platform-stats 若只有 3 项）
   - 必须：3 个方框均分整行宽度，无右侧留白
   - 禁止：2 个占一行、第 3 个单独一行，或使用 auto-fill 导致最后一行留大片空白
   - 实现建议：`grid-template-columns: repeat(3, 1fr)`；小屏可 1 列或 2+1

四、具体网格类调整
- `.platform-stats`：3 个时用 `repeat(3, 1fr)`；4 个时用 `repeat(4, 1fr)` 或 `repeat(2, 1fr)`
- `.metrics-grid`：4 个指标卡时用 `repeat(4, 1fr)`（桌面端），小屏 2×2
- `.strategy-grid`：3 个策略卡时用 `repeat(3, 1fr)`；若混入 1 个 metric-card（如年化 Alpha），可单独一行或与策略卡视觉统一成 4 列
- 移除或替换 `repeat(auto-fill, minmax(...))` 等易产生「3+1」「2+1 留白」的写法
- 卡片 `max-width` 不要限制过死，让 grid 自动均分

五、其他布局
- 筛选栏 .filter-bar：可适当压缩垂直高度，或两行展示，避免单行过长
- 研报列表 .report-row：保持左右分区清晰，右侧操作区固定宽度
- 今日强信号 .signal-grid：保持响应式 1/2/3 列，间距统一
- 市场状态横幅：保持 2×2 或 4 列，紧凑不浪费空间

【Design Tokens 与规范】
- 继续使用 demo.css 中已有的 :root 变量（--space-*、--fs-*、--radius、--shadow 等）
- 不新增未在 11 文档中约定的颜色；保持语义色一致（buy/sell/hold、type-a/b/c）
- 375px 下无横向溢出，小屏可单列或折叠

【Skills 调用】
- frontend-design：提升整体美观度、层次感、留白节奏
- responsive-design：断点策略、网格响应、移动端适配
- visual-design-foundations：排版、间距系统、视觉层级

【输出】
1. 修改后的完整 demo.css（或关键片段 + 说明）
2. 修改后的 login.html、register.html 导航栏部分
3. 若 index/dashboard/reports_list/sim_dashboard 的 HTML 结构需调整，给出具体修改
4. 简短说明：做了哪些布局改动、解决了哪些问题

【验收】
- 4 个方框：桌面端一行 4 个或 2×2，无 3+1 留白
- 3 个方框：一行 3 个均分，无 2+1 留白
- 登录/注册：使用简化导航，无业务入口
- 整体观感：左右更均衡，空白不扎眼
```

---

## 二、用法说明

1. 复制上方「一、完整提示词」整段（从 【任务】 到 【验收】）
2. 在 Cursor 中粘贴，并用 `@` 引用：
   - `@demo/index.html`
   - `@demo/dashboard.html`
   - `@demo/reports_list.html`
   - `@demo/sim_dashboard.html`
   - `@demo/login.html`
   - `@demo/register.html`
   - `@demo/demo.css`
   - `@docs/core/11_UI_设计规范.md`
3. 补充：`请结合 frontend-design、responsive-design、visual-design-foundations 三个 skill 执行`
4. 发送后，AI 将对上述文件做排版与布局优化，并输出修改内容

---

## 三、涉及页面与组件速查

| 页面 | 需优化的组件/布局 |
|------|-------------------|
| index.html | 市场横幅、Hero、今日强信号、platform-stats（3 个） |
| dashboard.html | metrics-grid（4 个）、strategy-grid（3+1 或 4） |
| sim_dashboard.html | metrics-grid（4 个）、strategy-grid（3 个） |
| reports_list.html | filter-bar、report-row |
| login.html | 导航栏改为简化版 |
| register.html | 导航栏改为简化版 |

---

## 四、CSS 网格调整参考

```css
/* 4 个方框：桌面端 4 列，小屏 2 列 */
.metrics-grid--4 { grid-template-columns: repeat(2, 1fr); }
@media (min-width: 640px) { .metrics-grid--4 { grid-template-columns: repeat(4, 1fr); } }

/* 3 个方框：桌面端 3 列均分 */
.strategy-grid--3 { grid-template-columns: repeat(3, 1fr); }
@media (max-width: 640px) { .strategy-grid--3 { grid-template-columns: 1fr; } }

/* 登录/注册简化导航 */
.site-nav--auth .nav-links { display: none; }
.site-nav--auth .nav-back { display: inline-flex; }
```

上述仅作参考，实际实现以 AI 优化结果为准。
