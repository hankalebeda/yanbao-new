# Demo 全站美化与优化提示词

> **用途**：对 demo 目录下全部页面的排版、文案、布局、登录流程进行综合优化。  
> **前置**：已存在 demo/*.html、demo.css，并已应用 08 号提示词的布局优化。  
> **Skills**：frontend-design、responsive-design、visual-design-foundations、interaction-design。

---

## 一、完整提示词（可直接复制使用）

```
【任务】对 A 股研报平台 Demo 做全站美化与优化，必须调用 frontend-design、responsive-design、visual-design-foundations、interaction-design 四个 skill。

【待优化文件】
- demo/index.html
- demo/dashboard.html
- demo/reports_list.html
- demo/sim_dashboard.html
- demo/login.html
- demo/register.html
- demo/forgot_password.html
- demo/profile.html
- demo/admin.html
- demo/report_view.html
- demo/demo.css

【优化要点（必须全部落实）】

一、导航栏
1. 业务页（index、reports_list、dashboard、sim_dashboard、profile、admin 等）：完整导航栏
2. 登录/注册/忘记密码：简化导航栏，仅保留「品牌 + ← 返回首页」，无业务入口；注册页须与登录页一致

二、登录页面
1. 表单顺序：账号密码表单在上，第三方登录（QQ、微信）在下
2. 文案：主表单提交后，用分隔线「—— 或 使用第三方账号 ——」，下方并列 QQ 登录、微信登录
3. 删除「Demo 页面，数据为 Mock」等占位说明

三、今日市场 / Hero 排版
1. 市场横幅：标签与数值分行清晰，专业名词旁附小字解释（MA20、量能比等），标签与数值之间留适当间距，避免挤在一起
2. Hero 区：四维度绩效改为分项展示，格式如「胜率 ≥55% · 盈亏比 ≥1.5 · 年化 Alpha（超额收益）≥10%」，年化 Alpha 旁附 term-tip 解释
3. 四维度绩效整体视觉统一，数字与单位不割裂

四、四维度绩效与年化 Alpha
1. 统计看板 dashboard：年化 Alpha 卡片与 A/B/C 策略卡视觉统一，使用 strategy-card 风格，含「年化 Alpha（超额收益）」标签、数值、副文案（如「相对沪深300」）
2. 禁止单独用简陋 metric-card 展示 24.3% 年化 Alpha，需与 A/B/C 三卡协调

五、A/B/C 类信号解释
1. 在今日强信号卡片上方或首页显眼位置，增加一行说明：
   - A类 = 事件驱动（公告/政策/业绩催化）
   - B类 = 趋势跟踪（突破/量价配合/MA 系统）
   - C类 = 低波套利（估值修复/超跌反弹）
2. 统计看板四维度绩效区：每张策略卡标签改为「A类 事件驱动」「B类 趋势跟踪」「C类 低波套利」

六、删除全部 Demo 相关字段
在以下页面中删除或替换所有「Demo」「demo」相关文案与占位：
- login.html、register.html、forgot_password.html：删除「Demo 页面，数据为 Mock」「Demo 占位，无实际发送功能」
- index.html、reports_list.html、sim_dashboard.html：删除「Demo：切换空状态示例」「Demo：切换回撤横幅」「Demo：切换冷启动提示」等调试链接
- report_view.html：删除「— Demo 占位」
- admin.html、profile.html：将「Demo 用户」「demo@example.com」改为「示例用户」「user@example.com」等通用占位

七、布局与间距（沿用 08 号提示词）
- 4 个方框：4 列或 2×2，禁止 3+1
- 3 个方框：3 列均分，禁止 2+1
- 375px 无横向溢出
- 沿用 Design Tokens，不新增未约定颜色

【Design Tokens】
- 遵循 docs/core/11_UI_设计规范.md
- 信号类型色：A 紫、B 蓝、C 青
- 中文 line-height ≥ 1.75

【Skills 调用】
- frontend-design：整体美观、层次、留白
- responsive-design：断点、网格、移动端
- visual-design-foundations：排版、间距、视觉层级
- interaction-design：登录流程、分隔线、按钮状态

【输出】
1. 修改后的完整 HTML 片段（login、register、index、dashboard 等关键页）
2. 修改后的 demo.css 新增/修改片段
3. 简短说明：做了哪些改动、解决了哪些问题

【验收】
- 登录：账号密码在上，第三方登录在下
- 注册/登录：简化导航一致
- 四维度绩效：年化 Alpha 与 A/B/C 风格统一
- A/B/C 解释：首页与看板均有说明
- 无任何「Demo」占位文案残留
```

---

## 二、涉及页面与改动速查

| 页面 | 主要改动 |
|------|----------|
| login.html | 表单顺序：账号密码→第三方；删 Demo 文案 |
| register.html | 简化导航；删 Demo 文案 |
| forgot_password.html | 删 Demo 占位说明 |
| index.html | Hero 四维度绩效排版；A/B/C 解释；删调试链接 |
| dashboard.html | 年化 Alpha 卡片样式；A/B/C 全称；删 Demo |
| sim_dashboard.html | 删调试链接 |
| reports_list.html | 删调试链接 |
| report_view.html | 删 Demo 占位 |
| admin.html | Demo 用户→示例用户 |
| profile.html | Demo 用户→示例用户 |
| demo.css | 登录 OAuth 行样式；hero-cred；signal-type-hint；strategy-card--alpha |

---

## 三、A/B/C 信号类型定义（供文案参考）

| 类型 | 全称 | 触发因子 | 有效期 |
|------|------|----------|--------|
| A类 | 事件驱动 | 公告/政策/业绩超预期/并购/概念催化 | T+2 |
| B类 | 趋势跟踪 | 突破关键阻力/量价配合/MA 系统向上 | T+3 |
| C类 | 低波套利 | 估值修复/超跌反弹/波动率极低均值回归 | T+5 |

---

## 四、用法说明

1. 复制「一、完整提示词」整段
2. 在 Cursor 中用 `@` 引用：demo/*.html、demo/demo.css、docs/core/11_UI_设计规范.md
3. 补充：`请结合 frontend-design、responsive-design、visual-design-foundations、interaction-design 四个 skill 执行`
4. 发送后由 AI 完成优化并输出修改说明
