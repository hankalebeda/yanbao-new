# 27_PRD_研报平台增强与整体验收基线.md

> **文档编号**：27  
> **文档名称**：研报平台增强与整体验收基线（Ralph 执行版 PRD）  
> **项目名称**：A 股个股研报平台（`yanbao-new`）  
> **版本**：v2.2（面向 Ralph 的两步闭环执行版）  
> **产出日期**：2026-04-26  
> **适用对象**：产品、研发、测试、运维、自治代理（Ralph / Codex / Gemini CLI）  
> **编写依据**：`AGENTS.md`、`docs/core/01_需求基线.md`、`02_系统架构.md`、`04_数据治理与血缘.md`、`05_API与数据契约.md`、`06_全量数据需求说明.md`、`22_全量功能进度总表_v12.md`、`25_系统问题分析角度清单.md`、`26_自动化执行记忆.md`，以及 `app/`、`tests/` 当前实现  
> **文档目标**：把“业务目标、关键对象、页面行为、失败与降级边界、数据血缘、任务拆解、测试锚点”收敛为一份 AI 可执行 PRD，并把 `prd.json` 拆到单轮可完成的原子故事；Step 2 只能把 `.claude/ralph/loop/prd.json` 作为直接任务入口，本文与 SSOT 文档只作为 Step 1 生成 JSON 的来源。  
> **真实性声明**：本文描述的是**目标系统**与**必须满足的真实约束**，同时显式保留当前运行态阻塞与外部依赖缺口；禁止把未接入或未恢复的能力写成“已完成”。

---

## 0. 阅读与执行说明

1. `01/02/04/05/06` 仍是正式 SSOT；本文不替代它们，而是把实现者真正需要的跨文档信息整合成执行视图。  
2. 本文读者包括 junior 开发者与 AI 代理，因此必须写明：**触发、输入、输出、状态、降级、异常、页面、接口、关键对象、验证方式**。  
3. 对 Ralph 而言，本文只定义**系统目标与切片原则**；真正的原子执行任务以 `.claude/ralph/loop/prd.json` 为准，Step 2 不再依赖口头补充或临时说明。  
4. 正式文档必须放在 `docs/core/`；禁止把正式需求、验收与中间产物放入桌面或无编号目录。  
5. 任何实现都必须遵守项目红线：**禁止伪造数据、禁止偷换分母、禁止以 HTTP 200 伪装功能可用、禁止把 soft delete 当成修复完成**。

---

## 1. Introduction / Overview

本项目要构建的是一个**面向 A 股个股研究场景的“真值优先”研报平台**。系统每日面对全量 A 股执行基础数据处理，但只对**200 只核心池股票**生成正式研报。每份研报不仅要给出结论，还要给出：

- 研判依据；
- 所用数据；
- 中文可读的分析过程；
- 风险提示；
- 三挡资金（`10k|100k|500k`）执行建议；
- 历史验证结果；
- 模拟持仓与结算闭环。

系统不是资讯堆砌站，也不是自动下单机器人。它的核心价值是：

1. **真实性**：所有证据都能追溯到真实来源与真实时间；  
2. **可解释性**：结论、证据、推理过程、高级区展示要能被用户与审计方理解；  
3. **可验证性**：FR-07/FR-08 必须把“说得对不对”变成可回算、可统计、可对比的事实；  
4. **可运营性**：管理员可以看到任务状态、复审状态、审计日志、补单与重建链路。  

本 PRD 的重点不是“描述愿景”，而是把系统拆成**可以逐步实现的真实模块**，让代理根据 `prd.json` 可连续生成本系统。

---

## 2. Goals

### 2.1 产品目标

- 对全量 A 股维护基础主数据、日线、热点/事件、市场状态与血缘记录。  
- 每个交易日严格选出 **200 只核心池股票**，并维护候补池。  
- 仅对核心池股票生成正式研报，且单股单日只有一份正式结果。  
- 每份研报必须同时输出：结论、置信度、策略类型、指令卡、三挡资金执行建议、证据列表、高级区全过程。  
- 对历史信号做 FR-07 结算，统计真实胜率、盈亏比、年化 Alpha、最大回撤，并强制显示**样本数与覆盖率**。  
- 对 BUY 信号做 FR-08 模拟持仓，形成三挡资金的账户快照与回撤治理。  
- 完成会员体系、注册/登录/Token 刷新、OAuth（QQ/微信）、支付订单与 Webhook 回调闭环。  
- 提供可用站点：首页、列表、详情、高级区、Dashboard、模拟看板、登录/注册/Profile/订阅、管理员页面。  
- 所有关键写操作都必须可追踪、可审计、可重放、可降级。  

### 2.2 交付目标

- 让 Ralph 仅依赖 runtime 版 `prd.json` 就能按依赖顺序完成系统实现，而不是依赖口头说明。  
- 让每个执行切片都足够小：一个切片只做一类模型、一个 API、一个页面区块、一个任务治理点或一个统计闭环。  
- 让每个切片都可验证：能通过 typecheck、targeted tests、必要时通过浏览器验证。  

### 2.3 业务目标

- 真实胜率目标：**≥55%**。  
- 盈亏比目标：**≥1.5**。  
- 年化 Alpha 目标：**≥10%**。  
- 最大回撤目标：**≤20%**。  
- 方向命中率可展示，但**不能代替**上述商业底线指标。  

---

## 3. Non-Goals / Out of Scope

以下能力不属于本轮必须交付范围，或者被明确禁止：

- 不做真实券商下单；模拟交易只用于内部回测与策略验证。  
- 不做分钟级/高频研报；本系统以**日级**为准。  
- 不做社交社区、聊天、论坛。  
- 不做“每日营销式股票推荐推送”；但允许 FR-13 中的事务性业务事件通知。  
- 不做 PDF / Excel / Word 导出。  
- 不做通过修改分母、隐藏 soft delete、伪造外部数据来换取“高可用率”。  
- 不做对 LLM 暴露用户邮箱、JWT、cookie 等敏感身份数据。  
- 不做“只留 501/占位接口”的 OAuth 与支付；首版即要求完整闭环或真实 Mock 闭环。  

---

## 4. User Roles and Permission Matrix

| 角色 | 核心动作 | 默认可见 | 受限项 |
| --- | --- | --- | --- |
| 访客 / Free | 浏览公开研报、查看基础结论、查看 100k 模拟看板 | 首页、列表、公开详情、基础 Dashboard | 高级区全文、三挡模拟细节、完整历史范围 |
| Pro | 查看完整研报、高级区、90 天历史、三挡模拟 | Free 全部 + 完整高级区 + 全三挡 sim | 无管理员权限 |
| Enterprise | 查看全部历史、完整高级区、全三挡模拟 | Pro 全部 + 不限历史 | 无管理员权限 |
| Admin | 管理用户、查看概览、审核研报、补单、触发运营动作 | 管理端 API / 页面 | 不能强制重建已被 sim 引用的研报 |
| Super Admin | 强制重建脏研报、处理高风险治理动作 | Admin 全部 | 必须保留完整审计链 |
| 系统调度器 / 内部调用方 | 触发 pool / ingest / report / settle / sim / cleanup / notify | internal API 与 handler | 仅允许内部鉴权，不暴露给前台 |

### 4.1 权益冻结规则

- `capital_tier` 枚举固定为 `10k|100k|500k`，展示层映射为 `1W|10W|50W`。  
- Free 用户的高级区必须由**后端**裁剪，不允许让前端自行“隐藏字段”。  
- Free 用户 `sim-dashboard` 仅允许 `100k`；Pro/Enterprise 可查看全部三挡；admin/super_admin 可豁免。  
- 已发布研报对普通用户只暴露中文业务语义，不暴露调试态原始字段。  

---

## 5. Core Domain Model / Core Entities

| 对象 | 作用 | 最小关键字段 | 说明 |
| --- | --- | --- | --- |
| `stock_master` | 全市场证券主数据 | `stock_code, stock_name, exchange, industry, is_st, is_delisted` | FR-01/04/06 基础主键域 |
| `kline_daily` | 日线行情主表 | `stock_code, trade_date, open, high, low, close, volume, amount, source_batch_id` | 技术分析、市场状态、结算、模拟共同依赖 |
| `market_hotspot_item*` | 多源热点与事件 | `topic_title, source_name, source_url, fetch_time, merged_rank` | 策略 A 判定与证据引用 |
| `data_batch*` | 批次与错误、血缘 | `batch_id, source_name, trade_date, status, started_at, completed_at` | 真实链路审计基础 |
| `report_data_usage` | 数据使用真值表 | `stock_code, trade_date, source_name, batch_id, status, status_reason` | 高级区“所用数据”来源 |
| `stock_pool_snapshot` | 每日核心池/候补池 | `pool_date, pool_version, stock_code, rank_no, is_core, score` | FR-01 输出实体 |
| `market_state_cache` | 市场状态缓存 | `trade_date, reference_date, market_state, state_reason, market_state_degraded` | FR-05 输出实体 |
| `report` | 研报主表 | `report_id, idempotency_key, recommendation, confidence, strategy_type, quality_flag, published, review_flag` | FR-06 核心对象 |
| `report_citation` | 证据引用 | `report_id, source_name, source_url, fetch_time, excerpt` | FR-00/FR-06 真实性约束 |
| `report_generation_task` | 研报任务态 | `task_id, stock_code, trade_date, status, error_message, resume_token` | 批量生成与恢复 |
| `settlement_result` | FR-07 结算事实 | `report_id, window_days, settlement_status, net_return_pct, is_misclassified` | 真实胜率与基线对照来源 |
| `sim_position` | FR-08 模拟仓位 | `position_id, report_id, capital_tier, position_status, sim_qty, sim_open_price, sim_close_price` | 三挡资金仿真核心对象 |
| `sim_account_snapshot` | 模拟账户快照 | `capital_tier, snapshot_date, nav, drawdown_pct, drawdown_state` | Dashboard / sim 看板来源 |
| `user` | 用户主表 | `user_id, email, password_hash, role, tier, membership_level, membership_expires_at` | FR-09 核心对象 |
| `refresh_token` | 刷新令牌 | `refresh_token_id, token_hash, grace_expires_at, revoked_at` | 刷新轮换与撤销 |
| `billing_order` | 支付订单 | `order_id, provider, status, amount, paid_at, event_id` | 计费与补单闭环 |
| `notification` | 业务通知与 Outbox | `notification_id, event_type, channel, dedupe_key, status, payload_json` | FR-13 主表 |
| `audit_log` | 审计链 | `actor_user_id, action_type, request_id, before, after, timestamp` | 所有高风险写操作必须落审计 |
| `cookie_session` | 采集登录态 | `provider, expires_at, status, last_refresh_at, status_reason` | FR-03 会话健康管理 |

### 5.1 冻结关键枚举

- `Recommendation = BUY | SELL | HOLD`  
- `QualityFlag = ok | stale_ok | missing | degraded`  
- `MarketState = BULL | BEAR | NEUTRAL`  
- `UserRole = user | admin | super_admin`  
- `UserTier = Free | Pro | Enterprise`  
- `PositionStatus = OPEN | TAKE_PROFIT | STOP_LOSS | TIMEOUT | CLOSED`  
- `TaskStatus = PENDING | PROCESSING | COMPLETED | FAILED | SUSPENDED | EXPIRED`  
- `NotificationStatus = sent | failed | skipped | pending`  

---

## 6. End-to-End Workflows

### 6.1 每日主链（交易日）

1. 调度器启动当日任务，先校验交易日与锁状态。  
2. FR-01 刷新股票池：从全量 A 股中过滤并排序，输出 200 核心池 + 候补池。  
3. FR-04 采集核心与必要长尾数据：证券主数据、日线、热点、资本/北向/ETF/公司简介等。  
4. FR-05 基于行情 + 热点生成 `market_state_cache`。  
5. FR-06 对核心池生成正式研报：先补采 non-report usage，再做输入门、LLM 生成、风险审计、公开门。  
6. FR-07 对历史信号进入结算窗口者做结算，并同步更新 KPI。  
7. FR-08 对 BUY 信号开仓，对满足条件的持仓平仓，生成账户快照与回撤状态。  
8. FR-13 对当日 BUY 强信号、平仓结果、回撤与复审事件做事务后推送。  
9. FR-10 页面与 Dashboard 从 read model 读取聚合结果，对外展示。  

### 6.2 用户浏览链

1. 用户访问首页，看到今日市场状态、精选研报、池子规模、今日研报数。  
2. 用户进入列表页，按日期、推荐、市场状态、池内、持仓状态等筛选研报。  
3. 用户进入详情页，查看结论、置信度、指令卡、证据摘要、风险提示、模拟关联。  
4. 付费用户进入高级区，查看“所用数据”与“生成全过程”；Free 只看到裁剪后的摘要。  

### 6.3 鉴权与支付链

1. 用户注册后获得未激活账户；激活成功后可登录。  
2. 登录成功时建立 access token / cookie；刷新链支持 token rotation。  
3. OAuth 支持 QQ / 微信标准授权码流程；缺真实参数时必须走全真 Mock，而不是 501。  
4. 用户创建订单、收到支付参数、支付 Webhook 回调、权益到账；管理员可补单。  

### 6.4 反馈与复审链

1. 用户对公开研报提交 positive / negative 反馈。  
2. 相同用户重复 negative 不重复计数；同日超频直接 429。  
3. 负反馈累计达到阈值后，`review_flag` 进入 `PENDING_REVIEW`。  
4. 管理员在后台审核、下架或保留；首次进入待复审时向管理员通道推送事件。  

### 6.5 强制重建链

1. 只有 super_admin 可以发起强制重建。  
2. 强制重建只能用于脏研报纠错，必须显式给出 `reason_code`。  
3. 若目标研报已被 FR-08 持仓或 FR-07 结算引用，则必须 409 阻断，避免血缘断裂。  
4. 重建成功时旧研报软删除，新研报获得新 `report_id`，全流程可审计。  

### 6.6 降级与失败链

- 上游数据不足：写 `quality_flag/status_reason`，必要时 `published=false`。  
- 外部数据源熔断：进入 `stale_ok` 或 `degraded`，并在前台显式展示横幅。  
- 调度链被卡死：使用 `upstream_timeout_next_open` 自解挂起并链式终止下游。  
- LLM provider 全失败：可记录失败态，但禁止以伪完成研报对外发布。  

---

## 7. Functional Requirements

### FR-00 真实性红线

- 每条公开研报必须具备真实 `citation` 三要素：`source_name / source_url / fetch_time`。  
- 研报最小核心来源门槛：至少有 `kline_daily` 与一类外部证据；任一核心来源不可用时不得静默成功。  
- `published=true` 后业务内容只读；修复脏数据只能走强制重建，不允许覆写原正文。  
- 前台必须用中文业务语义表达，不允许直接把英文内部键名暴露给用户。  
- 代码锚点：`app/services/report_generation_ssot.py`、`app/services/reports_query.py`、`app/api/routes_admin.py`。  

### FR-01 股票池筛选

- 覆盖全量 A 股基础 universe，过滤退市、ST、停牌过长、连续跌停、不满足市值/成交额/上市时长条件的股票。  
- 输出 `core_pool=200`、`standby_pool=Top201~250`、`pool_version`、`fallback_from`、`evicted_stocks`。  
- 刷新失败时回退到前一交易日池；若冷启动无历史池则直接 fail-close。  
- 同一交易日并发刷新必须互斥，避免双写与版本竞争。  
- 代码锚点：`app/services/stock_pool.py`、`app/api/routes_admin.py::admin_pool_refresh`、`app/api/routes_dashboard.py::pool_stocks`。  

### FR-02 定时调度

- 所有日批依赖必须按 DAG 完成事件推进，不能用“固定时间硬触发”替代上游完成。  
- 调度器需要记录任务状态、开始结束时间、错误信息、重试结果。  
- 多实例下必须有锁、TTL、续约与 fencing token；实例崩溃后可接管。  
- 跨日未完成任务不得无限挂起，必须在次日开盘前统一终止并写 `upstream_timeout_next_open`。  
- 代码锚点：`app/services/scheduler.py`、`dag_scheduler.py`、`scheduler_ops_ssot.py`。  

### FR-03 Cookie 与会话管理

- 需要 Cookie 的采集源必须维护 `cookie_session`，并且支持保存、探活、失败原因记录。  
- 健康探测要有并发互斥，避免多节点同时探测放大风控。  
- 日志只能输出脱敏后的状态，不得输出明文 cookie。  
- 代码锚点：`app/services/cookie_session_ssot.py`、`app/api/routes_admin.py`、`app/api/routes_internal.py::cookie_refresh`。  

### FR-04 多源数据采集

- 统一处理股票主数据、日线、热点、公司概况、估值、行业竞争、北向、ETF、资本面等数据。  
- 采集必须区分“正式研报所需数据”和“详情页 read-through 数据”；前者写 `report_data_usage`，后者可按需实时聚合。  
- 数据源需要支持限速、熔断、half-open 探测、Partial Commit、批次与错误落库。  
- 核心池关键数据失败时必须显式降级；长尾股票失败不能把整批次回滚成“无结果”。  
- 代码锚点：`app/services/multisource_ingest.py`、`hotspot.py`、`market_data.py`、`usage_lineage.py`、`runtime_materialization.py`。  

### FR-05 市场状态机

- 基于 `kline_daily + hotspot` 计算 `BULL / BEAR / NEUTRAL`。  
- 结果必须带 `reference_date` 与 `state_reason`，并支持 degraded 语义。  
- FR-06 生成研报时必须依赖当日市场状态；BEAR 下对 B/C 策略实施短路规则。  
- 代码锚点：`app/services/market_state.py`、`app/api/routes_sim.py::market_state`。  

### FR-06 研报生成

- 研报生成前必须补齐非研报数据 usage，并通过输入质量门。  
- 生成链路要输出：`recommendation`、`confidence`、`strategy_type`、`instruction_card`、`trade_instruction_by_tier`、`citations`、`reasoning_chain`、`plain_report`、`quality_gate`。  
- BUY 且高置信度报告必须经过风险审计分支；高风险结果不能无审计直接发布。  
- 输出质量门失败时必须 fail-close，不允许“有 report_id 但公开面为空”的伪成功。  
- 高级区必须中文展示“所用数据”和“生成全过程”，并能回溯 `report_data_usage`。  
- 代码锚点：`app/services/report_generation_ssot.py`、`llm_router.py`、`reports_query.py`、`routes_business.py`、`routes_internal.py`。  

### FR-07 结算与绩效统计

- 对历史信号做固定规则结算，写 `settlement_result` 与 `strategy_metric_snapshot`。  
- FR-07 口径固定为**标准 100 股**，不得与 FR-08 的资金挡位 shares 混用。  
- 所有 KPI 必须同时展示样本数与覆盖率。  
- `baseline_random` 与 `baseline_ma_cross` 必须独立运行，不得与主样本共用分母。  
- 代码锚点：`app/services/settlement_ssot.py`、`fr07_baseline_service.py`、`fr07_metrics.py`。  

### FR-08 模拟持仓

- 对 BUY 信号按三挡资金做实际可买股数仿真，处理佣金、印花税、滑点、回撤。  
- 开仓/平仓必须使用 pessimistic fill 规则；平仓原因至少包括止盈、止损、超时。  
- 三挡账户要分别维护 NAV、仓位、回撤与 `drawdown_state`。  
- 任何与持仓相关的外发通知都必须走 Outbox 事务后派发。  
- 代码锚点：`app/services/sim_position_service.py`、`sim_query.py`、`routes_sim.py`、`routes_dashboard.py`。  

### FR-09 会员、鉴权、OAuth、支付

- 支持注册、登录、登出、邮箱激活、刷新 token、忘记密码、重置密码。  
- 支持 QQ / 微信 OAuth，真实参数不足时也要通过 Mock 流程跑完整闭环。  
- 支持订单创建、支付回调、权益发放、管理员补单。  
- Token 必须支持 rotation、revocation、jti 黑名单；密码必须强哈希。  
- 代码锚点：`app/api/routes_auth.py`、`app/services/membership.py`、`oauth_service.py`、`routes_business.py` billing 端点、`routes_admin.py` reconcile。  

### FR-09-b 清理与归档

- 清理任务只处理临时或脏状态对象，例如未激活账户、过期 token、僵尸任务、不完整 report bundle。  
- 正式研报、历史行情、结算、模拟持仓、证据血缘属于永久保留域。  
- 清理任务不得阻断当日交易主链。  
- 代码锚点：`app/services/cleanup_service.py`。  

### FR-10 站点与看板

- 提供首页、公开列表、详情页、高级区、Dashboard、模拟看板、搜索与热点入口。  
- 页面必须区分 `READY / COMPUTING / DEGRADED`，并给出中文 `status_reason`。  
- Free/Pro/Enterprise 的显示差异必须在后端收敛，不能只靠前端隐藏。  
- 代码锚点：`app/services/home_query.py`、`dashboard_query.py`、`reports_query.py`、`app/web/templates/*`。  

### FR-11 用户反馈

- 反馈只能面向真实存在且公开可见的研报。  
- 同一用户重复 negative 不重复计数；超频反馈需要 429 防刷。  
- 首次触发 `PENDING_REVIEW` 时要进入 FR-13 管理员通知。  
- 代码锚点：`app/api/routes_business.py::report_feedback_by_path`、`app/services/feedback_ssot.py`。  

### FR-12 管理后台

- 管理端要支持：概览、用户分页与权限修改、研报复审、发布/下架、补单、Cookie 管理、强制重建。  
- 所有高风险操作都必须有审计记录，含 `actor/before/after/request_id/timestamp`。  
- Force Regenerate 只允许 super_admin 使用，并且必须阻断已有 sim/settlement 引用的研报。  
- 代码锚点：`app/api/routes_admin.py`、`app/services/admin_audit.py`、`report_admin.py`。  

### FR-13 业务事件推送

- 至少支持四类事件：`POSITION_CLOSED`、`BUY_SIGNAL_DAILY`、`DRAWDOWN_ALERT`、`REPORT_PENDING_REVIEW`。  
- 事件必须走 Outbox；事务回滚时不得外发。  
- 推送失败不阻塞主链；重复事件按幂等键写成 `skipped`。  
- 推送内容不得包含邮箱、JWT、cookie 等个人敏感信息。  
- 代码锚点：`app/services/notification.py`、`event_dispatcher.py`。  

---

## 8. User Stories

> 本节是**产品/执行层面的 Epic 级用户故事**；Ralph 的原子 story 见 `prd.json`。

### US-001：管理员刷新核心池
**Description:** 作为管理员，我希望系统每天自动或手动刷新核心池，以便后续采集和研报只围绕最值得跟踪的 200 只股票展开。  
**Acceptance Criteria:**
- [ ] 唯一写入口是 `POST /api/v1/admin/pool/refresh`；匿名请求返回 `401 UNAUTHORIZED`，普通用户返回 `403 FORBIDDEN`，`admin|super_admin` 可执行。  
- [ ] 成功结果必须返回 `pool_date`、`pool_version`、`core_pool_size=200`、`standby_pool_size=50`、`fallback_from`、`evicted_stocks`；不得只返回“刷新成功”文本。  
- [ ] 同一 `trade_date` 的并发刷新必须互斥；竞争失败的一方返回 `409 CONCURRENT_CONFLICT` 或 `429 RATE_LIMITED`，且不得产生第二个 `pool_version`。  
- [ ] 筛选失败时只能回退到上一有效交易日池并触发告警；若无历史池则必须 `500 COLD_START_ERROR`，不得输出伪核心池。  
- [ ] 验收映射固定到 `tests/test_fr01_pool_refresh.py` 与 `tests/test_fr12_admin.py`，且必须覆盖固定池规模、回退、冷启动和并发互斥四类断言。  

### US-002：调度器按依赖推进日批
**Description:** 作为系统维护者，我希望调度器按 DAG 推进 daily chain，以便 FR-06/07/08 不会因为时钟错乱而提前执行。  
**Acceptance Criteria:**
- [ ] `FR-06` 只能在 `FR-04` 完成事件到达后进入 `RUNNING`；`FR-07|FR-08` 只能在 `FR-06` 批次完成后进入 `RUNNING`，不得用固定时钟硬触发替代依赖。  
- [ ] 任务状态只允许 `PENDING|WAITING_UPSTREAM|RUNNING|SUCCESS|FAILED|SKIPPED`，且每条任务记录都必须带 `trade_date`、`started_at`、`finished_at`、`status_reason`、`error_message`。  
- [ ] 多实例必须以 `trade_date + task_name` 为互斥键，配套 TTL、续约和 fencing token；旧实例恢复后不得继续写入。  
- [ ] 超过次日开盘前死线仍未获上游完成信号时，当前节点及全量下游统一写 `status_reason=upstream_timeout_next_open` 并终止本交易日链路。  
- [ ] 验收映射固定到 `tests/test_fr02_dag_lock.py` 与 `tests/test_fr02_scheduler_ops.py`，覆盖 DAG 等待、锁接管、跨日兜底和重启幂等。  

### US-003：系统维护热点采集登录态
**Description:** 作为数据工程维护者，我希望系统能安全保存并探测 Cookie 会话，以便多源热点抓取保持可用。  
**Acceptance Criteria:**
- [ ] 管理入口固定为 `POST /api/v1/admin/cookie-session` 与 `GET /api/v1/admin/cookie-session/health`；`login_source` 只允许 `weibo|douyin|xueqiu|kuaishou`。  
- [ ] 健康结果只允许 `ok|fail|skipped` 三态，且 `fail` 时 `status_reason` 非空；并发探活抢锁失败时必须返回 `skipped`，不得真正发起第二次外部探测。  
- [ ] 日志、API 响应、页面渲染和审计记录都不得包含明文 `cookie_string`；只允许返回 provider、过期时间、状态和脱敏摘要。  
- [ ] 核心 Cookie 缺失只能让相关热点链路进入 `degraded`，不得让服务启动失败或让健康检查返回 500。  
- [ ] 验收映射固定到 `tests/test_fr03_cookie_routes.py`、`tests/test_fr03_cookie_session.py`、`tests/test_fr03_probe_startup_gate.py`。  

### US-004：系统统一采集多源数据并记录血缘
**Description:** 作为数据工程师，我希望所有研报相关数据都能统一采集、标准化并记录批次/血缘，以便后续研报与审计可追踪。  
**Acceptance Criteria:**
- [ ] 统一事实层最少覆盖 `stock_master`、`kline_daily`、`hotspot_top50`、`data_batch`、`data_batch_error`、`report_data_usage` 六类对象，且 `report_data_usage.status != ok` 时 `status_reason` 必填。  
- [ ] 热点源优先级、熔断状态、降级状态和 `source_name` 必须与 `05_API`、`03`、`01` 一致，不得新增未登记枚举。  
- [ ] 长尾股失败必须走 Partial Commit，核心池失败必须显式写 `stale_ok|degraded|missing`，不得把部分失败批次标成完整成功。  
- [ ] 回退、熔断、half-open 探测、批次错误和 lineage 修复都必须可追溯到 `batch_id`、`source_name`、`trade_date` 和 `stock_code`。  
- [ ] 验收映射固定到 `tests/test_fr04_multisource_ingest.py`、`tests/test_fr04_hotspot_bridge.py`、`tests/test_usage_lineage.py`。  

### US-005：系统生成市场状态
**Description:** 作为研报引擎，我希望在生成研报前拿到当日市场状态，以便对策略类型和风险暴露做正确判断。  
**Acceptance Criteria:**
- [ ] 公开入口固定为 `GET /api/v1/market/state`，返回 `market_state`、`trade_date`、`reference_date`、`state_reason`、`data_status`；`market_state` 只允许 `BULL|NEUTRAL|BEAR`。  
- [ ] 09:00 任务只能读取 `reference_date` 的有效交易日数据；幽灵时段只能回退上一有效缓存，绝对冷启动时返回 `NEUTRAL + state_reason=COLD_START_FALLBACK`。  
- [ ] 数据故障导致的 `NEUTRAL` 必须显式带 `market_state_degraded=true` 语义，供 FR-06 和前台横幅使用；不得把降级伪装成真实中性市。  
- [ ] 优先级固定为 `BEAR > BULL > NEUTRAL`，同时满足条件时必须输出 `BEAR`。  
- [ ] 验收映射固定到 `tests/test_fr05_market_state.py` 与 `tests/test_state_machine.py`。  

### US-006：系统为核心池单股生成正式研报
**Description:** 作为研究用户，我希望每只核心池股票每天都有至多一份正式研报，并包含结论、证据、指令卡和高级区过程，以便我能理解并使用它。  
**Acceptance Criteria:**
- [ ] 公开生成入口固定为 `POST /api/v1/reports/generate`；`stock_code`、`trade_date`、`strategy_type`、`idempotency_key` 的合法性校验必须在 LLM 调用前完成。  
- [ ] 正式结果必须同时落库 `report`、`report_generation_task`、`report_citation`、`report_data_usage_link`、`instruction_card`；输出最少包含 `recommendation`、`confidence`、`strategy_type`、`quality_flag`、`published`、`citations`、`trade_instruction_by_tier`。  
- [ ] 非核心池、停牌、BEAR 短路、上游缺失、LLM 解析失败、风险审计失败都必须有确定的 `HTTP 状态码 + error_code 或 status_reason`，不得静默跳过。  
- [ ] `published=true` 只允许发生在输入质量门、输出质量门、grounding、citation 三要素和风险审计规则全部通过之后；质量门失败时必须保留内部记录但对外不可见。  
- [ ] 验收映射固定到 `tests/test_fr06_report_generate.py`、`tests/test_fr06_llm_providers.py`、`tests/test_fr06_quality_pipeline.py`、`tests/test_llm_parse_response.py`。  

### US-007：用户查看公开研报列表与详情
**Description:** 作为普通用户，我希望从首页和列表进入详情页，查看公开研报结论、风险提示和证据摘要，以便快速了解个股结论。  
**Acceptance Criteria:**
- [ ] `GET /api/v1/reports`、`GET /api/v1/reports/{report_id}` 与 HTML 页面 `/`、`/reports`、`/reports/{report_id}` 的公开字段口径必须一致，且只暴露 `published=true` 且公开门通过的结果。  
- [ ] 未发布、被下架、超出 viewer 可见范围或 viewer tier 不满足的研报，必须返回 `403 REPORT_NOT_AVAILABLE` 或 `404 NOT_FOUND`，不得返回空字段 200。  
- [ ] 列表筛选、排序、分页、空态、降级横幅和错误页都要与 `05_API §2.1/§2.4/§16` 对齐，不得出现 API 成功但页面空白。  
- [ ] 详情页必须展示风险提示、数据质量横幅、证据摘要和免责声明；禁止泄露 prompt、raw LLM output、内部路径和 admin-only 字段。  
- [ ] 验收映射固定到 `tests/test_fr10_reports.py`、`tests/test_fr10_site_dashboard.py`、`tests/test_admin_dashboard_frontend_contract.py`，并需要浏览器验证。  

### US-008：付费用户查看高级区
**Description:** 作为付费用户，我希望高级区能以中文展示“所用数据”和“生成全过程”，以便我判断报告是否可信。  
**Acceptance Criteria:**
- [ ] 入口固定为 `GET /api/v1/reports/{report_id}/advanced`；匿名返回 `401 UNAUTHORIZED`，Free 返回后端裁剪摘要，`Pro|Enterprise|admin` 返回完整高级区。  
- [ ] 响应必须包含 `used_data_lineage`、`analysis_steps`、`prior_stats_snapshot`、`quality_gate_issues`、`risk_audit_status` 和降级数据说明，且字段名与 `05_API`、`report_data_usage` 事实层一致。  
- [ ] Free 的裁剪必须在后端完成；响应体和 DOM 中都不得包含完整推理链或完整高级区 JSON。  
- [ ] 高级区所有标题、降级说明、风险补充都必须是中文业务语义，不得直接暴露内部键名或调试态英文常量。  
- [ ] 验收映射固定到 `tests/test_fr10_reports.py`、`tests/test_fr10_site_dashboard.py`、`tests/test_fr09_auth.py`，并需要浏览器验证。  

### US-009：系统对历史信号做结算
**Description:** 作为策略评估者，我希望系统能对历史研报信号进行真实结算，以便衡量胜率、收益和失真情况。  
**Acceptance Criteria:**
- [ ] 受理入口固定为 `POST /api/v1/internal/settlement/run` 与 `POST /api/v1/admin/settlement/run`，同步返回 `202 Accepted + task_id`；结果查询与受理状态分离。  
- [ ] `settlement_result` 与 `strategy_metric_snapshot` 必须按 `strategy_type × window_days` 独立计算；固定窗口为 `1|7|14|30|60`，固定标准手数为 `100`，费用模型与 `NFR-05` 完全一致。  
- [ ] 对照组 `baseline_random` 与 `baseline_ma_cross` 必须独立生命周期、独立 `run_id/version`、独立 sample 分母，禁止与主体结算混写。  
- [ ] 样本数、覆盖率、四维指标、低样本 null 语义、`signal_validity_warning` 和 `display_hint` 都必须同时输出，不允许只返回单个百分比。  
- [ ] 验收映射固定到 `tests/test_fr07_settlement_run.py`、`tests/test_fr07_baseline_truth.py`、`tests/test_fr07_truth_filters.py`、`tests/test_admin_dashboard_frontend_contract.py`。  

### US-010：系统按三挡资金模拟持仓
**Description:** 作为用户，我希望系统能在 1W/10W/50W 三个资金规模下独立模拟持仓，以便看到不同资金条件下的真实可执行性。  
**Acceptance Criteria:**
- [ ] 三挡资金的底层枚举固定为 `10k|100k|500k`，账户、持仓、NAV、drawdown、事件和看板都必须按挡位独立落库与展示。  
- [ ] 开仓价使用 T+1 开盘价，平仓遵守悲观撮合、停牌顺延、退市清算、整数股向下取整和真实费用口径；FR-08 不得复用 FR-07 的固定 100 股分母。  
- [ ] 回撤状态只允许 `NORMAL|REDUCE|HALT`，阈值固定为 `-12%` 与 `-20%`，并与开仓权限和告警事件联动。  
- [ ] `GET /api/v1/portfolio/sim-dashboard`、`/api/v1/sim/positions`、`/api/v1/sim/account/snapshots` 的权限、tier 裁剪和字段语义必须一致。  
- [ ] 验收映射固定到 `tests/test_fr08_sim_positioning.py`、`tests/test_fr08_position_lifecycle_e2e.py`、`tests/test_fr08_sim_scenarios.py`、`tests/test_e2e_sim.py`。  

### US-011：用户注册登录并维持会话
**Description:** 作为访客，我希望完成注册、登录、激活、登出和 token 刷新，以便在浏览器和 API 端保持稳定身份。  
**Acceptance Criteria:**
- [ ] `/auth/register`、`/auth/activate`、`/auth/login`、`/auth/me`、`/auth/logout`、`/auth/refresh`、`/auth/forgot-password`、`/auth/reset-password` 必须形成完整闭环，且响应字段、状态码、错误码与 `05_API §3` 一致。  
- [ ] 密码必须强哈希；登录失败限流、邮箱未激活、refresh token 轮换、logout 撤销、reset-password 全设备登出都必须有精确状态和错误语义。  
- [ ] JWT claims、Cookie 优先级、黑名单、jti、sid、refresh grace window 和审计链必须在 API、服务和测试三层保持一致。  
- [ ] 任何日志、错误页、审计记录和 LLM prompt 中都不得包含明文密码、refresh token、access token 或 cookie。  
- [ ] 验收映射固定到 `tests/test_fr09_auth.py`、`tests/test_fr09_auth_supplemental.py`、`tests/test_nfr17_token.py`、`tests/test_nfr16_security.py`。  

### US-012：用户通过 OAuth 登录
**Description:** 作为用户，我希望能用 QQ/微信登录，以便减少注册阻力。  
**Acceptance Criteria:**
- [ ] `/auth/oauth/providers`、`/auth/oauth/{provider}/start`、`/auth/oauth/{provider}/callback` 必须形成真实或可验证 Mock 的完整闭环，不允许保留 501 或半截交换链。  
- [ ] provider 只允许 `qq|wechat`；非法 provider 返回 `400 INVALID_PROVIDER` 或 `422 VALIDATION_ERROR`，不得回 404。  
- [ ] `oauth_identity(provider, provider_user_id)` 是唯一幂等键；同一三方身份二次登录不得重复建户。  
- [ ] 缺失邮箱时必须采用显式 fallback 建户策略并保留可追溯身份绑定；不得把缺邮箱当作系统异常直接丢弃。  
- [ ] 验收映射固定到 `tests/test_fr09_auth.py` 与 `tests/test_auth_ui_copy.py`。  

### US-013：用户创建订单并获得会员权益
**Description:** 作为付费用户，我希望下单、支付、到账是一条完整可追踪的闭环，以便升级后立即解锁高级区与完整功能。  
**Acceptance Criteria:**
- [ ] `POST /billing/create_order`、`POST /billing/webhook`、`GET /membership/subscription/status` 必须形成订单、支付、权益到账与查询闭环。  
- [ ] `event_id` 是 Webhook 唯一幂等键；重复回调只能返回幂等结果，不得重复发放权益。  
- [ ] Webhook 签名错误返回 `400 PAYMENT_SIGNATURE_INVALID` 或 `401 INVALID_WEBHOOK_SIGNATURE`；订单状态更新与权益发放必须在同一事务中完成。  
- [ ] 低级套餐延迟回调不得覆盖仍有效的高级权益；支付方未配置时必须返回真实 `degraded|disabled` 语义。  
- [ ] 验收映射固定到 `tests/test_fr09_auth.py`、`tests/test_request_id_webhook_backlink.py`、`tests/test_fr12_admin.py`。  

### US-014：用户查看首页与看板
**Description:** 作为访客或会员，我希望首页和 Dashboard 展示系统当前可用结果与状态解释，以便快速判断平台是否值得信任。  
**Acceptance Criteria:**
- [ ] `GET /api/v1/home`、`GET /api/v1/dashboard/stats`、`GET /api/v1/portfolio/sim-dashboard` 的字段集合、分母口径、日期锚点和 `data_status` 语义必须一致。  
- [ ] 首页至少展示市场状态、池规模、今日研报数、精选研报；Dashboard 至少展示 FR-07 KPI、样本数、覆盖率、对照组和 FR-08 账户/回撤摘要。  
- [ ] 数据未就绪时必须显式返回 `COMPUTING|DEGRADED + status_reason`，前端必须展示骨架屏、解释文案或降级横幅，禁止空白页。  
- [ ] 所有百分比和汇总指标都必须披露分子、分母、样本数或覆盖率来源，不得制造“看起来很高”的孤立百分比。  
- [ ] 验收映射固定到 `tests/test_fr10_site_dashboard.py`、`tests/test_admin_dashboard_frontend_contract.py`、`tests/test_features_page.py`，并需要浏览器验证。  

### US-015：用户反馈触发复审
**Description:** 作为用户，我希望能对研报提交反馈，以便坏报告被尽快识别与处理。  
**Acceptance Criteria:**
- [ ] 入口固定为 `POST /api/v1/reports/{report_id}/feedback`；请求体只允许声明字段，未登录返回 `401`，不存在或不可见研报返回 `403|404`。  
- [ ] `positive` 与 `negative` 必须分离计数；同一用户对同一 report 的重复 `negative` 只允许幂等成功、不增加负反馈计数。  
- [ ] 单用户日频控必须原子生效；超过阈值返回 `429 RATE_LIMITED` 且不得落库。  
- [ ] 去重后负反馈达到阈值时，`review_flag` 必须首次进入 `PENDING_REVIEW` 并联动 FR-13 产生 `REPORT_PENDING_REVIEW` 事件。  
- [ ] 验收映射固定到 `tests/test_fr11_feedback_review.py` 与 `tests/test_fr13_event_dispatch.py`。  

### US-016：管理员管理用户、研报与补单
**Description:** 作为管理员，我希望能查看概览、管理用户、审核研报和执行补单，以便平台能持续运营。  
**Acceptance Criteria:**
- [ ] `GET /api/v1/admin/overview`、`GET /api/v1/admin/users`、`GET /api/v1/admin/reports`、`PATCH /api/v1/admin/users/{id}`、`PATCH /api/v1/admin/reports/{id}`、`POST /api/v1/admin/billing/orders/{order_id}/reconcile` 都必须按统一 envelope 返回。  
- [ ] 非登录态返回 `401`，非管理员返回 `403`，补单和高风险写操作必须要求明确 `reason_code` 或等效业务原因。  
- [ ] 审计日志必须记录 `actor_user_id`、`request_id`、`before`、`after`、`action_type` 和时间戳；审计写入失败时业务写入必须回滚。  
- [ ] 概览页中的任务状态、池规模、活跃持仓和日期锚点必须来自真实运行态，不得把 `NOT_RUN` 说成“计算中”。  
- [ ] 验收映射固定到 `tests/test_fr12_admin.py`、`tests/test_admin_dashboard_frontend_contract.py`、`tests/test_fr09_auth.py`。  

### US-017：超级管理员强制重建脏研报
**Description:** 作为超级管理员，我希望对确认脏数据的研报执行受控重建，以便纠错时不破坏历史血缘。  
**Acceptance Criteria:**
- [ ] 唯一入口是 `POST /api/v1/admin/reports/{report_id}/force-regenerate`；只有 `super_admin` 可调用，且请求体必须包含 `force_regenerate=true` 与非空 `reason_code`。  
- [ ] 已被 `sim_position`、`sim_settlement` 或 FR-07 结果引用的研报必须返回 `409 REPORT_ALREADY_REFERENCED_BY_SIM`，禁止直接覆盖。  
- [ ] 成功重建时必须软删除旧 report、生成新 `report_id`、建立 supersede 关系，并保留完整审计链和 request_id 关联。  
- [ ] 重建失败、门禁拒绝和审计失败都必须有确定的错误码和状态，不允许 silent rollback。  
- [ ] 验收映射固定到 `tests/test_fr12_admin.py`、`tests/test_fr06_report_generate.py`、`tests/test_fr07_settlement_run.py`。  

### US-018：系统对关键业务事件发通知
**Description:** 作为管理员或付费用户，我希望在平仓、BUY 强信号、回撤和待复审时收到事务性通知，以便及时响应。  
**Acceptance Criteria:**
- [ ] 事件类型固定为 `POSITION_CLOSED`、`BUY_SIGNAL_DAILY`、`DRAWDOWN_ALERT`、`REPORT_PENDING_REVIEW`；幂等键投影必须与 `03`、`01` 一致。  
- [ ] 任何通知都必须先写 Outbox，再在事务提交后异步派发；主事务回滚时不得出现 sent 记录。  
- [ ] 相同幂等键重复触发时通知状态必须是 `skipped`，不是再次 `sent`；失败重试耗尽后才允许 `failed`。  
- [ ] 推送失败、渠道未配置、用户无权限和抑制窗口命中都必须保留可审计的状态与原因，且不能阻塞主业务链。  
- [ ] 验收映射固定到 `tests/test_fr13_event_dispatch.py`、`tests/test_fr08_position_lifecycle_e2e.py`、`tests/test_fr11_feedback_review.py`。  

### US-019：系统清理临时脏状态但保留正式事实
**Description:** 作为平台维护者，我希望系统可以清理未激活账户、过期 token、僵尸任务和不完整报告，同时永久保留正式研报、行情、结算和模拟数据。  
**Acceptance Criteria:**
- [ ] 清理链只允许处理未激活账号、过期 token、超窗 `Pending|Suspended` 任务、不完整 bundle 和超保留期通知；正式研报、行情、结算、模拟、审计和已发布证据属于永久保留域。  
- [ ] 超窗任务必须先原子转 `Expired + status_reason=stale_task_expired`，再进入归档或清理；不得把仍可恢复的任务直接物理删除。  
- [ ] 清理任务必须使用独立时间窗和独立互斥策略，不得在交易主链内顺手执行 destructive cleanup。  
- [ ] 清理结果必须返回删除计数、过期计数、耗时、状态和错误原因；任何触及保护域的动作都必须失败并告警。  
- [ ] 验收映射固定到 `tests/test_fr09b_cleanup.py`、`tests/test_fr06_quality_pipeline.py`、`tests/test_usage_lineage.py`。  

---

## 9. Design Considerations

### 9.1 页面级设计要求

| 页面 | 必须展示的区块 | 降级 / 空态要求 |
| --- | --- | --- |
| 首页 `/` | 市场状态、精选研报、最新研报、今日研报数、池规模 | 无公开研报时显示真实空态；统计未就绪时显示 `DEGRADED/COMPUTING` |
| 研报列表 `/reports` | 筛选器、分页、列表卡片、推荐/置信度/市场状态/池内标签 | 无结果时给出筛选解释，不显示“伪空白” |
| 研报详情 `/reports/{report_id}` | 结论、置信度、指令卡、证据摘要、风险提示、数据质量横幅 | 未发布/无权限时返回真实错误页，不泄露内部字段 |
| 高级区 | 所用数据、生成全过程、prior stats、risk audit | Free 显示摘要 + 升级提示；付费显示完整内容 |
| Dashboard `/dashboard` | FR-07 指标卡、FR-08 资金曲线、样本数、覆盖率 | 统计未就绪要保留 `status_reason` |
| Sim Dashboard | 三挡资金曲线、账户汇总、持仓摘要、回撤状态 | Free 只能看 100k；其他挡位给出明确权限提示 |
| Login / Register | 表单、错误提示、成功路径 | 登录状态下访问登录/注册页应重定向 |
| Profile / Subscribe | 用户 tier、到期时间、升级入口、支付状态 | 支付链不可用时不能伪装“已可购买” |
| Admin | overview、users、reports、cookie、reconcile、audit 结果 | 权限不足必须 403，不得回落到公开页面 |

### 9.2 文案与展示原则

- 高级区必须用中文表达“所用数据”“生成全过程”。  
- 降级研报必须有醒目横幅，不能只在日志或管理后台提示。  
- 前台不得把空列表、空统计、缺数据页面包装成“成功”。  
- 所有百分比必须同时标注分子、分母、样本数与覆盖率来源。  

### 9.3 交互规则

- 详情页上的高级区权限判断必须后端裁剪 + 前端展示提示双保险。  
- 支付链路、OAuth 链路、强制重建链路都必须显示明确成功/失败原因。  
- 页面跳转与表单反馈必须与实际 API 返回语义一致，不得自造文案。  

---

## 10. Technical Considerations

### 10.1 架构约束

- 遵守 `02_系统架构.md` 的 L0-L4 分层：L2 做业务判定，L3 做锁/幂等/熔断/事务/Outbox，L4 做外部适配。  
- `capital_tier`、权限、状态枚举必须集中定义，禁止在各模块硬编码散落字符串。  
- 只读查询链与写入治理链必须分离：读模型不偷偷写状态，写链不偷跳过审计。  
- 所有依赖持久化结果的事件都必须在事务提交后派发。  

### 10.2 数据与真实性约束

- `report_data_usage` 是高级区“所用数据”的事实来源，不得用拼装文案替代。  
- `citations` 必须来自真实 usage 与真实 source_url，不允许空链接或伪时间戳。  
- `published=true` 是公开面准入，不等于“内部已落库”；对外页面必须再过公开 payload gate。  
- `quality_flag` 与 `publish_status` 是两个不同维度：前者是质量，后者是公开状态。  

### 10.3 LLM 与生成链约束

- Prompt 只能带业务数据，不得带用户身份信息。  
- Provider 失败必须显式记录 `llm_fallback_level`。  
- 风险审计、grounding 校验、公开 payload gate 都要在发布前完成。  
- Free 高级区摘要必须在后端生成与裁剪，禁止前端拿完整链后自己藏。  

### 10.4 调度与治理约束

- 同一业务主键（`idempotency_key` / `event_id` / `trade_date+task_name` 等）必须有统一幂等治理。  
- 调度器必须支持锁 TTL、fencing、续约和实例崩溃后接管。  
- 清理任务与日批任务必须隔离，不得在主链上顺手做 destructive cleanup。  

### 10.5 测试锚点（当前仓库）

| FR 域 | 代表测试文件 |
| --- | --- |
| FR-01 | `tests/test_fr01_pool_refresh.py` |
| FR-02 | `tests/test_fr02_dag_lock.py`, `tests/test_fr02_scheduler_ops.py` |
| FR-03 | `tests/test_fr03_cookie_routes.py`, `tests/test_fr03_cookie_session.py` |
| FR-04 | `tests/test_fr04_hotspot_bridge.py`, `tests/test_fr04_multisource_ingest.py` |
| FR-05 | `tests/test_fr05_market_state.py` |
| FR-06 | `tests/test_fr06_report_generate.py`, `tests/test_fr06_llm_providers.py`, `tests/test_fr06_quality_pipeline.py` |
| FR-07 | `tests/test_fr07_settlement_run.py`, `tests/test_fr07_baseline_truth.py`, `tests/test_fr07_truth_filters.py` |
| FR-08 | `tests/test_fr08_position_lifecycle_e2e.py`, `tests/test_fr08_sim_positioning.py` |
| FR-09 | `tests/test_fr09_auth.py`, `tests/test_ai_api_contract.py`, `tests/test_api_bridge_contract.py` |
| FR-10 | `tests/test_fr10_reports.py`, `tests/test_fr10_site_dashboard.py`, `tests/test_features_page.py` |
| FR-11 | `tests/test_fr11_feedback_review.py` |
| FR-12 | `tests/test_fr12_admin.py` |
| FR-13 | `tests/test_fr13_event_dispatch.py` |

### 10.6 Ralph 原子 story 规则

`.claude/ralph/loop/prd.json` 是 Ralph 的直接执行入口，不是本文的摘要。每条 story 必须满足以下规则：

- 只改一个原子能力：一张表/一组强相关模型、一个采集适配器、一个查询 API、一个页面入口、一个调度治理点、一个权限门或一个通知事件。  
- 不把“建模型 + 写服务 + 做页面 + 补测试”塞进同一条 story；确有依赖时，先模型，再服务，再 API，再页面。  
- 验收标准必须能由 Ralph 检查：字段、枚举、端点、模型、权限、状态码、`error_code`、幂等键或唯一键、数值阈值、降级语义、示例断言、测试命令、浏览器验证都要写成确定语句。  
- 每条 story 的 `acceptanceCriteria` 至少覆盖 5 类信息：入口与权限、状态码与错误码、幂等键或唯一键、枚举或阈值与降级、示例断言与 pytest 命令。  
- 每条 story 必须包含 `Typecheck passes`；涉及业务逻辑、API、持久化、权限或统计时必须包含 `Tests pass`；涉及 HTML/UI 时必须包含 `Verify in browser using dev-browser skill`。  
- `passes` 初始值统一为 `false`。`notes` 不能再留空，必须是紧凑 JSON 字符串，至少包含 `group`、`dependsOn`、`endpoints`、`models`、`permissions`、`errorCodes`、`idempotency`、`enums`、`thresholds`、`degradation`、`exampleAssert`、`pytest`。  
- `branchName` 以 `.claude/ralph/config.json` 与 `.claude/ralph/loop/prd.json` 为准，固定为 `ralph/ashare-research-platform`；当前仓库存在 `.git`，但 Ralph 不得自动 checkout、commit 或改写分支，除非用户在当轮明确授权。  
- `docs/core/28_严格验收与上线门禁.md` 与 `docs/core/29_Ralph_PRD字段映射说明.md` 是 Ralph 的规则补充来源；runtime 版 `prd.json` 仍必须内联 story 选择、依赖、验收、降级、blocked/fail-close 与 `passes` 变更规则，不能把关键执行语义外包给隐藏上下文。  
- `prd.json` 顶层与 story 级字段只能使用 Ralph 官方最小 schema：`project`、`branchName`、`description`、`userStories`，以及 story 的 `id`、`title`、`description`、`acceptanceCriteria`、`priority`、`passes`、`notes`。禁止自定义 `tags`、`deps`、`owner`、`component` 等扩展字段。  

#### 10.6.1 禁用模糊词

以下词语禁止出现在 `prd.json` 的 `description`、`acceptanceCriteria`、`notes` 中：

- `等价`
- `按项目规则`
- `视情况`
- `如有需要`
- `合理处理`
- `适当处理`
- `必要时`
- `兼容现有逻辑`
- `保持一致`
- `支持更多`
- `正常返回`
- `正确处理`
- `完成闭环`
- `异常情况`
- `等等`

替换原则：必须改写成精确路径、精确模型、精确角色、精确状态码、精确 `error_code`、精确唯一键、精确阈值、精确 fallback、精确示例断言、精确 pytest 命令。

#### 10.6.2 `notes` 结构规范

- `notes` 不是自由文本，必须是紧凑 JSON 字符串。  
- `group`：当前 story 所属执行组，例如 `G0`、`G7`。  
- `dependsOn`：只写前置 story id 数组。  
- `endpoints`：只写 `METHOD PATH` 或明确 service entry。  
- `models`：只写模型、核心表、read model 或任务对象。  
- `permissions`：只写 `public`、`user`、`paid`、`admin`、`super_admin`、`internal`、`system` 等实际权限语义。  
- `errorCodes`：只写实际 `error_code`，不写泛词。  
- `idempotency`：写幂等键或唯一键表达式。  
- `enums`：写冻结枚举或固定允许值。  
- `thresholds`：写数值阈值、窗口、上限、固定样本门槛。  
- `degradation`：写失败与降级语义，禁止写“正常处理”这类空话。  
- `exampleAssert`：写最小可验证断言。  
- `pytest`：写单条可执行 pytest 命令，且只能引用真实存在的测试文件。  

### 10.7 Ralph 单轮完成定义

一轮 Ralph 只能选择最高优先级且 `passes=false` 的一条 story，并在结束前满足：

1. 只实现该 story 的目标，不顺手重构无关模块。  
2. 如发现 story 过大，只做能完整闭环的最小子能力，并在 `progress.txt` 记录拆分建议。  
3. 代码改动后执行该 story 对应的最小测试切片；只改 Markdown/JSON/PowerShell 时至少完成 JSON parse 与编辑器诊断。  
4. 不引入 501、空壳接口、伪数据、伪 citation、伪成功 HTTP 200。  
5. 不修改 `scripts/`、`data/`、`output/`，不在根目录写临时文件。  
6. 成功后只把当前 story 的 `passes` 改为 `true`，并追加 `progress.txt`；未获用户明确授权时禁止自动提交 git commit。  

### 10.8 推荐实施阶段

| 阶段 | 目标 | 对应 story 群 |
| --- | --- | --- |
| Phase 0 | 共享契约、响应封装、request_id、health、审计与运行门禁 | US-001–US-006 |
| Phase 1 | 数据库与核心实体模型冻结 | US-007–US-020 |
| Phase 2 | 多源采集、热点、非研报数据、血缘、熔断与 Cookie 会话 | US-021–US-033 |
| Phase 3 | 股票池、市场状态、调度 DAG、锁、恢复与管理操作 | US-034–US-046 |
| Phase 4 | 研报提交、LLM、质量门、审计、持久化、公开查询与批量生成 | US-047–US-066 |
| Phase 5 | FR-07 结算、基线、KPI、覆盖率与重放 | US-067–US-074 |
| Phase 6 | FR-08 三挡模拟、撮合、账户、回撤与看板查询 | US-075–US-082 |
| Phase 7 | 注册登录、Token、OAuth、RBAC | US-083–US-090 |
| Phase 8 | 支付、Webhook、权益、补单与会员门禁 | US-091–US-095 |
| Phase 9 | 首页、搜索、详情、高级区、Dashboard、模拟、认证与管理页面 | US-096–US-100 |

说明：旧版 `prd.json` 的 110+ story 集存在重复切片、空 `notes`、伪测试映射和模糊表述。本版执行基线以 100 条故事为准，按上表重新收敛依赖顺序；27 文档与两份 `prd.json` 必须同步维护。

### 10.9 `prd.json` 维护要求

- Runtime 文件固定为 `.claude/ralph/loop/prd.json`。  
- 命名副本固定为 `.claude/ralph/prd/yanbao-platform-enhancement.json`，必须与 runtime 文件内容一致。  
- 更新 story 数量、优先级或验收标准后，必须做 JSON 解析校验，并确认两个 JSON 文件同步。  
- 不在 `prd.json` 内加入 Ralph 官方 schema 之外的自定义字段，避免 loop 脚本或后续工具解析歧义。  
- `description` 必须显式说明 28/29 规则与关键执行语义已经沉淀到当前 JSON。  
- `notes` 不能为空字符串；若某条 story 无额外局部元数据，也必须提供最小基线 JSON 键，并把细节回指到 `acceptanceCriteria`。  
- pytest 命令只能引用工作区内真实存在的测试文件；禁止发明 `tests/test_xxx.py`。  
- 修改 `branchName`、story 数量、优先级、测试命令或 `notes` 结构时，两个 JSON 文件必须做同内容更新。  

---

## 11. Success Metrics

### 11.1 交付完成判定

- 核心池每日输出精确 200 只，或显式回退且可解释。  
- 研报链可对核心池执行正式生成；失败时留下真实、可追踪的未发布记录。  
- 高级区能稳定展示中文“所用数据”与“生成全过程”。  
- FR-07/FR-08 看板上所有关键统计都附带样本数与覆盖率。  
- 注册/登录/OAuth/支付/补单全链可走通，不存在 501 或空壳回调。  
- 管理端所有高风险操作可追溯到审计记录。  

### 11.2 业务成效指标

- 真实胜率 ≥ 55%。  
- 盈亏比 ≥ 1.5。  
- 年化 Alpha ≥ 10%。  
- 最大回撤 ≤ 20%。  
- 方向命中率只做辅助监控，不允许替代核心商业指标。  

### 11.3 真实性指标

- 公开研报 100% 带 citation 三要素。  
- 所有关键统计 100% 披露样本数与覆盖率。  
- 任何降级态 100% 有 `status_reason` 或前台可读解释。  
- 任何已发布研报 100% 满足只读保护。  

---

## 12. Open Questions and Current Blockers

### 12.1 当前真实阻塞（来自 22 / 26，必须保留事实）

- `EXT-01`：部分热点源在真实环境下仍可能 0 命中，不能伪装成热点恢复。  
- `EXT-02`：北向等外部数据存在环境依赖缺口，缺失时必须诚实标记 `missing/degraded`。  
- `EXT-03`：真实 OAuth / Billing SaaS 接入参数可能仍未到位，若用 Mock 必须是**全真闭环 Mock**。  
- `EXT-04`：live LLM provider 可能不可达；provider 故障不能被包装成“研报已完成”。  
- `EXT-05`：真实 SQLite 写锁争用仍可能阻断 live 补数与批量落库。  
- `EXT-06`: `docs/core/08_AI????.md` is missing in the current worktree; Ralph runtime artifacts must preserve this as missing and must not invent the file contents.  

### 12.2 仍需明确但不允许靠猜的事项

- 真实支付渠道最终选型（若切换 provider，需保持同一订单/回调契约）。  
- 真实 SMTP / 邮件服务供应商。  
- OAuth 正式回调域名与审核参数。  
- 外部热点与北向数据在生产环境的稳定性基线。  

### 12.3 执行规则

- 若阻塞源未恢复，系统必须**显式报错或降级**，不能把“暂不可用”写成“已恢复”。  
- 若当前实现与 `01/05` 不一致，以 SSOT 为准；若 SSOT 之间存在冲突，必须先修文档再修代码。  
- 若某功能只有 API 200 而页面不可用，则判定为**未完成**。  

---

## 13. 结论

本文的目标不是生成“更漂亮的产品说明书”，而是生成一份**自治代理可执行的真值 PRD**。  

后续 `.claude/ralph/loop/prd.json` 必须继续遵守三条原则：

1. 故事必须足够小，一个故事只解决一个真实问题；  
2. 故事必须可验证，且验证方式要与当前仓库测试和页面一致；  
3. 故事必须保持真实性，不得因为想追求“自动生成系统”而偷换当前事实。
