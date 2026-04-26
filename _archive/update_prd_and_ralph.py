from pathlib import Path
import json
import shutil
import textwrap
from datetime import date


root = Path(r"D:\yanbao-new")
docs = root / "docs" / "core"
old_prd = docs / "27_产品需求文档_PRD.md"
new_prd = docs / "27_PRD_研报平台增强与整体验收基线.md"
loop_dir = root / ".claude" / "ralph" / "loop"
runtime_prd = loop_dir / "prd.json"
named_prd = root / ".claude" / "ralph" / "prd" / "yanbao-platform-enhancement.json"
progress_file = loop_dir / "progress.txt"
last_branch_file = loop_dir / ".last-branch"
config_path = root / ".claude" / "ralph" / "config.json"
today = date(2026, 4, 25).isoformat()
new_branch = "ralph/ashare-research-platform"

if old_prd.exists() and not new_prd.exists():
    old_prd.rename(new_prd)

old_branch = ""
if runtime_prd.exists():
    try:
        old_branch = json.loads(runtime_prd.read_text(encoding="utf-8")).get("branchName", "")
    except Exception:
        old_branch = ""

progress_text = progress_file.read_text(encoding="utf-8") if progress_file.exists() else ""
non_empty_lines = [line for line in progress_text.splitlines() if line.strip()]
if old_branch and old_branch != new_branch and len(non_empty_lines) > 3:
    archive_dir = loop_dir / "archive" / f"{today}-{old_branch.removeprefix('ralph/')}"
    archive_dir.mkdir(parents=True, exist_ok=True)
    if runtime_prd.exists():
        shutil.copy2(runtime_prd, archive_dir / "prd.json")
    if progress_file.exists():
        shutil.copy2(progress_file, archive_dir / "progress.txt")
    progress_file.write_text(f"# Ralph Progress Log\nStarted: {today}\n---\n", encoding="utf-8")
elif not progress_file.exists():
    progress_file.write_text(f"# Ralph Progress Log\nStarted: {today}\n---\n", encoding="utf-8")

markdown = textwrap.dedent("""
# 27_PRD_研报平台增强与整体验收基线.md

> **文档编号**：27  
> **文档名称**：研报平台增强与整体验收基线（Ralph 执行版 PRD）  
> **项目名称**：A 股个股研报平台（`yanbao-new`）  
> **版本**：v2.0（面向自治代理的细化执行版）  
> **产出日期**：2026-04-25  
> **适用对象**：产品、研发、测试、运维、自治代理（Ralph / Codex / Gemini CLI）  
> **编写依据**：`AGENTS.md`、`docs/core/01_需求基线.md`、`02_系统架构.md`、`04_数据治理与血缘.md`、`05_API与数据契约.md`、`06_全量数据需求说明.md`、`22_全量功能进度总表_v12.md`、`25_系统问题分析角度清单.md`、`26_自动化执行记忆.md`，以及 `app/`、`tests/` 当前实现  
> **文档目标**：把“业务目标、关键对象、页面行为、失败与降级边界、数据血缘、任务拆解、测试锚点”收敛为一份 AI 可执行 PRD，使代理仅依据本文与 SSOT 文档就能逐步生成系统。  
> **真实性声明**：本文描述的是**目标系统**与**必须满足的真实约束**，同时显式保留当前运行态阻塞与外部依赖缺口；禁止把未接入或未恢复的能力写成“已完成”。

---

## 0. 阅读与执行说明

1. `01/02/04/05/06` 仍是正式 SSOT；本文不替代它们，而是把实现者真正需要的跨文档信息整合成执行视图。  
2. 本文读者包括 junior 开发者与 AI 代理，因此必须写明：**触发、输入、输出、状态、降级、异常、页面、接口、关键对象、验证方式**。  
3. 对 Ralph 而言，本文只定义**系统目标与切片原则**；真正的原子执行任务以 `.claude/ralph/loop/prd.json` 为准。  
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

- 让代理仅依赖 `prd.json` 就能按依赖顺序完成系统实现，而不是依赖口头说明。  
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
- [ ] 核心池固定为 200 只，候补池按排序保留。  
- [ ] 刷新失败时有明确回退与告警。  
- [ ] 并发刷新不会产生双版本竞争。  
- [ ] Typecheck passes  

### US-002：调度器按依赖推进日批
**Description:** 作为系统维护者，我希望调度器按 DAG 推进 daily chain，以便 FR-06/07/08 不会因为时钟错乱而提前执行。  
**Acceptance Criteria:**
- [ ] FR-06 等待 FR-04 完成事件。  
- [ ] FR-07/08 等待 FR-06 批次完成事件。  
- [ ] 跨日超时会统一终止并留下可解释状态。  
- [ ] Typecheck passes  

### US-003：系统维护热点采集登录态
**Description:** 作为数据工程维护者，我希望系统能安全保存并探测 Cookie 会话，以便多源热点抓取保持可用。  
**Acceptance Criteria:**
- [ ] 可保存 provider 对应的 cookie_session。  
- [ ] 可返回 ok/fail/skipped 与失败原因。  
- [ ] 不会在日志输出明文 cookie。  
- [ ] Typecheck passes  

### US-004：系统统一采集多源数据并记录血缘
**Description:** 作为数据工程师，我希望所有研报相关数据都能统一采集、标准化并记录批次/血缘，以便后续研报与审计可追踪。  
**Acceptance Criteria:**
- [ ] 统一写入 stock_master、kline、hotspot、data_batch、report_data_usage。  
- [ ] 非 ok 数据明确写 `status_reason`。  
- [ ] 部分失败不伪装成整批成功。  
- [ ] Typecheck passes  

### US-005：系统生成市场状态
**Description:** 作为研报引擎，我希望在生成研报前拿到当日市场状态，以便对策略类型和风险暴露做正确判断。  
**Acceptance Criteria:**
- [ ] 输出 BULL/BEAR/NEUTRAL。  
- [ ] 结果带 reference_date 与 state_reason。  
- [ ] degraded 状态可对前台解释。  
- [ ] Typecheck passes  

### US-006：系统为核心池单股生成正式研报
**Description:** 作为研究用户，我希望每只核心池股票每天都有至多一份正式研报，并包含结论、证据、指令卡和高级区过程，以便我能理解并使用它。  
**Acceptance Criteria:**
- [ ] 单股单日幂等。  
- [ ] 输出 recommendation/confidence/strategy_type/citations/trade_instruction_by_tier。  
- [ ] 质量门失败时不公开发布。  
- [ ] Typecheck passes  

### US-007：用户查看公开研报列表与详情
**Description:** 作为普通用户，我希望从首页和列表进入详情页，查看公开研报结论、风险提示和证据摘要，以便快速了解个股结论。  
**Acceptance Criteria:**
- [ ] 首页、列表、详情三者字段口径一致。  
- [ ] 未发布研报不能被匿名或普通用户绕过门禁查看。  
- [ ] 页面有诚实的空态/错误态/降级态。  
- [ ] Typecheck passes  
- [ ] Verify in browser using dev-browser skill  

### US-008：付费用户查看高级区
**Description:** 作为付费用户，我希望高级区能以中文展示“所用数据”和“生成全过程”，以便我判断报告是否可信。  
**Acceptance Criteria:**
- [ ] 高级区返回完整 used_data_lineage。  
- [ ] 高级区返回 reasoning_chain 与 risk_audit 结果。  
- [ ] Free 仅看到摘要，付费用户看到完整内容。  
- [ ] Typecheck passes  
- [ ] Verify in browser using dev-browser skill  

### US-009：系统对历史信号做结算
**Description:** 作为策略评估者，我希望系统能对历史研报信号进行真实结算，以便衡量胜率、收益和失真情况。  
**Acceptance Criteria:**
- [ ] 生成 settlement_result 与 KPI 快照。  
- [ ] 统计同时带样本数与覆盖率。  
- [ ] 基线与主样本口径独立。  
- [ ] Typecheck passes  

### US-010：系统按三挡资金模拟持仓
**Description:** 作为用户，我希望系统能在 1W/10W/50W 三个资金规模下独立模拟持仓，以便看到不同资金条件下的真实可执行性。  
**Acceptance Criteria:**
- [ ] 开仓、平仓、账户快照、回撤状态完整闭环。  
- [ ] 费用、滑点、整数股数规则真实入账。  
- [ ] sim-dashboard 可按挡位查看。  
- [ ] Typecheck passes  

### US-011：用户注册登录并维持会话
**Description:** 作为访客，我希望完成注册、登录、激活、登出和 token 刷新，以便在浏览器和 API 端保持稳定身份。  
**Acceptance Criteria:**
- [ ] 注册/登录/激活/刷新/登出链完整可用。  
- [ ] 重置密码能撤销旧 token。  
- [ ] 敏感信息不出现在日志中。  
- [ ] Typecheck passes  

### US-012：用户通过 OAuth 登录
**Description:** 作为用户，我希望能用 QQ/微信登录，以便减少注册阻力。  
**Acceptance Criteria:**
- [ ] provider 列表可见并返回可用状态。  
- [ ] start/callback 能走完整闭环。  
- [ ] 缺失邮箱时采用明确 fallback 绑定策略。  
- [ ] Typecheck passes  

### US-013：用户创建订单并获得会员权益
**Description:** 作为付费用户，我希望下单、支付、到账是一条完整可追踪的闭环，以便升级后立即解锁高级区与完整功能。  
**Acceptance Criteria:**
- [ ] 订单创建返回支付所需参数。  
- [ ] Webhook 幂等且可重放。  
- [ ] 权益到账与订单确认原子一致。  
- [ ] Typecheck passes  

### US-014：用户查看首页与看板
**Description:** 作为访客或会员，我希望首页和 Dashboard 展示系统当前可用结果与状态解释，以便快速判断平台是否值得信任。  
**Acceptance Criteria:**
- [ ] 首页展示精选研报、池规模、今日研报数、市场状态。  
- [ ] Dashboard 展示 FR-07 与 FR-08 指标。  
- [ ] 未就绪时明确显示 COMPUTING/DEGRADED。  
- [ ] Typecheck passes  
- [ ] Verify in browser using dev-browser skill  

### US-015：用户反馈触发复审
**Description:** 作为用户，我希望能对研报提交反馈，以便坏报告被尽快识别与处理。  
**Acceptance Criteria:**
- [ ] positive/negative 反馈口径分离。  
- [ ] 同用户重复 negative 不重复计数。  
- [ ] 阈值触发后研报进入待复审。  
- [ ] Typecheck passes  

### US-016：管理员管理用户、研报与补单
**Description:** 作为管理员，我希望能查看概览、管理用户、审核研报和执行补单，以便平台能持续运营。  
**Acceptance Criteria:**
- [ ] overview/users/reports/reconcile 接口可用。  
- [ ] 每次高风险操作都留下审计日志。  
- [ ] 返回结构与 05_API 契约一致。  
- [ ] Typecheck passes  

### US-017：超级管理员强制重建脏研报
**Description:** 作为超级管理员，我希望对确认脏数据的研报执行受控重建，以便纠错时不破坏历史血缘。  
**Acceptance Criteria:**
- [ ] 只有 super_admin 可以触发。  
- [ ] 已被 sim/settlement 引用的研报被 409 阻断。  
- [ ] 新旧研报有 supersede 关系与审计链。  
- [ ] Typecheck passes  

### US-018：系统对关键业务事件发通知
**Description:** 作为管理员或付费用户，我希望在平仓、BUY 强信号、回撤和待复审时收到事务性通知，以便及时响应。  
**Acceptance Criteria:**
- [ ] 事件走 Outbox，事务回滚不外发。  
- [ ] 幂等键重复时记录 skipped。  
- [ ] 推送失败不阻塞主链。  
- [ ] Typecheck passes  

### US-019：系统清理临时脏状态但保留正式事实
**Description:** 作为平台维护者，我希望系统可以清理未激活账户、过期 token、僵尸任务和不完整报告，同时永久保留正式研报、行情、结算和模拟数据。  
**Acceptance Criteria:**
- [ ] 临时对象可按规则清理。  
- [ ] 正式保留域不会被误删。  
- [ ] 清理任务不阻断交易主链。  
- [ ] Typecheck passes  

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

### 10.6 Ralph 切片原则

- 一个 story 只做一类模型、一类接口、一段查询链、一块 UI、一个治理机制或一组紧密耦合的统计计算。  
- 依赖顺序固定为：**基础模型 → 数据采集 → 治理/调度 → 研报主链 → 结算/模拟 → 鉴权/支付 → 页面/后台 → 通知/清理/治理**。  
- UI story 必须附带浏览器验证，不允许只看快照或只跑接口测试。  
- 任何 story 如果无法在 2–3 句话说清“改什么、在哪改、如何验”，就说明太大，需要继续拆。  

### 10.7 推荐实施阶段

| 阶段 | 目标 | 对应 story 群 |
| --- | --- | --- |
| Phase 1 | 共享枚举、响应封装、基础模型、health | Story 1–6 |
| Phase 2 | 采集、血缘、熔断、池子、市场状态 | Story 7–17 |
| Phase 3 | 调度治理、研报主链、批量生成、清理 | Story 18–32 |
| Phase 4 | 结算与三挡模拟 | Story 33–40 |
| Phase 5 | 注册登录、OAuth、支付、会员权限 | Story 41–48 |
| Phase 6 | 页面、反馈、后台、通知、清理、治理目录 | Story 49–60 |

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
""").strip() + "\n"

stories_raw = [
    (
        "冻结共享枚举与统一响应封装",
        "作为平台维护者，我希望把 `capital_tier`、核心状态枚举和统一 JSON 响应封装冻结在共享配置中，以便所有模块使用同一套输入输出契约。",
        [
            "`config.CAPITAL_TIERS` 固定输出 `10k|100k|500k`，并提供展示映射 `1W|10W|50W`",
            "共享枚举至少覆盖 `Recommendation`、`QualityFlag`、`MarketState`、`UserRole`、`UserTier`、`PositionStatus`",
            "JSON API 统一返回 `{ success, request_id, data }`，错误态追加 `error_code` 与 `error_message`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "建立股票与行情基础模型",
        "作为平台维护者，我希望先冻结股票、行情和股票池相关的基础模型，以便后续采集、筛选和查询逻辑建立在稳定 schema 上。",
        [
            "ORM/建表覆盖 `stock_master`、`kline_daily`、`stock_pool_snapshot`、`stock_score`、`stock_pool_refresh_task`",
            "`kline_daily` 至少包含 `stock_code trade_date open high low close volume amount turnover_rate source_batch_id`",
            "`stock_pool_snapshot` 至少包含 `pool_date pool_version rank_no is_core score`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "建立研报主链模型",
        "作为平台维护者，我希望先定义研报、引用、任务与 usage link 的主链模型，以便研报生成和展示链可以按统一结构落库。",
        [
            "ORM/建表覆盖 `report`、`report_citation`、`report_generation_task`、`report_data_usage_link`",
            "`report` 至少包含 `report_id idempotency_key recommendation confidence strategy_type quality_flag published review_flag status_reason`",
            "`report_generation_task` 至少支持 `PENDING/PROCESSING/COMPLETED/FAILED/SUSPENDED/EXPIRED` 状态",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "建立结算与模拟模型",
        "作为平台维护者，我希望先定义结算、基线、模拟仓位和账户快照模型，以便 FR-07 与 FR-08 的写入和统计口径稳定。",
        [
            "ORM/建表覆盖 `settlement_result`、`strategy_metric_snapshot`、`sim_position`、`sim_account_snapshot`",
            "`settlement_result` 同时记录 `window_days buy_price sell_price net_return_pct is_misclassified`",
            "`sim_position` 明确区分 `capital_tier` 与 `position_status`，不与 FR-07 口径混用",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "建立会员、支付、审计与通知模型",
        "作为平台维护者，我希望把用户、刷新令牌、订单、审计日志、通知和 cookie 会话模型一次冻结，以便高风险业务动作都有可追踪承载。",
        [
            "ORM/建表覆盖 `user`、`refresh_token`、`billing_order`、`audit_log`、`notification`、`cookie_session`",
            "`user` 至少包含 `role tier membership_level membership_expires_at`",
            "`notification` 至少包含 `event_type channel status dedupe_key payload_json`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现健康检查与 request_id 传播",
        "作为平台维护者，我希望先打通 health 与 request_id 传播，以便所有接口和页面都有基础诊断与链路追踪能力。",
        [
            "`/health` 返回 database/scheduler/report_chain/hotspot 等聚合状态",
            "每个 JSON API 响应头和响应体包含同一 `request_id`",
            "`overall` 状态可输出 `ok|degraded|fail`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现证券主数据采集与标准化",
        "作为数据工程师，我希望系统先把 A 股证券主数据标准化落库，以便后续池子、研报、详情页都能使用统一证券主键。",
        [
            "可从行情基础源写入/更新 `stock_master`",
            "标准化字段至少覆盖 `stock_code stock_name exchange industry listed_at is_st is_delisted`",
            "退市/ST 状态可直接用于股票池过滤",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现日线行情采集与双源回退",
        "作为数据工程师，我希望系统可以稳定写入日线行情并在源异常时显式回退，以便后续技术分析和结算链不依赖伪数据。",
        [
            "可按 `trade_date + stock_code` 写入 `kline_daily`",
            "支持 exact/stale 回退并显式标记来源与状态",
            "缺失行情时不得伪造 OHLC 值",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现多源热点采集与标准化入库",
        "作为数据工程师，我希望系统能把热点源统一写入标准热点表，以便策略 A 判定、详情热点和 citation 引用口径一致。",
        [
            "至少接入 `eastmoney`、`weibo`、`douyin` 的统一热点写入链路",
            "写入 `market_hotspot_item`、`market_hotspot_item_source`、`market_hotspot_item_stock_link` 或等价结构",
            "热点记录包含 `topic_title source_name source_url fetch_time merged_rank`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现采集批次与错误落库",
        "作为数据工程师，我希望每次采集都有 batch、error 和 lineage 落库，以便未来排障时能回答“什么时候采的、采了哪些、哪里失败了”。",
        [
            "写入 `data_batch`、`data_batch_error`、`data_batch_lineage`",
            "批次记录可追踪 `source_name trade_date started_at completed_at status`",
            "单股失败可单独记录，而不是把整批失败隐藏为无痕丢失",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现 report_data_usage 真值写入器",
        "作为数据工程师，我希望把 report_data_usage 建成研报链的真值来源，以便高级区和审计都能从同一条 usage 事实追溯。",
        [
            "`report_data_usage` 主追溯粒度固定为 `trade_date + stock_code + source_name + batch_id`",
            "`status` 只允许 `ok|stale_ok|missing|degraded`，且 `status != ok` 时 `status_reason` 必填",
            "非研报补采与正式研报都能写出可追溯 usage 记录",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现数据源熔断与 half-open 探测",
        "作为平台维护者，我希望每个外部数据源都有独立熔断与半开探测，以便上游波动时系统能保守降级而不是无穷重试。",
        [
            "单数据源连续失败 3 次后进入熔断并记录 `circuit_open_at`",
            "冷却 300 秒内不再请求该源；冷却后仅允许 1 次探测",
            "熔断打开/关闭事件会进入告警或运行日志",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现 Cookie 会话保存与健康探测",
        "作为平台维护者，我希望管理员可以保存和探测采集会话，以便热点等需要登录态的源能被受控维护。",
        [
            "`POST /api/v1/admin/cookie-session` 可保存 `provider cookie_string expires_at`",
            "`GET /api/v1/admin/cookie-session/health` 返回 `ok|fail|skipped` 与 `status_reason`",
            "探活支持并发互斥，避免双节点风暴",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现股票池打分与候选过滤",
        "作为数据工程师，我希望股票池规则先把不合格股票过滤掉再排序打分，以便核心池的 200 只来自可解释的统一规则。",
        [
            "股票池规则包含市值、上市时长、成交额、行业约束、ST/跌停/停牌过滤",
            "输出 `score` 并可排序形成 core/standby 候选",
            "规则执行不依赖手工环境变量列表",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现核心池刷新与 Top200 固定输出",
        "作为平台维护者，我希望系统每天输出固定 200 只核心池和候补池，以便采集、研报和首页口径统一。",
        [
            "每个交易日产出精确 `200` 只 `core_pool` 和 `Top201~250` `standby_pool`",
            "刷新输出 `pool_date pool_version core_pool_size standby_pool_size`",
            "`/api/v1/admin/pool/refresh` 与 `/api/v1/pool/stocks` 返回一致数据",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现股票池回退、淘汰记录与并发锁",
        "作为平台维护者，我希望股票池刷新在失败或并发场景下有确定性行为，以便不会出现双版本和无来源的池子结果。",
        [
            "刷新失败时回退到上一交易日池并写 `fallback_from`",
            "本次淘汰股票写入 `evicted_stocks`",
            "同一 `trade_date` 并发刷新只允许一个成功，另一个返回 `409/429`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现市场状态缓存与降级语义",
        "作为研报生成链，我希望市场状态由统一缓存服务产出，并对降级原因做标准表达，以便 FR-06/FR-10 共享同一市场语义。",
        [
            "基于 `kline_daily + hotspot` 计算 `BULL|BEAR|NEUTRAL`",
            "写入 `market_state_cache` 并保存 `reference_date state_reason market_state_degraded`",
            "计算失败时显式返回降级状态，而不是静默 `ok`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现调度任务注册与状态表",
        "作为平台维护者，我希望先看到 daily chain 各节点的注册与状态，以便后续 DAG、锁和恢复逻辑都有可观测基础。",
        [
            "支持 FR-01/04/05/06/07/08/09-b 对应的可观测任务注册",
            "任务状态至少覆盖 `NOT_RUN|RUNNING|SUCCESS|FAILED|SKIPPED`",
            "`/api/v1/admin/scheduler/status` 可分页查看任务状态",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现 DAG 依赖驱动与分布式互斥",
        "作为平台维护者，我希望 daily chain 由完成事件驱动并具备互斥锁，以便多实例和重启场景下仍然只有一条真实执行路径。",
        [
            "FR-06 等待 FR-04 完成事件，FR-07/08 等待 FR-06 批次完成事件",
            "锁包含 TTL、续约心跳与 fencing token",
            "多实例下同一任务同一交易日只能有一个执行者",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现跨日兜底超时与任务续跑",
        "作为平台维护者，我希望卡死任务可以在次日开盘前被统一终止，并且服务重启后仍能恢复真实未完成任务。",
        [
            "支持 `config.dag_cascade_timeout_before_open` 作为跨日停止条件",
            "上游长期无响应时，当前节点及下游统一标记 `upstream_timeout_next_open`",
            "服务重启后可恢复未完成任务，并将超窗僵尸任务转 `Expired`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现研报任务提交与 idempotency_key 规则",
        "作为调用方，我希望公开生成接口有明确入参和幂等语义，以便单股单日不会被重复写出多份正式研报。",
        [
            "`POST /api/v1/reports/generate` 支持 `stock_code trade_date skip_pool_check`",
            "同一 `stock_code + trade_date` 不重复生成正式研报，重复请求返回既有结果或幂等结果",
            "非核心池股票在 `skip_pool_check=false` 时被明确拒绝",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现研报前置的 non-report 补采",
        "作为研报链，我希望在正式生成前补齐非研报数据，以便报告构建所需的真值层不依赖偶然存在的旧缓存。",
        [
            "生成单股研报前自动补齐 `market_state_input capital_usage stock_profile northbound_summary etf_flow_summary` 等非研报数据",
            "补采失败写入 truth-layer 缺口，而不是跳过记录",
            "正式研报与非研报补采的数据口径保持一致",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现 strategy_type 判定与 prior_stats 注入",
        "作为研报链，我希望在调用 LLM 前拿到策略类型和历史先验统计，以便模型基于当前市场事实和历史表现输出结论。",
        [
            "基于热点/行情规则判定 `A|B|C` 策略类型",
            "从历史 `settlement_result` 生成 `prior_stats_snapshot`",
            "先验只读取历史已结算快照，不阻塞当日主链",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现 LLM 路由与多级降级链",
        "作为平台维护者，我希望 LLM 调用链具备可观测的多级降级能力，以便 provider 波动时系统能诚实失败而不是假成功。",
        [
            "LLM 调用链支持主 Web API → 备 Web API → CLI → 本地 Ollama 的降级顺序",
            "每次生成记录 `llm_provider_name llm_actual_model llm_fallback_level`",
            "Provider 全失败时进入 fail-close，不得发布伪完成研报",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现研报输入/输出质量门",
        "作为平台维护者，我希望研报在发布前同时通过输入门和输出门，以便最终公开内容满足真实性、结构完整性和 grounding 要求。",
        [
            "输入门最少校验 `kline_daily` + 外部证据存在",
            "输出门校验结构化字段、grounding、核心结论字段、公开 payload 完整性",
            "任何质量门失败都显式写 `quality_flag/status_reason/publish_status`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现研报持久化、证据链与只读保护",
        "作为平台维护者，我希望 report、citation 和 usage link 在一次受控写入中落库，并且已发布正文默认只读。",
        [
            "保存 `report`、`report_citation`、`report_data_usage_link`",
            "已发布研报内容只读；管理员只能改 `review_flag/published`",
            "同日强制重建时新旧 report 通过 `superseded_by_report_id` 关联",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现 BUY 高置信度报告的风险审计分支",
        "作为平台维护者，我希望高风险 BUY 报告先经过风险审计分支，以便“高收益结论”不会绕过防御性复核直接公开。",
        [
            "`recommendation=BUY 且 confidence>=0.65` 时必须进入辩证审计或风险复核分支",
            "产出 `risk_audit_status` 与 `risk_audit_skip_reason`",
            "未通过风险审计的 BUY 不得直接进入公开终态",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现指令卡与三挡资金执行计划",
        "作为用户，我希望每份研报都给出三挡资金的执行计划，以便我能区分不同资金规模下的真实可操作性。",
        [
            "每份研报生成 `10k|100k|500k` 三挡 `trade_instruction_by_tier`",
            "每挡输出 `status position_ratio skip_reason`",
            "Free 只对外展示 `100k`，Pro/Enterprise 可查看全部三挡",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现公开研报列表与详情查询",
        "作为普通用户，我希望列表和详情接口同时遵守公开面门禁和统一契约，以便我拿到的始终是可公开解释的数据。",
        [
            "`GET /api/v1/reports` 支持日期、推荐、市场状态、池内、分页与排序筛选",
            "`GET /api/v1/reports/{report_id}` 对未发布研报执行访问门禁",
            "列表与详情返回字段遵守 `05_API与数据契约.md`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现高级区 payload 与中文全过程展示",
        "作为付费用户，我希望高级区接口直接返回中文可读的全过程和所用数据，以便我不用猜模型到底用了什么。",
        [
            "`GET /api/v1/reports/{report_id}/advanced` 返回 `used_data_lineage prior_stats_snapshot risk_audit_status reasoning_chain`",
            "高级区必须中文展示“所用数据”和“生成全过程”",
            "Free 仅能看到后端裁剪后的摘要，Pro/Enterprise 可查看完整高级区",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现批量研报生成与 one_per_strategy_type 预筛",
        "作为平台维护者，我希望批量生成接口能按受控并发执行，并在需要时按 A/B/C 每类各选最优候选。",
        [
            "内部批量生成支持并发上限、断点恢复和每轮数量限制",
            "`one_per_strategy_type=true` 时按 A/B/C 各选最优候选",
            "批量任务状态可通过 internal task API 查询",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现不完整研报清理与过期任务修复",
        "作为平台维护者，我希望异常中断的 report bundle 和过期 task 可以被清理或转终态，以便公开面只看到真实可解释的结果。",
        [
            "能识别输入缺口、公开 payload 缺口、异常终态形成的不完整 bundle",
            "清理会软删除脏 report 并保留审计原因",
            "支持单次清理与 until-clean 循环清理",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现 FR-07 信号结算主流程",
        "作为策略评估者，我希望系统用统一规则对历史研报信号结算，以便后续胜率、盈亏比和 Alpha 建立在真实结果上。",
        [
            "基于研报预测与窗口规则写入 `settlement_result`",
            "FR-07 shares 口径固定为 100 股标准单位，不与 FR-08 混用",
            "结算记录包含 `window_days settlement_status exit_reason net_return_pct cost_breakdown`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现独立基线回灌与重建流程",
        "作为策略评估者，我希望 baseline_random 和 baseline_ma_cross 有独立生命周期，以便基线结果不会因为样本共用而变成假对照。",
        [
            "`baseline_random` 与 `baseline_ma_cross` 具有独立异步生命周期",
            "基线结果不与主策略样本混写或偷换分母",
            "支持 rebuild/replay，并保留版本与时间戳",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现 FR-07 KPI 快照与统计口径",
        "作为平台使用者，我希望所有绩效指标都能展示真实口径和样本背景，以便不会被只看一个百分比误导。",
        [
            "生成 `sample_size coverage_pct win_rate profit_loss_ratio alpha_annual max_drawdown`",
            "所有统计卡片同时展示样本数与覆盖率",
            "零收益/缺样本/降级样本的统计口径与 `01_需求基线.md` 一致",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现 BUY 信号到模拟开仓的主链",
        "作为平台使用者，我希望 BUY 信号能按真实资金条件开模拟仓，以便后续能验证“建议是否可执行”。",
        [
            "只对 `BUY` 且满足置信度/资金条件的报告开模拟仓",
            "开仓遵守资金挡位、手续费、最小 100 股整数倍约束",
            "开仓失败不回滚已发布研报，但必须显式记录原因",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现模拟平仓与悲观撮合规则",
        "作为平台使用者，我希望模拟平仓结果使用保守撮合规则，而不是理想化成交价，以便收益统计更接近真实执行。",
        [
            "支持 `TAKE_PROFIT STOP_LOSS TIMEOUT` 三类平仓终态",
            "平仓价格使用日线 pessimistic fill 规则，不得直接复用开仓价",
            "平仓后写回 `sim_pnl_gross sim_pnl_net sim_pnl_pct`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现模拟账户快照与回撤状态机",
        "作为平台使用者，我希望每个资金挡位都有独立账户快照和回撤治理，以便三挡结果不会互相污染。",
        [
            "每挡资金生成日度 `sim_account_snapshot`",
            "维护 `NORMAL REDUCE HALT` 回撤状态机与 `sim_max_positions`",
            "回撤进入 `REDUCE/HALT` 时限制新开仓",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现模拟仓与账户查询 API",
        "作为付费用户，我希望通过 API 和看板查看持仓、账户快照和绩效摘要，以便知道当前模拟盘运行情况。",
        [
            "`GET /api/v1/sim/positions`、`.../{position_id}`、`.../by-report/{report_id}`、`GET /api/v1/sim/account/snapshots` 可按契约返回数据",
            "`GET /api/v1/portfolio/sim-dashboard` 返回绩效曲线、持仓摘要与 drawdown 状态",
            "Free 对 `sim-dashboard` 仅允许 `100k` 挡位，admin 可豁免",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现 POSITION_CLOSED 与 DRAWDOWN_ALERT 的 Outbox 事件",
        "作为平台维护者，我希望平仓与回撤事件先入 outbox 再外发，以便即使推送失败也不会破坏 FR-08 的资金事实。",
        [
            "平仓与回撤事件先落 outbox，再在事务提交后外发",
            "相同幂等键重复触发时状态为 `skipped`",
            "推送失败不阻塞 FR-08 资金结算主链",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现用户密码、角色与 Token Claim 模型",
        "作为平台维护者，我希望先冻结鉴权模型和 token claim，以便后续登录、刷新和 RBAC 链都建立在安全基础上。",
        [
            "密码使用 bcrypt/argon2 强哈希存储",
            "access token claims 至少包含 `sub role tier exp jti`",
            "禁止在日志中输出明文密码、cookie_session、access_token",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现注册与登录接口",
        "作为访客，我希望完成注册并登录到系统，以便进入会员能力和个人中心。",
        [
            "`/auth/register` 创建未激活账户并返回一致消息",
            "`/auth/login` 成功时返回 token/cookie，并对未激活账户给出明确拒绝",
            "注册与登录返回结构遵守统一 envelope 契约",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现登出、激活与当前用户接口",
        "作为用户，我希望能激活账户、查看当前身份并安全登出，以便浏览器态和服务端态保持一致。",
        [
            "`/auth/activate` 激活成功后形成可登录账户",
            "`/auth/me` 返回当前角色、tier、permissions 和时间字段",
            "`/auth/logout` 清除浏览器登录态并使当前会话失效",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现 refresh 轮换、重置密码与 token 撤销",
        "作为平台维护者，我希望 refresh token 具备轮换和撤销能力，以便重置密码或登出后旧令牌无法继续使用。",
        [
            "refresh token 使用一次即轮换旧 token",
            "重置密码会使旧 refresh 失效并将旧 access jti 写入黑名单",
            "撤销后的 token 继续访问时返回 401",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现 OAuth provider 列表、发起与回调链",
        "作为用户，我希望使用 QQ/微信完成登录授权，以便减少单独注册的阻力。",
        [
            "提供 QQ/微信 provider 列表与可用性状态",
            "start/callback 支持标准授权码流程或真实 Mock 回路",
            "provider 缺邮箱时执行项目约定的 fallback 绑定策略",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现订单创建接口",
        "作为付费用户，我希望先创建一笔可支付订单，以便后续支付渠道和页面能进入真实下单流程。",
        [
            "`POST /api/v1/billing/orders` 创建订单并返回支付参数或支付地址",
            "订单记录至少包含 `provider amount status order_id`",
            "未登录或 provider 未配置时返回明确错误码",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现支付 Webhook 幂等与权益发放",
        "作为平台维护者，我希望支付回调与权益发放在同一原子事务中完成，以便不会出现“到账未发权益”或“重复发权益”。",
        [
            "`POST /api/v1/billing/webhook` 以 `event_id` 幂等处理重复回调",
            "订单确认与权益发放在同一原子事务中提交",
            "签名错误、重复回调、provider 未配置时返回明确错误码",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现管理员补单与付费面权限门控",
        "作为管理员和付费用户，我希望后台补单与前台权限门控使用同一套权益事实，以便到账、展示和升级行为保持一致。",
        [
            "`POST /api/v1/admin/billing/orders/{order_id}/reconcile` 幂等补单",
            "会员权益统一控制高级区、历史研报范围、sim-dashboard 挡位可见性",
            "`/membership/subscription/status` 与页面权限判断口径一致",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现首页聚合与精选研报区块",
        "作为访客，我希望首页能直接看到市场状态和精选研报，以便快速判断平台今天有没有可看内容。",
        [
            "`/api/v1/home` 返回市场状态、pool_size、today_report_count、featured_reports、latest_reports",
            "首页在无研报或降级时提供诚实空态/降级态说明",
            "首页模板展示的关键数字与 API 聚合一致",
            "Typecheck passes",
            "Tests pass",
            "Verify in browser using dev-browser skill",
        ],
    ),
    (
        "实现股票搜索、自动补全、热点与快照 API",
        "作为用户，我希望能搜股票、看热点、看基础快照，以便在进入正式研报前先做基础判断。",
        [
            "`/api/v1/stocks`、`/stocks/autocomplete`、`/market/hotspots`、`/stocks/{stock_code}/snapshot` 可按契约返回数据",
            "详情页快照至少包含 `market_snapshot/hotspot/data_sources` 等核心区块",
            "热点为空时返回诚实的 `missing/degraded` 说明，不输出伪热点",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现研报列表页与详情页 HTML 入口",
        "作为普通用户，我希望从 HTML 页面直接浏览列表与详情，而不是只能靠 API 看 JSON。",
        [
            "`/reports` 与 `/reports/{report_id}` 渲染真实列表和详情块，而不是占位页",
            "详情页展示结论、指令卡、证据摘要、风险提示、数据质量横幅",
            "未发布或无权访问时返回真实的错误页或重定向语义",
            "Typecheck passes",
            "Tests pass",
            "Verify in browser using dev-browser skill",
        ],
    ),
    (
        "实现高级区访问控制与摘要裁剪页面逻辑",
        "作为平台用户，我希望高级区在不同 tier 下看到不同深度内容，而且差异是可解释的，不是空白页。",
        [
            "Free 用户高级区摘要在后端裁剪，长度不超过项目约定阈值",
            "Pro/Enterprise 可查看完整 reasoning_chain 与 used_data_lineage",
            "页面明确标识“已裁剪/需升级”而不是空白或 500",
            "Typecheck passes",
            "Tests pass",
            "Verify in browser using dev-browser skill",
        ],
    ),
    (
        "实现 Dashboard、股票池页与公开市场概览",
        "作为普通用户和付费用户，我希望页面能展示统计、池子和市场概览，并对未就绪状态给出解释。",
        [
            "`/api/v1/dashboard/stats`、`/api/v1/pool/stocks`、`/api/v1/market-overview` 返回可解释状态",
            "dashboard 页面同时展示研报统计、FR-07 指标、FR-08 资金曲线与 `data_status`",
            "当统计未就绪时显示 `COMPUTING/DEGRADED` 与 `status_reason`",
            "Typecheck passes",
            "Tests pass",
            "Verify in browser using dev-browser skill",
        ],
    ),
    (
        "实现登录、注册、Profile 与订阅页面流程",
        "作为用户，我希望浏览器页面和真实接口行为完全一致，以便注册、登录、查看权益和订阅升级是可用链路。",
        [
            "`/login`、`/register`、`/profile`、`/subscribe` 页面与真实接口结果一致",
            "已登录用户访问登录/注册页时按规则重定向",
            "支付不可用或未配置时，订阅页明确显示当前能力状态",
            "Typecheck passes",
            "Tests pass",
            "Verify in browser using dev-browser skill",
        ],
    ),
    (
        "实现反馈提交、防刷与复审阈值",
        "作为用户和管理员，我希望坏研报能通过反馈机制进入复审链路，同时避免被重复提交刷坏数据。",
        [
            "`POST /api/v1/reports/{report_id}/feedback` 只接受声明字段并去重",
            "同用户 negative 反馈达到阈值时将 `review_flag` 置为 `PENDING_REVIEW`",
            "当日超频反馈返回 429 且不落库",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现管理员概览与用户管理 API",
        "作为管理员，我希望先能看总览并管理用户角色/tier，以便运营与权限调整有真实后台入口。",
        [
            "`/api/v1/admin/overview` 返回 pool/reports/pending_review/active_positions/scheduler 概览",
            "`/api/v1/admin/users` 支持分页、筛选与必要字段，`PATCH /api/v1/admin/users/{user_id}` 可修改 `tier/role`",
            "所有管理写操作写入 `actor before after timestamp request_id` 审计记录",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现管理员研报管理与强制重建保护",
        "作为管理员和 super_admin，我希望后台可以审核、下架、筛选待复审研报，并在满足条件时执行受控重建。",
        [
            "`/api/v1/admin/reports` 支持分页与 `review_flag` 筛选，`PATCH /api/v1/admin/reports/{report_id}` 仅允许改 `review_flag/published`",
            "`POST /api/v1/admin/reports/{report_id}/force-regenerate` 仅 super_admin 可用",
            "目标研报已被 sim/settlement 引用时返回 `409 REPORT_ALREADY_REFERENCED_BY_SIM`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现 BUY_SIGNAL_DAILY 与 REPORT_PENDING_REVIEW 推送",
        "作为管理员，我希望当日 BUY 强信号和待复审事件能及时推送到管理员通道，以便快速跟进。",
        [
            "FR-06 批次完成后仅在存在 BUY 强信号时发送汇总事件",
            "反馈首次触发 `PENDING_REVIEW` 时发送管理员告警",
            "相同去重维度重复触发时记录 `skipped`",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现清理归档任务与永久保留域",
        "作为平台维护者，我希望临时脏状态可以定期清理，但正式事实永久保留，以便系统既能自愈又不破坏真实历史。",
        [
            "定时清理未激活账户、过期 pending/suspended task、临时 token 等非正式数据",
            "正式研报、证据源、历史日线、模拟持仓与结算记录永不被清理",
            "清理任务不阻塞当日研报主链",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
    (
        "实现治理目录、运行时门禁与内部诊断接口",
        "作为平台维护者，我希望系统提供治理目录、runtime gates 和内部诊断接口，以便运维、审计与文档契约核对有真实入口。",
        [
            "提供 governance catalog、runtime gates、audit context、关键 internal health 端点",
            "契约 smoke 信息可对齐 `docs/core/05_API与数据契约.md` 的主要 API 与枚举",
            "诊断接口只暴露治理状态，不泄露敏感令牌或 cookie 明文",
            "Typecheck passes",
            "Tests pass",
        ],
    ),
]

stories = []
for idx, (title, description, criteria) in enumerate(stories_raw, start=1):
    stories.append(
        {
            "id": f"US-{idx:03d}",
            "title": title,
            "description": description,
            "acceptanceCriteria": criteria,
            "priority": idx,
            "passes": False,
            "notes": "",
        }
    )

payload = {
    "project": "yanbao-new",
    "branchName": new_branch,
    "description": "Build a truth-first A-share individual-stock research platform with core-pool selection, multisource ingest, explainable report generation, settlement, three-tier simulation, membership, admin, and notification workflows.",
    "userStories": stories,
}

new_prd.write_text(markdown, encoding="utf-8")
runtime_prd.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
named_prd.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
last_branch_file.write_text(new_branch + "\n", encoding="utf-8")

if config_path.exists():
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["markdownPrd"] = "docs/core/27_PRD_研报平台增强与整体验收基线.md"
    config.setdefault("branchNamePolicy", {})["currentValue"] = new_branch
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

print(
    json.dumps(
        {
            "markdownPrd": str(new_prd),
            "runtimePrd": str(runtime_prd),
            "namedPrd": str(named_prd),
            "storyCount": len(stories),
            "branchName": new_branch,
        },
        ensure_ascii=False,
        indent=2,
    )
)
