import sys
sys.stdout.reconfigure(encoding='utf-8')

path = 'd:/yanbao/docs/core/01_需求基线.md'
content = open(path, encoding='utf-8').read()

old = """### FR-06 研报生成

| 维度 | 内容 |
|------|------|
| **功能域** | 模型生成研报 |
| **上下游** | 上游：FR-04、FR-01、FR-05（BEAR 时 B/C filtered_out）；下游：FR-07、FR-08、FR-10 |
| **流程** | 获取数据 → LLM 推理 → 规则校验 → 发布 →（BUY 强信号）触发 FR-08 开仓 |
| **触发** | POST /api/v1/reports/generate 或定时任务 |
| **输入** | `stock_code: str`（如 600519.SH）；`trade_date: str`（默认当日） |
| **输出** | `report_id`；`recommendation` 枚举 BUY/SELL/HOLD；`confidence: float`；`citations`（非空）；`idempotency_key`（daily:{stock_code}:{trade_date}）；`strategy_type` 枚举 A/B/C；BUY 且 confidence≥0.65 时 `sim_trade_instruction`（实操指令卡） |
| **LLM 降级** | 主 API → 备 API → CLI → 本地模型；全链路失败 `error_code=LLM_ALL_FAILED` |
| **边界** | 同一 idempotency_key 重复 → 返回已有 report_id；LLM 不可用 → 显式 `llm_fallback=True`；BUY 且 confidence<0.65 → 不生成实操指令卡、不触发开仓；BEAR 下 B/C 类 `published=False` |
| **信号类型 A/B/C** | A=事件驱动；B=趋势跟踪；C=低波套利；A 类 T+2、B T+3、C T+5；止损倍数 A×1.5、B×2.0、C×2.5 |
| **实操指令卡** | `entry_price`=当日收盘价；`stop_loss`=entry× (1 − ATR_pct×止损倍数)；ATR 不可用时 0.92；`target_price`=entry× (1 + 止损幅度×1.5)；`position_ratio`=基础仓位×drawdown_state 系数 |
| **研报内容** | 结论区（白话版）、实操指令卡、推理链（高级区）、证据→分析→结论可追溯 |
| **验收** | pytest 断言 recommendation 在枚举内、citations 非空、幂等返回相同 report_id；人工检查白话表述与推理链 |
| **优先级** | P0 |
| **相关** | 05_API、04_数据治理、03 §10 研报优化链路 |"""

new = """### FR-06 研报生成

| 维度 | 内容 |
|------|------|
| **功能域** | 模型生成研报 |
| **上下游** | 上游：FR-04、FR-01、FR-05（BEAR 时 B/C filtered_out）；下游：FR-07、FR-08、FR-10 |
| **流程** | 获取数据 → LLM 推理 → 规则校验 → 发布 →（BUY 强信号）触发 FR-08 开仓 |
| **触发** | POST /api/v1/reports/generate 或定时任务 |
| **输入** | `stock_code: str`（如 600519.SH）；`trade_date: str`（默认当日） |
| **输出** | `report_id`；`recommendation` 枚举 BUY/SELL/HOLD；`confidence: float (0.0~1.0)`；`citations`（非空）；`idempotency_key`（daily:{stock_code}:{trade_date}）；`strategy_type` 枚举 A/B/C；BUY 且 confidence≥0.65 时 `sim_trade_instruction`（实操指令卡） |
| **strategy_type 判定** | 由 LLM 根据证据特征判定并在输出 JSON 中返回，判定指引（写入 Prompt）：**A（事件驱动）**：存在重大事件催化（如重要公告、政策利好/利空、行业新闻）；**B（趋势跟踪）**：无重大事件，技术面趋势明确（均线排列、量价配合）；**C（低波套利）**：无重大事件，价格区间震荡、波动率低。LLM 返回结果若不在枚举内 → 默认 A 类；strategy_type 一经发布不可更改 |
| **BEAR 下信号过滤规则** | B 类、C 类：`published=False`，`filtered_reason="BEAR_MARKET_FILTER"`，不触发开仓；**A 类**：仍然发布（事件驱动不受趋势限制），但研报中强制追加 `market_state_warning="当前市场处于熊市状态，本信号为事件驱动型，请注意市场整体风险"` |
| **LLM 降级** | 主 API → 备 API → CLI → 本地模型；全链路失败 `error_code=LLM_ALL_FAILED` |
| **边界** | 同一 idempotency_key 重复 → 返回已有 report_id；LLM 不可用 → 显式 `llm_fallback=True`；BUY 且 confidence<0.65 → 不生成实操指令卡、不触发开仓 |
| **信号类型参数** | A 类：持仓期限 T+2，止损倍数×1.5；B 类：持仓期限 T+3，止损倍数×2.0；C 类：持仓期限 T+5，止损倍数×2.5 |
| **实操指令卡** | `entry_price`=当日收盘价；`stop_loss`=entry×(1−ATR_pct×止损倍数)，ATR 不可用时固定止损率 8%（即×0.92）；`target_price`=entry×(1+止损幅度×1.5)；`max_hold_days`=策略类型对应 T+N |
| **研报内容** | 结论区（白话版）、实操指令卡、推理链（高级区）、证据→分析→结论可追溯 |
| **验收** | `test_fr06_recommendation_enum`：recommendation ∈ {BUY,SELL,HOLD}；`test_fr06_strategy_type_enum`：strategy_type ∈ {A,B,C}；`test_fr06_citations_nonempty`：citations 非空且每条含三要素；`test_fr06_idempotency`：同 key 重复调用返回相同 report_id；`test_fr06_bear_bc_not_published`：BEAR 下 B/C 类 published=False；`test_fr06_bear_a_published`：BEAR 下 A 类 published=True 且含 market_state_warning |
| **优先级** | P0 |
| **相关** | 05_API、04_数据治理、03 §10 研报优化链路 |"""

if old in content:
    content = content.replace(old, new)
    open(path, 'w', encoding='utf-8').write(content)
    print('OK: FR-06 已替换')
else:
    print('ERROR: 未找到原文')
