import sys
sys.stdout.reconfigure(encoding='utf-8')

path = 'd:/yanbao/docs/core/01_需求基线.md'
content = open(path, encoding='utf-8').read()

old = """### FR-08 模拟实盘追踪

| 维度 | 内容 |
|------|------|
| **功能域** | 模拟持仓与结算 |
| **上下游** | 上游：FR-06（BUY 强信号 `sim_trade_instruction`）；下游：FR-10 |
| **流程** | 研报发布（强信号）→ 自动开仓 → 每日结算 → 止损/止盈/超时 → 平仓 |
| **触发** | BUY 强信号发布后自动开仓；定时任务每日结算 |
| **参数** | `sim_initial_capital`（默认 100 万）；`sim_position_ratio`（10%）；`sim_max_positions`（10 只） |
| **输入** | `sim_trade_instruction`；`report_id`、`stock_code`、`signal_date` |
| **输出** | `sim_position`；`sim_account`（净值、drawdown_state）；研报页：持有中/已止盈/已止损/已超时 |
| **边界** | max_drawdown_pct ≤ -12% → REDUCE，仓位×0.5；≤ -20% → HALT，停止新开仓；涨停次日 → `execution_blocked`；停牌 → 复牌次日开盘价（来源 FR-04 `is_suspended`）；持仓达上限 → 不开仓，`skipped_reason=MAX_POSITIONS_REACHED` |
| **验收** | pytest 断言 drawdown≤-20% 时不新开仓；E2E 开仓→平仓链路；持仓超上限不新开仓 |
| **优先级** | P0 |
| **相关** | 05_API、04_数据治理 |"""

new = """### FR-08 模拟实盘追踪

| 维度 | 内容 |
|------|------|
| **功能域** | 模拟持仓与结算 |
| **上下游** | 上游：FR-06（BUY 强信号 `sim_trade_instruction`）；下游：FR-10 |
| **流程** | 研报发布（强信号）→ 自动开仓 → 每日结算（15:30 定时）→ 止损/止盈/超时 → 平仓 |
| **触发** | BUY 强信号发布后自动开仓；定时任务每日 15:30 结算 |
| **参数** | `sim_initial_capital`（默认 100 万元人民币）；`sim_position_ratio`（基础仓位比例，默认 10%）；`sim_max_positions`（最大持仓数，默认 10 只） |
| **开仓金额公式** | `actual_investment = sim_initial_capital × sim_position_ratio × drawdown_state_factor`；单只股票实际开仓金额不超过 `sim_initial_capital × sim_position_ratio` |
| **drawdown_state 枚举** | **NORMAL**：账户 max_drawdown_pct > -12%，factor=1.0，允许新开仓；**REDUCE**：-20% < max_drawdown_pct ≤ -12%，factor=0.5，允许新开仓（仓位减半）；**HALT**：max_drawdown_pct ≤ -20%，factor=0（不开新仓），`skipped_reason=DRAWDOWN_HALT`；状态依据每日结算后账户净值与历史峰值计算 |
| **输入** | `sim_trade_instruction`（含 entry_price、stop_loss、target_price、max_hold_days）；`report_id`；`stock_code`；`signal_date` |
| **输出** | `sim_position`（持仓记录：open_price、shares、stop_loss、target_price、hold_days、status）；`sim_account`（净值 net_value、max_drawdown_pct、drawdown_state）；持仓状态枚举：HOLDING / TAKE_PROFIT / STOP_LOSS / TIMEOUT / EXECUTION_BLOCKED |
| **止盈触发** | 每日收盘（15:00）后结算：当日收盘价 ≥ target_price → 标记 TAKE_PROFIT，以当日收盘价平仓 |
| **止损触发** | 每日收盘（15:00）后结算：当日收盘价 ≤ stop_loss → 标记 STOP_LOSS，以**次日开盘价**平仓（模拟 T+1 市价卖出，避免 T+0 限制） |
| **超时平仓** | 持仓交易日数达到 max_hold_days（A 类 T+2、B 类 T+3、C 类 T+5）→ 标记 TIMEOUT，以**到期日收盘价**强制平仓 |
| **边界** | 涨停次日 → `status=EXECUTION_BLOCKED`，延迟至下一非涨停交易日执行（最多延迟 3 日，超过则强制以当日开盘价成交）；停牌 → 复牌次日开盘价平仓（来源 FR-04 `is_suspended`）；持仓达上限 → 不开仓，`skipped_reason=MAX_POSITIONS_REACHED` |
| **验收** | `test_fr08_halt_no_new_position`：drawdown≤-20% 时不开新仓；`test_fr08_take_profit`：收盘价≥target_price 时状态=TAKE_PROFIT；`test_fr08_stop_loss_next_open`：止损以次日开盘价结算；`test_fr08_timeout`：持仓天数超限时状态=TIMEOUT；`test_fr08_max_positions`：持仓 10 只后不开新仓 |
| **优先级** | P0 |
| **相关** | 05_API、04_数据治理 |"""

if old in content:
    content = content.replace(old, new)
    open(path, 'w', encoding='utf-8').write(content)
    print('OK: FR-08 已替换')
else:
    print('ERROR: 未找到原文')
