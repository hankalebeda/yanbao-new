import sys
sys.stdout.reconfigure(encoding='utf-8')

path = 'd:/yanbao/docs/core/01_需求基线.md'
content = open(path, encoding='utf-8').read()

old = """### FR-07 预测结算与回灌

| 维度 | 内容 |
|------|------|
| **功能域** | 预测结果结算与绩效统计 |
| **上下游** | 上游：FR-06；下游：FR-10 |
| **触发** | 定时任务（每日）/ 人工查询 |
| **输入** | 已发布研报；`trade_date`；结算窗口（1/7/14/30/60 日） |
| **输出** | 1/7/14/30/60 日结算结果；按 A/B/C 四维度绩效：真实胜率、盈亏比、年化 Alpha |
| **边界** | 样本<30 笔 → 不展示实盘胜率，展示「样本积累中」；口径：A/B/C 各自独立统计，全历史；行情缺失 → 跳过该日，`settlement_status=pending`；停牌 → 复牌次日以开盘价结算；数据延迟 → 显式标记，不静默填充 |
| **验收** | pytest 断言错误分类可追溯率=100%；样本≥30 时胜率/盈亏比字段存在 |
| **优先级** | P0 |
| **相关** | 05_API、04_数据治理 |"""

new = """### FR-07 预测结算与回灌

| 维度 | 内容 |
|------|------|
| **功能域** | 预测结果结算与绩效统计 |
| **上下游** | 上游：FR-06；下游：FR-10 |
| **触发** | 定时任务（每日 15:30，模拟结算后）/ 人工查询 |
| **输入** | 已发布研报；`trade_date`；行情收盘价（来自 FR-04） |
| **结算口径（按 recommendation 分类）** | **BUY**：通过 FR-08 模拟实盘追踪，平仓后计入四维度绩效统计（真实胜率、盈亏比、年化 Alpha）；**SELL**：不进入模拟实盘，仅统计「方向命中率」（T+5 收盘价低于发布日收盘价则算命中），方向命中率仅作运营监控，不计入四维度绩效；**HOLD**：不进入模拟实盘，不纳入任何绩效统计 |
| **四维度绩效** | 仅基于 BUY 推荐的 FR-08 模拟实盘结算结果计算；**真实胜率** = 盈利平仓笔数 / 总平仓笔数；**盈亏比** = 平均盈利幅度 / 平均亏损幅度；**年化 Alpha** = 年化收益率 − 同期沪深300年化收益率；A/B/C 各自独立统计，全历史口径 |
| **输出** | `settlement_status` 枚举 done / pending / skipped；`win_rate: float?`；`profit_loss_ratio: float?`；`annual_alpha_pct: float?`；`sample_count: int`；`sell_direction_hit_rate: float?`（监控用） |
| **边界** | 样本 < 30 笔 → 不展示四维度绩效，前台显示「样本积累中（当前 N 笔）」；行情缺失 → `settlement_status=pending`，次日重试；停牌股票 → 复牌次日以开盘价结算；数据延迟 → 显式标记 `data_delayed=True`，不静默填充 |
| **验收** | `test_fr07_buy_only_in_perf`：SELL/HOLD 研报不影响四维度绩效字段；`test_fr07_sample_threshold`：样本 < 30 时 win_rate 为 null；`test_fr07_settlement_traceable`：每笔结算有 report_id 可追溯；`test_fr07_pending_on_missing`：行情缺失时 status=pending |
| **优先级** | P0 |
| **相关** | 05_API、04_数据治理 |"""

if old in content:
    content = content.replace(old, new)
    open(path, 'w', encoding='utf-8').write(content)
    print('OK: FR-07 已替换')
else:
    print('ERROR: 未找到原文')
