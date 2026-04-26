import sys
sys.stdout.reconfigure(encoding='utf-8')

path = 'd:/yanbao/docs/core/01_需求基线.md'
content = open(path, encoding='utf-8').read()

old = """### FR-05 市场状态机

| 维度 | 内容 |
|------|------|
| **功能域** | 市场状态判定与缓存 |
| **上下游** | 上游：FR-04；下游：FR-06（BEAR 时 B/C filtered_out）、FR-02（09:00 触发） |
| **流程** | 拉取上证指数、沪深300、量能比等 → 按规则判定 → 写入缓存（TTL 至次日 09:00） |
| **触发** | 每日 09:00 定时任务 / GET /api/v1/market/state |
| **输入** | 上证指数、沪深300、量能比等 |
| **输出** | `market_state` 枚举 BULL / NEUTRAL / BEAR；`state_reason?`；`a_type_pct?` / `b_type_pct?` / `c_type_pct?` |
| **判定规则** | BULL：MA20 向上 + 沪深300 近 20 日收益 > 3%；BEAR：MA5 < MA20 + 收益 < -5%；NEUTRAL：其余 |
| **边界** | 获取失败 → `market_state=NEUTRAL`，`state_reason` 必填；缓存 TTL 至次日 09:00 |
| **验收** | pytest 断言 `market_state in ("BULL","NEUTRAL","BEAR")`；降级时 `state_reason` 非空 |
| **优先级** | P1 |
| **相关** | 05_API、04_数据治理 |"""

new = """### FR-05 市场状态机

| 维度 | 内容 |
|------|------|
| **功能域** | 市场状态判定与缓存 |
| **上下游** | 上游：FR-04；下游：FR-06（BEAR 时 B/C filtered_out）、FR-02（09:00 触发） |
| **流程** | 拉取沪深300后复权收盘价序列 → 计算 MA5/MA20 及近 20 交易日收益 → 按规则判定 → 写入缓存（TTL 至次日 09:00）→ 写入历史记录表 |
| **触发** | 每日 09:00 定时任务 / GET /api/v1/market/state |
| **输入** | 沪深300（000300.SH）近 25 个交易日后复权收盘价序列（来自 FR-04） |
| **计算规则** | 所有 MA 均基于**交易日**后复权收盘价；MA5 = 最近 5 交易日均值；MA20 = 最近 20 交易日均值；**MA20 向上** = 今日 MA20 > 5 交易日前 MA20（非单日涨跌）；**近 20 交易日收益** = (今日收盘价 / 20 交易日前收盘价 − 1) × 100% |
| **判定规则** | **BULL**（AND 关系，两条均满足）：MA20 向上 **且** 近 20 交易日收益 > 3%；**BEAR**（AND 关系，两条均满足）：MA5 < MA20 **且** 近 20 交易日收益 < −5%；**NEUTRAL**：不满足 BULL 也不满足 BEAR 的其余情况 |
| **输出** | `market_state` 枚举 BULL / NEUTRAL / BEAR；`state_reason: str`（非空，说明判定依据）；`ma5: float`；`ma20: float`；`return_20d_pct: float`；`calculated_at: str` |
| **持久化** | 每次判定结果写入 `market_state_history` 表（供趋势分析），缓存 TTL 至次日 09:00 |
| **边界** | 数据获取失败或序列不足 25 日 → `market_state=NEUTRAL`，`state_reason` 必填说明降级原因；缓存 TTL 至次日 09:00 |
| **验收** | `test_fr05_market_state_enum`：market_state ∈ {BULL,NEUTRAL,BEAR}；`test_fr05_bull_and_logic`：Mock MA20 向上但收益≤3% → 结果非 BULL；`test_fr05_degraded_reason`：数据失败时 state_reason 非空；`test_fr05_neutral_fallback`：数据不足时返回 NEUTRAL |
| **优先级** | P1 |
| **相关** | 05_API、04_数据治理 |"""

if old in content:
    content = content.replace(old, new)
    open(path, 'w', encoding='utf-8').write(content)
    print('OK: FR-05 已替换')
else:
    print('ERROR: 未找到原文')
