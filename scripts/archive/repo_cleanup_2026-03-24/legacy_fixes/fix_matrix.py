import sys
sys.stdout.reconfigure(encoding='utf-8')

path = 'd:/yanbao/docs/core/01_需求基线.md'
content = open(path, encoding='utf-8').read()

old = """### 1.3 权益能力矩阵（Free / Pro / Enterprise）

> 若产品尚未定稿，可标「待定义」；最终以 FR-09 与 05_API 为准。

| 能力 | Free | Pro | Enterprise |
|------|------|-----|------------|
| 研报结论与实操指令 | ✓ | ✓ | ✓ |
| 高级区推理链 | 摘要/隐藏 | 完整 | 完整 |
| 历史研报范围 | 近 7 天 | 近 90 天 | 不限 |"""

new = """### 1.3 权益能力矩阵（Free / Pro / Enterprise）

> 最终以 FR-09 与 05_API 为准。

| 能力 | Free | Pro | Enterprise |
|------|------|-----|------------|
| 研报结论与实操指令卡 | ✓ | ✓ | ✓ |
| 高级区推理链（evidence_items + analysis_steps） | 摘要（前 3 条证据）/其余隐藏 | 完整展示 | 完整展示 |
| 历史研报查看范围 | 近 7 天 | 近 90 天 | 不限（全历史） |
| 模拟收益看板（/portfolio/sim-dashboard） | 仅总体净值曲线 | 完整（含 A/B/C 分类绩效） | 完整 + 导出数据 |
| 用户反馈功能（FR-11） | 不可用（按钮 disabled） | ✓ | ✓ |
| 研报筛选维度（FR-10） | 仅按日期/股票代码 | 全部 7 种筛选维度 | 全部 7 种筛选维度 |
| 绩效统计（四维度） | 不显示 | 显示（样本≥30 时） | 显示（样本≥30 时） |
| 每日研报推送（计划中） | ✗ | ✗（待定义） | ✓（待定义） |
| API 访问（待定义） | ✗ | ✗ | 待定义 |
| Pro 与 Enterprise 区别 | — | — | Enterprise 额外支持数据导出；后续可扩展 API 接入、专属客服 |"""

if old in content:
    content = content.replace(old, new)
    open(path, 'w', encoding='utf-8').write(content)
    print('OK: 权益矩阵已替换')
else:
    print('ERROR: 未找到原文')
