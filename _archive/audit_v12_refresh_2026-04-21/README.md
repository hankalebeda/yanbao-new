# Audit v12.1 Refresh — 2026-04-21

本目录是 `docs/core/22_全量功能进度总表_v12.md` v12.1 增量复测的取证证据。

## 文件清单

| 文件 | 内容 |
|:---|:---|
| `db_snapshot.json` | SQLite 真相快照（`d:\yanbao-new\data\app.db`，125 MB），4-21 13:00 时点 |
| `app.db.bak_before_cleanup` | 4-21 16:00 软删 12 条 alive 前的 DB 完整备份（125 MB） |
| `http_probe.json` | 首轮探针（错前缀 `/api/*`），25 端点，全 404 → 证明 v23 老结论错误 |
| `http_probe_v2.json` | 修正后探针（`/api/v1/*`），41 端点，19/200 + 21/401 + 1/410 → §0.7 事实源 |
| `README.md` | 本文件 |

## 关键事实（一行总结）

DB 已被重置（report 总数 2592 → 36）+ 已清理污染（alive 16 → 4，备份存档）；4-21 业务流水线已重启（K 线 1053 只 / market_state 4-21 / 1 条 4-21 BUY 待发布）；HTTP 41 端点 0 个 5xx 健康度 95%，但 N-07 三源锚点不一致 / N-08 未发布研报信息泄漏 / N-09 dashboard/stats 完全不与 reports 同源 三个 P1 问题压制业务真实可用率到 ≤ 40%。settlement 9/9 仍 100% 误分类、hotspot 仍全 0、northbound_summary 等表仍不存在。

## 与 v23（4-17）证据的对照

详见 22 §0 v12.1 增量复测表（0.2 / 0.3 / 0.4 / 0.5 / 0.7）。

## v12.1 4-21 操作记录

1. **DB 真相快照** → `db_snapshot.json`
2. **DB 清理（软删）** → 16 alive 中筛出 12 条字段缺失的研报（trade_date NULL / market_state NULL / conclusion 空 / reasoning_chain 空），UPDATE is_deleted=1，备份至 `app.db.bak_before_cleanup`
3. **服务启动** → MOCK_LLM=true / ENABLE_SCHEDULER=false / SETTLEMENT_INLINE_EXECUTION=true 启动 uvicorn @ 127.0.0.1:8010
4. **首轮探针** → `http_probe.json`，使用错前缀 `/api/*`，全 404 → 自纠
5. **修正后探针** → `http_probe_v2.json`，`/api/v1/*`，41 端点取证
6. **关键端点 body 检查** → 发现 N-07/N-08/N-08b/N-09/N-10/N-11/N-12 共 7 项新问题（详见 22 §0.7.2）
7. **服务停止** → kill terminal

## 未执行项

1. Chrome MCP 三权限页面 a11y 快照：MCP server 未配置/未启动 → HTTP 鉴权层已用 21 个 401 间接验证
2. pytest 全量基线复跑：与本次纯 docs/DB 改动无直接耦合，且 windows 环境常出 WinError 32 → 22 §10.2 下轮 TODO

## 复跑方式

```python
# DB 快照
import sqlite3
conn = sqlite3.connect(r'd:\yanbao-new\data\app.db')
# 见 22 §5 与本目录 db_snapshot.json 字段定义

# 启服 + 探针
# python -m uvicorn app.main:app --host 127.0.0.1 --port 8010 (set MOCK_LLM=true)
# 然后请求 41 端点（见 http_probe_v2.json paths 列）
```

