# 废弃文档归档说明

> **归档日期**：2026-04-13
> **归档原因**：消除 Dual SSOT（双源真相）冲突
> **分析依据**：`docs/core/25_系统问题分析角度清单.md` 39 角度框架

## 背景

项目已按 `docs/core/99_AI驱动系统开发与Skill转化指南.md` 完成 SSOT 重写，
`docs/core/` 已建立完整的 5+4 个 SSOT 文档体系（01-05 + 22/25/99）。

原 `docs/guides/`、`docs/planning/`、`docs/research/` 目录中的文档为重写前的
旧版/过渡文档，与 `docs/core/` SSOT 体系形成双源真相冲突，具体包括：

- 数据严重滞后（pytest 数据 33/41/50/71 vs 实测 1899/47）
- 数据模型定义与 core/04 SSOT 冲突
- 50+ 条自循环引用脱离 core/ 血缘链
- 引用 8 个不存在的 core 文档（幽灵依赖）
- 商业底线等关键口径三处重复定义

## 归档内容

| 目录 | 文件数 | 原路径 |
|:---|:---:|:---|
| `guides/` | 13 | `docs/guides/` |
| `planning/` | 14 | `docs/planning/` |
| `research/` | 1 | `docs/research/` |
| `core_old_versions/` | 1 | `docs/core/22_全量功能进度总表_v12.md` |
| **合计** | **29** | |

## 处置建议

- 确认无需保留后可 **整目录删除** `_archive/deprecated_docs/`
- 所有规格/契约/验收标准以 `docs/core/` 为唯一来源
- Python 代码中的注释引用已同步更新
