# scripts 目录导航

本目录只把 `docs/core/01~05`、`docs/core/22_全量功能进度总表_v7_精审.md`、`docs/core/99_AI驱动系统开发与Skill转化指南.md` 视为现行说明。`docs/提示词/18_全量自动化提示词.md` 是自动化运行提示词，不是业务规格。

## 1. 活跃脚本

- 治理 / 验真：`continuous_repo_audit.py`、`scripts/doc_driven/**`
- 自动化修复：`live_fix_loop.py`、`codex_prompt6_hourly.py`、`github_guardian.py`、`issue_mining_22_codex.py`
- 仓库小时同步：`git_hourly_sync.py`、`run_git_hourly_sync.ps1`、`register_git_hourly_sync_task.ps1`
- 运行库修复与回填：`repair_runtime_history.py`、`rebuild_runtime_db.py`、`rebuild_ssot_db.py`、`rebuild_fr07_truth_snapshots.py`、`backfill_report_truth.py`、`backfill_baseline_history.py`、`backfill_sim_dashboard_history.py`
- 规格 / 页面辅助：`analyze_01_with_ai.py`、`check_stage123_agents.py`、`browser_audit_v711.py`、`frontend_audit.py`、`validate_baseline.py`、`validate_phase5_6.py`、`verify_lightweight.py`

这些脚本的输出应落到 `runtime/`、`output/`、`github/automation/` 或 `docs/_temp/stage123_loop/`，不要再把长期结果散落回根目录。
其中 Git 小时同步日志只写到 `github/automation/_local/git_hourly_sync/`，不写回 tracked 文件。

## 2. 历史 / 一次性脚本

- `scripts/archive/**` 是历史层。
- `_*.py`、`fix_*.py`、`patch_registry_audit*.py`、`round*.py` 默认按一次性诊断 / 修复脚本看待，不进入日常主流程。
- 若必须重跑这类脚本，先确认它们仍有当前证据链价值，再把输出直接归档到 `_archive/` 或 `docs/old/`。

## 3. 变更规则

1. 调整脚本职责或入口时，同步更新 `AGENTS.md`、根 `README.md`，以及受影响的 `docs/core/99` 或 `docs/提示词/18_全量自动化提示词.md`。
2. 新增业务行为必须能回链到 `docs/core/01~05`，并补对应 `tests/test_*.py`。
3. 临时实验脚本不要落在根目录；放 `scripts/` 时要么说明会长期保留，要么直接放进 `scripts/archive/`。
