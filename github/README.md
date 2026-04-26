# GitHub 自动化入口

本目录只承载 GitHub 自动化专题文档与运行产物约定，不承载业务规格。

- 业务 SSOT 仍只认 `docs/core/01_需求基线.md`、`docs/core/02_系统架构.md`、`docs/core/03_详细设计.md`、`docs/core/04_数据治理与血缘.md`、`docs/core/05_API与数据契约.md`。
- GitHub 自动化的长期说明统一放在 `github/docs/`。
- GitHub 自动化的当前运行产物统一放在 `github/automation/`。

文档索引
- `github/docs/01_自动化总览.md`
- `github/docs/02_运行矩阵与目录约定.md`
- `github/docs/03_凭据与安全.md`
- `github/docs/04_手动执行说明.md`

当前脚本入口
- `scripts/continuous_repo_audit.py`
- `scripts/live_fix_loop.py`
- `scripts/codex_prompt6_hourly.py`
- `scripts/github_guardian.py`
- `scripts/issue_mining_22_codex.py`
- `scripts/git_hourly_sync.py`
- `scripts/run_git_hourly_sync.ps1`
- `scripts/register_git_hourly_sync_task.ps1`

说明
- `github/automation/_local/` 是本机镜像产物目录，不是规格来源。
- `github/automation/runs/` 是 guardian worktree 内的标准运行产物路径。
- `scripts/prompt6_hourly_codex.py` 与 `scripts/run_prompt6_hourly.ps1` 仅保留旧兼容手动入口。
- Git 小时同步任务只允许提交白名单路径；运行日志写入 `github/automation/_local/git_hourly_sync/`。
