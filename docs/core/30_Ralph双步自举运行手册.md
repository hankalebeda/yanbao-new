# 30_Ralph双步自举运行手册

> 更新日期：2026-04-27
> 目标：以最小改动方式，让 Ralph 按仓库既有实现持续自动迭代，直到进入 **COMPLETE** 或 **BLOCKED** 终态。
> 维护注意：本文不属于 Step 1 compiler-owned 输出；Step 1 只负责 docs 27/28/29 与双份 `prd.json`，本文按运行手册手工维护。

## 1. 结论与适用边界

- 本仓库的正确主路径不是手工维护一套 9-story meta PRD。
- **Step 1** 必须走仓库内置编译器：`python -m codex.ralph_compile ...`
- **Step 2** 必须走现有 runner：`powershell -ExecutionPolicy Bypass -File .claude/ralph/run-ralph.ps1 -Tool claude`
- **Outer Loop** 必须用仓库内置控制器：`python -m codex.ralph_cycle run ...`
- 自动化的正确终态只有两个：
  - `COMPLETE`：全部 story 收敛完成
  - `BLOCKED`：遇到真实硬阻塞并 fail-close 停止
- **不能承诺无条件一定 COMPLETE**；外部依赖、浏览器验证、git 脏工作树、真实运行态证据不足，都可能触发 `BLOCKED`。

## 2. Step 1 / Step 2 固定边界

### 2.1 Step 1

- 命令：`python -m codex.ralph_compile rebuild --tool claude`
- 职责：
  - 重写 `docs/core/27_PRD_研报平台增强与整体验收基线.md`
  - 重写 `docs/core/28_严格验收与上线门禁.md`
  - 重写 `docs/core/29_Ralph_PRD字段映射说明.md`
  - 同步双份 PRD：
    - `.claude/ralph/loop/prd.json`
    - `.claude/ralph/prd/yanbao-platform-enhancement.json`
  - 依据真实 runtime truth 做 adjudication
- 禁止改业务代码。

### 2.2 Step 2

- 命令：`powershell -ExecutionPolicy Bypass -File .claude/ralph/run-ralph.ps1 -Tool claude`
- 职责：
  - 只按 runtime `prd.json` 执行当前最高优先级 `passes=false` 的 story
  - 只允许修改当前 story 的 `writeScope`、双 `prd.json`、`progress.txt`、对应代码/测试文件
  - 不得改 `docs/core/27/28/29`

### 2.3 Outer Loop

- 命令：`python -m codex.ralph_cycle run --tool claude --max-cycles 5`
- 职责：
  - `Step 1 rebuild -> Step 2 run -> Step 1 rebuild`
  - 判断当前是 `complete`、`blocked` 还是 `continue`

## 3. 真相源

- SSOT：`docs/core/01_需求基线.md`、`docs/core/02_系统架构.md`、`docs/core/05_API与数据契约.md`、`docs/core/06_全量数据需求说明.md`
- 问题与进度：`docs/core/22_全量功能进度总表_v12.md`、`docs/core/25_系统问题分析角度清单.md`、`docs/core/26_自动化执行记忆.md`
- 代码与测试：`app/**`、`tests/**`
- 运行态锚点：`check_state.py`、SQLite、FastAPI `TestClient`、`RuntimeAnchorService`

## 4. 运行前硬性前提

### 4.1 Git 前提

- 当前工作分支必须与 `.claude/ralph/config.json` 的 `branchNamePolicy.currentValue` 一致；当前基线为 `main`
- Step 2 启动前，工作树不应存在无关脏改动
- 如有无关改动，必须先 stash 或单独处理

### 4.2 工具前提

- Git for Windows 可用
- `claude` CLI 可用
- `powershell` 可用
- 本地 `jq` 固定路径：
  - `.claude/ralph/bin/jq.exe`

### 4.3 文档与真实性前提

- `docs/core/08_AI接入策略.md` 当前缺失，所有自动化与文档只能显式保留 `missing`，不得发明内容
- `US-101` 到 `US-108` 固定作为 pinned runtime closure baseline stories
- 新增 runtime gap 只能追加为 `US-109+`

## 5. 一次性标准启动流程

### 5.1 备份当前运行面

```powershell
$ErrorActionPreference = "Stop"
Set-Location "D:\yanbao-new"

$today = Get-Date -Format "yyyy-MM-dd"
$archive = "D:\yanbao-new\.claude\ralph\loop\archive\$today-pre-outer-loop"

New-Item $archive -ItemType Directory -Force | Out-Null

Copy-Item "D:\yanbao-new\.claude\ralph\loop\prd.json" "$archive\loop-prd.json" -Force
Copy-Item "D:\yanbao-new\.claude\ralph\prd\yanbao-platform-enhancement.json" "$archive\yanbao-platform-enhancement.json" -Force
Copy-Item "D:\yanbao-new\.claude\ralph\loop\progress.txt" "$archive\progress.txt" -Force
Copy-Item "D:\yanbao-new\.claude\ralph\loop\CLAUDE.md" "$archive\CLAUDE-step2.md" -Force
```

### 5.2 清理无关脏改动

先检查：

```powershell
Set-Location "D:\yanbao-new"
git status --short
```

如有无关改动，可按路径定向 stash，例如：

```powershell
Set-Location "D:\yanbao-new"
git stash push -m "pre-ralph-unrelated" -- "D:\yanbao-new\docs\core\plan.md"
git status --short
```

如果 `git status --short` 显示 `docs/core/30_Ralph双步自举运行手册.md`、`docs/core/plan.md` 或其他手工改动，必须先确认这些改动是本轮允许提交的文档改动，或先 stash / commit / 放弃；否则 Step 2 可能因无法创建单 story commit 而进入 `BLOCKED`。

### 5.3 确认当前在 Ralph 运行分支

```powershell
Set-Location "D:\yanbao-new"
git switch "main"
git branch --show-current
```

### 5.4 检查 runner 前置条件

```powershell
Set-Location "D:\yanbao-new"
powershell -ExecutionPolicy Bypass -File "D:\yanbao-new\.claude\ralph\run-ralph.ps1" -Tool claude -MaxIterations 1 -DryRun
```

### 5.5 采集当前真实运行态

```powershell
Set-Location "D:\yanbao-new"
python "D:\yanbao-new\check_state.py"
```

## 6. Step 1：标准化重编译

### 6.1 必跑命令

```powershell
Set-Location "D:\yanbao-new"
python -m codex.ralph_compile rebuild --tool claude
python -m codex.ralph_compile verify
```

若 `verify` 报 `missing_note_keys`、`dual_prd_mismatch` 或 runner dry-run 失败，不得进入 Step 2；先重新执行 `rebuild` 并保留错误输出，直到 `verify` 返回 0。

### 6.2 作用说明

- 补齐 `notes` 的 18 个固定键
- 同步双 PRD JSON
- 依据真实 runtime truth 保留或回退 `passes`
- 同步 27 / 28 / 29 / 30 文档

### 6.3 何时算通过

- `verify` 返回 exit code 0
- 双份 PRD 完全一致
- `notes` 可解析且包含 18 键
- runner `-DryRun` 通过

## 7. Step 2：单独执行 runtime stories

仅当需要直接跑实现循环时使用：

```powershell
Set-Location "D:\yanbao-new"
powershell -ExecutionPolicy Bypass -File "D:\yanbao-new\.claude\ralph\run-ralph.ps1" -Tool claude -MaxIterations 0
```

说明：

- `MaxIterations=0` 表示持续运行，直到全部 story 完成或出现真实硬阻塞
- Step 2 只对 runtime `prd.json` 负责，不负责重新编译 Step 1
- 直接单独跑 Step 2 时，`MaxIterations=0` 可能长时间占用当前终端；若只是想试跑或观察单轮行为，改用 `-MaxIterations 1` 或 `-MaxIterations 3`。

## 8. 推荐总控：Outer Loop

### 8.1 单次 outer loop

```powershell
Set-Location "D:\yanbao-new"
python -m codex.ralph_cycle run --tool claude --max-cycles 5
```

### 8.2 持续运行到终态的推荐包装器

```powershell
Set-Location "D:\yanbao-new"

@'
from pathlib import Path
import json
import time
from codex.ralph_cycle import run_cycles

repo = Path(r"D:\yanbao-new")
max_outer_rounds = 20

for outer in range(1, max_outer_rounds + 1):
    summary = run_cycles(repo_root=repo, tool="claude", max_cycles=5)
    print(f"\\n=== OUTER ROUND {outer} ===")
    print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))

    if summary.final_status == "complete":
        raise SystemExit(0)

    if summary.final_status == "blocked":
        raise SystemExit(2)

    time.sleep(3)

raise SystemExit(1)
'@ | python -
```

### 8.3 返回码定义

- 使用 8.2 的 Python 包装器时：
  - `0`：收敛完成，终态为 `COMPLETE`
  - `2`：真实硬阻塞，终态为 `BLOCKED`
  - `1`：达到外层轮数上限，或仍为 `incomplete`
- 直接运行 `python -m codex.ralph_cycle run --tool claude --max-cycles 5` 时，当前 CLI 只对 `complete` 返回 `0`；`blocked` 与 `incomplete` 都返回 `1`，必须读取 JSON 输出中的 `final_status` 区分。

### 8.4 小时级监控前置状态

- `python -m codex.ralph_cycle run --tool claude --max-cycles 5` 在进入 Outer Loop 前，必须先完成 branch gate + 只读预检。
- 若当前分支不是 `.claude/ralph/config.json` 的 `branchNamePolicy.currentValue`（当前基线为 `main`），或 `.claude/ralph/loop/.last-branch` / 目标分支 tip 不一致，必须直接返回 `final_status=branch_drift`，不得继续执行 Step 1 / Step 2。
- 若存在 tracked git 脏改动，必须直接返回 `final_status=workspace_dirty`；`_archive/case_*` 这类权限告警只算环境噪音，不算 tracked 脏改动。
- 若 `check_state.py`、`python -m codex.ralph_compile verify`、runner `-DryRun`、或 `tests/test_ralph_compile.py` + `tests/test_ralph_cycle.py` 的定向 pytest 失败，必须直接返回 `final_status=preflight_failed`。

## 9. COMPLETE / BLOCKED 的判定标准

### 9.1 COMPLETE

同时满足：

- `.claude/ralph/loop/prd.json` 中全部 story `passes=true`
- Step 1 `verify` 通过
- 所需 runtime/browser/sqlite/endpoint/check_state 证据真实成立

### 9.2 BLOCKED

任一命中即应 fail-close：

- 当前最高优先级 story 无法 truthfully 完成
- 外部依赖不可用
- 浏览器/UI 验证不可用或失败
- sqlite / endpoint / check_state 证据不足
- git 工作树无关脏改动导致无法做单 story commit
- Claude CLI / 本地工具异常，且当前 story 无法继续

## 10. 重点观察文件

运行中或阻塞后，优先检查：

- `.claude/ralph/loop/progress.txt`
- `.claude/ralph/loop/prd.json`
- `.claude/ralph/prd/yanbao-platform-enhancement.json`
- `.claude/ralph/loop/compile_report.json`
- `.claude/ralph/loop/compile_manifest.json`

## 11. Git 规则

- Step 1 如变更正式产物，当前实现会创建 baseline commit：
  - `ralph(prd): rebuild compile baseline`
- Step 2 每条 story 单独 commit：
  - `ralph(US-XXX): <title>`
- 禁止：
  - `git push`
  - `git pull`
  - `git fetch`
  - `git rebase`
  - `git reset --hard`
  - `git clean -fd`
  - `git checkout` / `git switch` 到其他分支
  - 删除分支

## 12. 禁区

- 不修改 `.claude/ralph/vendor/**`
- 不修改 `.claude/ralph/run-ralph.ps1`
- 不修改 `.claude/ralph/loop/ralph.sh`
- 不把临时文件写到仓库根目录
- 不写 `scripts/`、`data/`、`output/`
- 不把缺失事实写成“已恢复”

## 13. 当前基线说明（2026-04-27）

截至 2026-04-27，本仓库已观测到：

- `check_state.py` 可返回真实运行态
- 运行态存在已发布研报，不再是 `Total published: 0`
- runtime closure sentinels 可用于 Step 1 adjudication

但是否继续保持 `COMPLETE`，仍以每轮实际运行结果为准；如果真实证据回退，系统必须允许自动回退到 `BLOCKED` 或重新进入待实现状态。
