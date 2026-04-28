from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def render_doc27_compiler_appendix() -> str:
    return """## Compiler-owned Appendix

### Step 1 / Step 2 Boundary
- Step 1 only rewrites docs/core/27, docs/core/28, docs/core/29, and the dual `prd.json` files.
- Step 2 only executes the runtime `prd.json`, flips the current story `passes`, appends `progress.txt`, and commits the current story locally.
- Step 2 must not rewrite docs/core/27/28/29.

### 18-key `notes` Contract
Every runtime story note payload must include:
`group`, `dependsOn`, `endpoints`, `models`, `permissions`, `errorCodes`, `idempotency`, `enums`, `thresholds`, `degradation`, `exampleAssert`, `pytest`, `writeScope`, `readScope`, `runtimeChecks`, `dbTables`, `envDeps`, `hardBlockers`.

### Runtime Closure Rule
- `US-101` to `US-108` stay pinned as runtime closure baseline stories.
- New runtime-gap stories are appended as `US-109+`.
- Runtime closure stories cannot become `passes=true` without real runtime evidence.

### Missing Doc Rule
- `docs/core/08_AI接入策略.md` is currently missing and must remain explicitly marked missing until a real source-of-truth document exists.
"""


def render_doc28() -> str:
    return """# 28_严格验收与上线门禁

> 用途：为 Ralph Step 2 提供 deterministic 验收门禁。runtime 直接入口仍是 `.claude/ralph/loop/prd.json`。

## 1. 总原则
- `passes` 只能在真实实现完成且验证通过后从 `false` 改为 `true`。
- 任意 blocked、外部依赖缺失、JSON 解析失败、测试失败或浏览器验证失败都必须 fail-close。
- Step 2 不得改写 `docs/core/27`、`docs/core/28`、`docs/core/29`。
- 禁止写入 `scripts/`、`data/`、`output/`。
- `docs/core/08_AI接入策略.md` 当前缺失，只能显式保留 missing，不能发明内容。

## 2. Dual-PRD Gate
- `.claude/ralph/loop/prd.json` 与 `.claude/ralph/prd/yanbao-platform-enhancement.json` 必须完全一致。
- runtime 版 `prd.json` 是 Step 2 的唯一直接任务入口。
- 每条 story 的 `notes` 必须是可解析 JSON 且包含 18 个固定键。

## 3. 单轮执行门禁
- 每轮只允许处理最高优先级的一条 `passes=false` story。
- 只允许修改当前 story 的 `writeScope`、双 `prd.json`、`progress.txt` 与所需代码/测试文件。
- 代码 story 必须跑 focused pytest；UI story 必须完成浏览器验证；runtime story 必须补 `check_state.py` / sqlite / endpoint 证据。

## 4. 推荐验证命令
- `powershell -ExecutionPolicy Bypass -File .claude/ralph/run-ralph.ps1 -Tool claude -MaxIterations 1 -DryRun`
- `python -m pytest ... -q --tb=short`
- `python .\\check_state.py`

## 5. 通过定义
一条 story 只有同时满足以下条件才可置为 `passes=true`：
- 当前 story 实现范围完整；
- 对应 focused pytest 通过；
- 所需 runtime/browser/sqlite 证据已满足；
- 双 `prd.json` 已同步；
- `progress.txt` 已追加；
- 未触碰 forbidden paths。
"""


def render_doc29() -> str:
    note_keys = ", ".join(
        [
            "`group`",
            "`dependsOn`",
            "`endpoints`",
            "`models`",
            "`permissions`",
            "`errorCodes`",
            "`idempotency`",
            "`enums`",
            "`thresholds`",
            "`degradation`",
            "`exampleAssert`",
            "`pytest`",
            "`writeScope`",
            "`readScope`",
            "`runtimeChecks`",
            "`dbTables`",
            "`envDeps`",
            "`hardBlockers`",
        ]
    )
    return f"""# 29_Ralph_PRD字段映射说明

> 用途：说明 `docs/core/27` 如何转换为 Ralph 最小 schema 的双份 `prd.json`。

## 1. 输入与输出
- Markdown 输入：`docs/core/27_PRD_研报平台增强与整体验收基线.md`
- Runtime 输出：`.claude/ralph/loop/prd.json`
- 命名副本：`.claude/ralph/prd/yanbao-platform-enhancement.json`
- Step 2 只允许读取 runtime 输出。

## 2. 顶层字段
- `project`：项目名
- `branchName`：必须与当前 Ralph 运行分支一致；当前基线为 `main`
- `description`：执行描述
- `userStories`：原子 story 列表

## 3. Story 字段
- `id`：保留已有 `US-001~US-108`，新增追加 `US-109+`
- `title`：原子能力名称
- `description`：角色 / 动作 / 目标
- `acceptanceCriteria`：可验证条目
- `priority`：依赖顺序
- `passes`：真实裁定结果
- `notes`：紧凑 JSON 字符串

## 4. `notes` 强制键
必须包含：{note_keys}

## 5. Runtime Story 规则
- `US-101` 到 `US-108` 是 pinned runtime closure baseline stories。
- 这类 story 必须至少声明一种真实 runtime 证据：`check_state.py`、sqlite、endpoint、browser。
- `runtimeChecks` 用于 Step 1 的 deterministic sentinel adjudication。

## 6. 同步规则
- 两份 `prd.json` 必须完全一致。
- 修改 story 边界、验收、`notes`、priority 或 description 后，必须同步双文件。
- 禁止在 `prd.json` 中加入 Ralph 最小 schema 之外的顶层或 story 扩展字段。
"""


def render_doc30() -> str:
    return """# 30_Ralph双步自举运行手册

## 1. 模式
- Step 1：`python -m codex.ralph_compile rebuild --tool claude`
- Step 2：`powershell -ExecutionPolicy Bypass -File .claude/ralph/run-ralph.ps1 -Tool claude`
- Outer Loop：`python -m codex.ralph_cycle run --tool claude --max-cycles 5`
- 小时级监控必须先做 branch gate + 只读预检，再决定是否进入 Outer Loop。

## 2. 真相源
- SSOT：01 / 02 / 05 / 06
- 问题与进度：22 / 25 / 26
- 代码与测试：`app/**`、`tests/**`
- 运行态：`check_state.py`、SQLite、TestClient、`RuntimeAnchorService`

## 3. 小时级预检
- 当前分支必须与 `.claude/ralph/config.json` 的 `branchNamePolicy.currentValue` 一致；当前基线为 `main`，且 `.claude/ralph/loop/.last-branch` 必须与之相同。
- `HEAD...main` 不得有落后或分叉。
- tracked git diff 必须干净；`_archive/case_*` 这类权限告警只算环境噪音，不算 tracked 脏改动。
- 进入 Outer Loop 前必须通过：`check_state.py`、`python -m codex.ralph_compile verify`、runner `-DryRun`、以及 `tests/test_ralph_compile.py` + `tests/test_ralph_cycle.py` 的定向 pytest。
- 任一预检失败都必须先返回状态，不得直接跑 Step 1 / Step 2。

## 4. 小时级状态
- `complete`：预检通过，且 Outer Loop / fast path 判断当前已收敛。
- `blocked`：进入 Outer Loop 后遇到真实硬阻塞。
- `incomplete`：达到轮数上限，仍未收敛。
- `branch_drift`：当前 checkout 不是 Ralph 运行分支，或与 `.last-branch` / 目标分支 tip 不一致。
- `workspace_dirty`：存在 tracked 脏改动，不能 truthfully 启动 runtime 闭环。
- `preflight_failed`：`check_state.py`、`verify`、runner dry-run、或定向 pytest 未通过。

## 5. Git 规则
- Step 1 如变更正式产物，创建 baseline commit：`ralph(prd): rebuild compile baseline`
- Step 2 每条 story 单独 commit：`ralph(US-XXX): <title>`

## 6. 禁区
- 不修改 `.claude/ralph/vendor/**`
- 不修改 `.claude/ralph/run-ralph.ps1`
- 不修改 `.claude/ralph/loop/ralph.sh`
- 不写 `scripts/`、`data/`、`output/`
"""


def render_doc27(round1_markdown: str) -> str:
    body = round1_markdown.strip()
    if not body.startswith("#"):
        body = "# 27_PRD_研报平台增强与整体验收基线\n\n" + body
    return body.rstrip() + "\n\n---\n\n" + render_doc27_compiler_appendix().strip() + "\n"
