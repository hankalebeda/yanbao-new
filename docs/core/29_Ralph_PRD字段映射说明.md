# 29_Ralph_PRD字段映射说明

> 用途：说明 `docs/core/27` 如何转换为 Ralph 最小 schema 的双份 `prd.json`。

## 1. 输入与输出

- Markdown 输入：`docs/core/27_PRD_研报平台增强与整体验收基线.md`
- Runtime 输出：`.claude/ralph/loop/prd.json`
- 命名副本：`.claude/ralph/prd/yanbao-platform-enhancement.json`
- Step 2 只允许读取 runtime 输出。

## 2. 顶层字段

- `project`：项目名
- `branchName`：必须与 `.claude/ralph/config.json` 的 `branchNamePolicy.currentValue` 一致；当前基线为 `main`
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

必须包含：

- `group`
- `dependsOn`
- `endpoints`
- `models`
- `permissions`
- `errorCodes`
- `idempotency`
- `enums`
- `thresholds`
- `degradation`
- `exampleAssert`
- `pytest`
- `writeScope`
- `readScope`
- `runtimeChecks`
- `dbTables`
- `envDeps`
- `hardBlockers`

## 5. Runtime Story 规则

- `US-101` 到 `US-108` 是 pinned runtime closure baseline stories。
- 这类 story 必须至少声明一种真实 runtime 证据：`check_state.py`、sqlite、endpoint、browser。
- `runtimeChecks` 用于 Step 1 的 deterministic sentinel adjudication。

## 6. 同步规则

- 两份 `prd.json` 必须完全一致。
- 修改 story 边界、验收、`notes`、priority 或 description 后，必须同步双文件。
- 禁止在 `prd.json` 中加入 Ralph 最小 schema 之外的顶层或 story 扩展字段。
