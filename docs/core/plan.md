# Ralph 两步主轴计划

> Fixed axis: Step 1 only rewrites the requirement anchor and syncs both `prd.json` files; Step 2 only lets Ralph execute from runtime `prd.json` until all stories pass or a proven hard blocker appears.


## 第一步：用 Ralph skills 重写需求文档并生成双份 PRD JSON

- 使用仓库内 `.claude/skills/prd/SKILL.md` 与 `.claude/skills/ralph/SKILL.md`。
- 以 `docs/core/27_PRD_研报平台增强与整体验收基线.md` 为主锚，回收 `01/02/05/06/22/25/26` 与现有代码事实。
- 自动重写需求文档，并同步生成 `.claude/ralph/loop/prd.json` 与 `.claude/ralph/prd/yanbao-platform-enhancement.json`。
- 约束：双份 JSON 必须一致，`notes` 必须可解析，必须保留 `docs/core/08_AI接入策略.md` 当前缺失这一事实，且不自动执行任何 git 操作（包括 checkout / commit / reset）。

## 第二步：让 Ralph 仅凭 runtime prd.json 连续生成本系统

- Ralph 只以 runtime `prd.json`（即 `.claude/ralph/loop/prd.json`）作为唯一直接入口，不再依赖口头补充、隐藏上下文或额外任务清单。
- 每轮按 `priority` 选择 `passes=false` 的最高优先级 story，逐条闭环推进。
- 单条闭环顺序固定为：实现 → 运行对应 `pytest` 与必要校验 → 通过后翻转 `passes` → 同步双份 JSON → 追加 `progress.txt`。
- 完成一条后自动进入下一条，直到系统持续生成完成，或出现可证实、可记录的硬阻塞。

## 边界与目标

- 第一步只负责“优化需求文档 + 生成可执行 prd.json”。
- 第二步只负责“让 Ralph 仅凭 runtime prd.json 逐条实现系统”。
- 整个主轴必须始终遵守：不伪造事实、显式记录降级/阻塞、未获当轮明确授权不得自动 git。
