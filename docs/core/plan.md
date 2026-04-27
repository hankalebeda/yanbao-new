# Ralph 两步计划（300字版）

先最小修正 `docs/core/plan.md`，把主轴固定为 Ralph 两步。第一步：按仓库内 `.claude/skills/prd/SKILL.md` 与 `.claude/skills/ralph/SKILL.md`，以 `docs/core/27_PRD_研报平台增强与整体验收基线.md` 为主锚，回收 `01/02/05/06/22/25/26` 和现有代码事实，自动重写需求文档，并同步生成 `.claude/ralph/loop/prd.json` 与 `.claude/ralph/prd/yanbao-platform-enhancement.json`，确保双份一致、`notes` 可解析、保留 `08` 缺失事实、且不自动 git。第二步：让 Ralph 仅以 runtime `prd.json` 为唯一入口，按 `priority` 选择 `passes=false` 的最高优先级 story，逐条实现、运行对应 pytest 与必要校验，通过后翻转 `passes`、同步双份 JSON、追加 `progress.txt`，再自动进入下一条，直到系统持续生成完成或出现可证实硬阻塞。
