# 30_Ralph双步自举运行手册

## 1. 模式

- Step 1：`python -m codex.ralph_compile rebuild --tool claude`
- Step 2：`powershell -ExecutionPolicy Bypass -File .claude/ralph/run-ralph.ps1 -Tool claude`
- Outer Loop：`python -m codex.ralph_cycle run --tool claude --max-cycles 5`

## 2. 真相源

- SSOT：01 / 02 / 05 / 06
- 问题与进度：22 / 25 / 26
- 代码与测试：`app/**`、`tests/**`
- 运行态：`check_state.py`、SQLite、TestClient、`RuntimeAnchorService`

## 3. Git 规则

- Step 1 如变更正式产物，创建 baseline commit：`ralph(prd): rebuild compile baseline`
- Step 2 每条 story 单独 commit：`ralph(US-XXX): <title>`

## 4. 禁区

- 不修改 `.claude/ralph/vendor/**`
- 不修改 `.claude/ralph/run-ralph.ps1`
- 不修改 `.claude/ralph/loop/ralph.sh`
- 不写 `scripts/`、`data/`、`output/`

