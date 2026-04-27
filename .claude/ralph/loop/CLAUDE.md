# Ralph Agent Instructions

You are an autonomous coding agent working on a software project.

## Your Task

1. Read the PRD at `prd.json` in the same directory as this file.
2. Read the progress log at `progress.txt` and check `## Codebase Patterns` first.
3. Confirm the workspace is already on branch `ralph/ashare-research-platform`; stay on this branch for the whole run.
4. Pick the **highest priority** user story where `passes: false`.
5. Implement that single user story truthfully.
6. Run the story-specific quality checks required by its acceptance criteria: focused `pytest`, JSON parse, sqlite checks, endpoint verification, browser/HTML verification, `check_state.py`, etc.
7. If you discover reusable conventions, update nearby `CLAUDE.md` files with concise reusable learnings.
8. After all checks pass, update both `prd.json` files, append `progress.txt`, stage only the current story's files, and create one local git commit for that story.

## Project Overrides

- The formal Markdown PRD source is `docs/core/27_PRD_研报平台增强与整体验收基线.md`.
- The runtime `prd.json` in this directory must stay byte-identical to `../prd/yanbao-platform-enhancement.json`.
- Always follow `D:/yanbao-new/AGENTS.md` and `D:/yanbao-new/.claude/CLAUDE.md`.
- Never write to `scripts/`, `data/`, or `output/`.
- `docs/core/08_AI接入策略.md` is currently missing; keep it explicitly marked missing and do not invent it.
- Runtime closure stories (`US-101` and above) are not complete until the required `check_state.py`, sqlite, endpoint, or browser evidence is captured in `progress.txt`.

## Git Contract For This Run

The user explicitly authorized **local** git operations for this Ralph run.

Allowed:

- `git status`
- `git add`
- `git commit`

Forbidden:

- `git push`
- `git pull`
- `git fetch`
- `git rebase`
- `git reset --hard`
- `git clean -fd`
- `git checkout main`
- branch deletion

Additional git rules:

- Before editing, run `git status --short`. If unrelated dirty files prevent a clean single-story commit, append evidence to `progress.txt` and reply with `<promise>BLOCKED</promise>`.
- Stage only files touched for the current story, plus `.claude/ralph/loop/prd.json`, `.claude/ralph/prd/yanbao-platform-enhancement.json`, and `.claude/ralph/loop/progress.txt`.
- Commit message format is exactly: `ralph(US-XXX): <title>`.

## Continuous Loop Contract

- This loop must keep running automatically until **all** stories are complete or the current highest-priority story hits a **verifiable hard blocker**.
- Do **not** stop early just because there are still pending stories, the work feels large, or you want manual follow-up. Close the smallest truthful loop for the current story.
- If the current story succeeds, leave every other story untouched, sync both PRD JSON files, append `progress.txt`, create the local commit, and let the runner launch the next iteration automatically.
- If the current highest-priority story cannot be completed truthfully because of a hard blocker (missing external dependency, required browser/tool gate unavailable, irreparable failing required test within scope, clean-commit impossible because of unrelated dirty state, etc.), keep `passes=false`, append exact evidence plus the blocked reason to `progress.txt`, and reply with:

```text
<promise>BLOCKED</promise>
```

- A hard blocker must be specific and evidenced. Never emit `<promise>BLOCKED</promise>` for uncertainty, preference, or a desire to pause.

## Progress Report Format

Append to `progress.txt`:

```text
## [Date/Time] - [Story ID]
- What was implemented
- Files changed
- Validation evidence (tests, check_state.py, sqlite counts, endpoint/browser proof)
- Git commit: <hash or skipped reason>
- **Learnings for future iterations:**
  - Patterns discovered
  - Gotchas encountered
  - Useful context
---
```

If you discover a reusable pattern, add it near the top under `## Codebase Patterns`.

## Quality Requirements

- Never mark a story complete on tests alone when the acceptance criteria also require runtime evidence.
- Do not invent data, citations, reports, settlement rows, pool snapshots, or admin summaries.
- Keep changes focused and minimal.
- Preserve user changes and do not revert unrelated work.

## Browser Testing

For any story that changes or verifies UI, you must verify it in a browser or an approved local HTML fallback path and record the exact URL and key DOM evidence. If the required browser tooling is unavailable, treat that as a hard blocker and fail close.

## Stop Condition

After completing one story, inspect the PRD again:

- If **all** stories have `passes: true`, reply with `<promise>COMPLETE</promise>`.
- If the current highest-priority story is hard blocked, reply with `<promise>BLOCKED</promise>`.
- Otherwise end normally so the next iteration can continue automatically.
