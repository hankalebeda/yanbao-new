from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
PRD_SKILL_PATH = REPO_ROOT / ".claude" / "skills" / "prd" / "SKILL.md"
RALPH_SKILL_PATH = REPO_ROOT / ".claude" / "skills" / "ralph" / "SKILL.md"
DOC_SOURCE_PATHS = (
    "AGENTS.md",
    ".claude/CLAUDE.md",
    "docs/core/01_需求基线.md",
    "docs/core/02_系统架构.md",
    "docs/core/05_API与数据契约.md",
    "docs/core/06_全量数据需求说明.md",
    "docs/core/22_全量功能进度总表_v12.md",
    "docs/core/25_系统问题分析角度清单.md",
    "docs/core/26_自动化执行记忆.md",
)


@dataclass(slots=True)
class PromptInputs:
    truth_snapshot: dict[str, Any]
    current_doc27: str
    current_prd: dict[str, Any]
    source_snippets: dict[str, str]


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def load_skill_text(path: Path) -> str:
    return _read_text(path)


def _resolve_skill_path(repo_root: Path, default_path: Path) -> Path:
    relative = default_path.relative_to(REPO_ROOT)
    candidate = repo_root / relative
    return candidate if candidate.exists() else default_path


def collect_source_snippets(repo_root: Path = REPO_ROOT, *, max_chars: int = 2500) -> dict[str, str]:
    snippets: dict[str, str] = {}
    for rel in DOC_SOURCE_PATHS:
        path = repo_root / rel
        if not path.exists():
            snippets[rel] = "[missing]"
            continue
        text = _read_text(path).strip()
        snippets[rel] = text[:max_chars]
    return snippets


def _json_block(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def build_round1_prompt(inputs: PromptInputs, *, repo_root: Path = REPO_ROOT) -> str:
    skill_text = load_skill_text(_resolve_skill_path(repo_root, PRD_SKILL_PATH))
    return f"""You are a PRD compiler worker for the repository at {repo_root}.

Non-interactive constraints:
- Do NOT ask clarifying questions.
- Do NOT ask the user for input.
- Do NOT implement code.
- Do NOT suggest git actions.
- Do NOT invent docs/core/08_AI接入策略.md; keep it explicitly missing.
- Preserve external blockers truthfully; do not rewrite them as restored.

Source skill instructions:
```markdown
{skill_text}
```

Repository truths:
```json
{_json_block(inputs.truth_snapshot)}
```

Current runtime prd.json:
```json
{_json_block(inputs.current_prd)}
```

Relevant source snippets:
```json
{_json_block(inputs.source_snippets)}
```

Current doc 27 reference:
```markdown
{inputs.current_doc27[:8000]}
```

Task:
- Produce the LLM-owned narrative content for docs/core/27_PRD_研报平台增强与整体验收基线.md.
- Include: Introduction/Overview, Goals, Epic-level User Stories, Functional Requirements summary, Blockers narrative, Success Metrics narrative.
- Output markdown only. No code fences. No explanation.
"""


def build_round2_prompt(
    inputs: PromptInputs,
    *,
    round1_markdown: str,
    repo_root: Path = REPO_ROOT,
) -> str:
    skill_text = load_skill_text(_resolve_skill_path(repo_root, RALPH_SKILL_PATH))
    return f"""You are a Ralph story compiler worker for the repository at {repo_root}.

Non-interactive constraints:
- Do NOT ask questions.
- Do NOT request clarification.
- Do NOT implement code.
- Do NOT suggest deleting existing stories.
- Preserve existing story titles/order semantics when possible.
- New runtime-gap stories must be appended as US-109+.
- Output JSON only: an array of story objects. No markdown, no explanation.
- Each story must fit Ralph's minimal schema subset:
  id?, title, description, acceptanceCriteria, priority, passes?, notes?
- Leave notes empty or minimal; deterministic enrichment happens later.

Source skill instructions:
```markdown
{skill_text}
```

Repository truths:
```json
{_json_block(inputs.truth_snapshot)}
```

Current runtime prd.json:
```json
{_json_block(inputs.current_prd)}
```

Round 1 narrative:
```markdown
{round1_markdown[:12000]}
```

Task:
- Produce a raw story list for the target system.
- Reuse existing story identity/sequence when possible.
- Preserve US-101 to US-108 as pinned runtime closure baseline stories.
- Append any new runtime closure work as US-109+.
"""
