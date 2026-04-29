from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping


NOTE_KEYS = (
    "group",
    "dependsOn",
    "endpoints",
    "models",
    "permissions",
    "errorCodes",
    "idempotency",
    "enums",
    "thresholds",
    "degradation",
    "exampleAssert",
    "pytest",
    "writeScope",
    "readScope",
    "runtimeChecks",
    "dbTables",
    "envDeps",
    "hardBlockers",
)

DEFAULT_PROJECT = "A股研报平台整体验收与增强"
DEFAULT_BRANCH = "main"
ENDPOINT_RE = re.compile(r"\b(?:GET|POST|PUT|PATCH|DELETE)\s+/[^\s`'\"，。；;)]*")
PYTEST_RE = re.compile(r"python\s+-m\s+pytest[^\n]+")
PYTEST_PATH_RE = re.compile(r"(?P<path>(?:[A-Za-z0-9_.-]+[\\/])*tests[\\/][A-Za-z0-9_.\\/ -]+?\.py)")
PROGRESS_STORY_RE = re.compile(r"^##\s+.+?-\s+(US-\d{3})\s*$")
PROGRESS_FILES_RE = re.compile(r"^-\s+Files changed:\s*(?P<files>.+)$")


@dataclass(slots=True)
class StoryDraft:
    title: str
    description: str
    acceptance_criteria: list[str]
    priority: int
    raw: dict[str, Any]


@dataclass(slots=True)
class NormalizedStory:
    story: dict[str, Any]
    note_payload: dict[str, Any]
    fingerprint: str
    write_scope_hash: str


def parse_notes_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
            return dict(loaded) if isinstance(loaded, Mapping) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def compact_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def extract_pytest_commands(*texts: str) -> str:
    commands: list[str] = []
    for text in texts:
        for match in PYTEST_RE.findall(text or ""):
            cleaned = match.strip()
            if cleaned and cleaned not in commands:
                commands.append(cleaned)
    return " && ".join(commands)


def extract_pytest_paths(*texts: str) -> list[str]:
    paths: list[str] = []
    for text in texts:
        for match in PYTEST_PATH_RE.finditer(text or ""):
            cleaned = match.group("path").strip().strip("`'\".,;:)")
            cleaned = cleaned.replace("\\", "/")
            if cleaned and cleaned not in paths:
                paths.append(cleaned)
    return paths


def extract_progress_write_scopes(progress_text: str) -> dict[str, list[str]]:
    scopes: dict[str, list[str]] = {}
    current_story_id: str | None = None
    for raw_line in (progress_text or "").splitlines():
        line = raw_line.strip()
        story_match = PROGRESS_STORY_RE.match(line)
        if story_match:
            current_story_id = story_match.group(1)
            scopes.setdefault(current_story_id, [])
            continue
        files_match = PROGRESS_FILES_RE.match(line)
        if not files_match or not current_story_id:
            continue
        for raw_item in files_match.group("files").split(","):
            item = raw_item.strip().strip("`'\". ")
            item = item.replace("\\", "/")
            if not item:
                continue
            # PRD/progress files are Step-1/runner metadata, not the business/test
            # code surface whose drift should invalidate an implemented story.
            if item.startswith(".claude/ralph/"):
                continue
            if item.startswith(("app/", "codex/", "scripts/", "tests/")) and item not in scopes[current_story_id]:
                scopes[current_story_id].append(item)
    return {story_id: values for story_id, values in scopes.items() if values}


def extract_endpoints(*texts: str) -> list[str]:
    endpoints: list[str] = []
    for text in texts:
        for match in ENDPOINT_RE.findall(text or ""):
            cleaned = match.strip()
            if cleaned and cleaned not in endpoints:
                endpoints.append(cleaned)
    return endpoints


def _default_runtime_checks(story_id: str, title: str) -> list[str]:
    explicit = {
        "US-101": ["public_pool_snapshot_available"],
        "US-102": ["truth_layer_usage_nonzero"],
        "US-103": ["runtime_market_state_available"],
        "US-104": ["published_reports_nonzero"],
        "US-105": ["settlement_rows_nonzero"],
        "US-106": ["sim_positions_nonzero"],
        "US-107": ["public_read_model_nonempty"],
        "US-108": ["admin_overview_consistent"],
    }
    if story_id in explicit:
        return explicit[story_id]
    lowered = title.lower()
    if "market_state" in lowered or "市场状态" in title:
        return ["runtime_market_state_available"]
    if "结算" in title or "kpi" in lowered:
        return ["settlement_rows_nonzero"]
    if "模拟" in title or "仓位" in title:
        return ["sim_positions_nonzero"]
    if "正式发布" in title or "published" in lowered:
        return ["published_reports_nonzero"]
    if "高级区" in title or "页面" in title or "dashboard" in lowered:
        return ["public_read_model_nonempty"]
    if "股票池" in title or "pool" in lowered:
        return ["public_pool_snapshot_available"]
    if "truth" in lowered or "补采" in title:
        return ["truth_layer_usage_nonzero"]
    if "管理" in title or "admin" in lowered:
        return ["admin_overview_consistent"]
    return []


def _default_note_payload(
    story_id: str,
    title: str,
    acceptance_criteria: list[str],
    existing: Mapping[str, Any] | None,
    *,
    progress_write_scopes: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    existing_payload = parse_notes_payload(existing.get("notes")) if existing else {}
    text_blob = "\n".join([title, *acceptance_criteria])
    endpoints = existing_payload.get("endpoints") or extract_endpoints(text_blob)
    pytest_command = existing_payload.get("pytest") or extract_pytest_commands(text_blob)
    explicit_write_scope = list(existing_payload.get("writeScope") or [])
    inferred_write_scope = (
        explicit_write_scope
        or list((progress_write_scopes or {}).get(story_id) or [])
        or extract_pytest_paths(str(pytest_command or ""), text_blob)
    )
    group = existing_payload.get("group")
    if not group:
        numeric = int(story_id.split("-")[1])
        group = f"G{numeric // 10}"
    payload: dict[str, Any] = {
        "group": group,
        "dependsOn": list(existing_payload.get("dependsOn") or []),
        "endpoints": list(endpoints),
        "models": list(existing_payload.get("models") or []),
        "permissions": list(existing_payload.get("permissions") or []),
        "errorCodes": list(existing_payload.get("errorCodes") or []),
        "idempotency": str(existing_payload.get("idempotency") or ""),
        "enums": list(existing_payload.get("enums") or []),
        "thresholds": str(existing_payload.get("thresholds") or ""),
        "degradation": str(existing_payload.get("degradation") or ""),
        "exampleAssert": str(existing_payload.get("exampleAssert") or ""),
        "pytest": str(pytest_command or ""),
        "writeScope": inferred_write_scope,
        "readScope": list(existing_payload.get("readScope") or []),
        "runtimeChecks": list(existing_payload.get("runtimeChecks") or _default_runtime_checks(story_id, title)),
        "dbTables": list(existing_payload.get("dbTables") or []),
        "envDeps": list(existing_payload.get("envDeps") or []),
        "hardBlockers": list(existing_payload.get("hardBlockers") or []),
    }
    for key in NOTE_KEYS:
        payload.setdefault(key, [] if key.endswith("s") or key in {"dependsOn", "endpoints", "models", "permissions", "errorCodes", "enums", "writeScope", "readScope", "runtimeChecks", "dbTables", "envDeps", "hardBlockers"} else "")
    return payload


def story_fingerprint(story: Mapping[str, Any]) -> str:
    material = json.dumps(
        {
            "title": story.get("title"),
            "description": story.get("description"),
            "acceptanceCriteria": list(story.get("acceptanceCriteria") or []),
            "notes": parse_notes_payload(story.get("notes")),
            "priority": story.get("priority"),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def story_write_scope_hash(repo_root: Path, write_scope: Iterable[str]) -> str:
    files: list[Path] = []
    for entry in write_scope:
        if not entry:
            continue
        rel = Path(entry)
        if "*" in entry or "?" in entry or "[" in entry:
            files.extend(path for path in repo_root.glob(entry) if path.is_file())
            continue
        path = repo_root / rel
        if path.is_file():
            files.append(path)
        elif path.is_dir():
            files.extend(sorted(child for child in path.rglob("*") if child.is_file()))
    digest = hashlib.sha256()
    for path in sorted({file.resolve() for file in files}):
        digest.update(str(path.relative_to(repo_root)).encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _coerce_acceptance_criteria(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    if raw is None:
        return []
    return [str(raw).strip()]


def _existing_story_maps(existing_stories: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_id = {str(story.get("id")): story for story in existing_stories if story.get("id")}
    by_title = {str(story.get("title")): story for story in existing_stories if story.get("title")}
    return by_id, by_title


def _next_story_number(existing_stories: list[dict[str, Any]], normalized: list[dict[str, Any]]) -> int:
    numbers = []
    for story in [*existing_stories, *normalized]:
        story_id = str(story.get("id") or "")
        match = re.match(r"US-(\d+)$", story_id)
        if match:
            numbers.append(int(match.group(1)))
    return max(numbers or [0]) + 1


def normalize_story_list(
    raw_stories: list[dict[str, Any]],
    *,
    existing_prd: dict[str, Any] | None = None,
    repo_root: Path,
    project: str | None = None,
    branch_name: str | None = None,
    description: str | None = None,
) -> dict[str, Any]:
    existing_prd = existing_prd or {}
    existing_stories = list(existing_prd.get("userStories") or [])
    by_id, by_title = _existing_story_maps(existing_stories)
    normalized_stories: list[dict[str, Any]] = []
    used_ids: set[str] = set()
    progress_path = repo_root / ".claude" / "ralph" / "loop" / "progress.txt"
    progress_write_scopes = extract_progress_write_scopes(
        progress_path.read_text(encoding="utf-8") if progress_path.exists() else ""
    )

    for index, raw_story in enumerate(raw_stories, start=1):
        title = str(raw_story.get("title") or "").strip() or f"Untitled Story {index}"
        existing_story = None
        proposed_id = str(raw_story.get("id") or "").strip()
        if proposed_id and proposed_id in by_id and proposed_id not in used_ids:
            existing_story = by_id[proposed_id]
        elif title in by_title and str(by_title[title].get("id")) not in used_ids:
            existing_story = by_title[title]

        story_id = proposed_id if existing_story is None and proposed_id and proposed_id not in used_ids else None
        if existing_story is not None:
            story_id = str(existing_story.get("id"))
        if not story_id:
            story_id = f"US-{_next_story_number(existing_stories, normalized_stories):03d}"
        used_ids.add(story_id)

        acceptance_criteria = _coerce_acceptance_criteria(raw_story.get("acceptanceCriteria"))
        if not any("Typecheck passes" in item for item in acceptance_criteria):
            acceptance_criteria.append("Typecheck passes")

        note_payload = _default_note_payload(
            story_id,
            title,
            acceptance_criteria,
            existing_story,
            progress_write_scopes=progress_write_scopes,
        )
        story = {
            "id": story_id,
            "title": title,
            "description": str(
                raw_story.get("description")
                or (existing_story.get("description") if existing_story else "")
            ).strip(),
            "acceptanceCriteria": acceptance_criteria,
            "priority": int(
                raw_story.get("priority")
                or (existing_story.get("priority") if existing_story else index)
            ),
            "passes": bool(existing_story.get("passes")) if existing_story else bool(raw_story.get("passes", False)),
            "notes": compact_json(note_payload),
        }
        normalized_stories.append(story)

    normalized_stories.sort(key=lambda item: (int(item.get("priority") or 0), str(item.get("id") or "")))
    return {
        "project": project or str(existing_prd.get("project") or DEFAULT_PROJECT),
        "branchName": branch_name or str(existing_prd.get("branchName") or DEFAULT_BRANCH),
        "description": description or str(existing_prd.get("description") or DEFAULT_PROJECT),
        "userStories": normalized_stories,
    }


def prd_story_set_hash(prd_payload: Mapping[str, Any]) -> str:
    canonical = []
    for story in prd_payload.get("userStories") or []:
        canonical.append(
            {
                "id": story.get("id"),
                "title": story.get("title"),
                "description": story.get("description"),
                "acceptanceCriteria": list(story.get("acceptanceCriteria") or []),
                "priority": story.get("priority"),
                "notes": parse_notes_payload(story.get("notes")),
            }
        )
    blob = json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()
