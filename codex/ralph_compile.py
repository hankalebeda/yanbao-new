from __future__ import annotations

import argparse
import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from codex.ralph_adjudicate import adjudicate_prd, build_manifest, load_manifest
from codex.ralph_prompts import PromptInputs, build_round1_prompt, build_round2_prompt, collect_source_snippets
from codex.ralph_story_normalize import (
    NOTE_KEYS,
    normalize_story_list,
    parse_notes_payload,
    prd_story_set_hash,
)
from codex.ralph_templates import render_doc27, render_doc28, render_doc29, render_doc30
from codex.ralph_truth import TruthSnapshot, collect_truth_snapshot


REPO_ROOT = Path(__file__).resolve().parents[1]
DOC27_PATH = REPO_ROOT / "docs" / "core" / "27_PRD_研报平台增强与整体验收基线.md"
DOC28_PATH = REPO_ROOT / "docs" / "core" / "28_严格验收与上线门禁.md"
DOC29_PATH = REPO_ROOT / "docs" / "core" / "29_Ralph_PRD字段映射说明.md"
DOC30_PATH = REPO_ROOT / "docs" / "core" / "30_Ralph双步自举运行手册.md"
LOOP_PRD_PATH = REPO_ROOT / ".claude" / "ralph" / "loop" / "prd.json"
NAMED_PRD_PATH = REPO_ROOT / ".claude" / "ralph" / "prd" / "yanbao-platform-enhancement.json"
COMPILE_MANIFEST_PATH = REPO_ROOT / ".claude" / "ralph" / "loop" / "compile_manifest.json"
COMPILE_REPORT_PATH = REPO_ROOT / ".claude" / "ralph" / "loop" / "compile_report.json"


@dataclass(slots=True)
class CompileSummary:
    mode: str
    changed_docs: list[str]
    changed_prd: list[str]
    stories_total: int
    stories_passed: int
    stories_failed: int
    new_story_ids: list[str]
    regressed_story_ids: list[str]
    blocked_external_ids: list[str]
    story_set_hash: str
    baseline_commit_created: bool = False
    baseline_commit: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "changed_docs": self.changed_docs,
            "changed_prd": self.changed_prd,
            "stories_total": self.stories_total,
            "stories_passed": self.stories_passed,
            "stories_failed": self.stories_failed,
            "new_story_ids": self.new_story_ids,
            "regressed_story_ids": self.regressed_story_ids,
            "blocked_external_ids": self.blocked_external_ids,
            "story_set_hash": self.story_set_hash,
            "baseline_commit_created": self.baseline_commit_created,
            "baseline_commit": self.baseline_commit,
        }


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _load_prd(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}


def _write_if_changed(path: Path, content: str) -> bool:
    current = _read_text(path)
    if current == content:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def _write_json_if_changed(path: Path, payload: dict[str, Any]) -> bool:
    serialized = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    return _write_if_changed(path, serialized)


def _extract_json_array(text: str) -> list[dict[str, Any]]:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?", "", stripped).strip()
        stripped = re.sub(r"```$", "", stripped).strip()
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"round2_json_parse_failed:{exc}") from exc
    if not isinstance(payload, list):
        raise RuntimeError("round2_json_not_array")
    return [dict(item) for item in payload if isinstance(item, dict)]


def _run_claude(prompt: str, *, repo_root: Path, timeout_sec: int = 300) -> str:
    result = subprocess.run(
        ["claude", "--dangerously-skip-permissions", "--print"],
        cwd=str(repo_root),
        input=prompt,
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout_sec,
    )
    if result.returncode != 0:
        raise RuntimeError(f"claude_cli_failed:{result.returncode}:{(result.stderr or '').strip()}")
    return (result.stdout or "").strip()


def _current_inputs(repo_root: Path, truth_snapshot: TruthSnapshot) -> PromptInputs:
    current_prd = _load_prd(repo_root / ".claude" / "ralph" / "loop" / "prd.json")
    current_doc27 = _read_text(repo_root / "docs" / "core" / "27_PRD_研报平台增强与整体验收基线.md")
    snippets = collect_source_snippets(repo_root)
    return PromptInputs(
        truth_snapshot=truth_snapshot.to_dict(),
        current_doc27=current_doc27,
        current_prd=current_prd,
        source_snippets=snippets,
    )


def _git_is_available(repo_root: Path) -> bool:
    result = subprocess.run(["git", "rev-parse", "--is-inside-work-tree"], cwd=str(repo_root), capture_output=True, text=True, check=False)
    return result.returncode == 0 and result.stdout.strip() == "true"


def _git_commit(repo_root: Path, paths: list[Path], message: str) -> str | None:
    if not paths or not _git_is_available(repo_root):
        return None
    subprocess.run(["git", "add", "--", *[str(path.relative_to(repo_root)) for path in paths]], cwd=str(repo_root), check=True)
    status = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=str(repo_root), check=False)
    if status.returncode == 0:
        return None
    subprocess.run(["git", "commit", "-m", message], cwd=str(repo_root), check=True)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(repo_root), capture_output=True, text=True, check=True)
    return head.stdout.strip()


def _build_compile_report(summary: CompileSummary, adjudication: dict[str, Any]) -> dict[str, Any]:
    return {
        **summary.to_dict(),
        "adjudication": adjudication,
    }


def _story_id_set(prd_payload: dict[str, Any]) -> set[str]:
    return {str(item.get("id")) for item in prd_payload.get("userStories") or [] if item.get("id")}


def rebuild_repo(*, repo_root: Path = REPO_ROOT, tool: str = "claude") -> CompileSummary:
    if tool != "claude":
        raise ValueError("Only claude tool is supported by the deterministic compiler")

    truth = collect_truth_snapshot(repo_root=repo_root, database_path=repo_root / "data" / "app.db")
    inputs = _current_inputs(repo_root, truth)
    existing_prd = inputs.current_prd
    existing_story_ids = _story_id_set(existing_prd)

    round1_output = _run_claude(build_round1_prompt(inputs, repo_root=repo_root), repo_root=repo_root)
    round2_output = _run_claude(
        build_round2_prompt(inputs, round1_markdown=round1_output, repo_root=repo_root),
        repo_root=repo_root,
    )
    raw_stories = _extract_json_array(round2_output)

    normalized_prd = normalize_story_list(
        raw_stories,
        existing_prd=existing_prd,
        repo_root=repo_root,
        project=str(existing_prd.get("project") or ""),
        branch_name=str(existing_prd.get("branchName") or ""),
        description=str(existing_prd.get("description") or ""),
    )
    previous_manifest = load_manifest(repo_root / ".claude" / "ralph" / "loop" / "compile_manifest.json")
    adjudicated_prd, adjudication = adjudicate_prd(
        normalized_prd,
        truth_snapshot=truth,
        repo_root=repo_root,
        previous_manifest=previous_manifest,
        generated_at=truth.generated_at,
    )

    changed_docs: list[str] = []
    changed_prd: list[str] = []
    if _write_if_changed(repo_root / "docs" / "core" / "27_PRD_研报平台增强与整体验收基线.md", render_doc27(round1_output)):
        changed_docs.append("docs/core/27_PRD_研报平台增强与整体验收基线.md")
    if _write_if_changed(repo_root / "docs" / "core" / "28_严格验收与上线门禁.md", render_doc28()):
        changed_docs.append("docs/core/28_严格验收与上线门禁.md")
    if _write_if_changed(repo_root / "docs" / "core" / "29_Ralph_PRD字段映射说明.md", render_doc29()):
        changed_docs.append("docs/core/29_Ralph_PRD字段映射说明.md")
    if _write_if_changed(repo_root / "docs" / "core" / "30_Ralph双步自举运行手册.md", render_doc30()):
        changed_docs.append("docs/core/30_Ralph双步自举运行手册.md")
    if _write_json_if_changed(repo_root / ".claude" / "ralph" / "loop" / "prd.json", adjudicated_prd):
        changed_prd.append(".claude/ralph/loop/prd.json")
    if _write_json_if_changed(repo_root / ".claude" / "ralph" / "prd" / "yanbao-platform-enhancement.json", adjudicated_prd):
        changed_prd.append(".claude/ralph/prd/yanbao-platform-enhancement.json")

    new_story_ids = sorted(_story_id_set(adjudicated_prd) - existing_story_ids)
    regressed_story_ids = [item.story_id for item in adjudication.decisions if item.decision == "regress_to_false"]
    blocked_external_ids = [item.story_id for item in adjudication.decisions if item.decision == "blocked_external"]
    stories_total = len(adjudicated_prd.get("userStories") or [])
    stories_passed = sum(1 for item in adjudicated_prd.get("userStories") or [] if item.get("passes"))
    summary = CompileSummary(
        mode="rebuild",
        changed_docs=changed_docs,
        changed_prd=changed_prd,
        stories_total=stories_total,
        stories_passed=stories_passed,
        stories_failed=stories_total - stories_passed,
        new_story_ids=new_story_ids,
        regressed_story_ids=regressed_story_ids,
        blocked_external_ids=blocked_external_ids,
        story_set_hash=prd_story_set_hash(adjudicated_prd),
    )

    baseline_commit = _git_commit(
        repo_root,
        [repo_root / path for path in [*changed_docs, *changed_prd]],
        "ralph(prd): rebuild compile baseline",
    )
    if baseline_commit:
        summary.baseline_commit_created = True
        summary.baseline_commit = baseline_commit

    manifest_payload = build_manifest(
        prd_payload=adjudicated_prd,
        decisions=adjudication.decisions,
        repo_root=repo_root,
        baseline_commit=summary.baseline_commit,
    )
    (repo_root / ".claude" / "ralph" / "loop").mkdir(parents=True, exist_ok=True)
    (repo_root / ".claude" / "ralph" / "loop" / "compile_manifest.json").write_text(
        json.dumps(manifest_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (repo_root / ".claude" / "ralph" / "loop" / "compile_report.json").write_text(
        json.dumps(_build_compile_report(summary, adjudication.to_dict()), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def verify_repo(*, repo_root: Path = REPO_ROOT) -> CompileSummary:
    loop_prd = _load_prd(repo_root / ".claude" / "ralph" / "loop" / "prd.json")
    named_prd = _load_prd(repo_root / ".claude" / "ralph" / "prd" / "yanbao-platform-enhancement.json")
    if loop_prd != named_prd:
        raise RuntimeError("dual_prd_mismatch")
    for story in loop_prd.get("userStories") or []:
        notes = parse_notes_payload(story.get("notes"))
        missing = [key for key in NOTE_KEYS if key not in notes]
        if missing:
            raise RuntimeError(f"missing_note_keys:{story.get('id')}:{','.join(missing)}")
    subprocess.run(
        [
            "powershell",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(repo_root / ".claude" / "ralph" / "run-ralph.ps1"),
            "-Tool",
            "claude",
            "-MaxIterations",
            "1",
            "-DryRun",
        ],
        cwd=str(repo_root),
        check=True,
        capture_output=True,
        text=True,
    )
    stories_total = len(loop_prd.get("userStories") or [])
    stories_passed = sum(1 for story in loop_prd.get("userStories") or [] if story.get("passes"))
    return CompileSummary(
        mode="verify",
        changed_docs=[],
        changed_prd=[],
        stories_total=stories_total,
        stories_passed=stories_passed,
        stories_failed=stories_total - stories_passed,
        new_story_ids=[],
        regressed_story_ids=[],
        blocked_external_ids=[],
        story_set_hash=prd_story_set_hash(loop_prd),
    )


def collect_repo_truth(*, repo_root: Path = REPO_ROOT) -> TruthSnapshot:
    return collect_truth_snapshot(repo_root=repo_root, database_path=repo_root / "data" / "app.db")


def adjudicate_repo(*, repo_root: Path = REPO_ROOT) -> CompileSummary:
    truth = collect_repo_truth(repo_root=repo_root)
    current_prd = _load_prd(repo_root / ".claude" / "ralph" / "loop" / "prd.json")
    normalized_prd = normalize_story_list(
        list(current_prd.get("userStories") or []),
        existing_prd=current_prd,
        repo_root=repo_root,
        project=str(current_prd.get("project") or ""),
        branch_name=str(current_prd.get("branchName") or ""),
        description=str(current_prd.get("description") or ""),
    )
    previous_manifest = load_manifest(repo_root / ".claude" / "ralph" / "loop" / "compile_manifest.json")
    adjudicated_prd, adjudication = adjudicate_prd(
        normalized_prd,
        truth_snapshot=truth,
        repo_root=repo_root,
        previous_manifest=previous_manifest,
        generated_at=truth.generated_at,
    )
    changed_prd = []
    if _write_json_if_changed(repo_root / ".claude" / "ralph" / "loop" / "prd.json", adjudicated_prd):
        changed_prd.append(".claude/ralph/loop/prd.json")
    if _write_json_if_changed(repo_root / ".claude" / "ralph" / "prd" / "yanbao-platform-enhancement.json", adjudicated_prd):
        changed_prd.append(".claude/ralph/prd/yanbao-platform-enhancement.json")
    stories_total = len(adjudicated_prd.get("userStories") or [])
    stories_passed = sum(1 for story in adjudicated_prd.get("userStories") or [] if story.get("passes"))
    (repo_root / ".claude" / "ralph" / "loop" / "compile_report.json").write_text(
        json.dumps({"mode": "adjudicate", "adjudication": adjudication.to_dict()}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return CompileSummary(
        mode="adjudicate",
        changed_docs=[],
        changed_prd=changed_prd,
        stories_total=stories_total,
        stories_passed=stories_passed,
        stories_failed=stories_total - stories_passed,
        new_story_ids=[],
        regressed_story_ids=[item.story_id for item in adjudication.decisions if item.decision == "regress_to_false"],
        blocked_external_ids=[item.story_id for item in adjudication.decisions if item.decision == "blocked_external"],
        story_set_hash=prd_story_set_hash(adjudicated_prd),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deterministic Ralph Step-1 compiler")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)

    rebuild = subparsers.add_parser("rebuild")
    rebuild.add_argument("--tool", default="claude")

    subparsers.add_parser("verify")
    collect = subparsers.add_parser("collect")
    collect.add_argument("--json", action="store_true")
    adjudicate = subparsers.add_parser("adjudicate")
    adjudicate.add_argument("--write", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    repo_root = args.repo_root.resolve()
    if args.command == "rebuild":
        summary = rebuild_repo(repo_root=repo_root, tool=args.tool)
        print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))
        return 0
    if args.command == "verify":
        summary = verify_repo(repo_root=repo_root)
        print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))
        return 0
    if args.command == "collect":
        snapshot = collect_repo_truth(repo_root=repo_root)
        print(json.dumps(snapshot.to_dict(), ensure_ascii=False, indent=2))
        return 0
    if args.command == "adjudicate":
        summary = adjudicate_repo(repo_root=repo_root)
        print(json.dumps(summary.to_dict(), ensure_ascii=False, indent=2))
        return 0
    parser.error("unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

