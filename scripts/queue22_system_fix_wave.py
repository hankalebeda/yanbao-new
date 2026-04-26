#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from scripts import codex_mesh


RUN_LABEL = "queue22-20260327b"
CONTROLLER_ID = "controller04"
ACTIVE_LOCK = "docs/_temp/problem/ANALYSIS_MASTER_LOCK__owner-controller04__status-active.md"
PROMPT_DIR = Path("docs/_temp/problem/launch_prompts")
PROBLEM_QUEUE_DIR = Path("docs/_temp/problem")
DEFAULT_BENCHMARK_LABEL = "queue22-system-fix-20260327b"
DEFAULT_TIMEOUT_SECONDS = 35 * 60
DEFAULT_MAX_WORKERS = 6
DEFAULT_PROVIDER_ALLOWLIST = [
    "119.8.113.226",
    "api.925214.xyz",
    "freeapi.dgbmc.top",
    "wududu.edu.kg",
]
TRUTH_LAYER_FILES = [
    "docs/_temp/problem/README.md",
    "docs/_temp/problem/SLOT_OCCUPANCY.md",
    ACTIVE_LOCK,
]
ISSUE_MINER_TASK_ID = "slot04-issue-miner"
ISSUE_MINER_EXPORT_ROOT = Path("output/task_runs") / RUN_LABEL / "issue_mining"
WAVE_EXPORT_ROOT = Path("output/task_runs") / RUN_LABEL / "waves"
RUNTIME_EXECUTOR_TASK_ID = "20260326-111003__P1__runtime__runtime-rearm-executor-r2"
RUNTIME_WATCHDOG_TASK_ID = "20260326-111004__P1__runtime__runtime-rearm-watchdog-r2"
PROMOTE_TASK_ID = "20260326-111000__P1__shared-artifact__post-fix-promote"
ROUND1_EXECUTOR_TASK_ID = "20260326-111001__P1__runtime__runtime-rearm-executor"
ROUND1_WATCHDOG_TASK_ID = "20260326-111002__P1__runtime__runtime-rearm-watchdog"


def repo_root() -> Path:
    return _repo_root


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _resolve_doc(root: Path, pattern: str) -> str:
    matches = sorted((root / "docs" / "core").glob(pattern))
    if not matches:
        raise FileNotFoundError(f"Unable to locate docs/core/{pattern}")
    return matches[0].relative_to(root).as_posix()


def _doc_22(root: Path) -> str:
    return _resolve_doc(root, "22_*v7*.md")


def _doc_25(root: Path) -> str:
    return _resolve_doc(root, "25_*.md")


def _read_prompt(root: Path, prompt_file: str) -> str:
    candidate = root / PROMPT_DIR / prompt_file
    if candidate.exists():
        return candidate.read_text(encoding="utf-8")
    return (_repo_root / PROMPT_DIR / prompt_file).read_text(encoding="utf-8")


def _merge_truth_layer(paths: list[str]) -> list[str]:
    merged: list[str] = []
    for item in [*TRUTH_LAYER_FILES, *paths]:
        if item not in merged:
            merged.append(item)
    return merged


def _resolve_max_workers(requested_max_workers: int) -> int:
    return max(1, min(int(requested_max_workers), DEFAULT_MAX_WORKERS))


def _resolve_queue_task_file(root: Path, task_id: str) -> str:
    pattern = f"{task_id}__owner-*__status-*.md"
    for candidate_root in (root, _repo_root):
        matches = sorted((candidate_root / PROBLEM_QUEUE_DIR).glob(pattern))
        if matches:
            return _normalize_relpath(matches[0], root)
    raise FileNotFoundError(f"Unable to locate active queue task file for {task_id}")


def _support_slot_configs(root: Path) -> list[dict[str, object]]:
    doc_22 = _doc_22(root)
    doc_25 = _doc_25(root)
    runtime_executor_task = _resolve_queue_task_file(root, RUNTIME_EXECUTOR_TASK_ID)
    runtime_watchdog_task = _resolve_queue_task_file(root, RUNTIME_WATCHDOG_TASK_ID)
    round1_executor_task = _resolve_queue_task_file(root, ROUND1_EXECUTOR_TASK_ID)
    round1_watchdog_task = _resolve_queue_task_file(root, ROUND1_WATCHDOG_TASK_ID)
    return [
        {
            "task_id": ISSUE_MINER_TASK_ID,
            "goal": "readonly 25 -> 22 issue mining for controller04",
            "prompt_file": "issue_miner_20260327b.txt",
            "read_scope": [
                doc_22,
                doc_25,
                "docs/_temp/problem/",
                "output/junit.xml",
                "app/governance/catalog_snapshot.json",
                "output/blind_spot_audit.json",
                "github/automation/continuous_audit/latest_run.json",
            ],
        },
        {
            "task_id": "slot05-prompt-task-auditor",
            "goal": "readonly prompt, task-definition, and queue protocol audit",
            "prompt_file": "prompt_task_auditor_20260327b.txt",
            "read_scope": [
                doc_22,
                "docs/_temp/problem/",
                "docs/_temp/problem/launch_prompts/",
            ],
        },
        {
            "task_id": "slot06-claim-resource-guard",
            "goal": "readonly claim, write_scope, and resource_scope guard",
            "prompt_file": "claim_resource_guard_20260327b.txt",
            "read_scope": [
                "docs/_temp/problem/",
                "docs/_temp/problem/_claims/",
            ],
        },
        {
            "task_id": "slot07-runtime-evidence-verifier",
            "goal": "readonly runtime evidence verification for 111003 and 111004",
            "prompt_file": "runtime_evidence_verifier_20260327b.txt",
            "read_scope": [
                runtime_executor_task,
                runtime_watchdog_task,
                round1_executor_task,
                round1_watchdog_task,
                "output/task_runs/20260326-111001__P1__runtime__runtime-rearm-executor/",
                "output/task_runs/20260326-111002__P1__runtime__runtime-rearm-watchdog/",
                "output/task_runs/20260326-111003__P1__runtime__runtime-rearm-executor-r2/",
                "output/task_runs/20260326-111004__P1__runtime__runtime-rearm-watchdog-r2/",
            ],
        },
    ]


def _backlog_prep_slot_configs(root: Path) -> list[dict[str, object]]:
    doc_22 = _doc_22(root)
    promote_task = _resolve_queue_task_file(root, PROMOTE_TASK_ID)
    common_scope = [
        doc_22,
        promote_task,
        "docs/_temp/problem/",
        "output/junit.xml",
        "app/governance/catalog_snapshot.json",
        "output/blind_spot_audit.json",
        "github/automation/continuous_audit/latest_run.json",
    ]
    return [
        {
            "task_id": "slot08-backlog-task-slicer-a",
            "goal": "readonly backlog task slicing for first-wave fixer candidates",
            "prompt_file": "backlog_task_slicer_20260327b.txt",
            "read_scope": list(common_scope),
        },
        {
            "task_id": "slot09-backlog-task-slicer-b",
            "goal": "readonly backlog task slicing for first-wave verifier candidates",
            "prompt_file": "backlog_task_slicer_20260327b.txt",
            "read_scope": list(common_scope),
        },
        {
            "task_id": "slot10-backlog-conflict-checker",
            "goal": "readonly backlog conflict and single-writer guard",
            "prompt_file": "backlog_conflict_checker_20260327b.txt",
            "read_scope": list(common_scope),
        },
        {
            "task_id": "slot11-backlog-test-prep-a",
            "goal": "readonly backlog scoped-test readiness audit",
            "prompt_file": "backlog_test_prep_20260327b.txt",
            "read_scope": list(common_scope),
        },
        {
            "task_id": "slot12-backlog-test-prep-b",
            "goal": "readonly backlog artifact/test readiness audit",
            "prompt_file": "backlog_test_prep_20260327b.txt",
            "read_scope": list(common_scope),
        },
        {
            "task_id": "slot13-backlog-review-planner",
            "goal": "readonly backlog verifier and review planning",
            "prompt_file": "backlog_review_planner_20260327b.txt",
            "read_scope": list(common_scope),
        },
    ]


def _build_analysis_tasks(
    root: Path,
    slot_configs: list[dict[str, object]],
    *,
    provider_allowlist: list[str],
    timeout_seconds: int,
    benchmark_label: str,
) -> list[codex_mesh.MeshTaskManifest]:
    tasks: list[codex_mesh.MeshTaskManifest] = []
    for item in slot_configs:
        tasks.append(
            codex_mesh.MeshTaskManifest(
                task_id=str(item["task_id"]),
                goal=str(item["goal"]),
                prompt=_read_prompt(root, str(item["prompt_file"])),
                task_kind="analysis",
                read_scope=_merge_truth_layer(list(item["read_scope"])),
                write_scope=[],
                provider_allowlist=list(provider_allowlist),
                provider_denylist=[],
                timeout_seconds=timeout_seconds,
                benchmark_label=benchmark_label,
                output_mode="text",
                working_root=str(root),
                allow_native_subagents=True,
            )
        )
    return tasks


def _strip_ticks(raw: str) -> str:
    value = raw.strip()
    if value.startswith("`") and value.endswith("`") and len(value) >= 2:
        return value[1:-1]
    return value


def _task_field(text: str, field: str) -> str:
    match = re.search(rf"^- `{re.escape(field)}`:\s*(.*)$", text, flags=re.MULTILINE)
    if not match:
        return ""
    return _strip_ticks(match.group(1))


def _json_list_field(text: str, field: str) -> list[str]:
    raw = _task_field(text, field)
    if not raw:
        return []
    parsed = json.loads(raw)
    if not isinstance(parsed, list):
        raise ValueError(f"{field} must be a JSON list")
    return [str(item).replace("\\", "/") for item in parsed]


def _normalize_relpath(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _build_backlog_fix_prompt(task_file_rel: str, task_text: str) -> str:
    return (
        "You are a Queue22 backlog fixer running in an isolated codex mesh workspace.\n"
        "Truth boundary:\n"
        "- docs/_temp/problem/README.md\n"
        "- docs/_temp/problem/SLOT_OCCUPANCY.md\n"
        f"- {ACTIVE_LOCK}\n"
        f"- {task_file_rel}\n\n"
        "Hard rules:\n"
        "- Treat the task file as the execution contract.\n"
        "- Do not edit docs/core/22_全量功能进度总表_v7_精审.md, docs/_temp/problem/WRITEBACK_JOURNAL.md, or official shared artifact paths.\n"
        "- Do not edit any sibling task file.\n"
        "- Keep all temporary evidence under the task's task_run_dir / attempt_run_dir and write a resolution.md there.\n"
        "- resolution.md must contain the headings: 问题结论, 修改范围, 真实测试结果, 建议写回文案, 残余风险.\n"
        "- Run the task acceptance_tests and report the real results honestly.\n"
        "- If the task contract conflicts with reality, fail closed and explain the blocker.\n\n"
        "Task file follows:\n\n"
        f"{task_text}\n"
    )


def _build_backlog_fix_prompt_v2(task_file_rel: str, task_text: str) -> str:
    return (
        "You are a Queue22 backlog fixer running in an isolated codex mesh workspace.\n"
        "Truth boundary:\n"
        "- docs/_temp/problem/README.md\n"
        "- docs/_temp/problem/SLOT_OCCUPANCY.md\n"
        f"- {ACTIVE_LOCK}\n"
        f"- {task_file_rel}\n\n"
        "Hard rules:\n"
        "- Confirm the task remains claimable only after `Backlog-Open` before making any write.\n"
        "- Treat the task file as the execution contract.\n"
        "- Do not edit docs/core/22_全量功能进度总表_v7_精审.md, docs/_temp/problem/WRITEBACK_JOURNAL.md, or official shared artifact paths.\n"
        "- Do not edit any sibling task file.\n"
        "- Keep all temporary evidence under the task's task_run_dir / attempt_run_dir and write a resolution.md there.\n"
        "- resolution.md must contain the headings: 问题结论, 修改范围, 真实测试结果, 建议写回文案, 残余风险.\n"
        "- Run the task acceptance_tests and report the real results honestly.\n"
        "- If the task contract conflicts with reality, fail closed and explain the blocker.\n\n"
        "Task file follows:\n\n"
        f"{task_text}\n"
    )


def _build_backlog_fix_tasks(
    root: Path,
    task_files: list[Path],
    *,
    provider_allowlist: list[str],
    timeout_seconds: int,
    benchmark_label: str,
) -> list[codex_mesh.MeshTaskManifest]:
    if not task_files:
        raise ValueError("backlog_fix requires at least one --task-file")

    tasks: list[codex_mesh.MeshTaskManifest] = []
    for task_file in task_files:
        text = task_file.read_text(encoding="utf-8")
        task_id = _task_field(text, "task_id")
        title = _task_field(text, "title") or task_id
        control_state_required = _task_field(text, "control_state_required")
        if control_state_required != "Backlog-Open":
            raise ValueError(f"{task_file} is not claimable in Backlog-Open")
        write_scope = _json_list_field(text, "write_scope")
        if not write_scope:
            raise ValueError(f"{task_file} has empty write_scope; backlog_fix requires isolated write tasks")
        task_run_dir = _task_field(text, "task_run_dir")
        related_files = _json_list_field(text, "related_files")
        task_file_rel = _normalize_relpath(task_file, root)
        augmented_write_scope = [task_file_rel, *write_scope]
        if task_run_dir:
            augmented_write_scope.append(task_run_dir.rstrip("/") + "/")
        read_scope = _merge_truth_layer([task_file_rel, "docs/core/"] + related_files)
        tasks.append(
            codex_mesh.MeshTaskManifest(
                task_id=task_id,
                goal=title,
                prompt=_build_backlog_fix_prompt_v2(task_file_rel, text),
                task_kind="write",
                read_scope=read_scope,
                write_scope=augmented_write_scope,
                provider_allowlist=list(provider_allowlist),
                provider_denylist=[],
                timeout_seconds=timeout_seconds,
                benchmark_label=benchmark_label,
                output_mode="text",
                working_root=str(root),
                allow_native_subagents=True,
            )
        )
    return tasks


def build_manifest(
    root: Path,
    *,
    profile: str = "support",
    providers: list[str] | None = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    benchmark_label: str = DEFAULT_BENCHMARK_LABEL,
    task_files: list[Path] | None = None,
) -> codex_mesh.MeshRunManifest:
    provider_allowlist = list(providers or DEFAULT_PROVIDER_ALLOWLIST)
    if profile == "support":
        tasks = _build_analysis_tasks(
            root,
            _support_slot_configs(root),
            provider_allowlist=provider_allowlist,
            timeout_seconds=timeout_seconds,
            benchmark_label=benchmark_label,
        )
        return codex_mesh.MeshRunManifest(
            tasks=tasks,
            execution_mode="mesh",
            max_workers=_resolve_max_workers(max_workers),
            benchmark_label=benchmark_label,
            provider_allowlist=provider_allowlist,
            provider_denylist=[],
        )
    if profile == "backlog_prep":
        tasks = _build_analysis_tasks(
            root,
            _backlog_prep_slot_configs(root),
            provider_allowlist=provider_allowlist,
            timeout_seconds=timeout_seconds,
            benchmark_label=benchmark_label,
        )
        return codex_mesh.MeshRunManifest(
            tasks=tasks,
            execution_mode="mesh",
            max_workers=_resolve_max_workers(max_workers),
            benchmark_label=benchmark_label,
            provider_allowlist=provider_allowlist,
            provider_denylist=[],
        )
    if profile == "backlog_fix":
        tasks = _build_backlog_fix_tasks(
            root,
            list(task_files or []),
            provider_allowlist=provider_allowlist,
            timeout_seconds=timeout_seconds,
            benchmark_label=benchmark_label,
        )
        return codex_mesh.MeshRunManifest(
            tasks=tasks,
            execution_mode="mesh",
            max_workers=_resolve_max_workers(max_workers),
            benchmark_label=benchmark_label,
            dangerously_bypass=True,
            sandbox="danger-full-access",
            ephemeral=True,
            provider_allowlist=provider_allowlist,
            provider_denylist=[],
        )
    raise ValueError(f"Unsupported profile: {profile}")


def manifest_payload(manifest: codex_mesh.MeshRunManifest) -> dict[str, object]:
    return {
        "execution_mode": manifest.execution_mode,
        "max_workers": manifest.max_workers,
        "benchmark_label": manifest.benchmark_label,
        "dangerously_bypass": manifest.dangerously_bypass,
        "sandbox": manifest.sandbox,
        "ephemeral": manifest.ephemeral,
        "provider_allowlist": manifest.provider_allowlist,
        "provider_denylist": manifest.provider_denylist,
        "tasks": [asdict(task) for task in manifest.tasks],
    }


def _find_last_message(task: codex_mesh.MeshTaskResult) -> tuple[str | None, str]:
    if task.selected_provider:
        for attempt in task.attempts:
            if attempt.provider == task.selected_provider:
                path = Path(attempt.last_message_path)
                return attempt.provider, path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""
    for attempt in reversed(task.attempts):
        path = Path(attempt.last_message_path)
        if path.exists():
            return attempt.provider, path.read_text(encoding="utf-8", errors="replace")
    return None, ""


def _load_json_from_message(message: str) -> Any:
    text = message.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if value in (None, ""):
        return []
    return [str(value)]


def _normalize_finding(item: Any) -> dict[str, Any]:
    source = item if isinstance(item, dict) else {}
    return {
        "problem": str(source.get("problem", "")).strip(),
        "evidence": _string_list(source.get("evidence")),
        "suggested_task_title": str(source.get("suggested_task_title", "")).strip(),
        "write_scope": _string_list(source.get("write_scope")),
        "resource_scope": _string_list(source.get("resource_scope")),
        "acceptance_tests": _string_list(source.get("acceptance_tests")),
        "writeback_target": str(source.get("writeback_target", "")).strip(),
        "priority": str(source.get("priority", "P2")).strip() or "P2",
        "control_state_required": str(source.get("control_state_required", "Backlog-Open")).strip() or "Backlog-Open",
        "suggested_workspace_mode": str(source.get("suggested_workspace_mode", "isolated")).strip() or "isolated",
    }


def _export_issue_miner_findings(root: Path, summary: codex_mesh.MeshRunSummary) -> Path | None:
    issue_miner = next((task for task in summary.tasks if task.task_id == ISSUE_MINER_TASK_ID), None)
    if issue_miner is None:
        return None
    provider_id, message = _find_last_message(issue_miner)
    findings: list[dict[str, Any]] = []
    if message.strip():
        try:
            payload = _load_json_from_message(message)
            raw_findings = payload.get("findings", []) if isinstance(payload, dict) else payload
            if isinstance(raw_findings, list):
                findings = [_normalize_finding(item) for item in raw_findings]
        except Exception:
            findings = []
    export_path = root / ISSUE_MINER_EXPORT_ROOT / summary.run_id / "findings.json"
    _write_json(
        export_path,
        {
            "run_label": RUN_LABEL,
            "session_id": summary.run_id,
            "provider_id": provider_id,
            "generated_at": summary.finished_at,
            "findings": findings,
        },
    )
    return export_path


def _export_wave_summary(root: Path, profile: str, summary: codex_mesh.MeshRunSummary) -> Path:
    export_path = root / WAVE_EXPORT_ROOT / profile / summary.run_id / "summary.json"
    _write_json(export_path, asdict(summary))
    return export_path


def export_run_artifacts(
    root: Path,
    *,
    profile: str,
    summary: codex_mesh.MeshRunSummary,
) -> dict[str, str]:
    exports = {"wave_summary": str(_export_wave_summary(root, profile, summary))}
    findings_path = _export_issue_miner_findings(root, summary)
    if findings_path is not None:
        exports["issue_miner_findings"] = str(findings_path)
    return exports


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launch Queue22 controller04 system-fix waves via codex mesh."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command in ("print-manifest", "run"):
        sub = subparsers.add_parser(command)
        sub.add_argument("--repo-root", type=Path, default=repo_root())
        sub.add_argument("--profile", choices=("support", "backlog_prep", "backlog_fix"), default="support")
        sub.add_argument("--provider", action="append", dest="providers")
        sub.add_argument("--task-file", action="append", dest="task_files", type=Path)
        sub.add_argument("--max-workers", type=int)
        sub.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
        sub.add_argument("--benchmark-label", default=DEFAULT_BENCHMARK_LABEL)
        sub.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def _validate_cli_launch_contract(*, providers: list[str] | None, max_workers: int | None) -> str | None:
    if not providers:
        return "Queue22 controller04 launches must pass --provider explicitly; default provider fallback is forbidden."
    if max_workers != DEFAULT_MAX_WORKERS:
        return f"Queue22 controller04 launches must pass --max-workers {DEFAULT_MAX_WORKERS} explicitly."
    return None


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    launch_error = _validate_cli_launch_contract(providers=args.providers, max_workers=args.max_workers)
    if launch_error:
        print(launch_error, file=sys.stderr)
        return 2
    root = args.repo_root.resolve()
    task_files = [path.resolve() for path in (args.task_files or [])]
    manifest = build_manifest(
        root,
        profile=args.profile,
        providers=args.providers,
        max_workers=args.max_workers or DEFAULT_MAX_WORKERS,
        timeout_seconds=args.timeout_seconds,
        benchmark_label=args.benchmark_label,
        task_files=task_files,
    )
    if args.command == "print-manifest":
        print(json.dumps(manifest_payload(manifest), ensure_ascii=False, indent=2))
        return 0

    summary = codex_mesh.execute_manifest(root, manifest)
    exports = export_run_artifacts(root, profile=args.profile, summary=summary)
    payload = {
        "run_id": summary.run_id,
        "success": summary.success,
        "manifest_path": summary.manifest_path,
        "output_dir": summary.output_dir,
        "task_count": summary.task_count,
        "exports": exports,
        "tasks": [asdict(task) for task in summary.tasks],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"run_id={summary.run_id}")
        print(f"success={summary.success}")
        print(f"manifest_path={summary.manifest_path}")
        print(f"output_dir={summary.output_dir}")
        print(f"task_count={summary.task_count}")
        for key, value in exports.items():
            print(f"{key}={value}")
    return 0 if summary.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
