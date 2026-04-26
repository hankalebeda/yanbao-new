from __future__ import annotations

import json
from pathlib import Path

import pytest
from scripts import codex_mesh
from scripts import queue22_system_fix_wave

_infra_missing = pytest.mark.xfail(
    reason="docs/_temp/ 任务文件未部署到工作区", strict=False)


@_infra_missing
def test_support_manifest_uses_controller04_truth_layer_and_default_providers():
    root = queue22_system_fix_wave.repo_root()

    manifest = queue22_system_fix_wave.build_manifest(root, profile="support")

    assert manifest.execution_mode == "mesh"
    assert manifest.max_workers == 6
    assert manifest.provider_allowlist == queue22_system_fix_wave.DEFAULT_PROVIDER_ALLOWLIST
    assert [task.task_id for task in manifest.tasks] == [
        "slot04-issue-miner",
        "slot05-prompt-task-auditor",
        "slot06-claim-resource-guard",
        "slot07-runtime-evidence-verifier",
    ]
    assert all(task.task_kind == "analysis" for task in manifest.tasks)
    assert all(task.write_scope == [] for task in manifest.tasks)
    assert all(queue22_system_fix_wave.ACTIVE_LOCK in task.read_scope for task in manifest.tasks)


@_infra_missing
def test_support_manifest_resolves_current_runtime_task_files():
    root = queue22_system_fix_wave.repo_root()

    manifest = queue22_system_fix_wave.build_manifest(root, profile="support")

    verifier = next(task for task in manifest.tasks if task.task_id == "slot07-runtime-evidence-verifier")
    runtime_task_paths = [path for path in verifier.read_scope if "20260326-11100" in path and path.endswith(".md")]
    active_watchdog_task = queue22_system_fix_wave._resolve_queue_task_file(
        root, queue22_system_fix_wave.RUNTIME_WATCHDOG_TASK_ID
    )

    assert runtime_task_paths
    assert any("111003__P1__runtime__runtime-rearm-executor-r2__owner-unassigned__status-todo.md" in path for path in runtime_task_paths)
    assert active_watchdog_task in runtime_task_paths
    assert all((root / path).exists() for path in runtime_task_paths)


@_infra_missing
def test_backlog_prep_manifest_contains_slot08_to_slot13_readonly_tasks():
    root = queue22_system_fix_wave.repo_root()

    manifest = queue22_system_fix_wave.build_manifest(root, profile="backlog_prep")

    assert manifest.max_workers == 6
    assert [task.task_id for task in manifest.tasks] == [
        "slot08-backlog-task-slicer-a",
        "slot09-backlog-task-slicer-b",
        "slot10-backlog-conflict-checker",
        "slot11-backlog-test-prep-a",
        "slot12-backlog-test-prep-b",
        "slot13-backlog-review-planner",
    ]
    assert all(task.task_kind == "analysis" for task in manifest.tasks)
    assert all(task.write_scope == [] for task in manifest.tasks)
    assert all(queue22_system_fix_wave.ACTIVE_LOCK in task.read_scope for task in manifest.tasks)


def test_backlog_fix_manifest_builds_persistent_write_tasks(tmp_path):
    root = tmp_path
    task_file = root / "docs" / "_temp" / "problem" / "20260399-000001__P2__report__sample__owner-unassigned__status-todo.md"
    task_file.parent.mkdir(parents=True, exist_ok=True)
    task_file.write_text(
        "\n".join(
            [
                "# sample",
                "",
                "- `task_id`: `20260399-000001__P2__report__sample`",
                "- `title`: `Sample backlog task`",
                "- `control_state_required`: `Backlog-Open`",
                "- `write_scope`: `[\"app/sample.py\", \"tests/test_sample.py\"]`",
                "- `related_files`: `[\"app/sample.py\", \"tests/test_sample.py\"]`",
                "- `task_run_dir`: `output/task_runs/20260399-000001__P2__report__sample`",
            ]
        ),
        encoding="utf-8",
    )

    manifest = queue22_system_fix_wave.build_manifest(
        root,
        profile="backlog_fix",
        providers=["119.8.113.226"],
        task_files=[task_file],
    )

    assert manifest.max_workers == 6
    assert manifest.ephemeral is True
    assert manifest.provider_allowlist == ["119.8.113.226"]
    assert len(manifest.tasks) == 1
    task = manifest.tasks[0]
    assert task.task_kind == "write"
    assert "docs/_temp/problem/20260399-000001__P2__report__sample__owner-unassigned__status-todo.md" in task.write_scope
    assert "app/sample.py" in task.write_scope
    assert "output/task_runs/20260399-000001__P2__report__sample/" in task.write_scope
    assert queue22_system_fix_wave.ACTIVE_LOCK in task.read_scope
    assert "Backlog-Open" in task.prompt
    assert "问题结论" in task.prompt


@_infra_missing
def test_prompt_assets_capture_provider_strategy_and_promote_scope():
    root = queue22_system_fix_wave.repo_root()

    controller_prompt = (
        root / "docs" / "_temp" / "problem" / "launch_prompts" / "controller_queue22_20260327b_controller04.txt"
    ).read_text(encoding="utf-8")
    manifest_prompt = (
        root / "docs" / "_temp" / "problem" / "launch_prompts" / "queue22_system_fix_prompt_manifest_20260327b.md"
    ).read_text(encoding="utf-8")
    backlog_fixer_prompt = (
        root / "docs" / "_temp" / "problem" / "launch_prompts" / "backlog_fixer_20260327b.txt"
    ).read_text(encoding="utf-8")
    executor_task = (
        root
        / "docs"
        / "_temp"
        / "problem"
        / "20260326-111003__P1__runtime__runtime-rearm-executor-r2__owner-unassigned__status-todo.md"
    ).read_text(encoding="utf-8")
    watchdog_task = next(
        path.read_text(encoding="utf-8")
        for path in (root / "docs" / "_temp" / "problem").glob("20260326-111004__P1__runtime__runtime-rearm-watchdog-r2__owner-*__status-*.md")
    )
    promote_task = (
        root
        / "docs"
        / "_temp"
        / "problem"
        / "20260326-111000__P1__shared-artifact__post-fix-promote__owner-unassigned__status-todo.md"
    ).read_text(encoding="utf-8")
    readme_text = (root / "docs" / "_temp" / "problem" / "README.md").read_text(encoding="utf-8")

    assert "--max-workers 6" in controller_prompt
    assert "119.8.113.226" in controller_prompt and "api.925214.xyz" in controller_prompt
    assert "Do not drift providers within a single live attempt." in controller_prompt
    assert "Readonly support/backlog-prep allowlist order" in manifest_prompt
    assert "clean-repo `git worktree` preferred" in manifest_prompt
    assert "Confirm the task still requires `Backlog-Open`" in backlog_fixer_prompt
    assert "## 问题结论" in backlog_fixer_prompt
    assert "Preferred order: `119.8.113.226` -> `api.925214.xyz`" in executor_task
    assert "Preferred order: `api.925214.xyz` -> `119.8.113.226`" in watchdog_task
    assert "refreshes junit, catalog, blind spot, and latest_run" in promote_task
    assert "pytest tests --collect-only" not in promote_task
    assert "`readonly_support_slots=4`" in readme_text
    assert "`reserved_capacity_slots=6`" in readme_text


def test_export_run_artifacts_writes_wave_summary_and_issue_findings(tmp_path):
    last_message_path = tmp_path / "runtime" / "last_message.txt"
    last_message_path.parent.mkdir(parents=True, exist_ok=True)
    last_message_path.write_text(
        json.dumps(
            {
                "findings": [
                    {
                        "problem": "Sample problem",
                        "evidence": ["e1"],
                        "suggested_task_title": "Fix sample",
                        "write_scope": ["app/sample.py"],
                        "resource_scope": ["sample:write"],
                        "acceptance_tests": ["python -m pytest tests/test_sample.py -q"],
                        "writeback_target": "22 2.3",
                        "priority": "P2",
                        "control_state_required": "Backlog-Open",
                        "suggested_workspace_mode": "isolated",
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    attempt = codex_mesh.ProviderAttemptResult(
        provider="119.8.113.226",
        command=["codex", "exec"],
        returncode=0,
        stdout_path=str(tmp_path / "runtime" / "stdout.jsonl"),
        stderr_path=str(tmp_path / "runtime" / "stderr.log"),
        last_message_path=str(last_message_path),
        duration_seconds=1.0,
        ok=True,
        status="success",
        started_at="2026-03-27T02:00:00+00:00",
        finished_at="2026-03-27T02:00:01+00:00",
    )
    task = codex_mesh.MeshTaskResult(
        task_id="slot04-issue-miner",
        goal="readonly mining",
        task_kind="analysis",
        success=True,
        selected_provider="119.8.113.226",
        output_mode="text",
        provider_order=["119.8.113.226"],
        attempts=[attempt],
        execution_root=str(tmp_path),
        workspace_kind="shared",
        depth=1,
        parent_task_id=None,
        lineage_id="slot04-issue-miner",
        started_at="2026-03-27T02:00:00+00:00",
        finished_at="2026-03-27T02:00:01+00:00",
    )
    summary = codex_mesh.MeshRunSummary(
        run_id="20260327T102500",
        execution_mode="mesh",
        success=True,
        task_count=1,
        max_workers=4,
        benchmark_label="queue22-system-fix-20260327b",
        manifest_path=str(tmp_path / "runtime" / "manifest.json"),
        output_dir=str(tmp_path / "runtime" / "output"),
        tasks=[task],
        provider_health=[],
        started_at="2026-03-27T02:00:00+00:00",
        finished_at="2026-03-27T02:00:01+00:00",
    )

    exports = queue22_system_fix_wave.export_run_artifacts(tmp_path, profile="support", summary=summary)

    summary_path = Path(exports["wave_summary"])
    findings_path = Path(exports["issue_miner_findings"])
    assert summary_path.exists()
    assert findings_path.exists()

    findings_payload = json.loads(findings_path.read_text(encoding="utf-8"))
    assert findings_payload["run_label"] == "queue22-20260327b"
    assert findings_payload["session_id"] == "20260327T102500"
    assert findings_payload["provider_id"] == "119.8.113.226"
    assert findings_payload["findings"][0]["problem"] == "Sample problem"


def test_main_requires_explicit_providers(capsys) -> None:
    code = queue22_system_fix_wave.main(
        [
            "print-manifest",
            "--profile",
            "support",
            "--max-workers",
            "6",
        ]
    )

    captured = capsys.readouterr()
    assert code == 2
    assert "--provider explicitly" in captured.err


def test_main_requires_explicit_max_workers(capsys) -> None:
    code = queue22_system_fix_wave.main(
        [
            "print-manifest",
            "--profile",
            "support",
            "--provider",
            "119.8.113.226",
            "--max-workers",
            "5",
        ]
    )

    captured = capsys.readouterr()
    assert code == 2
    assert "--max-workers 6 explicitly" in captured.err
