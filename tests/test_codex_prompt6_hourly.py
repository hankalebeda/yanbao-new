"""Tests for the unified codex runner and backward-compatible wrappers."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from codex import run as codex_run
from codex import mesh as codex_mesh


def test_load_profile_hourly():
    profile = codex_run.load_profile("hourly")
    assert profile["task_id_prefix"] == "prompt6"
    assert profile["task_kind"] == "mixed"
    assert profile["allow_native_subagents"] is True
    assert profile["allow_native_subagents_at_external_limit"] is True
    assert profile["inner_agent_max_depth"] == 1
    assert profile["inner_agent_max_threads"] == 4
    assert profile["ensure_runtime"] is True
    assert profile["lock"] is True
    assert "app" in profile["read_scope"]


def test_load_profile_analysis():
    profile = codex_run.load_profile("analysis")
    assert profile["task_id_prefix"] == "prompt6-analysis"
    assert profile["task_kind"] == "analysis"
    assert profile["allow_native_subagents_at_external_limit"] is True
    assert profile["ensure_runtime"] is False


def test_load_profile_mining():
    profile = codex_run.load_profile("mining")
    assert profile["task_id_prefix"] == "issue-mining-22"
    assert profile["task_kind"] == "write"
    assert profile["allow_native_subagents_at_external_limit"] is True
    assert profile["prompt_source"]["builder"] == "mining"


def test_extract_fenced_text():
    doc = "## Heading\n\n```text\nhello world\n```\n"
    result = codex_run.extract_fenced_text(doc, "## Heading")
    assert result == "hello world"


def test_build_context_overlay_structural_only():
    overlay = codex_run.build_context_overlay(
        run_id="run-1",
        task_id="task-1",
        goal="test goal",
        depth=1,
        max_depth=2,
        execution_root=Path("/repo"),
        provider_order=["sub.jlypx.de", "infiniteai.cc"],
        read_scope=["app"],
        write_scope=["app"],
    )
    assert "run_id: run-1" in overlay
    assert "depth: 1/2" in overlay
    assert "subagents: enabled" in overlay
    assert "nested_codex: allowed" in overlay
    # Must NOT contain tactical instructions
    assert "must" not in overlay.lower()
    assert "should" not in overlay.lower()
    assert "delegate" not in overlay.lower()


def test_build_context_overlay_at_max_depth():
    overlay = codex_run.build_context_overlay(
        run_id="run-1",
        task_id="task-1",
        goal="test",
        depth=2,
        max_depth=2,
        execution_root=Path("/repo"),
        provider_order=["sub.jlypx.de"],
        read_scope=[],
        write_scope=[],
        subagents_enabled=False,
        parent_task_id="parent-task",
        lineage_id="lineage-1",
    )
    assert "subagents: disabled" in overlay
    assert "nested_codex: not_allowed" in overlay
    assert "parent_task_id: parent-task" in overlay
    assert "lineage_id: lineage-1" in overlay


def test_rotate_providers(tmp_path):
    state_file = tmp_path / "state.json"
    providers = ["sub.jlypx.de", "infiniteai.cc", "ai.qaq.al"]

    first = codex_run.rotate_providers(providers, state_file)
    second = codex_run.rotate_providers(providers, state_file)
    third = codex_run.rotate_providers(providers, state_file)

    assert first == ["sub.jlypx.de", "infiniteai.cc", "ai.qaq.al"]
    assert second == ["infiniteai.cc", "ai.qaq.al", "sub.jlypx.de"]
    assert third == ["ai.qaq.al", "sub.jlypx.de", "infiniteai.cc"]


def test_execute_dry_run(tmp_path, monkeypatch):
    profile = codex_run.load_profile("hourly")
    profile["ensure_runtime"] = False
    profile["lock"] = False

    # Provide a prompt override so we don't need the actual prompt doc
    result = codex_run.execute(
        profile,
        root=tmp_path,
        prompt_override="test prompt body",
        dry_run=True,
    )

    assert result["success"] is True
    assert result["dry_run"] is True
    assert result["profile"] == "prompt6"


def test_execute_dry_run_can_skip_overlay(tmp_path):
    profile = codex_run.load_profile("hourly")
    profile["ensure_runtime"] = False
    profile["lock"] = False

    result = codex_run.execute(
        profile,
        root=tmp_path,
        prompt_override="plain prompt body",
        dry_run=True,
        include_overlay=False,
    )

    prompt_text = (Path(result["output_dir"]) / "prompt.txt").read_text(encoding="utf-8")
    assert prompt_text == "plain prompt body\n"


def test_execute_mesh_delegate(tmp_path, monkeypatch):
    """Test mesh delegation produces correct manifest and handles result."""
    profile = codex_run.load_profile("hourly")
    profile["ensure_runtime"] = False
    profile["lock"] = False

    seen_manifest = {}

    def fake_execute_manifest(root, manifest):
        seen_manifest["manifest"] = manifest
        return codex_mesh.MeshRunSummary(
            run_id="mesh-run-1",
            execution_mode="mesh",
            success=True,
            task_count=1,
            max_workers=manifest.max_workers,
            benchmark_label=manifest.benchmark_label,
            manifest_path=str(tmp_path / "manifest.json"),
            output_dir=str(tmp_path / "output"),
            tasks=[
                codex_mesh.MeshTaskResult(
                    task_id="prompt6-test",
                    goal="Prompt 6 live fix loop",
                    task_kind="mixed",
                    success=True,
                    selected_provider="sub.jlypx.de",
                    output_mode="text",
                    provider_order=["sub.jlypx.de"],
                    attempts=[
                        codex_mesh.ProviderAttemptResult(
                            provider="sub.jlypx.de",
                            command=["codex", "exec"],
                            returncode=0,
                            stdout_path="stdout.log",
                            stderr_path="stderr.log",
                            last_message_path="last.txt",
                            duration_seconds=1.0,
                            ok=True,
                            status="success",
                        )
                    ],
                    execution_root=str(tmp_path),
                    workspace_kind="copy",
                    depth=1,
                    parent_task_id=None,
                    lineage_id="prompt6-test",
                    started_at="2026-03-25T00:00:00+00:00",
                    finished_at="2026-03-25T00:00:01+00:00",
                )
            ],
            provider_health=[],
            started_at="2026-03-25T00:00:00+00:00",
            finished_at="2026-03-25T00:00:01+00:00",
        )

    monkeypatch.setattr(codex_mesh, "execute_manifest", fake_execute_manifest)

    result = codex_run.execute(
        profile,
        root=tmp_path,
        prompt_override="test prompt",
        delegate_mode="mesh",
    )

    assert result["success"] is True
    assert result["selected_provider"] == "sub.jlypx.de"
    manifest = seen_manifest["manifest"]
    assert manifest.tasks[0].task_kind == "mixed"
    assert manifest.tasks[0].allow_native_subagents is True


def test_execute_mesh_delegate_uses_resolved_default_provider_allowlist(tmp_path, monkeypatch):
    profile = codex_run.load_profile("hourly")
    profile["ensure_runtime"] = False
    profile["lock"] = False
    resolved_allowlist = [
        "sub.jlypx.de",
        "ai.qaq.al",
        "infiniteai.cc",
        "119.8.113.226",
        "freeapi.dgbmc.top",
    ]
    seen_manifest = {}

    monkeypatch.setattr(
        codex_mesh,
        "resolve_provider_allowlist",
        lambda *args, **kwargs: resolved_allowlist,
    )

    def fake_execute_manifest(root, manifest):
        seen_manifest["manifest"] = manifest
        return codex_mesh.MeshRunSummary(
            run_id="mesh-run-2",
            execution_mode="mesh",
            success=True,
            task_count=1,
            max_workers=manifest.max_workers,
            benchmark_label=manifest.benchmark_label,
            manifest_path=str(tmp_path / "manifest.json"),
            output_dir=str(tmp_path / "output"),
            tasks=[
                codex_mesh.MeshTaskResult(
                    task_id="prompt6-test",
                    goal="Prompt 6 live fix loop",
                    task_kind="mixed",
                    success=True,
                    selected_provider="sub.jlypx.de",
                    output_mode="text",
                    provider_order=list(resolved_allowlist),
                    attempts=[],
                    execution_root=str(tmp_path),
                    workspace_kind="copy",
                    depth=1,
                    parent_task_id=None,
                    lineage_id="prompt6-test",
                    started_at="2026-03-25T00:00:00+00:00",
                    finished_at="2026-03-25T00:00:01+00:00",
                )
            ],
            provider_health=[],
            started_at="2026-03-25T00:00:00+00:00",
            finished_at="2026-03-25T00:00:01+00:00",
        )

    monkeypatch.setattr(codex_mesh, "execute_manifest", fake_execute_manifest)

    result = codex_run.execute(
        profile,
        root=tmp_path,
        prompt_override="test prompt",
        delegate_mode="mesh",
    )

    assert result["success"] is True
    manifest = seen_manifest["manifest"]
    assert manifest.provider_allowlist == resolved_allowlist
    assert manifest.tasks[0].provider_allowlist == resolved_allowlist


def test_execute_inherits_external_context_from_parent_env(tmp_path, monkeypatch):
    profile = codex_run.load_profile("hourly")
    profile["ensure_runtime"] = False
    profile["lock"] = False
    seen_manifest = {}

    monkeypatch.setenv("CODEX_MESH_DEPTH", "1")
    monkeypatch.setenv("CODEX_MESH_MAX_DEPTH", "2")
    monkeypatch.setenv("CODEX_MESH_TASK_ID", "parent-task")
    monkeypatch.setenv("CODEX_MESH_LINEAGE_ID", "lineage-1")

    def fake_execute_manifest(root, manifest):
        seen_manifest["manifest"] = manifest
        task = manifest.tasks[0]
        return codex_mesh.MeshRunSummary(
            run_id="mesh-run-3",
            execution_mode="mesh",
            success=True,
            task_count=1,
            max_workers=manifest.max_workers,
            benchmark_label=manifest.benchmark_label,
            manifest_path=str(tmp_path / "manifest.json"),
            output_dir=str(tmp_path / "output"),
            tasks=[
                codex_mesh.MeshTaskResult(
                    task_id=task.task_id,
                    goal=task.goal,
                    task_kind=task.task_kind,
                    success=True,
                    selected_provider="sub.jlypx.de",
                    output_mode="text",
                    provider_order=["sub.jlypx.de"],
                    attempts=[],
                    execution_root=str(tmp_path),
                    workspace_kind="copy",
                    depth=task.depth,
                    parent_task_id=task.parent_task_id,
                    lineage_id=task.lineage_id or task.task_id,
                    started_at="2026-03-25T00:00:00+00:00",
                    finished_at="2026-03-25T00:00:01+00:00",
                )
            ],
            provider_health=[],
            started_at="2026-03-25T00:00:00+00:00",
            finished_at="2026-03-25T00:00:01+00:00",
        )

    monkeypatch.setattr(codex_mesh, "execute_manifest", fake_execute_manifest)

    result = codex_run.execute(
        profile,
        root=tmp_path,
        prompt_override="test prompt",
        delegate_mode="mesh",
        max_depth=5,
    )

    assert result["success"] is True
    manifest = seen_manifest["manifest"]
    task = manifest.tasks[0]
    assert task.depth == 2
    assert task.max_external_depth == 2
    assert task.parent_task_id == "parent-task"
    assert task.lineage_id == "lineage-1"
    assert task.allow_native_subagents is True
    assert task.allow_native_subagents_at_external_limit is True
    assert task.inner_agent_max_depth == 1
    assert task.inner_agent_max_threads == 4
    assert "depth: 2/2" in task.prompt
    assert "subagents: enabled" in task.prompt
    assert "parent_task_id: parent-task" in task.prompt
    assert "lineage_id: lineage-1" in task.prompt


def test_execute_returns_external_depth_limit_without_invoking_mesh(tmp_path, monkeypatch):
    profile = codex_run.load_profile("hourly")
    profile["ensure_runtime"] = False
    profile["lock"] = False

    monkeypatch.setenv("CODEX_MESH_DEPTH", "2")
    monkeypatch.setenv("CODEX_MESH_MAX_DEPTH", "2")
    monkeypatch.setenv("CODEX_MESH_TASK_ID", "parent-task")
    monkeypatch.setattr(
        codex_mesh,
        "execute_manifest",
        lambda *args, **kwargs: pytest.fail("execute_manifest should not be called at external depth limit"),
    )

    result = codex_run.execute(
        profile,
        root=tmp_path,
        prompt_override="test prompt",
        delegate_mode="mesh",
    )

    assert result["success"] is False
    assert result["error"] == "external_depth_limit"


def test_automation_lock(tmp_path):
    lock_path = tmp_path / "test.lock"
    with codex_run.AutomationLock(lock_path):
        assert lock_path.exists()
    assert not lock_path.exists()


def test_automation_lock_blocks_concurrent(tmp_path):
    lock_path = tmp_path / "test.lock"
    with codex_run.AutomationLock(lock_path):
        with pytest.raises(codex_run.LockBusyError, match="lock file already exists"):
            codex_run.AutomationLock(lock_path).__enter__()
