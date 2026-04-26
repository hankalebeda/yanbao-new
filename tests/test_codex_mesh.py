from __future__ import annotations

import json
import threading
import time
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import codex_mesh


def _write_provider(root: Path, launcher: str, provider_name: str, *, multi_agent: bool = True, with_auth: bool = True) -> None:
    (root / f"open-with-{launcher}.cmd").write_text("@echo off\n", encoding="utf-8")
    (root / f"open-with-{launcher}.ps1").write_text(
        f'$base = Join-Path $projectRoot "ai-api\\codex\\{provider_name}"\n',
        encoding="utf-8",
    )
    provider_home = root / "ai-api" / "codex" / provider_name
    provider_home.mkdir(parents=True, exist_ok=True)
    (provider_home / "config.toml").write_text(
        "\n".join(
            [
                'model = "gpt-5.4"',
                'review_model = "gpt-5.2"',
                '[features]',
                f"multi_agent = {'true' if multi_agent else 'false'}",
            ]
        ),
        encoding="utf-8",
    )
    if with_auth:
        (provider_home / "auth.json").write_text(json.dumps({"OPENAI_API_KEY": "sk-test"}), encoding="utf-8")


def _summary_for(run_id: str, *, duration: float, success: bool = True) -> codex_mesh.MeshRunSummary:
    attempt = codex_mesh.ProviderAttemptResult(
        provider="sub.jlypx.de",
        command=["codex", "exec"],
        returncode=0 if success else 1,
        stdout_path="stdout.log",
        stderr_path="stderr.log",
        last_message_path="last_message.txt",
        duration_seconds=duration,
        ok=success,
        status="success" if success else "failed",
    )
    task = codex_mesh.MeshTaskResult(
        task_id=f"task-{run_id}",
        goal="benchmark",
        task_kind="analysis",
        success=success,
        selected_provider="sub.jlypx.de" if success else None,
        output_mode="text",
        provider_order=["sub.jlypx.de"],
        attempts=[attempt],
        execution_root=".",
        workspace_kind="shared",
        depth=1,
        parent_task_id=None,
        lineage_id=f"task-{run_id}",
        started_at="2026-03-24T00:00:00+00:00",
        finished_at="2026-03-24T00:00:01+00:00",
    )
    return codex_mesh.MeshRunSummary(
        run_id=run_id,
        execution_mode="mesh",
        success=success,
        task_count=1,
        max_workers=1,
        benchmark_label="analysis_review",
        manifest_path="manifest.json",
        output_dir="runtime/codex_mesh/runs/test",
        tasks=[task],
        provider_health=[],
        started_at="2026-03-24T00:00:00+00:00",
        finished_at="2026-03-24T00:00:01+00:00",
    )


def test_discover_providers_uses_launchers_and_excludes_denied(tmp_path):
    _write_provider(tmp_path, "subjlypx", "sub.jlypx.de", multi_agent=True, with_auth=True)
    _write_provider(tmp_path, "snew145678", "snew.145678.xyz", multi_agent=False, with_auth=True)
    _write_provider(tmp_path, "aiqaqal", "ai.qaq.al", multi_agent=True, with_auth=False)
    _write_provider(tmp_path, "infiniteai", "infiniteai.cc", multi_agent=True, with_auth=True)
    _write_provider(tmp_path, "925214", "api.925214.xyz", multi_agent=True, with_auth=True)

    providers = codex_mesh.discover_providers(tmp_path, denylist=["api.925214.xyz"])

    assert {item.provider_name for item in providers} == {"sub.jlypx.de", "infiniteai.cc"}


def test_order_providers_penalizes_cooldown_and_uses_health():
    specs = [
        codex_mesh.ProviderSpec("sub.jlypx.de", "subjlypx", "", "", "", "gpt-5.4", None, "xhigh", True, True),
        codex_mesh.ProviderSpec("ai.qaq.al", "aiqaqal", "", "", "", "gpt-5.4", None, "xhigh", True, True),
    ]
    state = codex_mesh.default_state()
    state["providers"] = {
        "sub.jlypx.de": {
            "attempts": 3,
            "successes": 1,
            "failures": 2,
            "durations": [15.0, 18.0],
            "cooldown_until": time.time() + 60,
        },
        "ai.qaq.al": {
            "attempts": 4,
            "successes": 4,
            "failures": 0,
            "durations": [8.0, 9.0],
            "cooldown_until": 0,
        },
    }

    ordered = codex_mesh.order_providers(specs, state)

    assert [item.provider_name for item in ordered] == ["ai.qaq.al", "sub.jlypx.de"]


def test_resolve_provider_allowlist_selects_five_healthiest_defaults(monkeypatch):
    monkeypatch.delenv("CODEX_AUDIT_GATEWAY_ONLY", raising=False)
    monkeypatch.delenv("CODEX_CANONICAL_PROVIDER", raising=False)
    specs = [
        codex_mesh.ProviderSpec(provider, provider, "", "", "", "gpt-5.4", None, "xhigh", True, True)
        for provider in codex_mesh.DEFAULT_PROVIDER_PRIORITY
    ]
    state = codex_mesh.default_state()
    state["providers"] = {}
    for index, provider in enumerate(codex_mesh.DEFAULT_PROVIDER_PRIORITY):
        state["providers"][provider] = {
            "attempts": 3,
            "successes": 3,
            "failures": 0,
            "durations": [5.0 + index],
            "cooldown_until": 0,
        }
    cooled_provider = codex_mesh.DEFAULT_PROVIDER_PRIORITY[1]
    fallback_provider = codex_mesh.DEFAULT_PROVIDER_PRIORITY[codex_mesh.DEFAULT_PROVIDER_SELECTION_COUNT]
    state["providers"][cooled_provider]["successes"] = 1
    state["providers"][cooled_provider]["failures"] = 1
    state["providers"][cooled_provider]["cooldown_until"] = time.time() + 60

    monkeypatch.setattr(codex_mesh, "discover_providers", lambda *args, **kwargs: specs)

    selected = codex_mesh.resolve_provider_allowlist(state=state)

    assert len(selected) == codex_mesh.DEFAULT_PROVIDER_SELECTION_COUNT
    assert cooled_provider not in selected
    assert fallback_provider in selected


def test_provider_env_includes_depth_and_scope(tmp_path):
    task = codex_mesh.MeshTaskManifest(
        task_id="t-1",
        goal="goal",
        prompt="prompt",
        write_scope=["app/services/a.py"],
        max_external_depth=2,
        depth=2,
    )

    env = codex_mesh._provider_env(
        tmp_path,
        run_id="run-1",
        task=task,
        provider_allowlist=["sub.jlypx.de"],
        provider_denylist=["api.925214.xyz"],
    )

    assert env["CODEX_MESH_RUN_ID"] == "run-1"
    assert env["CODEX_MESH_TASK_ID"] == "t-1"
    assert env["CODEX_MESH_DEPTH"] == "2"
    assert env["CODEX_MESH_MAX_DEPTH"] == "2"
    assert env["CODEX_MESH_PROVIDER_ALLOWLIST"] == "sub.jlypx.de"
    assert "app/services/a.py" in env["CODEX_MESH_WRITE_SCOPE"]
    assert "issue_register.md" in env["CODEX_MESH_SINGLE_WRITER_PATHS"]


def test_build_provider_home_uses_gateway_env_for_canonical_provider(tmp_path, monkeypatch):
    provider_home = tmp_path / "ai-api" / "codex" / "newapi-192.168.232.141-3000"
    provider_home.mkdir(parents=True, exist_ok=True)
    (provider_home / "config.toml").write_text(
        "\n".join(
            [
                'model = "gpt-5.4"',
                'review_model = "gpt-5.2"',
                'model_reasoning_effort = "xhigh"',
                '[features]',
                'multi_agent = true',
            ]
        ),
        encoding="utf-8",
    )
    (provider_home / "auth.json").write_text(json.dumps({"OPENAI_API_KEY": "sk-stale"}), encoding="utf-8")

    provider = codex_mesh.ProviderSpec(
        "newapi-192.168.232.141-3000",
        "newapi-192.168.232.141-3000",
        "",
        "",
        str(provider_home),
        "gpt-5.4",
        "gpt-5.2",
        "xhigh",
        True,
        True,
    )
    monkeypatch.setenv("CODEX_CANONICAL_PROVIDER", "newapi-192.168.232.141-3000")
    monkeypatch.setenv("NEW_API_BASE_URL", "http://192.168.232.141:3000")
    monkeypatch.setenv("NEW_API_TOKEN", "sk-gateway-live")

    portable_home = codex_mesh._build_provider_home(tmp_path, provider)

    config_text = (portable_home / ".codex" / "config.toml").read_text(encoding="utf-8")
    auth_payload = json.loads((portable_home / ".codex" / "auth.json").read_text(encoding="utf-8"))
    assert 'base_url = "http://192.168.232.141:3000/v1"' in config_text
    assert auth_payload["OPENAI_API_KEY"] == "sk-gateway-live"


def test_resolve_external_execution_context_defaults_to_first_external_layer():
    context = codex_mesh.resolve_external_execution_context(3, env={})

    assert context.depth == 1
    assert context.max_external_depth == 3
    assert context.parent_task_id is None
    assert context.lineage_id is None


def test_resolve_external_execution_context_inherits_parent_depth_and_lineage():
    context = codex_mesh.resolve_external_execution_context(
        5,
        env={
            "CODEX_MESH_DEPTH": "1",
            "CODEX_MESH_MAX_DEPTH": "2",
            "CODEX_MESH_TASK_ID": "parent-task",
        },
    )

    assert context.depth == 2
    assert context.max_external_depth == 2
    assert context.parent_task_id == "parent-task"
    assert context.lineage_id == "parent-task"


def test_resolve_external_execution_context_raises_at_limit():
    with pytest.raises(codex_mesh.ExternalDepthLimitError):
        codex_mesh.resolve_external_execution_context(
            4,
            env={
                "CODEX_MESH_DEPTH": "2",
                "CODEX_MESH_MAX_DEPTH": "2",
                "CODEX_MESH_TASK_ID": "parent-task",
                "CODEX_MESH_LINEAGE_ID": "lineage-1",
            },
        )


def test_apply_external_execution_context_env_writes_parent_and_lineage():
    env = {"CODEX_MESH_PARENT_TASK_ID": "stale-parent"}
    context = codex_mesh.ExternalExecutionContext(
        depth=2,
        max_external_depth=3,
        parent_task_id="parent-task",
        lineage_id="lineage-1",
    )

    updated = codex_mesh.apply_external_execution_context_env(
        env,
        run_id="run-1",
        task_id="task-1",
        context=context,
        lineage_id="lineage-override",
        parent_task_id="parent-override",
    )

    assert updated["CODEX_MESH_RUN_ID"] == "run-1"
    assert updated["CODEX_MESH_TASK_ID"] == "task-1"
    assert updated["CODEX_MESH_DEPTH"] == "2"
    assert updated["CODEX_MESH_MAX_DEPTH"] == "3"
    assert updated["CODEX_MESH_PARENT_TASK_ID"] == "parent-override"
    assert updated["CODEX_MESH_LINEAGE_ID"] == "lineage-override"


def test_apply_external_execution_context_env_clears_stale_parent_when_missing():
    env = {"CODEX_MESH_PARENT_TASK_ID": "stale-parent"}
    context = codex_mesh.ExternalExecutionContext(depth=1, max_external_depth=2)

    updated = codex_mesh.apply_external_execution_context_env(
        env,
        run_id="run-1",
        task_id="task-1",
        context=context,
    )

    assert "CODEX_MESH_PARENT_TASK_ID" not in updated
    assert updated["CODEX_MESH_LINEAGE_ID"] == "task-1"


def test_inner_codex_config_overrides_clamp_and_omit_threads():
    overrides = codex_mesh.inner_codex_config_overrides(
        True,
        agent_max_depth=0,
        agent_max_threads=None,
    )

    assert overrides == []


def test_resolve_inner_codex_options_keeps_subagents_by_default_at_external_limit():
    enabled = codex_mesh.resolve_inner_codex_options(
        allow_native_subagents=True,
        depth=1,
        max_external_depth=2,
        agent_max_depth=0,
        agent_max_threads=0,
    )
    still_enabled = codex_mesh.resolve_inner_codex_options(
        allow_native_subagents=True,
        depth=2,
        max_external_depth=2,
    )
    disabled = codex_mesh.resolve_inner_codex_options(
        allow_native_subagents=True,
        allow_native_subagents_at_external_limit=False,
        depth=2,
        max_external_depth=2,
    )

    assert enabled["enable_multi_agent"] is True
    assert enabled["agent_max_depth"] == 1
    assert enabled["agent_max_threads"] == 1
    assert still_enabled["enable_multi_agent"] is True
    assert disabled["enable_multi_agent"] is False


def test_build_codex_command_explicitly_enables_multi_agent_and_inner_limits(tmp_path):
    command = codex_mesh.build_codex_command(
        codex_binary="codex",
        execution_root=tmp_path,
        last_message_path=tmp_path / "last_message.txt",
        enable_multi_agent=True,
        agent_max_depth=2,
        agent_max_threads=3,
        dangerously_bypass=True,
        sandbox="danger-full-access",
        ephemeral=True,
    )

    enable_index = command.index("--enable")
    assert command[enable_index + 1] == "multi_agent"
    assert "agents.max_depth=2" not in command
    assert "agents.max_threads=3" not in command


def test_build_codex_command_explicitly_disables_multi_agent_and_keeps_inner_limits(tmp_path):
    command = codex_mesh.build_codex_command(
        codex_binary="codex",
        execution_root=tmp_path,
        last_message_path=tmp_path / "last_message.txt",
        enable_multi_agent=False,
        agent_max_depth=1,
        agent_max_threads=2,
        dangerously_bypass=True,
        sandbox="danger-full-access",
        ephemeral=True,
    )

    disable_index = command.index("--disable")
    assert command[disable_index + 1] == "multi_agent"
    assert "agents.max_depth=1" not in command
    assert "agents.max_threads=2" not in command


def test_shallow_copytree_skips_transient_dirs_and_copy_errors(tmp_path, monkeypatch):
    source = tmp_path / "source"
    target = tmp_path / "target"
    (source / "app").mkdir(parents=True, exist_ok=True)
    (source / "app" / "main.py").write_text("print('ok')\n", encoding="utf-8")
    (source / "docs").mkdir(parents=True, exist_ok=True)
    (source / "docs" / "keep.md").write_text("keep\n", encoding="utf-8")
    (source / "docs" / "locked.md").write_text("locked\n", encoding="utf-8")
    (source / ".pytest-tmp").mkdir(parents=True, exist_ok=True)
    (source / ".pytest-tmp" / "blocked.txt").write_text("skip\n", encoding="utf-8")
    (source / ".vscode-aiqaqal-userdata" / "Network").mkdir(parents=True, exist_ok=True)
    (source / ".vscode-aiqaqal-userdata" / "Network" / "Cookies").write_text("cookies\n", encoding="utf-8")
    (source / "_archive").mkdir(parents=True, exist_ok=True)
    (source / "_archive" / "old.txt").write_text("old\n", encoding="utf-8")

    real_copy2 = codex_mesh.shutil.copy2

    def flaky_copy2(src, dst, *args, **kwargs):
        if Path(src).name == "locked.md":
            raise PermissionError("locked")
        return real_copy2(src, dst, *args, **kwargs)

    monkeypatch.setattr(codex_mesh.shutil, "copy2", flaky_copy2)

    codex_mesh._shallow_copytree(source, target)

    assert (target / "app" / "main.py").exists()
    assert (target / "docs" / "keep.md").exists()
    assert not (target / "docs" / "locked.md").exists()
    assert not (target / ".pytest-tmp").exists()
    assert not (target / ".vscode-aiqaqal-userdata").exists()
    assert not (target / "_archive").exists()


def test_shallow_copytree_skips_heavy_nested_sources(tmp_path):
    source = tmp_path / "source"
    target = tmp_path / "target"
    (source / "docs" / "core").mkdir(parents=True, exist_ok=True)
    (source / "docs" / "core" / "keep.md").write_text("keep\n", encoding="utf-8")
    (source / "docs" / "old").mkdir(parents=True, exist_ok=True)
    (source / "docs" / "old" / "archive.md").write_text("skip\n", encoding="utf-8")
    (source / "ai-api" / "codex" / "provider").mkdir(parents=True, exist_ok=True)
    (source / "ai-api" / "codex" / "provider" / "logs.sqlite").write_text("skip\n", encoding="utf-8")
    (source / "ai-api" / "webai").mkdir(parents=True, exist_ok=True)
    (source / "ai-api" / "webai" / "keep.txt").write_text("keep\n", encoding="utf-8")

    codex_mesh._shallow_copytree(source, target)

    assert (target / "docs" / "core" / "keep.md").exists()
    assert not (target / "docs" / "old").exists()
    assert not (target / "ai-api" / "codex").exists()
    assert (target / "ai-api" / "webai" / "keep.txt").exists()


def test_ensure_execution_root_cleans_partial_target_when_copy_fails(tmp_path, monkeypatch):
    source = tmp_path / "repo"
    source.mkdir(parents=True, exist_ok=True)
    task = codex_mesh.MeshTaskManifest(
        task_id="copy-fail",
        goal="goal",
        prompt="prompt",
        task_kind="write",
        write_scope=["app/a.py"],
        working_root=str(source),
    )

    def broken_copytree(src: Path, dst: Path) -> None:
        dst.mkdir(parents=True, exist_ok=True)
        (dst / "partial.txt").write_text("partial\n", encoding="utf-8")
        raise OSError("copy_failed")

    monkeypatch.setattr(codex_mesh, "_is_git_root_dirty", lambda root: True)
    monkeypatch.setattr(codex_mesh, "_shallow_copytree", broken_copytree)

    try:
        codex_mesh._ensure_execution_root(tmp_path, "20260324T020000", task)
    except RuntimeError as exc:
        assert "workspace_copy_failed" in str(exc)
    else:
        raise AssertionError("expected workspace copy failure")

    target = tmp_path / "runtime" / "codex_mesh" / "worktrees" / "20260324T020000" / "copy-fail"
    assert not target.exists()


def test_execute_task_cleans_ephemeral_copy_workspace(tmp_path, monkeypatch):
    provider = codex_mesh.ProviderSpec("sub.jlypx.de", "subjlypx", "", "", "", "gpt-5.4", None, "xhigh", True, True)
    execution_root = tmp_path / "runtime" / "codex_mesh" / "worktrees" / "run-1" / "task-1"
    execution_root.mkdir(parents=True, exist_ok=True)
    (execution_root / "temp.txt").write_text("workspace\n", encoding="utf-8")
    task = codex_mesh.MeshTaskManifest(
        task_id="task-1",
        goal="goal",
        prompt="prompt",
        task_kind="mixed",
        write_scope=["app/a.py"],
        provider_allowlist=["sub.jlypx.de"],
    )

    monkeypatch.setattr(codex_mesh, "discover_providers", lambda *args, **kwargs: [provider])
    monkeypatch.setattr(codex_mesh, "order_providers", lambda specs, state: specs)
    monkeypatch.setattr(codex_mesh, "_ensure_execution_root", lambda *args, **kwargs: (execution_root, "copy"))
    monkeypatch.setattr(codex_mesh, "build_mesh_prompt", lambda **kwargs: "prompt")
    monkeypatch.setattr(codex_mesh, "resolve_codex_executable", lambda: "codex")
    monkeypatch.setattr(codex_mesh, "_build_provider_home", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(codex_mesh, "_provider_env", lambda *args, **kwargs: {})
    monkeypatch.setattr(codex_mesh, "build_codex_command", lambda **kwargs: ["codex", "exec"])
    monkeypatch.setattr(
        codex_mesh,
        "_run_attempt_blocking",
        lambda **kwargs: codex_mesh.ProviderAttemptResult(
            provider="sub.jlypx.de",
            command=["codex", "exec"],
            returncode=0,
            stdout_path="stdout.jsonl",
            stderr_path="stderr.log",
            last_message_path="last_message.txt",
            duration_seconds=1.0,
            ok=True,
            status="ok",
            started_at="2026-03-24T00:00:00+00:00",
            finished_at="2026-03-24T00:00:01+00:00",
        ),
    )

    result = codex_mesh._execute_task(
        root=tmp_path,
        run_id="run-1",
        task=task,
        state=codex_mesh.default_state(),
        execution_mode="serial",
        dangerously_bypass=True,
        sandbox="danger-full-access",
        ephemeral=True,
        provider_allowlist=["sub.jlypx.de"],
        provider_denylist=[],
    )

    assert result.success is True
    assert not execution_root.exists()
    assert (tmp_path / "runtime" / "codex_mesh" / "runs" / "run-1" / "tasks" / "task-1" / "summary.json").exists()


def test_execute_with_hedge_uses_nonblocking_result_for_late_cancelled_attempt(tmp_path, monkeypatch):
    providers = [
        codex_mesh.ProviderSpec("sub.jlypx.de", "subjlypx", "", "", "", "gpt-5.4", None, "xhigh", True, True),
        codex_mesh.ProviderSpec("snew.145678.xyz", "snew145678", "", "", "", "gpt-5.4", None, "xhigh", True, True),
    ]
    task = codex_mesh.MeshTaskManifest(
        task_id="hedge",
        goal="goal",
        prompt="prompt",
        task_kind="analysis",
        hedge_delay_seconds=1,
    )
    attempts_by_provider: dict[str, codex_mesh.RunningAttempt] = {}
    join_timeouts: dict[str, list[float | None]] = {}
    monotonic_values = iter([0.0, 2.0, 2.0, 2.0])

    monkeypatch.setattr(codex_mesh, "resolve_codex_executable", lambda: "codex")
    monkeypatch.setattr(codex_mesh, "_build_provider_home", lambda root, provider: tmp_path / "portable")
    monkeypatch.setattr(codex_mesh, "_provider_env", lambda *args, **kwargs: {})
    monkeypatch.setattr(codex_mesh, "build_codex_command", lambda **kwargs: ["codex", "exec"])
    monkeypatch.setattr(codex_mesh.time, "monotonic", lambda: next(monotonic_values))

    def fake_start(self):
        attempts_by_provider[self.provider.provider_name] = self
        self.stdout_path.parent.mkdir(parents=True, exist_ok=True)
        self.last_message_path.parent.mkdir(parents=True, exist_ok=True)

    def fake_join(self, timeout=None):
        join_timeouts.setdefault(self.provider.provider_name, []).append(timeout)

    def fake_terminate(self, reason):
        self._cancel_reason = reason

    def fake_sleep(seconds):
        winner = attempts_by_provider["sub.jlypx.de"]
        winner.returncode = 0
        winner.duration_seconds = 0.5
        winner.finished_at = "2026-03-24T00:00:02+00:00"
        winner.last_message_path.write_text("winner\n", encoding="utf-8")
        winner.done.set()

    monkeypatch.setattr(codex_mesh.RunningAttempt, "start", fake_start)
    monkeypatch.setattr(codex_mesh.RunningAttempt, "join", fake_join)
    monkeypatch.setattr(codex_mesh.RunningAttempt, "terminate", fake_terminate)
    monkeypatch.setattr(codex_mesh.time, "sleep", fake_sleep)

    results = codex_mesh._execute_with_hedge(
        root=tmp_path,
        providers=providers,
        prompt="prompt",
        task_dir=tmp_path / "task",
        execution_root=tmp_path,
        run_id="run-hedge",
        task=task,
        dangerously_bypass=True,
        sandbox="danger-full-access",
        ephemeral=True,
    )

    assert [item.provider for item in results] == ["sub.jlypx.de", "snew.145678.xyz"]
    assert join_timeouts["snew.145678.xyz"] == [15, 0]
    assert results[1].status == "late_cancelled"
    assert results[1].finished_at is not None


@pytest.mark.parametrize(
    ("depth", "max_external_depth", "allow_at_external_limit", "expected_enable"),
    [
        (1, 2, True, True),
        (2, 2, True, True),
        (2, 2, False, False),
    ],
)
def test_execute_task_passes_resolved_inner_codex_options_to_build_codex_command(
    tmp_path,
    monkeypatch,
    depth,
    max_external_depth,
    allow_at_external_limit,
    expected_enable,
):
    captured: dict[str, object] = {}
    provider = codex_mesh.ProviderSpec("sub.jlypx.de", "subjlypx", "", "", "", "gpt-5.4", None, "xhigh", True, True)
    task = codex_mesh.MeshTaskManifest(
        task_id="budget-test",
        goal="goal",
        prompt="prompt",
        task_kind="analysis",
        provider_allowlist=["sub.jlypx.de"],
        depth=depth,
        max_external_depth=max_external_depth,
        allow_native_subagents_at_external_limit=allow_at_external_limit,
    )

    monkeypatch.setattr(codex_mesh, "discover_providers", lambda *args, **kwargs: [provider])
    monkeypatch.setattr(codex_mesh, "order_providers", lambda specs, state: specs)
    monkeypatch.setattr(codex_mesh, "_ensure_execution_root", lambda *args, **kwargs: (tmp_path, "shared"))
    monkeypatch.setattr(codex_mesh, "build_mesh_prompt", lambda **kwargs: "prompt")
    monkeypatch.setattr(codex_mesh, "resolve_codex_executable", lambda: "codex")
    monkeypatch.setattr(codex_mesh, "_build_provider_home", lambda *args, **kwargs: tmp_path)
    monkeypatch.setattr(codex_mesh, "_provider_env", lambda *args, **kwargs: {})

    def fake_build_codex_command(**kwargs):
        captured.update(kwargs)
        return ["codex", "exec"]

    monkeypatch.setattr(codex_mesh, "build_codex_command", fake_build_codex_command)
    monkeypatch.setattr(
        codex_mesh,
        "_run_attempt_blocking",
        lambda **kwargs: codex_mesh.ProviderAttemptResult(
            provider="sub.jlypx.de",
            command=["codex", "exec"],
            returncode=0,
            stdout_path="stdout.log",
            stderr_path="stderr.log",
            last_message_path="last_message.txt",
            duration_seconds=0.1,
            ok=True,
            status="success",
        ),
    )

    result = codex_mesh._execute_task(
        root=tmp_path,
        run_id="20260325T120000",
        task=task,
        state=codex_mesh.default_state(),
        execution_mode="serial",
        dangerously_bypass=True,
        sandbox="danger-full-access",
        ephemeral=True,
        provider_allowlist=["sub.jlypx.de"],
        provider_denylist=[],
    )

    assert result.success is True
    assert captured["enable_multi_agent"] is expected_enable
    assert captured["agent_max_depth"] == codex_mesh.DEFAULT_INNER_AGENT_MAX_DEPTH
    assert captured["agent_max_threads"] == codex_mesh.DEFAULT_INNER_AGENT_MAX_THREADS


def test_build_single_task_manifest_from_args_inherits_external_context(tmp_path, monkeypatch):
    monkeypatch.setenv("CODEX_MESH_DEPTH", "1")
    monkeypatch.setenv("CODEX_MESH_MAX_DEPTH", "2")
    monkeypatch.setenv("CODEX_MESH_TASK_ID", "parent-task")
    monkeypatch.setenv("CODEX_MESH_LINEAGE_ID", "lineage-1")
    monkeypatch.setattr(codex_mesh, "resolve_provider_allowlist", lambda *args, **kwargs: ["sub.jlypx.de"])

    manifest = codex_mesh.build_single_task_manifest_from_args(
        SimpleNamespace(
            prompt_text="prompt",
            prompt_file=None,
            disable_provider=None,
            provider=None,
            max_external_depth=5,
            task_id="task-1",
            goal="goal",
            task_kind="analysis",
            read_scope=[],
            write_scope=[],
                disable_native_subagents=False,
                allow_native_subagents_at_external_limit=codex_mesh.DEFAULT_ALLOW_NATIVE_SUBAGENTS_AT_EXTERNAL_LIMIT,
                inner_agent_max_depth=codex_mesh.DEFAULT_INNER_AGENT_MAX_DEPTH,
                inner_agent_max_threads=codex_mesh.DEFAULT_INNER_AGENT_MAX_THREADS,
                timeout_seconds=60,
                mesh_benchmark_label=None,
                output_mode="text",
            working_root=tmp_path,
            parent_task_id=None,
            lineage_id=None,
            depth=None,
            hedge_delay_seconds=1,
            execution_mode="mesh",
            mesh_max_workers=1,
            no_dangerously_bypass=False,
            sandbox="danger-full-access",
            no_ephemeral=False,
        )
    )

    task = manifest.tasks[0]
    assert task.depth == 2
    assert task.max_external_depth == 2
    assert task.parent_task_id == "parent-task"
    assert task.lineage_id == "lineage-1"


def test_build_mesh_prompt_includes_single_writer_ledger_rules(tmp_path):
    task = codex_mesh.MeshTaskManifest(
        task_id="mesh-prompt",
        goal="goal",
        prompt="body",
        task_kind="mixed",
        read_scope=["app"],
        write_scope=["app", "github/automation/live_fix_loop"],
    )

    prompt = codex_mesh.build_mesh_prompt(
        task=task,
        run_id="run-1",
        provider_order=["sub.jlypx.de", "snew.145678.xyz"],
        execution_root=tmp_path,
    )

    assert "single_writer_paths" in prompt
    assert "issue_register.md" in prompt
    assert "review_log.md" in prompt


def test_write_workspace_agents_override_includes_single_writer_ledger_rules(tmp_path):
    task = codex_mesh.MeshTaskManifest(
        task_id="mesh-override",
        goal="goal",
        prompt="body",
        task_kind="write",
        read_scope=["app"],
        write_scope=["app", "github/automation/live_fix_loop"],
    )

    codex_mesh._write_workspace_agents_override(
        tmp_path,
        run_id="run-2",
        task=task,
        provider_order=["sub.jlypx.de"],
    )

    content = (tmp_path / "AGENTS.override.md").read_text(encoding="utf-8")
    assert "single_writer_paths" in content
    assert "issue_register.md" in content
    assert "review_log.md" in content


def test_execute_manifest_serializes_overlapping_write_scopes(tmp_path, monkeypatch):
    events: list[tuple[str, float]] = []
    event_lock = threading.Lock()

    def fake_execute_task(**kwargs):
        task = kwargs["task"]
        with event_lock:
            events.append((f"start:{task.task_id}", time.perf_counter()))
        time.sleep(0.25)
        with event_lock:
            events.append((f"end:{task.task_id}", time.perf_counter()))
        return codex_mesh.MeshTaskResult(
            task_id=task.task_id,
            goal=task.goal,
            task_kind=task.task_kind,
            success=True,
            selected_provider="sub.jlypx.de",
            output_mode=task.output_mode,
            provider_order=["sub.jlypx.de"],
            attempts=[],
            execution_root=".",
            workspace_kind="copy",
            depth=task.depth,
            parent_task_id=task.parent_task_id,
            lineage_id=task.lineage_id or task.task_id,
            started_at="2026-03-24T00:00:00+00:00",
            finished_at="2026-03-24T00:00:01+00:00",
        )

    monkeypatch.setattr(codex_mesh, "_execute_task", fake_execute_task)
    monkeypatch.setattr(codex_mesh, "discover_providers", lambda *args, **kwargs: [])
    monkeypatch.setattr(codex_mesh, "run_id_now", lambda: "20260324T000000")

    manifest = codex_mesh.MeshRunManifest(
        tasks=[
            codex_mesh.MeshTaskManifest("a", "A", "p", task_kind="write", write_scope=["app/a.py"]),
            codex_mesh.MeshTaskManifest("b", "B", "p", task_kind="write", write_scope=["app/a.py"]),
        ],
        max_workers=2,
    )

    codex_mesh.execute_manifest(tmp_path, manifest)

    times = {name: ts for name, ts in events}
    assert times["start:b"] >= times["end:a"] or times["start:a"] >= times["end:b"]


def test_execute_manifest_parallelizes_non_overlapping_write_scopes(tmp_path, monkeypatch):
    events: list[tuple[str, float]] = []
    event_lock = threading.Lock()

    def fake_execute_task(**kwargs):
        task = kwargs["task"]
        with event_lock:
            events.append((f"start:{task.task_id}", time.perf_counter()))
        time.sleep(0.25)
        with event_lock:
            events.append((f"end:{task.task_id}", time.perf_counter()))
        return codex_mesh.MeshTaskResult(
            task_id=task.task_id,
            goal=task.goal,
            task_kind=task.task_kind,
            success=True,
            selected_provider="sub.jlypx.de",
            output_mode=task.output_mode,
            provider_order=["sub.jlypx.de"],
            attempts=[],
            execution_root=".",
            workspace_kind="copy",
            depth=task.depth,
            parent_task_id=task.parent_task_id,
            lineage_id=task.lineage_id or task.task_id,
            started_at="2026-03-24T00:00:00+00:00",
            finished_at="2026-03-24T00:00:01+00:00",
        )

    monkeypatch.setattr(codex_mesh, "_execute_task", fake_execute_task)
    monkeypatch.setattr(codex_mesh, "discover_providers", lambda *args, **kwargs: [])
    monkeypatch.setattr(codex_mesh, "run_id_now", lambda: "20260324T000001")

    manifest = codex_mesh.MeshRunManifest(
        tasks=[
            codex_mesh.MeshTaskManifest("a", "A", "p", task_kind="write", write_scope=["app/a.py"]),
            codex_mesh.MeshTaskManifest("b", "B", "p", task_kind="write", write_scope=["tests/b.py"]),
        ],
        max_workers=2,
    )

    codex_mesh.execute_manifest(tmp_path, manifest)

    times = {name: ts for name, ts in events}
    assert times["start:b"] < times["end:a"] and times["start:a"] < times["end:b"]


def test_execute_manifest_recreates_executor_after_shutdown(tmp_path, monkeypatch):
    created_executors: list[int] = []
    submissions: list[tuple[int, str]] = []

    class FakeExecutor:
        _instances = 0

        def __init__(self, max_workers: int):
            del max_workers
            FakeExecutor._instances += 1
            self.instance_id = FakeExecutor._instances
            self.submit_count = 0
            created_executors.append(self.instance_id)

        def submit(self, fn, **kwargs):
            del fn
            if self.instance_id == 1 and self.submit_count >= 1:
                raise RuntimeError("cannot schedule new futures after shutdown")
            self.submit_count += 1
            task = kwargs["task"]
            submissions.append((self.instance_id, task.task_id))
            future = Future()
            future.set_result(
                codex_mesh.MeshTaskResult(
                    task_id=task.task_id,
                    goal=task.goal,
                    task_kind=task.task_kind,
                    success=True,
                    selected_provider="sub.jlypx.de",
                    output_mode=task.output_mode,
                    provider_order=["sub.jlypx.de"],
                    attempts=[],
                    execution_root=".",
                    workspace_kind="shared",
                    depth=task.depth,
                    parent_task_id=task.parent_task_id,
                    lineage_id=task.lineage_id or task.task_id,
                    started_at="2026-03-24T00:00:00+00:00",
                    finished_at="2026-03-24T00:00:01+00:00",
                )
            )
            return future

        def shutdown(self, wait: bool = True):
            del wait

    monkeypatch.setattr(codex_mesh, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr(codex_mesh, "discover_providers", lambda *args, **kwargs: [])
    monkeypatch.setattr(codex_mesh, "run_id_now", lambda: "20260324T000003")

    manifest = codex_mesh.MeshRunManifest(
        tasks=[
            codex_mesh.MeshTaskManifest("a", "A", "p", task_kind="analysis"),
            codex_mesh.MeshTaskManifest("b", "B", "p", task_kind="analysis"),
        ],
        max_workers=1,
    )

    summary = codex_mesh.execute_manifest(tmp_path, manifest)

    assert summary.success is True
    assert [task.task_id for task in summary.tasks] == ["a", "b"]
    assert created_executors == [1, 2]
    assert submissions == [(1, "a"), (2, "b")]


def test_execute_manifest_resolves_default_provider_allowlist(tmp_path, monkeypatch):
    seen_allowlists: list[list[str]] = []
    resolved_allowlist = [
        "sub.jlypx.de",
        "ai.qaq.al",
        "infiniteai.cc",
        "119.8.113.226",
        "freeapi.dgbmc.top",
    ]

    def fake_execute_task(**kwargs):
        seen_allowlists.append(list(kwargs["provider_allowlist"]))
        task = kwargs["task"]
        return codex_mesh.MeshTaskResult(
            task_id=task.task_id,
            goal=task.goal,
            task_kind=task.task_kind,
            success=True,
            selected_provider="sub.jlypx.de",
            output_mode=task.output_mode,
            provider_order=list(kwargs["provider_allowlist"]),
            attempts=[],
            execution_root=".",
            workspace_kind="shared",
            depth=task.depth,
            parent_task_id=task.parent_task_id,
            lineage_id=task.lineage_id or task.task_id,
            started_at="2026-03-24T00:00:00+00:00",
            finished_at="2026-03-24T00:00:01+00:00",
        )

    monkeypatch.setattr(codex_mesh, "_execute_task", fake_execute_task)
    monkeypatch.setattr(codex_mesh, "discover_providers", lambda *args, **kwargs: [])
    monkeypatch.setattr(codex_mesh, "resolve_provider_allowlist", lambda *args, **kwargs: resolved_allowlist)
    monkeypatch.setattr(codex_mesh, "run_id_now", lambda: "20260324T000002")

    manifest = codex_mesh.MeshRunManifest(
        tasks=[codex_mesh.MeshTaskManifest("a", "A", "p", task_kind="analysis")],
        max_workers=1,
        provider_allowlist=[],
        provider_denylist=[],
    )

    codex_mesh.execute_manifest(tmp_path, manifest)

    assert seen_allowlists == [resolved_allowlist]


def test_benchmark_manifest_writes_acceptance_report(tmp_path, monkeypatch):
    runs = [
        _summary_for("serial-1", duration=10.0),
        _summary_for("mesh-1", duration=7.0),
        _summary_for("serial-2", duration=11.0),
        _summary_for("mesh-2", duration=8.0),
        _summary_for("serial-3", duration=10.0),
        _summary_for("mesh-3", duration=7.0),
        _summary_for("serial-4", duration=12.0),
        _summary_for("mesh-4", duration=8.0),
        _summary_for("serial-5", duration=11.0),
        _summary_for("mesh-5", duration=7.0),
    ]

    monkeypatch.setattr(codex_mesh, "execute_manifest", lambda root, manifest: runs.pop(0))
    monkeypatch.setattr(codex_mesh, "run_id_now", lambda: "20260324T001500")

    manifest = codex_mesh.MeshRunManifest(
        tasks=[codex_mesh.MeshTaskManifest("bench", "bench", "prompt", task_kind="analysis")],
        max_workers=2,
    )

    payload = codex_mesh.benchmark_manifest(tmp_path, manifest, iterations=5, suite="analysis_review")

    assert payload["acceptance"]["accepted"] is True
    assert payload["mesh"]["median_wall_clock_seconds"] < payload["serial"]["median_wall_clock_seconds"]
    assert (tmp_path / "runtime" / "codex_mesh" / "benchmarks" / "analysis_review" / "20260324T001500.json").exists()


def test_status_payload_reports_latest_summary(tmp_path, monkeypatch):
    state = codex_mesh.default_state()
    state["latest_runs"] = ["20260324T010000"]
    state["providers"] = {
        "sub.jlypx.de": {"attempts": 2, "successes": 2, "failures": 0, "durations": [6.0, 7.0], "cooldown_until": 0}
    }
    codex_mesh.save_state(state, tmp_path)
    latest = tmp_path / "runtime" / "codex_mesh" / "latest_summary.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps({"run_id": "20260324T010000", "success": True}), encoding="utf-8")
    monkeypatch.setattr(
        codex_mesh,
        "discover_providers",
        lambda root, **kwargs: [codex_mesh.ProviderSpec("sub.jlypx.de", "subjlypx", "", "", "", "gpt-5.4", None, "xhigh", True, True)],
    )

    payload = codex_mesh.status_payload(tmp_path)

    assert payload["latest_runs"] == ["20260324T010000"]
    assert payload["latest_summary"]["run_id"] == "20260324T010000"
    assert payload["providers"][0]["provider"] == "sub.jlypx.de"
