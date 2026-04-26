from __future__ import annotations

from pathlib import Path

from scripts import prompt6_hourly_codex


def _write_prompt_doc(root: Path) -> Path:
    prompt_doc = root / "docs" / "提示词" / "18_全量自动化提示词.md"
    prompt_doc.parent.mkdir(parents=True, exist_ok=True)
    prompt_doc.write_text(
        "\n".join(
            [
                "## Prompt 5：占位",
                "",
                "```text",
                "old",
                "```",
                "",
                "## Prompt 6：真实验真 + 深度修复循环控制器",
                "",
                "```text",
                "【角色】",
                "你是测试代理。",
                "",
                "【任务】",
                "执行闭环。",
                "```",
                "",
                "## Prompt 7：占位",
            ]
        ),
        encoding="utf-8",
    )
    return prompt_doc


def test_extract_prompt_section_returns_prompt6_text(tmp_path):
    prompt_doc = _write_prompt_doc(tmp_path)

    result = prompt6_hourly_codex.extract_prompt_section(prompt_doc)

    assert "你是测试代理。" in result
    assert "执行闭环。" in result
    assert "Prompt 7" not in result


def test_build_prompt_includes_overlay_and_original_prompt(tmp_path):
    prompt_doc = _write_prompt_doc(tmp_path)

    result = prompt6_hourly_codex.build_prompt(
        root=tmp_path,
        prompt_doc=prompt_doc,
        base_url="http://127.0.0.1:8000",
        providers=["sub.jlypx.de", "infiniteai.cc", "ai.qaq.al"],
    )

    assert "【自动化调度上下文】" in result
    assert "必须显式使用子代理功能加速" in result
    assert "运行时自愈" in result
    assert "你是测试代理。" in result


def test_build_prompt_can_skip_overlay_for_smoke_runs(tmp_path):
    prompt_doc = _write_prompt_doc(tmp_path)

    result = prompt6_hourly_codex.build_prompt(
        root=tmp_path,
        prompt_doc=prompt_doc,
        base_url="http://127.0.0.1:8000",
        providers=["sub.jlypx.de"],
        include_overlay=False,
    )

    assert "【自动化调度上下文】" not in result
    assert result.strip().startswith("【角色】")


def test_provider_order_uses_saved_next_start_provider():
    state = {"next_start_provider": "ai.qaq.al"}

    result = prompt6_hourly_codex.provider_order(
        ["sub.jlypx.de", "infiniteai.cc", "ai.qaq.al"],
        state,
    )

    assert result == ["ai.qaq.al", "sub.jlypx.de", "infiniteai.cc"]


def test_resolve_codex_executable_prefers_exe(monkeypatch):
    def fake_which(name: str) -> str | None:
        mapping = {
            "codex.exe": r"C:\tools\codex.exe",
            "codex.cmd": r"C:\tools\codex.cmd",
            "codex": r"C:\tools\codex",
        }
        return mapping.get(name)

    monkeypatch.setattr(prompt6_hourly_codex.shutil, "which", fake_which)

    result = prompt6_hourly_codex.resolve_codex_executable()

    assert result == r"C:\tools\codex.exe"


def test_run_once_fails_over_to_next_provider_and_updates_rotation(tmp_path, monkeypatch):
    prompt_doc = _write_prompt_doc(tmp_path)

    def fake_invoke_codex_attempt(*, command, env, prompt, stdout_path, stderr_path, timeout_seconds):
        provider = Path(env["HOME"]).name.replace("portable_", "").replace("_", ".")
        last_message_path = Path(command[command.index("--output-last-message") + 1])
        stdout_path.write_text(f"{provider} stdout", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        if provider == "sub.jlypx.de":
            return 1, 0.1, "relay_failed"
        last_message_path.write_text(f"{provider} success", encoding="utf-8")
        return 0, 0.2, None

    def fake_prepare_provider_home(root: Path, provider: str) -> Path:
        portable_home = root / "ai-api" / "codex" / f"portable_{provider}"
        (portable_home / ".codex").mkdir(parents=True, exist_ok=True)
        return portable_home

    monkeypatch.setattr(prompt6_hourly_codex, "resolve_codex_executable", lambda: "codex.exe")
    monkeypatch.setattr(prompt6_hourly_codex, "prepare_provider_home", fake_prepare_provider_home)
    monkeypatch.setattr(prompt6_hourly_codex, "invoke_codex_attempt", fake_invoke_codex_attempt)

    payload = prompt6_hourly_codex.run_once(
        root=tmp_path,
        prompt_doc=prompt_doc,
        base_url="http://127.0.0.1:8000",
        providers=["sub.jlypx.de", "infiniteai.cc", "ai.qaq.al"],
        timeout_minutes=1,
    )

    assert payload["success"] is True
    assert [item["provider"] for item in payload["attempts"]] == ["sub.jlypx.de", "infiniteai.cc"]
    state = prompt6_hourly_codex.load_state(tmp_path)
    assert state["last_success_provider"] == "infiniteai.cc"
    assert state["next_start_provider"] == "ai.qaq.al"


def test_parse_args_only_supports_manual_commands():
    args = prompt6_hourly_codex.parse_args(["run-once", "--dry-run"])
    assert args.command == "run-once"


def test_run_once_mesh_uses_codex_mesh_summary(tmp_path, monkeypatch):
    prompt_doc = _write_prompt_doc(tmp_path)
    monkeypatch.setattr(
        prompt6_hourly_codex.codex_mesh,
        "execute_manifest",
        lambda root, manifest: prompt6_hourly_codex.codex_mesh.MeshRunSummary(
            run_id="mesh-manual-1",
            execution_mode="mesh",
            success=True,
            task_count=1,
            max_workers=manifest.max_workers,
            benchmark_label=None,
            manifest_path=str(tmp_path / "runtime" / "codex_mesh" / "runs" / "mesh-manual-1" / "manifest.json"),
            output_dir=str(tmp_path / "runtime" / "codex_mesh" / "runs" / "mesh-manual-1"),
            tasks=[
                prompt6_hourly_codex.codex_mesh.MeshTaskResult(
                    task_id="prompt6-run-once",
                    goal="Prompt 6 manual run-once",
                    task_kind="mixed",
                    success=True,
                    selected_provider="sub.jlypx.de",
                    output_mode="text",
                    provider_order=["sub.jlypx.de", "ai.qaq.al"],
                    attempts=[],
                    execution_root=str(tmp_path),
                    workspace_kind="copy",
                    depth=1,
                    parent_task_id=None,
                    lineage_id="prompt6-run-once",
                    started_at="2026-03-24T00:00:00+00:00",
                    finished_at="2026-03-24T00:00:01+00:00",
                )
            ],
            provider_health=[],
            started_at="2026-03-24T00:00:00+00:00",
            finished_at="2026-03-24T00:00:01+00:00",
        ),
    )

    payload = prompt6_hourly_codex.run_once_mesh(
        root=tmp_path,
        prompt_doc=prompt_doc,
        base_url="http://127.0.0.1:8000",
        providers=["sub.jlypx.de", "ai.qaq.al"],
        prompt_override="manual prompt",
    )

    assert payload["delegate_mode"] == "mesh"
    assert payload["success"] is True
    assert payload["providers"] == ["sub.jlypx.de", "ai.qaq.al"]
