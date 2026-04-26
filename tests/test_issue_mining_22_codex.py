from __future__ import annotations

from pathlib import Path

from scripts import issue_mining_22_codex


def _write_target_doc(root: Path) -> Path:
    doc = root / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    doc.parent.mkdir(parents=True, exist_ok=True)
    doc.write_text(
        "\n".join(
            [
                "## 单一问题台账",
                "",
                "## FR-10 完成站点与看板（10 个功能点）",
                "",
                "### FR10-PAGE-01 首页 API + 页面",
                "",
                "**差距**: 无",
                "",
                "---",
                "",
                "### PAGE-FEATURES 功能治理地图页",
                "",
                "| **差距** | 无 |",
                "",
                "---",
                "",
                "### NFR-03 文档-实现一致性",
                "",
                "| **差距** | 无 |",
            ]
        ),
        encoding="utf-8",
    )
    return doc


def test_collect_writeback_targets_reads_specific_subsections(tmp_path):
    doc = _write_target_doc(tmp_path)

    targets = issue_mining_22_codex.collect_writeback_targets(doc)

    assert [item["heading"] for item in targets] == [
        "FR10-PAGE-01 首页 API + 页面",
        "PAGE-FEATURES 功能治理地图页",
        "NFR-03 文档-实现一致性",
    ]


def test_build_prompt_contains_analysis_only_and_subagent_rules(tmp_path):
    doc = _write_target_doc(tmp_path)

    prompt = issue_mining_22_codex.build_prompt(
        root=tmp_path,
        target_doc=doc,
        base_url="http://127.0.0.1:8000",
        providers=["sub.jlypx.de", "infiniteai.cc", "ai.qaq.al"],
    )

    assert "只分析，不改代码" in prompt
    assert "至少并行启动 2 个子代理" in prompt
    assert f"唯一允许改动的文件是 `{doc.as_posix()}`" in prompt
    assert "优先写回最具体的 `### FRxx-* / PAGE-* / NFR-*` 子节" in prompt
    assert "## 单一问题台账" in prompt


def test_parse_args_defaults_to_30_minute_manual_run():
    args = issue_mining_22_codex.parse_args(["run-once"])

    assert args.command == "run-once"
    assert args.timeout_minutes == 30


def test_run_once_rotates_providers_and_updates_state(tmp_path, monkeypatch):
    doc = _write_target_doc(tmp_path)

    def fake_prepare_provider_home(root: Path, provider: str) -> Path:
        portable_home = root / "ai-api" / "codex" / f"portable_{provider}"
        (portable_home / ".codex").mkdir(parents=True, exist_ok=True)
        return portable_home

    def fake_invoke_codex_attempt(*, command, env, prompt, stdout_path, stderr_path, timeout_seconds):
        provider = Path(env["HOME"]).name.replace("portable_", "")
        last_message_path = Path(command[command.index("--output-last-message") + 1])
        stdout_path.write_text(f"{provider} stdout", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        if provider == "sub.jlypx.de":
            return 1, 0.1, "relay_failed"
        last_message_path.write_text(f"{provider} success", encoding="utf-8")
        return 0, 0.2, None

    monkeypatch.setattr(issue_mining_22_codex.codex_common, "resolve_codex_executable", lambda: "codex.exe")
    monkeypatch.setattr(issue_mining_22_codex.codex_common, "prepare_provider_home", fake_prepare_provider_home)
    monkeypatch.setattr(issue_mining_22_codex.codex_common, "invoke_codex_attempt", fake_invoke_codex_attempt)

    payload = issue_mining_22_codex.run_once(
        root=tmp_path,
        target_doc=doc,
        base_url="http://127.0.0.1:8000",
        providers=["sub.jlypx.de", "infiniteai.cc", "ai.qaq.al"],
        timeout_minutes=1,
    )

    assert payload["success"] is True
    assert [item["provider"] for item in payload["attempts"]] == ["sub.jlypx.de", "infiniteai.cc"]
    state = issue_mining_22_codex.load_state(tmp_path)
    assert state["last_success_provider"] == "infiniteai.cc"
    assert state["next_start_provider"] == "ai.qaq.al"


def test_run_once_mesh_returns_mesh_payload(tmp_path, monkeypatch):
    doc = _write_target_doc(tmp_path)
    monkeypatch.setattr(
        issue_mining_22_codex.codex_mesh,
        "execute_manifest",
        lambda root, manifest: issue_mining_22_codex.codex_mesh.MeshRunSummary(
            run_id="mesh-issue-1",
            execution_mode="mesh",
            success=True,
            task_count=1,
            max_workers=manifest.max_workers,
            benchmark_label=None,
            manifest_path=str(tmp_path / "runtime" / "codex_mesh" / "runs" / "mesh-issue-1" / "manifest.json"),
            output_dir=str(tmp_path / "runtime" / "codex_mesh" / "runs" / "mesh-issue-1"),
            tasks=[
                issue_mining_22_codex.codex_mesh.MeshTaskResult(
                    task_id="issue-mining-22",
                    goal="22_v7 analysis and writeback",
                    task_kind="write",
                    success=True,
                    selected_provider="infiniteai.cc",
                    output_mode="text",
                    provider_order=["sub.jlypx.de", "infiniteai.cc"],
                    attempts=[],
                    execution_root=str(tmp_path),
                    workspace_kind="copy",
                    depth=1,
                    parent_task_id=None,
                    lineage_id="issue-mining-22",
                    started_at="2026-03-24T00:00:00+00:00",
                    finished_at="2026-03-24T00:00:01+00:00",
                )
            ],
            provider_health=[],
            started_at="2026-03-24T00:00:00+00:00",
            finished_at="2026-03-24T00:00:01+00:00",
        ),
    )

    payload = issue_mining_22_codex.run_once_mesh(
        root=tmp_path,
        target_doc=doc,
        base_url="http://127.0.0.1:8000",
        providers=["sub.jlypx.de", "infiniteai.cc"],
        prompt_override="analysis prompt",
    )

    assert payload["delegate_mode"] == "mesh"
    assert payload["success"] is True
    assert payload["target_doc"] == str(doc)
