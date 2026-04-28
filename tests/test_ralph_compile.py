from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from codex import ralph_compile
from codex import ralph_prompts
from codex.ralph_truth import ProbeSummary, RuntimeSentinelState, TruthSnapshot


def _fake_truth_snapshot() -> TruthSnapshot:
    return TruthSnapshot(
        generated_at="2026-04-28T00:00:00+00:00",
        check_state_stdout="Active tasks: 0\n\nTotal published: 9",
        active_task_count=0,
        published_report_count=9,
        sqlite={
            "report_total": 12,
            "report_published": 9,
            "report_alive": 12,
            "published_buy": 3,
            "settlement_total": 3,
            "sim_position_open": 7,
            "pool_latest_trade_date": "2026-04-24",
            "kline_latest_trade_date": "2026-04-24",
            "market_state_latest_trade_date": "2026-04-27",
            "strategy_snapshot_latest_date": "2026-04-23",
            "baseline_snapshot_latest_date": "2026-04-23",
            "report_data_usage_count": 100,
            "report_citation_count": 50,
        },
        probes={
            "/api/v1/home": ProbeSummary("/api/v1/home", 200, True, {"data": {"pool_size": 200}}, None),
            "/api/v1/reports?limit=3": ProbeSummary("/api/v1/reports?limit=3", 200, True, {"data": {"total": 9}}, None),
            "/api/v1/market/state": ProbeSummary("/api/v1/market/state", 200, True, {"data": {"trade_date": "2026-03-13", "market_state": "NEUTRAL"}}, None),
            "/api/v1/dashboard/stats?window_days=7": ProbeSummary("/api/v1/dashboard/stats?window_days=7", 200, True, {"data": {"data_status": "DEGRADED"}}, None),
        },
        anchors={
            "runtime_trade_date": "2026-03-13",
            "latest_published_report_trade_date": "2026-03-13",
            "latest_complete_public_batch_trade_date": "2026-03-13",
            "public_pool_trade_date": "2026-04-24",
            "public_pool_size": 200,
            "runtime_market_state": {"trade_date": "2026-03-13", "market_state": "NEUTRAL"},
            "latest_public_market_state": {"trade_date": "2026-03-13"},
            "home_cache_key": ["demo"],
        },
        sentinels={
            "published_reports_nonzero": RuntimeSentinelState("published_reports_nonzero", True),
            "admin_overview_consistent": RuntimeSentinelState("admin_overview_consistent", True),
            "public_read_model_nonempty": RuntimeSentinelState("public_read_model_nonempty", True),
        },
    )


def _write_min_repo(root: Path) -> None:
    (root / "docs" / "core").mkdir(parents=True, exist_ok=True)
    (root / ".claude" / "ralph" / "loop").mkdir(parents=True, exist_ok=True)
    (root / ".claude" / "ralph" / "prd").mkdir(parents=True, exist_ok=True)
    (root / ".claude" / "skills" / "prd").mkdir(parents=True, exist_ok=True)
    (root / ".claude" / "skills" / "ralph").mkdir(parents=True, exist_ok=True)
    (root / "app" / "governance").mkdir(parents=True, exist_ok=True)
    (root / "data").mkdir(parents=True, exist_ok=True)
    for rel in [
        "AGENTS.md",
        ".claude/CLAUDE.md",
        "docs/core/01_需求基线.md",
        "docs/core/02_系统架构.md",
        "docs/core/05_API与数据契约.md",
        "docs/core/06_全量数据需求说明.md",
        "docs/core/22_全量功能进度总表_v12.md",
        "docs/core/25_系统问题分析角度清单.md",
        "docs/core/26_自动化执行记忆.md",
        "docs/core/27_PRD_研报平台增强与整体验收基线.md",
        ".claude/skills/prd/SKILL.md",
        ".claude/skills/ralph/SKILL.md",
        "app/governance/feature_registry.json",
        "app/governance/catalog_snapshot.json",
    ]:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{rel}\n", encoding="utf-8")

    existing_note_payload = {
        "group": "G10",
        "dependsOn": [],
        "endpoints": ["GET /api/v1/admin/overview"],
        "models": ["admin_overview"],
        "permissions": ["admin"],
        "errorCodes": [],
        "idempotency": "",
        "enums": [],
        "thresholds": "",
        "degradation": "",
        "exampleAssert": "",
        "pytest": "python -m pytest tests/test_fr12_admin.py -q --tb=short",
    }
    existing_prd = {
        "project": "demo",
        "branchName": "main",
        "description": "demo",
        "userStories": [
            {
                "id": "US-108",
                "title": "恢复 FR-12 管理入口与运行态修复闭环",
                "description": "existing runtime admin story",
                "acceptanceCriteria": [
                    "GET /api/v1/admin/overview returns 200",
                    "python -m pytest tests/test_fr12_admin.py -q --tb=short",
                    "Typecheck passes",
                ],
                "priority": 108,
                "passes": True,
                "notes": json.dumps(existing_note_payload, ensure_ascii=False),
            }
        ],
    }
    (root / ".claude" / "ralph" / "loop" / "prd.json").write_text(json.dumps(existing_prd, ensure_ascii=False, indent=2), encoding="utf-8")
    (root / ".claude" / "ralph" / "prd" / "yanbao-platform-enhancement.json").write_text(json.dumps(existing_prd, ensure_ascii=False, indent=2), encoding="utf-8")


def test_rebuild_repo_enriches_notes_and_appends_new_story(monkeypatch, tmp_path):
    _write_min_repo(tmp_path)
    monkeypatch.setattr(ralph_compile, "collect_truth_snapshot", lambda **_: _fake_truth_snapshot())
    outputs = iter(
        [
            "## Introduction\n\nGenerated doc 27 narrative",
            json.dumps(
                [
                    {
                        "title": "恢复 FR-12 管理入口与运行态修复闭环",
                        "description": "existing runtime admin story",
                        "acceptanceCriteria": [
                            "GET /api/v1/admin/overview returns 200",
                            "python -m pytest tests/test_fr12_admin.py -q --tb=short",
                        ],
                        "priority": 108,
                    },
                    {
                        "title": "新增运行态收口故事",
                        "description": "new story",
                        "acceptanceCriteria": [
                            "GET /api/v1/home returns 200",
                            "python -m pytest tests/test_fr10_site_dashboard.py -q --tb=short",
                        ],
                        "priority": 109,
                    },
                ],
                ensure_ascii=False,
            ),
        ]
    )
    monkeypatch.setattr(ralph_compile, "_run_claude", lambda *args, **kwargs: next(outputs))
    monkeypatch.setattr(ralph_compile, "_git_commit", lambda *args, **kwargs: None)

    summary = ralph_compile.rebuild_repo(repo_root=tmp_path)

    assert summary.new_story_ids == ["US-109"]
    assert summary.stories_total == 2
    payload = json.loads((tmp_path / ".claude" / "ralph" / "loop" / "prd.json").read_text(encoding="utf-8"))
    assert payload == json.loads((tmp_path / ".claude" / "ralph" / "prd" / "yanbao-platform-enhancement.json").read_text(encoding="utf-8"))
    assert {story["id"] for story in payload["userStories"]} == {"US-108", "US-109"}
    for story in payload["userStories"]:
        notes = json.loads(story["notes"])
        assert set(notes.keys()) == set(ralph_compile.NOTE_KEYS)
    compile_manifest = json.loads((tmp_path / ".claude" / "ralph" / "loop" / "compile_manifest.json").read_text(encoding="utf-8"))
    assert compile_manifest["story_set_hash"] == summary.story_set_hash
    compile_report = json.loads((tmp_path / ".claude" / "ralph" / "loop" / "compile_report.json").read_text(encoding="utf-8"))
    assert compile_report["mode"] == "rebuild"


def test_resolve_claude_executable_prefers_repo_wrapper_on_windows(monkeypatch, tmp_path):
    wrapper = tmp_path / "claude.cmd"
    wrapper.write_text("@echo off\r\necho wrapper\r\n", encoding="utf-8")
    monkeypatch.setattr(ralph_compile.os, "name", "nt", raising=False)
    monkeypatch.setenv("PATH", "")

    assert ralph_compile._resolve_claude_executable(tmp_path) == str(wrapper)


def test_resolve_claude_executable_raises_when_missing(monkeypatch, tmp_path):
    monkeypatch.setenv("PATH", "")
    monkeypatch.setattr(ralph_compile.os, "name", "posix", raising=False)

    with pytest.raises(RuntimeError, match="claude_cli_not_found"):
        ralph_compile._resolve_claude_executable(tmp_path)


def test_run_claude_writes_utf8_prompt_bytes(monkeypatch, tmp_path):
    wrapper = tmp_path / "claude.cmd"
    wrapper.write_text("@echo off\r\n", encoding="utf-8")
    monkeypatch.setattr(ralph_compile.os, "name", "nt", raising=False)
    monkeypatch.setenv("PATH", "")
    seen: dict[str, object] = {}

    def fake_run(args, **kwargs):
        seen["args"] = args
        seen["input"] = kwargs["input"]
        return SimpleNamespace(returncode=0, stdout="完成".encode("utf-8"), stderr=b"")

    monkeypatch.setattr(ralph_compile.subprocess, "run", fake_run)

    result = ralph_compile._run_claude("含有¥符号", repo_root=tmp_path)

    assert seen["args"][0] == str(wrapper.resolve())
    assert seen["input"] == "含有¥符号".encode("utf-8")
    assert result == "完成"


def test_run_claude_normalizes_windows_path_env(monkeypatch, tmp_path):
    wrapper = tmp_path / "claude.cmd"
    wrapper.write_text("@echo off\r\n", encoding="utf-8")
    monkeypatch.setattr(ralph_compile.os, "name", "nt", raising=False)
    monkeypatch.setattr(
        ralph_compile.os,
        "environ",
        {"PATH": "upper-path", "Path": "lower-path", "FOO": "bar"},
        raising=False,
    )
    seen: dict[str, object] = {}

    def fake_run(args, **kwargs):
        seen["env"] = kwargs["env"]
        return SimpleNamespace(returncode=0, stdout=b"ok", stderr=b"")

    monkeypatch.setattr(ralph_compile.subprocess, "run", fake_run)

    result = ralph_compile._run_claude("prompt", repo_root=tmp_path)

    env = seen["env"]
    assert "Path" not in env
    assert env["PATH"] == "upper-path"
    assert env["PYTHONIOENCODING"] == "utf-8"
    assert env["PYTHONUTF8"] == "1"
    assert result == "ok"


def test_build_round1_prompt_falls_back_to_workspace_skill_when_repo_root_missing(tmp_path):
    inputs = ralph_prompts.PromptInputs(
        truth_snapshot={"status": "ok"},
        current_doc27="doc27",
        current_prd={"userStories": []},
        source_snippets={},
    )

    prompt = ralph_prompts.build_round1_prompt(inputs, repo_root=tmp_path)

    assert f"repository at {tmp_path}" in prompt
    assert "Source skill instructions:" in prompt
    assert "Task:" in prompt


def test_rebuild_repo_does_not_rewrite_doc30(monkeypatch, tmp_path):
    _write_min_repo(tmp_path)
    doc30_path = tmp_path / "docs" / "core" / "30_Ralph双步自举运行手册.md"
    doc30_path.write_text("manual doc30\n", encoding="utf-8")
    original = doc30_path.read_text(encoding="utf-8")

    monkeypatch.setattr(ralph_compile, "collect_truth_snapshot", lambda **_: _fake_truth_snapshot())
    outputs = iter(
        [
            "## Introduction\n\nGenerated doc 27 narrative",
            json.dumps(
                [
                    {
                        "id": "US-101",
                        "title": "existing story",
                        "description": "existing story",
                        "acceptanceCriteria": ["GET /api/v1/home returns 200"],
                        "priority": 101,
                    }
                ],
                ensure_ascii=False,
            ),
        ]
    )
    monkeypatch.setattr(ralph_compile, "_run_claude", lambda *args, **kwargs: next(outputs))
    monkeypatch.setattr(ralph_compile, "_git_commit", lambda *args, **kwargs: None)

    summary = ralph_compile.rebuild_repo(repo_root=tmp_path)

    assert not any(path.startswith("docs/core/30_") for path in summary.changed_docs)
    assert doc30_path.read_text(encoding="utf-8") == original


def test_adjudicate_repo_bootstraps_existing_true_when_manifest_story_set_hash_is_stale(monkeypatch, tmp_path):
    _write_min_repo(tmp_path)
    (tmp_path / ".claude" / "ralph" / "loop" / "compile_manifest.json").write_text(
        json.dumps(
            {
                "baseline_commit": "old-baseline",
                "stories": {
                    "US-108": {
                        "fingerprint": "stale",
                        "write_scope_hash": "stale",
                        "runtime_sentinel_hash": "stale",
                        "last_decision": "keep_true",
                        "passes": True,
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(ralph_compile, "collect_truth_snapshot", lambda **_: _fake_truth_snapshot())

    summary = ralph_compile.adjudicate_repo(repo_root=tmp_path)

    assert summary.stories_total == 1
    assert summary.stories_passed == 1
    assert summary.regressed_story_ids == []
    payload = json.loads((tmp_path / ".claude" / "ralph" / "loop" / "prd.json").read_text(encoding="utf-8"))
    assert payload["userStories"][0]["passes"] is True
    refreshed_manifest = json.loads((tmp_path / ".claude" / "ralph" / "loop" / "compile_manifest.json").read_text(encoding="utf-8"))
    assert refreshed_manifest["story_set_hash"] == summary.story_set_hash
    assert refreshed_manifest["stories"]["US-108"]["passes"] is True
    compile_report = json.loads((tmp_path / ".claude" / "ralph" / "loop" / "compile_report.json").read_text(encoding="utf-8"))
    assert compile_report["adjudication"]["decisions"][0]["decision"] == "keep_true"
