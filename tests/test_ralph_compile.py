from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from codex import ralph_compile
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


def _full_note_payload() -> dict[str, object]:
    payload: dict[str, object] = {key: "" for key in ralph_compile.NOTE_KEYS}
    payload.update(
        {
            "group": "G10",
            "dependsOn": [],
            "endpoints": ["GET /api/v1/admin/overview"],
            "models": ["admin_overview"],
            "permissions": ["admin"],
            "errorCodes": [],
            "enums": [],
            "writeScope": ["app/admin.py"],
            "readScope": ["app/admin.py"],
            "runtimeChecks": [r"python .\check_state.py"],
            "dbTables": ["report"],
            "envDeps": [],
            "hardBlockers": [],
            "pytest": "python -m pytest tests/test_fr12_admin.py -q --tb=short",
        }
    )
    return payload


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
        "docs/core/01_\u9700\u6c42\u57fa\u7ebf.md",
        "docs/core/02_\u7cfb\u7edf\u67b6\u6784.md",
        "docs/core/05_API\u4e0e\u6570\u636e\u5951\u7ea6.md",
        "docs/core/06_\u5168\u91cf\u6570\u636e\u9700\u6c42\u8bf4\u660e.md",
        "docs/core/22_\u5168\u91cf\u529f\u80fd\u8fdb\u5ea6\u603b\u8868_v12.md",
        "docs/core/25_\u7cfb\u7edf\u95ee\u9898\u5206\u6790\u89d2\u5ea6\u6e05\u5355.md",
        "docs/core/26_\u81ea\u52a8\u5316\u6267\u884c\u8bb0\u5fc6.md",
        "docs/core/27_PRD_\u7814\u62a5\u5e73\u53f0\u589e\u5f3a\u4e0e\u6574\u4f53\u9a8c\u6536\u57fa\u7ebf.md",
        "docs/core/30_Ralph\u53cc\u6b65\u81ea\u4e3e\u8fd0\u884c\u624b\u518c.md",
        ".claude/skills/prd/SKILL.md",
        ".claude/skills/ralph/SKILL.md",
        "app/governance/feature_registry.json",
        "app/governance/catalog_snapshot.json",
    ]:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{rel}\n", encoding="utf-8")

    existing_prd = {
        "project": "demo",
        "branchName": "ralph/ashare-research-platform",
        "description": "demo",
        "userStories": [
            {
                "id": "US-108",
                "title": "restore FR-12 runtime closure",
                "description": "existing runtime admin story",
                "acceptanceCriteria": [
                    "GET /api/v1/admin/overview returns 200",
                    "python -m pytest tests/test_fr12_admin.py -q --tb=short",
                    "Typecheck passes",
                ],
                "priority": 108,
                "passes": True,
                "notes": json.dumps(_full_note_payload(), ensure_ascii=False),
            }
        ],
    }
    (root / ".claude" / "ralph" / "loop" / "prd.json").write_text(json.dumps(existing_prd, ensure_ascii=False, indent=2), encoding="utf-8")
    (root / ".claude" / "ralph" / "prd" / "yanbao-platform-enhancement.json").write_text(json.dumps(existing_prd, ensure_ascii=False, indent=2), encoding="utf-8")


def test_rebuild_repo_enriches_notes_and_appends_new_story_without_touching_doc30(monkeypatch, tmp_path):
    _write_min_repo(tmp_path)
    doc30_path = next((tmp_path / "docs" / "core").glob("30_*.md"))
    doc30_original = doc30_path.read_text(encoding="utf-8")
    monkeypatch.setattr(ralph_compile, "collect_truth_snapshot", lambda **_: _fake_truth_snapshot())
    outputs = iter(
        [
            "## Introduction\n\nGenerated doc 27 narrative",
            json.dumps(
                [
                    {
                        "title": "restore FR-12 runtime closure",
                        "description": "existing runtime admin story",
                        "acceptanceCriteria": [
                            "GET /api/v1/admin/overview returns 200",
                            "python -m pytest tests/test_fr12_admin.py -q --tb=short",
                        ],
                        "priority": 108,
                    },
                    {
                        "title": "add runtime closure story",
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
    assert not any(path.startswith("docs/core/30_") for path in summary.changed_docs)
    assert doc30_path.read_text(encoding="utf-8") == doc30_original
    payload = json.loads((tmp_path / ".claude" / "ralph" / "loop" / "prd.json").read_text(encoding="utf-8"))
    assert payload == json.loads((tmp_path / ".claude" / "ralph" / "prd" / "yanbao-platform-enhancement.json").read_text(encoding="utf-8"))
    assert {story["id"] for story in payload["userStories"]} == {"US-108", "US-109"}
    for story in payload["userStories"]:
        notes = json.loads(story["notes"])
        assert set(notes.keys()) == set(ralph_compile.NOTE_KEYS)
    compile_report = json.loads((tmp_path / ".claude" / "ralph" / "loop" / "compile_report.json").read_text(encoding="utf-8"))
    assert compile_report["mode"] == "rebuild"


def test_verify_repo_raises_when_dual_prd_mismatches(tmp_path):
    _write_min_repo(tmp_path)
    named_prd_path = tmp_path / ".claude" / "ralph" / "prd" / "yanbao-platform-enhancement.json"
    payload = json.loads(named_prd_path.read_text(encoding="utf-8"))
    payload["description"] = "different"
    named_prd_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with pytest.raises(RuntimeError, match="dual_prd_mismatch"):
        ralph_compile.verify_repo(repo_root=tmp_path)


def test_verify_repo_raises_when_note_keys_are_missing(tmp_path):
    _write_min_repo(tmp_path)
    loop_prd_path = tmp_path / ".claude" / "ralph" / "loop" / "prd.json"
    payload = json.loads(loop_prd_path.read_text(encoding="utf-8"))
    payload["userStories"][0]["notes"] = json.dumps({"group": "G10"}, ensure_ascii=False)
    loop_prd_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (tmp_path / ".claude" / "ralph" / "prd" / "yanbao-platform-enhancement.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with pytest.raises(RuntimeError, match=r"missing_note_keys:US-108:"):
        ralph_compile.verify_repo(repo_root=tmp_path)


def test_verify_repo_raises_when_runner_dry_run_fails(monkeypatch, tmp_path):
    _write_min_repo(tmp_path)

    def fake_run(*args, **kwargs):
        raise subprocess.CalledProcessError(1, args[0], output="", stderr="runner boom")

    monkeypatch.setattr(ralph_compile.subprocess, "run", fake_run)

    with pytest.raises(subprocess.CalledProcessError):
        ralph_compile.verify_repo(repo_root=tmp_path)


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
        return SimpleNamespace(returncode=0, stdout="??".encode("utf-8"), stderr=b"")

    monkeypatch.setattr(ralph_compile.subprocess, "run", fake_run)

    result = ralph_compile._run_claude("?????", repo_root=tmp_path)

    assert seen["args"][0] == str(wrapper.resolve())
    assert seen["input"] == "?????".encode("utf-8")
    assert result == "??"
