from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from codex import ralph_compile
from codex import ralph_prompts
from codex.ralph_story_normalize import normalize_story_list, parse_notes_payload
from codex.ralph_truth import ProbeSummary, RuntimeSentinelState, TruthSnapshot, derive_runtime_sentinels, query_sqlite_truth


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


def _verify_note_payload(
    *,
    group: str = "G1",
    write_scope: list[str] | None = None,
    runtime_checks: list[str] | None = None,
) -> dict[str, object]:
    return {
        "group": group,
        "dependsOn": [],
        "endpoints": [],
        "models": [],
        "permissions": [],
        "errorCodes": [],
        "idempotency": "",
        "enums": [],
        "thresholds": "",
        "degradation": "",
        "exampleAssert": "",
        "pytest": "python -m pytest tests/test_example.py -q --tb=short",
        "writeScope": write_scope if write_scope is not None else ["tests/test_example.py"],
        "readScope": [],
        "runtimeChecks": runtime_checks if runtime_checks is not None else [],
        "dbTables": [],
        "envDeps": [],
        "hardBlockers": [],
    }


def _verify_story(
    story_id: str,
    *,
    group: str = "G1",
    write_scope: list[str] | None = None,
    runtime_checks: list[str] | None = None,
    passes: bool = True,
) -> dict[str, object]:
    return {
        "id": story_id,
        "title": f"Story {story_id}",
        "description": "demo",
        "acceptanceCriteria": ["Typecheck passes"],
        "priority": int(story_id.split("-")[1]),
        "passes": passes,
        "notes": json.dumps(
            _verify_note_payload(group=group, write_scope=write_scope, runtime_checks=runtime_checks),
            ensure_ascii=False,
        ),
    }


def _write_verify_prd_pair(root: Path, stories: list[dict[str, object]]) -> None:
    (root / ".claude" / "ralph" / "loop").mkdir(parents=True)
    (root / ".claude" / "ralph" / "prd").mkdir(parents=True)
    for story in stories:
        notes = parse_notes_payload(story.get("notes"))
        for entry in notes.get("writeScope") or []:
            if any(char in str(entry) for char in ("*", "?", "[")):
                continue
            path = root / str(entry)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# write scope fixture\n", encoding="utf-8")
    prd = {
        "project": "demo",
        "branchName": "main",
        "description": "demo",
        "userStories": stories,
    }
    for rel in [
        ".claude/ralph/loop/prd.json",
        ".claude/ralph/prd/yanbao-platform-enhancement.json",
    ]:
        (root / rel).write_text(json.dumps(prd, ensure_ascii=False, indent=2), encoding="utf-8")


def _pinned_runtime_stories(*, omit: str | None = None) -> list[dict[str, object]]:
    stories: list[dict[str, object]] = []
    for number in range(101, 109):
        story_id = f"US-{number:03d}"
        if story_id == omit:
            continue
        stories.append(
            _verify_story(
                story_id,
                group=f"FR{number}_RUNTIME",
                runtime_checks=[f"runtime_check_{number}"],
            )
        )
    return stories


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


def test_normalize_story_list_infers_write_scope_from_progress_before_pytest(tmp_path):
    progress_path = tmp_path / ".claude" / "ralph" / "loop" / "progress.txt"
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.write_text(
        "\n".join(
            [
                "## 2026-04-27 19:42 - US-104",
                "- Files changed: app/services/llm_router.py, app/services/report_generation_ssot.py, tests/test_fr06_report_generate.py, .claude/ralph/loop/prd.json, .claude/ralph/loop/progress.txt.",
            ]
        ),
        encoding="utf-8",
    )
    existing_prd = {
        "userStories": [
            {
                "id": "US-104",
                "title": "恢复 FR-06 运行态正式发布链",
                "description": "existing",
                "acceptanceCriteria": ["python -m pytest tests/test_fr06_quality_pipeline.py -q --tb=short"],
                "priority": 104,
                "passes": True,
                "notes": json.dumps(
                    {
                        "group": "FR06",
                        "dependsOn": [],
                        "endpoints": [],
                        "models": ["report"],
                        "permissions": [],
                        "errorCodes": [],
                        "idempotency": "",
                        "enums": [],
                        "thresholds": "",
                        "degradation": "",
                        "exampleAssert": "",
                        "pytest": "python -m pytest tests/test_fr06_quality_pipeline.py -q --tb=short",
                        "writeScope": [],
                        "readScope": [],
                        "runtimeChecks": ["published_reports_nonzero"],
                        "dbTables": [],
                        "envDeps": [],
                        "hardBlockers": [],
                    },
                    ensure_ascii=False,
                ),
            }
        ]
    }

    normalized = normalize_story_list(
        list(existing_prd["userStories"]),
        existing_prd=existing_prd,
        repo_root=tmp_path,
    )

    notes = parse_notes_payload(normalized["userStories"][0]["notes"])
    assert notes["writeScope"] == [
        "app/services/llm_router.py",
        "app/services/report_generation_ssot.py",
        "tests/test_fr06_report_generate.py",
    ]


def test_verify_repo_rejects_passed_story_without_write_scope(monkeypatch, tmp_path):
    (tmp_path / ".claude" / "ralph" / "loop").mkdir(parents=True)
    (tmp_path / ".claude" / "ralph" / "prd").mkdir(parents=True)
    notes = {
        "group": "G1",
        "dependsOn": [],
        "endpoints": [],
        "models": [],
        "permissions": [],
        "errorCodes": [],
        "idempotency": "",
        "enums": [],
        "thresholds": "",
        "degradation": "",
        "exampleAssert": "",
        "pytest": "python -m pytest tests/test_example.py -q --tb=short",
        "writeScope": [],
        "readScope": [],
        "runtimeChecks": [],
        "dbTables": [],
        "envDeps": [],
        "hardBlockers": [],
    }
    prd = {
        "project": "demo",
        "branchName": "main",
        "description": "demo",
        "userStories": [
            {
                "id": "US-001",
                "title": "demo story",
                "description": "demo",
                "acceptanceCriteria": ["Typecheck passes"],
                "priority": 1,
                "passes": True,
                "notes": json.dumps(notes, ensure_ascii=False),
            }
        ],
    }
    for rel in [
        ".claude/ralph/loop/prd.json",
        ".claude/ralph/prd/yanbao-platform-enhancement.json",
    ]:
        (tmp_path / rel).write_text(json.dumps(prd, ensure_ascii=False, indent=2), encoding="utf-8")

    monkeypatch.setattr(ralph_compile.subprocess, "run", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="empty_write_scope:US-001"):
        ralph_compile.verify_repo(repo_root=tmp_path)


def test_verify_repo_rejects_runtime_story_without_runtime_checks(monkeypatch, tmp_path):
    (tmp_path / ".claude" / "ralph" / "loop").mkdir(parents=True)
    (tmp_path / ".claude" / "ralph" / "prd").mkdir(parents=True)
    (tmp_path / "tests").mkdir(parents=True)
    (tmp_path / "tests" / "test_example.py").write_text("# write scope fixture\n", encoding="utf-8")
    notes = {
        "group": "RUNTIME_HISTORY_RUNTIME",
        "dependsOn": [],
        "endpoints": [],
        "models": [],
        "permissions": [],
        "errorCodes": [],
        "idempotency": "",
        "enums": [],
        "thresholds": "",
        "degradation": "",
        "exampleAssert": "",
        "pytest": "python -m pytest tests/test_example.py -q --tb=short",
        "writeScope": ["tests/test_example.py"],
        "readScope": [],
        "runtimeChecks": [],
        "dbTables": [],
        "envDeps": [],
        "hardBlockers": [],
    }
    prd = {
        "project": "demo",
        "branchName": "main",
        "description": "demo",
        "userStories": [
            {
                "id": "US-109",
                "title": "runtime story",
                "description": "demo",
                "acceptanceCriteria": ["Typecheck passes"],
                "priority": 109,
                "passes": True,
                "notes": json.dumps(notes, ensure_ascii=False),
            }
        ],
    }
    for rel in [
        ".claude/ralph/loop/prd.json",
        ".claude/ralph/prd/yanbao-platform-enhancement.json",
    ]:
        (tmp_path / rel).write_text(json.dumps(prd, ensure_ascii=False, indent=2), encoding="utf-8")

    monkeypatch.setattr(ralph_compile.subprocess, "run", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="empty_runtime_checks:US-109"):
        ralph_compile.verify_repo(repo_root=tmp_path)


def test_verify_repo_rejects_prd_branch_name_policy_mismatch(monkeypatch, tmp_path):
    (tmp_path / ".claude" / "ralph").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".claude" / "ralph" / "config.json").write_text(
        json.dumps({"branchNamePolicy": {"currentValue": "main"}}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_verify_prd_pair(tmp_path, [_verify_story("US-001")])
    for rel in [
        ".claude/ralph/loop/prd.json",
        ".claude/ralph/prd/yanbao-platform-enhancement.json",
    ]:
        prd_path = tmp_path / rel
        prd = json.loads(prd_path.read_text(encoding="utf-8"))
        prd["branchName"] = "feature/foo"
        prd_path.write_text(json.dumps(prd, ensure_ascii=False, indent=2), encoding="utf-8")
    monkeypatch.setattr(ralph_compile.subprocess, "run", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match=r"branch_name_policy_mismatch:prd=feature/foo:expected=main"):
        ralph_compile.verify_repo(repo_root=tmp_path)


def test_verify_repo_rejects_passed_story_with_missing_write_scope_path(monkeypatch, tmp_path):
    stories = [
        *_pinned_runtime_stories(),
        _verify_story(
            "US-109",
            group="RUNTIME_HISTORY_RUNTIME",
            write_scope=["tests/missing_runtime_canary.py"],
            runtime_checks=["runtime_history_repair_consistent"],
        ),
    ]
    _write_verify_prd_pair(tmp_path, stories[:-1])
    prd = {
        "project": "demo",
        "branchName": "main",
        "description": "demo",
        "userStories": stories,
    }
    for rel in [
        ".claude/ralph/loop/prd.json",
        ".claude/ralph/prd/yanbao-platform-enhancement.json",
    ]:
        (tmp_path / rel).write_text(json.dumps(prd, ensure_ascii=False, indent=2), encoding="utf-8")
    monkeypatch.setattr(ralph_compile.subprocess, "run", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match=r"invalid_write_scope_path:US-109:tests/missing_runtime_canary\.py"):
        ralph_compile.verify_repo(repo_root=tmp_path)


def test_verify_repo_rejects_passed_story_with_write_scope_outside_repo(monkeypatch, tmp_path):
    stories = [
        *_pinned_runtime_stories(),
        _verify_story(
            "US-109",
            group="RUNTIME_HISTORY_RUNTIME",
            write_scope=["../outside.py"],
            runtime_checks=["runtime_history_repair_consistent"],
        ),
    ]
    _write_verify_prd_pair(tmp_path, stories[:-1])
    prd = {
        "project": "demo",
        "branchName": "main",
        "description": "demo",
        "userStories": stories,
    }
    for rel in [
        ".claude/ralph/loop/prd.json",
        ".claude/ralph/prd/yanbao-platform-enhancement.json",
    ]:
        (tmp_path / rel).write_text(json.dumps(prd, ensure_ascii=False, indent=2), encoding="utf-8")
    monkeypatch.setattr(ralph_compile.subprocess, "run", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match=r"invalid_write_scope_path:US-109:\.\./outside\.py"):
        ralph_compile.verify_repo(repo_root=tmp_path)


def test_verify_repo_rejects_missing_pinned_runtime_story(monkeypatch, tmp_path):
    _write_verify_prd_pair(tmp_path, _pinned_runtime_stories(omit="US-103"))
    monkeypatch.setattr(ralph_compile.subprocess, "run", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="missing_pinned_runtime_story:US-103"):
        ralph_compile.verify_repo(repo_root=tmp_path)


def test_verify_repo_rejects_non_append_runtime_story_ids(monkeypatch, tmp_path):
    stories = [
        *_pinned_runtime_stories(),
        _verify_story(
            "US-110",
            group="RUNTIME_HISTORY_RUNTIME",
            runtime_checks=["runtime_history_repair_consistent"],
        ),
    ]
    _write_verify_prd_pair(tmp_path, stories)
    monkeypatch.setattr(ralph_compile.subprocess, "run", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="non_append_runtime_story_ids:missing=US-109"):
        ralph_compile.verify_repo(repo_root=tmp_path)


def test_runtime_history_repair_sentinel_requires_strict_ok_clean_tasks():
    probes = {
        "/api/v1/home": ProbeSummary("/api/v1/home", 200, True, payload={"data": {"pool_size": 1}}),
        "/api/v1/market/state": ProbeSummary(
            "/api/v1/market/state",
            200,
            True,
            payload={"data": {"trade_date": "2026-03-21", "market_state": "BULL"}},
        ),
        "/api/v1/reports?limit=3": ProbeSummary(
            "/api/v1/reports?limit=3",
            200,
            True,
            payload={"data": {"total": 1}},
        ),
        "/api/v1/dashboard/stats?window_days=7": ProbeSummary(
            "/api/v1/dashboard/stats?window_days=7",
            200,
            True,
            payload={"data": {"data_status": "READY"}},
        ),
    }
    anchors = {
        "runtime_trade_date": "2026-03-21",
        "latest_published_report_trade_date": "2026-03-21",
        "latest_complete_public_batch_trade_date": "2026-03-21",
        "public_pool_trade_date": "2026-03-21",
        "public_pool_size": 1,
    }
    sqlite_truth = {
        "report_data_usage_count": 1,
        "report_published": 1,
        "settlement_total": 1,
        "sim_position_open": 1,
        "published_ok_nonterminal_task_count": 0,
    }

    sentinels = derive_runtime_sentinels(sqlite_truth=sqlite_truth, probes=probes, anchors=anchors)

    assert sentinels["runtime_history_repair_consistent"].ok is True
    assert sentinels["runtime_history_repair_consistent"].details == {
        "latest_complete_public_batch_trade_date": "2026-03-21",
        "missing_complete_public_batch_anchor": False,
        "warnings": [],
        "report_published": 1,
        "published_ok_nonterminal_task_count": 0,
    }

    sqlite_truth["published_ok_nonterminal_task_count"] = 1
    sentinels = derive_runtime_sentinels(sqlite_truth=sqlite_truth, probes=probes, anchors=anchors)
    assert sentinels["runtime_history_repair_consistent"].ok is False


def test_runtime_history_repair_sentinel_warns_without_complete_batch_anchor():
    probes = {
        "/api/v1/home": ProbeSummary("/api/v1/home", 200, True, payload={"data": {"pool_size": 1}}),
        "/api/v1/market/state": ProbeSummary(
            "/api/v1/market/state",
            200,
            True,
            payload={"data": {"trade_date": "2026-03-21", "market_state": "BULL"}},
        ),
        "/api/v1/reports?limit=3": ProbeSummary(
            "/api/v1/reports?limit=3",
            200,
            True,
            payload={"data": {"total": 1}},
        ),
    }
    anchors = {
        "runtime_trade_date": "2026-03-21",
        "latest_published_report_trade_date": "2026-03-21",
        "latest_complete_public_batch_trade_date": None,
        "public_pool_trade_date": "2026-03-21",
        "public_pool_size": 1,
    }
    sqlite_truth = {
        "report_data_usage_count": 1,
        "report_published": 1,
        "settlement_total": 1,
        "sim_position_open": 1,
        "published_ok_nonterminal_task_count": 0,
    }

    sentinels = derive_runtime_sentinels(sqlite_truth=sqlite_truth, probes=probes, anchors=anchors)

    sentinel = sentinels["runtime_history_repair_consistent"]
    assert sentinel.ok is True
    assert sentinel.details["latest_complete_public_batch_trade_date"] is None
    assert sentinel.details["missing_complete_public_batch_anchor"] is True
    assert sentinel.details["warnings"] == ["missing_complete_public_batch_anchor"]


def test_query_sqlite_truth_missing_db_fails_without_creating_file(tmp_path):
    missing_db = tmp_path / "missing-app.db"

    with pytest.raises(RuntimeError, match="sqlite_database_missing:"):
        query_sqlite_truth(missing_db)

    assert not missing_db.exists()


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
