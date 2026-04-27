from __future__ import annotations

from pathlib import Path

from codex.ralph_adjudicate import adjudicate_prd
from codex.ralph_story_normalize import compact_json, story_fingerprint, story_write_scope_hash
from codex.ralph_truth import ProbeSummary, RuntimeSentinelState, TruthSnapshot


def _truth(ok: bool = True) -> TruthSnapshot:
    return TruthSnapshot(
        generated_at="2026-04-28T00:00:00+00:00",
        check_state_stdout="Active tasks: 0\nTotal published: 9",
        active_task_count=0,
        published_report_count=9,
        sqlite={"report_published": 9},
        probes={"/api/v1/home": ProbeSummary("/api/v1/home", 200, True, {"data": {"pool_size": 200}}, None)},
        anchors={"runtime_trade_date": "2026-03-13", "public_pool_trade_date": "2026-04-24", "public_pool_size": 200},
        sentinels={"published_reports_nonzero": RuntimeSentinelState("published_reports_nonzero", ok)},
    )


def _story() -> dict:
    notes = compact_json(
        {
            "group": "G10",
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
            "pytest": "",
            "writeScope": ["app/example.py"],
            "readScope": [],
            "runtimeChecks": ["published_reports_nonzero"],
            "dbTables": [],
            "envDeps": [],
            "hardBlockers": [],
        }
    )
    return {
        "project": "demo",
        "branchName": "ralph/demo",
        "description": "demo",
        "userStories": [
            {
                "id": "US-104",
                "title": "恢复 FR-06 运行态正式发布链",
                "description": "demo story",
                "acceptanceCriteria": ["Typecheck passes"],
                "priority": 104,
                "passes": True,
                "notes": notes,
            }
        ],
    }


def test_adjudicate_keeps_true_when_fingerprint_scope_and_runtime_match(tmp_path: Path):
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "example.py").write_text("print('ok')\n", encoding="utf-8")
    prd = _story()
    story = prd["userStories"][0]
    manifest = {
        "stories": {
            "US-104": {
                "fingerprint": story_fingerprint(story),
                "write_scope_hash": story_write_scope_hash(tmp_path, ["app/example.py"]),
                "runtime_sentinel_hash": "ignored",
                "last_decision": "keep_true",
                "passes": True,
            }
        }
    }

    updated, result = adjudicate_prd(prd, truth_snapshot=_truth(True), repo_root=tmp_path, previous_manifest=manifest, generated_at="now")

    assert updated["userStories"][0]["passes"] is True
    assert result.decisions[0].decision == "keep_true"


def test_adjudicate_regresses_true_when_runtime_sentinel_fails(tmp_path: Path):
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "example.py").write_text("print('ok')\n", encoding="utf-8")
    prd = _story()
    story = prd["userStories"][0]
    manifest = {
        "stories": {
            "US-104": {
                "fingerprint": story_fingerprint(story),
                "write_scope_hash": story_write_scope_hash(tmp_path, ["app/example.py"]),
                "runtime_sentinel_hash": "ignored",
                "last_decision": "keep_true",
                "passes": True,
            }
        }
    }

    updated, result = adjudicate_prd(prd, truth_snapshot=_truth(False), repo_root=tmp_path, previous_manifest=manifest, generated_at="now")

    assert updated["userStories"][0]["passes"] is False
    assert result.decisions[0].decision == "regress_to_false"

