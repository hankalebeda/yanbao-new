from __future__ import annotations

import json
from pathlib import Path

import app.api.routes_internal as routes_internal


def test_audit_context_requires_internal_token(client):
    response = client.get("/api/v1/internal/audit/context")

    assert response.status_code == 401
    assert response.json()["error_code"] == "UNAUTHORIZED"


def test_audit_context_returns_runtime_and_doc_metadata(client, internal_headers, monkeypatch):
    monkeypatch.setattr(
        routes_internal,
        "runtime_metrics_summary",
        lambda db: {
            "runtime_state": "degraded",
            "runtime_flags": ["public_runtime_degraded"],
            "runtime_anchors": {
                "runtime_trade_date": "2026-03-27",
                "latest_published_report_trade_date": "2026-03-20",
            },
            "data_quality": {"flags": ["public_runtime_degraded"]},
            "service_health": {"status": "normal"},
            "business_health": {"status": "normal"},
        },
    )
    monkeypatch.setattr(
        routes_internal,
        "_shared_artifact_snapshot",
        lambda: {
            "junit": {"exists": True, "failures": 0, "errors": 0},
            "catalog_snapshot": {"exists": True, "test_result_freshness": "fresh", "total_collected": 119},
            "blind_spot_audit": {"exists": True, "fake": 0, "hollow": 0},
            "continuous_audit": {"exists": True, "status": "completed", "catalog_total_collected": 119},
        },
    )
    monkeypatch.setattr(routes_internal, "get_primary_status", lambda: "ok")

    response = client.get(
        "/api/v1/internal/audit/context",
        headers=internal_headers("internal-audit-context-token"),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["degraded"] is True
    data = body["data"]
    assert data["runtime_gates"]["status"] == "blocked"
    assert data["latest_published_report_trade_date"] == "2026-03-20"
    assert data["public_runtime_status"] == "degraded"
    assert data["docs"]["progress_doc_path"].endswith("22_全量功能进度总表_v7_精审.md")
    assert data["docs"]["analysis_lens_doc_path"].endswith("25_系统问题分析角度清单.md")


def test_audit_context_includes_local_automation_snapshots(client, internal_headers, monkeypatch, tmp_path):
    loop_state = tmp_path / "runtime" / "loop_controller" / "state.json"
    loop_state.parent.mkdir(parents=True, exist_ok=True)
    loop_state.write_text(
        json.dumps(
            {
                "mode": "fix",
                "phase": "verifying",
                "current_round_id": "fix-loop-20260328-003",
                "consecutive_fix_success_count": 4,
                "fix_goal": 10,
                "total_fixes": 7,
                "total_failures": 1,
                "problems_queue": [{"problem_id": "truth-lineage:TL-001"}],
                "round_history": [{"round_id": "fix-loop-20260328-001"}],
                "last_audit_run_id": "issue-mesh-20260328-101",
                "last_fix_wave_id": "fix-loop-20260328-003",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    issue_mesh_root = tmp_path / "runtime" / "issue_mesh" / "issue-mesh-20260328-101"
    issue_mesh_root.mkdir(parents=True, exist_ok=True)
    (issue_mesh_root / "status.json").write_text(
        json.dumps(
            {
                "status": "completed",
                "created_at": "2026-03-28T00:00:00+00:00",
                "started_at": "2026-03-28T00:01:00+00:00",
                "finished_at": "2026-03-28T00:05:00+00:00",
                "summary_path": "runtime/issue_mesh/issue-mesh-20260328-101/summary.json",
                "bundle_path": "runtime/issue_mesh/issue-mesh-20260328-101/bundle.json",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (issue_mesh_root / "bundle.json").write_text(
        json.dumps({"finding_count": 3}, ensure_ascii=False),
        encoding="utf-8",
    )
    code_fix_root = tmp_path / "runtime" / "issue_mesh" / "promote_prep" / "code_fix" / "fix-loop-20260328-003"
    code_fix_root.mkdir(parents=True, exist_ok=True)
    (code_fix_root / "patches.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-28T00:06:00+00:00",
                "patch_count": 2,
                "total_findings": 2,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (code_fix_root / "pytest_result.json").write_text(
        json.dumps(
            {
                "passed": True,
                "test_count": 6,
                "ran_at": "2026-03-28T00:08:00+00:00",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        routes_internal,
        "runtime_metrics_summary",
        lambda db: {
            "runtime_state": "normal",
            "runtime_flags": [],
            "runtime_anchors": {"runtime_trade_date": "2026-03-28"},
            "data_quality": {"flags": []},
            "service_health": {"status": "normal"},
            "business_health": {"status": "normal"},
        },
    )
    monkeypatch.setattr(
        routes_internal,
        "_shared_artifact_snapshot",
        lambda: {
            "junit": {"exists": True, "failures": 0, "errors": 0},
            "catalog_snapshot": {"exists": True, "test_result_freshness": "fresh", "total_collected": 119},
            "blind_spot_audit": {"exists": True, "fake": 0, "hollow": 0},
            "continuous_audit": {"exists": True, "status": "completed", "catalog_total_collected": 119},
        },
    )
    monkeypatch.setattr(routes_internal, "get_primary_status", lambda: "ok")
    monkeypatch.setattr(routes_internal, "_LOOP_CONTROLLER_STATE_PATH", loop_state)
    monkeypatch.setattr(routes_internal, "_ISSUE_MESH_RUNTIME_ROOT", tmp_path / "runtime" / "issue_mesh")
    monkeypatch.setattr(
        routes_internal,
        "_CODE_FIX_RUNTIME_ROOT",
        tmp_path / "runtime" / "issue_mesh" / "promote_prep" / "code_fix",
    )

    response = client.get(
        "/api/v1/internal/audit/context",
        headers=internal_headers("internal-audit-context-token"),
    )

    assert response.status_code == 200
    automation = response.json()["data"]["automation"]
    assert automation["promote_readiness"]["status"] == "promote_ready"
    assert automation["loop_controller"]["phase"] == "verifying"
    assert automation["loop_controller"]["latest_audit_run_id"] == "issue-mesh-20260328-101"
    assert automation["latest_issue_mesh_run"]["run_id"] == "issue-mesh-20260328-101"
    assert automation["latest_issue_mesh_run"]["finding_count"] == 3
    assert automation["latest_code_fix_wave"]["fix_run_id"] == "fix-loop-20260328-003"
    assert automation["latest_code_fix_wave"]["pytest_passed"] is True
