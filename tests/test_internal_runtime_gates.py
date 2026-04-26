from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

import app.api.routes_internal as routes_internal
from automation.loop_controller.app import create_app
from automation.loop_controller.controller import LoopControllerConfig
from automation.loop_controller.schemas import LoopMode, LoopPhase, LoopState


class _StubController:
    def __init__(self, state: LoopState, *, running: bool = True) -> None:
        self._state = state
        self.running = running

    def get_state(self) -> LoopState:
        return self._state


def test_runtime_gates_requires_internal_token(client):
    response = client.get("/api/v1/internal/runtime/gates")

    assert response.status_code == 401
    assert response.json()["error_code"] == "UNAUTHORIZED"


def test_runtime_gates_reports_blocked_runtime_and_artifact_state(client, internal_headers, monkeypatch):
    monkeypatch.setattr(
        routes_internal,
        "runtime_metrics_summary",
        lambda db: {
            "runtime_state": "degraded",
            "runtime_flags": ["sim_snapshot_missing", "public_runtime_degraded"],
            "runtime_anchors": {"runtime_trade_date": "2026-03-27"},
            "data_quality": {"flags": ["sim_snapshot_missing"]},
            "service_health": {"status": "normal"},
            "business_health": {"status": "normal"},
        },
    )
    monkeypatch.setattr(
        routes_internal,
        "_shared_artifact_snapshot",
        lambda: {
            "junit": {"exists": True, "failures": 0, "errors": 0},
            "catalog_snapshot": {"exists": True, "test_result_freshness": "fresh", "total_collected": 1515},
            "blind_spot_audit": {"exists": True, "fake": 0, "hollow": 0, "weak": 0, "guarded": 0},
            "continuous_audit": {
                "exists": True,
                "status": "completed",
                "finding_count": 0,
                "warn_features": 0,
                "mismatch_count": 0,
                "catalog_total_collected": 1515,
            },
        },
    )
    monkeypatch.setattr(routes_internal, "get_primary_status", lambda: "ok")

    response = client.get(
        "/api/v1/internal/runtime/gates",
        headers=internal_headers("internal-runtime-gates-token"),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["degraded"] is True
    assert body["degraded_reason"] == "runtime_live_recovery_blocked"
    data = body["data"]
    assert data["status"] == "blocked"
    assert data["runtime_live_recovery"]["allowed"] is False
    assert data["runtime_live_recovery"]["blocking_flags"] == [
        "sim_snapshot_missing",
        "public_runtime_degraded",
    ]
    assert data["shared_artifact_promote"]["allowed"] is False
    assert data["llm_router"]["ready"] is True


def test_runtime_gates_reports_ready_when_runtime_and_artifacts_are_clean(client, internal_headers, monkeypatch):
    monkeypatch.setattr(
        routes_internal,
        "runtime_metrics_summary",
        lambda db: {
            "runtime_state": "normal",
            "runtime_flags": [],
            "runtime_anchors": {"runtime_trade_date": "2026-03-27"},
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
            "catalog_snapshot": {"exists": True, "test_result_freshness": "fresh", "total_collected": 1515},
            "blind_spot_audit": {"exists": True, "fake": 0, "hollow": 0, "weak": 0, "guarded": 0},
            "continuous_audit": {
                "exists": True,
                "status": "completed",
                "finding_count": 0,
                "warn_features": 0,
                "mismatch_count": 0,
                "catalog_total_collected": 1515,
            },
        },
    )
    monkeypatch.setattr(routes_internal, "get_primary_status", lambda: "ok")

    response = client.get(
        "/api/v1/internal/runtime/gates",
        headers=internal_headers("internal-runtime-gates-token"),
    )

    assert response.status_code == 200
    body = response.json()
    assert body.get("degraded") in (None, False)
    data = body["data"]
    assert data["status"] == "ready"
    assert data["runtime_live_recovery"]["allowed"] is True
    assert data["shared_artifact_promote"]["allowed"] is True
    assert data["shared_artifact_promote"]["blind_spot_clean"] is True
    assert data["shared_artifact_promote"]["continuous_audit_complete"] is True
    assert data["shared_artifact_promote"]["artifacts_same_round"] is True


def test_runtime_gates_degrade_when_audit_backlog_or_round_drift_present(client, internal_headers, monkeypatch):
    monkeypatch.setattr(
        routes_internal,
        "runtime_metrics_summary",
        lambda db: {
            "runtime_state": "normal",
            "runtime_flags": [],
            "runtime_anchors": {"runtime_trade_date": "2026-03-31"},
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
            "catalog_snapshot": {"exists": True, "test_result_freshness": "fresh", "total_collected": 1515},
            "blind_spot_audit": {"exists": True, "fake": 0, "hollow": 0, "weak": 0, "guarded": 0},
            "continuous_audit": {
                "exists": True,
                "status": "completed",
                "finding_count": 0,
                "warn_features": 2,
                "mismatch_count": 1,
                "catalog_total_collected": 1315,
            },
        },
    )
    monkeypatch.setattr(routes_internal, "get_primary_status", lambda: "ok")

    response = client.get(
        "/api/v1/internal/runtime/gates",
        headers=internal_headers("internal-runtime-gates-token"),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["degraded"] is True
    assert body.get("degraded_reason") is None
    data = body["data"]
    assert data["status"] == "degraded"
    assert data["runtime_live_recovery"]["allowed"] is True
    assert data["shared_artifact_promote"]["continuous_audit_complete"] is False
    assert data["shared_artifact_promote"]["artifacts_same_round"] is False
    assert data["shared_artifact_promote"]["allowed"] is False


def test_shared_artifact_snapshot_parses_blind_spot_new_and_legacy_shapes(monkeypatch, tmp_path: Path):
    junit_path = tmp_path / "junit.xml"
    junit_path.write_text('<testsuite tests="1" failures="0" errors="0" skipped="0" />', encoding="utf-8")
    catalog_path = tmp_path / "catalog_snapshot.json"
    catalog_path.write_text(
        json.dumps({"generated_at": "2026-03-31T00:00:00Z", "test_result_freshness": "fresh", "total_collected": 10}),
        encoding="utf-8",
    )
    audit_path = tmp_path / "latest_run.json"
    audit_path.write_text(
        json.dumps(
            {
                "status": "completed",
                "findings": [],
                "registry_stats": {"warn_features": 0, "mismatch_count": 0},
                "shared_artifact_status": {"catalog_total_collected": 10},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        routes_internal,
        "_SHARED_ARTIFACT_PATHS",
        {
            "junit": junit_path,
            "catalog_snapshot": catalog_path,
            "blind_spot_audit": tmp_path / "blind_spot_audit.json",
            "continuous_audit": audit_path,
        },
    )

    (tmp_path / "blind_spot_audit.json").write_text(
        json.dumps({"summary": {"fake_count": 1, "hollow_count": 2, "weak_count": 3, "guarded_assertions": 4}}),
        encoding="utf-8",
    )
    snapshot = routes_internal._shared_artifact_snapshot()
    assert snapshot["blind_spot_audit"]["fake"] == 1
    assert snapshot["blind_spot_audit"]["hollow"] == 2
    assert snapshot["blind_spot_audit"]["weak"] == 3
    assert snapshot["blind_spot_audit"]["guarded"] == 4

    (tmp_path / "blind_spot_audit.json").write_text(
        json.dumps({"FAKE": 5, "HOLLOW": 6, "WEAK": 7, "GUARDED": 8}),
        encoding="utf-8",
    )
    snapshot = routes_internal._shared_artifact_snapshot()
    assert snapshot["blind_spot_audit"]["fake"] == 5
    assert snapshot["blind_spot_audit"]["hollow"] == 6
    assert snapshot["blind_spot_audit"]["weak"] == 7
    assert snapshot["blind_spot_audit"]["guarded"] == 8


def test_loop_controller_app_goal_reached_requires_real_promote():
    state = LoopState(
        mode=LoopMode.MONITOR,
        phase=LoopPhase.MONITORING,
        consecutive_fix_success_count=12,
        consecutive_verified_problem_fixes=12,
        fix_goal=12,
        success_goal_metric="verified_problem_count",
        last_promote_round_id=None,
    )
    app = create_app(
        config=LoopControllerConfig(
            repo_root=Path("."),
            mesh_runner_url="http://mesh",
            mesh_runner_token="mesh-token",
            promote_prep_url="http://prep",
            promote_prep_token="prep-token",
            writeback_a_url="http://wb-a",
            writeback_a_token="wb-a-token",
            writeback_b_url="http://wb-b",
            writeback_b_token="wb-b-token",
            auth_token="loop-token",
        ),
        controller=_StubController(state),
    )

    with TestClient(app) as client:
        health_resp = client.get("/health")
        state_resp = client.get("/v1/state", headers={"Authorization": "Bearer loop-token"})

    assert health_resp.status_code == 200
    assert health_resp.json()["goal_reached"] is False
    assert state_resp.status_code == 200
    assert state_resp.json()["goal_reached"] is False
