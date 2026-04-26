from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from fastapi.testclient import TestClient

from automation.mesh_runner.app import create_app
from automation.mesh_runner.runner import MeshRunnerConfig, MeshRunnerService, STALE_RUN_RECLAIM_ERROR
from scripts import codex_mesh


def test_mesh_runner_requires_explicit_allowlist_when_lane_is_enabled(tmp_path):
    cfg = MeshRunnerConfig(
        repo_root=tmp_path.resolve(),
        runtime_root=(tmp_path / "runtime" / "issue_mesh").resolve(),
        auth_token="runner-secret",
        canonical_provider="newapi-192.168.232.141-3000",
        readonly_max_workers=12,
        readonly_lane="codex-readonly",
        stable_lane="codex-stable",
    )
    service = MeshRunnerService(cfg, execute_manifest_fn=lambda root, manifest: {"success": True, "tasks": []})
    app = create_app(cfg, service=service)

    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/v1/runs",
            json={"run_id": "issue-mesh-20260327-999", "wait_for_completion": True},
            headers={"Authorization": "Bearer runner-secret"},
        )
        assert response.status_code == 400, response.text
        assert response.json()["detail"] == "READONLY_PROVIDER_ALLOWLIST_REQUIRED"


def test_mesh_runner_executes_readonly_run_and_returns_bundle(tmp_path, monkeypatch):
    last_message = tmp_path / "worker.txt"
    last_message.write_text(
        """```json
{
  "issue_key": "truth-lineage",
  "title": "truth-lineage gap",
  "risk_level": "P1",
  "issue_status": "still_alive",
  "handling_path": "manual_verify",
  "recommended_action": "backfill evidence",
  "evidence_refs": ["docs/core/22_全量功能进度总表_v7_精审.md"],
  "ssot_refs": ["04", "05"]
}
```""",
        encoding="utf-8",
    )

    task = codex_mesh.MeshTaskManifest(
        task_id="truth-lineage",
        goal="truth-lineage",
        prompt="prompt",
        read_scope=["docs/core/22_全量功能进度总表_v7_精审.md"],
        timeout_seconds=600,
        working_root=str(tmp_path.resolve()),
        output_mode="json",
    )
    monkeypatch.setattr(
        "automation.mesh_runner.runner.build_readonly_manifest",
        lambda **kwargs: codex_mesh.MeshRunManifest(tasks=[task], max_workers=kwargs["max_workers"]),
    )

    def _fake_execute(root: Path, manifest: object) -> dict:
        del root, manifest
        return {
            "execution_mode": "mesh",
            "success": True,
            "task_count": 1,
            "tasks": [
                {
                    "task_id": "truth-lineage",
                    "goal": "truth-lineage",
                    "success": True,
                    "selected_provider": "newapi-192.168.232.141-3000",
                    "attempts": [{"last_message_path": str(last_message)}],
                    "started_at": "2026-03-27T00:00:00+00:00",
                    "finished_at": "2026-03-27T00:01:00+00:00",
                    "error": None,
                }
            ],
            "started_at": "2026-03-27T00:00:00+00:00",
            "finished_at": "2026-03-27T00:01:00+00:00",
        }

    cfg = MeshRunnerConfig(
        repo_root=tmp_path.resolve(),
        runtime_root=(tmp_path / "runtime" / "issue_mesh").resolve(),
        auth_token="runner-secret",
        canonical_provider="newapi-192.168.232.141-3000",
        readonly_max_workers=12,
        readonly_lane="codex-readonly",
        stable_lane="codex-stable",
    )
    service = MeshRunnerService(cfg, execute_manifest_fn=_fake_execute)
    app = create_app(cfg, service=service)

    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/v1/runs",
            json={
                "run_id": "issue-mesh-20260327-001",
                "run_label": "test-run",
                "wait_for_completion": True,
                "max_workers": 9,
                "provider_allowlist": [
                    "newapi-192.168.232.141-3000-ro-a",
                    "newapi-192.168.232.141-3000-ro-b",
                    "newapi-192.168.232.141-3000-ro-c",
                    "newapi-192.168.232.141-3000-ro-d",
                ],
                "audit_scope": "current-layer",
                "shard_strategy": "family-view-ssot",
                "control_state_snapshot": "Recovery-Rearm",
            },
            headers={"Authorization": "Bearer runner-secret"},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["status"] == "completed"
        assert body["run_id"] == "issue-mesh-20260327-001"
        assert "Issue Mesh Summary" in body["summary_markdown"]

        task_spec_path = tmp_path / "runtime" / "issue_mesh" / body["run_id"] / "shard_01" / "task_spec.json"
        result_path = tmp_path / "runtime" / "issue_mesh" / body["run_id"] / "shard_01" / "result.json"
        bundle_path = tmp_path / "runtime" / "issue_mesh" / body["run_id"] / "bundle.json"
        legacy_bundle_path = tmp_path / "runtime" / "issue_mesh" / body["run_id"] / "findings_bundle.json"

        assert task_spec_path.exists()
        assert result_path.exists()
        assert bundle_path.exists()
        assert legacy_bundle_path.exists()

        task_spec = json.loads(task_spec_path.read_text(encoding="utf-8"))
        result = json.loads(result_path.read_text(encoding="utf-8"))
        assert task_spec == {
            "run_id": body["run_id"],
            "shard_id": "shard_01",
            "family_id": "truth-lineage",
            "role": "数据工程师",
            "read_scope": ["docs/core/22_全量功能进度总表_v7_精审.md"],
            "write_scope": [],
            "prompt_template_id": "family_01_truth_lineage_v1",
            "timeout_seconds": 600,
            "output_path": f"runtime/issue_mesh/{body['run_id']}/shard_01/result.json",
        }
        assert result == {
            "run_id": body["run_id"],
            "shard_id": "shard_01",
            "status": "COMPLETED",
            "findings": [
                {
                    "issue_key": "truth-lineage",
                    "title": "truth-lineage gap",
                    "risk_level": "P1",
                    "issue_status": "still_alive",
                    "handling_path": "manual_verify",
                    "recommended_action": "backfill evidence",
                    "evidence_refs": ["docs/core/22_全量功能进度总表_v7_精审.md"],
                    "ssot_refs": ["04", "05"],
                }
            ],
            "selected_provider": "newapi-192.168.232.141-3000",
            "shadow_path": f"docs/_temp/issue_mesh_shadow/{body['run_id']}/shard_01.md",
            "error": None,
            "started_at": "2026-03-27T00:00:00+00:00",
            "finished_at": "2026-03-27T00:01:00+00:00",
        }

        bundle_response = client.get(
            f"/v1/runs/{body['run_id']}/bundle",
            headers={"Authorization": "Bearer runner-secret"},
        )
        assert bundle_response.status_code == 200, bundle_response.text
        bundle = bundle_response.json()["bundle"]
        assert bundle["finding_count"] == 1
        finding = bundle["findings"][0]
        assert finding["issue_key"] == "truth-lineage"
        assert finding["title"] == "truth-lineage gap"
        assert finding["risk_level"] == "P1"
        assert finding["issue_status"] == "still_alive"
        assert finding["handling_path"] == "manual_verify"
        assert finding["recommended_action"] == "backfill evidence"
        assert finding["ssot_refs"] == ["04", "05"]
        assert finding["source_task_id"] == "truth-lineage"
        assert finding["source_run_id"] == body["run_id"]
        assert str(last_message) in finding["evidence_refs"]


def test_mesh_runner_autogenerates_canonical_run_id_sequence(tmp_path, monkeypatch):
    task = codex_mesh.MeshTaskManifest(
        task_id="truth-lineage",
        goal="truth-lineage",
        prompt="prompt",
        read_scope=["docs/core/22_全量功能进度总表_v7_精审.md"],
        timeout_seconds=600,
        working_root=str(tmp_path.resolve()),
        output_mode="json",
    )
    monkeypatch.setattr(
        "automation.mesh_runner.runner.build_readonly_manifest",
        lambda **kwargs: codex_mesh.MeshRunManifest(tasks=[task], max_workers=kwargs["max_workers"]),
    )
    monkeypatch.setattr(
        "automation.mesh_runner.runner.datetime",
        type(
            "FixedDateTime",
            (),
            {
                "now": staticmethod(lambda tz=None: __import__("datetime").datetime(2026, 3, 27, 12, 0, tzinfo=tz)),
            },
        ),
    )

    def _fake_execute(root: Path, manifest: object) -> dict:
        del root, manifest
        return {
            "execution_mode": "mesh",
            "success": True,
            "task_count": 1,
            "tasks": [
                {
                    "task_id": "truth-lineage",
                    "goal": "truth-lineage",
                    "success": True,
                    "selected_provider": "newapi-192.168.232.141-3000",
                    "attempts": [],
                }
            ],
        }

    cfg = MeshRunnerConfig(
        repo_root=tmp_path.resolve(),
        runtime_root=(tmp_path / "runtime" / "issue_mesh").resolve(),
        auth_token="runner-secret",
        canonical_provider="newapi-192.168.232.141-3000",
        readonly_max_workers=12,
        readonly_lane="codex-readonly",
        stable_lane="codex-stable",
    )
    service = MeshRunnerService(cfg, execute_manifest_fn=_fake_execute)
    app = create_app(cfg, service=service)
    existing_run = tmp_path / "runtime" / "issue_mesh" / "issue-mesh-20260327-001"
    existing_run.mkdir(parents=True, exist_ok=True)

    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/v1/runs",
            json={
                "wait_for_completion": True,
                "provider_allowlist": [
                    "newapi-192.168.232.141-3000-ro-a",
                    "newapi-192.168.232.141-3000-ro-b",
                ],
            },
            headers={"Authorization": "Bearer runner-secret"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["run_id"] == "issue-mesh-20260327-002"


def test_mesh_runner_downgrades_non_actionable_failed_task_output(tmp_path, monkeypatch):
    last_message = tmp_path / "empty_last_message.txt"
    last_message.write_text("", encoding="utf-8")
    stdout_path = tmp_path / "attempt_stdout.jsonl"
    stdout_path.write_text('{"type":"error","message":"provider failed"}\n', encoding="utf-8")
    stderr_path = tmp_path / "attempt_stderr.log"
    stderr_path.write_text("401 unauthorized\n", encoding="utf-8")

    task = codex_mesh.MeshTaskManifest(
        task_id="internal-contracts",
        goal="internal-contracts",
        prompt="prompt",
        read_scope=["docs/core/05_API与数据契约.md"],
        timeout_seconds=600,
        working_root=str(tmp_path.resolve()),
        output_mode="json",
    )
    monkeypatch.setattr(
        "automation.mesh_runner.runner.build_readonly_manifest",
        lambda **kwargs: codex_mesh.MeshRunManifest(tasks=[task], max_workers=kwargs["max_workers"]),
    )

    def _fake_execute(root: Path, manifest: object) -> dict:
        del root, manifest
        return {
            "execution_mode": "mesh",
            "success": False,
            "task_count": 1,
            "tasks": [
                {
                    "task_id": "internal-contracts",
                    "goal": "internal-contracts",
                    "success": False,
                    "selected_provider": None,
                    "attempts": [
                        {
                            "ok": False,
                            "status": "failed",
                            "last_message_path": str(last_message),
                            "stdout_path": str(stdout_path),
                            "stderr_path": str(stderr_path),
                        }
                    ],
                }
            ],
        }

    cfg = MeshRunnerConfig(
        repo_root=tmp_path.resolve(),
        runtime_root=(tmp_path / "runtime" / "issue_mesh").resolve(),
        auth_token="runner-secret",
        canonical_provider="newapi-192.168.232.141-3000",
        readonly_max_workers=12,
        readonly_lane="codex-readonly",
        stable_lane="codex-stable",
    )
    service = MeshRunnerService(cfg, execute_manifest_fn=_fake_execute)
    app = create_app(cfg, service=service)

    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/v1/runs",
            json={
                "wait_for_completion": True,
                "provider_allowlist": [
                    "newapi-192.168.232.141-3000-ro-a",
                ],
            },
            headers={"Authorization": "Bearer runner-secret"},
        )
        assert response.status_code == 200, response.text
        run_id = response.json()["run_id"]

        bundle_response = client.get(
            f"/v1/runs/{run_id}/bundle",
            headers={"Authorization": "Bearer runner-secret"},
        )
        assert bundle_response.status_code == 200, bundle_response.text
        finding = bundle_response.json()["bundle"]["findings"][0]
        assert finding["issue_key"] == "internal-contracts"
        assert finding["issue_status"] == "narrow_required"
        assert finding["handling_path"] == "execution_and_monitoring"
        assert "non-actionable" in finding["recommended_action"].lower()
        assert str(last_message) in finding["evidence_refs"]
        assert str(stdout_path) in finding["evidence_refs"]
        assert str(stderr_path) in finding["evidence_refs"]


def test_mesh_runner_resumes_incomplete_runs_on_service_start(tmp_path):
    runtime_root = (tmp_path / "runtime" / "issue_mesh").resolve()
    stale_root = runtime_root / "issue-mesh-20260327-009"
    stale_root.mkdir(parents=True, exist_ok=True)

    task = codex_mesh.MeshTaskManifest(
        task_id="truth-lineage",
        goal="truth-lineage",
        prompt="prompt",
        read_scope=["docs/core/22_全量功能进度总表_v7_精审.md"],
        timeout_seconds=600,
        working_root=str(tmp_path.resolve()),
        output_mode="json",
    )
    manifest = codex_mesh.MeshRunManifest(tasks=[task], max_workers=12)
    (stale_root / "manifest.json").write_text(
        json.dumps(asdict(manifest), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    (stale_root / "status.json").write_text(
        json.dumps(
            {
                "run_id": "issue-mesh-20260327-009",
                "run_label": "stale-run",
                "status": "running",
                "manifest_path": str(stale_root / "manifest.json"),
                "summary_path": None,
                "bundle_path": None,
                "output_dir": str(stale_root),
                "audit_context_path": str(stale_root / "audit_context.json"),
                "created_at": "2026-03-27T00:00:00+00:00",
                "started_at": "2026-03-27T00:01:00+00:00",
                "finished_at": None,
                "summary_markdown": None,
                "task_specs": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    cfg = MeshRunnerConfig(
        repo_root=tmp_path.resolve(),
        runtime_root=runtime_root,
        auth_token="runner-secret",
        canonical_provider="newapi-192.168.232.141-3000",
        readonly_max_workers=12,
    )

    def _fake_execute(root: Path, manifest: object) -> dict:
        del root, manifest
        return {
            "execution_mode": "mesh",
            "success": True,
            "task_count": 1,
            "tasks": [
                {
                    "task_id": "truth-lineage",
                    "goal": "truth-lineage",
                    "success": True,
                    "selected_provider": "newapi-192.168.232.141-3000",
                    "attempts": [],
                    "started_at": "2026-03-27T00:03:00+00:00",
                    "finished_at": "2026-03-27T00:04:00+00:00",
                    "error": None,
                }
            ],
            "started_at": "2026-03-27T00:03:00+00:00",
            "finished_at": "2026-03-27T00:04:00+00:00",
        }

    service = MeshRunnerService(cfg, execute_manifest_fn=_fake_execute)
    resumed_status = service.get_run(
        "issue-mesh-20260327-009",
        wait_for_completion=True,
        wait_timeout_seconds=5,
    )
    resumed_result = json.loads((stale_root / "shard_01" / "result.json").read_text(encoding="utf-8"))

    assert resumed_status["status"] == "completed"
    assert resumed_status["error"] is None
    assert resumed_status["summary_path"]
    assert resumed_status["bundle_path"]
    assert resumed_result["status"] == "COMPLETED"
    assert resumed_result["error"] is None


def test_mesh_runner_fail_closes_incomplete_runs_when_manifest_cannot_be_reloaded(tmp_path):
    runtime_root = (tmp_path / "runtime" / "issue_mesh").resolve()
    stale_root = runtime_root / "issue-mesh-20260327-010"
    running_shard_root = stale_root / "shard_01"
    completed_shard_root = stale_root / "shard_02"
    running_shard_root.mkdir(parents=True, exist_ok=True)
    completed_shard_root.mkdir(parents=True, exist_ok=True)

    (stale_root / "status.json").write_text(
        json.dumps(
            {
                "run_id": "issue-mesh-20260327-010",
                "run_label": "stale-run",
                "status": "running",
                "manifest_path": str(stale_root / "manifest.json"),
                "summary_path": None,
                "bundle_path": None,
                "output_dir": str(stale_root),
                "audit_context_path": str(stale_root / "audit_context.json"),
                "created_at": "2026-03-27T00:00:00+00:00",
                "started_at": "2026-03-27T00:01:00+00:00",
                "finished_at": None,
                "summary_markdown": None,
                "task_specs": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (running_shard_root / "result.json").write_text(
        json.dumps(
            {
                "run_id": "issue-mesh-20260327-010",
                "shard_id": "shard_01",
                "status": "RUNNING",
                "findings": [],
                "shadow_path": "docs/_temp/issue_mesh_shadow/issue-mesh-20260327-010/shard_01.md",
                "error": None,
                "started_at": "2026-03-27T00:01:00+00:00",
                "finished_at": None,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (completed_shard_root / "result.json").write_text(
        json.dumps(
            {
                "run_id": "issue-mesh-20260327-010",
                "shard_id": "shard_02",
                "status": "COMPLETED",
                "findings": [{"issue_key": "kept"}],
                "shadow_path": "docs/_temp/issue_mesh_shadow/issue-mesh-20260327-010/shard_02.md",
                "error": None,
                "started_at": "2026-03-27T00:01:00+00:00",
                "finished_at": "2026-03-27T00:02:00+00:00",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    cfg = MeshRunnerConfig(
        repo_root=tmp_path.resolve(),
        runtime_root=runtime_root,
        auth_token="runner-secret",
        canonical_provider="newapi-192.168.232.141-3000",
        readonly_max_workers=12,
    )

    MeshRunnerService(cfg, execute_manifest_fn=lambda root, manifest: {"success": True, "tasks": []})

    reclaimed_status = json.loads((stale_root / "status.json").read_text(encoding="utf-8"))
    reclaimed_running_shard = json.loads((running_shard_root / "result.json").read_text(encoding="utf-8"))
    completed_shard = json.loads((completed_shard_root / "result.json").read_text(encoding="utf-8"))

    assert reclaimed_status["status"] == "failed"
    assert reclaimed_status["error"] == STALE_RUN_RECLAIM_ERROR
    assert reclaimed_status["finished_at"]

    assert reclaimed_running_shard["status"] == "FAILED"
    assert reclaimed_running_shard["error"] == STALE_RUN_RECLAIM_ERROR
    assert reclaimed_running_shard["finished_at"] == reclaimed_status["finished_at"]

    assert completed_shard["status"] == "COMPLETED"
    assert completed_shard["error"] is None
    assert completed_shard["finished_at"] == "2026-03-27T00:02:00+00:00"
