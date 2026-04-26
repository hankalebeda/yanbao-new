from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from automation.writeback_service.app import DEFAULT_DENY_22, ServiceConfig, _sha256_text, create_app


def _make_client(tmp_path: Path) -> TestClient:
    cfg = ServiceConfig(
        repo_root=tmp_path.resolve(),
        audit_dir=(tmp_path / ".writeback_audit").resolve(),
        allow_prefixes=("LiteLLM/", "docs/_temp/"),
        deny_prefixes=("app/", "tests/"),
        deny_paths=(DEFAULT_DENY_22,),
        triage_dir=(tmp_path / "runtime" / "issue_mesh" / "promote_prep" / "triage").resolve(),
        auth_token="writeback-secret",
        require_triage=True,
        lock_timeout_seconds=2.0,
    )
    app = create_app(cfg)
    return TestClient(app, base_url="http://localhost")


def _seed_triage_record(
    tmp_path: Path,
    *,
    triage_record_id: str,
    target_path: str,
    base_sha256: str,
    patch_text: str,
    decision: str = "allow",
) -> None:
    triage_dir = tmp_path / "runtime" / "issue_mesh" / "promote_prep" / "triage"
    triage_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "triage_record_id": triage_record_id,
        "decision": decision,
        "auto_commit": decision == "allow",
        "target_path": target_path,
        "relative_target_path": target_path,
        "base_sha256": base_sha256,
        "patch_hash": _sha256_text(patch_text),
    }
    (triage_dir / f"{triage_record_id}.json").write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, indent=2),
        encoding="utf-8",
    )


def _write_promote_target_mode(tmp_path: Path, mode: str = "doc22") -> None:
    state_path = tmp_path / "automation" / "control_plane" / "current_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps({"_schema": "infra_promote_v1", "promote_target_mode": mode}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _post_json(client: TestClient, path: str, payload: dict) -> dict:
    response = client.post(path, json=payload, headers={"Authorization": "Bearer writeback-secret"})
    assert response.status_code == 200, response.text
    return response.json()


def test_writeback_service_requires_bearer_token(tmp_path: Path):
    target = tmp_path / "LiteLLM" / "auth.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("content\n", encoding="utf-8")

    with _make_client(tmp_path) as client:
        response = client.post("/v1/read", json={"target_path": "LiteLLM/auth.md"})
        assert response.status_code == 401
        assert response.json()["detail"] == "UNAUTHORIZED"


def test_writeback_service_read_commit_and_rollback(tmp_path: Path):
    target = tmp_path / "LiteLLM" / "plan.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("old line\n", encoding="utf-8")
    triage_record_id = "issue-mesh-20260327-401__report-writeback-writeback"

    with _make_client(tmp_path) as client:
        read_payload = _post_json(client, "/v1/read", {"target_path": "LiteLLM/plan.md"})
        assert read_payload["content"] == "old line\n"

        preview_payload = _post_json(
            client,
            "/v1/preview",
            {
                "target_path": "LiteLLM/plan.md",
                "base_sha256": read_payload["sha256"],
                "patch_text": "new line\n",
            },
        )
        assert preview_payload["conflict"] is False
        assert preview_payload["diff_summary"]["changed"] is True
        _seed_triage_record(
            tmp_path,
            triage_record_id=triage_record_id,
            target_path="LiteLLM/plan.md",
            base_sha256=read_payload["sha256"],
            patch_text="new line\n",
        )

        commit_payload = _post_json(
            client,
            "/v1/commit",
            {
                "target_path": "LiteLLM/plan.md",
                "base_sha256": read_payload["sha256"],
                "patch_text": "new line\n",
                "idempotency_key": "issue-mesh:issue-mesh-20260327-401:status-note:LiteLLM-plan-md",
                "actor": {"type": "workflow", "id": "kestra"},
                "request_id": "req-issue-mesh-20260327-401",
                "run_id": "issue-mesh-20260327-401",
                "triage_record_id": triage_record_id,
            },
        )
        assert commit_payload["status"] == "committed"

        detail_response = client.get(
            f"/v1/commits/{commit_payload['commit_id']}",
            headers={"Authorization": "Bearer writeback-secret"},
        )
        assert detail_response.status_code == 200
        assert detail_response.json()["operation"] == "commit"

        rollback_payload = _post_json(
            client,
            "/v1/rollback",
            {
                "commit_id": commit_payload["commit_id"],
                "idempotency_key": "issue-mesh:issue-mesh-20260327-401:status-note:LiteLLM-plan-md:rollback",
                "actor": {"type": "workflow", "id": "kestra"},
                "request_id": "req-issue-mesh-20260327-401:rollback",
                "run_id": "issue-mesh-20260327-401",
            },
        )
        assert rollback_payload["status"] == "rolled_back"


def test_writeback_service_commit_requires_preview_record(tmp_path: Path):
    target = tmp_path / "LiteLLM" / "plan.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("old line\n", encoding="utf-8")

    with _make_client(tmp_path) as client:
        response = client.post(
            "/v1/commit",
            json={
                "target_path": "LiteLLM/plan.md",
                "base_sha256": "missing-preview",
                "patch_text": "new line\n",
                "idempotency_key": "issue-mesh:issue-mesh-20260327-402:status-note:LiteLLM-plan-md",
                "actor": {"type": "workflow", "id": "kestra"},
                "request_id": "req-issue-mesh-20260327-402",
                "run_id": "issue-mesh-20260327-402",
            },
            headers={"Authorization": "Bearer writeback-secret"},
        )

    assert response.status_code == 409
    assert response.json()["detail"] == "PREVIEW_REQUIRED"


def test_writeback_service_commit_requires_triage_record_when_enabled(tmp_path: Path):
    target = tmp_path / "LiteLLM" / "plan.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("old line\n", encoding="utf-8")

    with _make_client(tmp_path) as client:
        read_payload = _post_json(client, "/v1/read", {"target_path": "LiteLLM/plan.md"})
        preview_payload = _post_json(
            client,
            "/v1/preview",
            {
                "target_path": "LiteLLM/plan.md",
                "base_sha256": read_payload["sha256"],
                "patch_text": "new line\n",
            },
        )
        assert preview_payload["conflict"] is False
        response = client.post(
            "/v1/commit",
            json={
                "target_path": "LiteLLM/plan.md",
                "base_sha256": read_payload["sha256"],
                "patch_text": "new line\n",
                "idempotency_key": "issue-mesh:issue-mesh-20260327-402:status-note:LiteLLM-plan-md",
                "actor": {"type": "workflow", "id": "kestra"},
                "request_id": "req-issue-mesh-20260327-402",
                "run_id": "issue-mesh-20260327-402",
            },
            headers={"Authorization": "Bearer writeback-secret"},
        )

    assert response.status_code == 409
    assert response.json()["detail"] == "TRIAGE_REQUIRED"


def test_writeback_service_blocks_forbidden_targets(tmp_path: Path):
    """Missing control plane state must fail closed to infra mode."""
    forbidden_22 = tmp_path / DEFAULT_DENY_22
    forbidden_22.parent.mkdir(parents=True, exist_ok=True)
    forbidden_22.write_text("x\n", encoding="utf-8")

    with _make_client(tmp_path) as client:
        response = client.post(
            "/v1/read",
            json={"target_path": DEFAULT_DENY_22},
            headers={"Authorization": "Bearer writeback-secret"},
        )
        assert response.status_code == 403
        assert response.json()["detail"] == "DOC22_WRITEBACK_BLOCKED_BY_INFRA_MODE"


def test_writeback_service_can_be_scoped_to_exact_progress_doc(tmp_path: Path):
    target = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("progress\n", encoding="utf-8")
    _write_promote_target_mode(tmp_path, "doc22")
    cfg = ServiceConfig(
        repo_root=tmp_path.resolve(),
        audit_dir=(tmp_path / ".writeback_b_audit").resolve(),
        allow_prefixes=("docs/core/22_全量功能进度总表_v7_精审.md",),
        deny_prefixes=("app/", "tests/", "runtime/", "docs/_temp/", "LiteLLM/"),
        deny_paths=(
            "output/junit.xml",
            "app/governance/catalog_snapshot.json",
            "output/blind_spot_audit.json",
            "github/automation/continuous_audit/latest_run.json",
        ),
        auth_token="writeback-b-secret",
        lock_timeout_seconds=2.0,
    )
    app = create_app(cfg)

    with TestClient(app, base_url="http://localhost") as client:
        ok = client.post(
            "/v1/read",
            json={"target_path": "docs/core/22_全量功能进度总表_v7_精审.md"},
            headers={"Authorization": "Bearer writeback-b-secret"},
        )
        denied = client.post(
            "/v1/read",
            json={"target_path": "docs/_temp/issue_mesh_shadow/example.md"},
            headers={"Authorization": "Bearer writeback-b-secret"},
        )
        assert ok.status_code == 200
        assert denied.status_code == 403
