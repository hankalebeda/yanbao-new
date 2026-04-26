from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from automation.writeback_service.app import (
    DEFAULT_DENY_22,
    DEFAULT_SHARED_ARTIFACT_DENY_PATHS,
    ServiceConfig,
    _atomic_write_json,
    _audit_commit_path,
    _idempotency_path,
    _sha256_text,
    create_app,
)


def _write_promote_target_mode(tmp_path: Path, mode: str = "doc22") -> None:
    state_path = tmp_path / "automation" / "control_plane" / "current_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps({"_schema": "infra_promote_v1", "promote_target_mode": mode}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _read_audit_events(audit_dir: Path) -> list[dict]:
    audit_path = audit_dir / "audit_log.jsonl"
    if not audit_path.exists():
        return []
    return [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_writeback_service_from_env_defaults_to_writeback_a_scope(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("WRITEBACK_REPO_ROOT", str(tmp_path))
    monkeypatch.delenv("WRITEBACK_ALLOW_PREFIXES", raising=False)
    monkeypatch.delenv("WRITEBACK_DENY_PREFIXES", raising=False)
    monkeypatch.delenv("WRITEBACK_DENY_PATHS", raising=False)
    monkeypatch.setenv("WRITEBACK_AUTH_TOKEN", "writeback-a-token")

    cfg = ServiceConfig.from_env()

    assert cfg.repo_root == tmp_path.resolve()
    assert cfg.allow_prefixes == ("LiteLLM/", "docs/_temp/")
    assert cfg.deny_prefixes == ("runtime/",)
    assert cfg.deny_paths == (DEFAULT_DENY_22, *DEFAULT_SHARED_ARTIFACT_DENY_PATHS)
    assert cfg.auth_token == "writeback-a-token"
    assert cfg.require_triage is True


def test_writeback_service_from_env_can_scope_writeback_b(monkeypatch, tmp_path: Path):
    progress_doc = tmp_path / "docs" / "core" / "22_全量功能进度总表_v7_精审.md"
    progress_doc.parent.mkdir(parents=True, exist_ok=True)
    progress_doc.write_text("progress\n", encoding="utf-8")
    _write_promote_target_mode(tmp_path, "doc22")
    blocked = tmp_path / "docs" / "_temp" / "issue_mesh_shadow" / "sample.md"
    blocked.parent.mkdir(parents=True, exist_ok=True)
    blocked.write_text("shadow\n", encoding="utf-8")

    monkeypatch.setenv("WRITEBACK_REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("WRITEBACK_AUTH_TOKEN", "writeback-b-token")
    monkeypatch.setenv("WRITEBACK_ALLOW_PREFIXES", DEFAULT_DENY_22)
    monkeypatch.setenv("WRITEBACK_DENY_PREFIXES", "app/,tests/,runtime/,docs/_temp/,LiteLLM/")
    monkeypatch.setenv(
        "WRITEBACK_DENY_PATHS",
        "output/junit.xml,app/governance/catalog_snapshot.json,output/blind_spot_audit.json,github/automation/continuous_audit/latest_run.json",
    )

    app = create_app(ServiceConfig.from_env())

    with TestClient(app, base_url="http://localhost") as client:
        ok = client.post(
            "/v1/read",
            json={"target_path": DEFAULT_DENY_22},
            headers={"Authorization": "Bearer writeback-b-token"},
        )
        denied = client.post(
            "/v1/read",
            json={"target_path": "docs/_temp/issue_mesh_shadow/sample.md"},
            headers={"Authorization": "Bearer writeback-b-token"},
        )

    assert ok.status_code == 200
    assert denied.status_code == 403
    assert denied.json()["detail"] == "TARGET_PATH_FORBIDDEN"


def test_writeback_service_from_env_blocks_writeback_b_without_control_plane_state(monkeypatch, tmp_path: Path):
    """Missing control plane state must fail closed to infra mode."""
    progress_doc = tmp_path / DEFAULT_DENY_22
    progress_doc.parent.mkdir(parents=True, exist_ok=True)
    progress_doc.write_text("progress\n", encoding="utf-8")

    monkeypatch.setenv("WRITEBACK_REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("WRITEBACK_AUTH_TOKEN", "writeback-b-token")
    monkeypatch.setenv("WRITEBACK_ALLOW_PREFIXES", DEFAULT_DENY_22)
    monkeypatch.setenv("WRITEBACK_DENY_PREFIXES", "app/,tests/,runtime/,docs/_temp/,LiteLLM/")
    monkeypatch.setenv(
        "WRITEBACK_DENY_PATHS",
        "output/junit.xml,app/governance/catalog_snapshot.json,output/blind_spot_audit.json,github/automation/continuous_audit/latest_run.json",
    )

    cfg = ServiceConfig.from_env()
    app = create_app(cfg)

    with TestClient(app, base_url="http://localhost") as client:
        resp = client.post(
            "/v1/read",
            json={"target_path": DEFAULT_DENY_22},
            headers={"Authorization": "Bearer writeback-b-token"},
        )

    assert resp.status_code == 403
    assert resp.json()["detail"] == "DOC22_WRITEBACK_BLOCKED_BY_INFRA_MODE"
    events = _read_audit_events(cfg.audit_dir)
    assert events[-1]["reason"] == "DOC22_WRITEBACK_BLOCKED_BY_INFRA_MODE"
    assert events[-1]["control_plane_state_reason"] == "CONTROL_PLANE_STATE_MISSING_DEFAULT_INFRA"


def test_writeback_service_from_env_shared_artifacts_remain_forbidden_even_if_allowlisted(monkeypatch, tmp_path: Path):
    shared_artifact = tmp_path / "output" / "junit.xml"
    shared_artifact.parent.mkdir(parents=True, exist_ok=True)
    shared_artifact.write_text("<testsuite tests=\"1\" failures=\"0\" errors=\"0\" />\n", encoding="utf-8")

    monkeypatch.setenv("WRITEBACK_REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("WRITEBACK_AUTH_TOKEN", "writeback-artifact-token")
    monkeypatch.setenv("WRITEBACK_ALLOW_PREFIXES", "output/")
    monkeypatch.setenv("WRITEBACK_DENY_PREFIXES", "")
    monkeypatch.setenv("WRITEBACK_DENY_PATHS", "")

    cfg = ServiceConfig.from_env()
    app = create_app(cfg)

    with TestClient(app, base_url="http://localhost") as client:
        denied = client.post(
            "/v1/read",
            json={"target_path": "output/junit.xml"},
            headers={"Authorization": "Bearer writeback-artifact-token"},
        )

    assert denied.status_code == 403
    assert denied.json()["detail"] == "TARGET_PATH_FORBIDDEN"
    events = _read_audit_events(cfg.audit_dir)
    assert events[-1]["reason"] == "SHARED_ARTIFACT_WRITEBACK_FORBIDDEN"


def test_writeback_service_can_write_app_path_when_explicitly_allowlisted(tmp_path: Path):
    target = tmp_path / "app" / "services" / "probe.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("before\n", encoding="utf-8")
    cfg = ServiceConfig(
        repo_root=tmp_path.resolve(),
        audit_dir=(tmp_path / ".writeback_app_audit").resolve(),
        allow_prefixes=("app/",),
        deny_prefixes=(),
        deny_paths=(DEFAULT_DENY_22, *DEFAULT_SHARED_ARTIFACT_DENY_PATHS),
        auth_token="writeback-app-token",
        lock_timeout_seconds=2.0,
    )
    app = create_app(cfg)
    headers = {"Authorization": "Bearer writeback-app-token"}

    with TestClient(app, base_url="http://localhost") as client:
        read_payload = client.post("/v1/read", json={"target_path": "app/services/probe.py"}, headers=headers)
        assert read_payload.status_code == 200, read_payload.text
        base_sha = read_payload.json()["sha256"]

        preview = client.post(
            "/v1/preview",
            json={
                "target_path": "app/services/probe.py",
                "base_sha256": base_sha,
                "patch_text": "after\n",
            },
            headers=headers,
        )
        assert preview.status_code == 200, preview.text

        commit = client.post(
            "/v1/commit",
            json={
                "target_path": "app/services/probe.py",
                "base_sha256": base_sha,
                "patch_text": "after\n",
                "idempotency_key": "writeback-app-allowlist-001",
                "actor": {"type": "workflow", "id": "kestra-writeback"},
                "request_id": "req-writeback-app-allowlist-001",
                "run_id": "writeback-app-allowlist-001",
            },
            headers=headers,
        )
        assert commit.status_code == 200, commit.text
        assert commit.json()["status"] == "committed"

    assert target.read_text(encoding="utf-8") == "after\n"


def test_writeback_service_commit_replay_is_idempotent(tmp_path: Path):
    target = tmp_path / "docs" / "_temp" / "sample.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("before\n", encoding="utf-8")
    cfg = ServiceConfig(
        repo_root=tmp_path.resolve(),
        audit_dir=(tmp_path / ".writeback_audit").resolve(),
        allow_prefixes=("docs/_temp/",),
        deny_prefixes=("app/", "tests/"),
        deny_paths=(DEFAULT_DENY_22,),
        auth_token="writeback-secret",
        lock_timeout_seconds=2.0,
    )
    app = create_app(cfg)
    payload = {
        "target_path": "docs/_temp/sample.md",
        "base_sha256": _sha256_text(target.read_text(encoding="utf-8")),
        "patch_text": "after\n",
        "idempotency_key": "issue-mesh:issue-mesh-20260327-001:status-note:current-writeback-detail",
        "actor": {"type": "workflow", "id": "kestra-status-note"},
        "request_id": "req-issue-mesh-20260327-001-status-note-current-writeback-detail",
        "run_id": "issue-mesh-20260327-001",
    }
    headers = {"Authorization": "Bearer writeback-secret"}

    with TestClient(app, base_url="http://localhost") as client:
        preview = client.post(
            "/v1/preview",
            json={
                "target_path": "docs/_temp/sample.md",
                "base_sha256": payload["base_sha256"],
                "patch_text": payload["patch_text"],
            },
            headers=headers,
        )
        assert preview.status_code == 200, preview.text
        first = client.post("/v1/commit", json=payload, headers=headers)
        second = client.post("/v1/commit", json=payload, headers=headers)

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert first.json()["commit_id"] == second.json()["commit_id"]
    assert second.json()["idempotent_replay"] is True
    assert target.read_text(encoding="utf-8") == "after\n"


def test_writeback_service_rejects_base_sha_mismatch(tmp_path: Path):
    target = tmp_path / "docs" / "_temp" / "sample.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("before\n", encoding="utf-8")
    cfg = ServiceConfig(
        repo_root=tmp_path.resolve(),
        audit_dir=(tmp_path / ".writeback_audit").resolve(),
        allow_prefixes=("docs/_temp/",),
        deny_prefixes=("app/", "tests/"),
        deny_paths=(DEFAULT_DENY_22,),
        auth_token="writeback-secret",
        lock_timeout_seconds=2.0,
    )
    app = create_app(cfg)
    headers = {"Authorization": "Bearer writeback-secret"}

    with TestClient(app, base_url="http://localhost") as client:
        preview = client.post(
            "/v1/preview",
            json={
                "target_path": "docs/_temp/sample.md",
                "base_sha256": _sha256_text("stale\n"),
                "patch_text": "after\n",
            },
            headers=headers,
        )
        assert preview.status_code == 200, preview.text
        response = client.post(
            "/v1/commit",
            json={
                "target_path": "docs/_temp/sample.md",
                "base_sha256": _sha256_text("stale\n"),
                "patch_text": "after\n",
                "idempotency_key": "issue-mesh:issue-mesh-20260327-002:status-note:current-writeback-detail",
                "actor": {"type": "workflow", "id": "kestra-status-note"},
                "request_id": "req-issue-mesh-20260327-002-status-note-current-writeback-detail",
                "run_id": "issue-mesh-20260327-002",
            },
            headers=headers,
        )

    assert response.status_code == 409
    assert response.json()["detail"] == "BASE_SHA_MISMATCH"


def test_writeback_service_rejects_idempotency_conflict(tmp_path: Path):
    target = tmp_path / "docs" / "_temp" / "sample.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("before\n", encoding="utf-8")
    cfg = ServiceConfig(
        repo_root=tmp_path.resolve(),
        audit_dir=(tmp_path / ".writeback_audit").resolve(),
        allow_prefixes=("docs/_temp/",),
        deny_prefixes=("app/", "tests/"),
        deny_paths=(DEFAULT_DENY_22,),
        auth_token="writeback-secret",
        lock_timeout_seconds=2.0,
    )
    app = create_app(cfg)
    headers = {"Authorization": "Bearer writeback-secret"}
    first_payload = {
        "target_path": "docs/_temp/sample.md",
        "base_sha256": _sha256_text("before\n"),
        "patch_text": "after\n",
        "idempotency_key": "issue-mesh:issue-mesh-20260327-003:status-note:current-writeback-detail",
        "actor": {"type": "workflow", "id": "kestra-status-note"},
        "request_id": "req-issue-mesh-20260327-003-status-note-current-writeback-detail",
        "run_id": "issue-mesh-20260327-003",
    }
    second_payload = dict(first_payload)
    second_payload["patch_text"] = "after-again\n"

    with TestClient(app, base_url="http://localhost") as client:
        preview = client.post(
            "/v1/preview",
            json={
                "target_path": "docs/_temp/sample.md",
                "base_sha256": first_payload["base_sha256"],
                "patch_text": first_payload["patch_text"],
            },
            headers=headers,
        )
        assert preview.status_code == 200, preview.text
        first = client.post("/v1/commit", json=first_payload, headers=headers)
        second = client.post("/v1/commit", json=second_payload, headers=headers)

    assert first.status_code == 200, first.text
    assert second.status_code == 409
    assert second.json()["detail"] == "IDEMPOTENCY_CONFLICT"


def test_writeback_service_commit_replay_ignores_actor_and_request_id(tmp_path: Path):
    target = tmp_path / "docs" / "_temp" / "sample.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("before\n", encoding="utf-8")
    cfg = ServiceConfig(
        repo_root=tmp_path.resolve(),
        audit_dir=(tmp_path / ".writeback_audit").resolve(),
        allow_prefixes=("docs/_temp/",),
        deny_prefixes=("app/", "tests/"),
        deny_paths=(DEFAULT_DENY_22,),
        auth_token="writeback-secret",
        lock_timeout_seconds=2.0,
    )
    app = create_app(cfg)
    headers = {"Authorization": "Bearer writeback-secret"}
    first_payload = {
        "target_path": "docs/_temp/sample.md",
        "base_sha256": _sha256_text("before\n"),
        "patch_text": "after\n",
        "idempotency_key": "issue-mesh:issue-mesh-20260327-004:status-note:current-writeback-detail",
        "actor": {"type": "workflow", "id": "manual-approved-status-note"},
        "request_id": "req-issue-mesh-20260327-004-status-note-current-writeback-detail-manual",
        "run_id": "issue-mesh-20260327-004",
    }
    second_payload = {
        "target_path": "docs/_temp/sample.md",
        "base_sha256": _sha256_text("before\n"),
        "patch_text": "after\n",
        "idempotency_key": "issue-mesh:issue-mesh-20260327-004:status-note:current-writeback-detail",
        "actor": {"type": "workflow", "id": "kestra-status-note"},
        "request_id": "req-issue-mesh-20260327-004-status-note-current-writeback-detail-kestra",
        "run_id": "issue-mesh-20260327-004",
    }

    with TestClient(app, base_url="http://localhost") as client:
        preview = client.post(
            "/v1/preview",
            json={
                "target_path": "docs/_temp/sample.md",
                "base_sha256": first_payload["base_sha256"],
                "patch_text": first_payload["patch_text"],
            },
            headers=headers,
        )
        assert preview.status_code == 200, preview.text
        first = client.post("/v1/commit", json=first_payload, headers=headers)
        second = client.post("/v1/commit", json=second_payload, headers=headers)

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert second.json()["idempotent_replay"] is True
    assert first.json()["commit_id"] == second.json()["commit_id"]


def test_writeback_service_commit_replay_accepts_legacy_idempotency_fingerprint(tmp_path: Path):
    target = tmp_path / "docs" / "_temp" / "sample.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("before\n", encoding="utf-8")
    cfg = ServiceConfig(
        repo_root=tmp_path.resolve(),
        audit_dir=(tmp_path / ".writeback_audit").resolve(),
        allow_prefixes=("docs/_temp/",),
        deny_prefixes=("app/", "tests/"),
        deny_paths=(DEFAULT_DENY_22,),
        auth_token="writeback-secret",
        lock_timeout_seconds=2.0,
    )
    app = create_app(cfg)
    headers = {"Authorization": "Bearer writeback-secret"}
    rel_path = "docs/_temp/sample.md"
    idempotency_key = "issue-mesh:issue-mesh-20260327-005:status-note:current-writeback-detail"
    commit_id = "legacy-commit-001"
    patch_text = "after\n"

    _atomic_write_json(
        _audit_commit_path(cfg, commit_id),
        {
            "commit_id": commit_id,
            "operation": "commit",
            "target_path": str(target.resolve()),
            "relative_path": rel_path,
            "base_sha256": _sha256_text("before\n"),
            "new_sha256": _sha256_text(patch_text),
            "patch_hash": _sha256_text(patch_text),
            "idempotency_key": idempotency_key,
            "request_id": "req-issue-mesh-20260327-005-status-note-current-writeback-detail-manual",
            "run_id": "issue-mesh-20260327-005",
            "actor": {"type": "workflow", "id": "manual-approved-status-note"},
            "created_at": "2026-03-27T13:00:00+00:00",
            "rolled_back_at": None,
            "rollback_commit_id": None,
            "rollback_of": None,
            "previous_content_b64": "YmVmb3JlCg==",
        },
    )
    _atomic_write_json(
        _idempotency_path(cfg, idempotency_key),
        {
            "idempotency_key": idempotency_key,
            "operation": "commit",
            "fingerprint": "legacy-fingerprint-with-actor-and-request-id",
            "commit_id": commit_id,
            "saved_at": "2026-03-27T13:00:00+00:00",
        },
    )

    payload = {
        "target_path": rel_path,
        "base_sha256": _sha256_text("before\n"),
        "patch_text": patch_text,
        "idempotency_key": idempotency_key,
        "actor": {"type": "workflow", "id": "kestra-status-note"},
        "request_id": "req-issue-mesh-20260327-005-status-note-current-writeback-detail-kestra",
        "run_id": "issue-mesh-20260327-005",
    }

    with TestClient(app, base_url="http://localhost") as client:
        replay = client.post("/v1/commit", json=payload, headers=headers)

    assert replay.status_code == 200, replay.text
    assert replay.json()["idempotent_replay"] is True
    assert replay.json()["commit_id"] == commit_id


def test_writeback_service_requires_matching_preview_before_commit(tmp_path: Path):
    target = tmp_path / "docs" / "_temp" / "sample.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("before\n", encoding="utf-8")
    cfg = ServiceConfig(
        repo_root=tmp_path.resolve(),
        audit_dir=(tmp_path / ".writeback_audit").resolve(),
        allow_prefixes=("docs/_temp/",),
        deny_prefixes=("app/", "tests/"),
        deny_paths=(DEFAULT_DENY_22,),
        auth_token="writeback-secret",
        lock_timeout_seconds=2.0,
    )
    app = create_app(cfg)
    headers = {"Authorization": "Bearer writeback-secret"}

    with TestClient(app, base_url="http://localhost") as client:
        response = client.post(
            "/v1/commit",
            json={
                "target_path": "docs/_temp/sample.md",
                "base_sha256": _sha256_text("before\n"),
                "patch_text": "after\n",
                "idempotency_key": "issue-mesh:issue-mesh-20260327-006:status-note:current-writeback-detail",
                "actor": {"type": "workflow", "id": "kestra-status-note"},
                "request_id": "req-issue-mesh-20260327-006-status-note-current-writeback-detail",
                "run_id": "issue-mesh-20260327-006",
            },
            headers=headers,
        )

    assert response.status_code == 409
    assert response.json()["detail"] == "PREVIEW_REQUIRED"


def test_writeback_service_skips_progress_doc_commit_when_run_id_already_present(tmp_path: Path):
    target = tmp_path / DEFAULT_DENY_22
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("header\nrun issue-mesh-20260327-301 already exists\n", encoding="utf-8")
    _write_promote_target_mode(tmp_path, "doc22")
    cfg = ServiceConfig(
        repo_root=tmp_path.resolve(),
        audit_dir=(tmp_path / ".writeback_b_audit").resolve(),
        allow_prefixes=(DEFAULT_DENY_22,),
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
    headers = {"Authorization": "Bearer writeback-b-secret"}
    patch_text = target.read_text(encoding="utf-8") + "new line\n"
    base_sha = _sha256_text(target.read_text(encoding="utf-8"))

    with TestClient(app, base_url="http://localhost") as client:
        preview = client.post(
            "/v1/preview",
            json={
                "target_path": DEFAULT_DENY_22,
                "base_sha256": base_sha,
                "patch_text": patch_text,
            },
            headers=headers,
        )
        assert preview.status_code == 200, preview.text
        commit = client.post(
            "/v1/commit",
            json={
                "target_path": DEFAULT_DENY_22,
                "base_sha256": base_sha,
                "patch_text": patch_text,
                "idempotency_key": "issue-mesh:issue-mesh-20260327-301:status-note:current-writeback-detail",
                "actor": {"type": "workflow", "id": "kestra-status-note"},
                "request_id": "req-issue-mesh-20260327-301",
                "run_id": "issue-mesh-20260327-301",
            },
            headers=headers,
        )

    assert commit.status_code == 200, commit.text
    assert commit.json()["status"] == "skipped"
    assert commit.json()["skip_reason"] == "RUN_ID_ALREADY_PRESENT"
    assert target.read_text(encoding="utf-8") == "header\nrun issue-mesh-20260327-301 already exists\n"


# ============================================================================
# Wave 3: Lease / fencing API  &  batch writeback tests
# ============================================================================


def _make_writeback_a_config(tmp_path: Path) -> ServiceConfig:
    """Create a ServiceConfig with writeback_a defaults for testing."""
    return ServiceConfig(
        repo_root=tmp_path.resolve(),
        audit_dir=(tmp_path / ".writeback_a_audit").resolve(),
        allow_prefixes=("LiteLLM/", "docs/_temp/"),
        deny_prefixes=("runtime/",),
        deny_paths=(DEFAULT_DENY_22, *DEFAULT_SHARED_ARTIFACT_DENY_PATHS),
        auth_token="wb-a-token",
        lock_timeout_seconds=2.0,
    )


def test_lease_claim_and_release_round_trip(tmp_path: Path):
    """POST /v1/lease/claim → POST /v1/lease/release round-trip."""
    cfg = _make_writeback_a_config(tmp_path)
    # Ensure writeback_coordination state dir exists
    (tmp_path / "runtime" / "writeback_coordination").mkdir(parents=True, exist_ok=True)
    app = create_app(cfg)
    headers = {"Authorization": "Bearer wb-a-token"}

    with TestClient(app, base_url="http://localhost") as client:
        claim_resp = client.post(
            "/v1/lease/claim",
            json={
                "round_id": "test-round-001",
                "target_paths": ["docs/_temp/test.md"],
                "lease_seconds": 120,
            },
            headers=headers,
        )
        assert claim_resp.status_code == 200, claim_resp.text
        claim_data = claim_resp.json()
        assert "lease_id" in claim_data
        assert "fencing_token" in claim_data
        assert isinstance(claim_data["fencing_token"], int)

        release_resp = client.post(
            "/v1/lease/release",
            json={
                "lease_id": claim_data["lease_id"],
                "reason": "test-complete",
            },
            headers=headers,
        )
        assert release_resp.status_code == 200, release_resp.text
        assert release_resp.json()["status"] == "released"


def test_lease_claim_requires_auth(tmp_path: Path):
    cfg = _make_writeback_a_config(tmp_path)
    (tmp_path / "runtime" / "writeback_coordination").mkdir(parents=True, exist_ok=True)
    app = create_app(cfg)
    with TestClient(app, base_url="http://localhost") as client:
        resp = client.post(
            "/v1/lease/claim",
            json={"round_id": "r1", "target_paths": ["a.py"]},
        )
        assert resp.status_code == 401


def test_batch_commit_with_valid_fencing(tmp_path: Path):
    """Batch commit with a valid lease+fencing_token should succeed."""
    cfg = _make_writeback_a_config(tmp_path)
    (tmp_path / "runtime" / "writeback_coordination").mkdir(parents=True, exist_ok=True)
    target = tmp_path / "docs" / "_temp" / "test_fenced.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("original\n", encoding="utf-8")
    base_sha = _sha256_text("original\n")

    app = create_app(cfg)
    headers = {"Authorization": "Bearer wb-a-token"}

    with TestClient(app, base_url="http://localhost") as client:
        # Claim lease
        claim_resp = client.post(
            "/v1/lease/claim",
            json={
                "round_id": "fenced-round-001",
                "target_paths": ["docs/_temp/test_fenced.md"],
                "lease_seconds": 120,
            },
            headers=headers,
        )
        assert claim_resp.status_code == 200
        lease_id = claim_resp.json()["lease_id"]
        fencing_token = claim_resp.json()["fencing_token"]

        # Batch preview
        preview_resp = client.post(
            "/v1/batch-preview",
            json={
                "items": [{
                    "target_path": "docs/_temp/test_fenced.md",
                    "base_sha256": base_sha,
                    "patch_text": "patched content\n",
                }],
            },
            headers=headers,
        )
        assert preview_resp.status_code == 200

        # Batch commit with fencing
        commit_resp = client.post(
            "/v1/batch-commit",
            json={
                "items": [{
                    "target_path": "docs/_temp/test_fenced.md",
                    "base_sha256": base_sha,
                    "patch_text": "patched content\n",
                }],
                "idempotency_key": "fenced-round-001-batch",
                "lease_id": lease_id,
                "fencing_token": fencing_token,
                "run_id": "fenced-round-001",
                "actor": {"type": "test", "id": "wave3"},
            },
            headers=headers,
        )
        assert commit_resp.status_code == 200, commit_resp.text
        assert commit_resp.json()["status"] in ("committed", "partial")

    assert target.read_text(encoding="utf-8") == "patched content\n"


def test_batch_commit_rejects_invalid_fencing_token(tmp_path: Path):
    """Batch commit with wrong fencing_token should be rejected."""
    cfg = _make_writeback_a_config(tmp_path)
    (tmp_path / "runtime" / "writeback_coordination").mkdir(parents=True, exist_ok=True)
    target = tmp_path / "docs" / "_temp" / "test_reject.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("original\n", encoding="utf-8")
    base_sha = _sha256_text("original\n")

    app = create_app(cfg)
    headers = {"Authorization": "Bearer wb-a-token"}

    with TestClient(app, base_url="http://localhost") as client:
        # Claim lease first
        claim_resp = client.post(
            "/v1/lease/claim",
            json={
                "round_id": "reject-round-001",
                "target_paths": ["docs/_temp/test_reject.md"],
                "lease_seconds": 120,
            },
            headers=headers,
        )
        assert claim_resp.status_code == 200
        lease_id = claim_resp.json()["lease_id"]

        # Batch commit with WRONG fencing_token
        commit_resp = client.post(
            "/v1/batch-commit",
            json={
                "items": [{
                    "target_path": "docs/_temp/test_reject.md",
                    "base_sha256": base_sha,
                    "patch_text": "should not be written\n",
                }],
                "idempotency_key": "reject-round-001-batch",
                "lease_id": lease_id,
                "fencing_token": 99999,
                "run_id": "reject-round-001",
            },
            headers=headers,
        )
        assert commit_resp.status_code == 409

    # Content should be unchanged
    assert target.read_text(encoding="utf-8") == "original\n"


def test_kestra_code_fix_flow_has_lease_claim_task():
    """Verify the Kestra code-fix flow now includes a claim_lease task."""
    import yaml

    flow_path = Path(__file__).resolve().parent.parent / "automation" / "kestra" / "flows" / "yanbao_issue_mesh_code_fix_wave.yml"
    raw = flow_path.read_text(encoding="utf-8")
    flow = yaml.safe_load(raw)

    # Find the claim_lease task somewhere in the nested task tree
    def find_task_ids(tasks, found=None):
        if found is None:
            found = []
        for task in (tasks or []):
            if "id" in task:
                found.append(task["id"])
            for sub_key in ("then", "else", "tasks"):
                if sub_key in task:
                    find_task_ids(task[sub_key], found)
        return found

    all_ids = find_task_ids(flow.get("tasks", []))
    assert "claim_lease" in all_ids, f"claim_lease task not found in flow; tasks: {all_ids}"
    assert "writeback_batch_commit" in all_ids
