"""End-to-end fencing tests for the writeback coordination → gateway → commit chain.

Tests verify:
1. Claim → preview → commit (with fence) → release lifecycle
2. Commit without fencing token is rejected
3. Expired leases are auto-pruned and reclaimable
4. Optimistic SHA256 concurrency check
5. Gateway apply_fixes passes triage + fence through to commit
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.services.writeback_coordination import (
    ClaimConflictError,
    LeaseRejectedError,
    WritebackCoordination,
    WritebackLease,
)


def _clock(start: datetime):
    holder = {"now": start}

    def _now() -> datetime:
        return holder["now"]

    return holder, _now


def _coord(tmp_path: Path, start: datetime):
    holder, now_fn = _clock(start)
    svc = WritebackCoordination(
        tmp_path / "runtime" / "writeback_coordination" / "state.json",
        now_fn=now_fn,
    )
    return svc, holder


# ---- Full lifecycle: claim → submit → release ----


def test_claim_preview_commit_release_lifecycle(tmp_path: Path):
    """Full happy path: claim lease, check submit allowed, release."""
    now = datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    lease = coord.claim("fix-round-001", ["app/services/foo.py"], lease_seconds=600)

    assert isinstance(lease, WritebackLease)
    assert lease.round_id == "fix-round-001"
    assert lease.fencing_token >= 1

    coord.assert_submit_allowed(
        lease.lease_id, lease.fencing_token, ["app/services/foo.py"]
    )

    coord.release(lease=lease, reason="round_finished")

    with pytest.raises(LeaseRejectedError) as exc:
        coord.assert_submit_allowed(
            lease.lease_id, lease.fencing_token, ["app/services/foo.py"]
        )
    assert "LEASE_NOT_ACTIVE" in str(exc.value)


# ---- Commit without fencing is rejected ----


def test_commit_without_valid_fencing_token_is_rejected(tmp_path: Path):
    now = datetime(2026, 3, 30, 10, 5, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    lease = coord.claim("fix-round-002", ["app/services/bar.py"], lease_seconds=600)

    with pytest.raises(LeaseRejectedError) as exc:
        coord.assert_submit_allowed(
            lease.lease_id, lease.fencing_token + 999, ["app/services/bar.py"]
        )
    assert "FENCING_TOKEN_MISMATCH" in str(exc.value)


def test_commit_with_empty_lease_id_is_rejected(tmp_path: Path):
    now = datetime(2026, 3, 30, 10, 6, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    with pytest.raises(LeaseRejectedError) as exc:
        coord.assert_submit_allowed("", 1, ["app/services/bar.py"])
    assert "LEASE_ID_MISSING" in str(exc.value)


def test_commit_with_out_of_scope_path_is_rejected(tmp_path: Path):
    now = datetime(2026, 3, 30, 10, 7, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    lease = coord.claim("fix-round-003", ["app/services/baz.py"], lease_seconds=600)

    with pytest.raises(LeaseRejectedError) as exc:
        coord.assert_submit_allowed(
            lease.lease_id, lease.fencing_token, ["app/services/other.py"]
        )
    assert "TARGET_PATH_OUT_OF_SCOPE" in str(exc.value)


# ---- Expired lease auto-pruning ----


def test_expired_lease_is_auto_pruned_and_reclaimable(tmp_path: Path):
    start = datetime(2026, 3, 30, 10, 10, tzinfo=timezone.utc)
    coord, holder = _coord(tmp_path, start)

    old_lease = coord.claim("fix-round-old", ["app/services/x.py"], lease_seconds=30)

    holder["now"] = start + timedelta(seconds=31)

    new_lease = coord.claim("fix-round-new", ["app/services/x.py"], lease_seconds=60)
    assert new_lease.lease_id != old_lease.lease_id
    assert new_lease.fencing_token > old_lease.fencing_token

    with pytest.raises(LeaseRejectedError) as exc:
        coord.assert_submit_allowed(
            old_lease.lease_id, old_lease.fencing_token, ["app/services/x.py"]
        )
    assert "LEASE_NOT_ACTIVE" in str(exc.value)

    coord.assert_submit_allowed(
        new_lease.lease_id, new_lease.fencing_token, ["app/services/x.py"]
    )


def test_expired_lease_does_not_block_subsequent_claim(tmp_path: Path):
    start = datetime(2026, 3, 30, 10, 15, tzinfo=timezone.utc)
    coord, holder = _coord(tmp_path, start)

    coord.claim("fix-round-a", ["app/services/alpha.py"], lease_seconds=5)
    holder["now"] = start + timedelta(seconds=6)

    new = coord.claim("fix-round-b", ["app/services/alpha.py"], lease_seconds=60)
    assert new.round_id == "fix-round-b"


# ---- Optimistic SHA256 concurrency ----


def test_preview_sha256_verification_pass(tmp_path: Path):
    now = datetime(2026, 3, 30, 10, 20, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    lease = coord.claim("fix-round-sha-001", ["app/services/sha_test.py"], lease_seconds=120)

    coord.register_preview_sha256(lease.lease_id, "app/services/sha_test.py", "abc123")

    assert coord.verify_base_sha256(lease.lease_id, "app/services/sha_test.py", "abc123") is True


def test_preview_sha256_verification_fail_on_drift(tmp_path: Path):
    now = datetime(2026, 3, 30, 10, 21, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    lease = coord.claim("fix-round-sha-002", ["app/services/sha_test2.py"], lease_seconds=120)

    coord.register_preview_sha256(lease.lease_id, "app/services/sha_test2.py", "abc123")

    assert coord.verify_base_sha256(lease.lease_id, "app/services/sha_test2.py", "CHANGED") is False


def test_preview_sha256_verification_pass_when_no_preview_recorded(tmp_path: Path):
    now = datetime(2026, 3, 30, 10, 22, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    assert coord.verify_base_sha256("nonexistent", "app/x.py", "whatever") is True


# ---- Fencing token monotonically increases ----


def test_fencing_token_increases_across_claims(tmp_path: Path):
    now = datetime(2026, 3, 30, 10, 30, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    l1 = coord.claim("r1", ["app/a.py"], lease_seconds=600)
    l2 = coord.claim("r2", ["app/b.py"], lease_seconds=600)

    assert l2.fencing_token > l1.fencing_token


# ---- Gateway apply_fixes passes fence to commit ----


def test_gateway_apply_fixes_passes_lease_to_commit():
    """Verify that apply_fixes() follows triage → commit and passes lease/fence through."""
    from app.services.issue_mesh_gateway import HttpIssueMeshGateway, HttpIssueMeshGatewayConfig
    from automation.loop_controller.schemas import ProblemSpec

    cfg = HttpIssueMeshGatewayConfig(
        mesh_runner_base_url="http://localhost:8093",
        mesh_runner_token="tok",
        promote_prep_base_url="http://localhost:8094",
        promote_prep_token="tok",
        writeback_a_base_url="http://localhost:8092",
        writeback_a_token="tok",
        app_base_url="http://localhost:38001",
        internal_token="tok",
    )
    gw = HttpIssueMeshGateway(cfg)

    synthesize_resp = MagicMock()
    synthesize_resp.status_code = 200
    synthesize_resp.raise_for_status = MagicMock()
    synthesize_resp.json.return_value = {
        "patches": [
            {
                "issue_key": "test-issue-001",
                "valid": True,
                "target_path": "app/services/test_target.py",
                "patch_text": "# patched",
                "base_sha256": "abc123",
            }
        ]
    }

    preview_resp = MagicMock()
    preview_resp.status_code = 200
    preview_resp.json.return_value = {"conflict": False}

    triage_resp = MagicMock()
    triage_resp.status_code = 200
    triage_resp.json.return_value = {
        "auto_commit": True,
        "triage_record_id": "fix-round-test__code-fix-writeback",
    }

    commit_resp = MagicMock()
    commit_resp.status_code = 200
    commit_resp.json.return_value = {"status": "committed", "commit_id": "c-001"}

    call_log: list[tuple[str, dict[str, Any]]] = []

    def _mock_post(url: str, *, json: dict[str, Any] | None = None, headers: dict[str, str] | None = None):
        call_log.append((url, json or {}))
        if "/triage/synthesize-patches" in url:
            return synthesize_resp
        if "/triage/writeback" in url:
            return triage_resp
        if "/v1/preview" in url:
            return preview_resp
        if "/v1/commit" in url:
            return commit_resp
        return MagicMock(status_code=200)

    gw._client = MagicMock()
    gw._client.post = _mock_post

    problem = ProblemSpec(
        problem_id="test-issue-001",
        severity="P1",
        family="truth-lineage",
        title="Test issue",
        affected_files=["app/services/test_target.py"],
        write_scope=["app/services"],
    )

    results = gw.apply_fixes(
        problems=[problem],
        round_id="fix-round-test",
        audit_run_id="audit-001",
        runtime_context={
            "runtime_gates": {"status": "blocked"},
            "public_runtime_status": "DEGRADED",
        },
        lease={
            "lease_id": "lease-abc",
            "fencing_token": 42,
        },
    )

    assert len(results) == 1
    assert results[0]["outcome"] == "success"

    commit_call = [c for c in call_log if "/v1/commit" in c[0]]
    triage_call = [c for c in call_log if "/triage/writeback" in c[0]]
    synthesize_call = [c for c in call_log if "/triage/synthesize-patches" in c[0]]

    assert len(synthesize_call) == 1
    assert synthesize_call[0][1]["runtime_gates"] == {"status": "blocked"}
    assert len(commit_call) == 1
    assert len(triage_call) == 1
    assert triage_call[0][1]["layer"] == "code-fix"
    assert triage_call[0][1]["preview_summary"] == {"conflict": False}
    commit_body = commit_call[0][1]
    assert commit_body["lease_id"] == "lease-abc"
    assert commit_body["fencing_token"] == 42
    assert commit_body["triage_record_id"] == "fix-round-test__code-fix-writeback"


def test_gateway_apply_fixes_refreshes_lease_before_commit():
    from app.services.issue_mesh_gateway import HttpIssueMeshGateway, HttpIssueMeshGatewayConfig
    from automation.loop_controller.schemas import ProblemSpec

    cfg = HttpIssueMeshGatewayConfig(
        mesh_runner_base_url="http://localhost:8093",
        mesh_runner_token="tok",
        promote_prep_base_url="http://localhost:8094",
        promote_prep_token="tok",
        writeback_a_base_url="http://localhost:8092",
        writeback_a_token="tok",
        app_base_url="http://localhost:38001",
        internal_token="tok",
    )
    gw = HttpIssueMeshGateway(cfg)

    synthesize_resp = MagicMock()
    synthesize_resp.status_code = 200
    synthesize_resp.raise_for_status = MagicMock()
    synthesize_resp.json.return_value = {
        "patches": [
            {
                "issue_key": "test-issue-lease-refresh",
                "valid": True,
                "target_path": "app/services/test_target.py",
                "patch_text": "# patched",
                "base_sha256": "abc123",
            }
        ]
    }

    preview_resp = MagicMock()
    preview_resp.status_code = 200
    preview_resp.json.return_value = {"conflict": False}

    triage_resp = MagicMock()
    triage_resp.status_code = 200
    triage_resp.json.return_value = {
        "auto_commit": True,
        "triage_record_id": "fix-round-refresh__code-fix-writeback",
    }

    commit_resp = MagicMock()
    commit_resp.status_code = 200
    commit_resp.json.return_value = {"status": "committed", "commit_id": "c-refresh"}

    call_log: list[tuple[str, dict[str, Any]]] = []

    def _mock_post(url: str, *, json: dict[str, Any] | None = None, headers: dict[str, str] | None = None):
        call_log.append((url, json or {}))
        if "/triage/synthesize-patches" in url:
            return synthesize_resp
        if "/triage/writeback" in url:
            return triage_resp
        if "/v1/preview" in url:
            return preview_resp
        if "/v1/commit" in url:
            return commit_resp
        return MagicMock(status_code=200)

    gw._client = MagicMock()
    gw._client.post = _mock_post

    class _RefreshCoordinator:
        def __init__(self) -> None:
            self.calls = 0

        def refresh(self, *, lease: dict[str, Any]) -> dict[str, Any] | None:
            self.calls += 1
            refreshed = dict(lease)
            refreshed["fencing_token"] = 43
            return refreshed

    coordinator = _RefreshCoordinator()
    problem = ProblemSpec(
        problem_id="test-issue-lease-refresh",
        severity="P1",
        family="truth-lineage",
        title="Test issue",
        affected_files=["app/services/test_target.py"],
        write_scope=["app/services"],
    )

    results = gw.apply_fixes(
        problems=[problem],
        round_id="fix-round-refresh",
        audit_run_id="audit-refresh-001",
        runtime_context={"runtime_gates": {"status": "ready"}},
        coordinator=coordinator,
        lease={
            "lease_id": "lease-refresh",
            "fencing_token": 42,
        },
    )

    assert len(results) == 1
    assert results[0]["outcome"] == "success"
    commit_call = [c for c in call_log if "/v1/commit" in c[0]]
    assert len(commit_call) == 1
    assert commit_call[0][1]["fencing_token"] == 43
    assert coordinator.calls == 1


def test_gateway_promote_round_claims_fencing_for_status_note_commit():
    from app.services.issue_mesh_gateway import HttpIssueMeshGateway, HttpIssueMeshGatewayConfig

    cfg = HttpIssueMeshGatewayConfig(
        mesh_runner_base_url="http://localhost:8093",
        mesh_runner_token="tok",
        promote_prep_base_url="http://localhost:8094",
        promote_prep_token="tok",
        writeback_a_base_url="http://localhost:8092",
        writeback_a_token="tok",
        writeback_b_base_url="http://localhost:8095",
        writeback_b_token="tok",
        app_base_url="http://localhost:38001",
        internal_token="tok",
    )
    gw = HttpIssueMeshGateway(cfg)

    status_prepare = MagicMock()
    status_prepare.status_code = 200
    status_prepare.json.return_value = {
        "run_id": "issue-mesh-lease-001",
        "layer": "status-note",
        "target_path": "docs/core/22_progress.md",
        "target_anchor": "current-writeback-detail",
        "patch_text": "patched",
        "base_sha256": "abc123",
        "idempotency_key": "issue-mesh:lease-001:status-note",
        "request_id": "req-issue-mesh-lease-001-status-note",
        "semantic_fingerprint": "fp-lease-1",
        "skip_commit": False,
    }
    triage_resp = MagicMock()
    triage_resp.status_code = 200
    triage_resp.json.return_value = {
        "auto_commit": True,
        "triage_record_id": "issue-mesh-lease-001__status-note",
    }
    preview_resp = MagicMock()
    preview_resp.status_code = 200
    preview_resp.json.return_value = {"conflict": False}
    commit_resp = MagicMock()
    commit_resp.status_code = 200
    commit_resp.json.return_value = {"status": "committed", "commit_id": "commit-lease-001"}

    call_log: list[tuple[str, dict[str, Any]]] = []

    def _mock_post(url: str, *, json: dict[str, Any] | None = None, headers: dict[str, str] | None = None):
        call_log.append((url, json or {}))
        if "/v1/promote/status-note" in url:
            return status_prepare
        if "/v1/triage" in url:
            return triage_resp
        if "/v1/preview" in url:
            return preview_resp
        if "/v1/commit" in url:
            return commit_resp
        raise AssertionError(f"unexpected POST: {url}")

    gw._client = MagicMock()
    gw._client.post = _mock_post

    class _PromoteCoordinator:
        def __init__(self) -> None:
            self.claim_calls: list[dict[str, Any]] = []
            self.refresh_calls: list[dict[str, Any]] = []
            self.assert_calls: list[tuple[str, int, list[str]]] = []
            self.release_calls: list[dict[str, Any]] = []

        def claim(self, *, round_id: str, target_paths: list[str], lease_seconds: int) -> dict[str, Any]:
            self.claim_calls.append(
                {
                    "round_id": round_id,
                    "target_paths": list(target_paths),
                    "lease_seconds": lease_seconds,
                }
            )
            return {"lease_id": "promote-lease-001", "fencing_token": 7, "target_paths": list(target_paths)}

        def refresh(self, *, lease: dict[str, Any]) -> dict[str, Any] | None:
            self.refresh_calls.append(dict(lease))
            refreshed = dict(lease)
            refreshed["fencing_token"] = 8
            return refreshed

        def assert_submit_allowed(self, lease_id: str, fencing_token: int, target_paths: list[str]) -> None:
            self.assert_calls.append((lease_id, fencing_token, list(target_paths)))

        def release(self, *, lease: dict[str, Any], reason: str) -> None:
            self.release_calls.append({"lease": dict(lease), "reason": reason})

    coordinator = _PromoteCoordinator()

    result = gw.promote_round(
        round_id="fix-round-lease-001",
        audit_run_id="issue-mesh-lease-001",
        runtime_context={
            "runtime_gates": {
                "status": "blocked",
                "shared_artifact_promote": {"allowed": False},
            }
        },
        coordinator=coordinator,
        lease_seconds=180,
    )

    assert result == {
        "promoted": False,
        "reason": "CURRENT_LAYER_RUNTIME_GATE_BLOCKED",
        "status_note_committed": True,
    }
    assert coordinator.claim_calls == [
        {
            "round_id": "fix-round-lease-001:status-note",
            "target_paths": ["docs/core/22_progress.md"],
            "lease_seconds": 180,
        }
    ]
    assert coordinator.refresh_calls[0]["fencing_token"] == 7
    assert coordinator.assert_calls == [
        ("promote-lease-001", 8, ["docs/core/22_progress.md"])
    ]
    assert coordinator.release_calls[0]["reason"] == "promote_status-note_finished"
    commit_call = [c for c in call_log if "/v1/commit" in c[0]]
    assert commit_call[0][1]["lease_id"] == "promote-lease-001"
    assert commit_call[0][1]["fencing_token"] == 8


def test_gateway_promote_round_fails_closed_when_promote_lease_check_rejected():
    from app.services.issue_mesh_gateway import HttpIssueMeshGateway, HttpIssueMeshGatewayConfig

    cfg = HttpIssueMeshGatewayConfig(
        mesh_runner_base_url="http://localhost:8093",
        mesh_runner_token="tok",
        promote_prep_base_url="http://localhost:8094",
        promote_prep_token="tok",
        writeback_a_base_url="http://localhost:8092",
        writeback_a_token="tok",
        writeback_b_base_url="http://localhost:8095",
        writeback_b_token="tok",
        app_base_url="http://localhost:38001",
        internal_token="tok",
    )
    gw = HttpIssueMeshGateway(cfg)

    status_prepare = MagicMock()
    status_prepare.status_code = 200
    status_prepare.json.return_value = {
        "run_id": "issue-mesh-lease-002",
        "layer": "status-note",
        "target_path": "docs/core/22_progress.md",
        "target_anchor": "current-writeback-detail",
        "patch_text": "patched",
        "base_sha256": "abc123",
        "skip_commit": False,
    }
    triage_resp = MagicMock()
    triage_resp.status_code = 200
    triage_resp.json.return_value = {
        "auto_commit": True,
        "triage_record_id": "issue-mesh-lease-002__status-note",
    }
    preview_resp = MagicMock()
    preview_resp.status_code = 200
    preview_resp.json.return_value = {"conflict": False}

    call_log: list[str] = []

    def _mock_post(url: str, *, json: dict[str, Any] | None = None, headers: dict[str, str] | None = None):
        call_log.append(url)
        if "/v1/promote/status-note" in url:
            return status_prepare
        if "/v1/triage" in url:
            return triage_resp
        if "/v1/preview" in url:
            return preview_resp
        raise AssertionError(f"unexpected POST: {url}")

    gw._client = MagicMock()
    gw._client.post = _mock_post

    class _RejectingCoordinator:
        def claim(self, *, round_id: str, target_paths: list[str], lease_seconds: int) -> dict[str, Any]:
            return {"lease_id": "promote-lease-002", "fencing_token": 11, "target_paths": list(target_paths)}

        def refresh(self, *, lease: dict[str, Any]) -> dict[str, Any] | None:
            return dict(lease)

        def assert_submit_allowed(self, lease_id: str, fencing_token: int, target_paths: list[str]) -> None:
            raise RuntimeError("LEASE_NOT_ACTIVE")

        def release(self, *, lease: dict[str, Any], reason: str) -> None:
            return None

    result = gw.promote_round(
        round_id="fix-round-lease-002",
        audit_run_id="issue-mesh-lease-002",
        runtime_context={
            "runtime_gates": {
                "status": "blocked",
                "shared_artifact_promote": {"allowed": False},
            }
        },
        coordinator=_RejectingCoordinator(),
        lease_seconds=180,
    )

    assert result["promoted"] is False
    assert result["reason"] == "PROMOTE_LEASE_CHECK_FAILED:status-note:LEASE_NOT_ACTIVE"
    assert all("/v1/commit" not in url for url in call_log)


def test_gateway_apply_fixes_fails_closed_without_lease():
    """Gateway must not attempt preview/commit when lease/fencing is missing."""
    from app.services.issue_mesh_gateway import HttpIssueMeshGateway, HttpIssueMeshGatewayConfig
    from automation.loop_controller.schemas import ProblemSpec

    cfg = HttpIssueMeshGatewayConfig(
        mesh_runner_base_url="http://localhost:8093",
        mesh_runner_token="tok",
        promote_prep_base_url="http://localhost:8094",
        promote_prep_token="tok",
        writeback_a_base_url="http://localhost:8092",
        writeback_a_token="tok",
        app_base_url="http://localhost:38001",
        internal_token="tok",
    )
    gw = HttpIssueMeshGateway(cfg)
    gw._client = MagicMock()

    problem = ProblemSpec(
        problem_id="test-issue-002",
        severity="P1",
        family="truth-lineage",
        title="Test issue",
        affected_files=["app/services/test_target.py"],
        write_scope=["app/services"],
    )

    results = gw.apply_fixes(
        problems=[problem],
        round_id="fix-round-test",
        audit_run_id="audit-001",
        runtime_context={},
        lease=None,
    )

    assert results == [
        {
            "problem_id": "test-issue-002",
            "outcome": "failed",
            "patches_applied": [],
            "error": "WRITEBACK_LEASE_REQUIRED",
        }
    ]
    gw._client.post.assert_not_called()


def test_gateway_promote_round_still_runs_status_note_when_current_layer_gate_blocked():
    """Status-note may still commit, but the round must remain non-promoted when current-layer is blocked."""
    from app.services.issue_mesh_gateway import HttpIssueMeshGateway, HttpIssueMeshGatewayConfig

    cfg = HttpIssueMeshGatewayConfig(
        mesh_runner_base_url="http://localhost:8093",
        mesh_runner_token="tok",
        promote_prep_base_url="http://localhost:8094",
        promote_prep_token="tok",
        writeback_a_base_url="http://localhost:8092",
        writeback_a_token="tok",
        writeback_b_base_url="http://localhost:8095",
        writeback_b_token="tok",
        app_base_url="http://localhost:38001",
        internal_token="tok",
    )
    gw = HttpIssueMeshGateway(cfg)

    status_prepare = MagicMock()
    status_prepare.status_code = 200
    status_prepare.json.return_value = {
        "run_id": "issue-mesh-001",
        "layer": "status-note",
        "target_path": "docs/core/22_progress.md",
        "target_anchor": "current-writeback-detail",
        "patch_text": "patched",
        "base_sha256": "abc123",
        "idempotency_key": "issue-mesh:issue-mesh-001:status-note:current-writeback-detail",
        "request_id": "req-issue-mesh-001-status-note-current-writeback-detail",
        "semantic_fingerprint": "fp-1",
        "skip_commit": False,
    }
    triage_resp = MagicMock()
    triage_resp.status_code = 200
    triage_resp.json.return_value = {
        "auto_commit": True,
        "triage_record_id": "issue-mesh-001__status-note",
    }
    preview_resp = MagicMock()
    preview_resp.status_code = 200
    preview_resp.json.return_value = {"conflict": False}
    commit_resp = MagicMock()
    commit_resp.status_code = 200
    commit_resp.json.return_value = {"status": "committed", "commit_id": "commit-001"}

    call_log: list[tuple[str, dict[str, Any]]] = []

    def _mock_post(url: str, *, json: dict[str, Any] | None = None, headers: dict[str, str] | None = None):
        call_log.append((url, json or {}))
        if "/v1/promote/status-note" in url:
            return status_prepare
        if "/v1/triage" in url:
            return triage_resp
        if "/v1/preview" in url:
            return preview_resp
        if "/v1/commit" in url:
            return commit_resp
        raise AssertionError(f"unexpected POST: {url}")

    gw._client = MagicMock()
    gw._client.post = _mock_post

    result = gw.promote_round(
        round_id="fix-round-001",
        audit_run_id="issue-mesh-001",
        runtime_context={
            "runtime_gates": {
                "status": "blocked",
                "shared_artifact_promote": {"allowed": False},
            },
            "public_runtime_status": "DEGRADED",
        },
    )

    assert result == {
        "promoted": False,
        "reason": "CURRENT_LAYER_RUNTIME_GATE_BLOCKED",
        "status_note_committed": True,
    }
    prepare_calls = [c for c in call_log if "/v1/promote/status-note" in c[0]]
    triage_calls = [c for c in call_log if c[0].endswith("/v1/triage")]
    current_layer_calls = [c for c in call_log if "/v1/promote/current-layer" in c[0]]

    assert len(prepare_calls) == 1
    assert current_layer_calls == []
    assert prepare_calls[0][1]["run_id"] == "issue-mesh-001"
    assert prepare_calls[0][1]["runtime_gates"]["status"] == "blocked"
    assert prepare_calls[0][1]["audit_context"]["round_id"] == "fix-round-001"
    assert len(triage_calls) == 1
    assert triage_calls[0][1]["layer"] == "status-note"
    assert triage_calls[0][1]["target_anchor"] == "current-writeback-detail"
