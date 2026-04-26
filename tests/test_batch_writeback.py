"""Tests for writeback_service batch endpoints — batch-preview/commit/rollback."""
from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from automation.writeback_service.app import (
    DEFAULT_DENY_22,
    ServiceConfig,
    _sha256_text,
    create_app,
)


def _make_cfg(tmp_path: Path) -> ServiceConfig:
    return ServiceConfig(
        repo_root=tmp_path.resolve(),
        audit_dir=(tmp_path / ".writeback_audit").resolve(),
        allow_prefixes=("app/", "docs/_temp/"),
        deny_prefixes=(),
        deny_paths=(DEFAULT_DENY_22,),
        auth_token="test-token",
        require_triage=False,
        lock_timeout_seconds=2.0,
    )


def _headers() -> dict[str, str]:
    return {"Authorization": "Bearer test-token"}


class TestBatchPreview:
    def test_batch_preview_returns_results(self, tmp_path: Path) -> None:
        f1 = tmp_path / "app" / "a.py"
        f2 = tmp_path / "app" / "b.py"
        f1.parent.mkdir(parents=True, exist_ok=True)
        f1.write_text("old_a\n", encoding="utf-8")
        f2.write_text("old_b\n", encoding="utf-8")

        app = create_app(_make_cfg(tmp_path))
        with TestClient(app) as c:
            resp = c.post(
                "/v1/batch-preview",
                json={
                    "items": [
                        {"target_path": "app/a.py", "base_sha256": _sha256_text("old_a\n"), "patch_text": "new_a\n"},
                        {"target_path": "app/b.py", "base_sha256": _sha256_text("old_b\n"), "patch_text": "new_b\n"},
                    ]
                },
                headers=_headers(),
            )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["results"]) == 2
        assert body["results"][0]["conflict"] is False
        assert body["results"][1]["conflict"] is False

    def test_batch_preview_detects_conflict(self, tmp_path: Path) -> None:
        f1 = tmp_path / "app" / "a.py"
        f1.parent.mkdir(parents=True, exist_ok=True)
        f1.write_text("current\n", encoding="utf-8")

        app = create_app(_make_cfg(tmp_path))
        with TestClient(app) as c:
            resp = c.post(
                "/v1/batch-preview",
                json={
                    "items": [
                        {"target_path": "app/a.py", "base_sha256": "wrong_sha", "patch_text": "new\n"},
                    ]
                },
                headers=_headers(),
            )
        assert resp.status_code == 200
        assert resp.json()["results"][0]["conflict"] is True


class TestBatchCommit:
    def test_batch_commit_all_or_nothing(self, tmp_path: Path) -> None:
        f1 = tmp_path / "app" / "a.py"
        f2 = tmp_path / "app" / "b.py"
        f1.parent.mkdir(parents=True, exist_ok=True)
        f1.write_text("old_a\n", encoding="utf-8")
        f2.write_text("old_b\n", encoding="utf-8")

        cfg = _make_cfg(tmp_path)
        app = create_app(cfg)
        with TestClient(app) as c:
            # First preview both
            c.post(
                "/v1/batch-preview",
                json={
                    "items": [
                        {"target_path": "app/a.py", "base_sha256": _sha256_text("old_a\n"), "patch_text": "new_a\n"},
                        {"target_path": "app/b.py", "base_sha256": _sha256_text("old_b\n"), "patch_text": "new_b\n"},
                    ]
                },
                headers=_headers(),
            )
            # Commit
            resp = c.post(
                "/v1/batch-commit",
                json={
                    "items": [
                        {"target_path": "app/a.py", "base_sha256": _sha256_text("old_a\n"), "patch_text": "new_a\n"},
                        {"target_path": "app/b.py", "base_sha256": _sha256_text("old_b\n"), "patch_text": "new_b\n"},
                    ],
                    "idempotency_key": "batch-test-001",
                    "run_id": "test-round-1",
                },
                headers=_headers(),
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "committed"
        assert len(body["commits"]) == 2
        assert f1.read_text(encoding="utf-8") == "new_a\n"
        assert f2.read_text(encoding="utf-8") == "new_b\n"

    def test_batch_commit_rollback_on_sha_mismatch(self, tmp_path: Path) -> None:
        f1 = tmp_path / "app" / "a.py"
        f2 = tmp_path / "app" / "b.py"
        f1.parent.mkdir(parents=True, exist_ok=True)
        f1.write_text("old_a\n", encoding="utf-8")
        f2.write_text("old_b\n", encoding="utf-8")

        cfg = _make_cfg(tmp_path)
        app = create_app(cfg)
        with TestClient(app) as c:
            # Preview both
            c.post(
                "/v1/batch-preview",
                json={
                    "items": [
                        {"target_path": "app/a.py", "base_sha256": _sha256_text("old_a\n"), "patch_text": "new_a\n"},
                        {"target_path": "app/b.py", "base_sha256": "BAD_SHA", "patch_text": "new_b\n"},
                    ]
                },
                headers=_headers(),
            )
            # Commit should fail because b.py base_sha mismatch
            resp = c.post(
                "/v1/batch-commit",
                json={
                    "items": [
                        {"target_path": "app/a.py", "base_sha256": _sha256_text("old_a\n"), "patch_text": "new_a\n"},
                        {"target_path": "app/b.py", "base_sha256": "BAD_SHA", "patch_text": "new_b\n"},
                    ],
                    "idempotency_key": "batch-test-002",
                },
                headers=_headers(),
            )

        assert resp.status_code == 409
        assert "BASE_SHA_MISMATCH" in resp.json()["detail"]
        # Both files unchanged
        assert f1.read_text(encoding="utf-8") == "old_a\n"
        assert f2.read_text(encoding="utf-8") == "old_b\n"

    def test_batch_commit_idempotent_replay(self, tmp_path: Path) -> None:
        f1 = tmp_path / "app" / "a.py"
        f1.parent.mkdir(parents=True, exist_ok=True)
        f1.write_text("old\n", encoding="utf-8")

        cfg = _make_cfg(tmp_path)
        app = create_app(cfg)
        with TestClient(app) as c:
            c.post(
                "/v1/batch-preview",
                json={"items": [{"target_path": "app/a.py", "base_sha256": _sha256_text("old\n"), "patch_text": "new\n"}]},
                headers=_headers(),
            )
            r1 = c.post(
                "/v1/batch-commit",
                json={
                    "items": [{"target_path": "app/a.py", "base_sha256": _sha256_text("old\n"), "patch_text": "new\n"}],
                    "idempotency_key": "batch-idem-001",
                    "run_id": "run-1",
                },
                headers=_headers(),
            )
            r2 = c.post(
                "/v1/batch-commit",
                json={
                    "items": [{"target_path": "app/a.py", "base_sha256": _sha256_text("old\n"), "patch_text": "new\n"}],
                    "idempotency_key": "batch-idem-001",
                    "run_id": "run-1",
                },
                headers=_headers(),
            )

        assert r1.status_code == 200
        assert r2.status_code == 200
        assert r2.json()["idempotent_replay"] is True


class TestBatchRollback:
    def test_batch_rollback_reverses_commits(self, tmp_path: Path) -> None:
        f1 = tmp_path / "app" / "a.py"
        f2 = tmp_path / "app" / "b.py"
        f1.parent.mkdir(parents=True, exist_ok=True)
        f1.write_text("old_a\n", encoding="utf-8")
        f2.write_text("old_b\n", encoding="utf-8")

        cfg = _make_cfg(tmp_path)
        app = create_app(cfg)
        with TestClient(app) as c:
            c.post(
                "/v1/batch-preview",
                json={
                    "items": [
                        {"target_path": "app/a.py", "base_sha256": _sha256_text("old_a\n"), "patch_text": "new_a\n"},
                        {"target_path": "app/b.py", "base_sha256": _sha256_text("old_b\n"), "patch_text": "new_b\n"},
                    ]
                },
                headers=_headers(),
            )
            commit_resp = c.post(
                "/v1/batch-commit",
                json={
                    "items": [
                        {"target_path": "app/a.py", "base_sha256": _sha256_text("old_a\n"), "patch_text": "new_a\n"},
                        {"target_path": "app/b.py", "base_sha256": _sha256_text("old_b\n"), "patch_text": "new_b\n"},
                    ],
                    "idempotency_key": "batch-rollback-test",
                    "run_id": "run-rb",
                },
                headers=_headers(),
            )
            commit_ids = [c_item["commit_id"] for c_item in commit_resp.json()["commits"]]

            rb_resp = c.post(
                "/v1/batch-rollback",
                json={
                    "commit_ids": commit_ids,
                    "idempotency_key": "batch-rb-001",
                    "run_id": "run-rb",
                },
                headers=_headers(),
            )

        assert rb_resp.status_code == 200
        assert rb_resp.json()["status"] == "rolled_back"
        assert f1.read_text(encoding="utf-8") == "old_a\n"
        assert f2.read_text(encoding="utf-8") == "old_b\n"
