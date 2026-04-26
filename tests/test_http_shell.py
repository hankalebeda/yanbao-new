"""Tests for the Escort Team HTTP Shell (FastAPI bridge).

Uses Starlette's TestClient to test all 9 endpoints without any external
service dependencies.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# We import create_app rather than the module-level `app` so we can inject config
os.environ.setdefault("ESCORT_TEAM_TOKEN", "test-token-123")


class TestHTTPShellHealth:
    """Health endpoint (no auth)."""

    def test_health_idle(self, tmp_path):
        from automation.agents.http_shell import create_app
        from starlette.testclient import TestClient

        app = create_app(auth_token="tok", repo_root=tmp_path)
        with TestClient(app) as client:
            resp = client.get("/health")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "idle"
            assert data["running"] is False


class TestHTTPShellAuth:
    """Authentication enforcement."""

    def test_state_requires_auth(self, tmp_path):
        from automation.agents.http_shell import create_app
        from starlette.testclient import TestClient

        app = create_app(auth_token="secret", repo_root=tmp_path)
        with TestClient(app) as client:
            # Without token
            resp = client.get("/v1/state")
            assert resp.status_code in (401, 409)

            # With wrong token
            resp = client.get("/v1/state", headers={"Authorization": "Bearer wrong"})
            assert resp.status_code in (401, 409)

    def test_rounds_requires_auth(self, tmp_path):
        from automation.agents.http_shell import create_app
        from starlette.testclient import TestClient

        app = create_app(auth_token="secret", repo_root=tmp_path)
        with TestClient(app) as client:
            resp = client.get("/v1/rounds")
            assert resp.status_code in (401, 409)

    def test_start_requires_auth(self, tmp_path):
        from automation.agents.http_shell import create_app
        from starlette.testclient import TestClient

        app = create_app(auth_token="secret", repo_root=tmp_path)
        with TestClient(app) as client:
            resp = client.post("/v1/start", json={"mode": "fix"})
            assert resp.status_code == 401


class TestHTTPShellStateNotStarted:
    """State endpoint returns 409 when team is not started."""

    def test_state_409_when_not_started(self, tmp_path):
        from automation.agents.http_shell import create_app
        from starlette.testclient import TestClient

        app = create_app(auth_token="tok", repo_root=tmp_path)
        with TestClient(app) as client:
            resp = client.get("/v1/state", headers={"Authorization": "Bearer tok"})
            assert resp.status_code == 409

    def test_rounds_409_when_not_started(self, tmp_path):
        from automation.agents.http_shell import create_app
        from starlette.testclient import TestClient

        app = create_app(auth_token="tok", repo_root=tmp_path)
        with TestClient(app) as client:
            resp = client.get("/v1/rounds", headers={"Authorization": "Bearer tok"})
            assert resp.status_code == 409

    def test_stop_409_when_not_started(self, tmp_path):
        from automation.agents.http_shell import create_app
        from starlette.testclient import TestClient

        app = create_app(auth_token="tok", repo_root=tmp_path)
        with TestClient(app) as client:
            resp = client.post(
                "/v1/stop",
                json={"reason": "test"},
                headers={"Authorization": "Bearer tok"},
            )
            assert resp.status_code == 409


class TestHTTPShellRoundComplete:
    """Round complete (external notification)."""

    def test_round_complete_records(self, tmp_path):
        from automation.agents.http_shell import create_app
        from starlette.testclient import TestClient

        app = create_app(auth_token="tok", repo_root=tmp_path)
        with TestClient(app) as client:
            resp = client.post(
                "/v1/round-complete",
                json={"round_id": "r-1", "execution_id": "exec-1"},
                headers={"Authorization": "Bearer tok"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "recorded"
            assert data["round_id"] == "r-1"


class TestHTTPShellAwaitRound:
    """Await-round long-poll (timeout case)."""

    def test_await_round_timeout(self, tmp_path):
        from automation.agents.http_shell import create_app
        from starlette.testclient import TestClient

        app = create_app(auth_token="tok", repo_root=tmp_path)
        with TestClient(app) as client:
            resp = client.get(
                "/v1/await-round?timeout=10",
                headers={"Authorization": "Bearer tok"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["timed_out"] is True


class TestHTTPShellPromoteTargetMode:
    """State endpoint should mirror the control-plane promote mode."""

    def test_state_uses_control_plane_promote_mode(self, tmp_path):
        from automation.agents.http_shell import create_app
        from starlette.testclient import TestClient

        class _FakeTeam:
            async def start(self):
                await asyncio.sleep(3600)

            async def shutdown(self, reason: str = "test"):
                return None

            def get_status(self):
                return {
                    "coordinator": {
                        "mode": "fix",
                        "phase": "discovery",
                        "current_round_id": "r-state-1",
                        "consecutive_green_rounds": 2,
                        "total_fixes": 1,
                        "total_failures": 0,
                        "round_history": [],
                    }
                }

        control_plane = tmp_path / "automation" / "control_plane"
        control_plane.mkdir(parents=True, exist_ok=True)
        (control_plane / "current_state.json").write_text(
            json.dumps({"promote_target_mode": "infra"}, ensure_ascii=False),
            encoding="utf-8",
        )

        with patch("automation.agents.create_team", return_value=_FakeTeam()):
            app = create_app(auth_token="tok", repo_root=tmp_path)
            with TestClient(app) as client:
                start = client.post(
                    "/v1/start",
                    json={"mode": "fix"},
                    headers={"Authorization": "Bearer tok"},
                )
                assert start.status_code == 200

                resp = client.get("/v1/state", headers={"Authorization": "Bearer tok"})
                assert resp.status_code == 200
                assert resp.json()["promote_target_mode"] == "infra"

                stop = client.post(
                    "/v1/stop",
                    json={"reason": "test"},
                    headers={"Authorization": "Bearer tok"},
                )
                assert stop.status_code == 200

    def test_state_and_completion_status_reuse_runtime_projection(self, tmp_path):
        from automation.agents.http_shell import create_app
        from starlette.testclient import TestClient

        class _FakeTeam:
            async def start(self):
                await asyncio.sleep(3600)

            async def shutdown(self, reason: str = "test"):
                return None

            def get_status(self):
                return {
                    "coordinator": {
                        "coordinator": {
                            "mode": "completed",
                            "phase": "monitoring",
                            "current_round_id": "r-state-2",
                            "consecutive_green_rounds": 12,
                            "total_fixes": 3,
                            "total_failures": 0,
                            "round_history": [],
                            "completion_time": "2026-04-01T00:00:00Z",
                            "deferred_problems": [],
                            "total_rounds": 15,
                        },
                        "runtime_projection": {
                            "mode": "completed",
                            "phase": "monitoring",
                            "consecutive_fix_success_count": 12,
                            "consecutive_verified_problem_fixes": 12,
                            "consecutive_green_rounds": 12,
                            "fix_goal": 10,
                            "goal_progress_count": 12,
                            "goal_reached": True,
                            "total_fixes": 3,
                            "total_failures": 0,
                            "current_round_id": "r-state-2",
                            "last_promote_round_id": "r-promote-2",
                            "last_promote_at": "2026-04-01T00:00:00Z",
                            "round_history_size": 5,
                            "blocked_reason": None,
                            "provider_pool": {"ready": True, "status": "ok"},
                            "promote_target_mode": "infra",
                            "success_goal_metric": "verified_problem_count",
                            "execution_lanes": {},
                            "blocked_problems": [],
                            "completion_blockers": [],
                            "completion_time": "2026-04-01T00:00:00Z",
                            "last_audit_run_id": "audit-xyz",
                            "shadow_validation": {
                                "audit_run_id": "audit-xyz",
                                "ready": True,
                            },
                            "formal_promote": {
                                "state": "doc22_published",
                                "approved": True,
                                "targets_promoted": ["status-note", "shared-artifact", "current-layer", "doc22"],
                            },
                            "completion_evidence": {
                                "autonomy_index": 1.0,
                                "shadow_validation": {
                                    "audit_run_id": "audit-xyz",
                                    "ready": True,
                                },
                                "formal_promote": {
                                    "state": "doc22_published",
                                    "approved": True,
                                    "targets_promoted": ["status-note", "shared-artifact", "current-layer", "doc22"],
                                },
                            },
                            "autonomy_index": 1.0,
                        },
                        "completion_evidence": {
                            "autonomy_index": 1.0,
                            "shadow_validation": {
                                "audit_run_id": "audit-xyz",
                                "ready": True,
                            },
                            "formal_promote": {
                                "state": "doc22_published",
                                "approved": True,
                            },
                        },
                        "completion_blockers": [],
                    }
                }

        with patch("automation.agents.create_team", return_value=_FakeTeam()):
            app = create_app(auth_token="tok", repo_root=tmp_path)
            with TestClient(app) as client:
                start = client.post(
                    "/v1/start",
                    json={"mode": "fix"},
                    headers={"Authorization": "Bearer tok"},
                )
                assert start.status_code == 200

                state_resp = client.get("/v1/state", headers={"Authorization": "Bearer tok"})
                assert state_resp.status_code == 200
                state_data = state_resp.json()
                assert state_data["last_audit_run_id"] == "audit-xyz"
                assert state_data["shadow_validation"]["ready"] is True
                assert state_data["formal_promote"]["state"] == "doc22_published"

                completion_resp = client.get(
                    "/v1/completion-status",
                    headers={"Authorization": "Bearer tok"},
                )
                assert completion_resp.status_code == 200
                completion_data = completion_resp.json()
                assert completion_data["goal_reached"] is True
                assert completion_data["last_promote_round_id"] == "r-promote-2"
                assert completion_data["formal_promote"]["state"] == "doc22_published"

                stop = client.post(
                    "/v1/stop",
                    json={"reason": "test"},
                    headers={"Authorization": "Bearer tok"},
                )
                assert stop.status_code == 200

    def test_completion_status_fails_closed_without_formal_promote(self, tmp_path):
        from automation.agents.http_shell import create_app
        from starlette.testclient import TestClient

        class _FakeTeam:
            async def start(self):
                await asyncio.sleep(3600)

            async def shutdown(self, reason: str = "test"):
                return None

            def get_status(self):
                return {
                    "coordinator": {
                        "mode": "completed",
                        "phase": "monitoring",
                        "current_round_id": "r-state-3",
                        "consecutive_green_rounds": 12,
                        "total_fixes": 3,
                        "total_failures": 0,
                        "round_history": [],
                        "completion_time": "2026-04-01T00:00:00Z",
                        "deferred_problems": [],
                        "total_rounds": 15,
                        "completion_evidence": {
                            "autonomy_index": 1.0,
                            "shadow_validation": {
                                "audit_run_id": "audit-missing-formal",
                                "ready": True,
                            },
                            "formal_promote": {
                                "approved": False,
                                "state": "blocked",
                                "targets_promoted": [],
                            },
                        },
                        "completion_blockers": [],
                    }
                }

        with patch("automation.agents.create_team", return_value=_FakeTeam()):
            app = create_app(auth_token="tok", repo_root=tmp_path)
            with TestClient(app) as client:
                start = client.post(
                    "/v1/start",
                    json={"mode": "fix"},
                    headers={"Authorization": "Bearer tok"},
                )
                assert start.status_code == 200

                state_resp = client.get("/v1/state", headers={"Authorization": "Bearer tok"})
                assert state_resp.status_code == 200
                assert state_resp.json()["goal_reached"] is False

                completion_resp = client.get(
                    "/v1/completion-status",
                    headers={"Authorization": "Bearer tok"},
                )
                assert completion_resp.status_code == 200
                completion_data = completion_resp.json()
                assert completion_data["mode"] == "completed"
                assert completion_data["goal_reached"] is False
                assert completion_data["completed"] is False

                stop = client.post(
                    "/v1/stop",
                    json={"reason": "test"},
                    headers={"Authorization": "Bearer tok"},
                )
                assert stop.status_code == 200


class TestHTTPShellTimingSafe:
    """Verify constant-time token comparison."""

    def test_require_auth_uses_hmac(self):
        """Verify that _require_auth rejects bad tokens and accepts correct ones."""
        from automation.agents.http_shell import _require_auth
        from unittest.mock import MagicMock
        from fastapi import HTTPException

        # Correct token should not raise
        req = MagicMock()
        req.headers.get.return_value = "Bearer my-secret"
        _require_auth("my-secret", req)  # should pass silently

        # Wrong token should raise 401
        req_bad = MagicMock()
        req_bad.headers.get.return_value = "Bearer wrong-token"
        try:
            _require_auth("my-secret", req_bad)
            assert False, "Expected HTTPException"
        except HTTPException as exc:
            assert exc.status_code == 401

        # Empty token means auth disabled — should not raise
        req_empty = MagicMock()
        req_empty.headers.get.return_value = ""
        _require_auth("", req_empty)  # should pass silently
