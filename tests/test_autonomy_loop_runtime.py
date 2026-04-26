from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from fastapi.testclient import TestClient

import app.api.routes_internal as routes_internal
import app.main as app_main
import app.services.autonomy_loop_runtime as runtime_mod
from app.services.codex_client import CodexProviderSpec
from automation.loop_controller.controller import LoopControllerConfig
from automation.loop_controller.schemas import LoopMode, LoopPhase, LoopState, RoundSummary, VerifyResult


class FakeController:
    def __init__(self, state: LoopState | None = None) -> None:
        self.running = False
        self.state = state or LoopState()
        self.start_calls: list[tuple[LoopMode, int | None]] = []
        self.stop_calls: list[str] = []
        self.force_calls: list[tuple[LoopMode, int | None]] = []

    def start(self, mode: LoopMode = LoopMode.FIX, fix_goal: int | None = None) -> LoopState:
        self.running = True
        self.start_calls.append((mode, fix_goal))
        self.state.mode = mode
        self.state.phase = LoopPhase.AUDITING if mode == LoopMode.FIX else LoopPhase.MONITORING
        self.state.fix_goal = fix_goal or self.state.fix_goal
        return self.get_state()

    def stop(self, reason: str = "manual") -> LoopState:
        self.running = False
        self.stop_calls.append(reason)
        self.state.phase = LoopPhase.IDLE
        return self.get_state()

    def get_state(self) -> LoopState:
        return self.state.model_copy(deep=True)

    def force_new_round(self, mode: LoopMode = LoopMode.FIX, fix_goal: int | None = None) -> None:
        self.force_calls.append((mode, fix_goal))
        self.state.mode = mode
        if fix_goal is not None:
            self.state.fix_goal = fix_goal


class FakeRuntimeService:
    def __init__(self) -> None:
        self.ensure_calls: list[str] = []
        self.shutdown_calls: list[str] = []
        self.start_calls: list[dict[str, object]] = []
        self.stop_calls: list[str] = []
        self.force_calls: list[dict[str, object]] = []
        self.await_calls: list[int] = []
        self.running = True
        self.await_round_result: dict[str, object] = {
            "status": "completed",
            "round_id": "round-12",
            "timed_out": False,
            "round_summary": {"round_id": "round-12", "status": "green"},
        }

    def status(self) -> dict[str, object]:
        return {
            "enabled": True,
            "initialized": True,
            "running": self.running,
            "mode": "monitor",
            "phase": "monitoring",
            "current_round_id": "round-12",
            "consecutive_fix_success_count": 12,
            "consecutive_verified_problem_fixes": 12,
            "fix_goal": 12,
            "goal_progress_count": 12,
            "success_goal_metric": "verified_problem_count",
            "goal_reached": True,
            "round_history_size": 6,
            "total_fixes": 15,
            "total_failures": 1,
            "fixed_problems": ["problem-1", "problem-2"],
            "last_round_summary": {"round_id": "round-12", "verify_all_green": True},
            "last_round_failed_reasons": ["CURRENT_LAYER_RUNTIME_GATE_BLOCKED"],
            "last_promote_reason": "CURRENT_LAYER_RUNTIME_GATE_BLOCKED",
            "last_runtime_gate_status": "blocked",
            "last_round_note": "CURRENT_LAYER_RUNTIME_GATE_BLOCKED",
            "shadow_round_history_size": 3,
            "issue_mesh_worker_cap": 12,
            "effective_fix_workers": 12,
            "effective_monitor_workers": 4,
            "lease": {"status": "owned_by_self"},
        }

    def ensure_started(self, *, reason: str = "app_startup") -> dict[str, object]:
        self.ensure_calls.append(reason)
        return {"status": "started"}

    def shutdown(self, *, reason: str = "app_shutdown") -> dict[str, object]:
        self.shutdown_calls.append(reason)
        return {"status": "stopped"}

    def start(
        self,
        *,
        mode: str | None = None,
        fix_goal: int | None = None,
        force_new_round: bool = False,
        reason: str = "manual_start",
    ) -> dict[str, object]:
        self.start_calls.append(
            {
                "mode": mode,
                "fix_goal": fix_goal,
                "force_new_round": force_new_round,
                "reason": reason,
            }
        )
        self.running = True
        return {
            **self.status(),
            "status": "started",
            "mode": mode or "fix",
            "fix_goal": fix_goal or 12,
            "force_new_round": force_new_round,
        }

    def stop(self, *, reason: str = "manual_stop") -> dict[str, object]:
        self.stop_calls.append(reason)
        self.running = False
        return {**self.status(), "status": "stopped", "reason": reason}

    def force_new_round(
        self,
        *,
        mode: str | None = None,
        fix_goal: int | None = None,
        reason: str = "force_round_api",
    ) -> dict[str, object]:
        self.force_calls.append(
            {
                "mode": mode,
                "fix_goal": fix_goal,
                "reason": reason,
            }
        )
        return {
            **self.status(),
            "status": "force_requested",
            "mode": mode or "fix",
            "fix_goal": fix_goal or 12,
            "reason": reason,
        }

    def await_round(self, timeout_seconds: int = 600) -> dict[str, object]:
        self.await_calls.append(timeout_seconds)
        return dict(self.await_round_result)


def _base_config(tmp_path: Path) -> LoopControllerConfig:
    return LoopControllerConfig(
        repo_root=tmp_path.resolve(),
        mesh_runner_url="http://mesh:8093",
        mesh_runner_token="mesh-token",
        promote_prep_url="http://prep:8094",
        promote_prep_token="prep-token",
        writeback_a_url="http://writeback-a:8092",
        writeback_a_token="wb-a-token",
        writeback_b_url="http://writeback-b:8095",
        writeback_b_token="wb-b-token",
        auth_token="loop-token",
        app_base_url="http://app:38001",
        internal_token="legacy-internal-token",
        fix_goal=10,
        audit_interval_seconds=300,
        monitor_interval_seconds=1800,
    )


def test_build_autonomy_loop_controller_config_uses_app_settings(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        runtime_mod,
        "_base_loop_controller_config",
        lambda: replace(_base_config(tmp_path), internal_token=""),
    )
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_fix_goal", 12)
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_audit_interval_seconds", 45)
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_monitor_interval_seconds", 120)
    monkeypatch.setattr(runtime_mod.settings, "internal_cron_token", "internal-from-app")
    monkeypatch.setattr(runtime_mod.settings, "internal_api_key", "")

    cfg = runtime_mod.build_autonomy_loop_controller_config()

    assert cfg.fix_goal == 12
    assert cfg.audit_interval_seconds == 45
    assert cfg.monitor_interval_seconds == 120
    assert cfg.internal_token == "internal-from-app"


def test_build_autonomy_loop_controller_config_prefers_app_token_over_control_plane_alias(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        runtime_mod,
        "_base_loop_controller_config",
        lambda: replace(_base_config(tmp_path), internal_token="control-plane-token"),
    )
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_fix_goal", 12)
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_audit_interval_seconds", 45)
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_monitor_interval_seconds", 120)
    monkeypatch.setattr(runtime_mod.settings, "internal_cron_token", "internal-from-app")
    monkeypatch.setattr(runtime_mod.settings, "internal_api_key", "")

    cfg = runtime_mod.build_autonomy_loop_controller_config()

    assert cfg.internal_token == "internal-from-app"


def test_build_autonomy_loop_controller_config_uses_control_plane_token_when_app_token_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        runtime_mod,
        "_base_loop_controller_config",
        lambda: replace(_base_config(tmp_path), internal_token="control-plane-token"),
    )
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_fix_goal", 12)
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_audit_interval_seconds", 45)
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_monitor_interval_seconds", 120)
    monkeypatch.setattr(runtime_mod.settings, "internal_cron_token", "")
    monkeypatch.setattr(runtime_mod.settings, "internal_api_key", "")

    cfg = runtime_mod.build_autonomy_loop_controller_config()

    assert cfg.internal_token == "control-plane-token"


def test_build_autonomy_loop_controller_config_discovers_new_api_gateway_when_explicit_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        runtime_mod,
        "_base_loop_controller_config",
        lambda: replace(
            _base_config(tmp_path),
            new_api_base_url="",
            new_api_token="",
        ),
    )
    monkeypatch.setattr(runtime_mod.settings, "codex_api_base_url", "")
    monkeypatch.setattr(runtime_mod.settings, "codex_api_key", "")
    monkeypatch.setattr(
        runtime_mod,
        "discover_audit_codex_provider_specs",
        lambda: [
            CodexProviderSpec(
                provider_name="newapi-192.168.232.141-3000",
                base_url="http://192.168.232.141:3000/v1",
                api_key="sk-gateway",
                model="gpt-5.4",
            )
        ],
    )

    cfg = runtime_mod.build_autonomy_loop_controller_config()

    assert cfg.new_api_base_url == "http://192.168.232.141:3000"
    assert cfg.new_api_token == "sk-gateway"


def test_autonomy_loop_runtime_blocks_fresh_foreign_lease(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(runtime_mod, "_base_loop_controller_config", lambda: _base_config(tmp_path))
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_fix_goal", 12)
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_lease_seconds", 300)
    fake = FakeController()
    runtime = runtime_mod.AutonomyLoopRuntime(controller_factory=lambda cfg: fake)

    lease_path = tmp_path / "runtime" / "loop_controller" / "runtime_lease.json"
    lease_path.parent.mkdir(parents=True, exist_ok=True)
    lease_path.write_text(
        json.dumps(
            {
                "owner_id": "foreign-owner",
                "heartbeat_at": runtime_mod._now_utc().isoformat(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload = runtime.start(reason="test")

    assert payload["status"] == "lease_conflict"
    assert fake.start_calls == []


def test_autonomy_loop_runtime_can_take_over_stale_lease_and_release_on_stop(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(runtime_mod, "_base_loop_controller_config", lambda: _base_config(tmp_path))
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_fix_goal", 12)
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_lease_seconds", 30)
    fake = FakeController()
    runtime = runtime_mod.AutonomyLoopRuntime(controller_factory=lambda cfg: fake)

    lease_path = tmp_path / "runtime" / "loop_controller" / "runtime_lease.json"
    lease_path.parent.mkdir(parents=True, exist_ok=True)
    lease_path.write_text(
        json.dumps(
            {
                "owner_id": "stale-owner",
                "heartbeat_at": "2026-03-01T00:00:00+00:00",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    started = runtime.start(reason="test-start")
    stopped = runtime.stop(reason="test-stop")

    assert started["status"] == "started"
    assert fake.start_calls == [(LoopMode.FIX, 12)]
    assert stopped["status"] == "stopped"
    assert fake.stop_calls == ["test-stop"]
    assert not lease_path.exists()


def test_autonomy_loop_runtime_force_new_round_when_already_running(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(runtime_mod, "_base_loop_controller_config", lambda: _base_config(tmp_path))
    monkeypatch.setattr(runtime_mod.settings, "autonomy_loop_fix_goal", 12)
    fake = FakeController()
    runtime = runtime_mod.AutonomyLoopRuntime(controller_factory=lambda cfg: fake)

    first = runtime.start(reason="initial-start")
    second = runtime.start(mode="monitor", fix_goal=15, force_new_round=True, reason="manual-force")

    assert first["status"] == "started"
    assert second["status"] == "force_requested"
    assert fake.force_calls == [(LoopMode.MONITOR, 15)]
    runtime.stop(reason="cleanup")


def test_autonomy_loop_runtime_status_reports_goal_reached_after_twelve_rounds(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(runtime_mod, "_base_loop_controller_config", lambda: _base_config(tmp_path))
    state = LoopState(
        mode=LoopMode.MONITOR,
        phase=LoopPhase.MONITORING,
        consecutive_fix_success_count=12,
        consecutive_verified_problem_fixes=12,
        fix_goal=12,
        total_fixes=12,
        last_promote_round_id="promote-round-12",
    )
    fake = FakeController(state=state)
    fake.running = True
    runtime = runtime_mod.AutonomyLoopRuntime(controller_factory=lambda cfg: fake)
    runtime._controller = fake

    payload = runtime.status()

    assert payload["running"] is True
    assert payload["mode"] == "monitor"
    assert payload["phase"] == "monitoring"
    assert payload["goal_reached"] is True


def test_autonomy_loop_runtime_status_uses_verified_metric_for_goal(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(runtime_mod, "_base_loop_controller_config", lambda: _base_config(tmp_path))
    state = LoopState(
        mode=LoopMode.FIX,
        phase=LoopPhase.IDLE,
        consecutive_fix_success_count=12,
        consecutive_verified_problem_fixes=4,
        fix_goal=10,
        success_goal_metric="verified_problem_count",
    )
    fake = FakeController(state=state)
    fake.running = True
    runtime = runtime_mod.AutonomyLoopRuntime(controller_factory=lambda cfg: fake)
    runtime._controller = fake

    payload = runtime.status()

    assert payload["goal_progress_count"] == 4
    assert payload["success_goal_metric"] == "verified_problem_count"
    assert payload["goal_reached"] is False


def test_autonomy_loop_runtime_status_reads_persisted_state_when_controller_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(runtime_mod, "_base_loop_controller_config", lambda: _base_config(tmp_path))
    persisted_state = LoopState(
        mode=LoopMode.MONITOR,
        phase=LoopPhase.MONITORING,
        consecutive_fix_success_count=7,
        consecutive_verified_problem_fixes=7,
        fix_goal=7,
        goal_ever_reached=True,
        total_fixes=9,
        total_failures=2,
        last_audit_run_id="audit-007",
        last_fix_wave_id="fix-wave-007",
        last_promote_round_id="promote-round-007",
        provider_pool={"status": "ok", "ready": True},
    )
    state_path = tmp_path / "runtime" / "loop_controller" / "state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(persisted_state.model_dump(mode="json"), ensure_ascii=False),
        encoding="utf-8",
    )

    runtime = runtime_mod.AutonomyLoopRuntime()

    payload = runtime.status()

    assert payload["initialized"] is False
    assert payload["running"] is False
    assert payload["mode"] == "monitor"
    assert payload["phase"] == "monitoring"
    assert payload["goal_progress_count"] == 7
    assert payload["goal_reached"] is True
    assert payload["goal_ever_reached"] is True
    assert payload["total_fixes"] == 9
    assert payload["last_audit_run_id"] == "audit-007"
    assert payload["provider_pool"]["status"] == "ok"


def test_autonomy_loop_runtime_status_projects_last_round_summary_and_worker_snapshot(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(runtime_mod, "_base_loop_controller_config", lambda: _base_config(tmp_path))
    monkeypatch.setenv("ISSUE_MESH_MAX_WORKERS_CAP", "12")
    monkeypatch.setenv("ISSUE_MESH_READONLY_MAX_WORKERS", "9")
    monkeypatch.setenv("AUTONOMY_LOOP_MAX_WORKERS_FIX", "10")
    monkeypatch.setenv("AUTONOMY_LOOP_MAX_WORKERS_MONITOR", "7")
    state = LoopState(
        mode=LoopMode.FIX,
        phase=LoopPhase.PROMOTING,
        consecutive_fix_success_count=5,
        consecutive_verified_problem_fixes=5,
        fix_goal=10,
        round_history=[
            RoundSummary(
                round_id="round-009",
                started_at="2026-04-01T00:00:00+00:00",
                finished_at="2026-04-01T00:05:00+00:00",
                phase_reached=LoopPhase.PROMOTING,
                mode=LoopMode.FIX,
                audit_run_id="audit-009",
                problems_found=3,
                problems_fixed=2,
                problems_failed=1,
                problems_skipped=0,
                all_success=False,
                error="PROMOTE_GATE_BLOCKED",
                verify_result=VerifyResult(
                    full_pytest_total=1402,
                    full_pytest_failed=0,
                    all_green=True,
                ),
            )
        ],
    )
    fake = FakeController(state=state)
    fake.running = True
    runtime = runtime_mod.AutonomyLoopRuntime(controller_factory=lambda cfg: fake)
    runtime._controller = fake

    payload = runtime.status()

    assert payload["last_round_summary"]["round_id"] == "round-009"
    assert payload["last_round_summary"]["verify_all_green"] is True
    assert payload["issue_mesh_worker_cap"] == 9
    assert payload["effective_fix_workers"] == 9
    assert payload["effective_monitor_workers"] == 7


def test_autonomy_loop_runtime_status_projects_shadow_diagnostics(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(runtime_mod, "_base_loop_controller_config", lambda: _base_config(tmp_path))
    shadow_state_path = tmp_path / "runtime" / "autonomous_fix_loop" / "state.json"
    shadow_state_path.parent.mkdir(parents=True, exist_ok=True)
    shadow_state_path.write_text(
        json.dumps(
            {
                "round_history": [
                    {
                        "round_id": "autofix-20260401-001",
                        "failed_reasons": ["CURRENT_LAYER_RUNTIME_GATE_BLOCKED", "SHARED_ARTIFACTS_NOT_ALIGNED"],
                        "promote_reason": "CURRENT_LAYER_RUNTIME_GATE_BLOCKED",
                        "runtime_gate_status": "blocked",
                        "note": "CURRENT_LAYER_RUNTIME_GATE_BLOCKED;SHARED_ARTIFACTS_NOT_ALIGNED",
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    runtime = runtime_mod.AutonomyLoopRuntime()

    payload = runtime.status()

    assert payload["last_round_failed_reasons"] == [
        "CURRENT_LAYER_RUNTIME_GATE_BLOCKED",
        "SHARED_ARTIFACTS_NOT_ALIGNED",
    ]
    assert payload["last_promote_reason"] == "CURRENT_LAYER_RUNTIME_GATE_BLOCKED"
    assert payload["last_runtime_gate_status"] == "blocked"
    assert payload["last_round_note"] == "CURRENT_LAYER_RUNTIME_GATE_BLOCKED;SHARED_ARTIFACTS_NOT_ALIGNED"
    assert payload["shadow_round_history_size"] == 1


def test_autonomy_loop_runtime_status_uses_control_plane_promote_mode_when_state_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(runtime_mod, "_base_loop_controller_config", lambda: _base_config(tmp_path))
    control_plane = tmp_path / "automation" / "control_plane"
    control_plane.mkdir(parents=True, exist_ok=True)
    (control_plane / "current_state.json").write_text(
        json.dumps({"promote_target_mode": "doc22"}, ensure_ascii=False),
        encoding="utf-8",
    )

    runtime = runtime_mod.AutonomyLoopRuntime()

    payload = runtime.status()

    assert payload["promote_target_mode"] == "doc22"


def test_autonomy_loop_runtime_lease_snapshot_includes_verified_goal_fields(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(runtime_mod, "_base_loop_controller_config", lambda: _base_config(tmp_path))
    state = LoopState(
        mode=LoopMode.SAFE_HOLD,
        phase=LoopPhase.BLOCKED,
        consecutive_fix_success_count=9,
        consecutive_verified_problem_fixes=4,
        fix_goal=10,
        success_goal_metric="verified_problem_count",
        blocked_reason="PROVIDER_NOT_READY",
        provider_pool={"status": "provider_safe_hold", "ready": False},
    )
    fake = FakeController(state=state)
    runtime = runtime_mod.AutonomyLoopRuntime(controller_factory=lambda cfg: fake)

    snapshot = runtime._write_lease_snapshot(runtime_mod.build_autonomy_loop_controller_config(), state=state, running=True)

    assert snapshot["consecutive_verified_problem_fixes"] == 4
    assert snapshot["success_goal_metric"] == "verified_problem_count"
    assert snapshot["goal_progress_count"] == 4
    assert snapshot["goal_reached"] is False
    assert snapshot["blocked_reason"] == "PROVIDER_NOT_READY"


def test_internal_autonomy_loop_routes_require_internal_token(client) -> None:
    response = client.get("/api/v1/internal/autonomy/loop")

    assert response.status_code == 401
    assert response.json()["error_code"] == "UNAUTHORIZED"


def test_internal_autonomy_loop_routes_proxy_runtime_service(client, internal_headers, monkeypatch) -> None:
    fake_runtime = FakeRuntimeService()
    monkeypatch.setattr(routes_internal, "get_autonomy_loop_runtime", lambda: fake_runtime)

    status_resp = client.get(
        "/api/v1/internal/autonomy/loop",
        headers=internal_headers("internal-autonomy-token"),
    )
    start_resp = client.post(
        "/api/v1/internal/autonomy/loop/start",
        json={"mode": "fix", "fix_goal": 12, "force_new_round": True},
        headers=internal_headers("internal-autonomy-token"),
    )
    stop_resp = client.post(
        "/api/v1/internal/autonomy/loop/stop",
        json={"reason": "manual-stop"},
        headers=internal_headers("internal-autonomy-token"),
    )

    assert status_resp.status_code == 200
    assert status_resp.json()["data"]["goal_reached"] is True
    assert status_resp.json()["data"]["last_round_failed_reasons"] == ["CURRENT_LAYER_RUNTIME_GATE_BLOCKED"]
    assert start_resp.status_code == 200
    assert start_resp.json()["data"]["status"] == "started"
    assert fake_runtime.start_calls[0]["force_new_round"] is True
    assert stop_resp.status_code == 200
    assert stop_resp.json()["data"]["status"] == "stopped"
    assert fake_runtime.stop_calls == ["manual-stop"]


def test_internal_legacy_fix_loop_routes_proxy_formal_runtime(client, internal_headers, monkeypatch) -> None:
    fake_runtime = FakeRuntimeService()
    monkeypatch.setattr(routes_internal, "get_autonomy_loop_runtime", lambda: fake_runtime)

    state_resp = client.get(
        "/api/v1/internal/automation/fix-loop/state",
        headers=internal_headers("internal-autonomy-token"),
    )
    start_resp = client.post(
        "/api/v1/internal/automation/fix-loop/start",
        json={"mode": "monitor", "success_round_goal": 9, "force_new_round": True},
        headers=internal_headers("internal-autonomy-token"),
    )
    force_resp = client.post(
        "/api/v1/internal/automation/fix-loop/force-round",
        json={"mode": "fix", "success_round_goal": 11},
        headers=internal_headers("internal-autonomy-token"),
    )
    await_resp = client.get(
        "/api/v1/internal/automation/fix-loop/await-round?timeout_seconds=77",
        headers=internal_headers("internal-autonomy-token"),
    )
    stop_resp = client.post(
        "/api/v1/internal/automation/fix-loop/stop",
        json={"reason": "legacy-stop"},
        headers=internal_headers("internal-autonomy-token"),
    )

    assert state_resp.status_code == 200
    state_data = state_resp.json()["data"]
    assert state_data["success_round_streak"] == state_data["goal_progress_count"] == 12
    assert state_data["success_round_goal"] == state_data["fix_goal"] == 12
    assert state_data["total_rounds"] == state_data["round_history_size"] == 6
    assert state_data["round_seq"] == 6
    assert state_data["fixed_problem_ids"] == ["problem-1", "problem-2"]
    assert state_data["last_round_failed_reasons"] == ["CURRENT_LAYER_RUNTIME_GATE_BLOCKED"]

    assert start_resp.status_code == 200
    assert start_resp.json()["data"]["status"] == "started"
    assert start_resp.json()["data"]["success_round_goal"] == 9
    assert fake_runtime.start_calls == [
        {
            "mode": "monitor",
            "fix_goal": 9,
            "force_new_round": True,
            "reason": "legacy_internal_api_start",
        }
    ]

    assert force_resp.status_code == 200
    assert force_resp.json()["data"]["status"] == "force_requested"
    assert force_resp.json()["data"]["success_round_goal"] == 11
    assert fake_runtime.force_calls == [
        {
            "mode": "fix",
            "fix_goal": 11,
            "reason": "legacy_internal_api_force_round",
        }
    ]

    assert await_resp.status_code == 200
    assert await_resp.json()["data"]["round_id"] == "round-12"
    assert fake_runtime.await_calls == [77]

    assert stop_resp.status_code == 200
    assert stop_resp.json()["data"]["status"] == "stopped"
    assert stop_resp.json()["data"]["success_round_goal"] == 12
    assert stop_resp.json()["data"]["running"] is False
    assert fake_runtime.stop_calls == ["legacy-stop"]


def test_internal_autonomy_loop_state_alias_returns_same_as_status(client, internal_headers, monkeypatch) -> None:
    """GET /autonomy/loop/state should return the same envelope as GET /autonomy/loop."""
    fake_runtime = FakeRuntimeService()
    monkeypatch.setattr(routes_internal, "get_autonomy_loop_runtime", lambda: fake_runtime)

    status_resp = client.get(
        "/api/v1/internal/autonomy/loop",
        headers=internal_headers("internal-autonomy-token"),
    )
    state_resp = client.get(
        "/api/v1/internal/autonomy/loop/state",
        headers=internal_headers("internal-autonomy-token"),
    )

    assert state_resp.status_code == 200
    # Both must use envelope format {data: {...}}
    assert "data" in state_resp.json()
    assert "data" in status_resp.json()
    # Compare data payloads (ignoring request_id which is per-call unique)
    state_data = {k: v for k, v in state_resp.json()["data"].items() if k != "request_id"}
    status_data = {k: v for k, v in status_resp.json()["data"].items() if k != "request_id"}
    assert state_data == status_data
    assert state_resp.json()["data"]["goal_reached"] is True


def test_internal_autonomy_loop_await_round_uses_timeout_seconds_param(client, internal_headers, monkeypatch) -> None:
    """Verify await-round accepts timeout_seconds query parameter (not timeout)."""
    fake_runtime = FakeRuntimeService()
    fake_runtime.await_round_result = {"status": "completed", "round_id": "test-round", "timed_out": False}

    def fake_await_round(timeout_seconds: int = 600):
        return fake_runtime.await_round_result

    fake_runtime.await_round = fake_await_round
    monkeypatch.setattr(routes_internal, "get_autonomy_loop_runtime", lambda: fake_runtime)

    resp = client.get(
        "/api/v1/internal/autonomy/loop/await-round?timeout_seconds=120",
        headers=internal_headers("internal-autonomy-token"),
    )

    assert resp.status_code == 200
    assert "data" in resp.json()


def test_all_autonomy_responses_use_envelope_format(client, internal_headers, monkeypatch) -> None:
    """All autonomy loop endpoints must return {data: {...}} envelope."""
    fake_runtime = FakeRuntimeService()
    monkeypatch.setattr(routes_internal, "get_autonomy_loop_runtime", lambda: fake_runtime)

    endpoints = [
        ("GET", "/api/v1/internal/autonomy/loop"),
        ("GET", "/api/v1/internal/autonomy/loop/state"),
    ]
    for method, url in endpoints:
        if method == "GET":
            resp = client.get(url, headers=internal_headers("internal-autonomy-token"))
        else:
            resp = client.post(url, json={}, headers=internal_headers("internal-autonomy-token"))
        assert resp.status_code == 200, f"{method} {url} returned {resp.status_code}"
        body = resp.json()
        assert "data" in body, f"{method} {url} missing envelope 'data' key"


def test_app_lifespan_starts_and_stops_autonomy_runtime(monkeypatch) -> None:
    fake_runtime = FakeRuntimeService()
    monkeypatch.setattr(app_main, "get_autonomy_loop_runtime", lambda: fake_runtime)
    monkeypatch.setattr(app_main.settings, "autonomy_loop_enabled", True)

    with TestClient(app_main.app, base_url="http://localhost") as client:
        response = client.get("/health")
        assert response.status_code == 200

    # Kestra is the sole scheduler; app lifespan no longer calls ensure_started.
    assert fake_runtime.ensure_calls == []
    assert fake_runtime.shutdown_calls == ["app_shutdown"]
