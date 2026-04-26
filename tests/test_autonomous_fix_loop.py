from __future__ import annotations

from pathlib import Path
from typing import Any

from app.core.config import settings
from app.services.autonomous_fix_loop import (
    AutonomousFixLoop,
    AutonomousFixLoopConfig,
    build_default_autonomous_fix_loop_service,
)
from app.services.autonomous_fix_state import (
    AutonomousFixState,
    AutonomousFixStateStore,
    LoopMode,
)


class FakeGateway:
    def __init__(self) -> None:
        self.audit_payloads: list[dict[str, Any]] = []
        self.runtime_context: dict[str, Any] = {
            "runtime_gates": {
                "status": "ready",
                "shared_artifact_promote": {
                    "allowed": True,
                    "artifact_files_present": True,
                    "catalog_fresh": True,
                    "junit_clean": True,
                    "blind_spot_clean": True,
                    "continuous_audit_complete": True,
                    "artifacts_same_round": True,
                },
            }
        }
        self.last_apply_coordinator = None
        self.last_apply_lease = None
        self.promote_results: list[dict[str, Any]] = []
        self.promote_calls: list[dict[str, Any]] = []

    def push_audit(self, payload: dict[str, Any]) -> None:
        self.audit_payloads.append(payload)

    def push_promote_result(self, payload: dict[str, Any]) -> None:
        self.promote_results.append(payload)

    def run_audit(self, *, mode: str, max_workers: int) -> dict[str, Any]:
        del mode, max_workers
        if self.audit_payloads:
            return self.audit_payloads.pop(0)
        return {"audit_run_id": "issue-mesh-default", "bundle": {"findings": []}, "artifact_fingerprints": {}}

    def get_runtime_context(self) -> dict[str, Any]:
        return dict(self.runtime_context)

    def apply_fixes(
        self,
        *,
        problems: list[Any],
        round_id: str,
        audit_run_id: str,
        runtime_context: dict[str, Any],
        coordinator: Any | None = None,
        lease: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        del round_id, audit_run_id, runtime_context
        self.last_apply_coordinator = coordinator
        self.last_apply_lease = lease
        return [
            {
                "problem_id": problem.problem_id,
                "outcome": "success",
                "patches_applied": [f"app/services/{problem.family}.py"],
            }
            for problem in problems
        ]

    def verify_round(self, *, round_id: str, changed_files: list[str]) -> dict[str, Any]:
        del round_id, changed_files
        return {"all_green": True}

    def promote_round(
        self,
        *,
        round_id: str,
        audit_run_id: str,
        runtime_context: dict[str, Any],
        coordinator: Any | None = None,
        lease_seconds: int | None = None,
    ) -> dict[str, Any]:
        self.promote_calls.append(
            {
                "round_id": round_id,
                "audit_run_id": audit_run_id,
                "runtime_context": runtime_context,
                "coordinator": coordinator,
                "lease_seconds": lease_seconds,
            }
        )
        if self.promote_results:
            return self.promote_results.pop(0)
        return {"promoted": True, "reason": "READY"}


class FakeCoordinator:
    def __init__(self) -> None:
        self.claim_calls: list[dict[str, Any]] = []
        self.release_calls: list[dict[str, Any]] = []
        self.refresh_calls: list[dict[str, Any]] = []

    def claim(self, *, round_id: str, target_paths: list[str], lease_seconds: int) -> dict[str, Any]:
        record = {
            "round_id": round_id,
            "target_paths": list(target_paths),
            "lease_seconds": lease_seconds,
        }
        self.claim_calls.append(record)
        return {"lease_id": f"lease-{round_id}", "round_id": round_id, "fencing_token": 1}

    def refresh(self, *, lease: dict[str, Any]) -> dict[str, Any] | None:
        self.refresh_calls.append(dict(lease))
        return dict(lease)

    def release(self, *, lease: dict[str, Any], reason: str) -> None:
        self.release_calls.append({"lease": dict(lease), "reason": reason})


def _store(path: Path) -> AutonomousFixStateStore:
    return AutonomousFixStateStore(path)


def test_autonomous_fix_state_persistence_roundtrip(tmp_path: Path) -> None:
    store = _store(tmp_path / "runtime" / "autonomous_fix_loop" / "state.json")
    initial = store.load()
    assert initial.mode == LoopMode.FIX
    assert initial.success_round_streak == 0

    saved = store.update(
        mode=LoopMode.MONITOR,
        success_round_streak=7,
        total_rounds=9,
    )
    assert saved.mode == LoopMode.MONITOR
    assert saved.success_round_streak == 7

    reloaded = _store(store.path).load()
    assert reloaded.mode == LoopMode.MONITOR
    assert reloaded.success_round_streak == 7
    assert reloaded.total_rounds == 9


def test_autonomous_fix_loop_config_prefers_formal_autonomy_env_names(monkeypatch) -> None:
    monkeypatch.setenv("AUTONOMY_LOOP_FIX_GOAL", "14")
    monkeypatch.setenv("AUTONOMY_LOOP_AUDIT_INTERVAL_SECONDS", "45")
    monkeypatch.setenv("AUTONOMY_LOOP_MONITOR_INTERVAL_SECONDS", "600")
    monkeypatch.setenv("AUTONOMY_LOOP_ENABLED", "true")
    monkeypatch.setenv("AUTONOMOUS_FIX_LOOP_SUCCESS_ROUND_GOAL", "9")
    monkeypatch.setenv("AUTONOMOUS_FIX_LOOP_FIX_INTERVAL_SECONDS", "99")
    monkeypatch.setenv("AUTONOMOUS_FIX_LOOP_MONITOR_INTERVAL_SECONDS", "1800")
    monkeypatch.setenv("AUTONOMOUS_FIX_LOOP_ENABLED", "false")

    cfg = AutonomousFixLoopConfig.from_env()

    assert cfg.success_round_goal == 14
    assert cfg.fix_interval_seconds == 45
    assert cfg.monitor_interval_seconds == 600
    assert cfg.enabled is True


def test_autonomous_fix_loop_config_caps_workers_to_issue_mesh_limit(monkeypatch) -> None:
    monkeypatch.setenv("ISSUE_MESH_MAX_WORKERS_CAP", "8")
    monkeypatch.setenv("ISSUE_MESH_READONLY_MAX_WORKERS", "12")
    monkeypatch.setenv("AUTONOMY_LOOP_MAX_WORKERS_FIX", "10")
    monkeypatch.setenv("AUTONOMY_LOOP_MAX_WORKERS_MONITOR", "7")

    cfg = AutonomousFixLoopConfig.from_env()

    assert cfg.max_workers_fix == 8
    assert cfg.max_workers_monitor == 7


def test_autonomous_fix_loop_reaches_monitor_after_12_success_rounds(tmp_path: Path) -> None:
    store = _store(tmp_path / "runtime" / "autonomous_fix_loop" / "state.json")
    # Seed state with fix_success_count metric so empty green rounds count
    seeded = AutonomousFixState(success_goal_metric="fix_success_count")
    store.save(seeded)
    gateway = FakeGateway()
    # Provide real findings so rounds are not no-ops
    for i in range(12):
        gateway.push_audit({
            "audit_run_id": f"issue-mesh-{i:03d}",
            "bundle": {"findings": [{"family": "test_gap", "finding_id": f"tg-{i}", "title": f"gap {i}"}]},
            "artifact_fingerprints": {},
        })
    loop = AutonomousFixLoop(
        store=store,
        gateway=gateway,
        coordinator=FakeCoordinator(),
        config=AutonomousFixLoopConfig(success_round_goal=12),
    )

    final_state = loop.run_rounds(12)

    assert final_state.total_rounds == 12
    assert final_state.success_round_streak == 12
    assert final_state.mode == LoopMode.MONITOR
    assert final_state.goal_ever_reached is True
    assert len(final_state.round_history) == 12

    reloaded = _store(store.path).load()
    assert reloaded.mode == LoopMode.MONITOR
    assert reloaded.success_round_streak == 12
    assert len(reloaded.round_history) == 12


def test_monitor_mode_reenters_fix_on_artifact_drift(tmp_path: Path) -> None:
    store = _store(tmp_path / "runtime" / "autonomous_fix_loop" / "state.json")
    seeded = AutonomousFixState(
        mode=LoopMode.MONITOR,
        success_round_goal=20,
        success_round_streak=12,
        last_artifact_fingerprints={"output/junit.xml": "fp-old"},
    )
    store.save(seeded)

    gateway = FakeGateway()
    gateway.push_audit(
        {
            "audit_run_id": "issue-mesh-drift-001",
            "bundle": {"findings": []},
            "artifact_fingerprints": {"output/junit.xml": "fp-new"},
        }
    )

    loop = AutonomousFixLoop(
        store=store,
        gateway=gateway,
        config=AutonomousFixLoopConfig(success_round_goal=20),
    )
    state = loop.run_single_round()

    assert state.mode == LoopMode.FIX
    assert state.round_history[-1].reentered_fix is True
    assert state.round_history[-1].drift_paths == ["output/junit.xml"]


def test_monitor_mode_reenters_fix_on_regression(tmp_path: Path) -> None:
    store = _store(tmp_path / "runtime" / "autonomous_fix_loop" / "state.json")
    seeded = AutonomousFixState(
        mode=LoopMode.MONITOR,
        success_round_goal=20,
        fixed_problem_ids=["truth-lineage:TL-001"],
    )
    store.save(seeded)

    gateway = FakeGateway()
    gateway.push_audit(
        {
            "audit_run_id": "issue-mesh-reg-001",
            "bundle": {
                "findings": [
                    {
                        "family": "truth-lineage",
                        "finding_id": "TL-001",
                        "title": "regression reappeared",
                    }
                ]
            },
            "artifact_fingerprints": {},
        }
    )
    loop = AutonomousFixLoop(
        store=store,
        gateway=gateway,
        coordinator=FakeCoordinator(),
        config=AutonomousFixLoopConfig(success_round_goal=20),
    )

    state = loop.run_single_round()

    assert state.mode == LoopMode.FIX
    assert state.round_history[-1].reentered_fix is True
    assert state.round_history[-1].regression_count == 1
    assert state.total_fixes == 1


def test_autonomous_fix_loop_can_work_with_writeback_coordinator(tmp_path: Path) -> None:
    store = _store(tmp_path / "runtime" / "autonomous_fix_loop" / "state.json")
    gateway = FakeGateway()
    gateway.push_audit(
        {
            "audit_run_id": "issue-mesh-coord-001",
            "bundle": {
                "findings": [
                    {
                        "family": "truth-lineage",
                        "finding_id": "TL-100",
                        "title": "lineage fix needed",
                    }
                ]
            },
            "artifact_fingerprints": {},
        }
    )
    coordinator = FakeCoordinator()
    loop = AutonomousFixLoop(
        store=store,
        gateway=gateway,
        coordinator=coordinator,
        config=AutonomousFixLoopConfig(success_round_goal=10, lease_seconds=90),
    )

    state = loop.run_single_round()

    assert state.total_rounds == 1
    assert len(coordinator.claim_calls) == 1
    assert coordinator.claim_calls[0]["lease_seconds"] == 90
    assert coordinator.claim_calls[0]["target_paths"] == ["app/governance", "app/services"]
    assert gateway.last_apply_coordinator is coordinator
    assert gateway.last_apply_lease is not None
    assert gateway.last_apply_lease["fencing_token"] == 1
    assert len(coordinator.release_calls) == 1


def test_autonomous_fix_loop_records_runtime_gate_failures(tmp_path: Path) -> None:
    store = _store(tmp_path / "runtime" / "autonomous_fix_loop" / "state.json")
    gateway = FakeGateway()
    gateway.runtime_context = {
        "runtime_gates": {
            "status": "blocked",
            "shared_artifact_promote": {
                "allowed": False,
                "artifact_files_present": True,
                "catalog_fresh": False,
                "junit_clean": True,
                "blind_spot_clean": True,
                "continuous_audit_complete": True,
                "artifacts_same_round": False,
            },
        }
    }
    gateway.push_promote_result({"promoted": False, "reason": "CURRENT_LAYER_RUNTIME_GATE_BLOCKED"})
    gateway.push_audit(
        {
            "audit_run_id": "issue-mesh-gate-001",
            "bundle": {
                "findings": [
                    {
                        "family": "truth-lineage",
                        "finding_id": "TL-102",
                        "title": "lineage fix needed",
                    }
                ]
            },
            "artifact_fingerprints": {},
        }
    )

    loop = AutonomousFixLoop(
        store=store,
        gateway=gateway,
        coordinator=FakeCoordinator(),
        config=AutonomousFixLoopConfig(success_round_goal=10),
    )

    state = loop.run_single_round()
    latest = state.round_history[-1]

    assert latest.green_round is False
    assert latest.promote_gate_passed is False
    assert latest.artifacts_aligned is False
    assert latest.runtime_gate_status == "blocked"
    assert "CURRENT_LAYER_RUNTIME_GATE_BLOCKED" in latest.failed_reasons
    assert "SHARED_ARTIFACTS_NOT_ALIGNED" in latest.failed_reasons
    assert "RUNTIME_GATES_BLOCKED" in latest.failed_reasons
    assert latest.note is not None


def test_autonomous_fix_loop_retries_transient_promote_failures(tmp_path: Path) -> None:
    store = _store(tmp_path / "runtime" / "autonomous_fix_loop" / "state.json")
    gateway = FakeGateway()
    gateway.push_promote_result({"promoted": False, "reason": "PREVIEW_B_FAILED:current-layer:503"})
    gateway.push_promote_result({"promoted": True, "reason": "READY"})
    gateway.push_audit(
        {
            "audit_run_id": "issue-mesh-promote-retry-001",
            "bundle": {
                "findings": [
                    {
                        "family": "truth-lineage",
                        "finding_id": "TL-103",
                        "title": "lineage fix needed",
                    }
                ]
            },
            "artifact_fingerprints": {},
        }
    )

    loop = AutonomousFixLoop(
        store=store,
        gateway=gateway,
        coordinator=FakeCoordinator(),
        config=AutonomousFixLoopConfig(success_round_goal=10),
    )

    state = loop.run_single_round()
    latest = state.round_history[-1]

    assert latest.green_round is True
    assert latest.promoted is True
    assert latest.promote_reason == "READY"
    assert len(gateway.promote_calls) == 2
    assert gateway.promote_calls[0]["coordinator"] is not None
    assert gateway.promote_calls[0]["lease_seconds"] == 120


def test_autonomous_fix_loop_fails_closed_without_fencing_token(tmp_path: Path) -> None:
    store = _store(tmp_path / "runtime" / "autonomous_fix_loop" / "state.json")
    gateway = FakeGateway()
    gateway.push_audit(
        {
            "audit_run_id": "issue-mesh-coord-002",
            "bundle": {
                "findings": [
                    {
                        "family": "truth-lineage",
                        "finding_id": "TL-101",
                        "title": "lineage fix needed",
                    }
                ]
            },
            "artifact_fingerprints": {},
        }
    )

    class BadCoordinator(FakeCoordinator):
        def claim(self, *, round_id: str, target_paths: list[str], lease_seconds: int) -> dict[str, Any]:
            record = {
                "round_id": round_id,
                "target_paths": list(target_paths),
                "lease_seconds": lease_seconds,
            }
            self.claim_calls.append(record)
            return {"lease_id": f"lease-{round_id}", "round_id": round_id}

    coordinator = BadCoordinator()
    loop = AutonomousFixLoop(
        store=store,
        gateway=gateway,
        coordinator=coordinator,
        config=AutonomousFixLoopConfig(success_round_goal=10, lease_seconds=90),
    )

    state = loop.run_single_round()

    assert state.total_rounds == 1
    assert state.total_fixes == 0
    assert state.total_failures == 1
    assert state.round_history[-1].green_round is False
    assert gateway.last_apply_lease is None


def test_build_default_autonomous_fix_loop_service_prefers_app_internal_token(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AUTONOMOUS_FIX_LOOP_REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("INTERNAL_TOKEN", "control-plane-token")
    monkeypatch.setattr(settings, "internal_cron_token", "app-cron-token")
    monkeypatch.setattr(settings, "internal_api_key", "")

    service = build_default_autonomous_fix_loop_service()

    assert service._loop._gateway._cfg.internal_token == "app-cron-token"


def test_build_default_autonomous_fix_loop_service_accepts_plain_helper_tokens(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AUTONOMY_LOOP_REPO_ROOT", str(tmp_path))
    monkeypatch.delenv("AUTONOMOUS_FIX_LOOP_REPO_ROOT", raising=False)
    monkeypatch.delenv("MESH_RUNNER_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("PROMOTE_PREP_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("WRITEBACK_A_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("WRITEBACK_B_AUTH_TOKEN", raising=False)
    monkeypatch.setenv("MESH_RUNNER_TOKEN", "mesh-plain")
    monkeypatch.setenv("PROMOTE_PREP_TOKEN", "prep-plain")
    monkeypatch.setenv("WRITEBACK_A_TOKEN", "wb-a-plain")
    monkeypatch.setenv("WRITEBACK_B_TOKEN", "wb-b-plain")

    service = build_default_autonomous_fix_loop_service()

    assert service._loop._store.path == tmp_path.resolve() / "runtime" / "autonomous_fix_loop" / "state.json"
    assert service._loop._gateway._cfg.mesh_runner_token == "mesh-plain"
    assert service._loop._gateway._cfg.promote_prep_token == "prep-plain"
    assert service._loop._gateway._cfg.writeback_a_token == "wb-a-plain"
    assert service._loop._gateway._cfg.writeback_b_token == "wb-b-plain"
