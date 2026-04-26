"""Tests for the Loop Controller service — state machine, analyzer, verifier, and API."""
from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from automation.loop_controller.analyzer import (
    analyze_bundle,
    detect_drift,
)
from automation.loop_controller.schemas import (
    FixOutcome,
    FixResult,
    LoopMode,
    LoopPhase,
    LoopState,
    ProblemSpec,
    RoundSummary,
    Severity,
    VerifyResult,
)
from automation.loop_controller.state import StateStore


# ============================================================================
# StateStore
# ============================================================================


class TestStateStore:
    def test_load_creates_default_on_missing_file(self, tmp_path: Path) -> None:
        store = StateStore(tmp_path / "state.json")
        state = store.load()
        assert state.mode == LoopMode.FIX
        assert state.phase == LoopPhase.IDLE
        assert state.consecutive_fix_success_count == 0

    def test_save_and_reload_roundtrip(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        store = StateStore(path)
        state = LoopState(mode=LoopMode.MONITOR, consecutive_fix_success_count=7, fix_goal=10)
        store.save(state)
        assert path.exists()
        reloaded = store.load()
        assert reloaded.mode == LoopMode.MONITOR
        assert reloaded.consecutive_fix_success_count == 7

    def test_update_partial(self, tmp_path: Path) -> None:
        store = StateStore(tmp_path / "state.json")
        store.save(LoopState())
        updated = store.update(consecutive_fix_success_count=5, mode=LoopMode.MONITOR)
        assert updated.consecutive_fix_success_count == 5
        assert updated.mode == LoopMode.MONITOR

    def test_trim_history(self, tmp_path: Path) -> None:
        store = StateStore(tmp_path / "state.json")
        state = LoopState(
            round_history=[
                RoundSummary(round_id=f"r-{i}", started_at="2026-01-01T00:00:00")
                for i in range(250)
            ]
        )
        store.save(state)
        store.trim_history(max_rounds=100)
        trimmed = store.load()
        assert len(trimmed.round_history) == 100
        assert trimmed.round_history[0].round_id == "r-150"

    def test_get_returns_cached_copy(self, tmp_path: Path) -> None:
        store = StateStore(tmp_path / "state.json")
        store.save(LoopState(fix_goal=20))
        s1 = store.get()
        s2 = store.get()
        assert s1.fix_goal == 20
        # mutations to one don't affect the other
        s1.fix_goal = 99
        assert s2.fix_goal == 20


# ============================================================================
# Analyzer
# ============================================================================


class TestAnalyzer:
    def _make_bundle(self, findings: list[dict]) -> dict:
        return {"findings": findings}

    def test_empty_bundle(self) -> None:
        problems, new, reg, skip = analyze_bundle({"findings": []}, [])
        assert problems == []
        assert new == 0

    def test_single_finding_new(self) -> None:
        bundle = self._make_bundle([{
            "family": "truth-lineage",
            "finding_id": "TL-001",
            "title": "Blood lineage drift",
            "severity": "P1",
        }])
        problems, new, reg, skip = analyze_bundle(bundle, [])
        assert len(problems) == 1
        assert new == 1
        assert reg == 0
        assert problems[0].severity == Severity.P1
        assert problems[0].family == "truth-lineage"
        assert problems[0].category == "事实/契约/数据"

    def test_regression_detected(self) -> None:
        fixed = ["truth-lineage:TL-001"]
        bundle = self._make_bundle([{
            "family": "truth-lineage",
            "finding_id": "TL-001",
            "title": "Re-appeared",
        }])
        problems, new, reg, skip = analyze_bundle(bundle, fixed)
        assert len(problems) == 1
        assert reg == 1
        assert new == 0
        assert problems[0].is_regression is True
        # regression promotes severity P1 → P0
        assert problems[0].severity == Severity.P0

    def test_external_blocked_skipped(self) -> None:
        bundle = self._make_bundle([{
            "family": "runtime-anchor",
            "finding_id": "RA-001",
            "title": "mootdx data source unavailable",
            "description": "mootdx provider-not-configured",
        }])
        problems, new, reg, skip = analyze_bundle(bundle, [])
        assert skip == 1
        assert new == 0
        assert problems[0].is_external_blocked is True

    def test_non_fix_handling_path_marked_blocked(self) -> None:
        bundle = self._make_bundle([{
            "family": "shared-artifacts",
            "finding_id": "SA-001",
            "title": "monitoring only finding",
            "handling_path": "execution_and_monitoring",
        }])
        problems, new, reg, skip = analyze_bundle(bundle, [])
        assert reg == 0
        assert new == 0
        assert skip == 1
        assert len(problems) == 1
        assert problems[0].is_external_blocked is True

    def test_deduplication(self) -> None:
        bundle = self._make_bundle([
            {"family": "truth-lineage", "finding_id": "TL-001"},
            {"family": "truth-lineage", "finding_id": "TL-001"},  # duplicate
        ])
        problems, new, _, _ = analyze_bundle(bundle, [])
        assert len(problems) == 1
        assert new == 1

    def test_sorting_p0_first(self) -> None:
        bundle = self._make_bundle([
            {"family": "repo-governance", "finding_id": "RG-001", "severity": "P2"},
            {"family": "truth-lineage", "finding_id": "TL-001", "severity": "P0"},
            {"family": "fr07-rebuild", "finding_id": "FR7-001", "severity": "P1"},
        ])
        problems, _, _, _ = analyze_bundle(bundle, [])
        assert problems[0].severity == Severity.P0
        assert problems[1].severity == Severity.P1
        assert problems[2].severity == Severity.P2

    def test_alternate_bundle_structure(self) -> None:
        """Bundle that uses shards instead of flat findings."""
        bundle = {
            "shards": [
                {"findings": [{"family": "truth-lineage", "finding_id": "TL-001"}]},
                {"findings": [{"family": "issue-registry", "finding_id": "IR-001"}]},
            ]
        }
        problems, new, _, _ = analyze_bundle(bundle, [])
        assert len(problems) == 2
        assert new == 2

    def test_affected_frs_extraction(self) -> None:
        bundle = self._make_bundle([{
            "family": "fr06-failure-semantics",
            "finding_id": "F6-001",
            "title": "FR-06 and FR-07 timeout mismatch",
        }])
        problems, _, _, _ = analyze_bundle(bundle, [])
        assert "FR-06" in problems[0].affected_frs
        assert "FR-07" in problems[0].affected_frs


class TestDriftDetection:
    def test_no_drift(self) -> None:
        prev = {"a.json": "abc123", "b.json": "def456"}
        curr = {"a.json": "abc123", "b.json": "def456"}
        assert detect_drift(curr, prev) == []

    def test_drift_detected(self) -> None:
        prev = {"a.json": "abc123", "b.json": "def456"}
        curr = {"a.json": "abc123", "b.json": "changed"}
        assert detect_drift(curr, prev) == ["b.json"]

    def test_new_artifact_not_drift(self) -> None:
        prev = {"a.json": "abc123"}
        curr = {"a.json": "abc123", "b.json": "new"}
        assert detect_drift(curr, prev) == []


# ============================================================================
# Schemas
# ============================================================================


class TestSchemas:
    def test_loop_state_defaults(self) -> None:
        state = LoopState()
        assert state.mode == LoopMode.FIX
        assert state.phase == LoopPhase.IDLE
        assert state.consecutive_fix_success_count == 0
        assert state.fix_goal == 10

    def test_problem_spec_serialization(self) -> None:
        p = ProblemSpec(
            problem_id="truth-lineage:TL-001",
            severity=Severity.P1,
            family="truth-lineage",
        )
        data = p.model_dump()
        assert data["severity"] == "P1"
        round_trip = ProblemSpec.model_validate(data)
        assert round_trip.problem_id == p.problem_id

    def test_fix_result_model(self) -> None:
        r = FixResult(
            problem_id="truth-lineage:TL-001",
            outcome=FixOutcome.SUCCESS,
            patches_applied=["app/services/lineage.py"],
        )
        assert r.outcome == FixOutcome.SUCCESS

    def test_verify_result_all_green(self) -> None:
        v = VerifyResult(
            scoped_pytest_passed=True,
            full_pytest_passed=True,
            full_pytest_total=1315,
            full_pytest_failed=0,
            blind_spot_clean=True,
            catalog_improved=True,
            artifacts_aligned=True,
            all_green=True,
        )
        assert v.all_green

    def test_round_summary(self) -> None:
        s = RoundSummary(
            round_id="fix-loop-20260328-001",
            started_at="2026-03-28T00:00:00+00:00",
            finished_at="2026-03-28T00:30:00+00:00",
            all_success=True,
        )
        assert s.round_id.startswith("fix-loop-")


# ============================================================================
# Controller state transitions (unit-level, mocked external calls)
# ============================================================================


class TestControllerStateTransitions:
    """Test the LoopController state machine transitions without real HTTP calls."""

    def _make_controller(self, tmp_path: Path):
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=tmp_path,
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="t",
            fix_goal=3,
            audit_interval_seconds=1,
            monitor_interval_seconds=1,
        )
        store = StateStore(tmp_path / "state.json")
        ctrl = LoopController(cfg, store=store)
        return ctrl, store

    def test_initial_state(self, tmp_path: Path) -> None:
        ctrl, store = self._make_controller(tmp_path)
        state = ctrl.get_state()
        assert state.mode == LoopMode.FIX
        assert state.phase == LoopPhase.IDLE

    def test_success_counter_increment(self, tmp_path: Path) -> None:
        """Simulate counter increment logic."""
        _, store = self._make_controller(tmp_path)
        state = store.load()
        state.consecutive_fix_success_count = 2
        store.save(state)
        reloaded = store.load()
        assert reloaded.consecutive_fix_success_count == 2

    def test_failure_resets_counter(self, tmp_path: Path) -> None:
        """Simulate failure resetting counter to 0."""
        _, store = self._make_controller(tmp_path)
        state = store.load()
        state.consecutive_fix_success_count = 8
        state.total_failures = 0
        store.save(state)
        # simulate failure
        state = store.load()
        state.consecutive_fix_success_count = 0
        state.total_failures += 1
        store.save(state)
        final = store.load()
        assert final.consecutive_fix_success_count == 0
        assert final.total_failures == 1

    def test_goal_triggers_monitor_mode(self, tmp_path: Path) -> None:
        _, store = self._make_controller(tmp_path)
        state = store.load()
        state.consecutive_fix_success_count = 3  # == fix_goal
        state.fix_goal = 3
        if state.consecutive_fix_success_count >= state.fix_goal:
            state.mode = LoopMode.MONITOR
        store.save(state)
        assert store.load().mode == LoopMode.MONITOR

    def test_monitor_reenter_fix_on_regression(self, tmp_path: Path) -> None:
        _, store = self._make_controller(tmp_path)
        state = store.load()
        state.mode = LoopMode.MONITOR
        state.consecutive_fix_success_count = 10
        store.save(state)
        # simulate regression detection
        state = store.load()
        state.mode = LoopMode.FIX
        state.consecutive_fix_success_count = 0
        store.save(state)
        final = store.load()
        assert final.mode == LoopMode.FIX
        assert final.consecutive_fix_success_count == 0

    def test_problems_queue_dedup(self, tmp_path: Path) -> None:
        _, store = self._make_controller(tmp_path)
        state = store.load()
        state.fixed_problems = ["truth-lineage:TL-001"]
        store.save(state)
        # analyze with that same finding
        bundle = {"findings": [{"family": "truth-lineage", "finding_id": "TL-001"}]}
        problems, _, reg, _ = analyze_bundle(bundle, state.fixed_problems)
        assert len(problems) == 1
        assert reg == 1
        assert problems[0].is_regression is True

    def test_idempotent_writeback_key(self, tmp_path: Path) -> None:
        """Verify idempotency key format is deterministic."""
        round_id = "fix-loop-20260328-001"
        target = "app/services/lineage.py"
        key1 = f"{round_id}:{target}"
        key2 = f"{round_id}:{target}"
        assert key1 == key2


# ============================================================================
# FastAPI app (integration)
# ============================================================================


class TestLoopControllerAPI:
    @pytest.fixture
    def client(self, tmp_path: Path):
        from fastapi.testclient import TestClient
        from automation.loop_controller.controller import LoopController, LoopControllerConfig
        from automation.loop_controller.app import create_app

        cfg = LoopControllerConfig(
            repo_root=tmp_path,
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="test-token",
        )
        store = StateStore(tmp_path / "state.json")
        ctrl = LoopController(cfg, store=store)
        app = create_app(config=cfg, controller=ctrl)
        return TestClient(app)

    def test_health_endpoint(self, client) -> None:
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["running"] is False

    def test_state_endpoint_requires_auth(self, client) -> None:
        resp = client.get("/v1/state")
        assert resp.status_code == 401

    def test_state_endpoint_with_auth(self, client) -> None:
        resp = client.get("/v1/state", headers={"Authorization": "Bearer test-token"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["mode"] == "fix"
        assert data["running"] is False

    def test_analyze_endpoint(self, client) -> None:
        resp = client.post(
            "/v1/analyze",
            json={
                "audit_run_id": "issue-mesh-20260328-001",
                "bundle": {
                    "findings": [
                        {"family": "truth-lineage", "finding_id": "TL-001", "title": "test"},
                    ]
                },
            },
            headers={"Authorization": "Bearer test-token"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["new_count"] == 1
        assert len(data["problems"]) == 1

    def test_verify_endpoint(self, client) -> None:
        resp = client.post(
            "/v1/verify",
            json={
                "round_id": "fix-loop-20260328-001",
                "fix_results": [],
                "affected_test_paths": [],
            },
            headers={"Authorization": "Bearer test-token"},
        )
        assert resp.status_code == 200

    def test_start_stop_lifecycle(self, client) -> None:
        # start
        resp = client.post(
            "/v1/start",
            json={"mode": "fix", "fix_goal": 5},
            headers={"Authorization": "Bearer test-token"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "started"

        # stop
        resp = client.post(
            "/v1/stop",
            json={"reason": "test_stop"},
            headers={"Authorization": "Bearer test-token"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "stopped"

    def test_double_start_rejected(self, client) -> None:
        client.post(
            "/v1/start",
            json={"mode": "fix", "fix_goal": 5},
            headers={"Authorization": "Bearer test-token"},
        )
        resp = client.post(
            "/v1/start",
            json={"mode": "fix", "fix_goal": 5},
            headers={"Authorization": "Bearer test-token"},
        )
        assert resp.status_code == 409
        # cleanup
        client.post(
            "/v1/stop",
            json={"reason": "cleanup"},
            headers={"Authorization": "Bearer test-token"},
        )

    def test_rounds_endpoint(self, client) -> None:
        resp = client.get("/v1/rounds", headers={"Authorization": "Bearer test-token"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["rounds"] == []


# ============================================================================
# Phase A: Integration defect fixes
# ============================================================================


class TestPatchesParsing:
    """A1-A3: Verify _do_fix handles promote_prep's actual response format."""

    def test_fix_result_patches_raw_field(self) -> None:
        """FixResult should accept patches_raw as list[dict]."""
        raw = [{"target_path": "app/x.py", "patch_text": "diff", "base_sha256": "abc"}]
        r = FixResult(
            problem_id="test:001",
            outcome=FixOutcome.SUCCESS,
            patches_applied=["app/x.py"],
            patches_raw=raw,
        )
        assert r.patches_raw == raw
        assert r.patches_applied == ["app/x.py"]

    def test_fix_result_patches_raw_default_empty(self) -> None:
        """patches_raw defaults to empty list."""
        r = FixResult(problem_id="test:002", outcome=FixOutcome.FAILED)
        assert r.patches_raw == []

    def test_fix_result_roundtrip_with_patches_raw(self) -> None:
        """Round-trip serialization preserves patches_raw."""
        raw = [{"target_path": "a.py", "patch_text": "p", "base_sha256": "h"}]
        r = FixResult(
            problem_id="x:1",
            outcome=FixOutcome.SUCCESS,
            patches_applied=["a.py"],
            patches_raw=raw,
        )
        data = r.model_dump()
        r2 = FixResult.model_validate(data)
        assert r2.patches_raw == raw

    def test_collect_affected_tests_with_string_paths(self) -> None:
        """_collect_affected_tests keeps changed app/tests paths for scoped pytest inference."""
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=Path("."),
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="t",
        )
        store = StateStore(Path(tempfile.mkdtemp()) / "s.json")
        ctrl = LoopController(cfg, store=store)
        results = [
            FixResult(
                problem_id="p1",
                outcome=FixOutcome.SUCCESS,
                patches_applied=["tests/test_a.py", "app/x.py", "tests/test_b.py"],
            ),
        ]
        affected = ctrl._collect_affected_tests(results)
        assert affected == ["app/x.py", "tests/test_a.py", "tests/test_b.py"]

    def test_do_fix_marks_no_fixable_as_skipped(self) -> None:
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=Path("."),
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="t",
        )
        store = StateStore(Path(tempfile.mkdtemp()) / "s.json")
        ctrl = LoopController(cfg, store=store)
        ctrl._http = _FakeHTTP(
            post_responses=[
                _FakeResponse({
                    "patch_count": 0,
                    "patches": [],
                    "skip_reason": "NO_FIXABLE_FINDINGS",
                })
            ]
        )

        state = LoopState(last_audit_run_id="issue-mesh-20260329-001")
        problems = [
            ProblemSpec(
                problem_id="shared-artifacts:SA-001",
                severity=Severity.P1,
                family="shared-artifacts",
            )
        ]
        results = ctrl._do_fix(
            problems,
            round_id="fix-loop-20260329-009",
            state=state,
            runtime_context={"runtime_gates": {"status": "blocked"}},
        )

        assert len(results) == 1
        assert results[0].outcome == FixOutcome.SKIPPED
        assert results[0].error == "NO_FIXABLE_FINDINGS"

    def test_do_fix_marks_zero_valid_patches_as_failed(self) -> None:
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=Path("."),
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="t",
        )
        store = StateStore(Path(tempfile.mkdtemp()) / "s.json")
        ctrl = LoopController(cfg, store=store)
        ctrl._http = _FakeHTTP(
            post_responses=[
                _FakeResponse(
                    {
                        "patch_count": 0,
                        "patches": [
                            {
                                "target_path": "",
                                "patch_text": "",
                                "valid": False,
                                "explanation": "PATCH_TEXT_NOT_FULL_FILE_CONTENT",
                            }
                        ],
                        "skip_reason": None,
                    }
                )
            ]
        )

        state = LoopState(last_audit_run_id="issue-mesh-20260329-001")
        problems = [
            ProblemSpec(
                problem_id="shared-artifacts:SA-002",
                severity=Severity.P1,
                family="shared-artifacts",
            )
        ]
        results = ctrl._do_fix(
            problems,
            round_id="fix-loop-20260329-010",
            state=state,
            runtime_context={"runtime_gates": {"status": "blocked"}},
        )

        assert len(results) == 1
        assert results[0].outcome == FixOutcome.FAILED
        assert results[0].patches_applied == []
        assert results[0].error == "PATCH_TEXT_NOT_FULL_FILE_CONTENT"


class TestAuditFailedError:
    """A6: Verify AuditFailedError is a RuntimeError."""

    def test_audit_failed_error_is_runtime_error(self) -> None:
        from automation.loop_controller.controller import AuditFailedError
        exc = AuditFailedError("network down")
        assert isinstance(exc, RuntimeError)
        assert str(exc) == "network down"


class TestBackoffConstants:
    """A7-A8: Verify backoff constants and httpx resilience configuration."""

    def test_backoff_constants_exist(self) -> None:
        from automation.loop_controller import controller
        assert controller._MAX_BACKOFF_FAILURES == 5
        assert controller._BASE_BACKOFF_SECONDS == 30

    def test_httpx_timeout_configuration(self) -> None:
        """Verify the controller uses granular timeouts, not just a flat default."""
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=Path("."),
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="t",
        )
        store = StateStore(Path(tempfile.mkdtemp()) / "s.json")
        ctrl = LoopController(cfg, store=store)
        http = ctrl._http
        # The timeout should specify read=3660 (61 min for long codex mesh runs)
        assert http.timeout.read == 3660.0
        assert http.timeout.connect == 15.0


class TestScopedPytestFields:
    """A5: Verify verifier sends correct ScopedPytestRequest fields."""

    def test_scoped_pytest_sends_correct_payload(self) -> None:
        """Mock the HTTP call and verify the payload matches
        promote_prep's ScopedPytestRequest schema."""
        from automation.loop_controller.verifier import Verifier

        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"passed": True, "detail": "ok"}
        mock_client.post.return_value = mock_resp

        v = Verifier(
            repo_root=Path("."),
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
        )
        v._client = mock_client

        passed, details = v.run_scoped_pytest(
            affected_test_paths=["tests/test_a.py"],
            round_id="fix-loop-20260328-001",
        )
        assert passed is True

        # Inspect the actual payload sent
        call_args = mock_client.post.call_args
        payload = call_args.kwargs.get("json") or call_args[1].get("json")
        assert "fix_run_id" in payload
        assert "changed_files" in payload
        assert "timeout_seconds" in payload
        # Must NOT contain old field names
        assert "test_paths" not in payload
        assert "round_id" not in payload


class TestVerifierTruth:
    def test_parse_junit_supports_testsuites_root(self, tmp_path: Path) -> None:
        from automation.loop_controller.verifier import _parse_junit

        junit_path = tmp_path / "junit.xml"
        junit_path.write_text(
            """
<testsuites>
  <testsuite name="suite-a" tests="3" failures="1" errors="0" />
  <testsuite name="suite-b" tests="2" failures="0" errors="1" />
</testsuites>
""".strip(),
            encoding="utf-8",
        )

        total, failed = _parse_junit(junit_path)

        assert total == 5
        assert failed == 2

    def test_run_full_pytest_rejects_zero_test_junit(self, tmp_path: Path, monkeypatch) -> None:
        from automation.loop_controller.verifier import Verifier

        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True)
        (output_dir / "junit.xml").write_text("<testsuites></testsuites>", encoding="utf-8")

        monkeypatch.setattr(
            "automation.loop_controller.verifier.subprocess.run",
            lambda *args, **kwargs: MagicMock(returncode=0, stdout="", stderr=""),
        )

        verifier = Verifier(repo_root=tmp_path)
        passed, total, failed = verifier.run_full_pytest()

        assert passed is False
        assert total == 0
        assert failed == 0

    def test_check_artifact_alignment_infra_mode_ignores_doc22(self, tmp_path: Path) -> None:
        from automation.loop_controller.verifier import DOC22_PROGRESS_DOC, RUNTIME_SHARED_ARTIFACTS, Verifier

        for rel in RUNTIME_SHARED_ARTIFACTS:
            path = tmp_path / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("ok", encoding="utf-8")

        control_plane = tmp_path / "automation" / "control_plane"
        control_plane.mkdir(parents=True, exist_ok=True)
        (control_plane / "current_state.json").write_text(
            json.dumps({"promote_target_mode": "infra"}, ensure_ascii=False),
            encoding="utf-8",
        )

        verifier = Verifier(repo_root=tmp_path)
        aligned, fingerprints = verifier.check_artifact_alignment()

        assert aligned is True
        assert set(fingerprints) == set(RUNTIME_SHARED_ARTIFACTS)
        assert DOC22_PROGRESS_DOC not in fingerprints

    def test_check_artifact_alignment_non_infra_mode_requires_doc22(self, tmp_path: Path) -> None:
        from automation.loop_controller.verifier import DOC22_PROGRESS_DOC, DOC22_SHARED_ARTIFACTS, RUNTIME_SHARED_ARTIFACTS, Verifier

        for rel in RUNTIME_SHARED_ARTIFACTS:
            path = tmp_path / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("ok", encoding="utf-8")

        control_plane = tmp_path / "automation" / "control_plane"
        control_plane.mkdir(parents=True, exist_ok=True)
        (control_plane / "current_state.json").write_text(
            json.dumps({"promote_target_mode": "doc22"}, ensure_ascii=False),
            encoding="utf-8",
        )

        verifier = Verifier(repo_root=tmp_path)
        aligned, fingerprints = verifier.check_artifact_alignment()

        assert aligned is False
        assert set(fingerprints) == set(DOC22_SHARED_ARTIFACTS)
        assert fingerprints[DOC22_PROGRESS_DOC] == ""


class TestControlPlaneSync:
    def _make_controller(self, tmp_path: Path):
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=tmp_path,
            mesh_runner_url="http://fake:8193",
            mesh_runner_token="",
            promote_prep_url="http://fake:8094",
            promote_prep_token="",
            writeback_a_url="http://fake:8092",
            writeback_a_token="",
            writeback_b_url="http://fake:8095",
            writeback_b_token="",
            auth_token="",
        )
        return LoopController(cfg, store=StateStore(tmp_path / "state.json"))

    def test_sync_control_plane_defaults_to_infra_when_state_missing(self, tmp_path: Path) -> None:
        ctrl = self._make_controller(tmp_path)
        state = LoopState(promote_target_mode="doc22")

        ctrl._sync_control_plane_config(state)

        assert state.promote_target_mode == "infra"

    def test_sync_control_plane_defaults_to_infra_when_state_invalid(self, tmp_path: Path) -> None:
        control_plane = tmp_path / "automation" / "control_plane"
        control_plane.mkdir(parents=True, exist_ok=True)
        (control_plane / "current_state.json").write_text(
            json.dumps({"promote_target_mode": "unexpected-mode"}, ensure_ascii=False),
            encoding="utf-8",
        )

        ctrl = self._make_controller(tmp_path)
        state = LoopState(promote_target_mode="doc22")

        ctrl._sync_control_plane_config(state)

        assert state.promote_target_mode == "infra"


class _FakeResponse:
    def __init__(self, payload: dict | None = None, *, status_code: int = 200, text: str = "") -> None:
        self._payload = payload or {}
        self.status_code = status_code
        self.text = text or json.dumps(self._payload)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict:
        return self._payload


class _FakeHTTP:
    def __init__(self, *, get_responses: list[_FakeResponse] | None = None, post_responses: list[_FakeResponse] | None = None) -> None:
        self.get_responses = list(get_responses or [])
        self.post_responses = list(post_responses or [])
        self.calls: list[tuple[str, str, dict | None, dict | None]] = []

    def get(self, url: str, *, headers: dict | None = None, **kwargs):
        self.calls.append(("GET", url, None, headers))
        if not self.get_responses:
            raise AssertionError(f"unexpected GET: {url}")
        return self.get_responses.pop(0)

    def post(self, url: str, *, json: dict | None = None, headers: dict | None = None, **kwargs):
        self.calls.append(("POST", url, json, headers))
        if not self.post_responses:
            raise AssertionError(f"unexpected POST: {url}")
        return self.post_responses.pop(0)


def _stub_provider_ready(monkeypatch, ctrl) -> None:
    monkeypatch.setattr(
        ctrl,
        "_check_provider_readiness",
        lambda: {"ready": True, "status": "ok", "stage": "done", "error": None},
    )


class TestRuntimeContextAndGuardedWriteback:
    def _make_controller(self, tmp_path: Path):
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=tmp_path,
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="mesh-token",
            promote_prep_url="http://fake:8094",
            promote_prep_token="prep-token",
            writeback_a_url="http://fake:8092",
            writeback_a_token="wb-a-token",
            writeback_b_url="http://fake:8095",
            writeback_b_token="wb-b-token",
            auth_token="loop-token",
            app_base_url="http://fake-app:38001",
            internal_token="internal-token",
            new_api_base_url="http://fake-newapi:3000",
            new_api_token="test-key",
        )
        return LoopController(cfg, store=StateStore(tmp_path / "state.json"))

    def test_load_runtime_context_uses_app_audit_context_and_internal_token(self, tmp_path: Path) -> None:
        ctrl = self._make_controller(tmp_path)
        fake_http = _FakeHTTP(
            get_responses=[
                _FakeResponse(
                    {
                        "data": {
                            "runtime_gates": {"status": "blocked"},
                            "public_runtime_status": "DEGRADED",
                        }
                    }
                )
            ]
        )
        ctrl._http = fake_http

        payload = ctrl._load_runtime_context()

        assert payload["runtime_gates"]["status"] == "blocked"
        assert fake_http.calls == [
            (
                "GET",
                "http://fake-app:38001/api/v1/internal/audit/context",
                None,
                {"X-Internal-Token": "internal-token"},
            )
        ]

    def test_do_audit_polls_mesh_runner_until_terminal_status(self, tmp_path: Path, monkeypatch) -> None:
        ctrl = self._make_controller(tmp_path)
        fake_http = _FakeHTTP(
            post_responses=[
                _FakeResponse(
                    {
                        "run_id": "issue-mesh-20260328-401",
                        "status": "queued",
                        "manifest_path": "m.json",
                        "output_dir": "runtime/issue_mesh/issue-mesh-20260328-401",
                    }
                )
            ],
            get_responses=[
                _FakeResponse(
                    {
                        "data": {
                            "runtime_gates": {"status": "ready"},
                        }
                    }
                ),
                _FakeResponse(
                    {
                        "run_id": "issue-mesh-20260328-401",
                        "status": "running",
                        "manifest_path": "m.json",
                        "output_dir": "runtime/issue_mesh/issue-mesh-20260328-401",
                    }
                ),
                _FakeResponse(
                    {
                        "run_id": "issue-mesh-20260328-401",
                        "status": "completed",
                        "manifest_path": "m.json",
                        "output_dir": "runtime/issue_mesh/issue-mesh-20260328-401",
                    }
                ),
                _FakeResponse({"bundle": {"findings": [{"issue_key": "truth-lineage"}]}}),
            ],
        )
        ctrl._http = fake_http
        monkeypatch.setattr("automation.loop_controller.controller._AUDIT_STATUS_POLL_SECONDS", 0.0)

        run_id, bundle = ctrl._do_audit(LoopState(current_round_id="fix-loop-20260328-401"))

        assert run_id == "issue-mesh-20260328-401"
        assert bundle == {"findings": [{"issue_key": "truth-lineage"}]}
        assert fake_http.calls[0][0:2] == ("GET", "http://fake-app:38001/api/v1/internal/audit/context")
        assert fake_http.calls[1][0:2] == ("POST", "http://fake:8093/v1/runs")
        assert fake_http.calls[1][2]["max_workers"] == 12
        assert fake_http.calls[1][2]["wait_for_completion"] is False
        assert fake_http.calls[2][0:2] == ("GET", "http://fake:8093/v1/runs/issue-mesh-20260328-401")
        assert fake_http.calls[3][0:2] == ("GET", "http://fake:8093/v1/runs/issue-mesh-20260328-401")
        assert fake_http.calls[4][0:2] == ("GET", "http://fake:8093/v1/runs/issue-mesh-20260328-401/bundle")

    def test_do_audit_accepts_failed_mesh_run_when_bundle_exists(self, tmp_path: Path, monkeypatch) -> None:
        ctrl = self._make_controller(tmp_path)
        fake_http = _FakeHTTP(
            post_responses=[
                _FakeResponse(
                    {
                        "run_id": "issue-mesh-20260328-402",
                        "status": "queued",
                        "manifest_path": "m.json",
                        "output_dir": "runtime/issue_mesh/issue-mesh-20260328-402",
                    }
                )
            ],
            get_responses=[
                _FakeResponse(
                    {
                        "data": {
                            "runtime_gates": {"status": "ready"},
                        }
                    }
                ),
                _FakeResponse(
                    {
                        "run_id": "issue-mesh-20260328-402",
                        "status": "running",
                        "manifest_path": "m.json",
                        "output_dir": "runtime/issue_mesh/issue-mesh-20260328-402",
                    }
                ),
                _FakeResponse(
                    {
                        "run_id": "issue-mesh-20260328-402",
                        "status": "failed",
                        "manifest_path": "m.json",
                        "output_dir": "runtime/issue_mesh/issue-mesh-20260328-402",
                    }
                ),
                _FakeResponse({"bundle": {"finding_count": 2, "findings": [{"issue_key": "issue-registry"}]}}),
            ],
        )
        ctrl._http = fake_http
        monkeypatch.setattr("automation.loop_controller.controller._AUDIT_STATUS_POLL_SECONDS", 0.0)

        run_id, bundle = ctrl._do_audit(LoopState(current_round_id="fix-loop-20260328-402"))

        assert run_id == "issue-mesh-20260328-402"
        assert fake_http.calls[1][2]["max_workers"] == 12
        assert bundle["finding_count"] == 2
        assert bundle["findings"][0]["issue_key"] == "issue-registry"

    def test_apply_fix_commits_requires_triage_record_before_commit(self, tmp_path: Path) -> None:
        ctrl = self._make_controller(tmp_path)
        fake_http = _FakeHTTP(
            post_responses=[
                _FakeResponse({"conflict": False}),
                _FakeResponse({"auto_commit": True, "triage_record_id": "fix-round-001__code-fix-writeback"}),
                _FakeResponse({"status": "committed", "commit_id": "commit-001", "idempotent_replay": False}),
            ]
        )
        ctrl._http = fake_http
        fix_results = [
            FixResult(
                problem_id="truth-lineage:TL-001",
                outcome=FixOutcome.SUCCESS,
                fix_run_id="fix-round-001",
                patches_applied=["app/services/probe.py"],
                patches_raw=[
                    {
                        "target_path": "app/services/probe.py",
                        "patch_text": "after\n",
                        "base_sha256": "base-sha",
                    }
                ],
            )
        ]

        updated, commits = ctrl._apply_fix_commits(
            fix_results,
            round_id="fix-round-001",
            runtime_context={
                "runtime_gates": {"status": "blocked", "shared_artifact_promote": {"allowed": False}},
                "public_runtime_status": "DEGRADED",
            },
        )

        assert updated[0].outcome == FixOutcome.SUCCESS
        assert commits == [
            {
                "problem_id": "truth-lineage:TL-001",
                "target_path": "app/services/probe.py",
                "commit_id": "commit-001",
            }
        ]
        assert fake_http.calls[1][0:2] == ("POST", "http://fake:8094/v1/triage/writeback")
        assert fake_http.calls[2][2]["triage_record_id"] == "fix-round-001__code-fix-writeback"

    def test_do_promote_runs_prepare_triage_preview_commit_chain(self, tmp_path: Path) -> None:
        ctrl = self._make_controller(tmp_path)
        fake_http = _FakeHTTP(
            post_responses=[
                _FakeResponse(
                    {
                        "run_id": "issue-mesh-20260328-201",
                        "layer": "status-note",
                        "target_path": "docs/core/22_全量功能进度总表_v7_精审.md",
                        "target_anchor": "current-writeback-detail",
                        "patch_text": "patched status note",
                        "base_sha256": "sha-22",
                        "idempotency_key": "issue-mesh:issue-mesh-20260328-201:status-note:current-writeback-detail",
                        "request_id": "req-status-note",
                        "semantic_fingerprint": "fp-1",
                        "skip_commit": False,
                    }
                ),
                _FakeResponse({"auto_commit": True, "triage_record_id": "issue-mesh-20260328-201__status-note"}),
                _FakeResponse({"conflict": False}),
                _FakeResponse({"status": "committed", "commit_id": "commit-status-note"}),
                _FakeResponse(
                    {
                        "run_id": "issue-mesh-20260328-201",
                        "layer": "current-layer",
                        "target_path": "docs/core/22_全量功能进度总表_v7_精审.md",
                        "target_anchor": "2.1|2.3|4.5",
                        "patch_text": "patched current layer",
                        "base_sha256": "sha-22",
                        "idempotency_key": "issue-mesh:issue-mesh-20260328-201:current-layer:2.1|2.3|4.5",
                        "request_id": "req-current-layer",
                        "semantic_fingerprint": "fp-2",
                        "skip_commit": False,
                    }
                ),
                _FakeResponse({"auto_commit": True, "triage_record_id": "issue-mesh-20260328-201__current-layer"}),
                _FakeResponse({"conflict": False}),
                _FakeResponse({"status": "committed", "commit_id": "commit-current-layer"}),
            ]
        )
        ctrl._http = fake_http
        state = LoopState(last_audit_run_id="issue-mesh-20260328-201")

        result = ctrl._do_promote(
            "fix-loop-20260328-001",
            state,
            {
                "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
                "public_runtime_status": "READY",
            },
        )

        commit_calls = [call for call in fake_http.calls if call[1].endswith("/v1/commit")]
        assert len(commit_calls) == 2
        assert commit_calls[0][2]["triage_record_id"] == "issue-mesh-20260328-201__status-note"
        assert commit_calls[1][2]["triage_record_id"] == "issue-mesh-20260328-201__current-layer"
        assert result["status_note"]["status"] == "committed"
        assert result["current_layer"]["status"] == "committed"

    def test_do_promote_bails_early_when_status_note_fails(self, tmp_path: Path, monkeypatch) -> None:
        ctrl = self._make_controller(tmp_path)
        monkeypatch.setattr("automation.loop_controller.controller.time.sleep", lambda _: None)
        fake_http = _FakeHTTP(
            post_responses=[
                # status-note prepare response
                _FakeResponse(
                    {
                        "run_id": "issue-mesh-20260328-201",
                        "layer": "status-note",
                        "target_path": "docs/core/22_全量功能进度总表_v7_精审.md",
                        "target_anchor": "current-writeback-detail",
                        "patch_text": "patched status note",
                        "base_sha256": "sha-22",
                        "idempotency_key": "issue-mesh:issue-mesh-20260328-201:status-note:current-writeback-detail",
                        "request_id": "req-status-note",
                        "semantic_fingerprint": "fp-1",
                        "skip_commit": False,
                    }
                ),
                # triage response
                _FakeResponse({"auto_commit": True, "triage_record_id": "issue-mesh-20260328-201__status-note"}),
                # preview 403 (x3 retries exhausted)
                _FakeResponse({}, status_code=403),
                _FakeResponse({}, status_code=403),
                _FakeResponse({}, status_code=403),
            ]
        )
        ctrl._http = fake_http
        state = LoopState(last_audit_run_id="issue-mesh-20260328-201")

        result = ctrl._do_promote(
            "fix-loop-20260328-001",
            state,
            {
                "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
                "public_runtime_status": "READY",
            },
        )

        # status_note should be failed
        assert result["status_note"]["status"] == "failed"
        assert "WRITEBACK_B_PREVIEW_403" in result["status_note"]["error"]
        # current_layer should NOT have been attempted — still default skip
        assert result["current_layer"]["status"] == "skipped"
        assert result["current_layer"]["skip_reason"] == "NOT_ATTEMPTED"
        # Only 5 HTTP calls: status-note prepare + triage + 3 preview retries
        assert len(fake_http.calls) == 5

    def test_run_one_round_counts_successful_repairs_in_problem_units(self, tmp_path: Path, monkeypatch) -> None:
        ctrl = self._make_controller(tmp_path)
        store = ctrl._store
        _stub_provider_ready(monkeypatch, ctrl)
        monkeypatch.setattr(
            "automation.loop_controller.controller.analyze_bundle",
            lambda bundle, fixed: (
                [
                    ProblemSpec(problem_id="truth-lineage:TL-001", severity=Severity.P1, family="truth-lineage"),
                    ProblemSpec(problem_id="runtime-anchor:RA-001", severity=Severity.P1, family="runtime-anchor"),
                ],
                2,
                0,
                0,
            ),
        )
        monkeypatch.setattr(ctrl, "_do_audit", lambda state: ("issue-mesh-20260328-301", {"findings": []}))
        monkeypatch.setattr(
            ctrl._verifier,
            "check_artifact_alignment",
            lambda: (True, {"output/junit.xml": "fp-1"}),
        )
        monkeypatch.setattr(ctrl, "_load_runtime_context", lambda: {
            "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
        })
        monkeypatch.setattr(
            ctrl,
            "_do_fix",
            lambda actionable, round_id, state, runtime_context: [
                FixResult(problem_id="truth-lineage:TL-001", outcome=FixOutcome.SUCCESS, patches_applied=["app/a.py"]),
                FixResult(problem_id="runtime-anchor:RA-001", outcome=FixOutcome.SUCCESS, patches_applied=["tests/test_a.py"]),
            ],
        )
        monkeypatch.setattr(
            ctrl,
            "_apply_fix_commits",
            lambda fix_results, round_id, runtime_context, **kw: (fix_results, [{"commit_id": "c1"}, {"commit_id": "c2"}]),
        )
        monkeypatch.setattr(
            ctrl._verifier,
            "run_full_pipeline",
            lambda changed_files, round_id: VerifyResult(
                scoped_pytest_passed=True,
                full_pytest_passed=True,
                full_pytest_total=10,
                full_pytest_failed=0,
                blind_spot_clean=True,
                catalog_improved=True,
                artifacts_aligned=True,
                all_green=True,
            ),
        )
        monkeypatch.setattr(
            ctrl,
            "_do_promote",
            lambda round_id, state, runtime_context, **kwargs: {
                "status_note": {"status": "committed"},
                "current_layer": {"status": "committed"},
            },
        )

        ctrl._run_one_round(store.load())

        final = store.load()
        assert final.consecutive_fix_success_count == 2
        assert final.total_fixes == 2
        assert final.phase == LoopPhase.IDLE
        assert final.round_history[-1].all_success is True

    def test_run_one_round_resets_success_counter_when_any_fix_is_skipped(self, tmp_path: Path, monkeypatch) -> None:
        ctrl = self._make_controller(tmp_path)
        store = ctrl._store
        _stub_provider_ready(monkeypatch, ctrl)
        state = store.load()
        state.consecutive_fix_success_count = 4
        store.save(state)

        monkeypatch.setattr(
            "automation.loop_controller.controller.analyze_bundle",
            lambda bundle, fixed: (
                [
                    ProblemSpec(problem_id="truth-lineage:TL-001", severity=Severity.P1, family="truth-lineage"),
                    ProblemSpec(problem_id="runtime-anchor:RA-001", severity=Severity.P1, family="runtime-anchor"),
                ],
                2,
                0,
                0,
            ),
        )
        monkeypatch.setattr(ctrl, "_do_audit", lambda state: ("issue-mesh-20260328-302", {"findings": []}))
        monkeypatch.setattr(
            ctrl._verifier,
            "check_artifact_alignment",
            lambda: (True, {"output/junit.xml": "fp-2"}),
        )
        monkeypatch.setattr(ctrl, "_load_runtime_context", lambda: {"runtime_gates": {"status": "blocked"}})
        monkeypatch.setattr(
            ctrl,
            "_do_fix",
            lambda actionable, round_id, state, runtime_context: [
                FixResult(problem_id="truth-lineage:TL-001", outcome=FixOutcome.SUCCESS, patches_applied=["app/a.py"]),
                FixResult(problem_id="runtime-anchor:RA-001", outcome=FixOutcome.SUCCESS, patches_applied=["tests/test_a.py"]),
            ],
        )
        monkeypatch.setattr(
            ctrl,
            "_apply_fix_commits",
            lambda fix_results, round_id, runtime_context, **kw: (
                [
                    fix_results[0],
                    FixResult(
                        problem_id="runtime-anchor:RA-001",
                        outcome=FixOutcome.SKIPPED,
                        error="TRIAGE_BLOCKED:HIGH_RISK_REQUIRES_AI_ALLOW",
                    ),
                ],
                [{"commit_id": "c1"}],
            ),
        )
        monkeypatch.setattr(
            ctrl._verifier,
            "run_full_pipeline",
            lambda changed_files, round_id: VerifyResult(
                scoped_pytest_passed=True,
                full_pytest_passed=True,
                full_pytest_total=10,
                full_pytest_failed=0,
                blind_spot_clean=True,
                catalog_improved=True,
                artifacts_aligned=True,
                all_green=True,
            ),
        )
        monkeypatch.setattr(
            ctrl,
            "_do_promote",
            lambda round_id, state, runtime_context, **kwargs: {
                "status_note": {"status": "committed"},
                "current_layer": {"status": "skipped", "skip_reason": "CURRENT_LAYER_RUNTIME_GATE_BLOCKED"},
            },
        )

        ctrl._run_one_round(store.load())

        final = store.load()
        assert final.consecutive_fix_success_count == 0
        assert final.total_fixes == 1
        assert final.total_failures == 1
        assert final.round_history[-1].all_success is False
        assert "GREEN_VERDICT_FAILED" in final.round_history[-1].error


# ============================================================================
# Phase B: Business fix tests
# ============================================================================


class TestFrozenV1Tags:
    """B5: Verify frozen-v1 tags are present on contract-frozen routes."""

    def test_frozen_routes_have_tags(self) -> None:
        from app.main import app

        frozen_paths = {"/predictions/stats", "/platform/config", "/platform/plans", "/auth/me"}
        for route in app.routes:
            path = getattr(route, "path", "")
            if path in frozen_paths:
                tags = getattr(route, "tags", [])
                assert "frozen-v1" in tags, f"{path} missing frozen-v1 tag"


class TestCatalogStaleGaps:
    """B7: Verify stale gap detection in feature catalog builder."""

    def test_stale_gaps_detected_when_both_pass(self) -> None:
        """When code+test both ✅ but gaps remain, they should be flagged as stale."""
        feat = {
            "code_verdict": "✅ implemented",
            "test_verdict": "✅ covered",
            "gaps": ["some old gap"],
        }
        cv = (feat.get("code_verdict") or "")
        tv = (feat.get("test_verdict") or "")
        stale_gaps: list[str] = []
        if feat.get("gaps"):
            if cv.startswith("✅") and tv.startswith("✅"):
                stale_gaps = list(feat["gaps"])
        assert stale_gaps == ["some old gap"]

    def test_no_stale_gaps_when_code_fails(self) -> None:
        feat = {
            "code_verdict": "❌ not implemented",
            "test_verdict": "✅ covered",
            "gaps": ["real gap"],
        }
        cv = (feat.get("code_verdict") or "")
        tv = (feat.get("test_verdict") or "")
        stale_gaps: list[str] = []
        if feat.get("gaps"):
            if cv.startswith("✅") and tv.startswith("✅"):
                stale_gaps = list(feat["gaps"])
        assert stale_gaps == []

    def test_no_stale_gaps_when_no_gaps(self) -> None:
        feat = {
            "code_verdict": "✅ implemented",
            "test_verdict": "✅ covered",
            "gaps": [],
        }
        cv = (feat.get("code_verdict") or "")
        tv = (feat.get("test_verdict") or "")
        stale_gaps: list[str] = []
        if feat.get("gaps"):
            if cv.startswith("✅") and tv.startswith("✅"):
                stale_gaps = list(feat["gaps"])
        assert stale_gaps == []


class TestCleanupStaleLock:
    """B3: Verify stale lock TTL constant is defined."""

    def test_stale_lock_ttl_constant(self) -> None:
        from app.services.cleanup_service import _STALE_LOCK_TTL_MINUTES
        assert _STALE_LOCK_TTL_MINUTES == 10


# ============================================================================
# Wave 2: Success metric convergence & provider readiness
# ============================================================================


class TestVerifiedProblemFixesMetric:
    """Verify consecutive_verified_problem_fixes field and goal logic."""

    def test_loop_state_has_verified_field(self) -> None:
        state = LoopState()
        assert state.consecutive_verified_problem_fixes == 0

    def test_state_response_includes_verified_field(self) -> None:
        from automation.loop_controller.schemas import StateResponse
        resp = StateResponse(
            mode=LoopMode.FIX,
            phase=LoopPhase.IDLE,
            consecutive_fix_success_count=3,
            consecutive_verified_problem_fixes=5,
            fix_goal=10,
            total_fixes=5,
            total_failures=0,
            current_round_id=None,
            problems_queue_size=0,
            fixed_count=0,
            round_history_size=0,
            running=False,
        )
        assert resp.consecutive_verified_problem_fixes == 5

    def test_verified_field_persists_through_state_store(self, tmp_path: Path) -> None:
        store = StateStore(tmp_path / "state.json")
        state = LoopState(consecutive_verified_problem_fixes=7)
        store.save(state)
        reloaded = store.load()
        assert reloaded.consecutive_verified_problem_fixes == 7

    def test_goal_comparison_uses_verified_field(self, tmp_path: Path) -> None:
        """Only consecutive_verified_problem_fixes should trigger monitor transition."""
        _, store = TestControllerStateTransitions()._make_controller(tmp_path)
        state = store.load()
        # Old field high but new field below goal
        state.consecutive_fix_success_count = 10
        state.consecutive_verified_problem_fixes = 2
        state.fix_goal = 3
        # Simulate the goal check from controller.py
        if state.consecutive_verified_problem_fixes >= state.fix_goal:
            state.mode = LoopMode.MONITOR
        store.save(state)
        assert store.load().mode == LoopMode.FIX  # should NOT have switched

        # Now set verified field at goal
        state.consecutive_verified_problem_fixes = 3
        if state.consecutive_verified_problem_fixes >= state.fix_goal:
            state.mode = LoopMode.MONITOR
        store.save(state)
        assert store.load().mode == LoopMode.MONITOR

    def test_run_one_round_increments_verified_fixes(self, tmp_path: Path, monkeypatch) -> None:
        """Full round with green verdict must increment consecutive_verified_problem_fixes."""
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=tmp_path,
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="t",
            app_base_url="http://fake-app:38001",
            internal_token="internal-token",
            fix_goal=10,
            new_api_base_url="http://fake-newapi:3000",
            new_api_token="test-key",
        )
        store = StateStore(tmp_path / "state.json")
        ctrl = LoopController(cfg, store=store)
        _stub_provider_ready(monkeypatch, ctrl)

        monkeypatch.setattr(
            "automation.loop_controller.controller.analyze_bundle",
            lambda bundle, fixed: (
                [ProblemSpec(problem_id="test:T-001", severity=Severity.P1, family="test")],
                1, 0, 0,
            ),
        )
        monkeypatch.setattr(ctrl, "_do_audit", lambda state: ("audit-001", {"findings": []}))
        monkeypatch.setattr(ctrl._verifier, "check_artifact_alignment", lambda: (True, {}))
        monkeypatch.setattr(ctrl, "_load_runtime_context", lambda: {
            "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
        })
        monkeypatch.setattr(
            ctrl, "_do_fix",
            lambda actionable, round_id, state, runtime_context: [
                FixResult(problem_id="test:T-001", outcome=FixOutcome.SUCCESS, patches_applied=["app/a.py"]),
            ],
        )
        monkeypatch.setattr(
            ctrl, "_apply_fix_commits",
            lambda fix_results, round_id, runtime_context, **kw: (fix_results, [{"commit_id": "c1"}]),
        )
        monkeypatch.setattr(
            ctrl._verifier, "run_full_pipeline",
            lambda changed_files, round_id: VerifyResult(
                scoped_pytest_passed=True, full_pytest_passed=True, full_pytest_total=10,
                full_pytest_failed=0, blind_spot_clean=True, catalog_improved=True,
                artifacts_aligned=True, all_green=True,
            ),
        )
        monkeypatch.setattr(
            ctrl, "_do_promote",
            lambda round_id, state, runtime_context, **kwargs: {
                "status_note": {"status": "committed"},
                "current_layer": {"status": "committed"},
            },
        )

        ctrl._run_one_round(store.load())
        final = store.load()
        assert final.consecutive_verified_problem_fixes == 1
        assert final.consecutive_fix_success_count == 1

    def test_verification_failure_resets_verified_fixes(self, tmp_path: Path, monkeypatch) -> None:
        """Verification failure must reset consecutive_verified_problem_fixes to 0."""
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=tmp_path,
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="t",
            app_base_url="http://fake-app:38001",
            internal_token="internal-token",
            fix_goal=10,
            new_api_base_url="http://fake-newapi:3000",
            new_api_token="test-key",
        )
        store = StateStore(tmp_path / "state.json")
        state = LoopState(consecutive_verified_problem_fixes=5)
        store.save(state)
        ctrl = LoopController(cfg, store=store)
        _stub_provider_ready(monkeypatch, ctrl)

        monkeypatch.setattr(
            "automation.loop_controller.controller.analyze_bundle",
            lambda bundle, fixed: (
                [ProblemSpec(problem_id="test:T-002", severity=Severity.P1, family="test")],
                1, 0, 0,
            ),
        )
        monkeypatch.setattr(ctrl, "_do_audit", lambda state: ("audit-002", {"findings": []}))
        monkeypatch.setattr(ctrl._verifier, "check_artifact_alignment", lambda: (True, {}))
        monkeypatch.setattr(ctrl, "_load_runtime_context", lambda: {"runtime_gates": {"status": "blocked"}})
        monkeypatch.setattr(
            ctrl, "_do_fix",
            lambda actionable, round_id, state, runtime_context: [
                FixResult(problem_id="test:T-002", outcome=FixOutcome.SUCCESS, patches_applied=["app/b.py"]),
            ],
        )
        monkeypatch.setattr(
            ctrl, "_apply_fix_commits",
            lambda fix_results, round_id, runtime_context, **kw: (fix_results, [{"commit_id": "c1"}]),
        )
        monkeypatch.setattr(
            ctrl._verifier, "run_full_pipeline",
            lambda changed_files, round_id: VerifyResult(all_green=False),
        )
        monkeypatch.setattr(ctrl, "_rollback_fix_commits", lambda commits, round_id: None)
        monkeypatch.setattr(ctrl, "_release_lease", lambda lease_id, round_id: None)

        ctrl._run_one_round(store.load())
        final = store.load()
        assert final.consecutive_verified_problem_fixes == 0
        assert final.consecutive_fix_success_count == 0

    def test_api_state_returns_verified_field(self, tmp_path: Path) -> None:
        """The /v1/state endpoint must return consecutive_verified_problem_fixes."""
        from fastapi.testclient import TestClient
        from automation.loop_controller.controller import LoopController, LoopControllerConfig
        from automation.loop_controller.app import create_app

        cfg = LoopControllerConfig(
            repo_root=tmp_path,
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="test-token",
        )
        store = StateStore(tmp_path / "state.json")
        state = LoopState(consecutive_verified_problem_fixes=3)
        store.save(state)
        ctrl = LoopController(cfg, store=store)
        app = create_app(config=cfg, controller=ctrl)
        client = TestClient(app)

        resp = client.get("/v1/state", headers={"Authorization": "Bearer test-token"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["consecutive_verified_problem_fixes"] == 3


class TestProviderReadiness3Stage:
    """Verify the 3-stage provider readiness check (models → responses smoke)."""

    def _make_controller(self, tmp_path: Path):
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=tmp_path,
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="t",
            new_api_base_url="http://fake-newapi:3000",
            new_api_token="test-key",
        )
        store = StateStore(tmp_path / "state.json")
        return LoopController(cfg, store=store)

    def test_unconfigured_returns_unconfigured_block(self, tmp_path: Path) -> None:
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=tmp_path,
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="t",
        )
        store = StateStore(tmp_path / "state.json")
        ctrl = LoopController(cfg, store=store)
        result = ctrl._check_provider_readiness()
        assert result["ready"] is False
        assert result["status"] == "unconfigured"
        assert result["stage"] == "done"
        assert result["error"] == "NEW_API_BASE_URL_NOT_SET"

    def test_models_failure_returns_degraded(self, tmp_path: Path) -> None:
        ctrl = self._make_controller(tmp_path)
        ctrl._http = _FakeHTTP(
            get_responses=[_FakeResponse({"error": "down"}, status_code=503)],
        )
        result = ctrl._check_provider_readiness()
        assert result["ready"] is False
        assert result["stage"] == "models"
        assert "503" in result["error"]

    def test_models_ok_responses_failure_returns_safe_hold(self, tmp_path: Path) -> None:
        ctrl = self._make_controller(tmp_path)
        ctrl._http = _FakeHTTP(
            get_responses=[
                _FakeResponse({"data": [{"id": "gpt-4o"}]}),
            ],
            post_responses=[
                _FakeResponse({"error": "rate limit"}, status_code=429),
            ],
        )
        result = ctrl._check_provider_readiness()
        assert result["ready"] is False
        assert result["stage"] == "responses"
        assert result["status"] == "provider_safe_hold"
        assert "429" in result["error"]

    def test_both_stages_pass_returns_ready(self, tmp_path: Path) -> None:
        ctrl = self._make_controller(tmp_path)
        ctrl._http = _FakeHTTP(
            get_responses=[
                _FakeResponse({"data": [{"id": "gpt-4o"}]}),
            ],
            post_responses=[
                _FakeResponse({"output": [{"type": "message", "content": [{"text": "pong"}]}]}),
            ],
        )
        result = ctrl._check_provider_readiness()
        assert result["ready"] is True
        assert result["status"] == "ok"
        assert result["stage"] == "done"

    def test_empty_models_returns_degraded(self, tmp_path: Path) -> None:
        ctrl = self._make_controller(tmp_path)
        ctrl._http = _FakeHTTP(
            get_responses=[_FakeResponse({"data": []})],
        )
        result = ctrl._check_provider_readiness()
        assert result["ready"] is False
        assert result["stage"] == "models"
        assert result["error"] == "NEW_API_NO_MODELS"


class TestControlPlaneProjection:
    def _make_controller(self, tmp_path: Path):
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=tmp_path,
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="t",
            app_base_url="http://fake-app:38001",
            internal_token="internal-token",
            fix_goal=10,
            new_api_base_url="http://fake-newapi:3000",
            new_api_token="test-key",
        )
        store = StateStore(tmp_path / "state.json")
        return LoopController(cfg, store=store), store

    def test_green_round_projects_control_plane_state_and_status(self, tmp_path: Path, monkeypatch) -> None:
        ctrl, store = self._make_controller(tmp_path)
        _stub_provider_ready(monkeypatch, ctrl)

        monkeypatch.setattr(
            "automation.loop_controller.controller.analyze_bundle",
            lambda bundle, fixed: (
                [ProblemSpec(problem_id="test:T-301", severity=Severity.P1, family="test")],
                1, 0, 0,
            ),
        )
        monkeypatch.setattr(ctrl, "_do_audit", lambda state: ("audit-301", {"findings": []}))
        monkeypatch.setattr(ctrl._verifier, "check_artifact_alignment", lambda: (True, {}))
        monkeypatch.setattr(ctrl, "_load_runtime_context", lambda: {
            "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
        })
        monkeypatch.setattr(
            ctrl,
            "_do_fix",
            lambda actionable, round_id, state, runtime_context: [
                FixResult(problem_id="test:T-301", outcome=FixOutcome.SUCCESS, patches_applied=["app/c.py"]),
            ],
        )
        monkeypatch.setattr(
            ctrl,
            "_apply_fix_commits",
            lambda fix_results, round_id, runtime_context, **kw: (fix_results, [{"commit_id": "c-301"}]),
        )
        monkeypatch.setattr(
            ctrl._verifier,
            "run_full_pipeline",
            lambda changed_files, round_id: VerifyResult(
                scoped_pytest_passed=True,
                full_pytest_passed=True,
                full_pytest_total=10,
                full_pytest_failed=0,
                blind_spot_clean=True,
                catalog_improved=True,
                artifacts_aligned=True,
                all_green=True,
            ),
        )
        monkeypatch.setattr(
            ctrl,
            "_do_promote",
            lambda round_id, state, runtime_context, **kwargs: {
                "status_note": {"status": "committed"},
                "current_layer": {"status": "committed"},
            },
        )

        ctrl._run_one_round(store.load())

        final = store.load()
        current_state = json.loads(
            (tmp_path / "automation" / "control_plane" / "current_state.json").read_text(encoding="utf-8")
        )
        current_status = (tmp_path / "automation" / "control_plane" / "current_status.md").read_text(encoding="utf-8")

        assert current_state["consecutive_verified_problem_fixes"] == 1
        assert current_state["goal_progress_count"] == 1
        assert current_state["success_goal_metric"] == "verified_problem_count"
        assert current_state["goal_reached"] is False
        assert current_state["last_promote_round_id"] == final.last_promote_round_id
        assert current_state["last_round_summary"]["round_id"] == final.round_history[-1].round_id
        assert current_state["last_round_summary"]["verify_all_green"] is True
        assert "| 连续已验证问题修复数 | 1 |" in current_status
        assert "| 成功度量指标 | verified_problem_count |" in current_status

    def test_provider_safe_hold_projects_block_reason(self, tmp_path: Path, monkeypatch) -> None:
        ctrl, store = self._make_controller(tmp_path)

        monkeypatch.setattr(
            "automation.loop_controller.controller.analyze_bundle",
            lambda bundle, fixed: (
                [ProblemSpec(problem_id="test:T-302", severity=Severity.P1, family="test")],
                1, 0, 0,
            ),
        )
        monkeypatch.setattr(ctrl, "_do_audit", lambda state: ("audit-302", {"findings": []}))
        monkeypatch.setattr(ctrl._verifier, "check_artifact_alignment", lambda: (True, {}))
        monkeypatch.setattr(ctrl, "_load_runtime_context", lambda: {})
        monkeypatch.setattr(
            ctrl,
            "_check_provider_readiness",
            lambda: {
                "ready": False,
                "status": "provider_safe_hold",
                "stage": "responses",
                "error": "NEW_API_RESPONSES_SMOKE_HTTP_429",
            },
        )

        ctrl._run_one_round(store.load())

        current_state = json.loads(
            (tmp_path / "automation" / "control_plane" / "current_state.json").read_text(encoding="utf-8")
        )
        current_status = (tmp_path / "automation" / "control_plane" / "current_status.md").read_text(encoding="utf-8")

        assert current_state["mode"] == "safe_hold"
        assert current_state["phase"] == "blocked"
        assert current_state["blocked_reason"] == "NEW_API_RESPONSES_SMOKE_HTTP_429"
        assert current_state["provider_pool"]["status"] == "provider_safe_hold"
        assert current_state["last_round_summary"]["error"] == "NEW_API_RESPONSES_SMOKE_HTTP_429"
        assert "| 阻塞原因 | NEW_API_RESPONSES_SMOKE_HTTP_429 |" in current_status
        assert "| Provider 状态 | provider_safe_hold |" in current_status


# ============================================================================
# Wave 4: Long-running mode — goal auto-monitor & re-entry
# ============================================================================


class TestGoalAutoMonitor:
    """Goal reached → MONITOR (never stop), monitor re-entry on problems/drift."""

    def _make_controller(self, tmp_path: Path, fix_goal: int = 3):
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=tmp_path,
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="t",
            app_base_url="http://fake-app:38001",
            internal_token="internal-token",
            fix_goal=fix_goal,
            new_api_base_url="http://fake-newapi:3000",
            new_api_token="test-key",
        )
        store = StateStore(tmp_path / "state.json")
        return LoopController(cfg, store=store), store

    def _patch_green_round(self, ctrl, monkeypatch, *, problems_fixed: int = 2):
        """Monkey-patch a fully successful round that fixes N problems."""
        _stub_provider_ready(monkeypatch, ctrl)
        monkeypatch.setattr(
            "automation.loop_controller.controller.analyze_bundle",
            lambda bundle, fixed: (
                [ProblemSpec(problem_id=f"test:T-{i}", severity=Severity.P1, family="test")
                 for i in range(problems_fixed)],
                problems_fixed, 0, 0,
            ),
        )
        monkeypatch.setattr(ctrl, "_do_audit", lambda state: ("audit-g", {"findings": []}))
        monkeypatch.setattr(ctrl._verifier, "check_artifact_alignment", lambda: (True, {}))
        monkeypatch.setattr(ctrl, "_load_runtime_context", lambda: {
            "runtime_gates": {"status": "ready", "shared_artifact_promote": {"allowed": True}},
        })
        monkeypatch.setattr(
            ctrl, "_do_fix",
            lambda actionable, round_id, state, runtime_context: [
                FixResult(problem_id=p.problem_id, outcome=FixOutcome.SUCCESS, patches_applied=["app/x.py"])
                for p in actionable
            ],
        )
        monkeypatch.setattr(
            ctrl, "_apply_fix_commits",
            lambda fix_results, round_id, runtime_context, **kw: (
                fix_results, [{"commit_id": f"c{i}"} for i in range(len(fix_results))]
            ),
        )
        monkeypatch.setattr(
            ctrl._verifier, "run_full_pipeline",
            lambda changed_files, round_id: VerifyResult(
                scoped_pytest_passed=True, full_pytest_passed=True, full_pytest_total=10,
                full_pytest_failed=0, blind_spot_clean=True, catalog_improved=True,
                artifacts_aligned=True, all_green=True,
            ),
        )
        monkeypatch.setattr(
            ctrl, "_do_promote",
            lambda round_id, state, runtime_context, **kwargs: {
                "status_note": {"status": "committed"},
                "current_layer": {"status": "committed"},
            },
        )

    def test_goal_reached_switches_to_monitor_not_stopped(self, tmp_path: Path, monkeypatch) -> None:
        """After fix_goal verified fixes the controller switches to MONITOR but doesn't stop."""
        ctrl, store = self._make_controller(tmp_path, fix_goal=3)
        state = store.load()
        state.fix_goal = 3
        store.save(state)
        self._patch_green_round(ctrl, monkeypatch, problems_fixed=3)

        ctrl._run_one_round(store.load())

        final = store.load()
        assert final.mode == LoopMode.MONITOR
        assert final.consecutive_verified_problem_fixes == 3
        assert final.phase == LoopPhase.MONITORING
        # The stop_event should NOT be set
        assert not ctrl._stop_event.is_set()

    def test_monitor_keeps_looping_with_no_problems(self, tmp_path: Path, monkeypatch) -> None:
        """In MONITOR mode with no problems, round completes cleanly without mode change."""
        ctrl, store = self._make_controller(tmp_path, fix_goal=3)
        _stub_provider_ready(monkeypatch, ctrl)
        # Pre-set state to MONITOR
        state = store.load()
        state.mode = LoopMode.MONITOR
        state.consecutive_verified_problem_fixes = 5
        store.save(state)

        monkeypatch.setattr(ctrl, "_do_audit", lambda state: ("audit-m", {"findings": []}))
        monkeypatch.setattr(
            "automation.loop_controller.controller.analyze_bundle",
            lambda bundle, fixed: ([], 0, 0, 0),
        )
        monkeypatch.setattr(ctrl._verifier, "check_artifact_alignment", lambda: (True, {}))
        monkeypatch.setattr(ctrl, "_load_runtime_context", lambda: {})

        ctrl._run_one_round(store.load())

        final = store.load()
        assert final.mode == LoopMode.MONITOR
        assert not ctrl._stop_event.is_set()

    def test_monitor_reenter_fix_on_new_problems(self, tmp_path: Path, monkeypatch) -> None:
        """MONITOR mode detects new problems → auto-wake to FIX."""
        ctrl, store = self._make_controller(tmp_path, fix_goal=3)
        state = store.load()
        state.mode = LoopMode.MONITOR
        state.consecutive_verified_problem_fixes = 5
        store.save(state)

        self._patch_green_round(ctrl, monkeypatch, problems_fixed=1)

        ctrl._run_one_round(store.load())

        final = store.load()
        # Should have auto-woken to FIX and completed a fix round
        assert final.mode == LoopMode.FIX
        # Counters should have been reset then incremented
        assert final.consecutive_verified_problem_fixes == 1

    def test_monitor_reenter_fix_on_drift(self, tmp_path: Path, monkeypatch) -> None:
        """MONITOR mode detects artifact drift → auto-wake to FIX."""
        ctrl, store = self._make_controller(tmp_path, fix_goal=3)
        _stub_provider_ready(monkeypatch, ctrl)
        state = store.load()
        state.mode = LoopMode.MONITOR
        state.consecutive_verified_problem_fixes = 5
        state.last_artifact_fingerprints = {"output/junit.xml": "old-hash"}
        store.save(state)

        # Audit returns no problems, but drift is detected
        monkeypatch.setattr(ctrl, "_do_audit", lambda state: ("audit-d", {"findings": []}))
        monkeypatch.setattr(
            "automation.loop_controller.controller.analyze_bundle",
            lambda bundle, fixed: ([], 0, 0, 0),
        )
        # Drift: fingerprint changed from "old-hash"
        monkeypatch.setattr(
            ctrl._verifier, "check_artifact_alignment",
            lambda: (True, {"output/junit.xml": "new-hash"}),
        )
        monkeypatch.setattr(ctrl, "_load_runtime_context", lambda: {})
        monkeypatch.setattr(
            ctrl, "_do_fix",
            lambda actionable, round_id, state, runtime_context: [],
        )
        monkeypatch.setattr(
            ctrl, "_apply_fix_commits",
            lambda fix_results, round_id, runtime_context, **kw: (fix_results, []),
        )
        monkeypatch.setattr(
            ctrl._verifier, "run_full_pipeline",
            lambda changed_files, round_id: VerifyResult(all_green=True, artifacts_aligned=True),
        )
        monkeypatch.setattr(
            ctrl, "_do_promote",
            lambda round_id, state, runtime_context, **kwargs: {
                "status_note": {"status": "committed"},
                "current_layer": {"status": "skipped"},
            },
        )

        ctrl._run_one_round(store.load())

        final = store.load()
        assert final.mode == LoopMode.FIX
        assert final.consecutive_verified_problem_fixes == 0

    def test_loop_thread_continues_after_goal_reached(self, tmp_path: Path, monkeypatch) -> None:
        """Start the loop, simulate goal reached, verify thread keeps running."""
        from automation.loop_controller.controller import LoopController, LoopControllerConfig

        cfg = LoopControllerConfig(
            repo_root=tmp_path,
            mesh_runner_url="http://fake:8093",
            mesh_runner_token="t",
            promote_prep_url="http://fake:8094",
            promote_prep_token="t",
            writeback_a_url="http://fake:8092",
            writeback_a_token="t",
            writeback_b_url="http://fake:8095",
            writeback_b_token="t",
            auth_token="t",
            app_base_url="http://fake-app:38001",
            internal_token="internal-token",
            fix_goal=1,
            audit_interval_seconds=0,
            monitor_interval_seconds=0,
        )
        store = StateStore(tmp_path / "state.json")
        ctrl = LoopController(cfg, store=store)

        round_count = {"n": 0}

        def _fake_run_one_round(state):
            round_count["n"] += 1
            if round_count["n"] == 1:
                state.consecutive_verified_problem_fixes = 1
                state.mode = LoopMode.MONITOR
                state.phase = LoopPhase.MONITORING
                store.save(state)
            elif round_count["n"] >= 3:
                ctrl._stop_event.set()

        monkeypatch.setattr(ctrl, "_run_one_round", _fake_run_one_round)
        monkeypatch.setattr(ctrl, "_check_provider_readiness", lambda: {"ready": True})

        ctrl._running = True
        ctrl._stop_event.clear()
        ctrl._force_event.clear()

        thread = threading.Thread(target=ctrl._run_loop, daemon=True)
        thread.start()
        thread.join(timeout=10)

        assert round_count["n"] >= 3, f"Loop only ran {round_count['n']} rounds, expected >= 3"
        assert not thread.is_alive()
