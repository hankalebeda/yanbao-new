"""Tests for lease/fencing validation and controller green-round evaluation."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from automation.loop_controller.controller import LoopController, LoopControllerConfig
from automation.loop_controller.schemas import (
    FixOutcome,
    FixResult,
    GreenRoundVerdict,
    LoopMode,
    LoopState,
    ProblemSpec,
    RoundSummary,
    Severity,
    VerifyResult,
)
from automation.loop_controller.state import StateStore


def _make_ctrl(tmp_path: Path) -> LoopController:
    cfg = LoopControllerConfig(
        repo_root=tmp_path,
        mesh_runner_url="http://127.0.0.1:18093",
        mesh_runner_token="",
        promote_prep_url="http://127.0.0.1:18094",
        promote_prep_token="",
        writeback_a_url="http://127.0.0.1:18092",
        writeback_a_token="",
        writeback_b_url="http://127.0.0.1:18095",
        writeback_b_token="",
        auth_token="",
        fix_goal=10,
    )
    store = StateStore(tmp_path / "state.json")
    return LoopController(cfg, store=store)


class TestEvaluateGreenRound:
    def test_fully_green_round(self, tmp_path: Path) -> None:
        ctrl = _make_ctrl(tmp_path)
        summary = RoundSummary(
            round_id="test-001",
            started_at="2026-01-01T00:00:00",
            problems_fixed=2,
            problems_failed=0,
            problems_skipped=0,
            fix_results=[
                FixResult(problem_id="p1", outcome=FixOutcome.SUCCESS),
                FixResult(problem_id="p2", outcome=FixOutcome.SUCCESS),
            ],
        )
        verify = VerifyResult(all_green=True, artifacts_aligned=True)
        # Actionable problems are the ones we found and fixed — not leftover
        actionable_problems = [
            ProblemSpec(problem_id="p1", severity=Severity.P1),
            ProblemSpec(problem_id="p2", severity=Severity.P1),
        ]
        result = ctrl._evaluate_green_round(
            summary=summary,
            verify=verify,
            applied_commits=[{"commit_id": "c1"}, {"commit_id": "c2"}],
            promote_result={
                "status_note": {"status": "committed"},
                "current_layer": {"status": "committed"},
            },
            drifted=[],
            actionable=actionable_problems,
            state=LoopState(),
        )
        assert result.is_green is True

    def test_partial_fail_blocks_green(self, tmp_path: Path) -> None:
        ctrl = _make_ctrl(tmp_path)
        summary = RoundSummary(
            round_id="test-002",
            started_at="2026-01-01T00:00:00",
            problems_fixed=1,
            problems_failed=1,
            problems_skipped=0,
            fix_results=[
                FixResult(problem_id="p1", outcome=FixOutcome.SUCCESS),
                FixResult(problem_id="p2", outcome=FixOutcome.FAILED, error="test"),
            ],
        )
        verify = VerifyResult(all_green=True, artifacts_aligned=True)
        result = ctrl._evaluate_green_round(
            summary=summary,
            verify=verify,
            applied_commits=[{"commit_id": "c1"}],
            promote_result={
                "status_note": {"status": "committed"},
                "current_layer": {"status": "skipped"},
            },
            drifted=[],
            actionable=[],
            state=LoopState(),
        )
        assert result.is_green is False
        assert result.no_partial_fail is False

    def test_drift_blocks_green(self, tmp_path: Path) -> None:
        ctrl = _make_ctrl(tmp_path)
        summary = RoundSummary(
            round_id="test-003",
            started_at="2026-01-01T00:00:00",
            problems_fixed=1,
            problems_failed=0,
            problems_skipped=0,
            fix_results=[
                FixResult(problem_id="p1", outcome=FixOutcome.SUCCESS),
            ],
        )
        verify = VerifyResult(all_green=True, artifacts_aligned=True)
        result = ctrl._evaluate_green_round(
            summary=summary,
            verify=verify,
            applied_commits=[{"commit_id": "c1"}],
            promote_result={
                "status_note": {"status": "committed"},
                "current_layer": {"status": "committed"},
            },
            drifted=["output/junit.xml"],
            actionable=[],
            state=LoopState(),
        )
        assert result.is_green is False
        assert result.no_drift is False

    def test_promote_fail_blocks_green(self, tmp_path: Path) -> None:
        ctrl = _make_ctrl(tmp_path)
        summary = RoundSummary(
            round_id="test-004",
            started_at="2026-01-01T00:00:00",
            problems_fixed=1,
            problems_failed=0,
            problems_skipped=0,
            fix_results=[
                FixResult(problem_id="p1", outcome=FixOutcome.SUCCESS),
            ],
        )
        verify = VerifyResult(all_green=True, artifacts_aligned=True)
        result = ctrl._evaluate_green_round(
            summary=summary,
            verify=verify,
            applied_commits=[{"commit_id": "c1"}],
            promote_result={
                "status_note": {"status": "failed", "error": "BOOM"},
                "current_layer": {"status": "committed"},
            },
            drifted=[],
            actionable=[],
            state=LoopState(),
        )
        assert result.is_green is False
        assert result.promote_gate_passed is False

    def test_masked_skip_blocks_green(self, tmp_path: Path) -> None:
        ctrl = _make_ctrl(tmp_path)
        summary = RoundSummary(
            round_id="test-005",
            started_at="2026-01-01T00:00:00",
            problems_fixed=1,
            problems_failed=0,
            problems_skipped=1,
            fix_results=[
                FixResult(problem_id="p1", outcome=FixOutcome.SUCCESS),
                FixResult(problem_id="p2", outcome=FixOutcome.SKIPPED),  # no error → masked
            ],
        )
        verify = VerifyResult(all_green=True, artifacts_aligned=True)
        result = ctrl._evaluate_green_round(
            summary=summary,
            verify=verify,
            applied_commits=[{"commit_id": "c1"}],
            promote_result={
                "status_note": {"status": "committed"},
                "current_layer": {"status": "committed"},
            },
            drifted=[],
            actionable=[],
            state=LoopState(),
        )
        assert result.is_green is False
        assert result.no_masked_skip is False
        assert result.no_partial_fail is False  # skipped count > 0

    def test_zero_fixes_with_zero_commits_is_batch_complete(self, tmp_path: Path) -> None:
        ctrl = _make_ctrl(tmp_path)
        summary = RoundSummary(
            round_id="test-006",
            started_at="2026-01-01T00:00:00",
            problems_fixed=0,
            problems_failed=0,
            problems_skipped=0,
            fix_results=[],
        )
        verify = VerifyResult(all_green=True, artifacts_aligned=True)
        result = ctrl._evaluate_green_round(
            summary=summary,
            verify=verify,
            applied_commits=[],
            promote_result={
                "status_note": {"status": "committed"},
                "current_layer": {"status": "committed"},
            },
            drifted=[],
            actionable=[],
            state=LoopState(),
        )
        assert result.batch_writeback_complete is True
