"""Tests for the unified Round State Machine.

Validates FSM transitions, atomic round lifecycle, control state
management, evidence chain integrity, and error handling.
"""
import json
import pytest
from pathlib import Path

from automation.agents.state_machine import (
    ControlState,
    InvalidTransitionError,
    RoundPhase,
    RoundState,
    RoundStateManager,
    SystemControlState,
)


# -----------------------------------------------------------------------
# RoundState FSM
# -----------------------------------------------------------------------

class TestRoundState:
    """Unit tests for RoundState FSM transitions."""

    def test_initial_state_is_pending(self):
        rs = RoundState(round_id="r-001")
        assert rs.phase == RoundPhase.PENDING.value

    def test_valid_forward_transition(self):
        rs = RoundState(round_id="r-001")
        rs.transition(RoundPhase.DISCOVERED)
        assert rs.phase == RoundPhase.DISCOVERED.value
        assert len(rs.history) == 1
        assert rs.history[0]["from"] == "pending"
        assert rs.history[0]["to"] == "discovered"

    def test_invalid_transition_raises(self):
        rs = RoundState(round_id="r-001")
        with pytest.raises(InvalidTransitionError, match="Cannot transition"):
            rs.transition(RoundPhase.PROMOTED)

    def test_full_happy_path(self):
        """Walk through the entire success path."""
        rs = RoundState(round_id="r-full")
        phases = [
            RoundPhase.DISCOVERED,
            RoundPhase.ANALYSED,
            RoundPhase.FIXING,
            RoundPhase.FIXED,
            RoundPhase.VERIFYING,
            RoundPhase.VERIFIED,
            RoundPhase.WRITING_BACK,
            RoundPhase.WRITTEN_BACK,
            RoundPhase.PROMOTING,
            RoundPhase.PROMOTED,
            RoundPhase.CLOSED,
        ]
        for phase in phases:
            rs.transition(phase)
        assert rs.phase == RoundPhase.CLOSED.value
        assert rs.is_terminal()
        assert len(rs.history) == len(phases)

    def test_fail_from_any_non_terminal(self):
        """Can transition to FAILED from any active phase."""
        for start_phase in [
            RoundPhase.PENDING, RoundPhase.DISCOVERED, RoundPhase.ANALYSED,
            RoundPhase.FIXING, RoundPhase.FIXED, RoundPhase.VERIFYING,
            RoundPhase.VERIFIED, RoundPhase.WRITING_BACK, RoundPhase.WRITTEN_BACK,
            RoundPhase.PROMOTING,
        ]:
            rs = RoundState(round_id="r-fail")
            rs.phase = start_phase.value
            rs.transition(RoundPhase.FAILED)
            assert rs.phase == RoundPhase.FAILED.value

    def test_cannot_transition_from_closed(self):
        rs = RoundState(round_id="r-closed")
        rs.phase = RoundPhase.CLOSED.value
        with pytest.raises(InvalidTransitionError):
            rs.transition(RoundPhase.PENDING)

    def test_retry_from_failed(self):
        rs = RoundState(round_id="r-retry")
        rs.transition(RoundPhase.DISCOVERED)
        rs.transition(RoundPhase.FAILED)
        # Can retry (go back to PENDING)
        rs.transition(RoundPhase.PENDING)
        assert rs.phase == RoundPhase.PENDING.value

    def test_evidence_hash_changes_on_update(self):
        rs = RoundState(round_id="r-hash")
        rs.update_evidence(problem_count=5, patch_count=3)
        h1 = rs.evidence_hash
        rs.update_evidence(problem_count=5, patch_count=4)
        h2 = rs.evidence_hash
        assert h1 != h2

    def test_checkpoint_recorded_on_transition(self):
        rs = RoundState(round_id="r-cp")
        rs.transition(RoundPhase.DISCOVERED)
        rs.transition(RoundPhase.ANALYSED)
        assert rs.checkpoint_phase == RoundPhase.DISCOVERED.value

    def test_serialization_roundtrip(self):
        rs = RoundState(round_id="r-ser")
        rs.transition(RoundPhase.DISCOVERED)
        rs.update_evidence(problem_count=10)
        d = rs.to_dict()
        rs2 = RoundState.from_dict(d)
        assert rs2.round_id == "r-ser"
        assert rs2.phase == RoundPhase.DISCOVERED.value
        assert rs2.problem_count == 10

    def test_discovered_can_close_directly(self):
        """When no problems found, round goes DISCOVERED → CLOSED."""
        rs = RoundState(round_id="r-green")
        rs.transition(RoundPhase.DISCOVERED)
        rs.transition(RoundPhase.CLOSED)
        assert rs.is_terminal()


# -----------------------------------------------------------------------
# SystemControlState
# -----------------------------------------------------------------------

class TestSystemControlState:
    """Tests for system-level control state transitions."""

    def test_initial_state(self):
        scs = SystemControlState()
        assert scs.state == ControlState.RECOVERY_REARM.value

    def test_valid_transitions(self):
        scs = SystemControlState()
        scs.transition(ControlState.RECOVERY_EXECUTING)
        assert scs.state == ControlState.RECOVERY_EXECUTING.value
        scs.transition(ControlState.PROMOTE_READY)
        assert scs.state == ControlState.PROMOTE_READY.value
        scs.transition(ControlState.RESIDENCY)
        assert scs.state == ControlState.RESIDENCY.value

    def test_invalid_transition(self):
        scs = SystemControlState()
        with pytest.raises(InvalidTransitionError):
            scs.transition(ControlState.RESIDENCY)  # skip RECOVERY_EXECUTING

    def test_fix_goal_tracking(self):
        scs = SystemControlState(fix_goal=3)
        scs.transition(ControlState.RECOVERY_EXECUTING)
        assert not scs.should_promote()
        scs.record_fix(2)
        assert not scs.should_promote()
        scs.record_fix(1)
        assert scs.should_promote()

    def test_green_round_tracking(self):
        scs = SystemControlState()
        scs.record_green_round()
        scs.record_green_round()
        assert scs.consecutive_green == 2
        scs.reset_green()
        assert scs.consecutive_green == 0

    def test_residency_entered_timestamp(self):
        scs = SystemControlState()
        scs.transition(ControlState.RECOVERY_EXECUTING)
        scs.transition(ControlState.PROMOTE_READY)
        scs.transition(ControlState.RESIDENCY)
        assert scs.residency_entered_at != ""

    def test_regression_recovery(self):
        """From RESIDENCY, regression sends back to RECOVERY_EXECUTING."""
        scs = SystemControlState()
        scs.transition(ControlState.RECOVERY_EXECUTING)
        scs.transition(ControlState.PROMOTE_READY)
        scs.transition(ControlState.RESIDENCY)
        scs.transition(ControlState.RECOVERY_EXECUTING)
        assert scs.state == ControlState.RECOVERY_EXECUTING.value

    def test_history_recorded(self):
        scs = SystemControlState()
        scs.transition(ControlState.RECOVERY_EXECUTING)
        assert len(scs.history) == 1
        assert scs.history[0]["from"] == ControlState.RECOVERY_REARM.value
        assert scs.history[0]["to"] == ControlState.RECOVERY_EXECUTING.value

    def test_serialization_roundtrip(self):
        scs = SystemControlState(fix_goal=5)
        scs.record_fix(3)
        d = scs.to_dict()
        scs2 = SystemControlState.from_dict(d)
        assert scs2.fix_goal == 5
        assert scs2.fixes_achieved == 3


# -----------------------------------------------------------------------
# RoundStateManager (persistence)
# -----------------------------------------------------------------------

class TestRoundStateManager:
    """Tests for RoundStateManager with file persistence."""

    def test_begin_and_close_round(self, tmp_path: Path):
        mgr = RoundStateManager(tmp_path / "rsm")
        rs = mgr.begin_round("r-001")
        assert rs.round_id == "r-001"
        assert rs.phase == RoundPhase.PENDING.value

        mgr.advance(RoundPhase.DISCOVERED)
        mgr.advance(RoundPhase.CLOSED)
        assert mgr.active_round.is_terminal()

    def test_cannot_begin_while_active(self, tmp_path: Path):
        mgr = RoundStateManager(tmp_path / "rsm")
        mgr.begin_round("r-001")
        mgr.advance(RoundPhase.DISCOVERED)
        with pytest.raises(RuntimeError, match="still active"):
            mgr.begin_round("r-002")

    def test_can_begin_after_closed(self, tmp_path: Path):
        mgr = RoundStateManager(tmp_path / "rsm")
        mgr.begin_round("r-001")
        mgr.advance(RoundPhase.DISCOVERED)
        mgr.advance(RoundPhase.CLOSED)
        rs2 = mgr.begin_round("r-002")
        assert rs2.round_id == "r-002"

    def test_fail_round(self, tmp_path: Path):
        mgr = RoundStateManager(tmp_path / "rsm")
        mgr.begin_round("r-fail")
        mgr.advance(RoundPhase.DISCOVERED)
        mgr.fail_round("test error")
        assert mgr.active_round.phase == RoundPhase.FAILED.value
        assert mgr.active_round.error == "test error"

    def test_persistence(self, tmp_path: Path):
        state_dir = tmp_path / "rsm"
        mgr = RoundStateManager(state_dir)
        mgr.begin_round("r-persist")
        mgr.advance(RoundPhase.DISCOVERED, problem_count=5)
        mgr.advance(RoundPhase.CLOSED)

        # Check files exist
        assert (state_dir / "active_round.json").exists()

    def test_control_state_persistence(self, tmp_path: Path):
        state_dir = tmp_path / "rsm"
        mgr = RoundStateManager(state_dir)
        mgr.advance_control(ControlState.RECOVERY_EXECUTING)

        # Reload
        mgr2 = RoundStateManager(state_dir)
        assert mgr2.control.state == ControlState.RECOVERY_EXECUTING.value

    def test_advance_with_evidence(self, tmp_path: Path):
        mgr = RoundStateManager(tmp_path / "rsm")
        mgr.begin_round("r-evi")
        mgr.advance(RoundPhase.DISCOVERED, problem_count=3)
        assert mgr.active_round.problem_count == 3
        assert mgr.active_round.evidence_hash != ""
