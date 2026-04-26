"""Tests for ResidencyController — long-term autonomous monitoring.

Validates phase transitions, scan interval logic, wake/sleep cycles,
metric tracking, and persistence.
"""
import pytest
from pathlib import Path

from automation.agents.residency import (
    ResidencyController,
    ResidencyPhase,
    ResidencyState,
    SCAN_TIER_ACTIVE,
    SCAN_TIER_WARM,
    SCAN_TIER_IDLE,
    SCAN_TIER_DEEP_SLEEP,
    WARM_TO_IDLE_SCANS,
    IDLE_TO_DEEP_SCANS,
)


class TestResidencyController:
    """Tests for ResidencyController lifecycle."""

    def test_initial_state(self, tmp_path: Path):
        rc = ResidencyController(tmp_path)
        assert rc.phase == ResidencyPhase.ACTIVE
        assert rc.is_active

    def test_enter_residency(self, tmp_path: Path):
        rc = ResidencyController(tmp_path)
        rc.enter_residency()
        assert rc.phase == ResidencyPhase.WARM

    def test_warm_to_idle_transition(self, tmp_path: Path):
        rc = ResidencyController(tmp_path)
        rc.enter_residency()  # → WARM
        for _ in range(WARM_TO_IDLE_SCANS):
            rc.record_clean_scan()
        assert rc.phase == ResidencyPhase.IDLE

    def test_idle_to_deep_sleep(self, tmp_path: Path):
        rc = ResidencyController(tmp_path)
        rc.enter_residency()  # → WARM
        total_scans = WARM_TO_IDLE_SCANS + IDLE_TO_DEEP_SCANS
        for _ in range(total_scans):
            rc.record_clean_scan()
        assert rc.phase == ResidencyPhase.DEEP_SLEEP

    def test_wake_resets_to_active(self, tmp_path: Path):
        rc = ResidencyController(tmp_path)
        rc.enter_residency()
        for _ in range(WARM_TO_IDLE_SCANS):
            rc.record_clean_scan()
        assert rc.phase == ResidencyPhase.IDLE

        rc.wake("regression detected")
        assert rc.phase == ResidencyPhase.ACTIVE
        assert rc.is_active

    def test_wake_increments_count(self, tmp_path: Path):
        rc = ResidencyController(tmp_path)
        rc.enter_residency()
        rc.wake("issue 1")
        rc.wake("issue 2")
        assert rc.stats["total_wakes"] == 2

    def test_fix_success_transitions_to_warm(self, tmp_path: Path):
        rc = ResidencyController(tmp_path)
        rc.record_fix_success()
        assert rc.phase == ResidencyPhase.WARM

    def test_scan_interval_by_phase(self, tmp_path: Path):
        rc = ResidencyController(tmp_path)
        assert rc.next_scan_interval() == SCAN_TIER_ACTIVE

        rc.enter_residency()
        assert rc.next_scan_interval() == SCAN_TIER_WARM

        for _ in range(WARM_TO_IDLE_SCANS):
            rc.record_clean_scan()
        assert rc.next_scan_interval() == SCAN_TIER_IDLE

    def test_should_scan_initial(self, tmp_path: Path):
        rc = ResidencyController(tmp_path)
        assert rc.should_scan()  # always True initially

    def test_clean_streak_tracking(self, tmp_path: Path):
        rc = ResidencyController(tmp_path)
        rc.enter_residency()
        for _ in range(5):
            rc.record_clean_scan()
        assert rc.stats["consecutive_clean"] == 5
        assert rc.stats["longest_streak"] == 5

        rc.wake("issue")
        assert rc.stats["consecutive_clean"] == 0
        assert rc.stats["longest_streak"] == 5  # preserved

    def test_finding_recorded(self, tmp_path: Path):
        rc = ResidencyController(tmp_path)
        rc.record_finding(count=3)
        assert rc.stats["total_scans"] == 1

    def test_persistence(self, tmp_path: Path):
        rc1 = ResidencyController(tmp_path)
        rc1.enter_residency()
        for _ in range(3):
            rc1.record_clean_scan()

        # Reload
        rc2 = ResidencyController(tmp_path)
        assert rc2.stats["consecutive_clean"] == 3
        assert rc2.phase == ResidencyPhase.WARM

    def test_stats_summary(self, tmp_path: Path):
        rc = ResidencyController(tmp_path)
        s = rc.stats
        assert "phase" in s
        assert "consecutive_clean" in s
        assert "total_wakes" in s
        assert "total_fixes" in s
        assert "total_scans" in s


class TestResidencyState:
    """Tests for ResidencyState serialization."""

    def test_serialization_roundtrip(self):
        rs = ResidencyState(
            phase=ResidencyPhase.IDLE,
            total_wake_cycles=3,
            consecutive_clean_scans=10,
        )
        d = rs.to_dict()
        rs2 = ResidencyState.from_dict(d)
        assert rs2.phase == ResidencyPhase.IDLE
        assert rs2.total_wake_cycles == 3
        assert rs2.consecutive_clean_scans == 10
