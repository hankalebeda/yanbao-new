"""Tests for GreenRoundVerdict — the 9-item autonomous completeness gate."""
from __future__ import annotations

from automation.loop_controller.schemas import GreenRoundVerdict


class TestGreenRoundVerdict:
    def test_all_true_is_green(self) -> None:
        v = GreenRoundVerdict(
            no_new_active=True,
            no_regression=True,
            no_drift=True,
            verify_all_green=True,
            no_partial_fail=True,
            no_masked_skip=True,
            batch_writeback_complete=True,
            promote_gate_passed=True,
            artifacts_aligned=True,
        )
        assert v.is_green is True

    def test_single_false_is_not_green(self) -> None:
        for field in [
            "no_new_active",
            "no_regression",
            "no_drift",
            "verify_all_green",
            "no_partial_fail",
            "no_masked_skip",
            "batch_writeback_complete",
            "promote_gate_passed",
            "artifacts_aligned",
        ]:
            kwargs = {
                "no_new_active": True,
                "no_regression": True,
                "no_drift": True,
                "verify_all_green": True,
                "no_partial_fail": True,
                "no_masked_skip": True,
                "batch_writeback_complete": True,
                "promote_gate_passed": True,
                "artifacts_aligned": True,
            }
            kwargs[field] = False
            v = GreenRoundVerdict(**kwargs)
            assert v.is_green is False, f"Expected not green when {field}=False"

    def test_defaults_all_false(self) -> None:
        v = GreenRoundVerdict()
        assert v.is_green is False
        assert v.no_new_active is False
        assert v.artifacts_aligned is False
