"""FR07-SETTLE-02/03/06 supplemental acceptance tests.

Verifies:
  - display_hint returns Chinese '样本积累中' when sample < 30 (FR07-SETTLE-02)
  - win_rate denominator excludes zero returns (FR07-SETTLE-03)
  - profit_loss_ratio uses ZERO_RETURN_THRESHOLD correctly (FR07-SETTLE-03)
  - Monte Carlo baseline with sample < 30 returns null metrics (FR07-SETTLE-06)
  - Monte Carlo baseline with sample >= 30 runs real simulation (FR07-SETTLE-06)
  - baseline_ma_cross_metrics with real signal returns metrics (FR07-SETTLE-06)
"""
from __future__ import annotations

import pytest

from app.services.fr07_metrics import (
    FR07_SAMPLE_ACCUMULATING_HINT,
    ZERO_RETURN_THRESHOLD,
    annualized_return_from_cumulative,
    build_metric_payload,
)
from app.services.settlement_ssot import baseline_ma_cross_metrics, baseline_random_metrics


# ---------------------------------------------------------------------------
# FR07-SETTLE-02 — display_hint value must be Chinese '样本积累中'
# ---------------------------------------------------------------------------


@pytest.mark.feature("FR07-SETTLE-02")
def test_fr07_display_hint_is_chinese_when_sample_lt_30():
    """display_hint must equal '样本积累中' (not a code key) when sample < 30."""
    returns = [0.02, -0.01, 0.03]  # only 3 returns → sample < 30
    payload = build_metric_payload(returns, trade_day_count=10, sample_size=3)
    assert payload["display_hint"] == "样本积累中", (
        f"Expected '样本积累中' but got {payload['display_hint']!r}"
    )
    assert payload["win_rate"] is None
    assert payload["profit_loss_ratio"] is None


@pytest.mark.feature("FR07-SETTLE-02")
def test_fr07_display_hint_is_none_when_sample_gte_30():
    """display_hint must be None when sample >= 30."""
    returns = [0.02] * 20 + [-0.01] * 10  # 30 returns, sample_size=30
    payload = build_metric_payload(returns, trade_day_count=30, sample_size=30)
    assert payload["display_hint"] is None
    assert payload["win_rate"] is not None


@pytest.mark.feature("FR07-SETTLE-02")
def test_fr07_annualized_return_fail_closes_absurd_values():
    """极端短窗年化会制造源快照假值，必须直接置空。"""
    assert annualized_return_from_cumulative(0.03, trade_day_count=1) is None


@pytest.mark.feature("FR07-SETTLE-02")
def test_fr07_sample_accumulating_hint_constant_value():
    """FR07_SAMPLE_ACCUMULATING_HINT constant must equal '样本积累中'."""
    assert FR07_SAMPLE_ACCUMULATING_HINT == "样本积累中"


# ---------------------------------------------------------------------------
# FR07-SETTLE-03 — win_rate denominator excludes zero returns
# ---------------------------------------------------------------------------


@pytest.mark.feature("FR07-SETTLE-03")
def test_fr07_win_rate_excludes_zero_returns():
    """win_rate = wins / (wins + losses) where ties (|r| <= THRESHOLD) excluded."""
    # scale to 30+ for sample_size to avoid the <30 early-return path
    # 20 wins, 10 losses, 20 zeros — denominator should be 30 (not 50)
    returns = [0.05] * 20 + [-0.02] * 10 + [0.0] * 20  # 50 total, 30 decisive
    payload = build_metric_payload(returns, trade_day_count=30, sample_size=len(returns))
    expected_win_rate = round(20 / 30, 6)
    assert payload["win_rate"] == expected_win_rate, (
        f"Expected win_rate {expected_win_rate} but got {payload['win_rate']}"
    )


@pytest.mark.feature("FR07-SETTLE-03")
def test_fr07_win_rate_uses_zero_return_threshold():
    """Returns <= ZERO_RETURN_THRESHOLD must be treated as zero (excluded from denominator)."""
    threshold = ZERO_RETURN_THRESHOLD  # 0.0001
    # Only one return exactly equal to threshold → treated as zero, not a win
    near_zero = threshold  # exactly at boundary → NOT a win (> threshold required)
    just_above = threshold + 1e-6   # just above → counts as win
    returns = [just_above] * 15 + [near_zero] * 15  # 15 wins + 15 near-zero
    payload = build_metric_payload(returns, trade_day_count=30, sample_size=30)
    # No losses → profit_loss_ratio None; win_rate = 15/15 = 1.0 (only wins count)
    assert payload["win_rate"] == 1.0


@pytest.mark.feature("FR07-SETTLE-03")
def test_fr07_profit_loss_ratio_uses_threshold():
    """profit_loss_ratio computed from wins and losses excluding near-zero returns."""
    # 20 wins avg 0.04, 10 losses avg 0.02 → ratio 0.04/0.02 = 2.0
    wins = [0.04] * 20
    losses = [-0.02] * 10
    zeros = [0.00005] * 5  # < ZERO_RETURN_THRESHOLD → excluded
    returns = wins + losses + zeros
    payload = build_metric_payload(returns, trade_day_count=30, sample_size=len(returns))
    assert payload["profit_loss_ratio"] is not None
    # avg_win = 0.04, avg_loss = 0.02, ratio = 2.0
    assert round(payload["profit_loss_ratio"], 1) == 2.0


@pytest.mark.feature("FR07-SETTLE-03")
def test_fr07_win_rate_is_none_when_all_returns_are_zero():
    """win_rate must be None when all returns are near-zero (no decisive positions)."""
    returns = [0.0] * 30
    payload = build_metric_payload(returns, trade_day_count=30, sample_size=30)
    assert payload["win_rate"] is None


# ---------------------------------------------------------------------------
# FR07-SETTLE-06 — Monte Carlo baseline (sample < 30 → null, >= 30 → real simulation)
# ---------------------------------------------------------------------------


@pytest.mark.feature("FR07-SETTLE-06")
def test_fr07_baseline_random_returns_null_when_sample_lt_30():
    """baseline_random_metrics with sample < 30 must return null metrics and display_hint."""
    results = [{"net_return_pct": 0.02, "strategy_type": "A"}] * 5  # only 5
    output = baseline_random_metrics(results, window_days=20)
    assert output["win_rate"] is None
    assert output["profit_loss_ratio"] is None
    assert output["sample_size"] == 5
    assert output["simulation_runs"] == 500
    assert output["display_hint"] == "样本积累中"


@pytest.mark.feature("FR07-SETTLE-06")
def test_fr07_baseline_random_returns_real_metrics_when_sample_gte_30():
    """baseline_random_metrics with sample >= 30 must produce real computed metrics."""
    results = [{"net_return_pct": 0.03 if i % 3 != 0 else -0.01, "strategy_type": "A"}
               for i in range(40)]  # 40 samples
    output = baseline_random_metrics(results, window_days=20)
    assert output["win_rate"] is not None
    assert 0.0 < output["win_rate"] < 1.0
    assert output["sample_size"] == 40
    assert output["simulation_runs"] == 500
    assert output["display_hint"] is None


@pytest.mark.feature("FR07-SETTLE-06")
def test_fr07_baseline_random_returns_empty_when_no_results():
    """baseline_random_metrics with empty results must return all-null metrics."""
    output = baseline_random_metrics([], window_days=20)
    assert output["win_rate"] is None
    assert output["sample_size"] == 0


@pytest.mark.feature("FR07-SETTLE-06")
def test_fr07_baseline_ma_cross_returns_real_metrics():
    """baseline_ma_cross_metrics with >= 30 results must compute real metrics."""
    results = [{"net_return_pct": 0.02 if i % 4 != 0 else -0.015, "strategy_type": "B"}
               for i in range(40)]
    output = baseline_ma_cross_metrics(results, window_days=20)
    assert output["baseline_type"] == "baseline_ma_cross"
    assert output["win_rate"] is not None
    assert output["simulation_runs"] is None  # MA cross is not Monte Carlo


@pytest.mark.feature("FR07-SETTLE-06")
def test_fr07_baseline_ma_cross_returns_null_when_sample_lt_30():
    """baseline_ma_cross_metrics with sample < 30 must return null metrics."""
    results = [{"net_return_pct": 0.02, "strategy_type": "B"}] * 10
    output = baseline_ma_cross_metrics(results, window_days=20)
    assert output["win_rate"] is None
    assert output["display_hint"] == "样本积累中"
