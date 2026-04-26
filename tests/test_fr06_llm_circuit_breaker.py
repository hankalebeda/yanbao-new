"""FR06-LLM-02 acceptance tests.

Verifies:
  - Global LLM circuit breaker opens after N consecutive failures (FR06-LLM-02)
  - Circuit breaker blocks requests when open (FR06-LLM-02)
  - Circuit breaker auto-resets after OPEN_SECONDS (FR06-LLM-02)
  - Circuit breaker resets immediately on success (FR06-LLM-02)
  - prior_stats is None when settled_result sample_count < 30 (FR06-LLM-02)
  - prior_stats contains required fields when sample_count >= 30 (FR06-LLM-02)
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pytest

from app.services.llm_router import (
    GlobalLLMCircuitBreaker,
    _CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    _CIRCUIT_BREAKER_OPEN_SECONDS,
    _CIRCUIT_BREAKER_FAILURE_WINDOW_SECONDS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fresh_breaker() -> GlobalLLMCircuitBreaker:
    """Return a fresh independent circuit breaker for each test."""
    return GlobalLLMCircuitBreaker()


# ---------------------------------------------------------------------------
# FR06-LLM-02 — Circuit breaker behaviour
# ---------------------------------------------------------------------------


@pytest.mark.feature("FR06-LLM-02")
def test_fr06_circuit_breaker_opens_after_threshold_failures():
    """After THRESHOLD consecutive failures the breaker must open."""
    cb = _fresh_breaker()
    now = 1000.0  # synthetic timestamp

    # Record THRESHOLD failures within the window
    for i in range(_CIRCUIT_BREAKER_FAILURE_THRESHOLD):
        cb.record_failure(now=now + float(i))

    # Now the breaker must be open: before_request should raise
    with pytest.raises(RuntimeError, match="circuit breaker is open"):
        cb.before_request(now=now + float(_CIRCUIT_BREAKER_FAILURE_THRESHOLD))


@pytest.mark.feature("FR06-LLM-02")
def test_fr06_circuit_breaker_does_not_open_below_threshold():
    """Fewer than THRESHOLD failures within window must NOT open breaker."""
    cb = _fresh_breaker()
    now = 1000.0

    for i in range(_CIRCUIT_BREAKER_FAILURE_THRESHOLD - 1):
        cb.record_failure(now=now + float(i))

    # Should not raise — breaker still closed
    cb.before_request(now=now + float(_CIRCUIT_BREAKER_FAILURE_THRESHOLD - 1))


@pytest.mark.feature("FR06-LLM-02")
def test_fr06_circuit_breaker_auto_resets_after_open_seconds():
    """Circuit breaker must auto-reset after OPEN_SECONDS have elapsed."""
    cb = _fresh_breaker()
    now = 1000.0

    # Trip the breaker: 3 failures at now, now+1, now+2
    # Breaker opens until now+2 + OPEN_SECONDS
    for i in range(_CIRCUIT_BREAKER_FAILURE_THRESHOLD):
        cb.record_failure(now=now + float(i))

    trip_time = now + float(_CIRCUIT_BREAKER_FAILURE_THRESHOLD - 1)
    open_until = trip_time + _CIRCUIT_BREAKER_OPEN_SECONDS

    # Still open just before expiry
    with pytest.raises(RuntimeError):
        cb.before_request(now=open_until - 1)

    # After OPEN_SECONDS past the last failure the breaker should accept
    cb.before_request(now=open_until + 1)


@pytest.mark.feature("FR06-LLM-02")
def test_fr06_circuit_breaker_resets_on_success():
    """A successful call must clear failure timestamps and close the breaker."""
    cb = _fresh_breaker()
    now = 1000.0

    # Record failures (but not enough to trip yet)
    for i in range(_CIRCUIT_BREAKER_FAILURE_THRESHOLD - 1):
        cb.record_failure(now=now + float(i))

    # Record success — should clear all failures
    cb.record_success(now=now + float(_CIRCUIT_BREAKER_FAILURE_THRESHOLD))

    # Now record THRESHOLD - 1 more failures; should NOT trip (counters reset)
    for i in range(_CIRCUIT_BREAKER_FAILURE_THRESHOLD - 1):
        cb.record_failure(now=now + 100.0 + float(i))

    cb.before_request(now=now + 100.0 + float(_CIRCUIT_BREAKER_FAILURE_THRESHOLD))


@pytest.mark.feature("FR06-LLM-02")
def test_fr06_circuit_breaker_failure_window_expires():
    """Failures outside the WINDOW should not count toward the threshold."""
    cb = _fresh_breaker()
    now = 1000.0

    # Record THRESHOLD failures very long ago (outside window)
    for i in range(_CIRCUIT_BREAKER_FAILURE_THRESHOLD):
        cb.record_failure(now=now - _CIRCUIT_BREAKER_FAILURE_WINDOW_SECONDS - 100.0 + float(i))

    # All old failures are pruned — should not trip
    cb.before_request(now=now)


# ---------------------------------------------------------------------------
# FR06-LLM-02 — prior_stats validation
# ---------------------------------------------------------------------------


@contextmanager
def _session_with_settlement_data(returns: list[float], strategy_type: str = "A", signal_date: str = "2025-01-01"):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from app.models import Base
    from datetime import datetime, timezone

    db_path = Path("d:/yanbao/.pytest-tmp/fr06_prior_stats.sqlite3")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    db = Session()
    try:
        table = Base.metadata.tables["settlement_result"]
        signal_day = date.fromisoformat(signal_date)
        exit_day = date(2025, 2, 1)
        now_ts = datetime(2025, 2, 1, tzinfo=timezone.utc)
        for i, ret in enumerate(returns):
            db.execute(
                table.insert().values(
                    settlement_id=f"sr-{i:04d}",
                    settlement_result_id=f"sr-{i:04d}",
                    report_id=f"rpt-{i:04d}",
                    stock_code=f"60{i:04d}.SH",
                    trade_date=signal_day,
                    strategy_type=strategy_type,
                    signal_date=signal_day,
                    exit_trade_date=exit_day,
                    net_return_pct=ret,
                    settlement_status="settled",
                    is_misclassified=False,
                    quality_flag="ok",
                    shares=100,
                    window_days=20,
                    created_at=now_ts,
                    updated_at=now_ts,
                )
            )
        db.commit()
        yield db
    finally:
        db.close()
        engine.dispose()
        if db_path.exists():
            db_path.unlink()


@pytest.mark.feature("FR06-LLM-02")
def test_fr06_prior_stats_is_none_when_sample_lt_30():
    """_compute_prior_stats must return None when settled sample_count < 30."""
    from app.services.report_generation_ssot import _compute_prior_stats

    # Insert only 5 settled results (< 30)
    with _session_with_settlement_data([0.02] * 5, signal_date="2025-01-15") as db:
        result = _compute_prior_stats(
            db,
            strategy_type="A",
            trade_day=date(2025, 2, 1),
        )

    assert result is None


@pytest.mark.feature("FR06-LLM-02")
def test_fr06_prior_stats_contains_required_fields_when_sample_gte_30():
    """_compute_prior_stats must return sample_count, data_cutoff when sample >= 30."""
    from app.services.report_generation_ssot import _compute_prior_stats

    # Insert 35 settled results with signal_date before 2025-02-01 (data_cutoff = 2025-02-01)
    returns = [0.03] * 20 + [-0.01] * 15
    with _session_with_settlement_data(returns, signal_date="2025-01-15") as db:
        result = _compute_prior_stats(
            db,
            strategy_type="A",
            trade_day=date(2025, 2, 1),
        )

    assert result is not None
    assert result["sample_count"] == 35
    assert "data_cutoff" in result
    # data_cutoff must equal month start of trade_day (2025-02-01)
    assert result["data_cutoff"] == "2025-02-01"
    assert "win_rate_historical" in result
    assert 0.0 <= result["win_rate_historical"] <= 1.0


@pytest.mark.feature("FR06-LLM-02")
def test_fr06_prior_stats_data_cutoff_is_month_start():
    """_compute_prior_stats data_cutoff must be the first day of trade_day's month."""
    from app.services.report_generation_ssot import _compute_prior_stats

    returns = [0.02] * 30
    # signal_date 2025-02-15 < data_cutoff 2025-04-01 → all 30 rows counted
    with _session_with_settlement_data(returns, signal_date="2025-02-15") as db:
        result = _compute_prior_stats(
            db,
            strategy_type="A",
            trade_day=date(2025, 4, 15),  # month start = 2025-04-01
        )

    # 30 rows with signal_date 2025-02-15 < 2025-04-01, so result must be non-None
    assert result is not None
    assert result["data_cutoff"] == "2025-04-01"
    assert result["sample_count"] == 30


@pytest.mark.feature("FR06-LLM-02")
def test_fr06_prior_stats_excludes_future_signal_dates():
    """prior_stats must only count results with signal_date before month start."""
    from app.services.report_generation_ssot import _compute_prior_stats

    # Put signal_date AFTER the month start — those should not be counted
    returns = [0.02] * 35
    with _session_with_settlement_data(returns, signal_date="2025-03-15") as db:
        # trade_day month start = 2025-03-01; signal_date 2025-03-15 is NOT < 2025-03-01
        result = _compute_prior_stats(
            db,
            strategy_type="A",
            trade_day=date(2025, 3, 20),
        )

    assert result is None  # 0 eligible rows (all signal_dates are AFTER month start)
