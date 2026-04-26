"""Tests for Persistent Retry — smart retry with exponential backoff.

Validates retry logic, category-specific behavior, backoff computation,
heartbeat integration, and error classification.
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

from automation.agents.persistent_retry import (
    PermanentError,
    RetryCategory,
    RetryStats,
    RetryableError,
    _classify_error,
    _compute_backoff,
    with_retry,
)


class TestErrorClassification:
    """Tests for error classification."""

    def test_retryable_error_class(self):
        is_retryable, status = _classify_error(RetryableError("rate limit", 429))
        assert is_retryable
        assert status == 429

    def test_permanent_error_class(self):
        is_retryable, status = _classify_error(PermanentError("not found"))
        assert not is_retryable

    def test_429_in_message(self):
        is_retryable, status = _classify_error(Exception("HTTP 429 Too Many Requests"))
        assert is_retryable
        assert status == 429

    def test_503_in_message(self):
        is_retryable, status = _classify_error(Exception("503 Service Unavailable"))
        assert is_retryable
        assert status == 503

    def test_timeout_error(self):
        is_retryable, _ = _classify_error(asyncio.TimeoutError())
        assert is_retryable

    def test_connection_error(self):
        is_retryable, _ = _classify_error(ConnectionError("refused"))
        assert is_retryable

    def test_unknown_error_retryable(self):
        is_retryable, _ = _classify_error(RuntimeError("something"))
        assert is_retryable  # default to retryable


class TestBackoffComputation:
    """Tests for backoff computation."""

    def test_first_attempt_reasonable(self):
        delay = _compute_backoff(0)
        assert 0.1 <= delay <= 2.0  # first attempt: ~500ms base

    def test_increases_over_attempts(self):
        delays = [_compute_backoff(i) for i in range(5)]
        # Not necessarily strictly increasing due to jitter, but trend up
        assert delays[-1] >= delays[0]

    def test_respects_max(self):
        delay = _compute_backoff(20, max_ms=5000)
        assert delay <= 5.5  # max 5s + small jitter tolerance

    def test_retry_after_override(self):
        delay = _compute_backoff(0, retry_after=10.0)
        assert delay == 10.0


@pytest.mark.asyncio
class TestWithRetry:
    """Tests for the with_retry function."""

    async def test_success_first_try(self):
        fn = AsyncMock(return_value="ok")
        stats = RetryStats()
        result = await with_retry(fn, category=RetryCategory.FOREGROUND, stats=stats)
        assert result == "ok"
        assert stats.total_attempts == 1
        assert stats.total_successes == 1
        assert stats.total_retries == 0

    async def test_retry_then_success(self):
        call_count = 0

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RetryableError("rate limit", 429)
            return "ok"

        stats = RetryStats()
        result = await with_retry(
            flaky,
            category=RetryCategory.FOREGROUND,
            max_retries=5,
            stats=stats,
        )
        assert result == "ok"
        assert stats.total_retries == 2
        assert stats.total_successes == 1

    async def test_permanent_error_no_retry(self):
        fn = AsyncMock(side_effect=PermanentError("auth failed"))
        stats = RetryStats()
        with pytest.raises(PermanentError):
            await with_retry(fn, category=RetryCategory.FOREGROUND, stats=stats)
        assert stats.total_attempts == 1
        assert stats.total_retries == 0

    async def test_max_retries_exceeded(self):
        fn = AsyncMock(side_effect=RetryableError("rate limit", 429))
        stats = RetryStats()
        with pytest.raises(RetryableError):
            await with_retry(
                fn,
                category=RetryCategory.FOREGROUND,
                max_retries=2,
                stats=stats,
            )
        assert stats.total_attempts == 3  # initial + 2 retries
        assert stats.total_failures == 1

    async def test_background_bails_on_429(self):
        fn = AsyncMock(side_effect=RetryableError("rate limit", 429))
        stats = RetryStats()
        with pytest.raises(RetryableError):
            await with_retry(
                fn,
                category=RetryCategory.BACKGROUND,
                stats=stats,
            )
        assert stats.total_attempts == 1  # no retry for background 429

    async def test_on_retry_callback(self):
        call_count = 0
        retry_info = []

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RetryableError("temp", 503)
            return "ok"

        def on_retry(attempt, exc, delay):
            retry_info.append((attempt, str(exc)))

        await with_retry(
            flaky,
            category=RetryCategory.FOREGROUND,
            on_retry=on_retry,
        )
        assert len(retry_info) == 1
        assert retry_info[0][0] == 1

    async def test_retry_stats_tracking(self):
        call_count = 0

        async def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RetryableError("err", 503)
            return "done"

        stats = RetryStats()
        result = await with_retry(
            flaky,
            category=RetryCategory.FOREGROUND,
            stats=stats,
        )
        assert result == "done"
        assert stats.total_attempts == 3
        assert stats.total_retries == 2
        assert stats.total_successes == 1
        assert stats.total_failures == 0

    async def test_stats_to_dict(self):
        stats = RetryStats(total_attempts=5, total_retries=2, total_successes=3)
        d = stats.to_dict()
        assert d["total_attempts"] == 5
        assert d["total_retries"] == 2
