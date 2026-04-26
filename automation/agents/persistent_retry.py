"""Persistent Retry — smart retry with exponential backoff for long-running sessions.

Ported from the LiteLLM claude-code-sourcemap ``withRetry.ts`` pattern
to Python asyncio, providing:

1. **Foreground retry**: Standard exponential backoff for interactive tasks
2. **Persistent retry**: Indefinite retry with higher backoff for unattended
   sessions (e.g., overnight escort team runs)
3. **429/503 classification**: Distinguishes rate-limit (retryable) from
   permanent errors
4. **Heartbeat yields**: Periodic keep-alive for idle detection during
   long backoff periods
5. **Capacity cascade prevention**: Background tasks bail immediately on
   rate limits to prevent amplification

Thresholds match the LiteLLM reference:
  - DEFAULT_MAX_RETRIES = 10
  - MAX_529_RETRIES = 3
  - BASE_DELAY_MS = 500
  - PERSISTENT_MAX_BACKOFF = 5 minutes
  - PERSISTENT_RESET_CAP = 6 hours
  - HEARTBEAT_INTERVAL = 30 seconds
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Constants (aligned with claude-code-sourcemap withRetry.ts)
DEFAULT_MAX_RETRIES = 10
MAX_529_RETRIES = 3
BASE_DELAY_MS = 500
PERSISTENT_MAX_BACKOFF_MS = 5 * 60 * 1000      # 5 minutes
PERSISTENT_RESET_CAP_MS = 6 * 60 * 60 * 1000   # 6 hours
HEARTBEAT_INTERVAL_MS = 30_000                   # 30 seconds


class RetryCategory(str, Enum):
    """Classification of retry source."""
    FOREGROUND = "foreground"     # User is waiting — retry on 529
    BACKGROUND = "background"    # Background task — bail on 529
    PERSISTENT = "persistent"    # Unattended — retry indefinitely


class RetryableError(Exception):
    """Error that should be retried."""
    def __init__(self, message: str, status_code: int = 0, retry_after: float = 0):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class PermanentError(Exception):
    """Error that should NOT be retried."""
    pass


@dataclass
class RetryStats:
    """Tracks retry statistics for monitoring."""
    total_attempts: int = 0
    total_retries: int = 0
    total_successes: int = 0
    total_failures: int = 0
    last_error: str = ""
    last_retry_at: float = 0.0
    max_backoff_reached: int = 0      # times we hit max backoff

    def to_dict(self) -> dict:
        return {
            "total_attempts": self.total_attempts,
            "total_retries": self.total_retries,
            "total_successes": self.total_successes,
            "total_failures": self.total_failures,
            "last_error": self.last_error,
            "max_backoff_reached": self.max_backoff_reached,
        }


def _classify_error(exc: Exception) -> tuple[bool, int]:
    """Classify an exception as retryable and extract status code.

    Returns (is_retryable, status_code).
    """
    status = 0
    msg = str(exc).lower()

    # Extract status code from common error patterns
    for code_str in ("429", "503", "529", "502", "504"):
        if code_str in msg:
            status = int(code_str)
            break

    # Check httpx response attribute
    if hasattr(exc, "response") and hasattr(exc.response, "status_code"):
        status = exc.response.status_code

    # Retryable: rate limits, server errors, timeouts
    if isinstance(exc, RetryableError):
        return True, exc.status_code or status
    if status in (429, 503, 529, 502, 504):
        return True, status
    if "timeout" in msg or "connection" in msg or "reset" in msg:
        return True, status
    if isinstance(exc, (asyncio.TimeoutError, ConnectionError, OSError)):
        return True, status

    # Permanent: auth errors, validation errors, etc.
    if isinstance(exc, PermanentError):
        return False, status
    if status in (401, 403, 404, 422):
        return False, status

    # Default: retry unknown errors up to limit
    return True, status


def _compute_backoff(
    attempt: int,
    base_ms: float = BASE_DELAY_MS,
    max_ms: float = 60_000,
    retry_after: float = 0,
) -> float:
    """Compute backoff delay in seconds with jitter.

    Uses decorrelated jitter: delay = min(max_ms, random(base_ms, prev * 3))
    """
    if retry_after > 0:
        return retry_after

    exp_delay_ms = base_ms * (2 ** attempt)
    jittered_ms = random.uniform(base_ms, min(exp_delay_ms, max_ms))
    return jittered_ms / 1000.0


async def with_retry(
    fn: Callable[..., Awaitable[T]],
    *args: Any,
    category: RetryCategory = RetryCategory.FOREGROUND,
    max_retries: int = DEFAULT_MAX_RETRIES,
    stats: Optional[RetryStats] = None,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None,
    heartbeat: Optional[Callable[[], Awaitable[None]]] = None,
    **kwargs: Any,
) -> T:
    """Execute *fn* with smart retry logic.

    Args:
        fn: Async callable to execute.
        category: Retry behavior category.
        max_retries: Max retry attempts (ignored for PERSISTENT).
        stats: Optional RetryStats tracker.
        on_retry: Optional callback(attempt, error, delay) before each retry.
        heartbeat: Optional async callable for keep-alive during long backoffs.
        *args, **kwargs: Passed through to *fn*.

    Returns:
        Result of *fn* on success.

    Raises:
        Last exception if all retries exhausted.
    """
    if stats is None:
        stats = RetryStats()

    attempt = 0
    last_exc: Optional[Exception] = None
    persistent_backoff_ms = BASE_DELAY_MS

    while True:
        stats.total_attempts += 1
        attempt += 1

        try:
            result = await fn(*args, **kwargs)
            stats.total_successes += 1
            return result

        except Exception as exc:
            last_exc = exc
            is_retryable, status = _classify_error(exc)
            stats.last_error = f"[{status}] {str(exc)[:200]}"

            if not is_retryable:
                stats.total_failures += 1
                raise

            # Category-specific behavior
            if category == RetryCategory.BACKGROUND:
                if status in (429, 529):
                    # Background tasks bail immediately on rate limit
                    # to prevent capacity cascade (10x amplification)
                    stats.total_failures += 1
                    raise
                if attempt > max_retries:
                    stats.total_failures += 1
                    raise

            elif category == RetryCategory.FOREGROUND:
                max_529 = MAX_529_RETRIES if status == 529 else max_retries
                if attempt > max_529:
                    stats.total_failures += 1
                    raise

            elif category == RetryCategory.PERSISTENT:
                # Persistent: retry indefinitely with increasing backoff
                pass  # never exhaust

            stats.total_retries += 1

            # Compute delay
            retry_after = getattr(exc, "retry_after", 0) or 0

            if category == RetryCategory.PERSISTENT:
                # Persistent uses its own escalating backoff
                persistent_backoff_ms = min(
                    persistent_backoff_ms * 2,
                    PERSISTENT_MAX_BACKOFF_MS,
                )
                if persistent_backoff_ms >= PERSISTENT_MAX_BACKOFF_MS:
                    stats.max_backoff_reached += 1
                    # Reset after cap to prevent permanent max-backoff
                    if stats.max_backoff_reached * PERSISTENT_MAX_BACKOFF_MS > PERSISTENT_RESET_CAP_MS:
                        persistent_backoff_ms = BASE_DELAY_MS
                        stats.max_backoff_reached = 0
                delay = max(retry_after, persistent_backoff_ms / 1000.0)
            else:
                delay = _compute_backoff(
                    attempt - 1,
                    retry_after=retry_after,
                )

            stats.last_retry_at = time.monotonic()

            if on_retry:
                on_retry(attempt, exc, delay)

            logger.info(
                "[retry] Attempt %d failed (%s %d), retrying in %.1fs [%s]",
                attempt, type(exc).__name__, status, delay, category.value,
            )

            # Wait with periodic heartbeat for long backoffs
            if heartbeat and delay > HEARTBEAT_INTERVAL_MS / 1000.0:
                remaining = delay
                while remaining > 0:
                    wait = min(remaining, HEARTBEAT_INTERVAL_MS / 1000.0)
                    await asyncio.sleep(wait)
                    remaining -= wait
                    if remaining > 0 and heartbeat:
                        await heartbeat()
            else:
                await asyncio.sleep(delay)
