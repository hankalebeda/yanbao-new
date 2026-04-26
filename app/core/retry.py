"""Lightweight async retry with exponential backoff for external I/O calls."""

from __future__ import annotations

import asyncio
import functools
import logging
from typing import Any, Callable, TypeVar

from app.core.config import settings

logger = logging.getLogger(__name__)

T = TypeVar("T")

_RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    OSError,           # connection reset, timeout at socket level
    TimeoutError,
)
try:
    import httpx
    _RETRYABLE_EXCEPTIONS = (*_RETRYABLE_EXCEPTIONS, httpx.HTTPStatusError, httpx.TransportError)
except ImportError:
    pass


async def async_retry(
    fn: Callable[..., Any],
    *args: Any,
    max_retries: int | None = None,
    backoff_base: float = 2.0,
    retryable: tuple[type[BaseException], ...] | None = None,
    label: str = "",
    **kwargs: Any,
) -> Any:
    """Call *fn* with retries on transient failures.

    Parameters
    ----------
    fn : Callable  — async or sync function to call
    max_retries : int — total retry count (default: ``settings.scheduler_retry_count``)
    backoff_base : float — exponential back-off base in seconds (2 → 2s, 4s, 8s …)
    retryable : tuple — exception types considered transient
    label : str — human-friendly label for log lines
    """
    if max_retries is None:
        max_retries = settings.scheduler_retry_count  # default 2
    if retryable is None:
        retryable = _RETRYABLE_EXCEPTIONS
    if not label:
        label = getattr(fn, "__name__", str(fn))

    last_exc: BaseException | None = None
    for attempt in range(max_retries + 1):
        try:
            if asyncio.iscoroutinefunction(fn):
                return await fn(*args, **kwargs)
            else:
                return fn(*args, **kwargs)
        except retryable as exc:
            last_exc = exc
            if attempt < max_retries:
                delay = backoff_base ** (attempt + 1)
                logger.warning(
                    "retry | %s attempt %d/%d failed: %s — retrying in %.1fs",
                    label, attempt + 1, max_retries + 1, exc, delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    "retry | %s all %d attempts exhausted: %s",
                    label, max_retries + 1, exc,
                )
    raise last_exc  # type: ignore[misc]
