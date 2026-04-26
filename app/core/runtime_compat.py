from __future__ import annotations

import sqlite3
import warnings
from datetime import date, datetime, timezone

_SQLITE_ADAPTERS_REGISTERED = False
_WARNING_FILTERS_APPLIED = False


def _adapt_sqlite_date(value: date) -> str:
    return value.isoformat()


def _adapt_sqlite_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        return value.isoformat(sep=" ")
    return value.astimezone(timezone.utc).isoformat(sep=" ")


def register_sqlite_adapters() -> None:
    """Register explicit SQLite adapters to avoid Python 3.12 default-adapter warnings."""
    global _SQLITE_ADAPTERS_REGISTERED
    if _SQLITE_ADAPTERS_REGISTERED:
        return
    sqlite3.register_adapter(date, _adapt_sqlite_date)
    sqlite3.register_adapter(datetime, _adapt_sqlite_datetime)
    _SQLITE_ADAPTERS_REGISTERED = True


def suppress_known_third_party_warnings() -> None:
    """Silence known third-party deprecation noise we cannot fix in-repo yet."""
    global _WARNING_FILTERS_APPLIED
    if _WARNING_FILTERS_APPLIED:
        return
    warnings.filterwarnings(
        "ignore",
        message=r"datetime\.datetime\.utcfromtimestamp\(\) is deprecated.*",
        category=DeprecationWarning,
        module=r"tqdm\.std",
    )
    _WARNING_FILTERS_APPLIED = True


def apply_runtime_compat() -> None:
    register_sqlite_adapters()
    suppress_known_third_party_warnings()

