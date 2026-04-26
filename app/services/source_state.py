from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock

from app.core.config import settings


@dataclass
class SourceHealth:
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_error: str | None = None
    last_update: str | None = None
    circuit_open: bool = False
    opened_at: str | None = None


@dataclass
class RuntimeState:
    market: dict[str, SourceHealth] = field(default_factory=dict)
    hotspot: dict[str, SourceHealth] = field(default_factory=dict)


_state = RuntimeState(
    market={"eastmoney": SourceHealth(), "tdx": SourceHealth(), "fallback": SourceHealth()},
    hotspot={"weibo": SourceHealth(), "douyin": SourceHealth()},
)
_lock = Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_group(kind: str) -> dict[str, SourceHealth]:
    return _state.market if kind == "market" else _state.hotspot


def should_skip_source(kind: str, source: str) -> bool:
    group = _get_group(kind)
    h = group.get(source)
    if not h or not h.circuit_open:
        return False
    opened = h.opened_at
    if not opened:
        return True
    try:
        opened_dt = datetime.fromisoformat(opened)
    except Exception:
        return True
    cooldown = max(1, int(settings.source_circuit_cooldown_seconds))
    elapsed = (datetime.now(timezone.utc) - opened_dt).total_seconds()
    # Half-open probe after cooldown: allow one request to verify recovery.
    return elapsed < cooldown


def record_source_result(kind: str, source: str, success: bool, error: str | None = None):
    group = _get_group(kind)
    with _lock:
        if source not in group:
            group[source] = SourceHealth()
        h = group[source]
        h.last_update = _now_iso()
        if success:
            h.consecutive_successes += 1
            h.consecutive_failures = 0
            h.last_error = None
            if h.circuit_open and h.consecutive_successes >= settings.source_recover_success_threshold:
                h.circuit_open = False
                h.opened_at = None
        else:
            h.consecutive_failures += 1
            h.consecutive_successes = 0
            h.last_error = error
            if h.consecutive_failures >= settings.source_fail_open_threshold:
                h.circuit_open = True
                h.opened_at = _now_iso()


def get_source_runtime_status() -> dict:
    with _lock:
        market = {
            k: {
                "consecutive_failures": v.consecutive_failures,
                "consecutive_successes": v.consecutive_successes,
                "last_error": v.last_error,
                "last_update": v.last_update,
                "circuit_open": v.circuit_open,
                "opened_at": v.opened_at,
            }
            for k, v in _state.market.items()
        }
        hotspot = {
            k: {
                "consecutive_failures": v.consecutive_failures,
                "consecutive_successes": v.consecutive_successes,
                "last_error": v.last_error,
                "last_update": v.last_update,
                "circuit_open": v.circuit_open,
                "opened_at": v.opened_at,
            }
            for k, v in _state.hotspot.items()
        }
    return {"market": market, "hotspot": hotspot}
