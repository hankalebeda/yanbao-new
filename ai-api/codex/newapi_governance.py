"""
New API Channel Governance — external truth layer for dual-track (stable/readonly)
channel management with three-state machine (active/quarantine/retired) and
governor lease/fencing for exclusive mutation.

This is infrastructure-level governance, not business SSOT. It uses a local
JSON-backed store (no external DB required) with file-locking for concurrency.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterator


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = str(os.environ.get(name, "")).strip()
    try:
        return max(minimum, int(raw))
    except (TypeError, ValueError):
        return default

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOVERNANCE_DIR = Path(os.environ.get("NEWAPI_GOVERNANCE_DIR", "")) or (
    Path(__file__).resolve().parent / ".governance"
)
GOVERNANCE_STATE_PATH = GOVERNANCE_DIR / "channel_state.json"
LEASE_PATH = GOVERNANCE_DIR / "governor_lease.json"

LANE_STABLE = "codex-stable"
LANE_READONLY = "codex-readonly"
READONLY_SHARDS = ("ro-a", "ro-b", "ro-c", "ro-d")
LANE_BY_SHARD = {
    "stable": LANE_STABLE,
    "ro-a": "codex-ro-a",
    "ro-b": "codex-ro-b",
    "ro-c": "codex-ro-c",
    "ro-d": "codex-ro-d",
}
VALID_STATES = {"active", "quarantine", "retired"}
VALID_LANES = set(LANE_BY_SHARD.values()) | {LANE_READONLY}
VALID_SHARDS = {"stable"} | set(READONLY_SHARDS)

# Exponential back-off schedule for quarantine re-probe (minutes)
QUARANTINE_BACKOFF_MINUTES = [10, 20, 40, 120]

# Errors that should route to quarantine vs retired
QUARANTINE_ERROR_CLASSES = {
    "timeout",
    "connection_reset",
    "status_429",
    "status_500",
    "status_503",
    "service_temporarily_unavailable",
    "too_many_requests",
    "system_cpu_overloaded",
    "xhigh_not_supported",
    "proxy_required",
    "no_available_providers",
}

RETIRED_ERROR_CLASSES = {
    "model_not_found",
    "token_expired",
    "token_invalidated",
    "unsupported_legacy_protocol",
    "missing_provider_metadata",
    "hard_cloudflare_block",
    "quota_terminated",
}

DEFAULT_LEASE_TTL_SECONDS = _env_int("NEW_API_GOVERNANCE_LEASE_TTL_SECONDS", 1800)  # 30 minutes


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ChannelGovernanceEntry:
    channel_identity: str
    channel_id: int | None = None
    lane: str = "codex-ro-a"
    shard: str = "ro-a"
    desired_state: str = "active"  # active | quarantine | retired
    inventory_class: str = "managed"
    reason_class: str = ""
    reason_code: str = ""
    cooldown_until: float = 0.0
    last_probe_at: float = 0.0
    last_ok_at: float = 0.0
    consecutive_failures: int = 0
    manual_hold: bool = False
    direct_model: bool = True  # True if channel delivers direct model, False if mapped
    preserve: bool = True
    allow_auto_create: bool = False
    allow_auto_enable: bool = False
    allow_auto_disable: bool = True
    archive_reason: str = ""
    last_mutation_source: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChannelGovernanceEntry":
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)


@dataclass
class LaneAssignment:
    lane: str
    group: str
    token_name: str
    shard: str
    channel_identities: list[str] = field(default_factory=list)
    max_concurrent: int = 3

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class GovernorLease:
    lease_id: str
    holder_id: str
    acquired_at: float
    expires_at: float
    fencing_token: int

    def is_expired(self, now: float | None = None) -> bool:
        return (now or time.time()) >= self.expires_at

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GovernorLease":
        return cls(
            lease_id=str(data["lease_id"]),
            holder_id=str(data["holder_id"]),
            acquired_at=float(data["acquired_at"]),
            expires_at=float(data["expires_at"]),
            fencing_token=int(data["fencing_token"]),
        )


def lane_for_shard(shard: str) -> str:
    clean = str(shard or "").strip().lower()
    return LANE_BY_SHARD.get(clean, LANE_READONLY)


def assign_shard(identity: str) -> str:
    """Deterministically pin a channel identity to one readonly shard."""

    normalized = str(identity or "").strip().lower()
    if normalized.endswith("-stable"):
        return "stable"
    for shard in READONLY_SHARDS:
        if normalized.endswith(shard):
            return shard
    if not normalized:
        return READONLY_SHARDS[0]
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()
    return READONLY_SHARDS[int(digest[:2], 16) % len(READONLY_SHARDS)]


def default_channel_entry(identity: str, channel_id: int | None = None) -> ChannelGovernanceEntry:
    shard = assign_shard(identity)
    return ChannelGovernanceEntry(
        channel_identity=identity,
        channel_id=channel_id,
        lane=lane_for_shard(shard),
        shard=shard,
    )


# ---------------------------------------------------------------------------
# State persistence (JSON-backed, file-lock protected)
# ---------------------------------------------------------------------------

def _ensure_governance_dir() -> Path:
    GOVERNANCE_DIR.mkdir(parents=True, exist_ok=True)
    return GOVERNANCE_DIR


def load_governance_state() -> dict[str, Any]:
    _ensure_governance_dir()
    if not GOVERNANCE_STATE_PATH.exists():
        return {"channels": {}, "lanes": {}, "next_fencing_token": 1}
    try:
        return json.loads(GOVERNANCE_STATE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"channels": {}, "lanes": {}, "next_fencing_token": 1}


def save_governance_state(state: dict[str, Any]) -> None:
    _ensure_governance_dir()
    tmp_path = GOVERNANCE_STATE_PATH.with_suffix(".tmp")
    tmp_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    tmp_path.replace(GOVERNANCE_STATE_PATH)


def get_channel_entry(state: dict[str, Any], identity: str) -> ChannelGovernanceEntry | None:
    channels = state.get("channels", {})
    data = channels.get(identity)
    if data is None:
        return None
    return ChannelGovernanceEntry.from_dict(data)


def set_channel_entry(state: dict[str, Any], entry: ChannelGovernanceEntry) -> None:
    state.setdefault("channels", {})[entry.channel_identity] = entry.to_dict()


# ---------------------------------------------------------------------------
# Three-state machine transitions
# ---------------------------------------------------------------------------

def classify_error(error_text: str) -> tuple[str, str]:
    """Classify an error string into (desired_state, reason_class).

    Returns ("quarantine", class) or ("retired", class).
    """
    normalized = error_text.lower().replace(" ", "_").replace("-", "_")
    for cls in RETIRED_ERROR_CLASSES:
        if cls in normalized:
            return ("retired", cls)
    for cls in QUARANTINE_ERROR_CLASSES:
        if cls in normalized:
            return ("quarantine", cls)
    # Default: transient errors go to quarantine, not retired
    return ("quarantine", "unknown_transient")


def compute_cooldown_until(consecutive_failures: int) -> float:
    """Compute the next cooldown timestamp based on exponential back-off."""
    idx = min(consecutive_failures, len(QUARANTINE_BACKOFF_MINUTES) - 1)
    minutes = QUARANTINE_BACKOFF_MINUTES[idx]
    return time.time() + minutes * 60


def transition_channel(
    state: dict[str, Any],
    identity: str,
    *,
    event: str,  # "probe_ok" | "probe_fail" | "manual_retire" | "manual_activate"
    error_text: str = "",
    channel_id: int | None = None,
) -> ChannelGovernanceEntry:
    """Apply a state-machine transition and return the updated entry."""
    entry = get_channel_entry(state, identity)
    if entry is None:
        entry = default_channel_entry(identity, channel_id=channel_id)

    if channel_id is not None:
        entry.channel_id = channel_id

    now = time.time()

    if event == "probe_ok":
        entry.desired_state = "active"
        entry.consecutive_failures = 0
        entry.cooldown_until = 0.0
        entry.last_ok_at = now
        entry.last_probe_at = now
        entry.reason_class = ""
        entry.reason_code = ""

    elif event == "probe_fail":
        entry.last_probe_at = now
        entry.consecutive_failures += 1
        desired, reason_cls = classify_error(error_text)
        entry.reason_class = reason_cls
        entry.reason_code = error_text[:200]
        if desired == "retired":
            entry.desired_state = "retired"
            entry.cooldown_until = 0.0
        else:
            entry.desired_state = "quarantine"
            entry.cooldown_until = compute_cooldown_until(entry.consecutive_failures)

    elif event == "manual_retire":
        entry.desired_state = "retired"
        entry.manual_hold = True
        entry.cooldown_until = 0.0

    elif event == "manual_activate":
        entry.desired_state = "active"
        entry.manual_hold = False
        entry.consecutive_failures = 0
        entry.cooldown_until = 0.0

    set_channel_entry(state, entry)
    return entry


def channels_due_for_reprobe(
    state: dict[str, Any],
    *,
    lane: str | None = None,
    shard: str | None = None,
    limit: int | None = None,
) -> list[ChannelGovernanceEntry]:
    """Return quarantined channels whose cooldown has expired."""
    now = time.time()
    due: list[ChannelGovernanceEntry] = []
    for identity, data in state.get("channels", {}).items():
        entry = ChannelGovernanceEntry.from_dict(data)
        if lane and entry.lane != lane:
            continue
        if shard and entry.shard != shard:
            continue
        if entry.desired_state == "quarantine" and not entry.manual_hold:
            if now >= entry.cooldown_until:
                due.append(entry)
                if limit is not None and len(due) >= max(0, int(limit)):
                    break
    return due


# ---------------------------------------------------------------------------
# Governor lease (exclusive mutation rights)
# ---------------------------------------------------------------------------

def _load_lease() -> GovernorLease | None:
    _ensure_governance_dir()
    if not LEASE_PATH.exists():
        return None
    try:
        data = json.loads(LEASE_PATH.read_text(encoding="utf-8"))
        return GovernorLease.from_dict(data)
    except (json.JSONDecodeError, OSError, KeyError):
        return None


def _save_lease(lease: GovernorLease | None) -> None:
    _ensure_governance_dir()
    if lease is None:
        LEASE_PATH.unlink(missing_ok=True)
    else:
        tmp_path = LEASE_PATH.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(lease.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(LEASE_PATH)


def acquire_lease(holder_id: str, ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS) -> GovernorLease:
    """Acquire the exclusive governor lease. Raises RuntimeError if held by another."""
    now = time.time()
    existing = _load_lease()
    if existing is not None and not existing.is_expired(now):
        if existing.holder_id != holder_id:
            raise RuntimeError(
                f"Governor lease held by {existing.holder_id} until "
                f"{time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(existing.expires_at))}"
            )
        # Same holder: extend
        existing.expires_at = now + ttl_seconds
        _save_lease(existing)
        return existing

    # Acquire new lease
    state = load_governance_state()
    fencing_token = int(state.get("next_fencing_token", 1))
    state["next_fencing_token"] = fencing_token + 1
    save_governance_state(state)

    lease = GovernorLease(
        lease_id=f"gov-{int(now)}",
        holder_id=holder_id,
        acquired_at=now,
        expires_at=now + ttl_seconds,
        fencing_token=fencing_token,
    )
    _save_lease(lease)
    return lease


def renew_lease(
    holder_id: str,
    *,
    ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
    fencing_token: int | None = None,
) -> GovernorLease:
    """Renew the active lease held by the same holder.

    Raises RuntimeError when the lease is missing, expired, fenced by another
    session, or owned by a different holder.
    """
    now = time.time()
    existing = _load_lease()
    if existing is None or existing.is_expired(now):
        raise RuntimeError("No active governor lease to renew")
    if existing.holder_id != holder_id:
        raise RuntimeError(f"Governor lease held by {existing.holder_id}")
    if fencing_token is not None and existing.fencing_token != fencing_token:
        raise RuntimeError(
            f"Fencing token mismatch: expected {existing.fencing_token}, got {fencing_token}"
        )
    existing.expires_at = now + ttl_seconds
    _save_lease(existing)
    return existing


def release_lease(holder_id: str) -> None:
    """Release the governor lease if held by the given holder."""
    existing = _load_lease()
    if existing is not None and existing.holder_id == holder_id:
        _save_lease(None)


def validate_fencing(fencing_token: int) -> None:
    """Ensure the given fencing token matches the current lease."""
    existing = _load_lease()
    if existing is None:
        raise RuntimeError("No active governor lease")
    if existing.fencing_token != fencing_token:
        raise RuntimeError(
            f"Fencing token mismatch: expected {existing.fencing_token}, got {fencing_token}"
        )


@contextmanager
def governor_session(holder_id: str, ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS) -> Iterator[GovernorLease]:
    """Context manager for governor lease acquisition and release."""
    lease = acquire_lease(holder_id, ttl_seconds)
    try:
        yield lease
    finally:
        release_lease(holder_id)


# ---------------------------------------------------------------------------
# Lane assignment helpers
# ---------------------------------------------------------------------------

DEFAULT_LANE_ASSIGNMENTS = [
    LaneAssignment(lane=LANE_STABLE, group=LANE_STABLE, token_name=LANE_STABLE, shard="stable", max_concurrent=2),
    LaneAssignment(lane="codex-ro-a", group="codex-ro-a", token_name="codex-ro-a", shard="ro-a", max_concurrent=3),
    LaneAssignment(lane="codex-ro-b", group="codex-ro-b", token_name="codex-ro-b", shard="ro-b", max_concurrent=3),
    LaneAssignment(lane="codex-ro-c", group="codex-ro-c", token_name="codex-ro-c", shard="ro-c", max_concurrent=3),
    LaneAssignment(lane="codex-ro-d", group="codex-ro-d", token_name="codex-ro-d", shard="ro-d", max_concurrent=3),
]


def get_shard_channels(state: dict[str, Any], shard: str) -> list[ChannelGovernanceEntry]:
    """Return active channels assigned to the given shard."""
    result: list[ChannelGovernanceEntry] = []
    for data in state.get("channels", {}).values():
        entry = ChannelGovernanceEntry.from_dict(data)
        if entry.shard == shard and entry.desired_state == "active":
            result.append(entry)
    return result


def get_lane_channels(state: dict[str, Any], lane: str) -> list[ChannelGovernanceEntry]:
    """Return active channels for a given lane."""
    result: list[ChannelGovernanceEntry] = []
    for data in state.get("channels", {}).values():
        entry = ChannelGovernanceEntry.from_dict(data)
        if entry.lane == lane and entry.desired_state == "active":
            result.append(entry)
    return result


def summary(state: dict[str, Any]) -> dict[str, Any]:
    """Produce a summary of the governance state for diagnostics."""
    channels = state.get("channels", {})
    by_state: dict[str, int] = {"active": 0, "quarantine": 0, "retired": 0}
    by_lane: dict[str, int] = {}
    by_shard: dict[str, int] = {}
    by_inventory_class: dict[str, int] = {}
    for data in channels.values():
        s = data.get("desired_state", "active")
        by_state[s] = by_state.get(s, 0) + 1
        lane = data.get("lane", "")
        by_lane[lane] = by_lane.get(lane, 0) + 1
        shard = data.get("shard", "")
        by_shard[shard] = by_shard.get(shard, 0) + 1
        inventory_class = data.get("inventory_class", "managed")
        by_inventory_class[inventory_class] = by_inventory_class.get(inventory_class, 0) + 1
    return {
        "total_channels": len(channels),
        "by_state": by_state,
        "by_lane": by_lane,
        "by_shard": by_shard,
        "by_inventory_class": by_inventory_class,
        "next_fencing_token": state.get("next_fencing_token", 1),
    }
