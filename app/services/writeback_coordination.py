"""Lease and fencing coordination for writeback operations.

This module provides a minimal single-writer coordinator that supports:
1) deterministic idempotent claim for same round/targets
2) stale lease takeover
3) fencing checks before submit
4) target-path overlap conflict detection
"""
from __future__ import annotations

import contextlib
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator
from uuid import uuid4

_LOCK_TIMEOUT_SECONDS = 10.0
_LOCK_POLL_INTERVAL = 0.05


@contextlib.contextmanager
def _cross_process_lock(lock_path: Path, timeout: float = _LOCK_TIMEOUT_SECONDS) -> Iterator[None]:
    """Cross-process file lock using O_CREAT|O_EXCL (same pattern as writeback_service)."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    fd: int | None = None
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            os.write(fd, f"{os.getpid()}:{_utc_now().isoformat()}".encode("utf-8"))
            break
        except FileExistsError:
            if (time.monotonic() - started) >= timeout:
                raise RuntimeError(f"COORDINATION_LOCK_TIMEOUT after {timeout}s")
            time.sleep(_LOCK_POLL_INTERVAL)
    try:
        yield
    finally:
        if fd is not None:
            os.close(fd)
        with contextlib.suppress(OSError):
            lock_path.unlink()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _from_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_targets(target_paths: Iterable[str]) -> tuple[str, ...]:
    normalized = sorted({str(item or "").strip().replace("\\", "/") for item in target_paths if str(item or "").strip()})
    if not normalized:
        raise ValueError("target_paths cannot be empty")
    return tuple(normalized)


@dataclass(frozen=True)
class WritebackLease:
    lease_id: str
    round_id: str
    target_paths: tuple[str, ...]
    fencing_token: int
    issued_at: str
    lease_until: str


class ClaimConflictError(RuntimeError):
    def __init__(self, *, conflict_paths: tuple[str, ...], holder_round_id: str, holder_lease_id: str):
        self.conflict_paths = conflict_paths
        self.holder_round_id = holder_round_id
        self.holder_lease_id = holder_lease_id
        super().__init__("TARGET_PATH_CONFLICT")


class LeaseRejectedError(RuntimeError):
    def __init__(self, reason: str):
        self.reason = str(reason or "LEASE_REJECTED")
        super().__init__(self.reason)


class WritebackCoordination:
    """JSON-backed single-writer lease coordinator with fencing."""

    def __init__(
        self,
        state_path: Path,
        *,
        now_fn: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._state_path = Path(state_path).resolve()
        self._now_fn = now_fn
        self._lock_path = self._state_path.with_suffix(".lock")

    def claim(self, round_id: str, target_paths: Iterable[str], lease_seconds: int) -> WritebackLease:
        if int(lease_seconds) <= 0:
            raise ValueError("lease_seconds must be > 0")
        normalized_round_id = str(round_id or "").strip()
        if not normalized_round_id:
            raise ValueError("round_id cannot be empty")

        targets = _normalize_targets(target_paths)
        now = self._now_fn()
        with _cross_process_lock(self._lock_path):
            state = self._load_state()
            self._prune_expired_claims(state, now=now)

            existing = self._find_active_idempotent_claim(
                state,
                round_id=normalized_round_id,
                targets=targets,
                now=now,
            )
            if existing is not None:
                self._save_state(state)
                return self._claim_to_lease(existing)

            overlap = self._find_overlap_claim(state, targets=targets, now=now)
            if overlap is not None:
                conflict = tuple(sorted(set(overlap["target_paths"]).intersection(targets)))
                raise ClaimConflictError(
                    conflict_paths=conflict,
                    holder_round_id=str(overlap["round_id"]),
                    holder_lease_id=str(overlap["lease_id"]),
                )

            fence = int(state.get("next_fencing_token", 1))
            lease_until = now + timedelta(seconds=int(lease_seconds))
            record = {
                "lease_id": str(uuid4()),
                "round_id": normalized_round_id,
                "target_paths": list(targets),
                "fencing_token": fence,
                "issued_at": _to_iso(now),
                "lease_until": _to_iso(lease_until),
            }
            state["next_fencing_token"] = fence + 1
            state.setdefault("claims", []).append(record)
            self._save_state(state)
            return self._claim_to_lease(record)

    def assert_submit_allowed(self, lease_id: str, fencing_token: int, target_paths: Iterable[str]) -> None:
        normalized_lease_id = str(lease_id or "").strip()
        if not normalized_lease_id:
            raise LeaseRejectedError("LEASE_ID_MISSING")
        targets = _normalize_targets(target_paths)

        now = self._now_fn()
        with _cross_process_lock(self._lock_path):
            state = self._load_state()
            self._prune_expired_claims(state, now=now)
            claim = next((item for item in state.get("claims", []) if str(item.get("lease_id")) == normalized_lease_id), None)
            if claim is None:
                self._save_state(state)
                raise LeaseRejectedError("LEASE_NOT_ACTIVE")

            if int(claim.get("fencing_token") or 0) != int(fencing_token):
                raise LeaseRejectedError("FENCING_TOKEN_MISMATCH")

            claim_targets = set(claim.get("target_paths") or [])
            if not set(targets).issubset(claim_targets):
                raise LeaseRejectedError("TARGET_PATH_OUT_OF_SCOPE")

            lease_until = _from_iso(str(claim["lease_until"]))
            if lease_until <= now:
                self._save_state(state)
                raise LeaseRejectedError("LEASE_EXPIRED")

            self._save_state(state)

    def refresh(self, *, lease: WritebackLease | dict[str, object], lease_seconds: int | None = None) -> WritebackLease | None:
        lease_id = str(getattr(lease, "lease_id", "") or (lease.get("lease_id") if isinstance(lease, dict) else "") or "").strip()
        if not lease_id:
            return None

        now = self._now_fn()
        with _cross_process_lock(self._lock_path):
            state = self._load_state()
            self._prune_expired_claims(state, now=now)
            claim = next((item for item in state.get("claims", []) if str(item.get("lease_id")) == lease_id), None)
            if claim is None:
                self._save_state(state)
                return None

            try:
                issued_at = _from_iso(str(claim.get("issued_at") or claim["lease_until"]))
                previous_until = _from_iso(str(claim["lease_until"]))
                previous_duration = max(1, int((previous_until - issued_at).total_seconds()))
            except Exception:
                previous_duration = 1
            ttl_seconds = max(1, int(lease_seconds or previous_duration))
            claim["lease_until"] = _to_iso(now + timedelta(seconds=ttl_seconds))
            self._save_state(state)
            return self._claim_to_lease(claim)

    def release(self, *, lease: WritebackLease | dict[str, object], reason: str = "released") -> None:
        lease_id = str(getattr(lease, "lease_id", "") or (lease.get("lease_id") if isinstance(lease, dict) else "") or "").strip()
        if not lease_id:
            return
        with _cross_process_lock(self._lock_path):
            state = self._load_state()
            now = self._now_fn()
            self._prune_expired_claims(state, now=now)
            state["claims"] = [
                claim for claim in state.get("claims", [])
                if str(claim.get("lease_id")) != lease_id
            ]
            previews = state.get("preview_sha256")
            if isinstance(previews, dict):
                previews.pop(lease_id, None)
            self._save_state(state)

    def register_preview_sha256(self, lease_id: str, target_path: str, base_sha256: str) -> None:
        """Record the file SHA256 observed at preview time for optimistic concurrency."""
        normalized_lease = str(lease_id or "").strip()
        normalized_path = str(target_path or "").strip().replace("\\", "/")
        if not normalized_lease or not normalized_path:
            return
        with _cross_process_lock(self._lock_path):
            state = self._load_state()
            previews = state.setdefault("preview_sha256", {})
            previews.setdefault(normalized_lease, {})[normalized_path] = str(base_sha256 or "")
            self._save_state(state)

    def verify_base_sha256(self, lease_id: str, target_path: str, current_sha256: str) -> bool:
        """Verify that file content has not changed since preview (optimistic concurrency check)."""
        normalized_lease = str(lease_id or "").strip()
        normalized_path = str(target_path or "").strip().replace("\\", "/")
        if not normalized_lease or not normalized_path:
            return True
        with _cross_process_lock(self._lock_path):
            state = self._load_state()
            previews = state.get("preview_sha256", {})
            lease_previews = previews.get(normalized_lease, {})
            recorded = lease_previews.get(normalized_path)
            if recorded is None:
                import logging
                logging.getLogger(__name__).warning(
                    "verify_base_sha256: no recorded SHA256 for lease=%s path=%s — allowing (soft mode)",
                    normalized_lease,
                    normalized_path,
                )
                return True
            return str(current_sha256 or "") == recorded

    def _claim_to_lease(self, claim: dict) -> WritebackLease:
        return WritebackLease(
            lease_id=str(claim["lease_id"]),
            round_id=str(claim["round_id"]),
            target_paths=tuple(str(item) for item in claim.get("target_paths") or []),
            fencing_token=int(claim["fencing_token"]),
            issued_at=str(claim["issued_at"]),
            lease_until=str(claim["lease_until"]),
        )

    def _find_active_idempotent_claim(
        self,
        state: dict,
        *,
        round_id: str,
        targets: tuple[str, ...],
        now: datetime,
    ) -> dict | None:
        target_set = set(targets)
        for claim in state.get("claims", []):
            if str(claim.get("round_id")) != round_id:
                continue
            if set(claim.get("target_paths") or []) != target_set:
                continue
            lease_until = _from_iso(str(claim["lease_until"]))
            if lease_until > now:
                return claim
        return None

    def _find_overlap_claim(self, state: dict, *, targets: tuple[str, ...], now: datetime) -> dict | None:
        incoming = set(targets)
        for claim in state.get("claims", []):
            lease_until = _from_iso(str(claim["lease_until"]))
            if lease_until <= now:
                continue
            existing = set(claim.get("target_paths") or [])
            if existing.intersection(incoming):
                return claim
        return None

    def _prune_expired_claims(self, state: dict, *, now: datetime) -> None:
        active_claims = []
        for claim in state.get("claims", []):
            try:
                lease_until = _from_iso(str(claim["lease_until"]))
            except Exception:
                continue
            if lease_until > now:
                active_claims.append(claim)
        state["claims"] = active_claims
        active_ids = {str(claim.get("lease_id") or "") for claim in active_claims}
        preview_sha256 = state.get("preview_sha256")
        if isinstance(preview_sha256, dict):
            state["preview_sha256"] = {
                lease_id: payload
                for lease_id, payload in preview_sha256.items()
                if str(lease_id or "") in active_ids
            }

    def _load_state(self) -> dict:
        if not self._state_path.exists():
            return {"next_fencing_token": 1, "claims": [], "preview_sha256": {}}
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        except Exception:
            return {"next_fencing_token": 1, "claims": [], "preview_sha256": {}}
        if not isinstance(payload, dict):
            return {"next_fencing_token": 1, "claims": [], "preview_sha256": {}}
        claims = payload.get("claims")
        if not isinstance(claims, list):
            claims = []
        next_token = int(payload.get("next_fencing_token") or 1)
        if next_token <= 0:
            next_token = 1
        preview_sha256 = payload.get("preview_sha256")
        if not isinstance(preview_sha256, dict):
            preview_sha256 = {}
        return {"next_fencing_token": next_token, "claims": claims, "preview_sha256": preview_sha256}

    def _save_state(self, state: dict) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True)
        tmp = self._state_path.with_suffix(self._state_path.suffix + ".tmp")
        tmp.write_text(serialized, encoding="utf-8")
        if sys.platform == "win32":
            # Ensure temp file handle is fully released before rename on Windows
            try:
                import ctypes
                _MOVEFILE_REPLACE_EXISTING = 0x1
                if not ctypes.windll.kernel32.MoveFileExW(  # type: ignore[attr-defined]
                    str(tmp), str(self._state_path), _MOVEFILE_REPLACE_EXISTING,
                ):
                    # Fallback to os.replace on MoveFileExW failure
                    os.replace(str(tmp), str(self._state_path))
            except Exception:
                os.replace(str(tmp), str(self._state_path))
        else:
            tmp.replace(self._state_path)
        # Clean up temp file if it still exists (e.g. replace succeeded but unlink didn't)
        if tmp.exists():
            with contextlib.suppress(OSError):
                tmp.unlink()
