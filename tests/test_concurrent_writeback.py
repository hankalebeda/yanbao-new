"""Concurrent writeback collision tests.

Tests verify:
1. Two agents claiming overlapping paths → second is rejected
2. Two agents claiming non-overlapping paths → both succeed
3. Sequential claim-after-release works
4. Concurrent commit with stale fencing token is rejected
5. Optimistic SHA256 concurrency prevents stale-write
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.services.writeback_coordination import (
    ClaimConflictError,
    LeaseRejectedError,
    WritebackCoordination,
)


def _clock(start: datetime):
    holder = {"now": start}

    def _now() -> datetime:
        return holder["now"]

    return holder, _now


def _coord(tmp_path: Path, now: datetime):
    holder, now_fn = _clock(now)
    svc = WritebackCoordination(
        tmp_path / "runtime" / "writeback_coordination" / "state.json",
        now_fn=now_fn,
    )
    return svc, holder


# ---- Overlapping path collision ----


def test_two_agents_overlapping_paths_second_rejected(tmp_path: Path):
    """Agent B claiming overlapping paths while Agent A holds active lease → conflict."""
    now = datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    lease_a = coord.claim(
        "agent-a-round-001",
        ["app/services/shared.py", "app/services/a_only.py"],
        lease_seconds=600,
    )
    assert lease_a.round_id == "agent-a-round-001"

    with pytest.raises(ClaimConflictError) as exc:
        coord.claim(
            "agent-b-round-001",
            ["app/services/shared.py", "app/services/b_only.py"],
            lease_seconds=600,
        )

    assert "app/services/shared.py" in exc.value.conflict_paths
    assert exc.value.holder_round_id == "agent-a-round-001"
    assert exc.value.holder_lease_id == lease_a.lease_id


# ---- Non-overlapping paths succeed ----


def test_two_agents_non_overlapping_paths_both_succeed(tmp_path: Path):
    """Two agents with disjoint write_scope can both hold leases."""
    now = datetime(2026, 3, 30, 12, 5, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    lease_a = coord.claim(
        "agent-a-round-002",
        ["app/services/module_a.py"],
        lease_seconds=600,
    )
    lease_b = coord.claim(
        "agent-b-round-002",
        ["app/services/module_b.py"],
        lease_seconds=600,
    )

    assert lease_a.lease_id != lease_b.lease_id
    assert lease_b.fencing_token > lease_a.fencing_token

    coord.assert_submit_allowed(
        lease_a.lease_id, lease_a.fencing_token, ["app/services/module_a.py"]
    )
    coord.assert_submit_allowed(
        lease_b.lease_id, lease_b.fencing_token, ["app/services/module_b.py"]
    )


# ---- Sequential claim after release ----


def test_sequential_claim_after_release(tmp_path: Path):
    """After Agent A releases, Agent B can claim same paths."""
    now = datetime(2026, 3, 30, 12, 10, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    lease_a = coord.claim(
        "agent-a-round-003",
        ["app/services/contested.py"],
        lease_seconds=600,
    )

    coord.release(lease=lease_a, reason="round_finished")

    lease_b = coord.claim(
        "agent-b-round-003",
        ["app/services/contested.py"],
        lease_seconds=600,
    )
    assert lease_b.round_id == "agent-b-round-003"
    assert lease_b.fencing_token > lease_a.fencing_token

    coord.assert_submit_allowed(
        lease_b.lease_id, lease_b.fencing_token, ["app/services/contested.py"]
    )


def test_refresh_extends_active_lease(tmp_path: Path):
    start = datetime(2026, 3, 30, 12, 12, tzinfo=timezone.utc)
    coord, holder = _coord(tmp_path, start)

    lease = coord.claim(
        "agent-refresh-round-001",
        ["app/services/refreshed.py"],
        lease_seconds=30,
    )
    original_until = datetime.fromisoformat(lease.lease_until)

    holder["now"] = start + timedelta(seconds=20)
    refreshed = coord.refresh(lease=lease)

    assert refreshed is not None
    assert refreshed.lease_id == lease.lease_id
    assert datetime.fromisoformat(refreshed.lease_until) > original_until


# ---- Stale fencing token rejected ----


def test_stale_fencing_token_from_expired_lease_rejected(tmp_path: Path):
    """Agent A's lease expires, Agent B reclaims; Agent A's old fence token is rejected."""
    start = datetime(2026, 3, 30, 12, 15, tzinfo=timezone.utc)
    coord, holder = _coord(tmp_path, start)

    lease_a = coord.claim(
        "agent-a-round-004",
        ["app/services/target.py"],
        lease_seconds=30,
    )

    holder["now"] = start + timedelta(seconds=31)

    lease_b = coord.claim(
        "agent-b-round-004",
        ["app/services/target.py"],
        lease_seconds=600,
    )

    with pytest.raises(LeaseRejectedError) as exc:
        coord.assert_submit_allowed(
            lease_a.lease_id, lease_a.fencing_token, ["app/services/target.py"]
        )
    assert "LEASE_NOT_ACTIVE" in str(exc.value)

    coord.assert_submit_allowed(
        lease_b.lease_id, lease_b.fencing_token, ["app/services/target.py"]
    )


# ---- Optimistic SHA256 concurrency ----


def test_sha256_optimistic_concurrency_rejects_stale_write(tmp_path: Path):
    """If file content changes between preview and commit, verify_base_sha256 returns False."""
    now = datetime(2026, 3, 30, 12, 20, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    lease = coord.claim("sha-round-001", ["app/services/evolving.py"], lease_seconds=600)

    coord.register_preview_sha256(
        lease.lease_id, "app/services/evolving.py", "original_sha256"
    )

    assert coord.verify_base_sha256(
        lease.lease_id, "app/services/evolving.py", "original_sha256"
    ) is True

    assert coord.verify_base_sha256(
        lease.lease_id, "app/services/evolving.py", "modified_sha256"
    ) is False


def test_sha256_tracks_multiple_files_per_lease(tmp_path: Path):
    """SHA256 tracking works for multiple files within the same lease."""
    now = datetime(2026, 3, 30, 12, 25, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    lease = coord.claim(
        "sha-round-002",
        ["app/services/a.py", "app/services/b.py"],
        lease_seconds=600,
    )

    coord.register_preview_sha256(lease.lease_id, "app/services/a.py", "sha_a")
    coord.register_preview_sha256(lease.lease_id, "app/services/b.py", "sha_b")

    assert coord.verify_base_sha256(lease.lease_id, "app/services/a.py", "sha_a") is True
    assert coord.verify_base_sha256(lease.lease_id, "app/services/b.py", "sha_b") is True
    assert coord.verify_base_sha256(lease.lease_id, "app/services/a.py", "sha_b") is False


# ---- Idempotent claim returns same lease ----


def test_idempotent_claim_same_round_same_targets(tmp_path: Path):
    """Claiming twice with same round_id + targets returns identical lease."""
    now = datetime(2026, 3, 30, 12, 30, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    first = coord.claim("idem-round-001", ["app/services/x.py", "app/services/y.py"], lease_seconds=120)
    second = coord.claim("idem-round-001", ["app/services/y.py", "app/services/x.py"], lease_seconds=120)

    assert first.lease_id == second.lease_id
    assert first.fencing_token == second.fencing_token


# ---- Multiple concurrent leases with disjoint paths ----


def test_many_agents_disjoint_paths(tmp_path: Path):
    """Five agents with completely disjoint paths can all coexist."""
    now = datetime(2026, 3, 30, 12, 35, tzinfo=timezone.utc)
    coord, _ = _coord(tmp_path, now)

    leases = []
    for i in range(5):
        lease = coord.claim(
            f"agent-{i}-round",
            [f"app/services/module_{i}.py"],
            lease_seconds=600,
        )
        leases.append(lease)

    assert len(set(l.lease_id for l in leases)) == 5

    for lease in leases:
        for path in lease.target_paths:
            coord.assert_submit_allowed(lease.lease_id, lease.fencing_token, [path])

    for lease in leases:
        coord.release(lease=lease, reason="done")
