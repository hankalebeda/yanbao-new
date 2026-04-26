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


def _service(tmp_path: Path, now: datetime):
    holder, now_fn = _clock(now)
    svc = WritebackCoordination(tmp_path / "runtime" / "writeback_coordination" / "state.json", now_fn=now_fn)
    return svc, holder


def test_claim_returns_lease_payload(tmp_path: Path):
    now = datetime(2026, 3, 29, 1, 0, tzinfo=timezone.utc)
    svc, _ = _service(tmp_path, now)

    lease = svc.claim("fix-loop-001", ["app/services/a.py", "tests/test_a.py"], lease_seconds=120)

    assert lease.round_id == "fix-loop-001"
    assert lease.fencing_token == 1
    assert set(lease.target_paths) == {"app/services/a.py", "tests/test_a.py"}
    assert lease.lease_id
    assert lease.issued_at
    assert lease.lease_until


def test_claim_is_idempotent_for_same_round_and_same_targets(tmp_path: Path):
    now = datetime(2026, 3, 29, 1, 5, tzinfo=timezone.utc)
    svc, _ = _service(tmp_path, now)

    first = svc.claim("fix-loop-002", ["tests/test_b.py", "app/services/b.py"], lease_seconds=60)
    second = svc.claim("fix-loop-002", ["app/services/b.py", "tests/test_b.py", "app/services/b.py"], lease_seconds=60)

    assert second.lease_id == first.lease_id
    assert second.fencing_token == first.fencing_token
    assert second.target_paths == first.target_paths


def test_claim_blocks_overlap_between_different_rounds(tmp_path: Path):
    now = datetime(2026, 3, 29, 1, 10, tzinfo=timezone.utc)
    svc, _ = _service(tmp_path, now)
    svc.claim("fix-loop-003", ["app/services/c.py", "tests/test_c.py"], lease_seconds=300)

    with pytest.raises(ClaimConflictError) as exc:
        svc.claim("fix-loop-004", ["tests/test_c.py", "app/services/d.py"], lease_seconds=300)

    assert exc.value.conflict_paths == ("tests/test_c.py",)
    assert exc.value.holder_round_id == "fix-loop-003"


def test_stale_lease_auto_takeover(tmp_path: Path):
    start = datetime(2026, 3, 29, 1, 20, tzinfo=timezone.utc)
    svc, holder = _service(tmp_path, start)
    first = svc.claim("fix-loop-005", ["app/services/e.py"], lease_seconds=10)

    holder["now"] = start + timedelta(seconds=11)
    second = svc.claim("fix-loop-006", ["app/services/e.py"], lease_seconds=10)

    assert second.lease_id != first.lease_id
    assert second.fencing_token == first.fencing_token + 1
    assert second.round_id == "fix-loop-006"


def test_old_lease_submit_rejected_after_takeover(tmp_path: Path):
    start = datetime(2026, 3, 29, 1, 30, tzinfo=timezone.utc)
    svc, holder = _service(tmp_path, start)
    old_lease = svc.claim("fix-loop-007", ["app/services/f.py"], lease_seconds=5)

    holder["now"] = start + timedelta(seconds=6)
    new_lease = svc.claim("fix-loop-008", ["app/services/f.py"], lease_seconds=30)

    with pytest.raises(LeaseRejectedError) as old_exc:
        svc.assert_submit_allowed(
            old_lease.lease_id,
            old_lease.fencing_token,
            ["app/services/f.py"],
        )
    assert old_exc.value.reason == "LEASE_NOT_ACTIVE"

    svc.assert_submit_allowed(
        new_lease.lease_id,
        new_lease.fencing_token,
        ["app/services/f.py"],
    )


def test_submit_rejects_fencing_token_mismatch(tmp_path: Path):
    now = datetime(2026, 3, 29, 1, 40, tzinfo=timezone.utc)
    svc, _ = _service(tmp_path, now)
    lease = svc.claim("fix-loop-009", ["tests/test_f.py"], lease_seconds=30)

    with pytest.raises(LeaseRejectedError) as exc:
        svc.assert_submit_allowed(lease.lease_id, lease.fencing_token + 1, ["tests/test_f.py"])

    assert exc.value.reason == "FENCING_TOKEN_MISMATCH"

