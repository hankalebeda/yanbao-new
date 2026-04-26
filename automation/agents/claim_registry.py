"""Claim Registry — centralized task claiming with write-scope validation.

Inspired by the LiteLLM claude-code-sourcemap ``claimTask()`` pattern
and the issue_mesh workspace_modes isolation model, this module provides:

1. **Atomic task claiming**: Only one agent can claim a problem at a time
2. **Write-scope validation**: Claims include write-scope so overlapping
   modifications are detected and prevented
3. **Lease-based expiry**: Claims auto-expire after a configurable TTL
   to prevent deadlocks from crashed agents
4. **Conflict detection**: Before claiming, checks for scope overlap
   with existing active claims
5. **Audit trail**: All claim/release events are logged

Usage::

    registry = ClaimRegistry(state_dir)

    # Claim a problem for fixing
    claim = registry.claim("problem-123", "fix-agent-01",
                          write_scope=["app/services/foo.py"])
    if claim:
        try:
            ... # do work
        finally:
            registry.release(claim.claim_id)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

DEFAULT_CLAIM_TTL = 600.0  # 10 minutes


@dataclass
class Claim:
    """A single task claim."""
    claim_id: str = ""
    problem_id: str = ""
    agent_id: str = ""
    round_id: str = ""
    write_scope: List[str] = field(default_factory=list)
    claimed_at: float = 0.0          # monotonic time
    expires_at: float = 0.0          # monotonic time
    released: bool = False
    released_at: float = 0.0

    def is_active(self) -> bool:
        if self.released:
            return False
        return time.monotonic() < self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Convert monotonic times to ISO for persistence
        now_mono = time.monotonic()
        now_wall = time.time()
        d["claimed_at_iso"] = datetime.fromtimestamp(
            now_wall - (now_mono - self.claimed_at), tz=timezone.utc
        ).isoformat() if self.claimed_at else ""
        d["ttl_remaining"] = max(0, self.expires_at - now_mono) if not self.released else 0
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Claim":
        filtered = {k: v for k, v in d.items() if k in cls.__dataclass_fields__}
        return cls(**filtered)


class ClaimConflictError(Exception):
    """Raised when a claim conflicts with an existing active claim."""
    def __init__(self, problem_id: str, conflicting_claim: Claim):
        self.problem_id = problem_id
        self.conflicting_claim = conflicting_claim
        super().__init__(
            f"Problem {problem_id} conflicts with active claim "
            f"{conflicting_claim.claim_id} by {conflicting_claim.agent_id}"
        )


class ClaimRegistry:
    """Centralized registry for atomic task claiming with scope validation.

    Thread-safe. Persists claims to disk for crash recovery.
    """

    def __init__(self, state_dir: Path, default_ttl: float = DEFAULT_CLAIM_TTL):
        self._state_dir = state_dir
        self._claims_path = state_dir / "claims_registry.json"
        self._log_path = state_dir / "claims_log.jsonl"
        self._default_ttl = default_ttl
        self._lock = threading.Lock()
        self._claims: Dict[str, Claim] = {}
        self._load()

    def claim(
        self,
        problem_id: str,
        agent_id: str,
        write_scope: Optional[List[str]] = None,
        round_id: str = "",
        ttl: Optional[float] = None,
    ) -> Optional[Claim]:
        """Attempt to claim a problem for exclusive work.

        Returns the Claim if successful, None if the problem is already
        claimed (same problem_id), raises ClaimConflictError if write
        scopes overlap with existing claims.
        """
        with self._lock:
            self._expire_stale()

            # Check for exact problem_id claim
            for c in self._claims.values():
                if c.is_active() and c.problem_id == problem_id:
                    logger.info(
                        "[claims] Problem %s already claimed by %s",
                        problem_id, c.agent_id,
                    )
                    return None

            # Check for scope overlap with active claims
            scope = write_scope or []
            if scope:
                for c in self._claims.values():
                    if c.is_active() and self._scopes_overlap(scope, c.write_scope):
                        raise ClaimConflictError(problem_id, c)

            now = time.monotonic()
            claim_ttl = ttl if ttl is not None else self._default_ttl
            claim_id = f"claim-{problem_id[:16]}-{hashlib.md5(f'{agent_id}{now}'.encode()).hexdigest()[:8]}"

            new_claim = Claim(
                claim_id=claim_id,
                problem_id=problem_id,
                agent_id=agent_id,
                round_id=round_id,
                write_scope=scope,
                claimed_at=now,
                expires_at=now + claim_ttl,
            )

            self._claims[claim_id] = new_claim
            self._persist()
            self._log_event("claimed", new_claim)

            logger.info(
                "[claims] %s claimed %s (scope=%s, ttl=%.0fs)",
                agent_id, problem_id, scope, claim_ttl,
            )
            return new_claim

    def release(self, claim_id: str) -> bool:
        """Release a claim."""
        with self._lock:
            claim = self._claims.get(claim_id)
            if not claim:
                return False
            if claim.released:
                return True  # idempotent

            claim.released = True
            claim.released_at = time.monotonic()
            self._persist()
            self._log_event("released", claim)

            logger.info(
                "[claims] Released %s (problem=%s, agent=%s)",
                claim_id, claim.problem_id, claim.agent_id,
            )
            return True

    def release_by_agent(self, agent_id: str) -> int:
        """Release all claims held by a specific agent. Returns count."""
        with self._lock:
            count = 0
            for claim in self._claims.values():
                if claim.agent_id == agent_id and claim.is_active():
                    claim.released = True
                    claim.released_at = time.monotonic()
                    count += 1
            if count:
                self._persist()
            return count

    def release_by_round(self, round_id: str) -> int:
        """Release all claims for a specific round. Returns count."""
        with self._lock:
            count = 0
            for claim in self._claims.values():
                if claim.round_id == round_id and claim.is_active():
                    claim.released = True
                    claim.released_at = time.monotonic()
                    count += 1
            if count:
                self._persist()
            return count

    def active_claims(self) -> List[Claim]:
        """Return all active (non-expired, non-released) claims."""
        with self._lock:
            self._expire_stale()
            return [c for c in self._claims.values() if c.is_active()]

    def is_claimed(self, problem_id: str) -> bool:
        """Check if a problem is currently claimed."""
        with self._lock:
            self._expire_stale()
            return any(
                c.is_active() and c.problem_id == problem_id
                for c in self._claims.values()
            )

    def get_claim(self, problem_id: str) -> Optional[Claim]:
        """Get the active claim for a problem, if any."""
        with self._lock:
            self._expire_stale()
            for c in self._claims.values():
                if c.is_active() and c.problem_id == problem_id:
                    return c
            return None

    def summary(self) -> Dict[str, Any]:
        """Return summary statistics."""
        with self._lock:
            self._expire_stale()
            active = [c for c in self._claims.values() if c.is_active()]
            return {
                "total_claims": len(self._claims),
                "active_claims": len(active),
                "agents": list(set(c.agent_id for c in active)),
                "scoped_files": sorted(set(
                    f for c in active for f in c.write_scope
                )),
            }

    def _expire_stale(self) -> None:
        """Mark expired claims as released."""
        now = time.monotonic()
        for claim in self._claims.values():
            if not claim.released and now >= claim.expires_at:
                claim.released = True
                claim.released_at = now
                logger.info(
                    "[claims] Expired claim %s (problem=%s, agent=%s)",
                    claim.claim_id, claim.problem_id, claim.agent_id,
                )

    @staticmethod
    def _scopes_overlap(scope_a: List[str], scope_b: List[str]) -> bool:
        """Check if two write scopes overlap.

        Uses path-prefix matching so that:
        - "app/services/foo.py" overlaps with "app/services/foo.py"
        - "app/services/**" overlaps with "app/services/foo.py"
        - "app/services/foo.py" does NOT overlap with "tests/test_foo.py"
        """
        if not scope_a or not scope_b:
            return False
        for a in scope_a:
            a_root = a.split("**")[0].rstrip("*").rstrip("/")
            for b in scope_b:
                b_root = b.split("**")[0].rstrip("*").rstrip("/")
                if not a_root or not b_root:
                    return True  # wildcard scope — always overlaps
                if (a_root.startswith(b_root) or b_root.startswith(a_root)
                        or a == b):
                    return True
        return False

    def _persist(self) -> None:
        """Atomic write."""
        import tempfile
        self._state_dir.mkdir(parents=True, exist_ok=True)
        data = {k: v.to_dict() for k, v in self._claims.items()}
        fd, tmp = tempfile.mkstemp(
            dir=str(self._state_dir), suffix=".tmp", prefix=".cr_"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, str(self._claims_path))
        except Exception:
            try:
                os.unlink(tmp)
            except Exception:
                pass
            raise

    def _load(self) -> None:
        """Load claims from disk."""
        if self._claims_path.exists():
            try:
                data = json.loads(self._claims_path.read_text(encoding="utf-8"))
                for key, val in data.items():
                    self._claims[key] = Claim.from_dict(val)
            except Exception:
                logger.warning("Failed to load claims registry, starting fresh")

    def _log_event(self, event: str, claim: Claim) -> None:
        """Append audit log."""
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            entry = {
                "event": event,
                "claim_id": claim.claim_id,
                "problem_id": claim.problem_id,
                "agent_id": claim.agent_id,
                "write_scope": claim.write_scope,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            with open(self._log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            logger.warning("Failed to write claims log")
