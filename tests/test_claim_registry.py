"""Tests for ClaimRegistry — centralized task claiming with scope validation.

Validates atomic claiming, scope overlap detection, TTL expiry,
conflict handling, and persistence.
"""
import time
import pytest
from pathlib import Path

from automation.agents.claim_registry import (
    Claim,
    ClaimConflictError,
    ClaimRegistry,
)


class TestClaimRegistry:
    """Tests for ClaimRegistry claiming and releasing."""

    def test_basic_claim_and_release(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        claim = reg.claim("prob-001", "agent-1")
        assert claim is not None
        assert claim.problem_id == "prob-001"
        assert claim.agent_id == "agent-1"
        assert claim.is_active()

        reg.release(claim.claim_id)
        assert not reg.is_claimed("prob-001")

    def test_duplicate_claim_returns_none(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        c1 = reg.claim("prob-001", "agent-1")
        c2 = reg.claim("prob-001", "agent-2")
        assert c1 is not None
        assert c2 is None

    def test_scope_overlap_raises(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        reg.claim("prob-001", "agent-1", write_scope=["app/services/foo.py"])
        with pytest.raises(ClaimConflictError):
            reg.claim("prob-002", "agent-2", write_scope=["app/services/foo.py"])

    def test_non_overlapping_scopes(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        c1 = reg.claim("prob-001", "agent-1", write_scope=["app/services/foo.py"])
        c2 = reg.claim("prob-002", "agent-2", write_scope=["tests/test_bar.py"])
        assert c1 is not None
        assert c2 is not None

    def test_wildcard_scope_overlap(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        reg.claim("prob-001", "agent-1", write_scope=["app/services/**"])
        with pytest.raises(ClaimConflictError):
            reg.claim("prob-002", "agent-2", write_scope=["app/services/bar.py"])

    def test_ttl_expiry(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path, default_ttl=0.1)
        claim = reg.claim("prob-001", "agent-1")
        assert claim is not None
        assert reg.is_claimed("prob-001")
        time.sleep(0.2)
        # After TTL, claim should be expired
        assert not reg.is_claimed("prob-001")

    def test_release_by_agent(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        reg.claim("prob-001", "agent-1")
        reg.claim("prob-002", "agent-1", write_scope=["tests/test_a.py"])
        count = reg.release_by_agent("agent-1")
        assert count == 2
        assert len(reg.active_claims()) == 0

    def test_release_by_round(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        reg.claim("prob-001", "agent-1", round_id="r-001")
        reg.claim("prob-002", "agent-2", round_id="r-001", write_scope=["tests/a.py"])
        reg.claim("prob-003", "agent-3", round_id="r-002", write_scope=["tests/b.py"])
        count = reg.release_by_round("r-001")
        assert count == 2
        assert len(reg.active_claims()) == 1

    def test_release_idempotent(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        claim = reg.claim("prob-001", "agent-1")
        assert reg.release(claim.claim_id)
        assert reg.release(claim.claim_id)  # idempotent

    def test_active_claims(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        reg.claim("prob-001", "agent-1")
        reg.claim("prob-002", "agent-2", write_scope=["tests/x.py"])
        assert len(reg.active_claims()) == 2

    def test_get_claim(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        reg.claim("prob-001", "agent-1")
        claim = reg.get_claim("prob-001")
        assert claim is not None
        assert claim.agent_id == "agent-1"

    def test_no_claim_returns_none(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        assert reg.get_claim("nonexistent") is None

    def test_summary(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        reg.claim("prob-001", "agent-1", write_scope=["app/a.py"])
        reg.claim("prob-002", "agent-2", write_scope=["app/b.py"])
        s = reg.summary()
        assert s["active_claims"] == 2
        assert set(s["agents"]) == {"agent-1", "agent-2"}
        assert set(s["scoped_files"]) == {"app/a.py", "app/b.py"}

    def test_claim_after_release(self, tmp_path: Path):
        """After releasing, a new claim on the same problem succeeds."""
        reg = ClaimRegistry(tmp_path)
        c1 = reg.claim("prob-001", "agent-1")
        reg.release(c1.claim_id)
        c2 = reg.claim("prob-001", "agent-2")
        assert c2 is not None
        assert c2.agent_id == "agent-2"


class TestClaimScopeOverlap:
    """Tests for scope overlap detection logic."""

    def test_exact_match(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        assert reg._scopes_overlap(["app/a.py"], ["app/a.py"])

    def test_prefix_match(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        assert reg._scopes_overlap(["app/services/**"], ["app/services/foo.py"])

    def test_no_overlap(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        assert not reg._scopes_overlap(["app/a.py"], ["tests/b.py"])

    def test_empty_scope_no_overlap(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        assert not reg._scopes_overlap([], ["app/a.py"])
        assert not reg._scopes_overlap(["app/a.py"], [])

    def test_wildcard_always_overlaps(self, tmp_path: Path):
        reg = ClaimRegistry(tmp_path)
        # Empty root from "**" means wildcard
        assert reg._scopes_overlap(["**"], ["app/anything.py"])


class TestClaimSerialization:
    """Tests for Claim serialization."""

    def test_claim_to_dict(self):
        c = Claim(
            claim_id="c-001",
            problem_id="p-001",
            agent_id="a-1",
            write_scope=["app/foo.py"],
            claimed_at=1000.0,
            expires_at=1600.0,
        )
        d = c.to_dict()
        assert d["claim_id"] == "c-001"
        assert d["problem_id"] == "p-001"
        assert "ttl_remaining" in d
