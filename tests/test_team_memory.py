"""Tests for TeamMemory — cross-agent shared scratchpad.

Validates idempotency, secret guarding, CRUD operations,
and persistence.
"""
import json
import pytest
from pathlib import Path

from automation.agents.team_memory import TeamMemory, MemoryEntry, _contains_secret


class TestSecretGuard:
    """Test that secrets are blocked from team memory."""

    def test_blocks_api_key(self):
        assert _contains_secret("my api_key=sk-abc123def456789012")

    def test_blocks_bearer_token(self):
        assert _contains_secret("Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.xxxxx")

    def test_blocks_password(self):
        assert _contains_secret("password: SuperSecret123!")

    def test_allows_normal_text(self):
        assert not _contains_secret("Fixed issue in app/services/foo.py")

    def test_allows_code_content(self):
        assert not _contains_secret("def calculate_total(items): return sum(i.price for i in items)")


class TestTeamMemory:
    """Tests for TeamMemory CRUD and idempotency."""

    def test_write_and_read(self, tmp_path: Path):
        tm = TeamMemory(tmp_path)
        eid = tm.write("agent-1", "finding", "Test finding content", round_id="r-001")
        assert eid is not None

        entries = tm.read(category="finding")
        assert len(entries) == 1
        assert entries[0].content == "Test finding content"
        assert entries[0].agent_id == "agent-1"
        assert entries[0].round_id == "r-001"

    def test_idempotent_write(self, tmp_path: Path):
        tm = TeamMemory(tmp_path)
        eid1 = tm.write("agent-1", "finding", "Duplicate content")
        eid2 = tm.write("agent-2", "finding", "Duplicate content")
        assert eid1 is not None
        assert eid2 is None  # duplicate skipped
        assert len(tm.read()) == 1

    def test_secret_blocked(self, tmp_path: Path):
        tm = TeamMemory(tmp_path)
        eid = tm.write("agent-1", "note", "key: sk-abcdefghijklmnopqrstuvwxyz")
        assert eid is None
        assert len(tm.read()) == 0

    def test_supersedes_replaces(self, tmp_path: Path):
        tm = TeamMemory(tmp_path)
        eid1 = tm.write("agent-1", "evidence", "Version 1")
        eid2 = tm.write("agent-1", "evidence", "Version 2", supersedes=eid1)
        assert eid2 is not None
        entries = tm.read()
        assert len(entries) == 1
        assert entries[0].content == "Version 2"

    def test_filter_by_round(self, tmp_path: Path):
        tm = TeamMemory(tmp_path)
        tm.write("a1", "finding", "Round 1 item", round_id="r-001")
        tm.write("a1", "finding", "Round 2 item", round_id="r-002")
        entries = tm.read(round_id="r-001")
        assert len(entries) == 1
        assert entries[0].round_id == "r-001"

    def test_filter_by_category(self, tmp_path: Path):
        tm = TeamMemory(tmp_path)
        tm.write("a1", "finding", "A finding")
        tm.write("a1", "evidence", "Some evidence")
        assert len(tm.read(category="finding")) == 1
        assert len(tm.read(category="evidence")) == 1

    def test_read_by_id(self, tmp_path: Path):
        tm = TeamMemory(tmp_path)
        eid = tm.write("a1", "note", "My note")
        entry = tm.read_by_id(eid)
        assert entry is not None
        assert entry.content == "My note"

    def test_clear_round(self, tmp_path: Path):
        tm = TeamMemory(tmp_path)
        tm.write("a1", "finding", "Item 1", round_id="r-001")
        tm.write("a1", "finding", "Item 2", round_id="r-001")
        tm.write("a1", "finding", "Item 3", round_id="r-002")
        count = tm.clear_round("r-001")
        assert count == 2
        assert len(tm.read()) == 1

    def test_summary(self, tmp_path: Path):
        tm = TeamMemory(tmp_path)
        tm.write("a1", "finding", "F1")
        tm.write("a1", "finding", "F2")
        tm.write("a1", "evidence", "E1")
        s = tm.summary()
        assert s["total_entries"] == 3
        assert s["categories"]["finding"] == 2
        assert s["categories"]["evidence"] == 1

    def test_persistence(self, tmp_path: Path):
        tm1 = TeamMemory(tmp_path)
        tm1.write("a1", "finding", "Persisted content")

        # Reload
        tm2 = TeamMemory(tmp_path)
        entries = tm2.read()
        assert len(entries) == 1
        assert entries[0].content == "Persisted content"

    def test_audit_log(self, tmp_path: Path):
        tm = TeamMemory(tmp_path)
        tm.write("a1", "note", "Entry 1")
        tm.write("a1", "note", "Entry 2")

        log_path = tmp_path / "team_memory_log.jsonl"
        assert log_path.exists()
        lines = log_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2

    def test_memory_entry_serialization(self):
        entry = MemoryEntry(
            entry_id="test-id",
            agent_id="a1",
            category="finding",
            content="Test",
            content_hash="abc123",
        )
        d = entry.to_dict()
        e2 = MemoryEntry.from_dict(d)
        assert e2.entry_id == "test-id"
        assert e2.content == "Test"
