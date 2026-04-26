"""Team Memory — cross-agent shared scratchpad with SHA256 idempotency.

Inspired by the LiteLLM claude-code-sourcemap ``teamMemorySync`` module:
- Central scratchpad file accessible by all agents
- SHA256 delta idempotency: duplicate writes are silently skipped
- Secret guard: blocks credential-like content from being persisted
- Append-only log for audit trail
- Thread-safe + async-safe access

The scratchpad is a JSON file at::

    runtime/agents/team_memory.json

Each entry is keyed by a deterministic ID derived from the content hash
to ensure absolute idempotency across retries and concurrent writers.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Secret patterns to block from team memory (OWASP-aware)
_SECRET_PATTERNS = [
    re.compile(r"sk-[A-Za-z0-9]{20,}", re.IGNORECASE),
    re.compile(r"password\s*[:=]\s*\S+", re.IGNORECASE),
    re.compile(r"Bearer\s+[A-Za-z0-9._~+/=-]{20,}", re.IGNORECASE),
    re.compile(r"api[_-]?key\s*[:=]\s*\S+", re.IGNORECASE),
]


def _contains_secret(text: str) -> bool:
    """Check if text contains credential-like patterns."""
    for pattern in _SECRET_PATTERNS:
        if pattern.search(text):
            return True
    return False


def _content_hash(content: str) -> str:
    """Deterministic SHA256 hash of content for idempotency."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


@dataclass
class MemoryEntry:
    """A single entry in the team memory scratchpad."""
    entry_id: str = ""
    agent_id: str = ""
    category: str = ""  # "finding", "fix_result", "evidence", "note"
    content: str = ""
    content_hash: str = ""
    round_id: str = ""
    timestamp: str = ""
    supersedes: str = ""  # entry_id that this entry replaces

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "MemoryEntry":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


class TeamMemory:
    """Cross-agent shared scratchpad with SHA256 idempotency.

    Thread-safe and supports both sync and async access patterns.
    """

    def __init__(self, state_dir: Path):
        self._path = state_dir / "team_memory.json"
        self._log_path = state_dir / "team_memory_log.jsonl"
        self._lock = threading.Lock()
        self._entries: Dict[str, MemoryEntry] = {}
        self._known_hashes: set = set()
        self._load()

    def _load(self) -> None:
        """Load existing entries from disk."""
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
                for key, val in data.items():
                    entry = MemoryEntry.from_dict(val)
                    self._entries[key] = entry
                    self._known_hashes.add(entry.content_hash)
            except Exception:
                logger.warning("Failed to load team memory, starting fresh")

    def write(
        self,
        agent_id: str,
        category: str,
        content: str,
        round_id: str = "",
        supersedes: str = "",
    ) -> Optional[str]:
        """Write an entry to team memory.

        Returns the entry_id if written, None if skipped (duplicate or secret).
        Idempotent: if content with the same hash already exists, no-op.
        """
        if _contains_secret(content):
            logger.warning(
                "[team_memory] Blocked secret-containing write from %s",
                agent_id,
            )
            return None

        ch = _content_hash(content)

        with self._lock:
            if ch in self._known_hashes and not supersedes:
                # Idempotent skip — content already present
                return None

            entry_id = f"{category}-{ch}"
            now = datetime.now(timezone.utc).isoformat()

            entry = MemoryEntry(
                entry_id=entry_id,
                agent_id=agent_id,
                category=category,
                content=content,
                content_hash=ch,
                round_id=round_id,
                timestamp=now,
                supersedes=supersedes,
            )

            # If superseding, remove old entry
            if supersedes and supersedes in self._entries:
                old = self._entries.pop(supersedes)
                self._known_hashes.discard(old.content_hash)

            self._entries[entry_id] = entry
            self._known_hashes.add(ch)
            self._persist()
            self._append_log(entry)
            return entry_id

    def read(
        self,
        category: Optional[str] = None,
        round_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[MemoryEntry]:
        """Read entries from team memory with optional filters."""
        with self._lock:
            entries = list(self._entries.values())

        if category:
            entries = [e for e in entries if e.category == category]
        if round_id:
            entries = [e for e in entries if e.round_id == round_id]

        # Most recent first
        entries.sort(key=lambda e: e.timestamp, reverse=True)
        return entries[:limit]

    def read_by_id(self, entry_id: str) -> Optional[MemoryEntry]:
        """Read a specific entry by ID."""
        with self._lock:
            return self._entries.get(entry_id)

    def clear_round(self, round_id: str) -> int:
        """Remove all entries for a specific round. Returns count removed."""
        with self._lock:
            to_remove = [
                eid for eid, e in self._entries.items()
                if e.round_id == round_id
            ]
            for eid in to_remove:
                entry = self._entries.pop(eid)
                self._known_hashes.discard(entry.content_hash)
            if to_remove:
                self._persist()
            return len(to_remove)

    def summary(self) -> Dict[str, Any]:
        """Return summary statistics."""
        with self._lock:
            categories: Dict[str, int] = {}
            for e in self._entries.values():
                categories[e.category] = categories.get(e.category, 0) + 1
            return {
                "total_entries": len(self._entries),
                "categories": categories,
                "unique_hashes": len(self._known_hashes),
            }

    def _persist(self) -> None:
        """Atomic write to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        import tempfile, os
        data = {k: v.to_dict() for k, v in self._entries.items()}
        fd, tmp = tempfile.mkstemp(
            dir=str(self._path.parent), suffix=".tmp", prefix=".tm_"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, str(self._path))
        except Exception:
            try:
                os.unlink(tmp)
            except Exception:
                pass
            raise

    def _append_log(self, entry: MemoryEntry) -> None:
        """Append-only log for audit trail."""
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry.to_dict(), ensure_ascii=False) + "\n")
        except Exception:
            logger.warning("Failed to append to team memory log")
