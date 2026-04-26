"""Async Mailbox for inter-agent communication.

Ported from the claude-code-sourcemap ``mailbox.ts`` pattern to Python
asyncio with optional file-backed persistence for crash recovery.

Core API:
    send(msg)              – non-blocking enqueue
    poll(predicate)        – non-blocking check
    receive(predicate)     – async blocking wait with timeout
    subscribe(callback)    – notification on activity

Design choices vs the TypeScript reference:
* Uses ``asyncio.Event`` broadcast pattern internally so multiple
  concurrent ``receive()`` callers each poll their own predicate on
  the shared buffer without stealing messages from other agents.
* Adds optional SQLite persistence (``backing_path``) so the mailbox
  survives process restarts and supports audit queries.
* Thread-safe wrapper (``send_sync``) for agents running in threads.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .protocol import AgentMessage

logger = logging.getLogger(__name__)


class Mailbox:
    """Async message queue with predicate-based receive and optional persistence."""

    def __init__(
        self,
        name: str = "main",
        backing_path: Optional[Path] = None,
        max_queue_size: int = 10_000,
    ):
        self.name = name
        self._buffer: List[AgentMessage] = []          # for predicate scanning
        self._notify: asyncio.Event = asyncio.Event()  # wake all receive() waiters
        self._subscribers: List[Callable[[AgentMessage], Any]] = []
        self._lock = asyncio.Lock()
        self._thread_lock = threading.Lock()

        # Optional SQLite persistence
        self._db: Optional[sqlite3.Connection] = None
        if backing_path is not None:
            backing_path.parent.mkdir(parents=True, exist_ok=True)
            self._db = sqlite3.connect(str(backing_path), check_same_thread=False)
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute(
                """CREATE TABLE IF NOT EXISTS messages (
                    msg_id   TEXT PRIMARY KEY,
                    source   TEXT,
                    target   TEXT,
                    msg_type TEXT,
                    payload  TEXT,
                    ts       TEXT,
                    consumed INTEGER DEFAULT 0
                )"""
            )
            self._db.commit()
            self._restore_unconsumed()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def send(self, msg: AgentMessage) -> None:
        """Non-blocking enqueue.  Notifies subscribers."""
        async with self._lock:
            self._buffer.append(msg)
            self._persist(msg)
        # Wake all ``receive()`` waiters via the event
        self._notify.set()

        for cb in self._subscribers:
            try:
                cb(msg)
            except Exception:
                logger.exception("Subscriber callback error in mailbox %s", self.name)

    def send_sync(self, msg: AgentMessage) -> None:
        """Thread-safe synchronous send (for agents running in threads)."""
        with self._thread_lock:
            self._buffer.append(msg)
            self._persist(msg)
        # Notify async side if an event loop is running
        try:
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(self._notify.set)
        except RuntimeError:
            # No running loop — just buffer; next poll/receive will find it
            pass
        for cb in self._subscribers:
            try:
                cb(msg)
            except Exception:
                logger.exception("Subscriber callback error in mailbox %s", self.name)

    async def poll(
        self,
        predicate: Optional[Callable[[AgentMessage], bool]] = None,
    ) -> Optional[AgentMessage]:
        """Non-blocking: return first matching message or ``None``."""
        async with self._lock:
            for i, msg in enumerate(self._buffer):
                if predicate is None or predicate(msg):
                    self._buffer.pop(i)
                    self._mark_consumed(msg.msg_id)
                    return msg
        return None

    def poll_sync(
        self,
        predicate: Optional[Callable[[AgentMessage], bool]] = None,
    ) -> Optional[AgentMessage]:
        """Thread-safe synchronous poll."""
        with self._thread_lock:
            for i, msg in enumerate(self._buffer):
                if predicate is None or predicate(msg):
                    self._buffer.pop(i)
                    self._mark_consumed(msg.msg_id)
                    return msg
        return None

    async def receive(
        self,
        predicate: Optional[Callable[[AgentMessage], bool]] = None,
        timeout: float = 60.0,
    ) -> Optional[AgentMessage]:
        """Async blocking: wait until a matching message arrives or timeout.

        Uses an Event-based broadcast pattern so multiple concurrent callers
        (different agents) each poll their own predicate on the shared buffer
        without stealing messages destined for other agents.
        """
        # First check buffer
        hit = await self.poll(predicate)
        if hit is not None:
            return hit

        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None

            # Clear event, then re-check buffer to avoid lost-wakeup race
            self._notify.clear()
            hit = await self.poll(predicate)
            if hit is not None:
                return hit

            # Wait for new message notification (broadcast to all waiters)
            try:
                await asyncio.wait_for(self._notify.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                # One last check before giving up
                return await self.poll(predicate)

            # Event fired — check buffer for our predicate
            hit = await self.poll(predicate)
            if hit is not None:
                return hit
            # No match for us — loop back and wait again

    async def poll_all(
        self,
        predicate: Optional[Callable[[AgentMessage], bool]] = None,
    ) -> List[AgentMessage]:
        """Return all matching messages from the buffer."""
        results: List[AgentMessage] = []
        async with self._lock:
            keep: List[AgentMessage] = []
            for msg in self._buffer:
                if predicate is None or predicate(msg):
                    results.append(msg)
                    self._mark_consumed(msg.msg_id)
                else:
                    keep.append(msg)
            self._buffer = keep
        return results

    def subscribe(self, callback: Callable[[AgentMessage], Any]) -> Callable[[], None]:
        """Register a notification callback.  Returns unsubscribe function."""
        self._subscribers.append(callback)
        def _unsub():
            try:
                self._subscribers.remove(callback)
            except ValueError:
                pass
        return _unsub

    @property
    def depth(self) -> int:
        """Current number of unconsumed messages in buffer."""
        return len(self._buffer)

    async def drain(self) -> List[AgentMessage]:
        """Remove and return all buffered messages."""
        async with self._lock:
            msgs = list(self._buffer)
            self._buffer.clear()
        return msgs

    def close(self) -> None:
        """Close backing store if open."""
        if self._db is not None:
            try:
                self._db.close()
            except Exception:
                pass
            self._db = None

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _persist(self, msg: AgentMessage) -> None:
        if self._db is None:
            return
        try:
            self._db.execute(
                "INSERT OR IGNORE INTO messages (msg_id, source, target, msg_type, payload, ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    msg.msg_id,
                    msg.source,
                    msg.target,
                    msg.msg_type,
                    json.dumps(msg.payload, ensure_ascii=False),
                    msg.timestamp,
                ),
            )
            self._db.commit()
        except Exception:
            logger.exception("Mailbox persist error")

    def _mark_consumed(self, msg_id: str) -> None:
        if self._db is None:
            return
        try:
            self._db.execute(
                "UPDATE messages SET consumed = 1 WHERE msg_id = ?", (msg_id,)
            )
            self._db.commit()
        except Exception:
            pass

    def _restore_unconsumed(self) -> None:
        """On startup, reload messages that were never consumed."""
        if self._db is None:
            return
        try:
            rows = self._db.execute(
                "SELECT msg_id, source, target, msg_type, payload, ts "
                "FROM messages WHERE consumed = 0 ORDER BY ts"
            ).fetchall()
            for row in rows:
                msg = AgentMessage(
                    msg_id=row[0],
                    source=row[1],
                    target=row[2],
                    msg_type=row[3],
                    payload=json.loads(row[4]) if row[4] else {},
                    timestamp=row[5],
                )
                self._buffer.append(msg)
            if rows:
                logger.info(
                    "Mailbox %s restored %d unconsumed messages", self.name, len(rows)
                )
                self._notify.set()  # Wake any early waiters
        except Exception:
            logger.exception("Mailbox restore error")

    # ------------------------------------------------------------------
    # Convenience factory
    # ------------------------------------------------------------------

    @classmethod
    def create_pair(
        cls,
        name_a: str = "coordinator",
        name_b: str = "worker",
        backing_dir: Optional[Path] = None,
    ) -> tuple["Mailbox", "Mailbox"]:
        """Create two mailboxes (one per direction) for point-to-point channels."""
        bp_a = backing_dir / f"{name_a}.db" if backing_dir else None
        bp_b = backing_dir / f"{name_b}.db" if backing_dir else None
        return cls(name=name_a, backing_path=bp_a), cls(name=name_b, backing_path=bp_b)
