"""Base class for all agents in the autonomous escort team.

Provides lifecycle management, heartbeat, metrics, graceful degradation,
and message dispatch following the claude-code-sourcemap teammate pattern
of context-isolated, self-contained workers.

Subclasses implement ``handle_task()`` for their domain logic.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from .mailbox import Mailbox
from .protocol import (
    AgentHealthSnapshot,
    AgentMessage,
    AgentResult,
    AgentRole,
    AgentState,
    MessageType,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Agent Configuration
# ---------------------------------------------------------------------------

@dataclass
class AgentConfig:
    """Per-agent tunables."""
    heartbeat_interval: float = 30.0        # seconds between heartbeats
    max_consecutive_failures: int = 5       # before entering DEGRADED
    task_timeout: float = 600.0             # seconds per task
    degraded_cooldown: float = 120.0        # seconds before retry after DEGRADED
    repo_root: Path = field(default_factory=lambda: Path("."))

    # Service URLs (agent-specific; usually set via env)
    service_urls: Dict[str, str] = field(default_factory=dict)
    service_tokens: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# BaseAgent
# ---------------------------------------------------------------------------

class BaseAgent(ABC):
    """Abstract base for all escort-team agents.

    Lifecycle::

        agent = MyAgent(role, mailbox, config)
        await agent.start()     # enters run_loop
        ...
        await agent.shutdown()  # graceful stop
    """

    def __init__(
        self,
        role: AgentRole,
        mailbox: Mailbox,
        config: Optional[AgentConfig] = None,
    ):
        self.role = role
        self.agent_id = f"{role.value}-{uuid.uuid4().hex[:6]}"
        self.mailbox = mailbox
        self.config = config or AgentConfig()

        # State
        self._state = AgentState.IDLE
        self._start_time: float = 0.0
        self._processed_count: int = 0
        self._error_count: int = 0
        self._consecutive_failures: int = 0
        self._last_active: str = ""
        self._shutdown_requested = False
        self._running = False

        # Background tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._loop_task: Optional[asyncio.Task] = None
        # v4: Abort signal for cascade shutdown from coordinator
        self._abort_event: asyncio.Event = asyncio.Event()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def state(self) -> AgentState:
        return self._state

    @property
    def uptime(self) -> float:
        if self._start_time == 0:
            return 0.0
        return time.monotonic() - self._start_time

    @property
    def health_snapshot(self) -> AgentHealthSnapshot:
        return AgentHealthSnapshot(
            agent_id=self.agent_id,
            agent_role=self.role.value,
            state=self._state.value,
            uptime_seconds=round(self.uptime, 1),
            processed_count=self._processed_count,
            error_count=self._error_count,
            consecutive_failures=self._consecutive_failures,
            last_active=self._last_active,
            mailbox_depth=self.mailbox.depth,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the agent's run loop and heartbeat."""
        if self._running:
            return
        self._running = True
        self._start_time = time.monotonic()
        self._state = AgentState.RUNNING
        self._shutdown_requested = False

        logger.info("[%s] Starting agent (role=%s)", self.agent_id, self.role.value)

        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.agent_id}-heartbeat"
        )
        self._loop_task = asyncio.create_task(
            self._run_loop(), name=f"{self.agent_id}-loop"
        )

    async def shutdown(self, reason: str = "requested") -> None:
        """Graceful shutdown."""
        logger.info("[%s] Shutdown requested: %s", self.agent_id, reason)
        self._shutdown_requested = True
        self._state = AgentState.SHUTDOWN

        # Send ack
        await self.mailbox.send(AgentMessage(
            source=self.agent_id,
            target="coordinator",
            msg_type=MessageType.SHUTDOWN_ACK.value,
            payload={"reason": reason},
        ))

        # Cancel tasks
        for task in (self._heartbeat_task, self._loop_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._running = False

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _run_loop(self) -> None:
        """Main message-processing loop."""
        while not self._shutdown_requested and not self._abort_event.is_set():
            try:
                self._state = AgentState.WAITING

                # Wait for a message addressed to us (or broadcast)
                msg = await self.mailbox.receive(
                    predicate=lambda m: (
                        m.target == self.agent_id
                        or m.target == self.role.value
                        or m.target == "*"
                    ),
                    timeout=self.config.heartbeat_interval,
                )

                if msg is None:
                    # Check abort signal on every idle cycle
                    if self._abort_event.is_set():
                        logger.info("[%s] Abort signal received, exiting run loop", self.agent_id)
                        break
                    # Timeout — no message, loop back (heartbeat keeps us alive)
                    continue

                self._state = AgentState.RUNNING
                self._last_active = datetime.now(timezone.utc).isoformat()

                await self._dispatch_message(msg)

            except asyncio.CancelledError:
                break
            except Exception:
                self._error_count += 1
                self._consecutive_failures += 1
                logger.exception("[%s] Run loop error", self.agent_id)

                if self._consecutive_failures >= self.config.max_consecutive_failures:
                    self._state = AgentState.DEGRADED
                    logger.warning(
                        "[%s] Entering DEGRADED after %d consecutive failures",
                        self.agent_id,
                        self._consecutive_failures,
                    )
                    await self._notify_degraded()
                    await asyncio.sleep(self.config.degraded_cooldown)
                    self._consecutive_failures = 0  # reset after cooldown

        self._state = AgentState.SHUTDOWN

    async def _dispatch_message(self, msg: AgentMessage) -> None:
        """Route incoming messages to handlers."""
        mt = msg.msg_type

        if mt == MessageType.SHUTDOWN_REQUEST.value:
            await self.shutdown(reason=msg.payload.get("reason", "coordinator"))
            return

        if mt == MessageType.HEALTH_PING.value:
            await self.mailbox.send(AgentMessage(
                source=self.agent_id,
                target=msg.source,
                msg_type=MessageType.HEALTH_PONG.value,
                payload=self.health_snapshot.to_dict(),
            ))
            return

        if mt == MessageType.MODE_SWITCH.value:
            new_mode = msg.payload.get("mode", "")
            logger.info("[%s] Mode switch → %s", self.agent_id, new_mode)
            await self.on_mode_switch(new_mode)
            return

        if mt == MessageType.TASK_DISPATCH.value:
            await self._handle_task_safe(msg)
            return

        # Subclass may handle additional message types
        await self.on_message(msg)

    async def _handle_task_safe(self, msg: AgentMessage) -> None:
        """Execute ``handle_task`` with timeout, error handling, and result reporting."""
        round_id = msg.payload.get("round_id", "")
        start = time.monotonic()

        # v4: Reset abort event for new task
        self._abort_event.clear()

        try:
            result = await asyncio.wait_for(
                self.handle_task(msg.payload),
                timeout=self.config.task_timeout,
            )
            elapsed = time.monotonic() - start
            self._processed_count += 1
            self._consecutive_failures = 0  # reset on success

            # Wrap and send result
            agent_result = AgentResult(
                agent_id=self.agent_id,
                agent_role=self.role.value,
                status=AgentState.COMPLETED.value,
                round_id=round_id,
                findings=result.get("findings", []) if isinstance(result, dict) else [],
                artifacts=result if isinstance(result, dict) else {"raw": result},
                duration_seconds=round(elapsed, 2),
            )
            await self.mailbox.send(AgentMessage(
                source=self.agent_id,
                target=msg.source or "coordinator",
                msg_type=MessageType.TASK_RESULT.value,
                payload=agent_result.to_dict(),
            ))

        except asyncio.TimeoutError:
            elapsed = time.monotonic() - start
            self._error_count += 1
            self._consecutive_failures += 1
            logger.error(
                "[%s] Task timed out after %.1fs (round=%s)",
                self.agent_id, elapsed, round_id,
            )
            await self._send_error_result(msg.source, round_id, "task_timeout", elapsed)

        except Exception as exc:
            elapsed = time.monotonic() - start
            self._error_count += 1
            self._consecutive_failures += 1
            logger.exception("[%s] Task failed (round=%s)", self.agent_id, round_id)
            await self._send_error_result(
                msg.source, round_id, str(exc), elapsed
            )

    async def _send_error_result(
        self, target: str, round_id: str, error: str, elapsed: float
    ) -> None:
        agent_result = AgentResult(
            agent_id=self.agent_id,
            agent_role=self.role.value,
            status=AgentState.FAILED.value,
            round_id=round_id,
            errors=[error],
            duration_seconds=round(elapsed, 2),
        )
        await self.mailbox.send(AgentMessage(
            source=self.agent_id,
            target=target or "coordinator",
            msg_type=MessageType.TASK_RESULT.value,
            payload=agent_result.to_dict(),
        ))

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        while not self._shutdown_requested:
            try:
                await self.mailbox.send(AgentMessage(
                    source=self.agent_id,
                    target="coordinator",
                    msg_type=MessageType.HEARTBEAT.value,
                    payload=self.health_snapshot.to_dict(),
                ))
            except Exception:
                logger.debug("[%s] Heartbeat send error", self.agent_id)
            await asyncio.sleep(self.config.heartbeat_interval)

    # ------------------------------------------------------------------
    # Degraded notification
    # ------------------------------------------------------------------

    async def _notify_degraded(self) -> None:
        await self.mailbox.send(AgentMessage(
            source=self.agent_id,
            target="coordinator",
            msg_type=MessageType.ESCALATION.value,
            payload={
                "level": "degraded",
                "agent_id": self.agent_id,
                "consecutive_failures": self._consecutive_failures,
                "snapshot": self.health_snapshot.to_dict(),
            },
        ))

    # ------------------------------------------------------------------
    # Abstract / overridable
    # ------------------------------------------------------------------

    @abstractmethod
    async def handle_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Execute domain-specific logic for a TASK_DISPATCH.

        Args:
            payload: The ``AgentMessage.payload`` dict containing task parameters.

        Returns:
            A dict with at least ``findings`` (list) and any domain artifacts.
        """
        ...

    async def on_mode_switch(self, new_mode: str) -> None:
        """Called when Coordinator broadcasts a mode change.  Override if needed."""
        pass

    async def on_message(self, msg: AgentMessage) -> None:
        """Called for unrecognised message types.  Override to extend."""
        logger.debug(
            "[%s] Unhandled message type: %s from %s",
            self.agent_id, msg.msg_type, msg.source,
        )
