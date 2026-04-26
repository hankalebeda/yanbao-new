"""Multi-Agent Autonomous Escort Team.

This package provides the 7-agent architecture for fully autonomous
"self-discover → self-analyse → self-fix → self-verify → self-writeback
→ self-promote" operations, inspired by the claude-code-sourcemap
Coordinator/Worker multi-agent coordination pattern.

Quick start::

    from automation.agents import create_team, Mailbox

    mailbox = Mailbox("escort-team")
    team = create_team(mailbox, repo_root=Path("."))
    await team.start()
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

from .base_agent import AgentConfig, BaseAgent
from .claim_registry import ClaimConflictError, ClaimRegistry
from .coordinator import CoordinatorAgent
from .discovery import DiscoveryAgent
from .analysis import AnalysisAgent
from .fix import FixAgent
from .verify import VerifyAgent
from .writeback import WritebackAgent
from .promote import PromoteAgent
from .mailbox import Mailbox
from .persistent_retry import (
    PermanentError,
    RetryCategory,
    RetryStats,
    RetryableError,
    with_retry,
)
from .pipeline import PipelineController, PipelinePhase
from .worker_pool import WorkerPool, WorkerResult, PoolStats
from .protocol import (
    AgentHealthSnapshot,
    AgentMessage,
    AgentResult,
    AgentRole,
    AgentState,
    AnalysisResult,
    CoordinatorMode,
    CoordinatorPhase,
    CoordinatorState,
    MessageType,
    PatchSet,
    ProblemSpec,
    PromoteDecision,
    Severity,
    TriageDecision,
    VerifyResult,
    WritebackReceipt,
)

from .residency import ResidencyController, ResidencyState
from .state_machine import (
    ControlState,
    InvalidTransitionError,
    RoundPhase,
    RoundState,
    RoundStateManager,
    SystemControlState,
)
from .team_memory import TeamMemory, MemoryEntry

__all__ = [
    # Agents
    "CoordinatorAgent",
    "DiscoveryAgent",
    "AnalysisAgent",
    "FixAgent",
    "VerifyAgent",
    "WritebackAgent",
    "PromoteAgent",
    # Infrastructure
    "Mailbox",
    "BaseAgent",
    "AgentConfig",
    # v8: New autonomous modules
    "ClaimRegistry",
    "ClaimConflictError",
    "TeamMemory",
    "MemoryEntry",
    "RoundStateManager",
    "RoundState",
    "RoundPhase",
    "SystemControlState",
    "ControlState",
    "InvalidTransitionError",
    "ResidencyController",
    "ResidencyState",
    "RetryCategory",
    "RetryStats",
    "RetryableError",
    "PermanentError",
    "with_retry",
    # Protocol
    "AgentMessage",
    "AgentResult",
    "AgentRole",
    "AgentState",
    "AgentHealthSnapshot",
    "MessageType",
    "CoordinatorMode",
    "CoordinatorPhase",
    "CoordinatorState",
    "ProblemSpec",
    "AnalysisResult",
    "PatchSet",
    "VerifyResult",
    "WritebackReceipt",
    "PromoteDecision",
    "Severity",
    "TriageDecision",
    # v10: Worker pool & pipeline
    "WorkerPool",
    "WorkerResult",
    "PoolStats",
    "PipelineController",
    "PipelinePhase",
    # Convenience
    "create_team",
    "EscortTeam",
]


class EscortTeam:
    """Manages the full 7-agent escort team lifecycle."""

    def __init__(
        self,
        mailbox: Mailbox,
        config: Optional[AgentConfig] = None,
    ):
        self.mailbox = mailbox
        self.config = config or AgentConfig()

        self.coordinator = CoordinatorAgent(
            mailbox=mailbox,
            config=self.config,
            state_path=self.config.repo_root / "runtime" / "agents" / "coordinator_state.json",
        )
        self.discovery = DiscoveryAgent(mailbox=mailbox, config=self.config)
        self.analysis = AnalysisAgent(mailbox=mailbox, config=self.config)
        self.fix = FixAgent(mailbox=mailbox, config=self.config)
        self.verify = VerifyAgent(mailbox=mailbox, config=self.config)
        self.writeback = WritebackAgent(mailbox=mailbox, config=self.config)
        self.promote = PromoteAgent(mailbox=mailbox, config=self.config)

        self._agents: List[BaseAgent] = [
            self.coordinator,
            self.discovery,
            self.analysis,
            self.fix,
            self.verify,
            self.writeback,
            self.promote,
        ]

        # v2: Register agent instances with coordinator for auto-restart
        self.coordinator.register_agent_instances({
            agent.agent_id: agent for agent in self._agents[1:]  # exclude coordinator
        })

        # v10: Initialize worker pools for Claude Code-style parallel execution
        self.coordinator.init_worker_pools(mailbox=mailbox, config=self.config)

    async def start(self) -> None:
        """Start all agents."""
        for agent in self._agents:
            await agent.start()
        # v4: Register graceful shutdown signal handlers (Windows-safe)
        self._register_signal_handlers()

    def _register_signal_handlers(self) -> None:
        """Register SIGINT/SIGTERM handlers for graceful shutdown (Windows-safe)."""
        import signal
        loop = asyncio.get_event_loop()
        for sig_name in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, sig_name, None)
            if sig is None:
                continue
            try:
                loop.add_signal_handler(sig, lambda: asyncio.ensure_future(self.shutdown("signal")))
            except NotImplementedError:
                # Windows: loop.add_signal_handler is not supported
                signal.signal(sig, lambda s, f: asyncio.ensure_future(self.shutdown("signal")))

    async def shutdown(self, reason: str = "team_shutdown") -> None:
        """Shutdown all agents gracefully."""
        await self.coordinator.shutdown(reason)
        for agent in self._agents[1:]:
            await agent.shutdown(reason)
        self.mailbox.close()

    def get_status(self) -> Dict:
        """Return status of all agents."""
        return {
            "coordinator": self.coordinator.get_status(),
            "agents": {
                agent.agent_id: agent.health_snapshot.to_dict()
                for agent in self._agents
            },
            "mailbox_depth": self.mailbox.depth,
        }

    @property
    def agents(self) -> List[BaseAgent]:
        return list(self._agents)


def create_team(
    mailbox: Optional[Mailbox] = None,
    repo_root: Optional[Path] = None,
    backing_dir: Optional[Path] = None,
    service_urls: Optional[Dict[str, str]] = None,
    service_tokens: Optional[Dict[str, str]] = None,
) -> EscortTeam:
    """Factory to create a fully configured EscortTeam.

    Args:
        mailbox: Shared mailbox.  If None, one is created with optional persistence.
        repo_root: Repository root directory.  Defaults to current directory.
        backing_dir: Directory for mailbox persistence (SQLite).
        service_urls: URLs for existing services (writeback, promote, codex, etc.)
            If None and a control-plane config exists, URLs are auto-loaded.
        service_tokens: Auth tokens for services.

    Returns:
        Ready-to-start EscortTeam.
    """
    import json as _json

    root = repo_root or Path(".")

    # Auto-load service URLs from environment or control_plane config
    if service_urls is None:
        service_urls = {}
        # 1) Prefer explicit env vars (set by start-all-services.ps1 / .env)
        _env_map = {
            "writeback_a": "WRITEBACK_A_BASE_URL",
            "writeback_b": "WRITEBACK_B_BASE_URL",
            "promote_prep": "PROMOTE_PREP_BASE_URL",
            "mesh_runner": "MESH_RUNNER_BASE_URL",
            "loop_controller": "LOOP_CONTROLLER_BASE_URL",
            "new_api": "NEW_API_BASE_URL",
            "webai": "WEBAI_BASE_URL",
        }
        import os as _os
        for key, env_name in _env_map.items():
            val = _os.environ.get(env_name, "").strip()
            if val:
                service_urls[key] = val.rstrip("/")

        # 2) Fall back to control_plane current_state.json
        config_path = root / "automation" / "control_plane" / "current_state.json"
        if config_path.exists():
            try:
                cp = _json.loads(config_path.read_text(encoding="utf-8"))
                # Extract URLs stored by loop_controller projection
                _cp_map = {
                    "writeback_a": cp.get("writeback_a_url"),
                    "writeback_b": cp.get("writeback_b_url"),
                    "promote_prep": cp.get("promote_prep_url"),
                    "mesh_runner": cp.get("mesh_runner_url"),
                    "new_api": cp.get("new_api_base_url"),
                }
                for key, val in _cp_map.items():
                    if val and key not in service_urls:
                        service_urls[key] = str(val).rstrip("/")
            except Exception:
                logger.warning("Failed to load control_plane config for service URLs")

        # 3) Final defaults for essential services
        service_urls.setdefault("writeback_a", "http://127.0.0.1:8092")
        service_urls.setdefault("writeback_b", "http://127.0.0.1:8095")
        service_urls.setdefault("promote_prep", "http://127.0.0.1:8094")
        service_urls.setdefault("mesh_runner", "http://127.0.0.1:8093")
        service_urls.setdefault("loop_controller", "http://127.0.0.1:8096")
        service_urls.setdefault("webai", "http://127.0.0.1:8000")

    # Auto-load service tokens from environment
    if service_tokens is None:
        service_tokens = {}
        _token_env_map = {
            "new_api": "NEW_API_TOKEN",
            "mesh_runner": "MESH_RUNNER_TOKEN",
        }
        import os as _os2
        for key, env_name in _token_env_map.items():
            val = _os2.environ.get(env_name, "").strip()
            if val:
                service_tokens[key] = val

    if mailbox is None:
        bp = backing_dir / "mailbox.db" if backing_dir else None
        mailbox = Mailbox(name="escort-team", backing_path=bp)

    config = AgentConfig(
        repo_root=root,
        service_urls=service_urls or {},
        service_tokens=service_tokens or {},
    )

    return EscortTeam(mailbox=mailbox, config=config)
