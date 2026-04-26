"""Multi-Agent protocol layer.

Defines all message types, agent states, and result structures used for
inter-agent communication in the autonomous escort team.

Inspired by claude-code-sourcemap Coordinator/Worker patterns:
- Structured control messages (shutdown, plan_approval, task_notification)
- MessageSource tagging
- TaskState union types

All dataclasses are JSON-serialisable via ``asdict()`` so they can be
persisted to the Mailbox backing store and transmitted over HTTP.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Agent Identity
# ---------------------------------------------------------------------------

class AgentRole(str, Enum):
    """Well-known agent roles in the escort team."""
    COORDINATOR = "coordinator"
    DISCOVERY = "discovery"
    ANALYSIS = "analysis"
    FIX = "fix"
    VERIFY = "verify"
    WRITEBACK = "writeback"
    PROMOTE = "promote"


# ---------------------------------------------------------------------------
# Agent Lifecycle States
# ---------------------------------------------------------------------------

class AgentState(str, Enum):
    """Lifecycle states for any agent."""
    IDLE = "idle"
    RUNNING = "running"
    WAITING = "waiting"          # blocked on mailbox receive
    COMPLETED = "completed"      # round finished, awaiting next dispatch
    FAILED = "failed"            # unrecoverable error
    DEGRADED = "degraded"        # partial capability loss
    SHUTDOWN = "shutdown"        # graceful shutdown requested / done


# ---------------------------------------------------------------------------
# Message Types
# ---------------------------------------------------------------------------

class MessageType(str, Enum):
    """Structured message types exchanged via Mailbox."""
    # --- task lifecycle ---
    TASK_DISPATCH = "task_dispatch"        # Coordinator → Worker
    TASK_RESULT = "task_result"            # Worker → Coordinator
    TASK_PROGRESS = "task_progress"        # Worker → Coordinator (interim)

    # --- control ---
    SHUTDOWN_REQUEST = "shutdown_request"
    SHUTDOWN_ACK = "shutdown_ack"
    MODE_SWITCH = "mode_switch"           # Coordinator → all (FIX/MONITOR/SAFE_HOLD)
    ESCALATION = "escalation"             # Worker → Coordinator (needs human)

    # --- health ---
    HEALTH_PING = "health_ping"           # Coordinator → Worker
    HEALTH_PONG = "health_pong"           # Worker → Coordinator
    HEARTBEAT = "heartbeat"               # Worker → Coordinator (periodic)

    # --- promote ---
    PROMOTE_DECISION = "promote_decision"
    PROMOTE_ROLLBACK = "promote_rollback"

    # --- generic ---
    INFO = "info"


# ---------------------------------------------------------------------------
# Core Message Envelope
# ---------------------------------------------------------------------------

@dataclass
class AgentMessage:
    """Envelope for all inter-agent communication.

    Follows the claude-code-sourcemap ``Message`` structure with added
    ``msg_type`` for structured dispatch.
    """
    msg_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    source: str = ""               # sender agent_id
    target: str = ""               # receiver agent_id or "*" for broadcast
    msg_type: str = MessageType.INFO.value
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AgentMessage":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# Problem / Finding / Patch primitives (thin wrappers around loop_controller)
# ---------------------------------------------------------------------------

class Severity(str, Enum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"


class TriageDecision(str, Enum):
    AUTO_FIX = "auto_fix"          # confidence >= 0.7
    NEEDS_REVIEW = "needs_review"  # 0.4 - 0.7
    DEFER = "defer"                # < 0.4


class ProblemStatus(str, Enum):
    """Lifecycle status for discovered problems."""
    ACTIVE = "active"
    BLOCKED = "blocked"
    REVIEW_REQUIRED = "review_required"
    COMPLETED = "completed"


class HandlingPath(str, Enum):
    """Expected handling path for a discovered problem."""
    FIX_CODE = "fix_code"
    FIX_THEN_REBUILD = "fix_then_rebuild"
    FIX_THEN_REPLAY = "fix_then_replay"
    EXECUTION_AND_MONITORING = "execution_and_monitoring"
    EXTERNAL_DEPENDENCY = "external_dependency"
    MANUAL_VERIFY = "manual_verify"
    FREEZE_OR_ISOLATE = "freeze_or_isolate"


@dataclass
class ProblemSpec:
    """A discovered problem, emitted by DiscoveryAgent."""
    problem_id: str = ""
    source_probe: str = ""         # which probe detected it
    severity: str = Severity.P2.value
    family: str = ""               # mesh runner family id
    task_family: str = ""          # execution family used by coordinator lanes
    lane_id: str = ""              # shard-safe execution lane id
    title: str = ""
    description: str = ""
    affected_files: List[str] = field(default_factory=list)
    affected_frs: List[str] = field(default_factory=list)
    suggested_approach: str = HandlingPath.FIX_CODE.value
    current_status: str = ProblemStatus.ACTIVE.value
    blocker_type: str = ""
    blocked_reason: str = ""
    recommended_angles: List[int] = field(default_factory=list)
    analysis_angles: List[int] = field(default_factory=list)  # doc25 angles matched
    write_scope: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ProblemSpec":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class AnalysisResult:
    """Analysis output for a single problem, emitted by AnalysisAgent."""
    problem_id: str = ""
    root_cause: str = ""
    fix_strategy: str = ""
    risk_level: str = Severity.P2.value
    confidence: float = 0.0
    triage: str = TriageDecision.DEFER.value
    source: str = ""               # "ai", "heuristic", or "codex_cli" — distinguishes real AI from fallback
    fix_description: str = ""      # human-readable fix plan returned by AI analysis
    task_family: str = ""
    lane_id: str = ""
    current_status: str = ProblemStatus.ACTIVE.value
    blocker_type: str = ""
    blocked_reason: str = ""
    analysis_angles: List[int] = field(default_factory=list)  # doc25 angles used in analysis
    write_scope: List[str] = field(default_factory=list)
    provider_votes: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AnalysisResult":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class PatchSet:
    """A set of file patches produced by FixAgent."""
    problem_id: str = ""
    patches: List[Dict[str, str]] = field(default_factory=list)  # [{path, before_sha, patch_text}]
    fix_strategy_used: str = ""
    task_family: str = ""
    lane_id: str = ""
    write_scope: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PatchSet":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class VerifyResult:
    """Verification outcome, emitted by VerifyAgent."""
    round_id: str = ""
    problem_id: str = ""
    scoped_pytest_passed: bool = False
    full_regression_passed: bool = False
    blind_spot_clean: bool = False
    catalog_fresh: bool = False
    artifacts_aligned: bool = False
    security_clean: bool = True
    all_passed: bool = False
    failed_gates: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "VerifyResult":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class WritebackReceipt:
    """Confirmation of a successful writeback, emitted by WritebackAgent."""
    round_id: str = ""
    problem_id: str = ""
    commit_sha: str = ""
    affected_files: List[str] = field(default_factory=list)
    audit_trail_path: str = ""
    lease_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "WritebackReceipt":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class PromoteDecision:
    """Promotion decision, emitted by PromoteAgent."""
    round_id: str = ""
    tier: int = 0                # 1=auto, 2=delayed, 3=protected
    approved: bool = False
    reason: str = ""
    targets_promoted: List[str] = field(default_factory=list)
    rollback_triggered: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PromoteDecision":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# Agent Result (generic wrapper)
# ---------------------------------------------------------------------------

@dataclass
class AgentResult:
    """Generic result envelope returned by any agent upon task completion."""
    agent_id: str = ""
    agent_role: str = ""
    status: str = AgentState.COMPLETED.value
    round_id: str = ""
    findings: List[Dict[str, Any]] = field(default_factory=list)
    artifacts: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AgentResult":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# Agent Health Snapshot
# ---------------------------------------------------------------------------

@dataclass
class AgentHealthSnapshot:
    """Health info periodically sent as HEARTBEAT payload."""
    agent_id: str = ""
    agent_role: str = ""
    state: str = AgentState.IDLE.value
    uptime_seconds: float = 0.0
    processed_count: int = 0
    error_count: int = 0
    consecutive_failures: int = 0
    last_active: str = ""
    mailbox_depth: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AgentHealthSnapshot":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# v4: Round Progress (live tracking for external consumers)
# ---------------------------------------------------------------------------

@dataclass
class RoundProgress:
    """Tracks progress within a single orchestration round.

    Exposed via /v1/round-progress for Kestra / external monitoring.
    """
    round_id: str = ""
    phase: str = ""
    started_at: str = ""
    phase_started_at: str = ""
    problem_count: int = 0
    patch_count: int = 0
    verified: Optional[bool] = None
    promoted: Optional[bool] = None
    ai_pressure: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Coordinator Round State
# ---------------------------------------------------------------------------

class CoordinatorPhase(str, Enum):
    """High-level phases the Coordinator cycles through."""
    BOOTSTRAP = "bootstrap"
    DISCOVERY = "discovery"
    ANALYSIS = "analysis"
    FIXING = "fixing"
    VERIFICATION = "verification"
    WRITEBACK = "writeback"
    PROMOTION = "promotion"
    MONITORING = "monitoring"
    SAFE_HOLD = "safe_hold"


class CoordinatorMode(str, Enum):
    """Operating mode mirrors LoopMode."""
    FIX = "fix"
    MONITOR = "monitor"
    SAFE_HOLD = "safe_hold"
    COMPLETED = "completed"  # v2: all-probes-clean + green rounds achieved


@dataclass
class CoordinatorState:
    """Persistent state for the CoordinatorAgent."""
    mode: str = CoordinatorMode.FIX.value
    phase: str = CoordinatorPhase.BOOTSTRAP.value
    current_round_id: str = ""
    consecutive_green_rounds: int = 0
    consecutive_fix_failures: int = 0
    consecutive_no_progress_rounds: int = 0
    total_rounds: int = 0
    total_fixes: int = 0
    total_failures: int = 0
    agents_registered: List[str] = field(default_factory=list)
    agents_healthy: List[str] = field(default_factory=list)
    last_discovery_time: str = ""
    last_promote_time: str = ""
    last_promote_round_id: str = ""
    last_audit_run_id: str = ""
    last_shadow_validation: Dict[str, Any] = field(default_factory=dict)
    last_formal_promote: Dict[str, Any] = field(default_factory=dict)
    deferred_problems: List[str] = field(default_factory=list)  # v2: problems past max retries
    agent_restart_counts: Dict[str, int] = field(default_factory=dict)  # v2: per-agent restart tracking
    completion_time: str = ""  # v2: when COMPLETED mode was entered
    safe_hold_count: int = 0  # v3: consecutive SAFE_HOLD entries (for exponential backoff)
    last_safe_hold_problem_count: int = 0  # v3: problem count at last SAFE_HOLD entry
    execution_lanes: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    blocked_problems: List[Dict[str, Any]] = field(default_factory=list)
    round_history: List[Dict[str, Any]] = field(default_factory=list)
    promote_target_mode: str = "infra"  # v7 A4: "infra" | "doc22"; auto-advanced by coordinator

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CoordinatorState":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})

    @property
    def autonomy_index(self) -> float:
        """Real-time autonomy index (0.0 - 1.0).

        v7: In MONITOR/COMPLETED mode, when the system has sustained >=5
        consecutive green rounds with zero fix failures, agent heartbeats have
        likely expired between rounds (1800s interval > 180s window).  Idleness
        in this stable state is *not* the same as unhealthy, so we credit full
        health rather than collapsing the 30%-weight component to 0.

        v9: Credit full health when stable_monitor is True regardless of
        agents_healthy count.  Stale registrations from prior sessions inflate
        agents_registered while only current-session agents send heartbeats,
        artificially deflating the ratio below the 0.85 completion threshold.
        """
        green = min(self.consecutive_green_rounds, 5) / 5
        stable_monitor = (
            self.mode in ("monitor", "completed")
            and self.consecutive_green_rounds >= 5
            and self.consecutive_fix_failures == 0
        )
        if stable_monitor:
            health = 1.0
        else:
            health = len(self.agents_healthy) / max(len(self.agents_registered), 1)
        fail_penalty = min(self.consecutive_fix_failures, 5) / 5
        return round((green * 0.5 + health * 0.3 + (1 - fail_penalty) * 0.2), 3)
