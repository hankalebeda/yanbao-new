"""Data models for the Loop Controller service."""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class LoopPhase(str, Enum):
    IDLE = "idle"
    AUDITING = "auditing"
    ANALYZING = "analyzing"
    PLANNING = "planning"
    FIXING = "fixing"
    VERIFYING = "verifying"
    WRITING_BACK = "writing_back"
    PROMOTING = "promoting"
    MONITORING = "monitoring"
    BLOCKED = "blocked"


class LoopMode(str, Enum):
    FIX = "fix"
    MONITOR = "monitor"
    SAFE_HOLD = "safe_hold"


class Severity(str, Enum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"


class FixOutcome(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    EXTERNAL_BLOCKED = "external_blocked"
    ROLLED_BACK = "rolled_back"


# ---------------------------------------------------------------------------
# Problem / Finding models
# ---------------------------------------------------------------------------

class ProblemSpec(BaseModel):
    """A single problem extracted from audit findings."""
    model_config = ConfigDict(extra="forbid")

    problem_id: str
    severity: Severity
    category: str = ""
    family: str = ""
    title: str = ""
    description: str = ""
    diagnosis_angles: list[int] = Field(default_factory=list)
    affected_files: list[str] = Field(default_factory=list)
    affected_frs: list[str] = Field(default_factory=list)
    suggested_fix_approach: str = ""
    write_scope: list[str] = Field(default_factory=list)
    is_regression: bool = False
    is_external_blocked: bool = False


class FixResult(BaseModel):
    """Result of fixing a single problem."""
    model_config = ConfigDict(extra="forbid")

    problem_id: str
    outcome: FixOutcome
    fix_run_id: str | None = None
    patches_applied: list[str] = Field(default_factory=list)
    patches_raw: list[dict[str, Any]] = Field(default_factory=list)
    error: str | None = None
    duration_seconds: float = 0.0


class VerifyResult(BaseModel):
    """Result of the verification pipeline for a round."""
    model_config = ConfigDict(extra="forbid")

    scoped_pytest_passed: bool = False
    full_pytest_passed: bool = False
    full_pytest_total: int = 0
    full_pytest_failed: int = 0
    blind_spot_clean: bool = False
    catalog_improved: bool = False
    artifacts_aligned: bool = False
    all_green: bool = False
    details: dict[str, Any] = Field(default_factory=dict)


class RoundSummary(BaseModel):
    """Summary of one loop iteration."""
    model_config = ConfigDict(extra="forbid")

    round_id: str
    started_at: str
    finished_at: str | None = None
    phase_reached: LoopPhase = LoopPhase.IDLE
    mode: LoopMode = LoopMode.FIX
    audit_run_id: str | None = None
    problems_found: int = 0
    problems_fixed: int = 0
    problems_failed: int = 0
    problems_skipped: int = 0
    verify_result: VerifyResult | None = None
    fix_results: list[FixResult] = Field(default_factory=list)
    all_success: bool = False
    error: str | None = None


# ---------------------------------------------------------------------------
# Persistent state
# ---------------------------------------------------------------------------

class LoopState(BaseModel):
    """Persistent state of the Loop Controller, serialized to JSON."""
    model_config = ConfigDict(extra="forbid")

    mode: LoopMode = LoopMode.FIX
    phase: LoopPhase = LoopPhase.IDLE
    consecutive_fix_success_count: int = 0
    consecutive_verified_problem_fixes: int = 0
    fix_goal: int = 10
    total_fixes: int = 0
    total_failures: int = 0
    current_round_id: str | None = None
    last_audit_run_id: str | None = None
    last_fix_wave_id: str | None = None
    last_promote_round_id: str | None = None
    last_promote_at: str | None = None
    problems_queue: list[ProblemSpec] = Field(default_factory=list)
    fixed_problems: list[str] = Field(default_factory=list)
    round_history: list[RoundSummary] = Field(default_factory=list)
    last_bundle_fingerprint: str | None = None
    last_artifact_fingerprints: dict[str, str] = Field(default_factory=dict)
    monitor_interval_seconds: int = 1800
    audit_interval_seconds: int = 300
    # v2 fields
    blocked_reason: str | None = None
    provider_pool: dict[str, Any] = Field(default_factory=dict)
    promote_target_mode: str = "infra"
    success_goal_metric: str = "verified_problem_count"
    goal_ever_reached: bool = False


# ---------------------------------------------------------------------------
# API request / response schemas
# ---------------------------------------------------------------------------

class StartLoopRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: LoopMode = LoopMode.FIX
    fix_goal: int = Field(default=10, ge=1, le=100)
    max_workers: int | None = None
    audit_interval_seconds: int = Field(default=300, ge=30)
    monitor_interval_seconds: int = Field(default=1800, ge=60)
    force_new_round: bool = False


class StartLoopResponse(BaseModel):
    status: str
    mode: str
    fix_goal: int
    current_round_id: str | None = None


class AwaitRoundResponse(BaseModel):
    status: str
    round_id: str | None = None
    round_summary: RoundSummary | None = None
    timed_out: bool = False


# ---------------------------------------------------------------------------
# Green Round Verdict — 9-item autonomous completeness gate
# ---------------------------------------------------------------------------

class GreenRoundVerdict(BaseModel):
    """All 9 conditions must be True for a round to be considered fully green."""
    model_config = ConfigDict(extra="forbid")

    no_new_active: bool = False
    no_regression: bool = False
    no_drift: bool = False
    verify_all_green: bool = False
    no_partial_fail: bool = False
    no_masked_skip: bool = False
    batch_writeback_complete: bool = False
    promote_gate_passed: bool = False
    """Formal gate truly passed (v2 semantics):
    runtime_gates.status='ready' AND shared_artifact_promote.allowed=true
    AND verifier green AND commits positively succeeded."""
    artifacts_aligned: bool = False

    @property
    def is_green(self) -> bool:
        return all([
            self.no_new_active,
            self.no_regression,
            self.no_drift,
            self.verify_all_green,
            self.no_partial_fail,
            self.no_masked_skip,
            self.batch_writeback_complete,
            self.promote_gate_passed,
            self.artifacts_aligned,
        ])


class StopLoopRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = "manual_stop"


class StateResponse(BaseModel):
    mode: LoopMode
    phase: LoopPhase
    consecutive_fix_success_count: int
    consecutive_verified_problem_fixes: int = 0
    fix_goal: int
    goal_progress_count: int = 0
    goal_reached: bool = False
    total_fixes: int
    total_failures: int
    current_round_id: str | None
    last_promote_round_id: str | None = None
    last_promote_at: str | None = None
    problems_queue_size: int
    fixed_count: int
    round_history_size: int
    running: bool
    # v2 fields
    blocked_reason: str | None = None
    provider_pool: dict[str, Any] = Field(default_factory=dict)
    promote_target_mode: str = "infra"
    success_goal_metric: str = "verified_problem_count"


class AnalyzeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    audit_run_id: str
    bundle: dict[str, Any] = Field(default_factory=dict)


class AnalyzeResponse(BaseModel):
    problems: list[ProblemSpec]
    new_count: int
    regression_count: int
    skipped_count: int


class VerifyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    round_id: str
    fix_results: list[FixResult] = Field(default_factory=list)
    affected_test_paths: list[str] = Field(default_factory=list)


class VerifyResponse(BaseModel):
    result: VerifyResult
    should_rollback: bool = False


class RoundCompleteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    round_id: str
    execution_id: str = ""
    status: str = "completed"
    error: str | None = None
