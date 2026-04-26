from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class LoopMode(str, Enum):
    FIX = "fix"
    MONITOR = "monitor"


class RoundRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    round_id: str
    mode: LoopMode
    started_at: str
    finished_at: str | None = None
    audit_run_id: str | None = None
    problems_found: int = 0
    new_problem_count: int = 0
    regression_count: int = 0
    skipped_count: int = 0
    drift_paths: list[str] = Field(default_factory=list)
    fixes_attempted: int = 0
    fixes_succeeded: int = 0
    verify_all_green: bool = False
    promoted: bool = False
    reentered_fix: bool = False
    note: str | None = None
    failed_reasons: list[str] = Field(default_factory=list)
    promote_reason: str | None = None
    runtime_gate_status: str | None = None
    # --- Green round verdict fields (9-item alignment) ---
    no_new_active: bool = False
    no_regression: bool = False
    no_drift: bool = False
    no_partial_fail: bool = False
    no_masked_skip: bool = False
    batch_writeback_complete: bool = False
    promote_gate_passed: bool = False
    artifacts_aligned: bool = False
    green_round: bool = False
    verified_problem_count: int = 0


class AutonomousFixState(BaseModel):
    model_config = ConfigDict(extra="ignore")

    mode: LoopMode = LoopMode.FIX
    phase: str = "idle"
    round_seq: int = 0
    success_round_streak: int = 0
    success_round_goal: int = 10
    consecutive_verified_problem_fixes: int = 0
    goal_ever_reached: bool = False
    success_goal_metric: str = "verified_problem_count"
    total_rounds: int = 0
    total_fixes: int = 0
    total_failures: int = 0
    fixed_problem_ids: list[str] = Field(default_factory=list)
    last_audit_run_id: str | None = None
    last_bundle_fingerprint: str | None = None
    last_artifact_fingerprints: dict[str, str] = Field(default_factory=dict)
    round_history: list[RoundRecord] = Field(default_factory=list)
    last_updated_at: str | None = None


def bundle_fingerprint(bundle: dict[str, Any]) -> str:
    raw = json.dumps(bundle, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class AutonomousFixStateStore:
    def __init__(
        self,
        path: Path,
        *,
        max_round_history: int = 400,
    ) -> None:
        self._path = path
        self._max_round_history = max_round_history
        self._cache: AutonomousFixState | None = None

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> AutonomousFixState:
        if not self._path.exists():
            state = AutonomousFixState(last_updated_at=_utc_now_iso())
            self.save(state)
            return self.get()
        payload = json.loads(self._path.read_text(encoding="utf-8"))
        state = AutonomousFixState.model_validate(payload)
        self._cache = state
        return self.get()

    def get(self) -> AutonomousFixState:
        if self._cache is None:
            return self.load()
        return AutonomousFixState.model_validate(self._cache.model_dump())

    def save(self, state: AutonomousFixState) -> AutonomousFixState:
        data = state.model_dump()
        data["last_updated_at"] = _utc_now_iso()
        data["round_history"] = data.get("round_history", [])[-self._max_round_history :]
        serialized = json.dumps(data, ensure_ascii=False, indent=2)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        tmp_path.write_text(serialized, encoding="utf-8")
        tmp_path.replace(self._path)
        self._cache = AutonomousFixState.model_validate(data)
        return self.get()

    def update(self, **changes: Any) -> AutonomousFixState:
        state = self.get()
        updated = state.model_copy(update=changes)
        return self.save(updated)

