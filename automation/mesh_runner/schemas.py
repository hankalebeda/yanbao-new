from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field
from typing import Any


class StartRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str | None = Field(default=None, pattern=r"^issue-mesh-\d{8}-\d{3}$")
    run_label: str | None = None
    benchmark_label: str | None = None
    max_workers: int | None = Field(default=None, ge=1)
    # Readonly runs must provide an explicit provider allowlist when lane isolation
    # is enabled. We keep this optional here and enforce in runner to avoid breaking
    # older callers that don't use lanes.
    provider_allowlist: list[str] | None = Field(default=None)
    audit_scope: str = "current-layer"
    shard_strategy: str = "family-view-ssot"
    control_state_snapshot: str | None = None
    audit_context: dict[str, Any] = Field(default_factory=dict)
    wait_for_completion: bool = False
    wait_timeout_seconds: int = Field(default=0, ge=0)


class RunAcceptedResponse(BaseModel):
    run_id: str
    status: str
    manifest_path: str
    summary_path: str | None = None
    bundle_path: str | None = None
    output_dir: str
    summary_markdown: str | None = None
