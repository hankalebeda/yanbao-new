from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.services import ssot_read_model as shared
from app.services.runtime_anchor_service import RuntimeAnchorService


def get_dashboard_stats_payload_ssot(
    db: Session,
    *,
    window_days: int = 30,
    runtime_anchor_service: RuntimeAnchorService | None = None,
) -> dict[str, Any]:
    return shared.get_dashboard_stats_payload_ssot(
        db,
        window_days=window_days,
        runtime_anchor_service=runtime_anchor_service,
    )


def get_public_performance_payload_ssot(
    db: Session,
    *,
    window_days: int = 30,
    runtime_anchor_service: RuntimeAnchorService | None = None,
) -> dict[str, Any]:
    return shared.get_public_performance_payload_ssot(
        db,
        window_days=window_days,
        runtime_anchor_service=runtime_anchor_service,
    )
