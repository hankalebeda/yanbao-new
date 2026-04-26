"""routes_dashboard.py - Home, dashboard, pool, sim-dashboard API routes."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.core.response import envelope
from app.core.security import get_current_user_optional

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["dashboard"])


def _has_paid_sim_access(user) -> bool:
    role = str(getattr(user, "role", "") or "").strip().lower()
    if role in {"admin", "super_admin"}:
        return True

    tier = str(getattr(user, "tier", "Free") or "Free")
    if tier not in {"Pro", "Enterprise"}:
        return False

    tier_expires_at = getattr(user, "tier_expires_at", None)
    if not tier_expires_at:
        return False

    now = datetime.now(timezone.utc)
    expiry = tier_expires_at
    if getattr(expiry, "tzinfo", None) is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    return expiry > now


@router.get("/home")
async def home_api(request: Request, db: Session = Depends(get_db)):
    from app.api import routes_business

    user = await get_current_user_optional(request)
    viewer_tier = getattr(user, "tier", None) or getattr(user, "membership_level", None) or "Free"
    viewer_role = getattr(user, "role", None) or "user"
    payload = routes_business.get_home_payload_cached(
        db,
        viewer_tier=viewer_tier,
        viewer_role=viewer_role,
    )
    return envelope(data=payload)


@router.get("/dashboard/stats")
async def dashboard_stats(
    window_days: int = Query(30),
    db: Session = Depends(get_db),
):
    from app.services.dashboard_query import get_dashboard_stats_payload_ssot

    if window_days not in {1, 7, 14, 30, 60, 90}:
        raise HTTPException(status_code=422, detail="INVALID_PAYLOAD")

    payload = get_dashboard_stats_payload_ssot(db, window_days=window_days)
    return envelope(data=payload)


@router.get("/portfolio/sim-dashboard")
async def sim_dashboard_api(
    request: Request,
    capital_tier: str = Query("100k"),
    db: Session = Depends(get_db),
):
    from app.services.sim_query import get_sim_dashboard_payload_ssot

    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    if not _has_paid_sim_access(user):
        raise HTTPException(status_code=403, detail="TIER_NOT_AVAILABLE")
    payload = get_sim_dashboard_payload_ssot(db, capital_tier=capital_tier)
    return envelope(data=payload)


@router.get("/pool/stocks")
async def pool_stocks(
    trade_date: str | None = Query(None),
    db: Session = Depends(get_db),
):
    from app.services.ssot_read_model import get_public_pool_snapshot_ssot

    if trade_date:
        task_row = db.execute(
            text(
                """
                SELECT task_id, trade_date
                FROM stock_pool_refresh_task
                WHERE trade_date = :trade_date
                  AND status IN ('COMPLETED', 'FALLBACK')
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"trade_date": trade_date},
        ).mappings().first()
        if task_row is None:
            return envelope(data={"trade_date": trade_date, "total": 0, "items": []})
        rows = db.execute(
            text(
                """
                SELECT s.stock_code, sm.stock_name
                FROM stock_pool_snapshot s
                LEFT JOIN stock_master sm ON sm.stock_code = s.stock_code
                WHERE s.refresh_task_id = :task_id
                  AND s.pool_role = 'core'
                ORDER BY s.rank_no ASC, s.stock_code ASC
                """
            ),
            {"task_id": task_row["task_id"]},
        ).mappings().all()
        items = [
            {
                "stock_code": str(row.get("stock_code") or ""),
                "stock_name": row.get("stock_name") or row.get("stock_code"),
            }
            for row in rows
            if row.get("stock_code")
        ]
        raw_trade_date = task_row.get("trade_date")
        td = raw_trade_date.isoformat() if hasattr(raw_trade_date, "isoformat") else trade_date
        return envelope(data={"trade_date": td, "total": len(items), "items": items})

    snapshot = get_public_pool_snapshot_ssot(db)
    pool_view = snapshot.get("pool_view")
    if pool_view is None:
        return envelope(data={"trade_date": trade_date, "total": 0, "items": []})

    rows = sorted(pool_view.core_rows, key=lambda row: ((row.rank_no or 0), row.stock_code))
    stock_codes = [row.stock_code for row in rows if getattr(row, "stock_code", None)]
    name_map: dict[str, str | None] = {}
    if stock_codes:
        bind_params = {f"stock_code_{idx}": code for idx, code in enumerate(stock_codes)}
        placeholders = ", ".join(f":stock_code_{idx}" for idx in range(len(stock_codes)))
        name_rows = db.execute(
            text(
                f"""
                SELECT stock_code, stock_name
                FROM stock_master
                WHERE stock_code IN ({placeholders})
                """
            ),
            bind_params,
        ).mappings().all()
        name_map = {str(row.get("stock_code") or ""): row.get("stock_name") for row in name_rows}

    items = [
        {
            "stock_code": row.stock_code,
            "stock_name": name_map.get(row.stock_code) or row.stock_code,
        }
        for row in rows
    ]
    td = pool_view.task.trade_date.isoformat() if hasattr(pool_view, "task") and pool_view.task else trade_date
    return envelope(data={"trade_date": td, "total": len(items), "items": items})
