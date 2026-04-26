from __future__ import annotations

from sqlalchemy.orm import Session

from app.services import ssot_read_model as shared
from app.services.runtime_anchor_service import RuntimeAnchorService
from app.services.stock_pool import get_exact_pool_view, get_public_pool_view


def get_home_payload_ssot(
    db: Session,
    *,
    viewer_tier: str | None = None,
    viewer_role: str | None = None,
    runtime_anchor_service: RuntimeAnchorService | None = None,
) -> dict:
    from app.services.reports_query import list_report_summaries_ssot

    service = runtime_anchor_service or RuntimeAnchorService(db)
    public_performance = service.merged_public_runtime_payload(window_days=30)
    public_runtime = service.public_runtime_status()
    market_row = service.runtime_market_state_row()
    pool_snapshot = service.public_pool_snapshot()
    pool_view = pool_snapshot["pool_view"]
    public_status_reason = public_performance.get("status_reason")
    public_display_reason = (
        public_performance.get("display_hint")
        or shared.humanize_status_reason(public_status_reason)
        or None
    )
    if pool_view is None:
        return {
            "latest_reports": [],
            "hot_stocks": [],
            "market_state": (market_row or {}).get("market_state") or "NEUTRAL",
            "pool_size": 0,
            "data_status": public_performance.get("data_status") or "COMPUTING",
            "status_reason": public_status_reason,
            "display_reason": public_display_reason,
            "trade_date": None,
            "today_report_count": 0,
            "public_performance": public_performance,
        }

    reference_trade_date = (
        service.home_reference_trade_date(
            public_runtime=public_runtime,
            market_row=market_row,
        )
        or pool_view.task.trade_date.isoformat()
    )
    reports_payload = list_report_summaries_ssot(
        db,
        viewer_tier=viewer_tier,
        viewer_role=viewer_role,
        ignore_viewer_cutoff=True,
        trade_date=reference_trade_date,
        limit=5,
        sort="-trade_date",
    )
    # fallback: if reference_trade_date has no published ok reports, use latest across any date
    if not reports_payload.get("items"):
        reports_payload = list_report_summaries_ssot(
            db,
            viewer_tier=viewer_tier,
            viewer_role=viewer_role,
            ignore_viewer_cutoff=True,
            trade_date=None,
            limit=5,
            sort="-trade_date",
        )
    hot_stocks_payload = get_public_hot_stocks_payload_ssot(
        db,
        limit=6,
        trade_date=reference_trade_date,
        pool_view=pool_view,
    )
    today_report_count = shared._scalar(
        db,
        """
        SELECT COUNT(*) FROM report
        WHERE published = 1 AND is_deleted = 0
                    AND COALESCE(LOWER(quality_flag), 'ok') = 'ok'
          AND trade_date = :td
        """,
        {"td": reference_trade_date},
    ) if reference_trade_date else 0

    return {
        "latest_reports": reports_payload["items"],
        "hot_stocks": hot_stocks_payload["items"],
        "market_state": (market_row or {}).get("market_state") or "NEUTRAL",
        "trade_date": reference_trade_date,
        "pool_size": int(pool_snapshot["pool_size"] or 0),
        "data_status": public_performance.get("data_status") or "COMPUTING",
        "status_reason": public_status_reason,
        "display_reason": public_display_reason,
        "today_report_count": today_report_count or 0,
        "public_performance": public_performance,
    }


def get_public_hot_stocks_payload_ssot(
    db: Session,
    *,
    limit: int = 50,
    trade_date: str | None = None,
    pool_view=None,
) -> dict:
    hot_rows = shared._execute_mappings(
        db,
        """
        SELECT
            lk.stock_code,
            h.topic_title,
            h.merged_rank,
            sm.stock_name
        FROM market_hotspot_item h
        JOIN market_hotspot_item_stock_link lk ON lk.hotspot_item_id = h.hotspot_item_id
        LEFT JOIN stock_master sm ON sm.stock_code = lk.stock_code
        WHERE lk.stock_code IS NOT NULL
          AND lk.stock_code != ''
          AND h.fetch_time > datetime('now', '-24 hours')
        ORDER BY h.merged_rank ASC, lk.stock_code ASC
        """,
    ).all()
    if hot_rows:
        items = []
        seen_codes = set()
        for row in hot_rows:
            stock_code = str(row.get("stock_code") or "").strip()
            if not stock_code or stock_code in seen_codes:
                continue
            seen_codes.add(stock_code)
            merged_rank = shared._to_int(row.get("merged_rank"))
            items.append(
                {
                    "stock_code": stock_code,
                    "stock_name": row.get("stock_name") or stock_code,
                    "topic_title": row.get("topic_title"),
                    "heat_score": int(100 - merged_rank) if merged_rank else 50,
                    "rank": merged_rank or (len(items) + 1),
                    "source_name": "hotspot",
                }
            )
            if len(items) >= limit:
                break
        return {"items": items, "source": "hotspot"}

    resolved_pool_view = pool_view
    if resolved_pool_view is None:
        resolved_pool_view = get_exact_pool_view(db, trade_date=trade_date) if trade_date else get_public_pool_view(db)
    if resolved_pool_view is None:
        return {"items": [], "source": "pool_fallback"}

    rows = sorted(resolved_pool_view.core_rows, key=lambda row: ((row.rank_no or 0), row.stock_code))[:limit]
    stock_codes = [row.stock_code for row in rows if getattr(row, "stock_code", None)]
    if not stock_codes:
        return {"items": [], "source": "pool_fallback"}

    bind_params = {f"stock_code_{idx}": code for idx, code in enumerate(stock_codes)}
    placeholders = ", ".join(f":stock_code_{idx}" for idx in range(len(stock_codes)))
    name_rows = shared._execute_mappings(
        db,
        f"""
        SELECT stock_code, stock_name
        FROM stock_master
        WHERE stock_code IN ({placeholders})
        """,
        bind_params,
    ).all()
    name_map = {str(row.get("stock_code") or ""): row.get("stock_name") for row in name_rows}
    items = [
        {
            "stock_code": row.stock_code,
            "stock_name": name_map.get(row.stock_code) or row.stock_code,
            "topic_title": None,
            "heat_score": int(100 - row.rank_no) if getattr(row, 'rank_no', None) else 50,
        }
        for row in rows
    ]
    return {"items": items, "source": "pool_fallback"}


def get_latest_complete_public_pool_view_ssot(db: Session):
    service = RuntimeAnchorService(db)
    public_anchor_trade_date = service.latest_complete_public_batch_trade_date()
    if not public_anchor_trade_date:
        return None
    return get_exact_pool_view(db, trade_date=public_anchor_trade_date)
