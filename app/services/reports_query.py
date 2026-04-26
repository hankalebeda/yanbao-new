from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.core.error_codes import normalize_error_code
from app.core.membership import has_paid_membership
from app.core.display_text import humanize_report_unavailable
from app.models import Report, StockMaster
from app.services import ssot_read_model as shared
from app.services.market_data import fetch_quote_snapshot
from app.services.membership import get_payment_capability, payment_browser_checkout_ready
from app.services.report_engine import trade_date_str

logger = logging.getLogger(__name__)

_STOCK_CODE_RE = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")
_STOCK_CODE_BARE_RE = re.compile(r"^\d{6}$")


def get_report_api_payload_ssot(
    db: Session,
    report_id: str,
    *,
    viewer_tier: str | None = None,
    viewer_role: str | None = None,
) -> dict[str, Any] | None:
    bundle = shared._load_ssot_report_bundle(db, report_id=report_id, viewer_tier=viewer_tier, viewer_role=viewer_role)
    if not bundle:
        return None
    return shared._build_report_payload(bundle, can_see_full=shared._can_view_full(viewer_tier, viewer_role), for_view=False)


def get_report_view_payload_ssot(
    db: Session,
    report_id: str,
    *,
    viewer_tier: str | None = None,
    viewer_role: str | None = None,
) -> dict[str, Any] | None:
    bundle = shared._load_ssot_report_bundle(db, report_id=report_id, viewer_tier=viewer_tier, viewer_role=viewer_role)
    if not bundle:
        return None
    return shared._build_report_payload(bundle, can_see_full=shared._can_view_full(viewer_tier, viewer_role), for_view=True)


def get_latest_report_view_payload_ssot(
    db: Session,
    stock_code: str,
    *,
    viewer_tier: str | None = None,
    viewer_role: str | None = None,
) -> dict[str, Any] | None:
    bundle = shared._load_ssot_report_bundle(db, stock_code=stock_code, viewer_tier=viewer_tier, viewer_role=viewer_role)
    if not bundle:
        return None
    return shared._build_report_payload(bundle, can_see_full=shared._can_view_full(viewer_tier, viewer_role), for_view=True)


def get_report_access_state_ssot(
    db: Session,
    report_id: str,
    *,
    viewer_tier: str | None = None,
    viewer_role: str | None = None,
) -> str:
    row = shared._execute_mappings(
        db,
        """
        SELECT r.trade_date
        FROM report r
        WHERE r.report_id = :report_id
          AND r.is_deleted = 0
          AND r.published = 1
        LIMIT 1
        """,
        {"report_id": report_id},
    ).first()
    if not row:
        return "not_found"
    cutoff = shared._viewer_cutoff_trade_date(db, viewer_tier, viewer_role)
    trade_date = row.get("trade_date")
    trade_date_text = trade_date.isoformat() if hasattr(trade_date, "isoformat") else str(trade_date or "")
    if cutoff and trade_date_text and trade_date_text < cutoff:
        return "hidden_by_viewer_cutoff"
    return "visible"


def _build_used_data_lineage(db: Session, report_id: str) -> list[dict[str, Any]]:
    rows = shared._execute_mappings(
        db,
        """
        SELECT
            u.usage_id,
            u.dataset_name,
            u.source_name,
            u.batch_id,
            u.status,
            u.status_reason,
            l.lineage_role,
            l.parent_batch_id
        FROM report_data_usage_link r
        JOIN report_data_usage u ON u.usage_id = r.usage_id
        LEFT JOIN data_batch_lineage l ON l.child_batch_id = u.batch_id
        WHERE r.report_id = :report_id
        ORDER BY r.created_at ASC, u.fetch_time DESC, u.dataset_name ASC, l.created_at ASC
        """,
        {"report_id": report_id},
    ).all()
    lineage_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        usage_id = str(row["usage_id"])
        current = lineage_map.setdefault(
            usage_id,
            {
                "usage_id": usage_id,
                "dataset_name": row.get("dataset_name"),
                "source_name": row.get("source_name"),
                "batch_id": row.get("batch_id"),
                "lineage_role": [],
                "parent_batch_ids": [],
                "status": row.get("status"),
                "status_reason": row.get("status_reason"),
            },
        )
        lineage_role = row.get("lineage_role")
        parent_batch_id = row.get("parent_batch_id")
        if lineage_role and lineage_role not in current["lineage_role"]:
            current["lineage_role"].append(lineage_role)
        if parent_batch_id and parent_batch_id not in current["parent_batch_ids"]:
            current["parent_batch_ids"].append(parent_batch_id)
    return list(lineage_map.values())


def get_report_advanced_payload_ssot(
    db: Session,
    report_id: str,
    *,
    viewer_tier: str | None = None,
    viewer_role: str | None = None,
) -> dict[str, Any] | None:
    bundle = shared._load_ssot_report_bundle(db, report_id=report_id, viewer_tier=viewer_tier, viewer_role=viewer_role)
    if not bundle:
        return None
    report = bundle["report"]
    reasoning_chain = shared._sanitize_reasoning_text(report.get("reasoning_chain_md"), report=report)
    is_truncated = False
    if not shared._can_view_full(viewer_tier, viewer_role) and len(reasoning_chain) > 200:
        reasoning_chain = f"{reasoning_chain[:197]}..."
        is_truncated = True
    return {
        "report_id": report.get("report_id"),
        "reasoning_chain": reasoning_chain,
        "is_truncated": is_truncated,
        "prior_stats_snapshot": shared._parse_prior_stats(report.get("prior_stats_snapshot")),
        "risk_audit_status": shared.humanize_risk_audit_status(report.get("risk_audit_status")),
        "risk_audit_skip_reason": shared.humanize_risk_audit_skip_reason(report.get("risk_audit_skip_reason")),
        "used_data_lineage": _build_used_data_lineage(db, str(report.get("report_id"))),
    }


def list_report_summaries_ssot(
    db: Session,
    *,
    viewer_tier: str | None = None,
    viewer_role: str | None = None,
    ignore_viewer_cutoff: bool = False,
    stock_code: str | None = None,
    stock_name: str | None = None,
    trade_date: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    recommendation: str | None = None,
    strategy_type: str | None = None,
    position_status: str | None = None,
    market_state: str | None = None,
    quality_flag: str | None = None,
    capital_tier: str | None = None,
    search: str | None = None,
    limit: int | None = None,
    in_pool_codes: list[str] | None = None,
    page: int = 1,
    page_size: int = 20,
    sort: str = "-trade_date",
) -> dict[str, Any]:
    if shared.report_storage_mode(db) != "ssot":
        return {
            "items": [],
            "total": 0,
            "data_status": "COMPUTING",
            "status_reason": "report_storage_not_ready",
            "degraded_banner": None,
        }

    where = ["r.is_deleted = 0", "r.published = 1"]
    params: dict[str, Any] = {}
    expanding: list[str] = []
    normalized_quality_flag = (quality_flag or "").strip().lower() if quality_flag is not None else None
    if normalized_quality_flag == "missing":
        return {"items": [], "total": 0, "data_status": "READY", "status_reason": None, "degraded_banner": None}
    if normalized_quality_flag and normalized_quality_flag != "ok":
        return {"items": [], "total": 0, "data_status": "READY", "status_reason": None, "degraded_banner": None}

    cutoff = None if ignore_viewer_cutoff else shared._viewer_cutoff_trade_date(db, viewer_tier, viewer_role)
    if cutoff:
        where.append("r.trade_date >= :viewer_cutoff")
        params["viewer_cutoff"] = cutoff
    if search and search.strip():
        query_text = search.strip()
        if query_text.isdigit() and len(query_text) == 6:
            where.append("r.stock_code LIKE :search_code")
            params["search_code"] = f"{query_text}.%"
        else:
            escaped = query_text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            where.append("r.stock_name_snapshot LIKE :search_name ESCAPE '\\'")
            params["search_name"] = f"%{escaped}%"
    if stock_code:
        where.append("r.stock_code = :stock_code")
        params["stock_code"] = stock_code
    if stock_name and stock_name.strip() and not (search and search.strip()):
        escaped_name = stock_name.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        where.append("r.stock_name_snapshot LIKE :stock_name ESCAPE '\\'")
        params["stock_name"] = f"%{escaped_name}%"
    if trade_date:
        where.append("r.trade_date = :trade_date")
        params["trade_date"] = trade_date
    if date_from:
        where.append("r.trade_date >= :date_from")
        params["date_from"] = date_from
    if date_to:
        where.append("r.trade_date <= :date_to")
        params["date_to"] = date_to
    if recommendation:
        where.append("r.recommendation = :recommendation")
        params["recommendation"] = recommendation
    if strategy_type:
        where.append("r.strategy_type = :strategy_type")
        params["strategy_type"] = strategy_type
    if market_state:
        where.append("r.market_state = :market_state")
        params["market_state"] = market_state
    if normalized_quality_flag:
        where.append("r.quality_flag = :quality_flag")
        params["quality_flag"] = normalized_quality_flag
    else:
        # Public list defaults to fully qualified reports only.
        where.append("COALESCE(LOWER(r.quality_flag), 'ok') = 'ok'")
    if in_pool_codes is not None:
        if not in_pool_codes:
            return {"items": [], "total": 0, "data_status": "READY", "status_reason": None, "degraded_banner": None}
        where.append("r.stock_code IN :pool_codes")
        params["pool_codes"] = in_pool_codes
        expanding.append("pool_codes")
    if position_status:
        statuses = shared.ssot_position_status_filter(position_status)
        if not statuses:
            return {"items": [], "total": 0, "data_status": "READY", "status_reason": None, "degraded_banner": None}
        report_ids = [
            str(row["report_id"])
            for row in shared._execute_mappings(
                db,
                """
                SELECT DISTINCT report_id
                FROM sim_position
                WHERE capital_tier = :capital_tier
                  AND position_status IN :statuses
                """,
                {"capital_tier": shared.normalize_capital_tier(capital_tier), "statuses": statuses},
                expanding=("statuses",),
            ).all()
        ]
        if not report_ids:
            return {"items": [], "total": 0, "data_status": "READY", "status_reason": None, "degraded_banner": None}
        where.append("r.report_id IN :report_ids")
        params["report_ids"] = report_ids
        expanding.append("report_ids")

    order_key = sort.lstrip("-")
    order_dir = "DESC" if sort.startswith("-") else "ASC"
    if order_key == "confidence":
        order_sql = f"r.confidence {order_dir}, r.trade_date DESC, r.report_id ASC"
    elif order_key == "stock_code":
        order_sql = f"r.stock_code {order_dir}, r.trade_date DESC, r.report_id ASC"
    else:
        order_sql = f"r.trade_date {order_dir}, r.confidence DESC, r.report_id ASC"
    where_sql = " AND ".join(where)
    _total_row = shared._execute_mappings(
        db,
        f"SELECT COUNT(*) AS total FROM report r WHERE {where_sql}",
        params,
        expanding=tuple(expanding),
    ).first()
    total = _total_row["total"] if _total_row else 0
    actual_limit = limit if limit is not None else page_size
    offset = 0 if limit is not None else (page - 1) * page_size
    query_rows = shared._execute_mappings(
        db,
        f"""
        SELECT
            r.report_id,
            r.stock_code,
            r.stock_name_snapshot,
            r.trade_date,
            r.recommendation,
            r.confidence,
            r.strategy_type,
            r.market_state,
            r.quality_flag,
            r.published,
            r.review_flag,
            r.status_reason,
            r.created_at
        FROM report r
        WHERE {where_sql}
        ORDER BY {order_sql}
        LIMIT :limit OFFSET :offset
        """,
        {**params, "limit": actual_limit, "offset": offset},
        expanding=tuple(expanding),
    ).all()
    report_ids = [str(row["report_id"]) for row in query_rows]
    position_map = shared._load_latest_position_map_ssot(
        db,
        report_ids,
        capital_tier=shared.normalize_capital_tier(capital_tier),
    )
    items = [
        {
            "report_id": row["report_id"],
            "stock_code": row["stock_code"],
            "stock_name": row["stock_name_snapshot"] or row["stock_code"],
            "trade_date": shared._iso_date(row["trade_date"]),
            "recommendation": row["recommendation"],
            "confidence": shared._to_float(row["confidence"]),
            "strategy_type": row["strategy_type"],
            "market_state": row["market_state"],
            "quality_flag": row["quality_flag"],
            "published": shared._to_bool(row["published"]),
            "review_flag": row["review_flag"],
            "status_reason": row["status_reason"],
            "position_status": position_map.get(str(row["report_id"])),
        }
        for row in query_rows
    ]
    has_degraded = any(item["quality_flag"] != "ok" for item in items)
    degraded_banner = shared._build_degraded_banner("degraded") if has_degraded else None
    data_status = "DEGRADED" if has_degraded else "READY"
    status_reason = "KLINE_COVERAGE_INSUFFICIENT" if has_degraded else None
    return {"items": items, "total": int(total or 0), "data_status": data_status, "status_reason": status_reason, "degraded_banner": degraded_banner}


def count_reports_ssot(
    db: Session,
    *,
    published_only: bool = False,
    created_at_from: datetime | None = None,
) -> int:
    if shared.report_storage_mode(db) != "ssot":
        return 0
    where = ["is_deleted = 0"]
    params: dict[str, Any] = {}
    if published_only:
        where.append("published = 1")
    if created_at_from is not None:
        where.append("created_at >= :created_at_from")
        params["created_at_from"] = created_at_from
    return int(shared._scalar(db, f"SELECT COUNT(*) FROM report WHERE {' AND '.join(where)}", params) or 0)


def latest_stock_name_from_reports_ssot(db: Session, stock_code: str) -> str | None:
    if shared.report_storage_mode(db) != "ssot":
        return None
    row = shared._execute_mappings(
        db,
        """
        SELECT stock_name_snapshot
        FROM report
        WHERE stock_code = :stock_code
          AND is_deleted = 0
        ORDER BY trade_date DESC, created_at DESC
        LIMIT 1
        """,
        {"stock_code": stock_code},
    ).first()
    if not row:
        return None
    return row.get("stock_name_snapshot")


def prediction_stats_ssot(db: Session) -> dict[str, Any]:
    if shared.report_storage_mode(db) != "ssot":
        return {}
    settled_rows = shared._execute_mappings(
        db,
        """
        SELECT
            report_id,
            stock_code,
            window_days,
            strategy_type,
            net_return_pct,
            exit_trade_date,
            updated_at
        FROM settlement_result
        WHERE settlement_status = 'settled'
        """,
    ).all()
    judged = len(settled_rows)
    correct = sum(1 for row in settled_rows if (shared._to_float(row.get("net_return_pct")) or 0.0) > 0)
    accuracy = round(correct / judged, 4) if judged else 0.0
    by_window = []
    for window_days in (1, 7, 14, 30, 60):
        rows = [row for row in settled_rows if shared._to_int(row.get("window_days")) == window_days]
        samples = len(rows)
        hits = sum(1 for row in rows if (shared._to_float(row.get("net_return_pct")) or 0.0) > 0)
        by_window.append(
            {
                "window_days": window_days,
                "accuracy": round(hits / samples, 4) if samples else None,
                "samples": samples,
                "coverage": round(samples / judged, 4) if judged else 0,
            }
        )
    stock_agg: dict[str, dict[str, int]] = {}
    for row in settled_rows:
        stock_code = str(row.get("stock_code") or "")
        stock_agg.setdefault(stock_code, {"correct": 0, "total": 0})
        stock_agg[stock_code]["total"] += 1
        if (shared._to_float(row.get("net_return_pct")) or 0.0) > 0:
            stock_agg[stock_code]["correct"] += 1
    by_stock = [
        {
            "stock_code": stock_code,
            "accuracy": round(item["correct"] / item["total"], 4) if item["total"] else 0,
            "samples": item["total"],
        }
        for stock_code, item in sorted(stock_agg.items())
    ]
    cutoff = datetime.now(timezone.utc) - timedelta(days=92)
    recent_rows = []
    for row in settled_rows:
        raw = row.get("updated_at") or row.get("exit_trade_date")
        if isinstance(raw, str) and len(raw) == 10:
            try:
                raw_dt = datetime.fromisoformat(raw + "T00:00:00+00:00")
            except ValueError:
                raw_dt = None
        elif isinstance(raw, datetime):
            raw_dt = raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
        else:
            raw_dt = None
        if raw_dt and raw_dt >= cutoff:
            recent_rows.append(row)
    recent_total = len(recent_rows)
    recent_correct = sum(1 for row in recent_rows if (shared._to_float(row.get("net_return_pct")) or 0.0) > 0)
    recent_3m = {
        "accuracy": round(recent_correct / recent_total, 4) if recent_total else None,
        "samples": recent_total,
        "coverage": round(recent_total / judged, 4) if judged else 0,
        "start_date": (cutoff + timedelta(days=2)).strftime("%Y-%m-%d"),
        "end_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    }
    strategy_rows = shared._execute_mappings(
        db,
        """
        SELECT
            strategy_type,
            sample_size,
            coverage_pct,
            win_rate,
            profit_loss_ratio,
            alpha_annual
        FROM strategy_metric_snapshot
        WHERE window_days = 30
          AND snapshot_date = (
              SELECT MAX(snapshot_date)
              FROM strategy_metric_snapshot s2
              WHERE s2.window_days = 30
          )
        """,
    ).all()
    by_strategy_type = {
        row["strategy_type"]: {
            "accuracy": shared._to_float(row.get("win_rate")),
            "pnl_ratio": shared._to_float(row.get("profit_loss_ratio")),
            "coverage": shared._to_float(row.get("coverage_pct")),
            "total": shared._to_int(row.get("sample_size")) or 0,
            "alpha": shared._to_float(row.get("alpha_annual")),
        }
        for row in strategy_rows
    }
    alpha_values = [shared._to_float(row.get("alpha_annual")) for row in strategy_rows if shared._to_float(row.get("alpha_annual")) is not None]
    alpha = round(sum(alpha_values) / len(alpha_values), 4) if alpha_values else None
    feedback_rows = shared._execute_mappings(
        db,
        """
        SELECT feedback_type
        FROM report_feedback
        WHERE created_at >= :cutoff
        """,
        {"cutoff": datetime.now(timezone.utc) - timedelta(days=7)},
    ).all()
    fb_total = len(feedback_rows)
    fb_negative = sum(1 for row in feedback_rows if row.get("feedback_type") == "negative")
    negative_feedback_rate = round(fb_negative / fb_total, 4) if fb_total else None
    return {
        "total_judged": judged,
        "judged": judged,
        "accuracy": accuracy,
        "by_window": by_window,
        "by_stock": by_stock,
        "recent_3m": recent_3m,
        "negative_feedback_rate": negative_feedback_rate,
        "negative_feedback_total": fb_total,
        "by_strategy_type": by_strategy_type,
        "alpha": alpha,
    }


def normalize_stock_code(stock_code: str) -> str:
    return (stock_code or "").strip().upper()


def validate_report_lookup_code(stock_code: str) -> str:
    code = normalize_stock_code(stock_code)
    if not _STOCK_CODE_RE.match(code) and not _STOCK_CODE_BARE_RE.match(code):
        raise HTTPException(status_code=400, detail="INVALID_PAYLOAD")
    return code


def _heuristic_stock_code(stock_code: str) -> str | None:
    code = normalize_stock_code(stock_code)
    if not _STOCK_CODE_BARE_RE.match(code):
        return None
    if code.startswith(("4", "8")):
        return f"{code}.BJ"
    if code.startswith(("0", "1", "2", "3")):
        return f"{code}.SZ"
    if code.startswith(("5", "6", "9")):
        return f"{code}.SH"
    return None


def resolve_stock_code(db: Session, stock_code: str) -> str:
    raw = normalize_stock_code(stock_code)
    if _STOCK_CODE_RE.match(raw):
        stock_row = (
            db.query(StockMaster.stock_code)
            .filter(StockMaster.stock_code == raw, StockMaster.is_delisted == False)  # noqa: E712
            .first()
        )
        if stock_row:
            return str(stock_row[0])
        report_row = (
            db.query(Report.stock_code)
            .filter(Report.stock_code == raw, Report.is_deleted == False)  # noqa: E712
            .order_by(Report.trade_date.desc(), Report.created_at.desc())
            .first()
        )
        if report_row:
            return str(report_row[0])
        raise HTTPException(status_code=404, detail="NOT_FOUND")
    if not _STOCK_CODE_BARE_RE.match(raw):
        raise HTTPException(status_code=404, detail="NOT_FOUND")
    stock_rows = (
        db.query(StockMaster.stock_code)
        .filter(StockMaster.stock_code.like(f"{raw}.%"), StockMaster.is_delisted == False)  # noqa: E712
        .order_by(StockMaster.stock_code.asc())
        .all()
    )
    if len(stock_rows) == 1:
        return str(stock_rows[0][0])
    report_row = (
        db.query(Report.stock_code)
        .filter(Report.stock_code.like(f"{raw}.%"), Report.is_deleted == False)  # noqa: E712
        .order_by(Report.trade_date.desc(), Report.created_at.desc())
        .first()
    )
    if report_row:
        return str(report_row[0])
    heuristic = _heuristic_stock_code(raw)
    if heuristic:
        stock_exists = (
            db.query(StockMaster.stock_code)
            .filter(StockMaster.stock_code == heuristic, StockMaster.is_delisted == False)  # noqa: E712
            .first()
        )
        if stock_exists:
            return heuristic
        report_exists = (
            db.query(Report.stock_code)
            .filter(Report.stock_code == heuristic, Report.is_deleted == False)  # noqa: E712
            .first()
        )
        if report_exists:
            return heuristic
    raise HTTPException(status_code=404, detail="NOT_FOUND")


def _to_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _latest_daily_report(db: Session, stock_code: str) -> Report | None:
    rows = (
        db.query(Report)
        .filter(Report.stock_code == stock_code, Report.run_mode == "daily", Report.is_deleted == False)  # noqa: E712
        .order_by(Report.trade_date.desc(), Report.created_at.desc())
        .limit(20)
        .all()
    )
    target_trade_date = trade_date_str()
    for row in rows:
        created_at = _to_utc(row.created_at)
        if not created_at:
            continue
        if trade_date_str(created_at) == target_trade_date:
            return row
    return None


def latest_report_id_for_code(
    db: Session,
    stock_code: str,
    *,
    viewer_tier: str | None = None,
    viewer_role: str | None = None,
) -> str | None:
    if shared.report_storage_mode(db) == "ssot":
        cached = get_latest_report_view_payload_ssot(
            db,
            stock_code,
            viewer_tier=viewer_tier,
            viewer_role=viewer_role,
        )
        return cached.get("report_id") if cached else None
    cached = _latest_daily_report(db, stock_code)
    return cached.report_id if cached else None


def build_report_unavailable_context(stock_code: str, fail_reason: str, current_user) -> dict[str, object]:
    copy = humanize_report_unavailable(fail_reason)
    is_admin = bool(current_user and (current_user.role or "").lower() in {"admin", "super_admin"})
    return {
        "stock_code": stock_code,
        "page_title": copy["title"],
        "page_message": copy["message"],
        "page_hint": "可先返回研报列表查看已发布内容，稍后再回来刷新本页。",
        "current_user": current_user,
        "is_admin": is_admin,
    }


def recent_report_failure(db: Session, stock_code: str) -> str | None:
    row = db.execute(
        shared.text(
            "SELECT status, status_reason, finished_at "
            "FROM report_generation_task "
            "WHERE stock_code = :code "
            "ORDER BY created_at DESC LIMIT 1"
        ),
        {"code": stock_code},
    ).mappings().first()
    if not row:
        return None
    status = str(row.get("status", ""))
    reason = normalize_error_code(row.get("status_reason"), default="")
    if status == "Failed" and reason in {"DEPENDENCY_NOT_READY", "NOT_IN_CORE_POOL"}:
        return reason
    return None


def report_status_payload(db: Session, stock_code: str) -> dict[str, Any]:
    code = resolve_stock_code(db, stock_code)
    report_id = latest_report_id_for_code(db, code, viewer_tier="Free", viewer_role=None)
    if report_id:
        return {
            "stock_code": code,
            "job": {
                "status": "done",
                "report_id": report_id,
                "ready": True,
                "error": None,
            },
        }

    task_row = db.execute(
        shared.text(
            """
            SELECT task_id, status, status_reason, started_at, finished_at
            FROM report_generation_task
            WHERE stock_code = :stock_code
            ORDER BY created_at DESC
            LIMIT 1
            """
        ),
        {"stock_code": code},
    ).mappings().first()
    if not task_row:
        return {
            "stock_code": code,
            "job": {
                "status": "idle",
                "report_id": None,
                "ready": False,
                "error": None,
            },
        }

    task_status = str(task_row.get("status") or "")
    mapped_status = "running" if task_status in {"Pending", "Processing", "Suspended"} else (
        "failed" if task_status == "Failed" else "idle"
    )
    return {
        "stock_code": code,
        "job": {
            "status": mapped_status,
            "task_id": task_row.get("task_id"),
            "started_at": str(task_row.get("started_at") or ""),
            "updated_at": str(task_row.get("finished_at") or task_row.get("started_at") or ""),
            "report_id": None,
            "ready": False,
            "error": task_row.get("status_reason") if mapped_status == "failed" else None,
        },
    }


def _build_market_dual_source_from_snapshot(market_snapshot: dict) -> dict:
    return {
        "dual_status": market_snapshot.get("dual_status"),
        "selected_by_order": market_snapshot.get("source_selected_by_order"),
        "comparison": market_snapshot.get("dual_comparison"),
        "sources": market_snapshot.get("dual_sources"),
    }


def _build_plain_fallback(report: dict, market_snapshot: dict, market_features: dict, dual_source_market: dict) -> dict:
    recommendation_cn = report.get("recommendation_cn") or {"BUY": "买入", "SELL": "卖出", "HOLD": "观望等待"}.get(
        report.get("recommendation"), "观望等待"
    )
    plain_report = report.get("plain_report") or {}
    return {
        "title": f"{report.get('stock_code')} 白话研报",
        "action_now": recommendation_cn,
        "one_sentence": f"结论：{recommendation_cn}。先看真实双源行情，再看 1~7 天方向和回测稳定性。",
        "execution_plan": plain_report.get("execution_plan") or {},
        "what_to_do_now": plain_report.get("what_to_do_now") or [],
        "key_numbers": plain_report.get("key_numbers") or [],
        "accuracy_explain": plain_report.get("accuracy_explain") or {},
        "cause_effect_chain": plain_report.get("cause_effect_chain") or [],
        "dual_source_market": {
            "selected_source": market_snapshot.get("source"),
            "selected_price": market_snapshot.get("last_price"),
            "eastmoney_price": (((dual_source_market or {}).get("sources") or {}).get("eastmoney") or {}).get("last_price"),
            "tdx_price": (((dual_source_market or {}).get("sources") or {}).get("tdx") or {}).get("last_price"),
            "diff_abs": ((dual_source_market or {}).get("comparison") or {}).get("last_price_diff_abs"),
            "diff_pct": ((dual_source_market or {}).get("comparison") or {}).get("last_price_diff_pct"),
            "price_consistent": ((dual_source_market or {}).get("comparison") or {}).get("price_consistent"),
            "dual_status": (dual_source_market or {}).get("dual_status"),
        },
        "terms": plain_report.get("terms") or [],
    }


def _ensure_list_for_view(val, *, split_lines: bool = False):
    if isinstance(val, list):
        return val
    if val is None:
        return []
    if isinstance(val, str):
        text = val.strip()
        if not text:
            return []
        if split_lines:
            return [line.strip() for line in text.split("\n") if line.strip()]
        return [text]
    return []


def _plain_need_fallback(plain: dict) -> bool:
    for key in ("what_to_do_now", "key_numbers", "cause_effect_chain", "terms"):
        value = plain.get(key)
        if not isinstance(value, list) or len(value) == 0:
            return True
    return False


async def _ensure_report_payload_for_view(report_payload: dict, stock_code: str) -> dict:
    report = dict(report_payload or {})
    report["stock_code"] = report.get("stock_code") or stock_code
    dims = dict(report.get("dimensions") or {})
    market_snapshot = dict(dims.get("market_snapshot") or {})
    market_features = dict(dims.get("market_features") or {})
    dual_source = dict(dims.get("market_dual_source") or {})
    if not dual_source.get("dual_status"):
        dual_source = {**dual_source, **_build_market_dual_source_from_snapshot(market_snapshot)}
    need_live_quote = not dual_source.get("dual_status") or not dual_source.get("sources")
    if need_live_quote:
        try:
            live = await fetch_quote_snapshot(stock_code)
            if live:
                old_price = market_snapshot.get("last_price")
                live_price = live.get("last_price")
                replace_snapshot = old_price is None and live_price is not None
                if isinstance(old_price, (int, float)) and isinstance(live_price, (int, float)) and live_price > 0:
                    replace_snapshot = abs(old_price - live_price) / live_price > 0.2
                if replace_snapshot:
                    market_snapshot = live
                dual_source = _build_market_dual_source_from_snapshot(live)
        except Exception as exc:
            logger.warning("live_quote_refresh_failed stock=%s err=%s", stock_code, str(exc) or exc.__class__.__name__)
    dims["market_snapshot"] = market_snapshot
    dims["market_features"] = market_features
    dims["market_dual_source"] = dual_source
    report["dimensions"] = dims
    plain = dict(report.get("plain_report") or {})
    fallback_plain = _build_plain_fallback(report, market_snapshot, market_features, dual_source)
    if _plain_need_fallback(plain):
        plain = fallback_plain
    else:
        for list_key in ("key_numbers", "cause_effect_chain", "terms", "what_to_do_now"):
            if list_key in plain and not isinstance(plain[list_key], list):
                plain[list_key] = []
        if plain.get("evidence_backing_points") is not None and not isinstance(plain["evidence_backing_points"], list):
            plain["evidence_backing_points"] = _ensure_list_for_view(plain["evidence_backing_points"], split_lines=True)
    if not plain.get("execution_plan"):
        plain["execution_plan"] = fallback_plain.get("execution_plan")
    if not plain.get("accuracy_explain"):
        plain["accuracy_explain"] = fallback_plain.get("accuracy_explain")
    report["plain_report"] = plain
    if not report.get("direction_forecast") and report.get("price_forecast"):
        report["direction_forecast"] = (report.get("price_forecast") or {}).get("direction_forecast")
    reasoning_trace = dict(report.get("reasoning_trace") or {})
    if reasoning_trace.get("data_sources") is not None and not isinstance(reasoning_trace["data_sources"], list):
        reasoning_trace["data_sources"] = _ensure_list_for_view(reasoning_trace["data_sources"], split_lines=True)
    if reasoning_trace.get("analysis_steps") is not None and not isinstance(reasoning_trace["analysis_steps"], list):
        reasoning_trace["analysis_steps"] = _ensure_list_for_view(reasoning_trace["analysis_steps"], split_lines=True)
    if reasoning_trace.get("evidence_items") is not None and not isinstance(reasoning_trace["evidence_items"], list):
        raw = reasoning_trace["evidence_items"]
        if isinstance(raw, str) and raw.strip():
            reasoning_trace["evidence_items"] = [{"title": raw.strip(), "summary": raw.strip()}]
        else:
            reasoning_trace["evidence_items"] = []
    report["reasoning_trace"] = reasoning_trace
    quality_gate = dict(report.get("quality_gate") or {})
    if not isinstance(quality_gate.get("missing_fields"), list):
        quality_gate["missing_fields"] = []
    if not isinstance(quality_gate.get("recover_actions"), list):
        quality_gate["recover_actions"] = []
    report["quality_gate"] = quality_gate
    return report


async def load_report_view_payload(
    db: Session,
    report_id: str,
    *,
    viewer_tier: str | None = None,
    viewer_role: str | None = None,
) -> dict[str, Any] | None:
    if shared.report_storage_mode(db) == "ssot":
        return get_report_view_payload_ssot(db, report_id, viewer_tier=viewer_tier, viewer_role=viewer_role)
    row = db.get(Report, report_id)
    if not row:
        return None
    view_report = await _ensure_report_payload_for_view(dict(row.content_json or {}), row.stock_code)
    view_report["report_id"] = row.report_id
    view_report["created_at"] = row.created_at.isoformat() if row.created_at else view_report.get("created_at")
    view_report["trade_date"] = row.trade_date or view_report.get("trade_date")
    return view_report


def build_report_template_context(
    *,
    view_report: dict[str, Any],
    current_user,
    membership_level: str | None,
    user_role: str | None,
    can_see_full: bool,
    can_access_sim: bool,
) -> dict[str, Any]:
    from app.services.strategy_failure import get_strategy_paused

    billing_capability = get_payment_capability()
    return {
        "report": view_report,
        "current_user": current_user,
        "membership_level": membership_level,
        "user_role": user_role,
        "can_see_instruction_full": can_see_full,
        "can_see_forecast_14_60": can_see_full,
        "can_see_advanced": can_see_full,
        "strategy_paused": get_strategy_paused(),
        "billing_capability": billing_capability,
        "payment_available": payment_browser_checkout_ready(billing_capability),
        "can_access_sim": can_access_sim,
    }


def build_report_template_context_for_user(
    *,
    view_report: dict[str, Any],
    current_user,
    subscription_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    is_admin = bool(current_user and (current_user.role or "").strip().lower() in {"admin", "super_admin"})
    membership_level = current_user.tier if current_user else None
    if current_user and not is_admin and isinstance(subscription_state, dict):
        if subscription_state.get("status") == "active":
            membership_level = str(subscription_state.get("tier") or membership_level or "Free")
        else:
            membership_level = "Free"
    is_paid = bool(
        current_user
        and isinstance(subscription_state, dict)
        and subscription_state.get("status") == "active"
        and has_paid_membership(subscription_state.get("tier"))
    )
    can_see_full = bool(current_user) and (is_admin or is_paid)
    return build_report_template_context(
        view_report=view_report,
        current_user=current_user,
        membership_level=membership_level,
        user_role=current_user.role if current_user else None,
        can_see_full=can_see_full,
        can_access_sim=bool(current_user) and (is_admin or is_paid),
    )
