"""v24 FR-04 extension: persist 3 capital datasets into report_data_usage.

Strict public-report policy (2026-04-23):
- Real snapshots remain status=ok.
- Deterministic proxy snapshots are persisted as status=proxy_ok.
- Realtime-only financing snapshots are persisted as status=realtime_only.
- Cached/stale snapshots that still carry concrete metrics are persisted as status=stale_ok.
- Only truly unavailable rows are written as status=missing.

Dataset mapping:
- main_force_flow   ← fetch_capital_dimensions()['capital_flow']['main_force']
- dragon_tiger_list ← fetch_capital_dimensions()['dragon_tiger']
- margin_financing  ← fetch_capital_dimensions()['margin_financing']

For ok rows we additionally serialize a compact metric snapshot into
status_reason as `capital_snapshot:<json>`, so downstream read_model can
render real numbers without a second fetch.

Usage:
    from app.services.capital_usage_collector import persist_capital_usage
    await persist_capital_usage(db, stock_code='600519.SH', trade_date='2026-04-16')
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.services.capital_flow import fetch_capital_dimensions
from app.services.usage_lineage import stable_upsert_usage_row

_CAPITAL_SNAPSHOT_PREFIX = "capital_snapshot:"

# Per-dataset allowed statuses. margin_financing历史接口早已失效，push2 实时快照
# (status=realtime_only) 为 docs/core 05 §7 合约正式枚举值，非伪造数据；予以
# 接受并标记为 ok，snapshot 里保留 latest_rzye/latest_rqye。
_PERSISTABLE_SNAPSHOT_STATUSES = {"ok", "proxy_ok", "realtime_only", "stale_ok"}

_DATASET_MAP = {
    "main_force_flow": ("capital_flow", "main_force", "eastmoney_fflow_daykline"),
    "dragon_tiger_list": ("dragon_tiger", None, "eastmoney_lhb"),
    "margin_financing": ("margin_financing", None, "eastmoney_push2_rzrq"),
}

# Minimal fields to persist per dataset for downstream rendering
_SNAPSHOT_FIELDS = {
    "main_force_flow": (
        "net_inflow_1d", "net_inflow_3d", "net_inflow_5d", "net_inflow_10d",
        "net_inflow_20d", "super_large_net_5d", "large_net_5d", "streak_days",
        "history_records", "history_end_date",
    ),
    "dragon_tiger_list": (
        "lhb_count_30d", "lhb_count_90d", "lhb_count_250d",
        "net_buy_total", "avg_net_buy_ratio", "seat_concentration", "source",
    ),
    "margin_financing": (
        "latest_rzye", "latest_rqye", "rzye_delta_5d", "rzye_delta_20d",
        "rqye_delta_5d", "rqye_delta_20d", "history_records", "history_end_date",
    ),
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _ensure_batch(
    db: Session,
    *,
    trade_date: str,
    batch_id: str | None,
) -> str:
    now = _utc_now()
    if batch_id:
        existing = db.execute(
            text("SELECT batch_id FROM data_batch WHERE batch_id = :batch_id LIMIT 1"),
            {"batch_id": batch_id},
        ).fetchone()
        if existing:
            return str(existing[0])

    ensured_batch_id = batch_id or str(uuid4())
    next_seq = db.execute(
        text(
            """
            SELECT COALESCE(MAX(batch_seq), 0) + 1
            FROM data_batch
            WHERE source_name = 'supplemental_capital'
              AND trade_date = :trade_date
              AND batch_scope = 'stock_supplemental'
            """
        ),
        {"trade_date": trade_date},
    ).scalar_one()
    db.execute(
        text(
            """
            INSERT INTO data_batch (
                batch_id, source_name, trade_date, batch_scope, batch_seq,
                batch_status, quality_flag, covered_stock_count, core_pool_covered_count,
                records_total, records_success, records_failed, status_reason,
                trigger_task_run_id, started_at, finished_at, updated_at, created_at
            ) VALUES (
                :batch_id, 'supplemental_capital', :trade_date, 'stock_supplemental', :batch_seq,
                'RUNNING', 'ok', NULL, NULL,
                3, 0, 0, NULL,
                NULL, :now, :now, :now, :now
            )
            """
        ),
        {"batch_id": ensured_batch_id, "trade_date": trade_date, "batch_seq": int(next_seq or 1), "now": now},
    )
    return ensured_batch_id


def _upsert_data_usage_fact(
    db: Session,
    *,
    usage_id: str,
    batch_id: str,
    trade_date: str,
    stock_code: str,
    source_name: str,
    fetch_time: datetime,
    status: str,
    status_reason: str | None,
) -> None:
    existing = db.execute(
        text("SELECT usage_id FROM data_usage_fact WHERE usage_id = :usage_id LIMIT 1"),
        {"usage_id": usage_id},
    ).fetchone()
    params = {
        "usage_id": usage_id,
        "batch_id": batch_id,
        "trade_date": trade_date,
        "stock_code": stock_code,
        "source_name": source_name,
        "fetch_time": fetch_time.isoformat(),
        "status": status,
        "status_reason": status_reason,
        "created_at": fetch_time.isoformat(),
    }
    if existing:
        db.execute(
            text(
                """
                UPDATE data_usage_fact
                SET batch_id=:batch_id, trade_date=:trade_date, stock_code=:stock_code,
                    source_name=:source_name, fetch_time=:fetch_time,
                    status=:status, status_reason=:status_reason, created_at=:created_at
                WHERE usage_id=:usage_id
                """
            ),
            params,
        )
        return
    db.execute(
        text(
            """
            INSERT INTO data_usage_fact
                (usage_id, batch_id, trade_date, stock_code, source_name, fetch_time, status, status_reason, created_at)
            VALUES
                (:usage_id, :batch_id, :trade_date, :stock_code, :source_name, :fetch_time, :status, :status_reason, :created_at)
            """
        ),
        params,
    )


def _extract_node(payload: dict[str, Any], *, outer_key: str, inner_key: str | None) -> dict[str, Any]:
    outer = payload.get(outer_key) or {}
    node = outer.get(inner_key) if inner_key else outer
    return node if isinstance(node, dict) else {}


def _extract_status(node: dict[str, Any], outer_key: str) -> tuple[str, str | None]:
    status = str(node.get("status") or "missing").lower()
    reason = node.get("reason")
    if reason is None:
        reason = f"capital_fetcher_returned_{status}"
    return status, str(reason)


def _normalize_persisted_status(
    dataset_name: str,
    node: dict[str, Any],
    *,
    fetched_status: str,
    raw_reason: str | None,
) -> str:
    reason_text = str(raw_reason or "").lower()
    proxy_hint = bool(node.get("proxy")) or any(
        tag in reason_text
        for tag in (
            "tdx_proxy",
            "kline_proxy",
            "proxy",
        )
    )

    if dataset_name == "main_force_flow":
        if fetched_status == "ok":
            return "ok"
        if fetched_status in {"proxy_ok", "stale_ok"} and proxy_hint:
            return "proxy_ok"
        if fetched_status == "stale_ok":
            return "stale_ok"
        return "missing"

    if dataset_name == "dragon_tiger_list":
        return "ok" if fetched_status == "ok" else "missing"

    if dataset_name == "margin_financing":
        if fetched_status == "ok":
            return "ok"
        if fetched_status == "realtime_only":
            return "realtime_only"
        if fetched_status in {"proxy_ok", "stale_ok"} and proxy_hint:
            return "proxy_ok"
        if fetched_status == "stale_ok":
            return "stale_ok"
        return "missing"

    return "missing"


def _build_snapshot(
    dataset_name: str,
    node: dict[str, Any],
    *,
    persisted_status: str,
    raw_reason: str | None,
) -> str:
    fields = _SNAPSHOT_FIELDS[dataset_name]
    snap = {k: node.get(k) for k in fields if k in node}
    if raw_reason:
        snap["source_reason"] = str(raw_reason)
    if persisted_status != "ok":
        snap["persisted_status"] = persisted_status
    if node.get("proxy") is not None:
        snap["proxy"] = bool(node.get("proxy"))
    return _CAPITAL_SNAPSHOT_PREFIX + json.dumps(snap, ensure_ascii=False, default=str)


def parse_capital_snapshot(status_reason: str | None) -> dict[str, Any] | None:
    """Decode the capital_snapshot JSON persisted in report_data_usage.status_reason."""
    if not status_reason or not status_reason.startswith(_CAPITAL_SNAPSHOT_PREFIX):
        return None
    try:
        return json.loads(status_reason[len(_CAPITAL_SNAPSHOT_PREFIX):])
    except Exception:
        return None


async def persist_capital_usage(
    db: Session,
    *,
    stock_code: str,
    trade_date: str,
    batch_id: str | None = None,
) -> dict[str, Any]:
    """Fetch capital dimensions for one stock and persist 3 usage rows.

    Returns summary: {stock_code, trade_date, per_dataset: {name: status}}.
    Only inserts rows when absent for (trade_date, stock_code, dataset_name);
    if exists with status != ok, updates in-place to reflect latest attempt.
    """
    dims = await fetch_capital_dimensions(stock_code)
    now = _utc_now()
    batch = _ensure_batch(db, trade_date=trade_date, batch_id=batch_id)
    summary: dict[str, Any] = {"stock_code": stock_code, "trade_date": trade_date, "per_dataset": {}}

    for dataset_name, (outer, inner, source_name) in _DATASET_MAP.items():
        node = _extract_node(dims, outer_key=outer, inner_key=inner)
        fetched_status, raw_reason = _extract_status(node, outer)
        persisted_status = _normalize_persisted_status(
            dataset_name,
            node,
            fetched_status=fetched_status,
            raw_reason=raw_reason,
        )
        if persisted_status in _PERSISTABLE_SNAPSHOT_STATUSES:
            persisted_reason = _build_snapshot(
                dataset_name,
                node,
                persisted_status=persisted_status,
                raw_reason=raw_reason,
            )
        else:
            persisted_reason = f"{fetched_status}:{raw_reason}"
        usage_id = stable_upsert_usage_row(
            db,
            trade_date=trade_date,
            stock_code=stock_code,
            dataset_name=dataset_name,
            source_name=source_name,
            batch_id=batch,
            fetch_time=now,
            status=persisted_status,
            status_reason=persisted_reason,
            created_at=now,
        )
        summary["per_dataset"][dataset_name] = {
            "fetched_status": fetched_status,
            "persisted_status": persisted_status,
            "snapshot": parse_capital_snapshot(persisted_reason) if persisted_status in _PERSISTABLE_SNAPSHOT_STATUSES else None,
        }
    persisted_statuses = [
        str(item.get("persisted_status") or "missing").lower()
        for item in summary["per_dataset"].values()
    ]
    success_count = sum(1 for status in persisted_statuses if status in _PERSISTABLE_SNAPSHOT_STATUSES)
    if success_count == len(persisted_statuses) and persisted_statuses:
        batch_status = "SUCCESS"
    elif success_count:
        batch_status = "PARTIAL_SUCCESS"
    else:
        batch_status = "FAILED"
    quality_flag = "missing"
    if success_count:
        quality_flag = "ok" if all(status == "ok" for status in persisted_statuses) else "stale_ok"
    db.execute(
        text(
            """
            UPDATE data_batch
            SET batch_status = :batch_status,
                quality_flag = :quality_flag,
                records_success = :records_success,
                records_failed = :records_failed,
                status_reason = :status_reason,
                finished_at = :now,
                updated_at = :now
            WHERE batch_id = :batch_id
            """
        ),
        {
            "batch_status": batch_status,
            "quality_flag": quality_flag,
            "records_success": success_count,
            "records_failed": max(0, len(persisted_statuses) - success_count),
            "status_reason": None if success_count else "capital_usage_collect_failed",
            "now": now,
            "batch_id": batch,
        },
    )
    db.commit()
    return summary

