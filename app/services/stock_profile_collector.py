"""v25 stock_profile collector.

Fetches fundamental snapshot (PE/PB/total_mv/region/industry/list_date) from
东方财富 push2 API in strictest remote-real-time mode and persists as a
report_data_usage row tagged `stock_profile`.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.services.usage_lineage import stable_upsert_usage_row

_DATASET = "stock_profile"
_SOURCE = "eastmoney_push2_stock_get"
_SNAPSHOT_PREFIX = "profile_snapshot:"
_ENDPOINT = "https://push2.eastmoney.com/api/qt/stock/get"
_FIELDS = "f57,f58,f84,f85,f116,f117,f127,f128,f162,f167,f173,f189"
_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"}
_COMPANY_INFO_ENDPOINT = "https://emh5.eastmoney.com/api/gongsi/getgongsijibenxinxi"


def _secid(stock_code: str) -> str:
    code = stock_code.split(".")[0]
    if stock_code.endswith(".SH"):
        return f"1.{code}"
    return f"0.{code}"


def _parse(data: dict[str, Any]) -> dict[str, Any] | None:
    if not data:
        return None
    pe_raw = data.get("f162")
    pb_raw = data.get("f167")
    roe_raw = data.get("f173")
    list_date_raw = data.get("f189")
    list_date = None
    if list_date_raw:
        s = str(list_date_raw)
        if len(s) == 8:
            list_date = f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return {
        "pe_ttm": round(pe_raw / 100, 2) if isinstance(pe_raw, (int, float)) else None,
        "pb": round(pb_raw / 100, 2) if isinstance(pb_raw, (int, float)) else None,
        "roe_pct": round(roe_raw, 2) if isinstance(roe_raw, (int, float)) else None,
        "total_mv": float(data["f116"]) if data.get("f116") is not None else None,
        "circulating_mv": float(data["f117"]) if data.get("f117") is not None else None,
        "industry": data.get("f127") or None,
        "region": data.get("f128") or None,
        "list_date": list_date,
        "total_shares": float(data["f84"]) if data.get("f84") is not None else None,
        "circulating_shares": float(data["f85"]) if data.get("f85") is not None else None,
    }


def _fetch_company_brief(stock_code: str) -> str | None:
    """Fetch company brief (简介) from East Money."""
    try:
        code = stock_code.split(".")[0]
        with httpx.Client(trust_env=False, timeout=10.0, headers=_HEADERS) as client:
            resp = client.get(
                _COMPANY_INFO_ENDPOINT,
                params={"gongsiCode": code, "needItems": "gongsijibenxinxi"},
            )
        if resp.status_code == 200:
            data = resp.json() or {}
            result = data.get("result", {}) or {}
            jibenxinxi = result.get("gongsijibenxinxi") or {}
            # Extract company brief from various possible fields
            brief = (
                jibenxinxi.get("gongsijianjie")
                or jibenxinxi.get("gongsi_jianjie")
                or jibenxinxi.get("jianjie")
                or None
            )
            if brief and isinstance(brief, str) and brief.strip():
                return brief.strip()
    except Exception:
        pass
    return None


def parse_profile_snapshot(status_reason: str | None) -> dict[str, Any] | None:
    if not status_reason or not status_reason.startswith(_SNAPSHOT_PREFIX):
        return None
    try:
        return json.loads(status_reason[len(_SNAPSHOT_PREFIX):])
    except Exception:
        return None


def _ensure_batch(
    db: Session,
    *,
    trade_date: str,
    batch_id: str | None,
) -> str:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
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
            WHERE source_name = 'stock_profile'
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
                :batch_id, 'stock_profile', :trade_date, 'stock_supplemental', :batch_seq,
                'RUNNING', 'ok', NULL, NULL,
                1, 0, 0, NULL,
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
    fetch_time: datetime,
    status: str,
    status_reason: str,
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
        "source_name": _SOURCE,
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


def _load_latest_profile_snapshot(
    db: Session,
    *,
    stock_code: str,
    trade_date: str,
) -> dict[str, Any] | None:
    row = db.execute(
        text(
            """
            SELECT trade_date, status_reason
            FROM report_data_usage
            WHERE stock_code = :sc
              AND dataset_name = :dn
              AND status IN ('ok', 'stale_ok')
              AND trade_date <= :td
            ORDER BY trade_date DESC, created_at DESC
            LIMIT 1
            """
        ),
        {"sc": stock_code, "dn": _DATASET, "td": trade_date},
    ).mappings().first()
    if not row:
        return None
    snapshot = parse_profile_snapshot(row.get("status_reason"))
    if not snapshot:
        return None
    return {
        "trade_date": str(row.get("trade_date") or trade_date),
        "snapshot": dict(snapshot),
    }


def fetch_stock_profile(stock_code: str) -> dict[str, Any]:
    try:
        with httpx.Client(trust_env=False, timeout=15.0, headers=_HEADERS) as client:
            resp = client.get(_ENDPOINT, params={"secid": _secid(stock_code), "fields": _FIELDS})
        if resp.status_code != 200:
            return {"status": "failed", "reason": f"http_{resp.status_code}"}
        snap = _parse((resp.json() or {}).get("data") or {})
        if not snap:
            return {"status": "failed", "reason": "empty_payload"}
        if snap.get("pe_ttm") is None and snap.get("pb") is None and snap.get("total_mv") is None:
            return {"status": "failed", "reason": "no_financial_metrics"}
        # Attempt to fetch company brief (non-critical)
        company_brief = _fetch_company_brief(stock_code)
        if company_brief:
            snap["company_brief"] = company_brief
        return {"status": "ok", "snapshot": snap}
    except Exception as exc:  # pragma: no cover
        return {"status": "failed", "reason": f"{type(exc).__name__}:{exc}"}


def persist_stock_profile(
    db: Session,
    *,
    stock_code: str,
    trade_date: str,
    batch_id: str | None = None,
) -> dict[str, Any]:
    result = fetch_stock_profile(stock_code)
    reason = result.get("reason")
    status = "ok" if result.get("status") == "ok" else "missing"
    snapshot = result.get("snapshot") or {}

    if status != "ok":
        fallback = _load_latest_profile_snapshot(db, stock_code=stock_code, trade_date=trade_date)
        if fallback:
            snapshot = dict(fallback.get("snapshot") or {})
            snapshot.setdefault("_source_trade_date", fallback["trade_date"])
            status = "stale_ok"
            reason = f"reused_latest_ok_snapshot:{fallback['trade_date']}"

    if status in {"ok", "stale_ok"}:
        status_reason = _SNAPSHOT_PREFIX + json.dumps(snapshot, ensure_ascii=False, default=str)
    else:
        status_reason = f"failed:{reason}"
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    batch = _ensure_batch(db, trade_date=trade_date, batch_id=batch_id)
    stable_upsert_usage_row(
        db,
        trade_date=trade_date,
        stock_code=stock_code,
        dataset_name=_DATASET,
        source_name=_SOURCE,
        batch_id=batch,
        fetch_time=now,
        status=status,
        status_reason=status_reason,
        created_at=now,
    )
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
            "batch_status": "SUCCESS" if status in {"ok", "stale_ok"} else "FAILED",
            "quality_flag": status if status in {"ok", "stale_ok"} else "missing",
            "records_success": 1 if status in {"ok", "stale_ok"} else 0,
            "records_failed": 0 if status in {"ok", "stale_ok"} else 1,
            "status_reason": None if status in {"ok", "stale_ok"} else status_reason,
            "now": now,
            "batch_id": batch,
        },
    )
    db.commit()
    return {"persisted_status": status, "snapshot": snapshot, "reason": reason}