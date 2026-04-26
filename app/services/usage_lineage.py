from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.models import Base


def _naive_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if value is None:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _date_value(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _dedupe_report_usage_links(db: Session, *, usage_id: str) -> None:
    link_table = Base.metadata.tables.get("report_data_usage_link")
    if link_table is None:
        return

    rows = db.execute(
        select(
            link_table.c.report_data_usage_link_id,
            link_table.c.report_id,
            link_table.c.usage_id,
            link_table.c.created_at,
        )
        .where(link_table.c.usage_id == usage_id)
        .order_by(link_table.c.created_at.asc(), link_table.c.report_data_usage_link_id.asc())
    ).mappings().all()

    seen: set[tuple[str, str]] = set()
    delete_ids: list[str] = []
    for row in rows:
        key = (str(row.get("report_id") or ""), str(row.get("usage_id") or ""))
        if key in seen:
            delete_ids.append(str(row["report_data_usage_link_id"]))
            continue
        seen.add(key)

    if delete_ids:
        db.execute(
            link_table.delete().where(link_table.c.report_data_usage_link_id.in_(delete_ids))
        )


def stable_upsert_usage_row(
    db: Session,
    *,
    trade_date: date | str,
    stock_code: str,
    dataset_name: str,
    source_name: str,
    batch_id: str,
    fetch_time: datetime | str,
    status: str,
    status_reason: str | None,
    created_at: datetime | str | None = None,
) -> str:
    usage_table = Base.metadata.tables["report_data_usage"]
    fact_table = Base.metadata.tables.get("data_usage_fact")
    link_table = Base.metadata.tables.get("report_data_usage_link")

    trade_day = _date_value(trade_date)
    fetch_dt = _naive_utc(fetch_time)
    created_dt = _naive_utc(created_at or fetch_dt)

    rows = db.execute(
        text(
            """
            SELECT
                u.usage_id,
                COALESCE((
                    SELECT COUNT(*)
                    FROM report_data_usage_link l
                    WHERE l.usage_id = u.usage_id
                ), 0) AS link_count,
                CASE WHEN EXISTS(
                    SELECT 1 FROM data_usage_fact f WHERE f.usage_id = u.usage_id
                ) THEN 1 ELSE 0 END AS has_fact
            FROM report_data_usage u
            WHERE u.trade_date = :trade_date
              AND u.stock_code = :stock_code
              AND u.dataset_name = :dataset_name
            ORDER BY
              link_count DESC,
              has_fact DESC,
              u.fetch_time DESC,
              u.created_at DESC,
              u.usage_id DESC
            """
        ),
        {
            "trade_date": trade_day,
            "stock_code": stock_code,
            "dataset_name": dataset_name,
        },
    ).mappings().all()

    usage_id = str(rows[0]["usage_id"]) if rows else str(uuid4())
    duplicate_ids = [str(row["usage_id"]) for row in rows[1:] if row.get("usage_id")]

    if duplicate_ids:
        if link_table is not None:
            db.execute(
                link_table.update()
                .where(link_table.c.usage_id.in_(duplicate_ids))
                .values(usage_id=usage_id)
            )
            _dedupe_report_usage_links(db, usage_id=usage_id)
        if fact_table is not None:
            db.execute(
                fact_table.delete().where(fact_table.c.usage_id.in_(duplicate_ids))
            )
        db.execute(
            usage_table.delete().where(usage_table.c.usage_id.in_(duplicate_ids))
        )

    usage_values = {
        "trade_date": trade_day,
        "stock_code": stock_code,
        "dataset_name": dataset_name,
        "source_name": source_name,
        "batch_id": batch_id,
        "fetch_time": fetch_dt,
        "status": status,
        "status_reason": status_reason,
        "created_at": created_dt,
    }
    if rows:
        db.execute(
            usage_table.update()
            .where(usage_table.c.usage_id == usage_id)
            .values(**usage_values)
        )
    else:
        db.execute(
            usage_table.insert().values(
                usage_id=usage_id,
                **usage_values,
            )
        )

    if fact_table is not None:
        fact_values = {
            "batch_id": batch_id,
            "trade_date": trade_day.isoformat(),
            "stock_code": stock_code,
            "source_name": source_name,
            "fetch_time": fetch_dt.isoformat(),
            "status": status,
            "status_reason": status_reason,
            "created_at": created_dt.isoformat(),
        }
        existing_fact = db.execute(
            select(fact_table.c.usage_id).where(fact_table.c.usage_id == usage_id)
        ).mappings().first()
        if existing_fact:
            db.execute(
                fact_table.update()
                .where(fact_table.c.usage_id == usage_id)
                .values(**fact_values)
            )
        else:
            db.execute(
                fact_table.insert().values(
                    usage_id=usage_id,
                    **fact_values,
                )
            )

    return usage_id


def infer_usage_batch_id(
    db: Session,
    *,
    trade_date: date | str,
    dataset_name: str,
    fallback_trade_date: date | str | None = None,
) -> str | None:
    for candidate in (trade_date, fallback_trade_date):
        if not candidate:
            continue
        row = db.execute(
            text(
                """
                SELECT batch_id
                FROM report_data_usage
                WHERE trade_date = :trade_date
                  AND dataset_name = :dataset_name
                  AND batch_id IS NOT NULL
                  AND batch_id <> ''
                GROUP BY batch_id
                ORDER BY COUNT(*) DESC, MAX(fetch_time) DESC, batch_id DESC
                LIMIT 1
                """
            ),
            {
                "trade_date": _date_value(candidate),
                "dataset_name": dataset_name,
            },
        ).mappings().first()
        if row and row.get("batch_id"):
            return str(row["batch_id"])
    return None


def _build_usage_scope(
    *,
    dataset_names: list[str],
    trade_date: date | str | None = None,
    stock_codes: list[str] | None = None,
) -> tuple[str, dict[str, Any]]:
    params: dict[str, Any] = {}
    clauses: list[str] = []

    dataset_filters = [str(name or "").strip() for name in dataset_names if str(name or "").strip()]
    if not dataset_filters:
        raise ValueError("dataset_names must not be empty")
    dataset_placeholders: list[str] = []
    for index, dataset_name in enumerate(dataset_filters):
        key = f"dataset_name_{index}"
        params[key] = dataset_name
        dataset_placeholders.append(f":{key}")
    clauses.append(f"u.dataset_name IN ({', '.join(dataset_placeholders)})")

    if trade_date is not None:
        params["trade_date"] = _date_value(trade_date)
        clauses.append("u.trade_date = :trade_date")

    normalized_stock_codes = [str(code or "").strip() for code in (stock_codes or []) if str(code or "").strip()]
    if normalized_stock_codes:
        stock_placeholders: list[str] = []
        for index, stock_code in enumerate(normalized_stock_codes):
            key = f"stock_code_{index}"
            params[key] = stock_code
            stock_placeholders.append(f":{key}")
        clauses.append(f"u.stock_code IN ({', '.join(stock_placeholders)})")

    return " AND ".join(clauses), params


def repair_usage_lineage(
    db: Session,
    *,
    dataset_names: list[str],
    trade_date: date | str | None = None,
    stock_codes: list[str] | None = None,
    fact_insert_batch_size: int = 1000,
) -> dict[str, int]:
    usage_table = Base.metadata.tables["report_data_usage"]
    fact_table = Base.metadata.tables.get("data_usage_fact")

    scope_sql, scope_params = _build_usage_scope(
        dataset_names=dataset_names,
        trade_date=trade_date,
        stock_codes=stock_codes,
    )

    duplicate_groups = db.execute(
        text(
            f"""
            SELECT u.trade_date, u.stock_code, u.dataset_name
            FROM report_data_usage u
            WHERE {scope_sql}
            GROUP BY u.trade_date, u.stock_code, u.dataset_name
            HAVING COUNT(*) > 1
            ORDER BY u.trade_date ASC, u.stock_code ASC, u.dataset_name ASC
            """
        ),
        scope_params,
    ).mappings().all()

    repaired_duplicate_groups = 0
    for group in duplicate_groups:
        canonical_row = db.execute(
            text(
                """
                SELECT
                    u.source_name,
                    u.batch_id,
                    u.fetch_time,
                    u.status,
                    u.status_reason,
                    u.created_at
                FROM report_data_usage u
                WHERE u.trade_date = :trade_date
                  AND u.stock_code = :stock_code
                  AND u.dataset_name = :dataset_name
                ORDER BY
                  COALESCE((
                      SELECT COUNT(*)
                      FROM report_data_usage_link l
                      WHERE l.usage_id = u.usage_id
                  ), 0) DESC,
                  CASE WHEN EXISTS(
                      SELECT 1 FROM data_usage_fact f WHERE f.usage_id = u.usage_id
                  ) THEN 1 ELSE 0 END DESC,
                  u.fetch_time DESC,
                  u.created_at DESC,
                  u.usage_id DESC
                LIMIT 1
                """
            ),
            {
                "trade_date": _date_value(group["trade_date"]),
                "stock_code": str(group["stock_code"]),
                "dataset_name": str(group["dataset_name"]),
            },
        ).mappings().first()
        if not canonical_row:
            continue

        stable_upsert_usage_row(
            db,
            trade_date=group["trade_date"],
            stock_code=str(group["stock_code"]),
            dataset_name=str(group["dataset_name"]),
            source_name=str(canonical_row.get("source_name") or ""),
            batch_id=str(canonical_row.get("batch_id") or ""),
            fetch_time=canonical_row.get("fetch_time"),
            status=str(canonical_row.get("status") or "missing"),
            status_reason=canonical_row.get("status_reason"),
            created_at=canonical_row.get("created_at"),
        )
        repaired_duplicate_groups += 1

    if fact_table is None:
        return {
            "duplicate_groups_repaired": repaired_duplicate_groups,
            "fact_rows_backfilled": 0,
        }

    missing_fact_rows = db.execute(
        text(
            f"""
            SELECT
                u.usage_id,
                u.batch_id,
                u.trade_date,
                u.stock_code,
                u.source_name,
                u.fetch_time,
                u.status,
                u.status_reason,
                u.created_at
            FROM report_data_usage u
            LEFT JOIN data_usage_fact f ON f.usage_id = u.usage_id
            WHERE {scope_sql}
              AND f.usage_id IS NULL
            ORDER BY u.trade_date ASC, u.stock_code ASC, u.dataset_name ASC, u.fetch_time DESC, u.created_at DESC, u.usage_id DESC
            """
        ),
        scope_params,
    ).mappings().all()

    fact_payloads = [
        {
            "usage_id": str(row["usage_id"]),
            "batch_id": str(row.get("batch_id") or ""),
            "trade_date": _date_value(row["trade_date"]).isoformat(),
            "stock_code": str(row["stock_code"]),
            "source_name": str(row.get("source_name") or ""),
            "fetch_time": _naive_utc(row.get("fetch_time")).isoformat(),
            "status": str(row.get("status") or "missing"),
            "status_reason": row.get("status_reason"),
            "created_at": _naive_utc(row.get("created_at") or row.get("fetch_time")).isoformat(),
        }
        for row in missing_fact_rows
    ]

    for start in range(0, len(fact_payloads), max(1, int(fact_insert_batch_size or 1))):
        db.execute(fact_table.insert(), fact_payloads[start:start + max(1, int(fact_insert_batch_size or 1))])

    return {
        "duplicate_groups_repaired": repaired_duplicate_groups,
        "fact_rows_backfilled": len(fact_payloads),
    }