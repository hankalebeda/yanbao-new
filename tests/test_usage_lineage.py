from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

from app.models import Base
from app.services.usage_lineage import repair_usage_lineage


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def test_repair_usage_lineage_backfills_fact_and_collapses_duplicates(db_session):
    usage_table = Base.metadata.tables["report_data_usage"]
    fact_table = Base.metadata.tables["data_usage_fact"]

    canonical_usage_id = str(uuid4())
    duplicate_usage_id = str(uuid4())
    single_usage_id = str(uuid4())
    trade_day = date(2026, 4, 24)
    earlier = _now_utc()
    later = earlier.replace(microsecond=min(earlier.microsecond + 1, 999999))

    db_session.execute(
        usage_table.insert().values(
            usage_id=canonical_usage_id,
            trade_date=trade_day,
            stock_code="600519.SH",
            dataset_name="hotspot_top50",
            source_name="eastmoney",
            batch_id="batch-hotspot-1",
            fetch_time=earlier,
            status="missing",
            status_reason="no_hotspot_match",
            created_at=earlier,
        )
    )
    db_session.execute(
        usage_table.insert().values(
            usage_id=duplicate_usage_id,
            trade_date=trade_day,
            stock_code="600519.SH",
            dataset_name="hotspot_top50",
            source_name="eastmoney",
            batch_id="batch-hotspot-2",
            fetch_time=later,
            status="ok",
            status_reason=None,
            created_at=later,
        )
    )
    db_session.execute(
        usage_table.insert().values(
            usage_id=single_usage_id,
            trade_date=trade_day,
            stock_code="000001.SZ",
            dataset_name="northbound_summary",
            source_name="northbound_summary",
            batch_id="batch-northbound-1",
            fetch_time=earlier,
            status="missing",
            status_reason="northbound_data_unavailable",
            created_at=earlier,
        )
    )
    db_session.commit()

    result = repair_usage_lineage(
        db_session,
        dataset_names=["hotspot_top50", "northbound_summary"],
        trade_date=trade_day,
    )
    db_session.commit()

    hotspot_rows = db_session.execute(
        usage_table.select().where(
            usage_table.c.trade_date == trade_day,
            usage_table.c.stock_code == "600519.SH",
            usage_table.c.dataset_name == "hotspot_top50",
        )
    ).mappings().all()
    fact_rows = db_session.execute(
        fact_table.select().where(
            fact_table.c.usage_id.in_([canonical_usage_id, duplicate_usage_id, single_usage_id])
        )
    ).mappings().all()

    assert result == {"duplicate_groups_repaired": 1, "fact_rows_backfilled": 1}
    assert len(hotspot_rows) == 1
    assert hotspot_rows[0]["usage_id"] == duplicate_usage_id
    assert hotspot_rows[0]["status"] == "ok"
    assert {row["usage_id"] for row in fact_rows} == {duplicate_usage_id, single_usage_id}