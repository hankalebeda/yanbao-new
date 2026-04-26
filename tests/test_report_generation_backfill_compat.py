from datetime import date, datetime, timedelta, timezone

from app.models import Base
from app.services import report_generation_ssot
from tests.helpers_ssot import insert_pool_snapshot, insert_report_bundle_ssot


def test_resolve_refresh_context_supports_disabling_previous_day_fallback(db_session):
    task_id = insert_pool_snapshot(
        db_session,
        trade_date="2026-03-05",
        stock_codes=["600519.SH"],
        pool_version=7,
    )

    strict_context = report_generation_ssot.resolve_refresh_context(
        db_session,
        trade_day=date(2026, 3, 6),
        stock_code="600519.SH",
        allow_same_day_fallback=False,
    )
    assert strict_context is None

    fallback_context = report_generation_ssot.resolve_refresh_context(
        db_session,
        trade_day=date(2026, 3, 6),
        stock_code="600519.SH",
    )
    assert fallback_context == {"task_id": task_id, "pool_version": 7}
    assert report_generation_ssot._load_pool_version_for_refresh_task(
        db_session,
        refresh_task_id=task_id,
    ) == 7


def test_market_state_usage_and_report_link_helpers_are_idempotent(db_session):
    report = insert_report_bundle_ssot(
        db_session,
        trade_date="2026-03-06",
        stock_code="600519.SH",
        stock_name="贵州茅台",
        pool_version=3,
    )
    market_state_table = Base.metadata.tables["market_state_cache"]
    link_table = Base.metadata.tables["report_data_usage_link"]
    lineage_table = Base.metadata.tables["data_batch_lineage"]

    market_state_row = db_session.execute(
        market_state_table.select().where(market_state_table.c.trade_date == date(2026, 3, 6))
    ).mappings().first()
    assert market_state_row is not None

    first_usage = report_generation_ssot._ensure_market_state_input_usage(
        db_session,
        stock_code="600519.SH",
        report_trade_day=date(2026, 3, 6),
        market_state_row=dict(market_state_row),
    )
    second_usage = report_generation_ssot._ensure_market_state_input_usage(
        db_session,
        stock_code="600519.SH",
        report_trade_day=date(2026, 3, 6),
        market_state_row=dict(market_state_row),
    )
    assert second_usage["usage_id"] == first_usage["usage_id"]

    lineage_rows = db_session.execute(
        lineage_table.select().where(lineage_table.c.child_batch_id == first_usage["batch_id"])
    ).mappings().all()
    assert {str(row["parent_batch_id"]) for row in lineage_rows} == {
        str(market_state_row["kline_batch_id"]),
        str(market_state_row["hotspot_batch_id"]),
    }

    db_session.execute(
        link_table.delete().where(
            link_table.c.report_id == report.report_id,
            link_table.c.usage_id == first_usage["usage_id"],
        )
    )
    db_session.commit()

    created_at = datetime(2026, 3, 6, 15, 0, tzinfo=timezone.utc)
    inserted_link = report_generation_ssot._ensure_report_usage_link(
        db_session,
        report_id=report.report_id,
        usage_id=first_usage["usage_id"],
        created_at=created_at,
    )
    reused_link = report_generation_ssot._ensure_report_usage_link(
        db_session,
        report_id=report.report_id,
        usage_id=first_usage["usage_id"],
        created_at=created_at + timedelta(minutes=1),
    )
    db_session.commit()

    links = db_session.execute(
        link_table.select().where(
            link_table.c.report_id == report.report_id,
            link_table.c.usage_id == first_usage["usage_id"],
        )
    ).mappings().all()
    assert len(links) == 1
    assert inserted_link["report_id"] == report.report_id
    assert reused_link["report_data_usage_link_id"] == links[0]["report_data_usage_link_id"]


def test_build_citations_accepts_backfill_signature(db_session):
    kline_row = {"open": 123.4, "close": 125.6, "high": 126.0, "low": 122.8}
    used_data = [
        {
            "usage_id": "ms-1",
            "stock_code": "600519.SH",
            "dataset_name": "market_state_input",
            "source_name": "market_state_cache",
            "batch_id": "batch-ms",
            "trade_date": date(2026, 3, 6),
            "fetch_time": datetime(2026, 3, 6, 15, 1, tzinfo=timezone.utc),
            "status": "ok",
            "status_reason": None,
        },
        {
            "usage_id": "kline-1",
            "stock_code": "600519.SH",
            "dataset_name": "kline_daily",
            "source_name": "tdx_local",
            "batch_id": "batch-kline",
            "trade_date": date(2026, 3, 6),
            "fetch_time": datetime(2026, 3, 6, 15, 5, tzinfo=timezone.utc),
            "status": "ok",
            "status_reason": None,
        },
    ]

    sorted_usage = report_generation_ssot._sort_used_data(list(reversed(used_data)))
    assert [item["usage_id"] for item in sorted_usage] == ["kline-1", "ms-1"]

    old_style = report_generation_ssot._build_citations(
        db_session,
        used_data=used_data,
        stock_name="贵州茅台",
        trade_day=date(2026, 3, 6),
        kline_row=kline_row,
        market_state_row={"market_state": "BULL", "reference_date": "2026-03-06"},
    )
    modern_style = report_generation_ssot._build_citations(used_data, kline_row=kline_row)

    assert [citation["source_name"] for citation in old_style] == ["tdx_local", "market_state_cache"]
    assert "开盘" in old_style[0]["excerpt"]
    assert "市场状态" in old_style[1]["excerpt"]
    assert modern_style[0]["source_name"] == "tdx_local"
