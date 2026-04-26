from sqlalchemy import text

from tests.helpers_ssot import (
    insert_market_state_cache,
    insert_open_position,
    insert_pool_snapshot,
    insert_report_bundle_ssot,
    insert_stock_master,
)


def test_seed_helpers_report_bundle_creates_linked_data_batch(db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
    )

    orphan_count = db_session.execute(
        text(
            "SELECT COUNT(*) "
            "FROM report_data_usage u "
            "JOIN report_data_usage_link l ON l.usage_id = u.usage_id "
            "WHERE l.report_id = :report_id "
            "AND u.batch_id NOT IN (SELECT batch_id FROM data_batch)"
        ),
        {"report_id": report.report_id},
    ).scalar()

    assert orphan_count == 0


def test_seed_helpers_market_state_cache_same_day_is_upsert(db_session):
    insert_market_state_cache(db_session, trade_date="2026-03-06", market_state="BULL")
    insert_market_state_cache(db_session, trade_date="2026-03-06", market_state="BEAR")

    row = db_session.execute(
        text(
            "SELECT market_state FROM market_state_cache WHERE trade_date = :trade_date"
        ),
        {"trade_date": "2026-03-06"},
    ).fetchone()

    assert row is not None
    assert row[0] == "BEAR"


def test_seed_helpers_open_position_auto_creates_sim_account(db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
    )

    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code=report.stock_code,
        capital_tier="100k",
        signal_date="2026-03-06",
        entry_date="2026-03-06",
        actual_entry_price=123.45,
        signal_entry_price=123.45,
        position_ratio=0.2,
        shares=100,
    )

    account_count = db_session.execute(
        text("SELECT COUNT(*) FROM sim_account WHERE capital_tier = :capital_tier"),
        {"capital_tier": "100k"},
    ).scalar()

    assert account_count == 1


def test_seed_helpers_report_bundle_uses_requested_pool_version(db_session):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="MOUTAI")

    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
        pool_version=2,
    )

    row = db_session.execute(
        text("SELECT pool_version FROM report WHERE report_id = :report_id"),
        {"report_id": report.report_id},
    ).fetchone()

    assert row is not None
    assert row[0] == 2


def test_seed_helpers_second_stock_same_day_gets_snapshot_bound_refresh_truth(db_session):
    first = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2026-03-06",
    )
    second = insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="PINGAN",
        trade_date="2026-03-06",
    )

    row = db_session.execute(
        text(
            """
            SELECT r.report_id, r.pool_version, t.refresh_task_id, s.pool_snapshot_id, s.pool_version AS snapshot_pool_version
            FROM report r
            JOIN report_generation_task t ON t.task_id = r.generation_task_id
            LEFT JOIN stock_pool_snapshot s
              ON s.refresh_task_id = t.refresh_task_id
             AND s.trade_date = r.trade_date
             AND s.stock_code = r.stock_code
            WHERE r.report_id = :report_id
            """
        ),
        {"report_id": second.report_id},
    ).mappings().one()

    assert first.trade_date == second.trade_date
    assert row["refresh_task_id"] is not None
    assert row["pool_snapshot_id"] is not None
    assert row["snapshot_pool_version"] == row["pool_version"]
