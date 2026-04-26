from sqlalchemy import text

from app.core.db import build_engine, ensure_sqlite_schema_alignment


def test_report_data_usage_status_alignment_migrates_legacy_sqlite(tmp_path):
    db_path = tmp_path / "legacy-status.db"
    engine = build_engine(f"sqlite:///{db_path.as_posix()}")
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE report_data_usage (
                        usage_id VARCHAR(36) PRIMARY KEY,
                        trade_date DATE NOT NULL,
                        stock_code VARCHAR(16) NOT NULL,
                        dataset_name VARCHAR(32) NOT NULL,
                        source_name VARCHAR(32) NOT NULL,
                        batch_id VARCHAR(36) NOT NULL,
                        fetch_time DATETIME NOT NULL,
                        status VARCHAR(16) NOT NULL DEFAULT 'ok',
                        status_reason TEXT,
                        created_at DATETIME NOT NULL,
                        CONSTRAINT ck_report_data_usage_status_enum CHECK (
                            status IN ('ok', 'stale_ok', 'missing', 'degraded')
                        )
                    )
                    """
                )
            )
            conn.execute(text("CREATE INDEX ix_report_data_usage_trade_date ON report_data_usage (trade_date)"))
            conn.execute(text("CREATE INDEX ix_report_data_usage_stock_code ON report_data_usage (stock_code)"))
            conn.execute(
                text(
                    """
                    INSERT INTO report_data_usage (
                        usage_id, trade_date, stock_code, dataset_name, source_name,
                        batch_id, fetch_time, status, status_reason, created_at
                    ) VALUES (
                        'usage-ok', '2026-04-24', '000001.SZ', 'stock_profile', 'eastmoney',
                        'batch-ok', '2026-04-24 00:00:00', 'ok', NULL, '2026-04-24 00:00:00'
                    )
                    """
                )
            )

        ensure_sqlite_schema_alignment(engine)

        with engine.begin() as conn:
            table_sql = conn.execute(
                text("SELECT sql FROM sqlite_master WHERE type='table' AND name='report_data_usage'")
            ).scalar_one()
            lowered = table_sql.lower()
            assert "proxy_ok" in lowered
            assert "realtime_only" in lowered

            conn.execute(
                text(
                    """
                    INSERT INTO report_data_usage (
                        usage_id, trade_date, stock_code, dataset_name, source_name,
                        batch_id, fetch_time, status, status_reason, created_at
                    ) VALUES (
                        'usage-proxy', '2026-04-24', '000002.SZ', 'main_force_flow', 'eastmoney',
                        'batch-proxy', '2026-04-24 00:00:00', 'proxy_ok', NULL, '2026-04-24 00:00:00'
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO report_data_usage (
                        usage_id, trade_date, stock_code, dataset_name, source_name,
                        batch_id, fetch_time, status, status_reason, created_at
                    ) VALUES (
                        'usage-realtime', '2026-04-24', '000003.SZ', 'margin_financing', 'eastmoney',
                        'batch-realtime', '2026-04-24 00:00:00', 'realtime_only', NULL, '2026-04-24 00:00:00'
                    )
                    """
                )
            )
            statuses = conn.execute(
                text("SELECT status FROM report_data_usage ORDER BY usage_id")
            ).scalars().all()
            assert statuses == ["ok", "proxy_ok", "realtime_only"]

            index_names = {
                row[1]
                for row in conn.execute(text("PRAGMA index_list('report_data_usage')")).fetchall()
            }
            assert "ix_report_data_usage_trade_date" in index_names
            assert "ix_report_data_usage_stock_code" in index_names
    finally:
        engine.dispose()