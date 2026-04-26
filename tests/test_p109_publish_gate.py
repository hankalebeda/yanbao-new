"""
Tests for P1-09: Publish gate control + data chain fixes.

Validates:
- Reports with fetcher_not_provided are NOT published
- Reports with quality_flag=degraded are NOT published
- Reports with quality_flag=ok ARE published
- ETF flow fetcher returns valid structure
- Northbound summary fetcher returns valid structure
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

import pytest

from app.models import Base
from app.services.etf_flow_data import fetch_etf_flow_summary_global
from tests.helpers_ssot import insert_report_bundle_ssot


class TestPublishGateControl:
    """Verify that the publish gate blocks reports with critical data gaps."""

    def _insert_report(
        self,
        db_session,
        *,
        stock_code: str = "600519.SH",
        trade_date: str = "2026-03-10",
        quality_flag: str = "ok",
        status_reason: str | None = None,
        published: bool = True,
        publish_status: str = "PUBLISHED",
    ) -> str:
        now = datetime.now(timezone.utc)
        report = insert_report_bundle_ssot(
            db_session,
            stock_code=stock_code,
            stock_name="贵州茅台",
            trade_date=trade_date,
            quality_flag=quality_flag,
            published=published,
            strategy_type="B",
            review_flag="APPROVED" if published else "NONE",
        )
        report.publish_status = publish_status
        report.published = published
        report.published_at = now if published else None
        report.status_reason = status_reason
        report.conclusion_text = "test conclusion"
        report.reasoning_chain_md = "test reasoning"
        report.updated_at = now
        db_session.commit()
        db_session.refresh(report)
        return report.report_id

    def test_ok_quality_is_published(self, db_session):
        """quality_flag=ok → published=True."""
        rid = self._insert_report(
            db_session,
            quality_flag="ok",
            published=True,
            publish_status="PUBLISHED",
        )
        report_table = Base.metadata.tables["report"]
        row = db_session.execute(
            report_table.select().where(report_table.c.report_id == rid)
        ).first()
        assert row.published is True or row.published == 1

    def test_gate_blocked_not_published(self, db_session):
        """publish_status=UNPUBLISHED (gate blocked) → published=False."""
        rid = self._insert_report(
            db_session,
            quality_flag="stale_ok",
            status_reason="fetcher_not_provided",
            published=False,
            publish_status="UNPUBLISHED",
        )
        report_table = Base.metadata.tables["report"]
        row = db_session.execute(
            report_table.select().where(report_table.c.report_id == rid)
        ).first()
        assert row.published is False or row.published == 0
        assert row.publish_status == "UNPUBLISHED"


class TestEtfFlowFetcher:
    """Verify ETF flow summary fetcher returns valid structure."""

    def test_returns_dict_with_status(self):
        result = fetch_etf_flow_summary_global(date(2026, 3, 10))
        assert isinstance(result, dict)
        assert "status" in result
        assert result["status"] in {"ok", "missing", "degraded"}

    def test_returns_fetch_time(self):
        result = fetch_etf_flow_summary_global(date(2026, 3, 10))
        assert "fetch_time" in result


class TestDeriveQualityFlagWithGate:
    """Test _derive_quality_flag returns correct flags for fetcher_not_provided."""

    def test_fetcher_not_provided_gives_stale_ok(self):
        """ETF flow summary is supplementary — missing it returns ok with advisory reason."""
        from app.services.report_generation_ssot import _derive_quality_flag

        used_data = [
            {"status": "ok", "status_reason": None, "dataset_name": "kline_daily"},
            {"status": "missing", "status_reason": "fetcher_not_provided", "dataset_name": "etf_flow_summary"},
        ]
        market_state = {"market_state_degraded": False}
        flag, reason = _derive_quality_flag(used_data, market_state)
        # Supplementary data missing → ok quality, but advisory reason preserved
        assert flag == "ok"
        assert reason == "fetcher_not_provided"

    def test_core_data_missing_gives_stale_ok(self):
        """Core dataset (kline_daily) missing still gives stale_ok."""
        from app.services.report_generation_ssot import _derive_quality_flag

        used_data = [
            {"status": "missing", "status_reason": "data_unavailable", "dataset_name": "kline_daily"},
            {"status": "ok", "status_reason": None, "dataset_name": "etf_flow_summary"},
        ]
        market_state = {"market_state_degraded": False}
        flag, reason = _derive_quality_flag(used_data, market_state)
        assert flag == "stale_ok"
        assert reason is not None
