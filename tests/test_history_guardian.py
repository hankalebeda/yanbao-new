from __future__ import annotations

from datetime import date

import pytest

from app.services import history_guardian
from tests.helpers_ssot import insert_report_bundle_ssot


pytestmark = [pytest.mark.feature("FR06-LLM-01")]


def test_history_guardian_ok_report_codes_excludes_stale_ok(db_session, monkeypatch):
    trade_date = "2026-03-06"
    insert_report_bundle_ssot(db_session, stock_code="600519.SH", trade_date=trade_date, quality_flag="ok")
    insert_report_bundle_ssot(db_session, stock_code="000001.SZ", trade_date=trade_date, quality_flag="stale_ok")

    monkeypatch.setattr(history_guardian, "SessionLocal", lambda: db_session)

    report_codes = history_guardian._ok_report_codes_for_trade_date(
        trade_date,
        ["600519.SH", "000001.SZ"],
    )

    assert report_codes == {"600519.SH"}


def test_history_guardian_published_non_ok_count_includes_stale_ok(db_session, monkeypatch):
    trade_date = "2026-03-06"
    insert_report_bundle_ssot(db_session, stock_code="600519.SH", trade_date=trade_date, quality_flag="ok")
    insert_report_bundle_ssot(db_session, stock_code="000001.SZ", trade_date=trade_date, quality_flag="stale_ok")
    insert_report_bundle_ssot(db_session, stock_code="000002.SZ", trade_date=trade_date, quality_flag="degraded")

    monkeypatch.setattr(history_guardian, "SessionLocal", lambda: db_session)

    assert history_guardian._published_non_ok_count() == 2


def test_history_guardian_run_one_cycle_treats_stale_ok_as_missing_and_enables_non_ok_cleanup(monkeypatch):
    cleanup_calls: list[dict] = []
    generated_batches: list[list[str]] = []

    class DummySession:
        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    def fake_session_local():
        return DummySession()

    def fake_cleanup_incomplete_reports(db, *, limit, dry_run, include_non_ok):
        cleanup_calls.append(
            {
                "kind": "single",
                "limit": limit,
                "dry_run": dry_run,
                "include_non_ok": include_non_ok,
            }
        )
        return {"candidates": 1, "scanned": 2}

    def fake_cleanup_incomplete_reports_until_clean(db, *, batch_limit, max_batches, dry_run, include_non_ok):
        cleanup_calls.append(
            {
                "kind": "all",
                "batch_limit": batch_limit,
                "max_batches": max_batches,
                "dry_run": dry_run,
                "include_non_ok": include_non_ok,
            }
        )
        return {"total_soft_deleted": 1, "remaining_candidates": 0}

    monkeypatch.setattr(history_guardian, "SessionLocal", fake_session_local)
    monkeypatch.setattr(history_guardian, "_resolve_target_trade_date", lambda: ("2026-03-06", "2026-03-06", "latest_complete_public_batch"))
    monkeypatch.setattr(history_guardian, "get_daily_stock_pool", lambda **kwargs: ["600519.SH", "000001.SZ"])
    monkeypatch.setattr(history_guardian, "_ok_report_codes_for_trade_date", lambda trade_date, stock_codes: {"600519.SH"})
    monkeypatch.setattr(history_guardian, "cleanup_incomplete_reports", fake_cleanup_incomplete_reports)
    monkeypatch.setattr(history_guardian, "cleanup_incomplete_reports_until_clean", fake_cleanup_incomplete_reports_until_clean)
    monkeypatch.setattr(
        history_guardian,
        "generate_reports_batch",
        lambda **kwargs: generated_batches.append(list(kwargs["stock_codes"])) or {"succeeded": 1, "failed": 0},
    )
    monkeypatch.setattr(
        history_guardian,
        "rebuild_fr07_snapshot",
        lambda db, trade_day, window_days, purge_invalid: {"window_days": window_days, "purge_invalid": purge_invalid},
    )
    monkeypatch.setattr(history_guardian, "_published_non_ok_count", lambda: 3)

    result = history_guardian._run_one_cycle(batch_size=20)

    assert result["missing_reports_for_trade_date"] == 1
    assert result["published_non_ok_total"] == 3
    assert result["cleanup"]["include_non_ok"] is True
    assert cleanup_calls[0] == {
        "kind": "single",
        "limit": 5000,
        "dry_run": True,
        "include_non_ok": True,
    }
    assert cleanup_calls[1] == {
        "kind": "all",
        "batch_limit": 500,
        "max_batches": 50,
        "dry_run": False,
        "include_non_ok": True,
    }
    assert generated_batches == [["000001.SZ"]]
    assert result["fr07_snapshots"] == [
        {"window_days": 1, "purge_invalid": True},
        {"window_days": 7, "purge_invalid": True},
        {"window_days": 14, "purge_invalid": True},
        {"window_days": 30, "purge_invalid": True},
        {"window_days": 60, "purge_invalid": True},
    ]


def test_history_guardian_run_one_cycle_caps_generation_to_five_per_round(monkeypatch):
    generated_batches: list[list[str]] = []

    class DummySession:
        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(history_guardian, "SessionLocal", lambda: DummySession())
    monkeypatch.setattr(history_guardian, "_resolve_target_trade_date", lambda: ("2026-03-06", "2026-03-06", "latest_complete_public_batch"))
    monkeypatch.setattr(
        history_guardian,
        "get_daily_stock_pool",
        lambda **kwargs: [
            "000001.SZ",
            "000002.SZ",
            "000003.SZ",
            "000004.SZ",
            "000005.SZ",
            "000006.SZ",
            "000007.SZ",
        ],
    )
    monkeypatch.setattr(history_guardian, "_ok_report_codes_for_trade_date", lambda trade_date, stock_codes: set())
    monkeypatch.setattr(history_guardian, "cleanup_incomplete_reports", lambda *args, **kwargs: {"candidates": 0, "scanned": 0})
    monkeypatch.setattr(
        history_guardian,
        "cleanup_incomplete_reports_until_clean",
        lambda *args, **kwargs: {"total_soft_deleted": 0, "remaining_candidates": 0},
    )
    monkeypatch.setattr(
        history_guardian,
        "generate_reports_batch",
        lambda **kwargs: generated_batches.append(list(kwargs["stock_codes"])) or {"succeeded": len(kwargs["stock_codes"]), "failed": 0},
    )
    monkeypatch.setattr(
        history_guardian,
        "rebuild_fr07_snapshot",
        lambda db, trade_day, window_days, purge_invalid: {"window_days": window_days, "purge_invalid": purge_invalid},
    )
    monkeypatch.setattr(history_guardian, "_published_non_ok_count", lambda: 0)

    result = history_guardian._run_one_cycle(batch_size=20)

    assert generated_batches == [["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"]]
    assert result["generation"]["requested"] == 7
    assert result["generation"]["scheduled_this_cycle"] == 5
    assert result["generation"]["deferred_due_to_round_limit"] == 2
    assert result["generation"]["round_limit"] == 5