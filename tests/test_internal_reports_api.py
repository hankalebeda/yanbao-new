from __future__ import annotations

from uuid import uuid4

from tests.helpers_ssot import seed_generation_context


def test_internal_reports_generate_requires_internal_token(client):
    response = client.post(
        "/api/v1/internal/reports/generate",
        json={"stock_code": "600519.SH"},
    )

    assert response.status_code == 401
    assert response.json()["error_code"] == "UNAUTHORIZED"


def test_internal_reports_generate_route_is_retired(client, internal_headers):
    response = client.post(
        "/api/v1/internal/reports/generate",
        json={
            "stock_code": "600519.SH",
            "trade_date": "2026-03-26",
            "idempotency_key": "daily:600519.SH:2026-03-26",
            "force": False,
        },
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 410
    assert response.json()["error_code"] == "ROUTE_RETIRED"


def test_internal_reports_task_status_returns_current_task(
    client,
    internal_headers,
    seed_report_bundle,
):
    report = seed_report_bundle()

    response = client.get(
        f"/api/v1/internal/reports/tasks/{report.generation_task_id}",
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["task_id"] == report.generation_task_id
    assert data["status"] == "Completed"
    assert data["retry_count"] == 0
    assert data["llm_fallback_level"] == "primary"
    assert data["risk_audit_status"] == "completed"
    assert data["report_id"] == report.report_id


def test_internal_reports_task_status_not_found(client, internal_headers):
    response = client.get(
        f"/api/v1/internal/reports/tasks/{uuid4()}",
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 404
    assert response.json()["error_code"] == "TASK_NOT_FOUND"


def test_internal_reports_generate_batch_requires_non_empty_stock_codes(client, internal_headers):
    response = client.post(
        "/api/v1/internal/reports/generate-batch",
        json={"stock_codes": []},
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 400
    assert "INVALID_PAYLOAD" in response.json()["error_code"]


def test_internal_reports_generate_batch_rejects_more_than_five_codes(client, internal_headers):
    response = client.post(
        "/api/v1/internal/reports/generate-batch",
        json={
            "stock_codes": [
                "600519.SH",
                "000001.SZ",
                "300750.SZ",
                "000858.SZ",
                "688001.SH",
                "688002.SH",
            ]
        },
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 400
    body = response.json()
    assert body["error_code"] == "INVALID_PAYLOAD"
    assert "max 5 stock_codes per round" in response.text


def test_internal_reports_generate_batch_supports_optional_pre_cleanup(client, internal_headers, monkeypatch):
    import app.services.report_generation_ssot as report_generation_ssot

    monkeypatch.setattr(
        report_generation_ssot,
        "cleanup_incomplete_reports",
        lambda db, limit=500, include_non_ok=True: {
            "scanned": 12,
            "soft_deleted": 3,
            "deleted_report_ids": ["r1", "r2", "r3"],
            "reason": "REPORT_DATA_INCOMPLETE",
            "limit": limit,
            "include_non_ok": include_non_ok,
        },
        raising=False,
    )
    monkeypatch.setattr(
        report_generation_ssot,
        "generate_reports_batch",
        lambda **kwargs: {
            "accepted": len(kwargs.get("stock_codes") or []),
            "trade_date": kwargs.get("trade_date"),
        },
        raising=False,
    )

    response = client.post(
        "/api/v1/internal/reports/generate-batch",
        json={
            "stock_codes": ["600519.SH", "000001.SZ"],
            "trade_date": "2026-04-14",
            "cleanup_incomplete_before_batch": True,
            "cleanup_limit": 321,
        },
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 202
    data = response.json()["data"]
    assert data["accepted"] == 2
    assert data["trade_date"] == "2026-04-14"
    assert data["cleanup_incomplete_before_batch"]["soft_deleted"] == 3
    assert data["cleanup_incomplete_before_batch"]["reason"] == "REPORT_DATA_INCOMPLETE"
    assert data["cleanup_incomplete_before_batch"]["include_non_ok"] is True


def test_internal_reports_generate_batch_defaults_to_pre_cleanup_enabled(client, internal_headers, monkeypatch):
    import app.services.report_generation_ssot as report_generation_ssot

    call_state = {"cleanup_called": 0}

    def _cleanup(db, limit=500, include_non_ok=True):
        call_state["cleanup_called"] += 1
        return {
            "scanned": 5,
            "soft_deleted": 1,
            "deleted_report_ids": ["r-default"],
            "reason": "REPORT_DATA_INCOMPLETE",
            "limit": limit,
            "include_non_ok": include_non_ok,
        }

    monkeypatch.setattr(report_generation_ssot, "cleanup_incomplete_reports", _cleanup, raising=False)
    monkeypatch.setattr(
        report_generation_ssot,
        "generate_reports_batch",
        lambda **kwargs: {
            "accepted": len(kwargs.get("stock_codes") or []),
            "trade_date": kwargs.get("trade_date"),
        },
        raising=False,
    )

    response = client.post(
        "/api/v1/internal/reports/generate-batch",
        json={
            "stock_codes": ["600519.SH"],
            "trade_date": "2026-04-14",
            "cleanup_limit": 123,
        },
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 202
    data = response.json()["data"]
    assert call_state["cleanup_called"] == 1
    assert data["cleanup_incomplete_before_batch"]["soft_deleted"] == 1
    assert data["cleanup_incomplete_before_batch"]["limit"] == 123
    assert data["cleanup_incomplete_before_batch"]["include_non_ok"] is True


def test_internal_reports_generate_batch_always_enforces_one_per_strategy_type(client, internal_headers, monkeypatch):
    import app.services.report_generation_ssot as report_generation_ssot

    captured: dict[str, object] = {}

    monkeypatch.setattr(
        report_generation_ssot,
        "cleanup_incomplete_reports",
        lambda db, limit=500, include_non_ok=True: {
            "scanned": 0,
            "soft_deleted": 0,
            "deleted_report_ids": [],
            "reason": None,
            "limit": limit,
            "include_non_ok": include_non_ok,
        },
        raising=False,
    )

    def _fake_generate_reports_batch(**kwargs):
        captured.update(kwargs)
        return {"accepted": len(kwargs.get("stock_codes") or [])}

    monkeypatch.setattr(
        report_generation_ssot,
        "generate_reports_batch",
        _fake_generate_reports_batch,
        raising=False,
    )

    response = client.post(
        "/api/v1/internal/reports/generate-batch",
        json={
            "stock_codes": ["600519.SH", "000001.SZ", "300750.SZ", "000858.SZ"],
            "trade_date": "2026-04-14",
            "one_per_strategy_type": False,
        },
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 202
    assert captured["one_per_strategy_type"] is True


def test_internal_reports_cleanup_incomplete_dry_run_returns_candidates_without_mutation(client, internal_headers, db_session):
    from app.services.report_generation_ssot import generate_report_ssot
    from app.models import Base

    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)
    report = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)
    report_table = Base.metadata.tables["report"]
    db_session.execute(
        report_table.update()
        .where(report_table.c.report_id == report["report_id"])
        .values(conclusion_text=None)
    )
    db_session.commit()

    response = client.post(
        "/api/v1/internal/reports/cleanup-incomplete",
        json={"limit": 200, "dry_run": True},
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["dry_run"] is True
    assert data["candidates"] >= 1
    assert data["soft_deleted"] == 0

    row = db_session.execute(
        report_table.select().where(report_table.c.report_id == report["report_id"])
    ).mappings().one()
    assert bool(row["is_deleted"]) is False


def test_internal_reports_cleanup_incomplete_exec_soft_deletes_reports(client, internal_headers, db_session):
    from app.services.report_generation_ssot import generate_report_ssot
    from app.models import Base

    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)
    report = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)
    report_table = Base.metadata.tables["report"]
    db_session.execute(
        report_table.update()
        .where(report_table.c.report_id == report["report_id"])
        .values(conclusion_text=None)
    )
    db_session.commit()

    response = client.post(
        "/api/v1/internal/reports/cleanup-incomplete",
        json={"limit": 200, "dry_run": False},
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["dry_run"] is False
    assert data["soft_deleted"] >= 1
    assert report["report_id"] in data["deleted_report_ids"]

    row = db_session.execute(
        report_table.select().where(report_table.c.report_id == report["report_id"])
    ).mappings().one()
    assert bool(row["is_deleted"]) is True
    assert row["publish_status"] == "UNPUBLISHED"


def test_internal_reports_incomplete_status_reports_complete_when_no_candidates(client, internal_headers, db_session):
    from app.services.report_generation_ssot import generate_report_ssot

    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)
    generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)
    db_session.commit()

    response = client.get(
        "/api/v1/internal/reports/incomplete-status?limit=200",
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["all_reports_complete"] is True
    assert data["incomplete_candidates"] == 0


def test_internal_reports_incomplete_status_reports_candidates_when_incomplete_exists(client, internal_headers, db_session):
    from app.services.report_generation_ssot import generate_report_ssot
    from app.models import Base

    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)
    report = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)
    report_table = Base.metadata.tables["report"]
    db_session.execute(
        report_table.update()
        .where(report_table.c.report_id == report["report_id"])
        .values(conclusion_text=None)
    )
    db_session.commit()

    response = client.get(
        "/api/v1/internal/reports/incomplete-status?limit=200",
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["all_reports_complete"] is False
    assert data["incomplete_candidates"] >= 1
    assert isinstance(data["candidate_examples"], list)


def test_internal_reports_cleanup_incomplete_all_dry_run(client, internal_headers, monkeypatch):
    import app.services.report_generation_ssot as report_generation_ssot

    monkeypatch.setattr(
        report_generation_ssot,
        "cleanup_incomplete_reports_until_clean",
        lambda db, batch_limit=500, max_batches=20, dry_run=False, include_non_ok=True: {
            "dry_run": dry_run,
            "include_non_ok": include_non_ok,
            "batch_limit": batch_limit,
            "max_batches": max_batches,
            "batches_run": 1,
            "total_scanned": 10,
            "total_candidates": 2,
            "total_soft_deleted": 0,
            "remaining_candidates": 2,
            "deleted_report_ids": [],
            "candidate_examples": [{"report_id": "r1"}, {"report_id": "r2"}],
            "reason": "REPORT_DATA_INCOMPLETE",
        },
        raising=False,
    )

    response = client.post(
        "/api/v1/internal/reports/cleanup-incomplete-all",
        json={"batch_limit": 120, "max_batches": 7, "dry_run": True},
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["dry_run"] is True
    assert data["batch_limit"] == 120
    assert data["max_batches"] == 7
    assert data["include_non_ok"] is True
    assert data["remaining_candidates"] == 2


def test_internal_reports_cleanup_incomplete_all_exec(client, internal_headers, monkeypatch):
    import app.services.report_generation_ssot as report_generation_ssot

    monkeypatch.setattr(
        report_generation_ssot,
        "cleanup_incomplete_reports_until_clean",
        lambda db, batch_limit=500, max_batches=20, dry_run=False, include_non_ok=True: {
            "dry_run": dry_run,
            "include_non_ok": include_non_ok,
            "batch_limit": batch_limit,
            "max_batches": max_batches,
            "batches_run": 3,
            "total_scanned": 80,
            "total_candidates": 6,
            "total_soft_deleted": 6,
            "remaining_candidates": 0,
            "deleted_report_ids": ["r1", "r2", "r3", "r4", "r5", "r6"],
            "candidate_examples": [],
            "reason": "REPORT_DATA_INCOMPLETE",
        },
        raising=False,
    )

    response = client.post(
        "/api/v1/internal/reports/cleanup-incomplete-all",
        json={"batch_limit": 200, "max_batches": 12, "dry_run": False},
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["dry_run"] is False
    assert data["include_non_ok"] is True
    assert data["total_soft_deleted"] == 6
    assert data["remaining_candidates"] == 0
    assert len(data["deleted_report_ids"]) == 6


def test_internal_reports_cleanup_incomplete_all_forwards_include_non_ok(client, internal_headers, monkeypatch):
    import app.services.report_generation_ssot as report_generation_ssot

    captured: dict[str, object] = {}

    def _cleanup_all(db, batch_limit=500, max_batches=20, dry_run=False, include_non_ok=False):
        captured["include_non_ok"] = include_non_ok
        return {
            "dry_run": dry_run,
            "include_non_ok": include_non_ok,
            "batch_limit": batch_limit,
            "max_batches": max_batches,
            "batches_run": 1,
            "total_scanned": 1,
            "total_candidates": 1,
            "total_soft_deleted": 0,
            "remaining_candidates": 1,
            "deleted_report_ids": [],
            "candidate_examples": [{"report_id": "r1"}],
            "reason": "REPORT_DATA_INCOMPLETE",
        }

    monkeypatch.setattr(
        report_generation_ssot,
        "cleanup_incomplete_reports_until_clean",
        _cleanup_all,
        raising=False,
    )

    response = client.post(
        "/api/v1/internal/reports/cleanup-incomplete-all",
        json={"batch_limit": 100, "max_batches": 3, "dry_run": True, "include_non_ok": True},
        headers=internal_headers("internal-reports-token"),
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["include_non_ok"] is True
    assert captured["include_non_ok"] is True
