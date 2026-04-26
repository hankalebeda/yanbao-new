"""NFR-18: schema contract validation tests."""

import pytest
import sqlite3

pytestmark = pytest.mark.feature("NFR-18-SCHEMA-CONTRACT")

from sqlalchemy import create_engine

from app.models import Base
from app.ssot_schema import build_metadata


def test_nfr18_schema_contract_generate(client, db_session, create_user, seed_report_bundle):
    user = create_user(email="nfr18gen@example.com", role="user")
    seed_report_bundle(stock_code="600519.SH", recommendation="BUY")

    login_resp = client.post(
        "/auth/login",
        json={"email": "nfr18gen@example.com", "password": "Password123"},
    )
    token = login_resp.json()["data"]["access_token"]

    resp = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json().get("data", resp.json())

    assert "recommendation" in data
    assert data["recommendation"] in ("BUY", "HOLD", "SELL")

    assert "confidence" in data
    confidence = float(data["confidence"])
    assert 0.0 <= confidence <= 1.0

    assert "strategy_type" in data
    assert data["strategy_type"] in ("A", "B", "C")


def test_nfr18_schema_contract_report_detail(client, db_session, create_user, seed_report_bundle):
    user = create_user(email="nfr18detail@example.com", role="user")
    report = seed_report_bundle(stock_code="600519.SH")

    login_resp = client.post(
        "/auth/login",
        json={"email": "nfr18detail@example.com", "password": "Password123"},
    )
    token = login_resp.json()["data"]["access_token"]

    resp = client.get(
        f"/api/v1/reports/{report.report_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    data = resp.json().get("data", resp.json())

    assert data["recommendation"] in ("BUY", "HOLD", "SELL")
    assert data["strategy_type"] in ("A", "B", "C")
    assert "quality_flag" in data
    assert data["quality_flag"] in ("ok", "stale_ok", "degraded")
    assert "market_state" in data
    assert data["market_state"] in ("BULL", "NEUTRAL", "BEAR")
    assert "review_flag" in data
    assert data["review_flag"] in ("NONE", "APPROVED", "PENDING_REVIEW", "REJECTED")
    assert "publish_status" in data
    assert data["publish_status"] in ("DRAFT_GENERATED", "PUBLISHED", "UNPUBLISHED", "RECALLED")


def test_nfr18_schema_contract_sim_dashboard(client, db_session, create_user, seed_report_bundle):
    user = create_user(email="nfr18sim@example.com", role="user", tier="Pro")
    seed_report_bundle(stock_code="600519.SH")

    login_resp = client.post(
        "/auth/login",
        json={"email": "nfr18sim@example.com", "password": "Password123"},
    )
    token = login_resp.json()["data"]["access_token"]

    resp = client.get(
        "/api/v1/portfolio/sim-dashboard",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True

    data = body.get("data")
    assert isinstance(data, dict)
    positions = data.get("positions") or data.get("open_positions") or []
    for pos in positions:
        assert "capital_tier" in pos
        assert pos["capital_tier"] in ("10k", "100k", "500k")
        assert "position_status" in pos
        assert pos["position_status"] in (
            "OPEN",
            "TAKE_PROFIT",
            "STOP_LOSS",
            "TIMEOUT",
            "DELISTED_LIQUIDATED",
            "SKIPPED",
        )


def test_nfr18_schema_contract_includes_pipeline_run_table():
    metadata, specs = build_metadata()
    spec_names = {spec.name for spec in specs}

    assert "pipeline_run" in spec_names
    assert "pipeline_run" in metadata.tables
    assert "pipeline_run" in Base.metadata.tables
    assert len(spec_names) == 55


def test_nfr18_schema_contract_builds_pipeline_run_into_real_sqlite(tmp_path):
    output = tmp_path / "schema.db"
    metadata, _ = build_metadata()
    engine = create_engine(f"sqlite:///{output.as_posix()}")
    metadata.create_all(bind=engine)

    with sqlite3.connect(output) as conn:
        tables = {row[0] for row in conn.execute("select name from sqlite_master where type='table'")}
        indexes = {row[1] for row in conn.execute("PRAGMA index_list('pipeline_run')")}

    assert len(tables) == 55
    assert "pipeline_run" in tables
    assert "idx_pipeline_run_trade_status" in indexes
