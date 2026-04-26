from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.core.config import Settings


def test_runtime_security_requires_jwt_secret():
    cfg = Settings(
        jwt_secret="",
        billing_webhook_secret="test-billing-secret",
        trusted_hosts="127.0.0.1,localhost",
        debug=False,
    )
    with pytest.raises(RuntimeError, match="JWT_SECRET"):
        cfg.validate_runtime_security()


def test_runtime_security_requires_billing_webhook_secret():
    cfg = Settings(
        jwt_secret="test-jwt-secret",
        billing_webhook_secret="",
        trusted_hosts="127.0.0.1,localhost",
        debug=False,
    )
    with pytest.raises(RuntimeError, match="BILLING_WEBHOOK_SECRET"):
        cfg.validate_runtime_security()


def test_runtime_security_rejects_legacy_default_billing_webhook_secret():
    cfg = Settings(
        jwt_secret="test-jwt-secret",
        billing_webhook_secret="dev-billing-secret",
        trusted_hosts="127.0.0.1,localhost",
        debug=False,
    )
    with pytest.raises(RuntimeError, match="legacy development default"):
        cfg.validate_runtime_security()


def test_runtime_security_accepts_explicit_billing_webhook_secret():
    cfg = Settings(
        jwt_secret="test-jwt-secret",
        billing_webhook_secret="explicit-billing-secret",
        trusted_hosts="127.0.0.1,localhost",
        debug=False,
    )

    cfg.validate_runtime_security()


def test_runtime_security_rejects_wildcard_trusted_hosts():
    cfg = Settings(
        jwt_secret="test-jwt-secret",
        billing_webhook_secret="test-billing-secret",
        trusted_hosts="127.0.0.1,localhost,*",
        debug=False,
    )
    with pytest.raises(RuntimeError, match="TRUSTED_HOSTS"):
        cfg.validate_runtime_security()


def test_test_isolation_never_uses_runtime_app_db(isolated_app):
    engine_url = str(isolated_app["engine"].url).replace("\\", "/").lower()
    assert "data/app.db" not in engine_url, (
        f"tests must not run against shared runtime database, got {engine_url}"
    )


def test_runtime_payment_webhook_event_schema_keeps_request_id_backlink():
    db_path = Path("data/app.db")
    if not db_path.exists():
        pytest.skip("runtime app.db not present in workspace")

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(payment_webhook_event)")}
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='payment_webhook_event'"
            )
        }

    assert "request_id" in columns
    assert "idx_payment_webhook_event_request" in indexes
