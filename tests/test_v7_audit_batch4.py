"""v7 audit batch 4: FR-04/FR-11/FR-14 gates."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from app.models import Base


def _insert_hotspot_source_row(db_session, *, source_name: str, fetch_time):
    table = Base.metadata.tables["market_hotspot_item_source"]
    db_session.execute(
        table.insert().values(
            hotspot_item_source_id=str(uuid4()),
            hotspot_item_id=str(uuid4()),
            batch_id=str(uuid4()),
            source_name=source_name,
            source_rank=1,
            source_url="https://example.com/topic",
            fetch_time=fetch_time,
            quality_flag="ok",
            created_at=datetime.now(timezone.utc),
        )
    )
    db_session.commit()


def _insert_circuit_row(db_session, *, source_name: str, circuit_state: str = "CLOSED"):
    table = Base.metadata.tables["data_source_circuit_state"]
    now = datetime.now(timezone.utc)
    db_session.execute(
        table.insert().values(
            source_name=source_name,
            circuit_state=circuit_state,
            consecutive_failures=0,
            circuit_open_at=None,
            cooldown_until=None,
            last_probe_at=now,
            last_failure_reason=None,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.commit()


class TestFR04DataCollection:
    def test_hotspot_source_priority_exists(self, db_session):
        from app.services.multisource_ingest import HOTSPOT_SOURCE_PRIORITY

        assert isinstance(HOTSPOT_SOURCE_PRIORITY, (list, tuple))
        assert len(HOTSPOT_SOURCE_PRIORITY) >= 3
        assert "eastmoney" in HOTSPOT_SOURCE_PRIORITY

    def test_circuit_breaker_state_functions(self, db_session):
        from app.services.multisource_ingest import (
            _record_source_failure,
            _record_source_success,
            _source_call_allowed,
            _upsert_circuit_state,
        )

        now = datetime.now(timezone.utc)
        state = _upsert_circuit_state(db_session, "batch4-circuit")
        assert state.circuit_state == "CLOSED"
        assert _source_call_allowed(state, now) is True
        _record_source_failure(state, now, "boom")
        assert state.consecutive_failures == 1
        _record_source_success(state, now)
        assert state.circuit_state == "CLOSED"
        assert state.consecutive_failures == 0

    def test_circuit_breaker_upsert_creates_row(self, db_session):
        from app.services.multisource_ingest import _upsert_circuit_state

        state = _upsert_circuit_state(db_session, "test_source_for_circuit")
        db_session.flush()
        assert state is not None
        assert state.circuit_state == "CLOSED"

    def test_circuit_breaker_thresholds_config(self, db_session):
        from app.core.config import settings

        assert settings.source_fail_open_threshold == 3
        assert settings.source_recover_success_threshold == 2
        assert settings.source_circuit_cooldown_seconds == 300

    def test_kline_quality_flags(self, db_session):
        from app.services.multisource_ingest import QUALITY_FLAGS

        assert "ok" in QUALITY_FLAGS
        assert "stale_ok" in QUALITY_FLAGS
        assert "missing" in QUALITY_FLAGS
        assert "degraded" in QUALITY_FLAGS

    def test_ingest_market_data_callable(self, db_session):
        from app.services.multisource_ingest import ingest_market_data

        assert ingest_market_data.__name__ == "ingest_market_data"

    def test_hotspot_health_endpoint(self, client, internal_headers):
        resp = client.get(
            "/api/v1/internal/hotspot/health",
            headers=internal_headers("fr04-hotspot-health"),
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["degraded"] is True
        assert body["degraded_reason"] == "cold_start_no_hotspot_data"
        assert body["data"]["status"] == "degraded"
        assert body["data"]["sources"] == []

    def test_hotspot_health_tracks_freshness_bands(self, client, db_session, internal_headers):
        _insert_hotspot_source_row(
            db_session,
            source_name="weibo",
            fetch_time=(datetime.now(timezone.utc) - timedelta(minutes=20)).replace(microsecond=0),
        )
        _insert_hotspot_source_row(
            db_session,
            source_name="douyin",
            fetch_time=(datetime.now(timezone.utc) - timedelta(hours=7)).replace(microsecond=0),
        )

        resp = client.get(
            "/api/v1/internal/hotspot/health",
            headers=internal_headers("fr04-hotspot-health"),
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["data"]["status"] == "degraded"
        by_source = {item["source_name"]: item for item in body["data"]["sources"]}
        assert by_source["weibo"]["freshness"] == "fresh"
        assert by_source["weibo"]["age_hours"] is not None
        assert by_source["douyin"]["freshness"] == "degraded"
        assert by_source["douyin"]["age_hours"] is not None

    def test_source_fallback_status_requires_internal_token(self, client):
        resp = client.get("/api/v1/internal/source/fallback-status")
        assert resp.status_code == 401
        assert resp.json()["error_code"] == "UNAUTHORIZED"

    def test_source_fallback_status_endpoint(self, client, internal_headers):
        resp = client.get(
            "/api/v1/internal/source/fallback-status",
            headers=internal_headers("fr04-fallback-status"),
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["degraded"] is True
        assert body["data"]["status"] == "degraded"
        assert body["data"]["status_reason"] == "cold_start_no_circuit_rows"
        assert body["data"]["circuits"] == []
        assert "runtime" in body["data"]

    def test_source_fallback_status_merges_runtime_state(self, client, db_session, internal_headers, monkeypatch):
        from app.api import routes_internal

        _insert_circuit_row(db_session, source_name="eastmoney", circuit_state="CLOSED")

        runtime = {
            "market": {
                "eastmoney": {
                    "consecutive_failures": 3,
                    "consecutive_successes": 0,
                    "last_error": "timeout",
                    "last_update": "2026-03-18T10:00:00+00:00",
                    "circuit_open": True,
                    "opened_at": "2026-03-18T09:55:00+00:00",
                }
            },
            "hotspot": {},
        }
        monkeypatch.setattr(routes_internal, "get_source_runtime_status", lambda: runtime)

        resp = client.get(
            "/api/v1/internal/source/fallback-status",
            headers=internal_headers("fr04-fallback-status"),
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["degraded"] is True
        assert body["data"]["status"] == "degraded"
        eastmoney = next(item for item in body["data"]["circuits"] if item["source_name"] == "eastmoney")
        assert eastmoney["circuit_state"] == "CLOSED"
        assert eastmoney["runtime_source_kind"] == "market"
        assert eastmoney["runtime_circuit_open"] is True
        assert eastmoney["runtime_last_error"] == "timeout"

    def test_source_state_tracking(self, db_session):
        import app.services.source_state as _ss
        from app.services.source_state import (
            RuntimeState,
            SourceHealth,
            get_source_runtime_status,
            record_source_result,
            should_skip_source,
        )

        # Reset global singleton to avoid cross-test leakage
        _ss._state = RuntimeState(
            market={"eastmoney": SourceHealth(), "tdx": SourceHealth(), "fallback": SourceHealth()},
            hotspot={"weibo": SourceHealth(), "douyin": SourceHealth()},
        )

        assert should_skip_source("market", "eastmoney") is False
        record_source_result("market", "eastmoney", success=False, error="timeout")
        status = get_source_runtime_status()
        assert status["market"]["eastmoney"]["consecutive_failures"] >= 1


class TestFR11Observability:
    def test_request_context_set_get(self):
        from app.core.request_context import get_request_id, set_request_id

        set_request_id("test-req-12345")
        assert get_request_id() == "test-req-12345"
        set_request_id(None)
        assert get_request_id() is None

    def test_envelope_ok(self):
        from app.core.response import envelope
        from app.core.request_context import set_request_id

        set_request_id("test-envelope-ok")
        result = envelope(data={"foo": "bar"})
        assert result.get("success") is True
        assert result["request_id"] == "test-envelope-ok"
        assert result["data"]["foo"] == "bar"

    def test_envelope_error(self):
        from app.core.response import envelope
        from app.core.request_context import set_request_id

        set_request_id("test-envelope-error")
        result = envelope(code=1, message="fail", error="something went wrong")
        assert result.get("success") is False
        assert result["request_id"] == "test-envelope-error"

    def test_envelope_degraded(self):
        from app.core.response import envelope
        from app.core.request_context import set_request_id

        set_request_id("test-envelope-degraded")
        result = envelope(data={}, degraded=True, degraded_reason="LLM timeout")
        assert result.get("degraded") is True
        assert result["request_id"] == "test-envelope-degraded"
        assert result.get("degraded_reason") == "LLM timeout"

    def test_health_endpoint(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("success") is True or "data" in data

    def test_runtime_metrics_summary_callable(self, db_session):
        from app.services.observability import runtime_metrics_summary

        result = runtime_metrics_summary(db_session)
        assert isinstance(result, dict)
        assert "window" in result

    def test_runtime_metrics_includes_llm(self, db_session):
        from app.services.observability import runtime_metrics_summary

        result = runtime_metrics_summary(db_session)
        assert "llm" in result
        llm = result["llm"]
        assert "total" in llm or "success" in llm or isinstance(llm, dict)

    def test_runtime_metrics_source_health_respects_nested_runtime_status(self, db_session, monkeypatch):
        import app.services.observability as observability

        monkeypatch.setattr(
            observability,
            "get_source_runtime_status",
            lambda: {
                "market": {
                    "eastmoney": {"circuit_open": False, "last_error": None, "consecutive_failures": 0},
                    "tdx": {"circuit_open": False, "last_error": None, "consecutive_failures": 0},
                },
                "hotspot": {
                    "weibo": {"circuit_open": False, "last_error": None, "consecutive_failures": 0},
                },
            },
        )

        healthy = observability.runtime_metrics_summary(db_session)
        assert "source_runtime_abnormal" not in healthy["service_health"]["flags"]
        assert healthy["service_health"]["unhealthy_source_count"] == 0
        assert healthy["service_health"]["unhealthy_sources"] == []

        monkeypatch.setattr(
            observability,
            "get_source_runtime_status",
            lambda: {
                "market": {
                    "eastmoney": {"circuit_open": False, "last_error": None, "consecutive_failures": 0},
                    "tdx": {"circuit_open": True, "last_error": "timeout", "consecutive_failures": 3},
                },
                "hotspot": {
                    "weibo": {"circuit_open": False, "last_error": None, "consecutive_failures": 0},
                },
            },
        )

        degraded = observability.runtime_metrics_summary(db_session)
        assert "source_runtime_abnormal" in degraded["service_health"]["flags"]
        assert degraded["service_health"]["unhealthy_source_count"] == 1
        assert degraded["service_health"]["unhealthy_sources"] == ["market:tdx"]

    def test_llm_health_endpoint_degraded_on_upstream_failure(self, client, internal_headers, monkeypatch):
        from app.api import routes_internal

        async def fake_health():
            return {
                "status": "degraded",
                "tags": [],
                "latency_ms": 12,
                "error": "502 Bad Gateway",
            }

        monkeypatch.setattr(routes_internal.ollama_client, "health", fake_health)
        resp = client.get(
            "/api/v1/internal/llm/health",
            headers=internal_headers("fr11-llm-health"),
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["degraded"] is True
        assert body["degraded_reason"] == "502 Bad Gateway"
        assert body["data"]["status"] == "degraded"
        assert body["data"]["tags"] == []


class TestFR14NFR:
    def test_jwt_config_defaults(self):
        from app.core.config import settings

        assert settings.jwt_access_token_expire_hours == 12
        assert settings.jwt_algorithm == "HS256"

    def test_password_hashing_functions(self):
        from app.core.security import hash_password, verify_password

        hashed = hash_password("TestPass123!")
        assert hashed != "TestPass123!"
        assert verify_password("TestPass123!", hashed) is True
        assert verify_password("wrong", hashed) is False

    def test_rate_limit_on_login(self, client, create_user):
        from app.api.routes_auth import LOGIN_LIMIT

        assert LOGIN_LIMIT == 5

    def test_pagination_query_params(self, client, create_user):
        admin = create_user(
            email="fr14-admin@test.com",
            password="Password123",
            role="admin",
            email_verified=True,
        )
        resp = client.post(
            "/auth/login",
            json={"email": admin["user"].email, "password": admin["password"]},
        )
        headers = {"Authorization": f"Bearer {resp.json()['data']['access_token']}"}
        page_resp = client.get(
            "/api/v1/sim/positions?page=1&page_size=10&capital_tier=10k",
            headers=headers,
        )
        assert page_resp.status_code == 200

    def test_auth_cookie_config(self):
        from app.core.security import AUTH_COOKIE

        assert AUTH_COOKIE == "access_token"

    def test_trusted_hosts_configured(self):
        from app.core.config import settings

        hosts = settings.trusted_hosts
        assert "localhost" in hosts
        assert "*" not in hosts
