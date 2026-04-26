"""FR04-DATA-05: 熔断事件写入 NFR-13 告警日志验收测试
SSOT: 01 §FR04-DATA-05, 03 §NFR-13
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest


@pytest.mark.feature("FR04-DATA-05")
class TestCircuitBreakerNFR13Alert:
    def test_emit_alert_calls_operational_alert_on_open(self, monkeypatch):
        """熔断器 OPEN 时 _emit_circuit_breaker_alert 向 NFR-13 分发告警。"""
        from app.services.multisource_ingest import _emit_circuit_breaker_alert
        from app.models import DataSourceCircuitState

        captured: list[dict] = []

        def mock_emit(*, alert_type, fr_id, message, payload=None, timestamp=None):
            captured.append({"alert_type": alert_type, "fr_id": fr_id, "payload": payload})
            return ("sent", None, "webhook")

        monkeypatch.setattr("app.services.notification.emit_operational_alert", mock_emit)

        now = datetime.now(timezone.utc)
        state = DataSourceCircuitState()
        state.source_name = "eastmoney"
        state.source_kind = "hotspot"
        state.circuit_state = "OPEN"
        state.consecutive_failures = 3
        state.last_failure_reason = "connection_error"

        _emit_circuit_breaker_alert(
            alert_type="CIRCUIT_BREAKER_OPEN",
            source_name="eastmoney",
            source_kind="hotspot",
            state=state,
            reason="connection_error",
            now=now,
        )

        assert len(captured) == 1
        call = captured[0]
        assert call["alert_type"] == "CIRCUIT_BREAKER_OPEN"
        assert call["fr_id"] == "FR-04"
        assert call["payload"]["source_name"] == "eastmoney"
        assert call["payload"]["circuit_state"] == "OPEN"

    def test_emit_alert_calls_operational_alert_on_close(self, monkeypatch):
        """熔断器 CLOSE 时 _emit_circuit_breaker_alert 向 NFR-13 分发告警。"""
        from app.services.multisource_ingest import _emit_circuit_breaker_alert
        from app.models import DataSourceCircuitState

        captured: list[dict] = []

        def mock_emit(*, alert_type, fr_id, message, payload=None, timestamp=None):
            captured.append({"alert_type": alert_type, "fr_id": fr_id})
            return ("sent", None, "webhook")

        monkeypatch.setattr("app.services.notification.emit_operational_alert", mock_emit)

        now = datetime.now(timezone.utc)
        state = DataSourceCircuitState()
        state.source_name = "xueqiu"
        state.circuit_state = "CLOSED"
        state.consecutive_failures = 0

        _emit_circuit_breaker_alert(
            alert_type="CIRCUIT_BREAKER_CLOSE",
            source_name="xueqiu",
            source_kind="hotspot",
            state=state,
            reason=None,
            now=now,
        )

        assert len(captured) == 1
        assert captured[0]["alert_type"] == "CIRCUIT_BREAKER_CLOSE"
        assert captured[0]["fr_id"] == "FR-04"

    def test_record_source_failure_opens_circuit_at_threshold(self):
        """source_fail_open_threshold=3 连续失败后熔断器 OPEN，第3次返回 True。"""
        from app.services.multisource_ingest import _record_source_failure
        from app.models import DataSourceCircuitState

        now = datetime.now(timezone.utc)
        state = DataSourceCircuitState()
        state.source_name = "test_source"
        state.circuit_state = "CLOSED"
        state.consecutive_failures = 0

        r1 = _record_source_failure(state, now, "err1")
        assert r1 is False
        assert state.circuit_state == "CLOSED"

        r2 = _record_source_failure(state, now, "err2")
        assert r2 is False
        assert state.circuit_state == "CLOSED"

        r3 = _record_source_failure(state, now, "err3")
        assert r3 is True
        assert state.circuit_state == "OPEN"

    def test_record_source_failure_already_open_not_retriggered(self):
        """已 OPEN 时再失败不重新触发（not newly opened）。"""
        from app.services.multisource_ingest import _record_source_failure
        from app.models import DataSourceCircuitState

        now = datetime.now(timezone.utc)
        state = DataSourceCircuitState()
        state.source_name = "test_source"
        state.circuit_state = "OPEN"
        state.consecutive_failures = 3

        result = _record_source_failure(state, now, "additional_err")
        assert result is False
        assert state.circuit_state == "OPEN"

    def test_alert_payload_contains_consecutive_failures(self, monkeypatch):
        """告警 payload 包含 consecutive_failures 字段。"""
        from app.services.multisource_ingest import _emit_circuit_breaker_alert
        from app.models import DataSourceCircuitState

        captured: list[dict] = []

        def mock_emit(*, alert_type, fr_id, message, payload=None, timestamp=None):
            captured.append(payload or {})
            return ("skipped", None, None)

        monkeypatch.setattr("app.services.notification.emit_operational_alert", mock_emit)

        now = datetime.now(timezone.utc)
        state = DataSourceCircuitState()
        state.source_name = "weibo"
        state.circuit_state = "OPEN"
        state.consecutive_failures = 5
        state.last_failure_reason = "timeout"

        _emit_circuit_breaker_alert(
            alert_type="CIRCUIT_BREAKER_OPEN",
            source_name="weibo",
            source_kind="hotspot",
            state=state,
            reason="timeout",
            now=now,
        )

        assert len(captured) == 1
        payload = captured[0]
        assert payload["consecutive_failures"] == 5
        assert payload["last_failure_reason"] == "timeout"
        assert payload["source_kind"] == "hotspot"
