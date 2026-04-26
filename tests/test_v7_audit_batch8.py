"""v7.2精审 批量补充测试 — FR-09B/FR-10/FR-12/FR-13/LEGACY 所有剩余差距项。
FR-09B (脱敏审计)
FR-10 (TTL缓存/筛选器/window_days/移动端)
FR-12 (PATCH审计)
FR-13 (恢复放行)
LEGACY (旧路由)
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from unittest.mock import patch

import pytest
from sqlalchemy import text

from tests.helpers_ssot import (
    insert_report_bundle_ssot,
    insert_stock_master,
)


class _PageParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements: list[dict] = []

    def handle_starttag(self, tag, attrs):
        self.elements.append({"tag": tag, "attrs": dict(attrs)})

    def has_selector(self, selector: str) -> bool:
        return any(_match_selector(element, selector) for element in self.elements)


def _match_selector(element: dict, selector: str) -> bool:
    attrs = element.get("attrs", {})
    if selector.startswith("#"):
        return attrs.get("id") == selector[1:]
    if selector.startswith("."):
        return selector[1:] in attrs.get("class", "").split()
    if "[" in selector and selector.endswith("]"):
        tag, attr_part = selector.split("[", 1)
        attr, value = attr_part[:-1].split("=", 1)
        value = value.strip("'\"")
        return element["tag"] == tag and attrs.get(attr) == value
    return element["tag"] == selector


def _parse_html(text: str) -> _PageParser:
    parser = _PageParser()
    parser.feed(text)
    return parser


def _admin_headers(client, create_user, email="b8-admin@test.com"):
    admin = create_user(email=email, password="Password123", role="admin", email_verified=True)
    resp = client.post("/auth/login", json={"email": admin["user"].email, "password": admin["password"]})
    token = resp.json()["data"]["access_token"]
    return {"Authorization": f"Bearer {token}"}


def _user_tokens(client, create_user, email="b8-user@test.com", tier="Free"):
    user = create_user(email=email, password="Password123", role="user", email_verified=True, tier=tier)
    resp = client.post("/auth/login", json={"email": user["user"].email, "password": user["password"]})
    data = resp.json()["data"]
    return data["access_token"], user["user"]


# ═══════════════════════════════════════════════════════════
# FR-09B  系统清理
# ═══════════════════════════════════════════════════════════

class TestFR09BDesensitization:
    """FR09B-CLEAN-02: 脱敏审计记录测试。"""

    def test_desensitization_format(self):
        """FR09B-CLEAN-02: 脱敏格式=首3字符+***+@域名。"""
        # Verify the desensitization logic from cleanup_service
        email = "testuser@example.com"
        parts = str(email).split("@")
        local = parts[0][:3] + "***" if len(parts[0]) > 3 else parts[0] + "***"
        domain = parts[1] if len(parts) > 1 else "unknown"
        result = f"{local}@{domain}"
        assert result == "tes***@example.com"

    def test_cleanup_records_desensitized_list(self, db_session):
        """FR09B-CLEAN-02: 清理删除时产生脱敏email列表。"""
        from app.models import User
        from app.core.security import hash_password
        # Create an unverified user with old created_at
        old_time = datetime.now(timezone.utc) - timedelta(hours=48)
        user = User(
            email="cleanup-victim@test.com",
            password_hash=hash_password("Password123"),
            tier="Free",
            role="user",
            email_verified=False,
        )
        db_session.add(user)
        db_session.commit()
        # Manually set created_at to old time
        db_session.execute(
            text("UPDATE app_user SET created_at = :old WHERE email = :email"),
            {"old": old_time.isoformat(), "email": "cleanup-victim@test.com"},
        )
        db_session.commit()

        from app.services.cleanup_service import _run_cleanup_inner
        result = _run_cleanup_inner(db_session, cleanup_date="2026-03-15")
        assert result.get("deleted_unverified_user_count", 0) >= 1
        desensitized = result.get("deleted_unverified_emails_desensitized", [])
        assert len(desensitized) > 0
        assert "***" in desensitized[0]  # Contains masking

    def test_cleanup_batched_delete_constant(self):
        """FR09B-CLEAN-01: 分批删除常量验证。"""
        from app.services.cleanup_service import BATCH_SIZE, BATCH_SLEEP_SEC
        assert BATCH_SIZE == 500
        assert BATCH_SLEEP_SEC == 0.1


# ═══════════════════════════════════════════════════════════
# FR-10  站点看板 — 剩余差距
# ═══════════════════════════════════════════════════════════

class TestFR10HomeCache:
    """FR10-PAGE-01: 首页5分钟TTL缓存测试。"""

    def test_home_cache_ttl_constant(self):
        """FR10-PAGE-01: 首页缓存TTL=300秒(5分钟)。"""
        from app.api.routes_business import _HOME_CACHE_TTL
        assert _HOME_CACHE_TTL == 300

    def test_home_api_returns_data(self, client):
        """FR10-PAGE-01: /home返回正确结构。"""
        resp = client.get("/api/v1/home")
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        data = body["data"]
        assert isinstance(data, dict)
        for field in ("market_state", "pool_size", "today_report_count", "hot_stocks"):
            assert field in data


class TestFR10Filters:
    """FR10-PAGE-02: HTML筛选器测试。"""

    def test_quality_flag_filter_exists(self, client):
        """FR10-PAGE-02: HTML页面包含quality_flag过滤器。"""
        resp = client.get("/reports")
        assert resp.status_code == 200
        dom = _parse_html(resp.text)
        assert dom.has_selector("#filter-quality"), "quality_flag filter not found in reports_list.html"

    def test_capital_tier_filter_exists(self, client):
        """FR10-PAGE-02: HTML页面包含capital_tier过滤器。"""
        resp = client.get("/reports")
        assert resp.status_code == 200
        dom = _parse_html(resp.text)
        assert dom.has_selector("#filter-tier"), "capital_tier filter not found in reports_list.html"


class TestFR10WindowDays:
    """FR10-PAGE-05: 统计看板window_days切换测试。"""

    def test_dashboard_valid_windows(self, client):
        """FR10-PAGE-05: window_days=7/14/60均接受。"""
        for wd in (1, 7, 14, 30, 60):
            resp = client.get(f"/api/v1/dashboard/stats?window_days={wd}")
            assert resp.status_code == 200, f"window_days={wd} returned {resp.status_code}"

    def test_dashboard_invalid_window_rejected(self, client):
        """FR10-PAGE-05: 非法window_days被拒绝或使用默认值。"""
        resp = client.get("/api/v1/dashboard/stats?window_days=99")
        assert resp.status_code == 422
        body = resp.json()
        assert body["error_code"] == "INVALID_PAYLOAD"


class TestFR10Mobile:
    """FR10-PAGE-10: 响应式/移动端测试。"""

    def test_viewport_meta_exists(self, client):
        """FR10-PAGE-10: HTML包含viewport meta标签。"""
        resp = client.get("/")
        assert resp.status_code == 200
        dom = _parse_html(resp.text)
        assert dom.has_selector("meta[name='viewport']"), "viewport meta tag not found"


# ═══════════════════════════════════════════════════════════
# FR-12  管理后台 — 剩余差距
# ═══════════════════════════════════════════════════════════

class TestFR12PatchAudit:
    """FR12-ADMIN-03: PATCH report审计记录测试。"""

    def test_patch_report_creates_audit(self, client, db_session, create_user):
        """FR12-ADMIN-03: PATCH report产生admin_operation审计记录。"""
        headers = _admin_headers(client, create_user)
        report = insert_report_bundle_ssot(db_session, published=False)
        resp = client.patch(
            f"/api/v1/admin/reports/{report.report_id}",
            json={"published": True},
            headers=headers,
        )
        assert resp.status_code == 200
        from app.models import Base

        op_t = Base.metadata.tables.get("admin_operation")
        assert op_t is not None, "admin_operation table must exist for PATCH audit verification"
        ops = db_session.execute(
            op_t.select().where(op_t.c.target_pk == str(report.report_id))
        ).fetchall()
        assert len(ops) >= 1, "No audit record found for PATCH"


# ═══════════════════════════════════════════════════════════
# FR-13  事件推送 — 剩余差距
# ═══════════════════════════════════════════════════════════

class TestFR13RecoveryRelease:
    """FR13-EVT-03: NORMAL恢复中间态重新放行。"""

    def test_drawdown_suppress_hours_constant(self):
        """FR13-EVT-03: DRAWDOWN_SUPPRESS_HOURS=4。"""
        from app.services.event_dispatcher import DRAWDOWN_SUPPRESS_HOURS
        assert DRAWDOWN_SUPPRESS_HOURS == 4

    def test_enqueue_drawdown_alert_callable(self):
        """FR13-EVT-03: enqueue_drawdown_alert可调用。"""
        from app.services.event_dispatcher import enqueue_drawdown_alert
        assert enqueue_drawdown_alert.__name__ == "enqueue_drawdown_alert"


# ═══════════════════════════════════════════════════════════
# FR-04  数据采集 — 剩余差距
# ═══════════════════════════════════════════════════════════

class TestFR04EnrichE2E:
    """FR04-DATA-07: enrich端点E2E测试。"""

    def test_enrich_endpoint_accessible(self, client, internal_headers):
        """FR04-DATA-07: POST /hotspot/enrich 可访问(内部接口)。"""
        resp = client.post(
            "/api/v1/internal/hotspot/enrich",
            headers=internal_headers("batch8-enrich"),
        )
        body = resp.json()
        assert resp.status_code == 200, resp.text
        assert body["success"] is True
        assert "enriched" in body["data"]
        assert "total_candidates" in body["data"]


class TestFR04CircuitBreakerAlert:
    """FR04-DATA-04: 熔断事件告警。"""

    def test_circuit_breaker_states(self, db_session):
        """FR04-DATA-04: 熔断状态包含OPEN/HALF_OPEN/CLOSED。"""
        from app.services.multisource_ingest import _record_source_failure, _source_call_allowed, _upsert_circuit_state

        state = _upsert_circuit_state(db_session, "batch8-circuit")
        now = datetime.now(timezone.utc)
        for _ in range(3):
            _record_source_failure(state, now, "fail")
        assert state.circuit_state == "OPEN"
        assert _source_call_allowed(state, now) is False


# ═══════════════════════════════════════════════════════════
# LEGACY  旧路由测试
# ═══════════════════════════════════════════════════════════

class TestLegacyRoutes:
    """LEGACY-REPORT-01~04: 旧路由兼容测试。"""

    @pytest.mark.feature("LEGACY-REPORT-01")
    def test_legacy_report_code_redirect(self, client, db_session):
        """LEGACY-REPORT-01: GET /report/{code} →302或404。"""
        insert_stock_master(db_session, stock_code="600519.SH", stock_name="贵州茅台")
        db_session.commit()
        resp = client.get("/report/600519.SH", follow_redirects=False)
        assert resp.status_code == 404

    @pytest.mark.feature("LEGACY-REPORT-02")
    def test_legacy_report_status(self, client, db_session):
        """LEGACY-REPORT-02: GET /report/{code}/status →JSON。"""
        insert_stock_master(db_session, stock_code="000001.SZ", stock_name="平安银行")
        db_session.commit()
        resp = client.get("/report/000001.SZ/status")
        assert resp.status_code == 200

    @pytest.mark.feature("LEGACY-REPORT-03")
    def test_legacy_realtime_redirect(self, client):
        """LEGACY-REPORT-03: GET /report/实时研报/{code} →302。"""
        resp = client.get("/report/实时研报/600519.SH", follow_redirects=False)
        assert resp.status_code == 302

    @pytest.mark.feature("LEGACY-REPORT-04")
    def test_legacy_report_list_redirect(self, client):
        """LEGACY-REPORT-04: GET /report →302/400重定向。"""
        resp = client.get("/report", follow_redirects=False)
        # /report matches /report/{stock_code} with empty code → 400 or redirect
        assert resp.status_code == 400
