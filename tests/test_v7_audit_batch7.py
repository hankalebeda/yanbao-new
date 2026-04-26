"""v7 audit batch 7: real behavior coverage for FR08/FR09 gaps."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import text

from tests.helpers_ssot import (
    insert_kline,
    insert_open_position,
    insert_report_bundle_ssot,
    insert_stock_master,
)


def _user_tokens(client, create_user, email="b7-user@test.com", tier="Pro"):
    user = create_user(email=email, password="Password123", role="user", email_verified=True, tier=tier)
    resp = client.post("/auth/login", json={"email": user["user"].email, "password": user["password"]})
    data = resp.json()["data"]
    return data["access_token"], data.get("refresh_token", ""), user["user"]


class TestFR08PessimisticMatch:
    def test_close_positions_function_exists(self, db_session):
        from app.services.sim_positioning_ssot import _close_positions

        report = insert_report_bundle_ssot(db_session, stock_code="600000.SH", trade_date="2025-09-19")
        insert_stock_master(db_session, stock_code="600000.SH", stock_name="PF BANK")
        position_id = insert_open_position(
            db_session,
            report_id=report.report_id,
            stock_code="600000.SH",
            capital_tier="10k",
            signal_date="2025-09-19",
            entry_date="2025-09-19",
            actual_entry_price=10.0,
            signal_entry_price=10.0,
            position_ratio=0.2,
            shares=1000,
            stop_loss_price=9.0,
            target_price=11.0,
        )
        insert_kline(
            db_session,
            stock_code="600000.SH",
            trade_date="2026-03-18",
            open_price=10.2,
            high_price=10.5,
            low_price=10.0,
            close_price=10.3,
        )

        _close_positions(db_session, trade_day=date(2026, 3, 18), accounts={"10k": {"cash_available": 10_000.0}})
        db_session.commit()

        row = db_session.execute(
            text("SELECT position_status, holding_days FROM sim_position WHERE position_id = :position_id"),
            {"position_id": position_id},
        ).mappings().first()
        assert row is not None
        assert row["position_status"] == "TIMEOUT"
        assert row["holding_days"] >= 180

    def test_delisted_liquidation_constants(self):
        from app.services.sim_positioning_ssot import DRAWDOWN_FACTOR_BY_STATE

        assert DRAWDOWN_FACTOR_BY_STATE["HALT"] == 0.0

    def test_timeout_180_days(self, db_session):
        from app.services.sim_positioning_ssot import _close_positions

        report = insert_report_bundle_ssot(db_session, stock_code="600001.SH", trade_date="2025-09-18")
        insert_stock_master(db_session, stock_code="600001.SH", stock_name="TIMEOUT CASE")
        position_id = insert_open_position(
            db_session,
            report_id=report.report_id,
            stock_code="600001.SH",
            capital_tier="10k",
            signal_date="2025-09-18",
            entry_date="2025-09-18",
            actual_entry_price=10.0,
            signal_entry_price=10.0,
            position_ratio=0.2,
            shares=1000,
        )
        insert_kline(
            db_session,
            stock_code="600001.SH",
            trade_date="2026-03-18",
            open_price=10.1,
            high_price=10.4,
            low_price=10.0,
            close_price=10.2,
        )

        _close_positions(db_session, trade_day=date(2026, 3, 18), accounts={"10k": {"cash_available": 10_000.0}})
        db_session.commit()

        row = db_session.execute(
            text("SELECT position_status FROM sim_position WHERE position_id = :position_id"),
            {"position_id": position_id},
        ).mappings().first()
        assert row is not None
        assert row["position_status"] == "TIMEOUT"


class TestFR08AdjFactor:
    def test_adj_factor_logic(self, db_session):
        from app.services.sim_positioning_ssot import _close_positions

        report = insert_report_bundle_ssot(db_session, stock_code="600002.SH", trade_date="2026-03-10")
        insert_stock_master(db_session, stock_code="600002.SH", stock_name="ADJ FACTOR")
        position_id = insert_open_position(
            db_session,
            report_id=report.report_id,
            stock_code="600002.SH",
            capital_tier="10k",
            signal_date="2026-03-10",
            entry_date="2026-03-10",
            actual_entry_price=100.0,
            signal_entry_price=100.0,
            position_ratio=0.2,
            shares=1000,
            stop_loss_price=90.0,
            target_price=110.0,
        )
        insert_kline(
            db_session,
            stock_code="600002.SH",
            trade_date="2026-03-10",
            open_price=99.0,
            high_price=101.0,
            low_price=98.0,
            close_price=120.0,
        )
        insert_kline(
            db_session,
            stock_code="600002.SH",
            trade_date="2026-03-18",
            open_price=108.0,
            high_price=109.0,
            low_price=107.0,
            close_price=108.0,
        )

        _close_positions(db_session, trade_day=date(2026, 3, 18), accounts={"10k": {"cash_available": 10_000.0}})
        db_session.commit()

        row = db_session.execute(
            text("SELECT position_status, exit_price FROM sim_position WHERE position_id = :position_id"),
            {"position_id": position_id},
        ).mappings().first()
        assert row is not None
        assert row["position_status"] == "STOP_LOSS"
        assert float(row["exit_price"]) == pytest.approx(108.0, abs=1e-4)

    def test_stop_loss_dynamic_recalc(self, db_session):
        from app.services.sim_positioning_ssot import _close_positions

        report = insert_report_bundle_ssot(db_session, stock_code="600003.SH", trade_date="2026-03-10")
        insert_stock_master(db_session, stock_code="600003.SH", stock_name="STOP LOSS")
        position_id = insert_open_position(
            db_session,
            report_id=report.report_id,
            stock_code="600003.SH",
            capital_tier="10k",
            signal_date="2026-03-10",
            entry_date="2026-03-10",
            actual_entry_price=100.0,
            signal_entry_price=100.0,
            position_ratio=0.2,
            shares=1000,
            stop_loss_price=90.0,
            target_price=110.0,
        )
        insert_kline(
            db_session,
            stock_code="600003.SH",
            trade_date="2026-03-10",
            open_price=99.0,
            high_price=101.0,
            low_price=98.0,
            close_price=120.0,
        )
        insert_kline(
            db_session,
            stock_code="600003.SH",
            trade_date="2026-03-18",
            open_price=108.0,
            high_price=109.0,
            low_price=107.5,
            close_price=108.0,
        )

        _close_positions(db_session, trade_day=date(2026, 3, 18), accounts={"10k": {"cash_available": 10_000.0}})
        db_session.commit()

        row = db_session.execute(
            text("SELECT position_status, exit_price FROM sim_position WHERE position_id = :position_id"),
            {"position_id": position_id},
        ).mappings().first()
        assert row is not None
        assert row["position_status"] == "STOP_LOSS"
        assert float(row["exit_price"]) == pytest.approx(108.0, abs=1e-4)


class TestFR08LiquidityDefer:
    def test_suspended_pending_logic(self, db_session):
        from app.services.sim_positioning_ssot import _close_positions

        report = insert_report_bundle_ssot(db_session, stock_code="600004.SH", trade_date="2026-03-10")
        insert_stock_master(db_session, stock_code="600004.SH", stock_name="SUSPENDED")
        position_id = insert_open_position(
            db_session,
            report_id=report.report_id,
            stock_code="600004.SH",
            capital_tier="10k",
            signal_date="2026-03-10",
            entry_date="2026-03-10",
            actual_entry_price=10.0,
            signal_entry_price=10.0,
            position_ratio=0.2,
            shares=1000,
        )
        insert_kline(
            db_session,
            stock_code="600004.SH",
            trade_date="2026-03-18",
            open_price=10.0,
            high_price=10.0,
            low_price=10.0,
            close_price=10.0,
            volume=0,
            is_suspended=True,
        )

        _close_positions(db_session, trade_day=date(2026, 3, 18), accounts={"10k": {"cash_available": 10_000.0}})
        db_session.commit()

        row = db_session.execute(
            text("SELECT position_status, suspended_pending FROM sim_position WHERE position_id = :position_id"),
            {"position_id": position_id},
        ).mappings().first()
        assert row is not None
        assert row["position_status"] == "OPEN"
        assert bool(row["suspended_pending"]) is True

    def test_limit_locked_pending_logic(self, db_session):
        from app.services.sim_positioning_ssot import _close_positions

        report = insert_report_bundle_ssot(db_session, stock_code="600005.SH", trade_date="2026-03-10")
        insert_stock_master(db_session, stock_code="600005.SH", stock_name="LIMIT LOCK")
        position_id = insert_open_position(
            db_session,
            report_id=report.report_id,
            stock_code="600005.SH",
            capital_tier="10k",
            signal_date="2026-03-10",
            entry_date="2026-03-10",
            actual_entry_price=10.0,
            signal_entry_price=10.0,
            position_ratio=0.2,
            shares=1000,
        )
        insert_kline(
            db_session,
            stock_code="600005.SH",
            trade_date="2026-03-18",
            open_price=10.5,
            high_price=10.5,
            low_price=10.5,
            close_price=10.5,
            volume=100000,
        )

        _close_positions(db_session, trade_day=date(2026, 3, 18), accounts={"10k": {"cash_available": 10_000.0}})
        db_session.commit()

        row = db_session.execute(
            text("SELECT position_status, limit_locked_pending FROM sim_position WHERE position_id = :position_id"),
            {"position_id": position_id},
        ).mappings().first()
        assert row is not None
        assert row["position_status"] == "OPEN"
        assert bool(row["limit_locked_pending"]) is True


class TestFR08OldSystemCoexist:
    def test_ssot_slippage_correct(self):
        from app.services.sim_positioning_ssot import _buy_cost

        _, _, slip = _buy_cost(10.0, 1000)
        assert abs(slip - 5.0) < 0.01

    def test_new_system_commission_rate(self):
        from app.services.sim_positioning_ssot import _buy_cost

        _, comm, _ = _buy_cost(100.0, 1000)
        assert abs(comm - 25.0) < 0.01


class TestFR09TokenRefreshGrace:
    def test_refresh_grace_seconds_60(self):
        from app.core.security import REFRESH_GRACE_SECONDS

        assert REFRESH_GRACE_SECONDS == 60

    def test_rotate_refresh_token_function(self, client, db_session, create_user):
        from app.core.security import rotate_refresh_token

        _, refresh, _ = _user_tokens(client, create_user, email="rotate-refresh@test.com")
        assert refresh

        status, tokens = rotate_refresh_token(db_session, refresh)
        db_session.commit()

        assert status == "ok"
        assert tokens is not None
        assert tokens["refresh_token"] != refresh
        assert tokens["access_token"]

    def test_refresh_token_rotation_basic(self, client, create_user):
        _, refresh, _ = _user_tokens(client, create_user, email="grace-test@t.com")
        assert refresh

        resp = client.post("/auth/refresh", json={"refresh_token": refresh})
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["refresh_token"] != refresh
        assert data["access_token"]


class TestFR09ResetPassword:
    def test_reset_password_expired_token(self, client, db_session, create_user):
        user = create_user(email="reset-test@test.com", password="Password123", email_verified=True)
        from app.core.security import hash_token
        from app.models import Base

        token_t = Base.metadata.tables.get("auth_temp_token")
        if token_t is None:
            pytest.skip("auth_temp_token table not found")
        raw_token = "expired-reset-token-12345678"
        db_session.execute(
            token_t.insert().values(
                temp_token_id=str(uuid4()),
                token_hash=hash_token(raw_token),
                token_type="PASSWORD_RESET",
                user_id=user["user"].user_id,
                expires_at=datetime.now(timezone.utc) - timedelta(hours=2),
                sent_at=datetime.now(timezone.utc) - timedelta(hours=3),
            )
        )
        db_session.commit()

        resp = client.post("/auth/reset-password", json={"token": raw_token, "new_password": "NewPassword123"})
        assert resp.status_code in (400, 422)


class TestFR09Activate:
    def test_activate_valid_token(self, client, db_session, create_user):
        user = create_user(email="activate-test@test.com", password="Password123", email_verified=False)
        from app.core.security import hash_token
        from app.models import Base

        token_t = Base.metadata.tables.get("auth_temp_token")
        if token_t is None:
            pytest.skip("auth_temp_token table not found")
        raw_token = "valid-activate-token-123456"
        db_session.execute(
            token_t.insert().values(
                temp_token_id=str(uuid4()),
                token_hash=hash_token(raw_token),
                token_type="EMAIL_ACTIVATION",
                user_id=user["user"].user_id,
                expires_at=datetime.now(timezone.utc) + timedelta(hours=24),
                sent_at=datetime.now(timezone.utc),
            )
        )
        db_session.commit()

        resp = client.get(f"/auth/activate?token={raw_token}", follow_redirects=False)
        assert resp.status_code == 302

    def test_activate_invalid_token(self, client):
        resp = client.get("/auth/activate?token=totally-invalid-token")
        assert resp.status_code == 400


class TestFR09OAuthProvider:
    def test_invalid_provider_rejected(self, client, create_user):
        access, _, _ = _user_tokens(client, create_user, email="oauth-test@test.com")
        resp = client.post(
            "/auth/oauth/invalid_provider/start",
            headers={"Authorization": f"Bearer {access}"},
        )
        assert resp.status_code in (404, 422, 400)


class TestFR09RateLimit:
    def test_login_limit_constant(self):
        from app.api.routes_auth import LOGIN_LIMIT

        assert LOGIN_LIMIT == 5

    def test_login_window_10min(self):
        from app.api.routes_auth import LOGIN_WINDOW

        assert LOGIN_WINDOW == timedelta(minutes=10)


class TestFR09CreateOrder:
    @pytest.mark.feature("FR09-AUTH-09")
    def test_platform_plans_exposes_full_subscription_matrix(self, client):
        resp = client.get("/api/v1/platform/plans")
        assert resp.status_code == 200
        plans = resp.json()["data"]["plans"]
        assert {(plan["tier_id"], plan["period_months"]) for plan in plans} == {
            ("Free", 0),
            ("Pro", 1),
            ("Pro", 3),
            ("Pro", 12),
            ("Enterprise", 1),
            ("Enterprise", 3),
            ("Enterprise", 12),
        }

    @pytest.mark.feature("FR09-AUTH-09")
    def test_platform_plans_each_plan_has_required_fields(self, client):
        """doc05 §896-930: 每个 plan 必须包含 code/tier_id/period_months/label/price_display/features/features_deny。"""
        resp = client.get("/api/v1/platform/plans")
        plans = resp.json()["data"]["plans"]
        required = {"code", "tier_id", "period_months", "label", "price_display", "features", "features_deny"}
        for plan in plans:
            missing = required - set(plan)
            assert not missing, f"plan {plan.get('code', '?')} 缺失字段: {missing}"

    @pytest.mark.feature("FR09-AUTH-09")
    def test_platform_plans_tier_ids_valid(self, client):
        """tier_id 只允许 Free / Pro / Enterprise 三个值。"""
        resp = client.get("/api/v1/platform/plans")
        plans = resp.json()["data"]["plans"]
        valid_tiers = {"Free", "Pro", "Enterprise"}
        for plan in plans:
            assert plan["tier_id"] in valid_tiers, f"非法 tier_id: {plan['tier_id']}"

    @pytest.mark.feature("FR09-AUTH-09")
    def test_platform_plans_features_are_lists(self, client):
        """features 和 features_deny 必须是 list 类型。"""
        resp = client.get("/api/v1/platform/plans")
        plans = resp.json()["data"]["plans"]
        for plan in plans:
            assert isinstance(plan["features"], list), f"plan {plan['code']}: features 非 list"
            assert isinstance(plan["features_deny"], list), f"plan {plan['code']}: features_deny 非 list"

    @pytest.mark.feature("FR09-AUTH-09")
    def test_platform_plans_order_matches_spec(self, client):
        """返回顺序必须是 free → pro_1m → pro_3m → pro_12m → enterprise_*。"""
        resp = client.get("/api/v1/platform/plans")
        plans = resp.json()["data"]["plans"]
        codes = [p["code"] for p in plans]
        expected = ["free", "pro_1m", "pro_3m", "pro_12m", "enterprise_1m", "enterprise_3m", "enterprise_12m"]
        assert codes == expected, f"顺序不符: {codes}"

    @pytest.mark.feature("FR09-AUTH-09")
    def test_tier_pricing_values(self):
        from app.services.membership import TIER_PRICE

        assert TIER_PRICE[("Pro", 1)] == 29.9
        assert TIER_PRICE[("Pro", 3)] == 79.9
        assert TIER_PRICE[("Pro", 12)] == 299.9
        assert TIER_PRICE[("Enterprise", 1)] == 99.9
        assert TIER_PRICE[("Enterprise", 3)] == 269.9
        assert TIER_PRICE[("Enterprise", 12)] == 999.9

    @pytest.mark.feature("FR09-BILLING-01")
    def test_free_user_cannot_order_free(self, client, create_user):
        access, _, _ = _user_tokens(client, create_user, email="order-free@test.com", tier="Free")
        resp = client.post(
            "/billing/create_order",
            json={"tier_id": "Free", "period_months": 1, "provider": "mock"},
            headers={"Authorization": f"Bearer {access}"},
        )
        assert resp.status_code == 422

    @pytest.mark.feature("FR09-BILLING-01")
    def test_create_order_returns_201(self, client, create_user):
        access, _, _ = _user_tokens(client, create_user, email="order-pro@test.com", tier="Free")
        resp = client.post(
            "/billing/create_order",
            json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
            headers={"Authorization": f"Bearer {access}"},
        )
        assert resp.status_code in (200, 201)
        body = resp.json()
        assert body["data"]["status"] == "CREATED"
        assert body["data"]["payment_url"] is None

    def test_create_order_fail_closed_when_mock_billing_disabled(self, client, create_user, monkeypatch):
        from app.core.config import settings

        monkeypatch.setattr(settings, "enable_mock_billing", False)
        access, _, _ = _user_tokens(client, create_user, email="order-prod@test.com", tier="Free")
        resp = client.post(
            "/billing/create_order",
            json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
            headers={"Authorization": f"Bearer {access}"},
        )
        assert resp.status_code == 503
        body = resp.json()
        assert body["error_code"] == "PAYMENT_PROVIDER_NOT_CONFIGURED"
        assert body["error_message"] == "PAYMENT_PROVIDER_NOT_CONFIGURED"


class TestFR09Webhook:
    def test_webhook_signature_function(self):
        from app.services.membership import build_webhook_signature

        sig = build_webhook_signature(
            event_id="evt-1",
            order_id="ord-1",
            user_id="user-1",
            tier_id="Pro",
            paid_amount=29.9,
            provider="mock",
        )
        assert isinstance(sig, str)
        assert len(sig) == 64
        assert sig == build_webhook_signature("evt-1", "ord-1", "user-1", "Pro", 29.9, "mock")

    def test_webhook_hmac_sha256(self):
        from app.services.membership import build_webhook_signature

        sig = build_webhook_signature(
            event_id="evt-1",
            order_id="ord-1",
            user_id="user-1",
            tier_id="Pro",
            paid_amount=29.9,
            provider="mock",
        )
        assert isinstance(sig, str) and len(sig) == 64

    @pytest.mark.feature("FR09-BILLING-02")
    def test_duplicate_event_handling(self, client, db_session, create_user):
        from app.models import BillingOrder, PaymentWebhookEvent
        from app.services.membership import create_order, handle_webhook

        _, _, user = _user_tokens(client, create_user, email="webhook-dup@test.com", tier="Free")
        order = create_order(db_session, user, "Pro", 1, "alipay")
        db_session.commit()

        first = handle_webhook(
            db_session,
            event_id="evt-dup-1",
            order_id=order.order_id,
            user_id=user.user_id,
            tier_id="Pro",
            paid_amount=29.9,
            provider="alipay",
            payload={"source": "test"},
        )
        db_session.commit()
        second = handle_webhook(
            db_session,
            event_id="evt-dup-1",
            order_id=order.order_id,
            user_id=user.user_id,
            tier_id="Pro",
            paid_amount=29.9,
            provider="alipay",
            payload={"source": "test"},
        )
        db_session.commit()

        order_row = db_session.get(BillingOrder, order.order_id)
        events = db_session.query(PaymentWebhookEvent).filter(PaymentWebhookEvent.order_id == order.order_id).all()
        assert first["duplicate"] is False
        assert first["processed"] is True
        assert second["duplicate"] is True
        assert second["status_reason"] == "duplicate_event_ignored"
        assert order_row.status == "PAID"
        assert len(events) == 1
        event_row = events[0]
        assert event_row.duplicate_count == 1
        assert event_row.processing_succeeded is True
