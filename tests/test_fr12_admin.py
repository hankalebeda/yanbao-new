"""
FR-12 管理后台补全 — 验收用例
验收断言来源：docs/core/01_需求基线.md §FR-12 追溯矩阵
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
import app.services.trade_calendar as trade_calendar
from sqlalchemy import select
from app.models import (
    AdminOperation,
    AuditLog,
    Base,
    BillingOrder,
    PaymentWebhookEvent,
    Report,
    User,
)
from tests.helpers_ssot import (
    insert_market_state_cache,
    insert_open_position,
    insert_pool_snapshot,
    insert_report_bundle_ssot,
)

pytestmark = [
    pytest.mark.feature("FR12-ADMIN-01"),
    pytest.mark.feature("FR12-ADMIN-02"),
    pytest.mark.feature("FR12-ADMIN-03"),
    pytest.mark.feature("FR12-ADMIN-04"),
    pytest.mark.feature("FR12-ADMIN-05"),
]


# ──── helpers ───────────────────────────────────────────
def _login(client, create_user, *, role="admin", email_prefix="adm"):
    """Create user with given role and return auth headers."""
    info = create_user(
        email=f"{email_prefix}-{uuid4().hex[:6]}@test.com",
        password="Password123",
        role=role,
        email_verified=True,
    )
    resp = client.post(
        "/auth/login",
        json={"email": info["user"].email, "password": info["password"]},
    )
    assert resp.status_code == 200
    token = resp.json()["data"]["access_token"]
    return {
        "Authorization": f"Bearer {token}",
        "X-Request-ID": str(uuid4()),
    }, info["user"]


def _make_report(db, *, stock_code="600000.SH", trade_date="2025-12-01") -> Report:
    return insert_report_bundle_ssot(
        db,
        stock_code=stock_code,
        stock_name="TEST_STOCK",
        trade_date=trade_date,
        recommendation="BUY",
        confidence=0.6,
        strategy_type="A",
        published=False,
        review_flag="NONE",
    )


# ──── 1. 非 admin → 403 ────────────────────────────────
def _add_successful_payment_webhook(db_session, *, order_id: str, provider: str = "alipay"):
    now = datetime.now(timezone.utc)
    db_session.add(
        PaymentWebhookEvent(
            event_id=f"evt-reconcile-{uuid4().hex}",
            order_id=order_id,
            provider=provider,
            event_type="PAYMENT_SUCCEEDED",
            payload_json={"source": "pytest"},
            processing_succeeded=True,
            duplicate_count=0,
            received_at=now,
            processed_at=now,
        )
    )
    db_session.commit()


def test_fr12_admin_403_non_admin(client, db_session):
    """非 admin 用户访问任何 admin 端点 → 401/403"""
    resp = client.get("/api/v1/admin/overview")
    assert resp.status_code in (401, 403)


# ──── 2. GET /admin/overview 最小字段 ──────────────────
def test_fr12_overview_min_fields(client, create_user, db_session):
    """overview 返回全部最小字段且类型正确"""
    headers, _ = _login(client, create_user, role="admin")
    resp = client.get("/api/v1/admin/overview", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    data = body["data"]

    # 必须字段存在且类型正确
    assert isinstance(data["pool_size"], int)
    assert isinstance(data["today_reports"], int)
    assert isinstance(data["today_buy_signals"], int)
    assert isinstance(data["pending_review"], int)
    assert isinstance(data["source_dates"], dict)
    for field in (
        "runtime_trade_date",
        "public_pool_trade_date",
        "latest_published_report_trade_date",
        "stats_snapshot_date",
        "sim_snapshot_date",
    ):
        assert field in data["source_dates"]

    # active_positions 是 dict，key 集合与 CAPITAL_TIERS 一致
    ap = data["active_positions"]
    assert isinstance(ap, dict)
    assert set(ap.keys()) == {"10k", "100k", "500k"}
    for v in ap.values():
        assert isinstance(v, int)


# ──── 3. billing reconcile 幂等 ────────────────────────
def test_fr12_overview_counts_buy_signals_by_trade_date(client, create_user, db_session, monkeypatch):
    trade_day = date(2026, 3, 13)

    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-13")
    headers, _ = _login(client, create_user, role="admin", email_prefix="overview")
    report = _make_report(db_session, trade_date="2026-03-13")
    report.confidence = 0.72
    db_session.commit()

    response = client.get("/api/v1/admin/overview", headers=headers)

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["today_reports"] == 1
    assert data["today_buy_signals"] == 1


def test_fr12_admin_users_supports_tier_filter_and_sort(client, create_user, db_session):
    headers, _ = _login(client, create_user, role="admin", email_prefix="users-filter")
    pro_old = create_user(
        email="pro-old@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
    )["user"]
    pro_new = create_user(
        email="pro-new@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
    )["user"]
    create_user(
        email="free-only@example.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )
    pro_old.last_login_at = datetime(2026, 3, 10, 8, 0, 0)
    pro_new.last_login_at = datetime(2026, 3, 11, 8, 0, 0)
    db_session.commit()

    response = client.get(
        "/api/v1/admin/users?tier=Pro&sort=-last_login_at&page=1&page_size=10",
        headers=headers,
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["total"] >= 2
    assert all(item["tier"] == "Pro" for item in data["items"])
    assert [item["email"] for item in data["items"][:2]] == [
        "pro-new@example.com",
        "pro-old@example.com",
    ]


def test_fr12_admin_user_patch_forbids_admin_downgrading_admin_target(client, create_user, db_session):
    headers, actor = _login(client, create_user, role="admin", email_prefix="patch-protect-admin")
    target = create_user(
        email="admin-target@example.com",
        password="Password123",
        tier="Pro",
        role="admin",
        email_verified=True,
    )["user"]

    response = client.patch(
        f"/api/v1/admin/users/{target.user_id}",
        headers=headers,
        json={"role": "user"},
    )

    assert response.status_code == 403
    assert response.json()["error_code"] == "FORBIDDEN"
    refreshed = db_session.get(User, target.user_id)
    assert refreshed is not None
    assert refreshed.role == "admin"

    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "PATCH_USER",
            AdminOperation.target_pk == str(target.user_id),
            AdminOperation.request_id == headers["X-Request-ID"],
        )
    ).scalars().first()
    assert op is not None
    assert op.status == "REJECTED"
    assert op.status_reason == "role_change_requires_super_admin"
    assert op.actor_user_id == actor.user_id


def test_fr12_admin_user_patch_forbids_admin_changing_super_admin_tier(client, create_user, db_session):
    headers, _ = _login(client, create_user, role="admin", email_prefix="patch-protect-super")
    target = create_user(
        email="super-target@example.com",
        password="Password123",
        tier="Enterprise",
        role="super_admin",
        email_verified=True,
    )["user"]

    response = client.patch(
        f"/api/v1/admin/users/{target.user_id}",
        headers=headers,
        json={"tier": "Free"},
    )

    assert response.status_code == 403
    assert response.json()["error_code"] == "FORBIDDEN"
    refreshed = db_session.get(User, target.user_id)
    assert refreshed is not None
    assert refreshed.tier == "Enterprise"

    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "PATCH_USER",
            AdminOperation.target_pk == str(target.user_id),
            AdminOperation.request_id == headers["X-Request-ID"],
        )
    ).scalars().first()
    assert op is not None
    assert op.status == "REJECTED"
    assert op.status_reason == "protected_super_admin_target"


def test_fr12_admin_reports_supports_sort_and_full_summary_fields(client, create_user, db_session):
    headers, _ = _login(client, create_user, role="admin", email_prefix="reports-sort")
    low = _make_report(db_session, stock_code="600000.SH", trade_date="2025-12-01")
    high = _make_report(db_session, stock_code="600001.SH", trade_date="2025-12-02")
    low.confidence = 0.31
    high.confidence = 0.92
    high.strategy_type = "C"
    high.publish_status = "PUBLISHED"
    high.review_flag = "PENDING_REVIEW"
    high.status_reason = "manual_review_required"
    db_session.commit()

    response = client.get(
        "/api/v1/admin/reports?review_flag=PENDING_REVIEW&sort=-confidence&page=1&page_size=10",
        headers=headers,
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["total"] >= 1
    item = data["items"][0]
    assert item["report_id"] == high.report_id
    assert item["confidence"] == 0.92
    assert item["strategy_type"] == "C"
    assert item["publish_status"] == "PUBLISHED"
    assert item["status_reason"] == "manual_review_required"


def test_fr12_billing_reconcile_idempotent(client, create_user, db_session):
    """同 order_id 补单两次 → 第二次幂等返回、不重复发放权益"""
    headers, admin_user = _login(client, create_user, role="admin")

    # 创建一个 PAID billing_order
    order = BillingOrder()
    order.order_id = str(uuid4())
    order.user_id = admin_user.user_id
    order.provider = "alipay"
    order.expected_tier = "Pro"
    order.period_months = 1
    order.currency = "CNY"
    order.status = "PENDING"
    order.amount_cny = 99.0
    db_session.add(order)
    db_session.commit()
    _add_successful_payment_webhook(db_session, order_id=order.order_id, provider=order.provider)

    payload = {
        "provider": "alipay",
        "order_id": order.order_id,
        "expected_tier": "Pro",
        "reason_code": "manual_reconcile",
    }

    # First call
    resp1 = client.post(
        f"/api/v1/admin/billing/orders/{order.order_id}/reconcile",
        json=payload,
        headers=headers,
    )
    assert resp1.status_code == 200
    data1 = resp1.json()["data"]
    assert data1["reconciled"] is True
    assert data1["operation_id"] is not None

    db_session.refresh(order)
    db_session.refresh(admin_user)
    assert order.status == "PAID"
    assert order.granted_tier == "Pro"
    assert order.paid_at is not None
    assert admin_user.tier == "Pro"
    assert admin_user.tier_expires_at is not None
    resp2 = client.post(
        f"/api/v1/admin/billing/orders/{order.order_id}/reconcile",
        json=payload,
        headers=headers,
    )
    assert resp2.status_code == 200
    data2 = resp2.json()["data"]
    assert data2["reconciled"] is True
    assert data2["operation_id"] == data1["operation_id"]

    ops = (
        db_session.query(AdminOperation)
        .filter_by(action_type="RECONCILE_ORDER", target_pk=order.order_id)
        .all()
    )
    assert len(ops) == 1
    assert ops[0].after_snapshot["provider_truth_source"] == "payment_webhook_event"


def test_fr12_billing_reconcile_requires_provider_truth_before_granting(client, create_user, db_session):
    headers, _ = _login(client, create_user, role="admin", email_prefix="reconcile-guard")
    target = create_user(
        email="reconcile-target@example.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )["user"]

    order = BillingOrder()
    order.order_id = str(uuid4())
    order.user_id = target.user_id
    order.provider = "alipay"
    order.expected_tier = "Pro"
    order.period_months = 1
    order.currency = "CNY"
    order.status = "EXPIRED"
    order.status_reason = "reconcile_probe_missing"
    order.amount_cny = 29.9
    db_session.add(order)
    db_session.commit()

    response = client.post(
        f"/api/v1/admin/billing/orders/{order.order_id}/reconcile",
        json={
            "provider": "alipay",
            "order_id": order.order_id,
            "expected_tier": "Pro",
            "reason_code": "provider_truth_guard",
        },
        headers=headers,
    )

    assert response.status_code == 503
    body = response.json()
    assert body["error_code"] == "UPSTREAM_TIMEOUT"

    db_session.refresh(order)
    db_session.refresh(target)
    assert order.status == "EXPIRED"
    assert order.granted_tier is None
    assert order.paid_at is None
    assert target.tier == "Free"

    failed_op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "RECONCILE_ORDER",
            AdminOperation.target_pk == order.order_id,
        )
    ).scalars().first()
    assert failed_op is not None
    assert failed_op.status == "FAILED"
    assert failed_op.status_reason == "reconcile_probe_missing"

    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "RECONCILE_ORDER",
            AuditLog.target_pk == order.order_id,
        )
    ).scalars().first()
    assert audit is not None
    assert audit.failure_category == "reconcile_probe_missing"
    assert audit.after_snapshot["reconciled"] is False
    assert audit.after_snapshot["error_code"] == "UPSTREAM_TIMEOUT"
    assert audit.after_snapshot["provider_truth_source"] is None
    payload = {
        "provider": "alipay",
        "order_id": order.order_id,
        "expected_tier": "Pro",
        "reason_code": "provider_truth_guard",
    }

    # Second call → idempotent
    resp2 = client.post(
        f"/api/v1/admin/billing/orders/{order.order_id}/reconcile",
        json=payload,
        headers=headers,
    )
    assert resp2.status_code == 503
    assert resp2.json()["error_code"] == "UPSTREAM_TIMEOUT"

    # Check: only one admin_operation for RECONCILE_ORDER on this order
    ops = (
        db_session.query(AdminOperation)
        .filter_by(action_type="RECONCILE_ORDER", target_pk=order.order_id)
        .all()
    )
    assert len(ops) == 1  # idempotent — single operation


def test_fr12_billing_reconcile_rejects_local_paid_without_webhook(client, create_user, db_session):
    headers, admin_user = _login(client, create_user, role="admin", email_prefix="reconcile-local-paid")

    order = BillingOrder()
    order.order_id = str(uuid4())
    order.user_id = admin_user.user_id
    order.provider = "alipay"
    order.expected_tier = "Pro"
    order.period_months = 1
    order.currency = "CNY"
    order.status = "PAID"
    order.amount_cny = 99.0
    db_session.add(order)
    db_session.commit()

    response = client.post(
        f"/api/v1/admin/billing/orders/{order.order_id}/reconcile",
        json={
            "provider": "alipay",
            "order_id": order.order_id,
            "expected_tier": "Pro",
            "reason_code": "local_paid_should_not_be_truth",
        },
        headers=headers,
    )

    assert response.status_code == 503
    assert response.json()["error_code"] == "UPSTREAM_TIMEOUT"

    db_session.refresh(order)
    db_session.refresh(admin_user)
    assert order.granted_tier is None
    assert admin_user.tier == "Free"

    failed_op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "RECONCILE_ORDER",
            AdminOperation.target_pk == order.order_id,
        )
    ).scalars().first()
    assert failed_op is not None
    assert failed_op.status == "FAILED"
    assert failed_op.after_snapshot["provider_truth_source"] is None


def test_fr12_billing_reconcile_super_admin_forbidden(client, create_user, db_session):
    headers, super_admin = _login(client, create_user, role="super_admin", email_prefix="reconcile-sa")

    order = BillingOrder()
    order.order_id = str(uuid4())
    order.user_id = super_admin.user_id
    order.provider = "alipay"
    order.expected_tier = "Pro"
    order.period_months = 1
    order.currency = "CNY"
    order.status = "PAID"
    order.amount_cny = 99.0
    db_session.add(order)
    db_session.commit()

    response = client.post(
        f"/api/v1/admin/billing/orders/{order.order_id}/reconcile",
        json={
            "provider": "alipay",
            "order_id": order.order_id,
            "expected_tier": "Pro",
            "reason_code": "manual_reconcile",
        },
        headers=headers,
    )

    assert response.status_code == 403
    assert response.json()["error_code"] == "FORBIDDEN"
    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "RECONCILE_ORDER",
            AdminOperation.target_pk == order.order_id,
            AdminOperation.request_id == headers["X-Request-ID"],
        )
    ).scalars().first()
    assert op is not None
    assert op.status == "REJECTED"
    assert op.status_reason == "role_requires_exact_admin"
    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "RECONCILE_ORDER",
            AuditLog.target_pk == order.order_id,
            AuditLog.request_id == headers["X-Request-ID"],
        )
    ).scalars().first()
    assert audit is not None
    assert audit.failure_category == "role_requires_exact_admin"
    assert audit.after_snapshot["status"] == "REJECTED"
    assert audit.after_snapshot["error_code"] == "FORBIDDEN"


def test_fr12_billing_reconcile_rejects_expected_tier_mismatch_without_side_effects(client, create_user, db_session):
    headers, admin_user = _login(client, create_user, role="admin", email_prefix="reconcile-mismatch")

    order = BillingOrder()
    order.order_id = str(uuid4())
    order.user_id = admin_user.user_id
    order.provider = "alipay"
    order.expected_tier = "Pro"
    order.period_months = 1
    order.currency = "CNY"
    order.status = "PAID"
    order.amount_cny = 99.0
    db_session.add(order)
    db_session.commit()

    response = client.post(
        f"/api/v1/admin/billing/orders/{order.order_id}/reconcile",
        json={
            "provider": "alipay",
            "order_id": order.order_id,
            "expected_tier": "Enterprise",
            "reason_code": "manual_reconcile",
        },
        headers=headers,
    )

    assert response.status_code == 422
    assert response.json()["error_code"] == "INVALID_PAYLOAD"

    db_session.expire_all()
    db_user = db_session.get(type(admin_user), admin_user.user_id)
    db_order = db_session.get(BillingOrder, order.order_id)
    assert db_user.tier == "Free"
    assert db_order.granted_tier is None


# ──── 4. force-regenerate 级联阻断 ────────────────────
def test_fr12_force_regenerate_guard(client, create_user, db_session):
    """已被 sim_position 引用的研报 → force-regenerate 返回 409"""
    headers, _ = _login(client, create_user, role="super_admin")
    report = _make_report(db_session)

    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code=report.stock_code,
        capital_tier="100k",
        signal_date=str(report.trade_date),
        entry_date=str(report.trade_date),
        actual_entry_price=10.0,
        signal_entry_price=10.0,
        position_ratio=0.2,
        shares=100,
    )

    resp = client.post(
        f"/api/v1/admin/reports/{report.report_id}/force-regenerate",
        json={"force_regenerate": True, "reason_code": "test_guard"},
        headers=headers,
    )
    assert resp.status_code == 409

    # Report should NOT be soft-deleted
    db_session.expire_all()
    report = db_session.get(Report, report.report_id)
    assert report.is_deleted is False


def test_fr12_force_regenerate_guard_records_rejected_audit(client, create_user, db_session):
    headers, _ = _login(client, create_user, role="super_admin", email_prefix="regen-guard-audit")
    report = _make_report(db_session, stock_code="600101.SH", trade_date="2026-03-12")

    insert_open_position(
        db_session,
        report_id=report.report_id,
        stock_code=report.stock_code,
        capital_tier="100k",
        signal_date=str(report.trade_date),
        entry_date=str(report.trade_date),
        actual_entry_price=10.0,
        signal_entry_price=10.0,
        position_ratio=0.2,
        shares=100,
    )

    response = client.post(
        f"/api/v1/admin/reports/{report.report_id}/force-regenerate",
        json={"force_regenerate": True, "reason_code": "guard_check"},
        headers=headers,
    )

    assert response.status_code == 409

    db_session.expire_all()
    op = (
        db_session.query(AdminOperation)
        .filter_by(action_type="FORCE_REGENERATE", target_pk=report.report_id)
        .order_by(AdminOperation.created_at.desc())
        .first()
    )
    assert op is not None
    assert op.status == "REJECTED"
    assert op.status_reason == "REPORT_ALREADY_REFERENCED_BY_SIM"

    audit = (
        db_session.query(AuditLog)
        .filter_by(action_type="FORCE_REGENERATE", target_pk=report.report_id)
        .order_by(AuditLog.created_at.desc())
        .first()
    )
    assert audit is not None
    assert audit.after_snapshot["status"] == "REJECTED"
    assert audit.after_snapshot["error_code"] == "REPORT_ALREADY_REFERENCED_BY_SIM"


# ──── 5. force-regenerate 成功（super_admin） ─────────
def test_fr12_force_regenerate_success(client, create_user, db_session):
    """super_admin force-regenerate 无引用 → 成功：旧记录软删除、新 report_id、审计"""
    headers, _ = _login(client, create_user, role="super_admin")
    report = _make_report(db_session)
    old_id = report.report_id

    resp = client.post(
        f"/api/v1/admin/reports/{old_id}/force-regenerate",
        json={"force_regenerate": True, "reason_code": "data_error"},
        headers=headers,
    )
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["old_report_id"] == old_id
    assert data["new_report_id"] != old_id
    assert data["status"] == "COMPLETED"

    # Old report should be soft-deleted
    db_session.expire_all()
    old_report = db_session.get(Report, old_id)
    assert old_report.is_deleted is True

    # Audit log should exist
    audit = (
        db_session.query(AuditLog)
        .filter_by(action_type="FORCE_REGENERATE", target_pk=old_id)
        .first()
    )
    assert audit is not None


def test_fr12_force_regenerate_same_request_id_returns_same_result(client, create_user, db_session):
    headers, _ = _login(client, create_user, role="super_admin", email_prefix="regen-idem")
    report = _make_report(db_session, stock_code="600102.SH", trade_date="2026-03-12")

    first = client.post(
        f"/api/v1/admin/reports/{report.report_id}/force-regenerate",
        json={"force_regenerate": True, "reason_code": "idem"},
        headers=headers,
    )
    assert first.status_code == 200
    first_data = first.json()["data"]

    second = client.post(
        f"/api/v1/admin/reports/{report.report_id}/force-regenerate",
        json={"force_regenerate": True, "reason_code": "idem"},
        headers=headers,
    )
    assert second.status_code == 200
    second_data = second.json()["data"]
    assert second_data["new_report_id"] == first_data["new_report_id"]

    ops = (
        db_session.query(AdminOperation)
        .filter_by(action_type="FORCE_REGENERATE", target_pk=report.report_id)
        .all()
    )
    assert len(ops) == 1


# ──── 6. force-regenerate 非 super_admin → 403 ────────
def test_fr12_force_regenerate_non_super_admin_403(client, create_user, db_session):
    """普通 admin 调用 force-regenerate → 403"""
    headers, _ = _login(client, create_user, role="admin")
    report = _make_report(db_session)

    resp = client.post(
        f"/api/v1/admin/reports/{report.report_id}/force-regenerate",
        json={"force_regenerate": True, "reason_code": "test"},
        headers=headers,
    )
    assert resp.status_code == 403
    assert resp.json()["error_code"] == "FORBIDDEN"
    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "FORCE_REGENERATE",
            AdminOperation.target_pk == report.report_id,
            AdminOperation.request_id == headers["X-Request-ID"],
        )
    ).scalars().first()
    assert op is not None
    assert op.status == "REJECTED"
    assert op.status_reason == "role_requires_super_admin"
    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "FORCE_REGENERATE",
            AuditLog.target_pk == report.report_id,
            AuditLog.request_id == headers["X-Request-ID"],
        )
    ).scalars().first()
    assert audit is not None
    assert audit.failure_category == "role_requires_super_admin"
    assert audit.after_snapshot["status"] == "REJECTED"
    assert audit.after_snapshot["error_code"] == "FORBIDDEN"


def test_fr12_force_regenerate_requires_literal_true(client, create_user, db_session):
    headers, _ = _login(client, create_user, role="super_admin", email_prefix="regen-false")
    report = _make_report(db_session, stock_code="600103.SH", trade_date="2026-03-12")

    response = client.post(
        f"/api/v1/admin/reports/{report.report_id}/force-regenerate",
        json={"force_regenerate": False, "reason_code": "test"},
        headers=headers,
    )

    assert response.status_code == 422
    assert response.json()["error_code"] == "INVALID_PAYLOAD"

    db_session.expire_all()
    old_report = db_session.get(Report, report.report_id)
    assert old_report is not None
    assert old_report.is_deleted is False


def test_fr12_admin_data_purge_route_not_exposed(client, create_user):
    headers, _ = _login(client, create_user, role="super_admin", email_prefix="purge-hidden")
    response = client.post(
        "/api/v1/admin/data/purge",
        json={"confirm": True, "reason_code": "manual"},
        headers=headers,
    )
    assert response.status_code == 404


def test_fr12_force_regenerate_dependency_not_ready_hard_fails_without_side_effects(
    client, create_user, db_session, monkeypatch
):
    from app.services.report_generation_ssot import ReportGenerationServiceError

    headers, _ = _login(client, create_user, role="super_admin", email_prefix="regen-dependency")
    report = _make_report(db_session, stock_code="600300.SH", trade_date="2026-03-13")
    old_report_id = report.report_id
    report_table = Base.metadata.tables["report"]
    task_table = Base.metadata.tables["report_generation_task"]
    before_report_count = db_session.execute(report_table.select()).fetchall()
    before_task_count = db_session.execute(task_table.select()).fetchall()

    def _raise_dependency_not_ready(*args, **kwargs):
        raise ReportGenerationServiceError(503, "DEPENDENCY_NOT_READY")

    monkeypatch.setattr("app.api.routes_admin.generate_report_ssot", _raise_dependency_not_ready)

    response = client.post(
        f"/api/v1/admin/reports/{old_report_id}/force-regenerate",
        json={"force_regenerate": True, "reason_code": "dependency_not_ready"},
        headers=headers,
    )

    assert response.status_code in {500, 503}
    assert response.json()["error_code"] == "DEPENDENCY_NOT_READY"

    db_session.expire_all()
    old_report = db_session.get(Report, old_report_id)
    assert old_report is not None
    assert old_report.is_deleted is False
    after_report_count = db_session.execute(report_table.select()).fetchall()
    after_task_count = db_session.execute(task_table.select()).fetchall()
    assert len(after_report_count) == len(before_report_count)
    assert len(after_task_count) == len(before_task_count)

    failed_op = (
        db_session.query(AdminOperation)
        .filter_by(action_type="FORCE_REGENERATE", target_pk=old_report_id)
        .order_by(AdminOperation.created_at.desc())
        .first()
    )
    assert failed_op is not None
    assert failed_op.status == "FAILED"
    assert failed_op.status_reason == "DEPENDENCY_NOT_READY"


def test_fr12_admin_system_status_degrades_runtime_state_on_business_health_failure(
    client, create_user, db_session, monkeypatch
):
    headers, _ = _login(client, create_user, role="admin", email_prefix="runtime-health")
    report = _make_report(db_session, stock_code="600305.SH", trade_date="2026-03-13")
    report.quality_flag = "degraded"
    report.created_at = datetime.now(timezone.utc)
    db_session.commit()

    monkeypatch.setattr(
        "app.services.observability.prediction_stats_ssot",
        lambda db: {
            "total_judged": 120,
            "accuracy": 0.37,
            "by_window": {},
            "recent_3m": {},
        },
    )

    response = client.get("/api/v1/admin/system-status", headers=headers)

    assert response.status_code == 200
    metrics = response.json()["data"]["metrics"]
    assert metrics["report"]["degraded_rate"] > 0
    assert metrics["prediction"]["accuracy"] == 0.37
    assert str(metrics["runtime_state"]).lower() == "degraded"


def test_fr12_admin_system_status_does_not_flatten_unknown_prediction_accuracy_to_zero(
    client, create_user, db_session, monkeypatch
):
    headers, _ = _login(client, create_user, role="admin", email_prefix="runtime-unknown")
    monkeypatch.setattr(
        "app.services.observability.prediction_stats_ssot",
        lambda db: {
            "total_judged": 0,
            "accuracy": None,
            "by_window": {},
            "recent_3m": {},
        },
    )

    response = client.get("/api/v1/admin/system-status", headers=headers)

    assert response.status_code == 200
    metrics = response.json()["data"]["metrics"]
    assert metrics["prediction"]["judged_total"] == 0
    assert metrics["prediction"]["accuracy"] is None


def test_fr12_admin_system_status_exposes_layered_service_business_and_data_health(
    client, create_user, db_session, monkeypatch
):
    from app.services.runtime_anchor_service import RuntimeAnchorService

    headers, _ = _login(client, create_user, role="admin", email_prefix="runtime-layers")
    report = _make_report(db_session, stock_code="600306.SH", trade_date="2026-03-20")
    report.quality_flag = "degraded"
    report.created_at = datetime.now(timezone.utc)
    db_session.commit()

    monkeypatch.setattr(
        "app.services.observability.prediction_stats_ssot",
        lambda db: {
            "total_judged": 120,
            "accuracy": 0.37,
            "by_window": {},
            "recent_3m": {},
        },
    )
    monkeypatch.setattr(
        "app.services.observability.get_source_runtime_status",
        lambda: {"tdx_local": {"state": "ERROR"}, "akshare": {"state": "NORMAL"}},
    )
    monkeypatch.setattr(
        RuntimeAnchorService,
        "runtime_anchor_dates",
        lambda self: {
            "runtime_trade_date": "2026-03-20",
            "latest_published_report_trade_date": "2026-03-20",
            "public_pool_trade_date": "2026-03-20",
            "latest_complete_public_batch_trade_date": "2026-03-19",
            "stats_snapshot_date": None,
            "sim_snapshot_date": "2026-03-19",
        },
    )
    monkeypatch.setattr(
        RuntimeAnchorService,
        "runtime_trade_date",
        lambda self: "2026-03-20",
    )
    monkeypatch.setattr(
        "app.services.observability.get_dashboard_stats_payload_ssot",
        lambda db, window_days=30, **kwargs: {
            "data_status": "DEGRADED",
            "status_reason": "stats_history_truncated",
            "display_hint": "历史窗口覆盖不足",
            "total_settled": 12,
            "total_reports": 12,
        },
    )
    _mock_task = SimpleNamespace(trade_date="2026-03-20", status="COMPLETED", fallback_from=None)
    _mock_pool_view = SimpleNamespace(core_rows=[object()] * 200, task=_mock_task)
    monkeypatch.setattr(
        RuntimeAnchorService,
        "public_pool_snapshot",
        lambda self: {
            "pool_view": _mock_pool_view,
            "public_pool_trade_date": "2026-03-20",
            "pool_size": 200,
        },
    )
    monkeypatch.setattr(
        RuntimeAnchorService,
        "runtime_market_state_row",
        lambda self: None,
    )
    monkeypatch.setattr(
        RuntimeAnchorService,
        "public_runtime_issue",
        lambda self: None,
    )

    response = client.get("/api/v1/admin/system-status", headers=headers)

    assert response.status_code == 200
    metrics = response.json()["data"]["metrics"]
    assert metrics["runtime_state"] == "degraded"
    assert metrics["service_health"]["status"] == "degraded"
    assert "source_runtime_abnormal" in metrics["service_health"]["flags"]
    assert metrics["business_health"]["status"] == "degraded"
    assert "today_report_progress_low" in metrics["business_health"]["flags"]
    assert "prediction_accuracy_below_target" in metrics["business_health"]["flags"]
    assert metrics["business_health"]["batch_completion_rate"] < 0.8
    assert metrics["business_health"]["batch_completion_target"] == 0.8
    assert metrics["data_quality"]["status"] == "degraded"
    assert "stats_snapshot_missing" in metrics["data_quality"]["flags"]
    assert "sim_snapshot_lagging" in metrics["data_quality"]["flags"]
    assert "dashboard_stats_not_ready" in metrics["data_quality"]["flags"]
    assert "market_state_snapshot_missing" in metrics["data_quality"]["flags"]


def test_fr12_admin_ignores_future_only_public_pool_rows(client, create_user, db_session, monkeypatch):
    import app.services.ssot_read_model as read_model
    import app.services.trade_calendar as trade_calendar

    monkeypatch.setattr(read_model, "latest_trade_date_str", lambda dt=None: "2026-03-20")
    monkeypatch.setattr(trade_calendar, "latest_trade_date_str", lambda dt=None: "2026-03-20")

    headers, _ = _login(client, create_user, role="admin", email_prefix="future-pool-admin")
    insert_market_state_cache(db_session, trade_date="2026-03-20", market_state="NEUTRAL")
    insert_pool_snapshot(db_session, trade_date="2026-04-01", stock_codes=["600519.SH"])

    overview_response = client.get("/api/v1/admin/overview", headers=headers)
    system_status_response = client.get("/api/v1/admin/system-status", headers=headers)

    assert overview_response.status_code == 200
    assert system_status_response.status_code == 200
    overview = overview_response.json()["data"]
    system_status = system_status_response.json()["data"]
    assert overview["pool_size"] == 0
    assert overview["source_dates"]["runtime_trade_date"] == "2026-03-20"
    assert overview["source_dates"]["public_pool_trade_date"] is None
    assert system_status["metrics"]["runtime_anchors"]["runtime_trade_date"] == "2026-03-20"
    assert system_status["metrics"]["runtime_anchors"]["public_pool_trade_date"] is None
    assert system_status["metrics"]["business_health"]["pool_size"] == 0
    assert system_status["stock_pool"]["count"] == 0


def test_fr12_dag_retrigger_clears_success_and_forces_execution(client, create_user, db_session, monkeypatch):
    from app.services.scheduler_ops_ssot import mark_scheduler_run_success, register_scheduler_run

    headers, admin_user = _login(client, create_user, role="admin", email_prefix="retrigger")

    created = register_scheduler_run(
        db_session,
        task_name="fr07_settlement",
        trade_date="2026-03-13",
        schedule_slot="manual_seed",
        trigger_source="event",
    )
    mark_scheduler_run_success(db_session, task_run_id=str(created["task_run_id"]))
    db_session.commit()

    response = client.post(
        "/api/v1/admin/dag/retrigger",
        json={
            "task_name": "fr07_settlement",
            "trade_date": "2026-03-13",
            "reason_code": "round30_retrigger",
        },
        headers=headers,
    )

    assert response.status_code == 410
    assert response.json()["error_code"] == "ROUTE_RETIRED"
    row = db_session.execute(
        select(Base.metadata.tables["scheduler_task_run"]).where(
            Base.metadata.tables["scheduler_task_run"].c.task_run_id == created["task_run_id"]
        )
    ).first()
    assert row is not None
    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.actor_user_id == admin_user.user_id,
            AdminOperation.request_id == headers["X-Request-ID"],
        )
    ).scalars().first()
    assert op is None
    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.actor_user_id == admin_user.user_id,
            AuditLog.request_id == headers["X-Request-ID"],
        )
    ).scalars().first()
    assert audit is None


def test_internal_reports_clear_route_is_retired(client, internal_headers):
    headers = internal_headers()
    response = client.post("/api/v1/internal/reports/clear", headers=headers, json={})
    assert response.status_code == 410
    assert response.json()["error_code"] == "ROUTE_RETIRED"


def test_internal_stats_clear_route_is_retired(client, internal_headers):
    headers = internal_headers()
    response = client.post("/api/v1/internal/stats/clear", headers=headers, json={})
    assert response.status_code == 410
    assert response.json()["error_code"] == "ROUTE_RETIRED"
