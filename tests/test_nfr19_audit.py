"""
NFR-19 审计覆盖 — 验收用例
冻结测试名: test_nfr19_admin_patch_audit,
            test_nfr19_force_regenerate_audit,
            test_nfr19_billing_reconcile_audit
来源: docs/core/01_需求基线.md §3 NFR-19 追溯矩阵
"""
from __future__ import annotations

from datetime import date
from uuid import uuid4

from sqlalchemy import select

from app.models import (
    AdminOperation,
    AuditLog,
    BillingOrder,
    PaymentWebhookEvent,
    Report,
)
from tests.helpers_ssot import insert_report_bundle_ssot


def _login(client, create_user, *, role="admin"):
    info = create_user(
        email=f"nfr19-{uuid4().hex[:6]}@test.com",
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


def _make_report(db) -> Report:
    return insert_report_bundle_ssot(
        db,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date="2025-06-01",
        recommendation="BUY",
        confidence=0.72,
        strategy_type="A",
        published=False,
        review_flag="NONE",
    )


def _add_successful_payment_webhook(db_session, *, order_id: str, provider: str = "alipay"):
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    db_session.add(
        PaymentWebhookEvent(
            event_id=f"evt-nfr19-{uuid4().hex}",
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


# ──── 1. admin PATCH 审计 ─────────────────────────────
def test_nfr19_admin_patch_audit(client, create_user, db_session):
    """管理员 PATCH 报告 → audit_log 含 actor/before/after/timestamp/request_id"""
    headers, admin = _login(client, create_user, role="admin")
    report = _make_report(db_session)
    rid = headers["X-Request-ID"]

    resp = client.patch(
        f"/api/v1/admin/reports/{report.report_id}",
        json={"review_flag": "APPROVED"},
        headers=headers,
    )
    assert resp.status_code == 200

    # Check audit_log
    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "PATCH_REPORT",
            AuditLog.target_pk == report.report_id,
        )
    ).scalars().first()
    assert audit is not None
    assert audit.actor_user_id == admin.user_id
    assert audit.request_id == rid
    assert audit.created_at is not None  # timestamp
    assert audit.before_snapshot is not None
    assert audit.after_snapshot is not None


def test_nfr19_admin_user_patch_audit(client, create_user, db_session):
    headers, admin = _login(client, create_user, role="admin")
    target = create_user(
        email=f"patch-target-{uuid4().hex[:6]}@test.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )["user"]
    rid = headers["X-Request-ID"]

    resp = client.patch(
        f"/api/v1/admin/users/{target.user_id}",
        json={"tier": "Pro"},
        headers=headers,
    )
    assert resp.status_code == 200

    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "PATCH_USER",
            AuditLog.target_pk == str(target.user_id),
        )
    ).scalars().first()
    assert audit is not None
    assert audit.actor_user_id == admin.user_id
    assert audit.request_id == rid
    assert audit.before_snapshot["tier"] == "Free"
    assert audit.after_snapshot["tier"] == "Pro"

    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "PATCH_USER",
            AdminOperation.target_pk == str(target.user_id),
        )
    ).scalars().first()
    assert op is not None
    assert op.request_id == rid
    assert op.before_snapshot["tier"] == "Free"
    assert op.after_snapshot["tier"] == "Pro"


def test_nfr19_admin_user_patch_rejected_forbidden_super_admin_target_writes_audit(
    client, create_user, db_session
):
    headers, admin = _login(client, create_user, role="admin")
    target = create_user(
        email=f"patch-super-target-{uuid4().hex[:6]}@test.com",
        password="Password123",
        tier="Enterprise",
        role="super_admin",
        email_verified=True,
    )["user"]
    rid = headers["X-Request-ID"]

    resp = client.patch(
        f"/api/v1/admin/users/{target.user_id}",
        json={"tier": "Free"},
        headers=headers,
    )
    assert resp.status_code == 403
    assert resp.json()["error_code"] == "FORBIDDEN"

    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "PATCH_USER",
            AuditLog.target_pk == str(target.user_id),
            AuditLog.request_id == rid,
        )
    ).scalars().first()
    assert audit is not None
    assert audit.actor_user_id == admin.user_id
    assert audit.failure_category == "protected_super_admin_target"
    assert audit.after_snapshot["status"] == "REJECTED"
    assert audit.after_snapshot["error_code"] == "FORBIDDEN"

    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "PATCH_USER",
            AdminOperation.target_pk == str(target.user_id),
            AdminOperation.request_id == rid,
        )
    ).scalars().first()
    assert op is not None
    assert op.status == "REJECTED"
    assert op.status_reason == "protected_super_admin_target"


def test_nfr19_admin_user_patch_rejected_invalid_role_writes_audit(client, create_user, db_session):
    headers, admin = _login(client, create_user, role="super_admin")
    target = create_user(
        email=f"patch-invalid-role-{uuid4().hex[:6]}@test.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )["user"]
    rid = headers["X-Request-ID"]

    resp = client.patch(
        f"/api/v1/admin/users/{target.user_id}",
        json={"role": "super_admin"},
        headers=headers,
    )
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "INVALID_PAYLOAD"

    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "PATCH_USER",
            AuditLog.target_pk == str(target.user_id),
            AuditLog.request_id == rid,
        )
    ).scalars().first()
    assert audit is not None
    assert audit.actor_user_id == admin.user_id
    assert audit.failure_category == "invalid_role"
    assert audit.after_snapshot["status"] == "REJECTED"
    assert audit.after_snapshot["error_code"] == "INVALID_PAYLOAD"

    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "PATCH_USER",
            AdminOperation.target_pk == str(target.user_id),
            AdminOperation.request_id == rid,
        )
    ).scalars().first()
    assert op is not None
    assert op.status == "REJECTED"
    assert op.status_reason == "invalid_role"


# ──── 2. force-regenerate 审计 ────────────────────────
def test_nfr19_force_regenerate_audit(client, create_user, db_session):
    """force-regenerate → audit_log 含完整审计五要素"""
    headers, admin = _login(client, create_user, role="super_admin")
    report = _make_report(db_session)
    rid = headers["X-Request-ID"]

    resp = client.post(
        f"/api/v1/admin/reports/{report.report_id}/force-regenerate",
        json={"force_regenerate": True, "reason_code": "data_quality"},
        headers=headers,
    )
    assert resp.status_code == 200

    # Check audit_log
    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "FORCE_REGENERATE",
            AuditLog.target_pk == report.report_id,
        )
    ).scalars().first()
    assert audit is not None
    assert audit.actor_user_id == admin.user_id
    assert audit.request_id == rid
    assert audit.created_at is not None
    assert audit.before_snapshot is not None
    assert audit.after_snapshot is not None

    # Check admin_operation
    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "FORCE_REGENERATE",
            AdminOperation.target_pk == report.report_id,
        )
    ).scalars().first()
    assert op is not None
    assert op.reason_code == "data_quality"
    assert op.status == "COMPLETED"


def test_nfr19_force_regenerate_rejected_path_writes_audit(client, create_user, db_session):
    from tests.helpers_ssot import insert_open_position

    headers, admin = _login(client, create_user, role="super_admin")
    report = _make_report(db_session)
    rid = headers["X-Request-ID"]

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
        json={"force_regenerate": True, "reason_code": "guard_blocked"},
        headers=headers,
    )
    assert resp.status_code == 409

    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "FORCE_REGENERATE",
            AuditLog.target_pk == report.report_id,
        )
    ).scalars().first()
    assert audit is not None
    assert audit.actor_user_id == admin.user_id
    assert audit.request_id == rid
    assert audit.after_snapshot["status"] == "REJECTED"
    assert audit.after_snapshot["error_code"] == "REPORT_ALREADY_REFERENCED_BY_SIM"

    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "FORCE_REGENERATE",
            AdminOperation.target_pk == report.report_id,
        )
    ).scalars().first()
    assert op is not None
    assert op.status == "REJECTED"
    assert op.status_reason == "REPORT_ALREADY_REFERENCED_BY_SIM"


def test_nfr19_force_regenerate_precheck_403_writes_rejected_audit(client, create_user, db_session):
    headers, admin = _login(client, create_user, role="admin")
    report = _make_report(db_session)
    rid = headers["X-Request-ID"]

    resp = client.post(
        f"/api/v1/admin/reports/{report.report_id}/force-regenerate",
        json={"force_regenerate": True, "reason_code": "role_gate"},
        headers=headers,
    )
    assert resp.status_code == 403
    assert resp.json()["error_code"] == "FORBIDDEN"

    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "FORCE_REGENERATE",
            AuditLog.target_pk == report.report_id,
            AuditLog.request_id == rid,
        )
    ).scalars().first()
    assert audit is not None
    assert audit.actor_user_id == admin.user_id
    assert audit.failure_category == "role_requires_super_admin"
    assert audit.after_snapshot["status"] == "REJECTED"
    assert audit.after_snapshot["error_code"] == "FORBIDDEN"

    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "FORCE_REGENERATE",
            AdminOperation.target_pk == report.report_id,
            AdminOperation.request_id == rid,
        )
    ).scalars().first()
    assert op is not None
    assert op.status == "REJECTED"
    assert op.status_reason == "role_requires_super_admin"


# ──── 3. billing reconcile 审计 ───────────────────────
def test_nfr19_billing_reconcile_audit(client, create_user, db_session):
    """billing reconcile → audit_log 含完整审计五要素"""
    headers, admin = _login(client, create_user, role="admin")
    rid = headers["X-Request-ID"]

    order = BillingOrder()
    order.order_id = str(uuid4())
    order.user_id = admin.user_id
    order.provider = "alipay"
    order.expected_tier = "Pro"
    order.period_months = 1
    order.currency = "CNY"
    order.status = "PENDING"
    order.amount_cny = 99.0
    db_session.add(order)
    db_session.commit()
    _add_successful_payment_webhook(db_session, order_id=order.order_id, provider=order.provider)

    resp = client.post(
        f"/api/v1/admin/billing/orders/{order.order_id}/reconcile",
        json={
            "provider": "alipay",
            "order_id": order.order_id,
            "expected_tier": "Pro",
            "reason_code": "manual",
        },
        headers=headers,
    )
    assert resp.status_code == 200

    # Check audit_log
    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "RECONCILE_ORDER",
            AuditLog.target_pk == order.order_id,
        )
    ).scalars().first()
    assert audit is not None
    assert audit.actor_user_id == admin.user_id
    assert audit.request_id == rid
    assert audit.created_at is not None
    assert audit.before_snapshot is not None
    assert audit.after_snapshot is not None
    assert audit.after_snapshot["provider_truth_source"] == "payment_webhook_event"

    # Check admin_operation
    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "RECONCILE_ORDER",
            AdminOperation.target_pk == order.order_id,
        )
    ).scalars().first()
    assert op is not None
    assert op.reason_code == "manual"
    assert op.status == "COMPLETED"


def test_nfr19_billing_reconcile_rejects_local_paid_without_webhook(client, create_user, db_session):
    headers, admin = _login(client, create_user, role="admin")
    rid = headers["X-Request-ID"]

    order = BillingOrder()
    order.order_id = str(uuid4())
    order.user_id = admin.user_id
    order.provider = "alipay"
    order.expected_tier = "Pro"
    order.period_months = 1
    order.currency = "CNY"
    order.status = "PAID"
    order.amount_cny = 29.9
    db_session.add(order)
    db_session.commit()

    resp = client.post(
        f"/api/v1/admin/billing/orders/{order.order_id}/reconcile",
        json={
            "provider": "alipay",
            "order_id": order.order_id,
            "expected_tier": "Pro",
            "reason_code": "local_paid_not_truth",
        },
        headers=headers,
    )
    assert resp.status_code == 503
    assert resp.json()["error_code"] == "UPSTREAM_TIMEOUT"

    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "RECONCILE_ORDER",
            AuditLog.target_pk == order.order_id,
        )
    ).scalars().first()
    assert audit is not None
    assert audit.actor_user_id == admin.user_id
    assert audit.request_id == rid
    assert audit.failure_category == "reconcile_probe_missing"
    assert audit.before_snapshot["order_status"] == "PAID"
    assert audit.after_snapshot["provider_truth_source"] is None

    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "RECONCILE_ORDER",
            AdminOperation.target_pk == order.order_id,
        )
    ).scalars().first()
    assert op is not None
    assert op.status == "FAILED"
    assert op.status_reason == "reconcile_probe_missing"


def test_nfr19_billing_reconcile_failed_probe_is_audited(client, create_user, db_session):
    headers, admin = _login(client, create_user, role="admin")
    rid = headers["X-Request-ID"]
    target = create_user(
        email="nfr19-reconcile-target@example.com",
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

    resp = client.post(
        f"/api/v1/admin/billing/orders/{order.order_id}/reconcile",
        json={
            "provider": "alipay",
            "order_id": order.order_id,
            "expected_tier": "Pro",
            "reason_code": "missing_provider_truth",
        },
        headers=headers,
    )
    assert resp.status_code == 503

    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "RECONCILE_ORDER",
            AuditLog.target_pk == order.order_id,
        )
    ).scalars().first()
    assert audit is not None
    assert audit.actor_user_id == admin.user_id
    assert audit.request_id == rid
    assert audit.failure_category == "reconcile_probe_missing"
    assert audit.before_snapshot["order_status"] == "EXPIRED"
    assert audit.after_snapshot["reconciled"] is False
    assert audit.after_snapshot["error_code"] == "UPSTREAM_TIMEOUT"

    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "RECONCILE_ORDER",
            AdminOperation.target_pk == order.order_id,
        )
    ).scalars().first()
    assert op is not None
    assert op.status == "FAILED"
    assert op.status_reason == "reconcile_probe_missing"


def test_nfr19_billing_reconcile_precheck_403_writes_rejected_audit(client, create_user, db_session):
    headers, super_admin = _login(client, create_user, role="super_admin")
    rid = headers["X-Request-ID"]

    order = BillingOrder()
    order.order_id = str(uuid4())
    order.user_id = super_admin.user_id
    order.provider = "alipay"
    order.expected_tier = "Pro"
    order.period_months = 1
    order.currency = "CNY"
    order.status = "PENDING"
    order.amount_cny = 29.9
    db_session.add(order)
    db_session.commit()

    resp = client.post(
        f"/api/v1/admin/billing/orders/{order.order_id}/reconcile",
        json={
            "provider": "alipay",
            "order_id": order.order_id,
            "expected_tier": "Pro",
            "reason_code": "role_gate",
        },
        headers=headers,
    )
    assert resp.status_code == 403
    assert resp.json()["error_code"] == "FORBIDDEN"

    audit = db_session.execute(
        select(AuditLog).where(
            AuditLog.action_type == "RECONCILE_ORDER",
            AuditLog.target_pk == order.order_id,
            AuditLog.request_id == rid,
        )
    ).scalars().first()
    assert audit is not None
    assert audit.actor_user_id == super_admin.user_id
    assert audit.failure_category == "role_requires_exact_admin"
    assert audit.after_snapshot["status"] == "REJECTED"
    assert audit.after_snapshot["error_code"] == "FORBIDDEN"

    op = db_session.execute(
        select(AdminOperation).where(
            AdminOperation.action_type == "RECONCILE_ORDER",
            AdminOperation.target_pk == order.order_id,
            AdminOperation.request_id == rid,
        )
    ).scalars().first()
    assert op is not None
    assert op.status == "REJECTED"
    assert op.status_reason == "role_requires_exact_admin"
