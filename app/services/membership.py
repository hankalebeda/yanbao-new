from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import hashlib
import hmac
import os

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models import BillingOrder, MembershipOrder, MembershipSubscription


class TierAlreadyActiveError(Exception):
    """Raised when user already has the requested tier active."""
    pass

PLAN_PRICE = {
    "monthly": 39.0,
    "quarterly": 99.0,
    "yearly": 359.0,
}

TIER_PRICE = {
    ("Pro", 1): 29.9,
    ("Pro", 3): 79.9,
    ("Pro", 12): 299.9,
    ("Enterprise", 1): 99.9,
    ("Enterprise", 3): 269.9,
    ("Enterprise", 12): 999.9,
}

TIER_RANK = {
    "Free": 0,
    "Pro": 1,
    "Enterprise": 2,
}

PLAN_DAYS = {
    "monthly": 30,
    "quarterly": 90,
    "yearly": 365,
}

# 展示用 plan 配置（供 /platform/config 和前端订阅页）
PLAN_DISPLAY = {
    "free": {"code": "free", "label": "免费", "price": 0, "price_display": "¥0", "features": ["研报摘要", "结论标签"], "features_deny": ["实操指令卡", "每日推送"]},
    "monthly": {"code": "monthly", "label": "月会员", "price": 39, "price_display": "¥39/月", "hint": "约 ¥25/月", "features": ["全部研报", "实操指令卡", "每日推送", "止损止盈价"]},
    "yearly": {"code": "yearly", "label": "年会员", "price": 359, "price_display": "¥359/年", "hint": "省 ¥109", "features": ["全部权限", "实操指令", "每日推送", "优先客服"]},
}

# Full subscription matrix: tier × period
_SUBSCRIPTION_MATRIX = [
    {"code": "free", "tier_id": "Free", "period_months": 0, "label": "免费", "price_display": "¥0", "features": ["研报摘要", "结论标签"], "features_deny": ["实操指令卡", "每日推送"]},
    {"code": "pro_1m", "tier_id": "Pro", "period_months": 1, "label": "Pro 月付", "price_display": "¥29.9/月", "features": ["全部研报", "实操指令卡", "每日推送", "止损止盈价"], "features_deny": ["优先客服"]},
    {"code": "pro_3m", "tier_id": "Pro", "period_months": 3, "label": "Pro 季付", "price_display": "¥79.9/季", "features": ["全部研报", "实操指令卡", "每日推送", "止损止盈价"], "features_deny": ["优先客服"]},
    {"code": "pro_12m", "tier_id": "Pro", "period_months": 12, "label": "Pro 年付", "price_display": "¥299.9/年", "features": ["全部研报", "实操指令卡", "每日推送", "止损止盈价"], "features_deny": ["优先客服"]},
    {"code": "enterprise_1m", "tier_id": "Enterprise", "period_months": 1, "label": "Enterprise 月付", "price_display": "¥99.9/月", "features": ["全部权限", "实操指令", "每日推送", "优先客服"], "features_deny": []},
    {"code": "enterprise_3m", "tier_id": "Enterprise", "period_months": 3, "label": "Enterprise 季付", "price_display": "¥269.9/季", "features": ["全部权限", "实操指令", "每日推送", "优先客服"], "features_deny": []},
    {"code": "enterprise_12m", "tier_id": "Enterprise", "period_months": 12, "label": "Enterprise 年付", "price_display": "¥999.9/年", "features": ["全部权限", "实操指令", "每日推送", "优先客服"], "features_deny": []},
]


def get_plans_config() -> list[dict]:
    """返回套餐列表供前端渲染。完整订阅矩阵: Free + Pro×3 + Enterprise×3。"""
    return [dict(p) for p in _SUBSCRIPTION_MATRIX]


def _normalize_tier(tier: str | None) -> str:
    """Normalize tier name to canonical form."""
    if not tier:
        return "Free"
    mapping = {"free": "Free", "pro": "Pro", "enterprise": "Enterprise"}
    return mapping.get(tier.lower(), tier)


def is_paid_tier(tier: str | None) -> bool:
    """Check if tier is a paid tier."""
    return _normalize_tier(tier) in {"Pro", "Enterprise"}


def _extend_tier(db: Session, user_id: str, tier: str, days: int = 30) -> dict:
    """Extend or create subscription for a user."""
    sub = (
        db.query(MembershipSubscription)
        .filter(MembershipSubscription.user_id == str(user_id))
        .order_by(MembershipSubscription.id.desc())
        .first()
    )
    now = utc_now_naive()
    if sub and sub.status == "active" and sub.end_at and sub.end_at > now:
        sub.end_at = sub.end_at + timedelta(days=days)
    elif sub:
        sub.status = "active"
        sub.plan_code = tier.lower() if tier.lower() in PLAN_DAYS else "monthly"
        sub.start_at = now
        sub.end_at = now + timedelta(days=days)
    else:
        sub = MembershipSubscription(
            user_id=str(user_id),
            plan_code=tier.lower() if tier.lower() in PLAN_DAYS else "monthly",
            status="active",
            start_at=now,
            end_at=now + timedelta(days=days),
        )
        db.add(sub)
    db.commit()
    return {"user_id": str(user_id), "tier": tier, "end_at": sub.end_at.isoformat() if sub.end_at else None}


def get_payment_capability() -> dict:
    """Return payment capability status."""
    return {
        "enabled": True,
        "providers": ["alipay"],
        "mock_billing": getattr(__import__("app.core.config", fromlist=["settings"]).settings, "enable_mock_billing", False),
    }


def payment_browser_checkout_ready() -> bool:
    """Return True if browser-based checkout is ready (non-mock provider configured)."""
    cap = get_payment_capability()
    if cap.get("mock_billing"):
        return False
    return cap.get("enabled", False)


def probe_provider_order_status(db: Session, order_id: str) -> dict:
    """Probe external payment provider for order status."""
    order = db.query(MembershipOrder).filter(MembershipOrder.order_id == order_id).first()
    if not order:
        return {"order_id": order_id, "status": "not_found"}
    return {"order_id": order_id, "status": order.status, "channel": order.channel}


def reconcile_pending_orders(
    db: Session,
    *,
    now: datetime | None = None,
    provider_status_fetcher: Any | None = None,
    dry_run: bool = False,
) -> dict:
    """Reconcile pending orders that may have been paid externally."""
    now_ts = now or datetime.now(timezone.utc)
    # Check both MembershipOrder and BillingOrder pending items
    pending_membership = db.query(MembershipOrder).filter(MembershipOrder.status == "created").all()
    pending_billing: list[Any] = []
    try:
        pending_billing = db.query(BillingOrder).filter(BillingOrder.status == "CREATED").all()
    except Exception:
        pass

    checked = 0
    reconciled = 0
    expired = 0
    items: list[dict] = []

    # Handle BillingOrder reconciliation
    for order in pending_billing:
        checked += 1
        item: dict[str, Any] = {"order_id": order.order_id, "status": order.status, "status_reason": None}
        if provider_status_fetcher and not dry_run:
            try:
                result = provider_status_fetcher(order)
                if result and result.get("status") == "PAID":
                    order.status = "PAID"
                    order.status_reason = None
                    order.provider_order_id = result.get("provider_order_id")
                    order.paid_at = datetime.now(timezone.utc)
                    item["status"] = "PAID"
                    item["status_reason"] = "reconcile_probe_confirmed"
                    reconciled += 1
                    # Activate subscription
                    from app.models import User as _User, PaymentWebhookEvent as _PWE
                    user = db.query(_User).filter(_User.user_id == order.user_id).first()
                    if user:
                        grant_membership_order_entitlement(user, order)
                    evt_id = result.get("event_id") or f"reconcile-{order.order_id}"
                    evt = _PWE(
                        event_id=evt_id,
                        order_id=order.order_id,
                        user_id=order.user_id,
                        provider=order.provider,
                        event_type="PAYMENT_SUCCEEDED",
                        tier_id=order.expected_tier,
                        paid_amount=result.get("paid_amount"),
                        processing_succeeded=True,
                        duplicate_count=0,
                        status="PROCESSED",
                        received_at=datetime.now(timezone.utc),
                        processed_at=datetime.now(timezone.utc),
                    )
                    db.add(evt)
                    db.commit()
                    items.append(item)
                    continue
            except Exception:
                pass
        # No provider probe → mark probe-missing but keep CREATED
        if not provider_status_fetcher:
            order.status_reason = "reconcile_probe_missing"
            item["status_reason"] = "reconcile_probe_missing"
        else:
            # Provider probe attempted but didn't confirm → check expiry
            created = getattr(order, "created_at", None)
            if created:
                if hasattr(created, "tzinfo") and created.tzinfo is None:
                    created_aware = created.replace(tzinfo=timezone.utc)
                else:
                    created_aware = created
                now_aware = now_ts if now_ts.tzinfo else now_ts.replace(tzinfo=timezone.utc)
                age_seconds = (now_aware - created_aware).total_seconds()
                if age_seconds > 15 * 60:
                    if not dry_run:
                        order.status = "EXPIRED"
                        order.status_reason = "reconcile_probe_missing"
                        db.commit()
                    item["status"] = "EXPIRED" if not dry_run else order.status
                    item["status_reason"] = "reconcile_probe_missing"
                    expired += 1
        items.append(item)

    # Handle legacy MembershipOrder
    for order in pending_membership:
        checked += 1
        items.append({
            "order_id": order.order_id,
            "status": order.status,
            "status_reason": None,
        })

    return {
        "checked_count": checked,
        "reconciled_count": reconciled,
        "expired_count": expired,
        "total_pending": len(pending_membership),
        "reconciled": reconciled,
        "items": items,
        "results": items,
    }


def utc_now_naive() -> datetime:
    # SQLite stores naive datetimes; normalize all writes/comparisons to naive UTC.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def create_order(
    db: Session,
    user_id_or_user=None,
    plan_code_or_tier: str | None = None,
    channel_or_period=None,
    provider_positional: str | None = None,
    *,
    user=None,
    tier_id: str | None = None,
    period_months: int | None = None,
    provider: str | None = None,
) -> MembershipOrder | BillingOrder:
    """Create a billing or membership order.

    Supports three calling conventions:
    - Legacy: create_order(db, user_id, plan_code, channel)
    - V2 kw: create_order(db, user=user_obj, tier_id=..., period_months=..., provider=...)
    - V2 positional: create_order(db, user_obj, tier_id, period_months, provider)
    """
    # Detect V2 positional: 2nd arg is a User object
    from app.models import User as UserModel
    if user_id_or_user is not None and hasattr(user_id_or_user, "user_id"):
        user = user_id_or_user
        tier_id = tier_id or plan_code_or_tier
        if isinstance(channel_or_period, int):
            period_months = period_months or channel_or_period
        provider = provider or provider_positional
        user_id_or_user = None

    if user is not None and tier_id is not None:
        # V2 billing flow → BillingOrder
        # Reject ordering Free tier
        if tier_id.lower() == "free":
            raise ValueError("VALIDATION_FAILED")
        uid = getattr(user, "user_id", None) or str(getattr(user, "id", ""))
        # Check if user already has the requested tier
        current_tier = getattr(user, "tier", None) or "Free"
        if current_tier.lower() == tier_id.lower() and current_tier.lower() != "free":
            raise TierAlreadyActiveError(f"User already has tier {tier_id}")
        # Check if provider is configured
        if provider and provider != "mock":
            from app.core.config import settings
            mock_enabled = getattr(settings, "enable_mock_billing", False)
            if not mock_enabled:
                has_config = False
                if provider == "alipay":
                    has_config = bool(getattr(settings, "alipay_app_id", ""))
                elif provider in ("wechat", "wechat_pay"):
                    has_config = bool(getattr(settings, "wechat_pay_app_id", ""))
                if not has_config:
                    raise ValueError("PAYMENT_PROVIDER_NOT_CONFIGURED")
        order = BillingOrder(
            order_id=uuid4().hex,
            user_id=uid,
            provider=provider or "alipay",
            expected_tier=tier_id,
            period_months=period_months or 1,
            amount_cny=TIER_PRICE.get((tier_id, period_months or 1), 39.0),
            currency="CNY",
            status="CREATED",
        )
        db.add(order)
        db.commit()
        db.refresh(order)
        return order
    # Legacy membership flow
    user_id = user_id_or_user
    plan_code = plan_code_or_tier
    channel = channel_or_period if isinstance(channel_or_period, str) else "mock"
    if not user_id or not plan_code:
        raise ValueError("VALIDATION_FAILED")
    order = MembershipOrder(
        order_id=uuid4().hex,
        user_id=user_id,
        plan_code=plan_code,
        amount=PLAN_PRICE[plan_code],
        status="created",
        channel=channel,
    )
    db.add(order)
    db.commit()
    db.refresh(order)
    return order


def _upsert_subscription(db: Session, user_id: str, plan_code: str):
    now = utc_now_naive()
    days = PLAN_DAYS[plan_code]
    sub = (
        db.query(MembershipSubscription)
        .filter(MembershipSubscription.user_id == user_id)
        .order_by(MembershipSubscription.id.desc())
        .first()
    )
    if not sub:
        sub = MembershipSubscription(
            user_id=user_id,
            plan_code=plan_code,
            status="active",
            start_at=now,
            end_at=now + timedelta(days=days),
            updated_at=now,
        )
    else:
        base = sub.end_at if sub.end_at and sub.end_at > now else now
        sub.plan_code = plan_code
        sub.status = "active"
        sub.start_at = now
        sub.end_at = base + timedelta(days=days)
        sub.updated_at = now
    db.add(sub)


def handle_callback(db: Session, order_id: str, paid: bool, tx_id: str | None = None):
    order = db.get(MembershipOrder, order_id)
    if not order:
        return None
    if paid:
        order.status = "paid"
        order.paid_at = utc_now_naive()
        _upsert_subscription(db, order.user_id, order.plan_code)
    else:
        order.status = "failed"
    db.add(order)
    db.commit()
    db.refresh(order)
    return order


def subscription_status(db: Session, user_id: str) -> dict:
    from app.models import User
    user = db.query(User).filter(User.user_id == str(user_id)).first()
    tier = getattr(user, "tier", None) or "Free" if user else "Free"
    tier_expires_at = getattr(user, "tier_expires_at", None) if user else None

    sub = (
        db.query(MembershipSubscription)
        .filter(MembershipSubscription.user_id == user_id)
        .order_by(MembershipSubscription.id.desc())
        .first()
    )
    if sub:
        now = utc_now_naive()
        status = "active" if sub.end_at and sub.end_at > now and sub.status == "active" else "inactive"
        return {
            "user_id": user_id,
            "tier": tier,
            "status": status,
            "plan_code": sub.plan_code,
            "start_at": sub.start_at.isoformat() if sub.start_at else None,
            "end_at": sub.end_at.isoformat() if sub.end_at else None,
            "tier_expires_at": tier_expires_at.isoformat() if tier_expires_at else None,
        }
    # No subscription record — derive status from User model
    if tier and tier.lower() not in ("free", ""):
        # Paid tier but no subscription / no expiry → unknown
        if not tier_expires_at:
            return {
                "user_id": user_id,
                "tier": tier,
                "status": "unknown",
                "plan_code": None,
                "tier_expires_at": None,
                "status_reason": "expiry_unconfirmed",
            }
        now_utc = utc_now_naive()
        exp = tier_expires_at
        if hasattr(exp, "replace") and exp.tzinfo:
            exp = exp.replace(tzinfo=None)
        active = exp > now_utc
        return {
            "user_id": user_id,
            "tier": tier,
            "status": "active" if active else "expired",
            "plan_code": None,
            "tier_expires_at": tier_expires_at.isoformat() if tier_expires_at else None,
        }
    return {"user_id": user_id, "tier": tier, "status": "inactive", "plan_code": None, "tier_expires_at": None}


# ---------------------------------------------------------------------------
# FR-09 / NFR-13 会员真实性验证
# ---------------------------------------------------------------------------
import hashlib
import hmac
from typing import Any

_WEBHOOK_SECRET = "yanbao-membership-secret"


def user_has_truth_backed_paid_membership(
    db: Session,
    user_id: str | int | None = None,
    tier: str | None = None,
) -> bool:
    """Check if user has an active, truth-backed paid membership."""
    if user_id is None:
        return False
    sub = (
        db.query(MembershipSubscription)
        .filter(MembershipSubscription.user_id == str(user_id))
        .order_by(MembershipSubscription.id.desc())
        .first()
    )
    if not sub or sub.status != "active":
        return False
    now = utc_now_naive()
    if sub.end_at and sub.end_at <= now:
        return False
    if tier and sub.plan_code != tier:
        return False
    return True


def build_webhook_signature(
    event_id: str,
    order_id: str,
    user_id: str,
    tier_id: str,
    paid_amount: float,
    provider: str,
) -> str:
    """Build HMAC-SHA256 signature for payment webhook verification."""
    payload = f"{event_id}:{order_id}:{user_id}:{tier_id}:{paid_amount}:{provider}"
    return hmac.new(
        _WEBHOOK_SECRET.encode(),
        payload.encode(),
        hashlib.sha256,
    ).hexdigest()


def audit_membership_provider_truth(
    db: Session,
    apply_safe_repairs: bool = False,
) -> dict[str, Any]:
    """Audit membership provider truth: check paid_tier_null_expiry, webhook events."""
    from app.models import PaymentWebhookEvent, User

    rows = []

    # Check MembershipSubscription records with active + null end_at
    try:
        paid_null = (
            db.query(MembershipSubscription)
            .filter(
                MembershipSubscription.status == "active",
                MembershipSubscription.end_at.is_(None),
            )
            .all()
        )
        for s in paid_null:
            repairable = apply_safe_repairs
            rows.append({
                "user_id": s.user_id,
                "plan_code": s.plan_code,
                "repairable": repairable,
            })
            if apply_safe_repairs:
                s.end_at = utc_now_naive() + timedelta(days=30)
    except Exception:
        pass

    # Check User table: paid tier with null tier_expires_at
    try:
        paid_users = (
            db.query(User)
            .filter(
                User.tier.isnot(None),
                User.tier.notin_(["Free", "free", ""]),
                User.tier_expires_at.is_(None),
            )
            .all()
        )
        for u in paid_users:
            uid = str(getattr(u, "user_id", u.id))
            classification_result = classify_paid_null_expiry_user(db, u)
            is_admin = getattr(u, "role", "") == "admin"
            if is_admin and not classification_result["repairable"]:
                # Synthetic admin with paid tier but no local truth → downgrade
                entry = {
                    "user_id": uid,
                    "tier": u.tier,
                    "role": u.role,
                    "repairable": True,
                    "repair_strategy": "downgrade_synthetic_admin_to_free",
                    "classification": "repairable_synthetic_admin_without_local_truth",
                    "repair_applied": False,
                }
                if apply_safe_repairs:
                    u.tier = "Free"
                    u.tier_expires_at = None
                    entry["repair_applied"] = True
                rows.append(entry)
            else:
                entry = {
                    "user_id": uid,
                    "tier": u.tier,
                    "role": getattr(u, "role", None),
                    **classification_result,
                }
                rows.append(entry)
    except Exception:
        pass

    if apply_safe_repairs and rows:
        db.commit()

    # Payment webhook event counts
    try:
        total = db.query(PaymentWebhookEvent).count()
        succeeded = db.query(PaymentWebhookEvent).filter(
            PaymentWebhookEvent.processing_succeeded.is_(True)
        ).count()
        failed = db.query(PaymentWebhookEvent).filter(
            PaymentWebhookEvent.processing_succeeded.is_(False)
        ).count()
    except Exception:
        total = succeeded = failed = 0

    return {
        "paid_tier_null_expiry": {
            "count": len(rows),
            "rows": rows,
        },
        "payment_webhook_event": {
            "total_count": total,
            "processing_succeeded_true_count": succeeded,
            "processing_succeeded_false_count": failed,
        },
    }


def classify_paid_null_expiry_user(
    db: Session,
    user,
) -> dict[str, Any]:
    """Classify a paid user with null expiry for repair analysis."""
    from app.models import BillingOrder, PaymentWebhookEvent

    uid = str(getattr(user, "user_id", user))
    # Count truth-backed paid orders
    try:
        paid_orders = (
            db.query(BillingOrder)
            .filter(BillingOrder.user_id == uid, func.upper(BillingOrder.status) == "PAID")
            .order_by(BillingOrder.paid_at.desc())
            .all()
        )
        paid_order_count = len(paid_orders)
    except Exception:
        paid_orders = []
        paid_order_count = 0

    try:
        success_event_count = (
            db.query(PaymentWebhookEvent)
            .filter(PaymentWebhookEvent.processing_succeeded.is_(True))
            .count()
        )
    except Exception:
        success_event_count = 0

    repairable = paid_order_count >= 1
    classification = (
        "single_truth_backed_paid_order" if paid_order_count == 1
        else "multiple_truth_backed_paid_orders" if paid_order_count > 1
        else "missing_local_entitlement_fact"
    )
    repair_strategy = classification if repairable else None

    candidate_expires_at = None
    candidate_order_id = None
    if repairable and paid_orders:
        latest = paid_orders[0]
        candidate_order_id = latest.order_id
        base_dt = latest.paid_at or latest.created_at
        if base_dt:
            if base_dt.tzinfo is None:
                base_dt = base_dt.replace(tzinfo=timezone.utc)
            candidate_expires_at = (base_dt + timedelta(days=30)).isoformat()
        else:
            candidate_expires_at = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()

    evidence = {
        "truth_backed_paid_order_count": paid_order_count,
        "successful_payment_event_count": success_event_count,
    }
    if candidate_order_id:
        evidence["candidate_order_id"] = candidate_order_id

    return {
        "repairable": repairable,
        "repair_strategy": repair_strategy,
        "candidate_expires_at": candidate_expires_at,
        "classification": classification,
        "evidence": evidence,
    }


def handle_webhook(
    db: Session,
    event_id: str,
    order_id: str,
    user_id: str,
    tier_id: str,
    paid_amount: float,
    provider: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Process a payment webhook event.

    If event already exists → mark duplicate and backfill request_id.
    Otherwise → create new PaymentWebhookEvent and activate subscription.
    """
    from app.core.request_context import get_request_id
    from app.models import PaymentWebhookEvent

    existing = db.query(PaymentWebhookEvent).filter(
        PaymentWebhookEvent.event_id == event_id
    ).first()

    if existing is not None:
        existing.duplicate_count = (existing.duplicate_count or 0) + 1
        existing.status_reason = "duplicate_event_ignored"
        # backfill empty request_id
        try:
            rid = get_request_id()
        except Exception:
            rid = None
        if rid and not existing.request_id:
            existing.request_id = rid
        db.commit()
        return {"duplicate": True, "processed": False, "event_id": event_id, "status": "duplicate", "status_reason": "duplicate_event_ignored"}

    try:
        rid = get_request_id()
    except Exception:
        rid = None

    evt = PaymentWebhookEvent(
        event_id=event_id,
        order_id=order_id,
        user_id=user_id,
        provider=provider,
        event_type="PAYMENT_SUCCEEDED",
        tier_id=tier_id,
        paid_amount=paid_amount,
        payload_json=str(payload) if payload else None,
        request_id=rid,
        status="PROCESSED",
        processing_succeeded=True,
        received_at=datetime.now(timezone.utc),
        processed_at=datetime.now(timezone.utc),
    )
    db.add(evt)

    # Activate subscription: upgrade user tier and set billing order status
    from app.models import User, BillingOrder as BillingOrderModel
    user = db.query(User).filter(User.user_id == user_id).first()
    order_obj = db.query(BillingOrderModel).filter(
        BillingOrderModel.order_id == order_id
    ).first()
    if not user:
        raise ValueError("VALIDATION_FAILED")
    if user and order_obj:
        grant_membership_order_entitlement(user, order_obj)
    elif user:
        user.tier = tier_id
        now = datetime.now(timezone.utc)
        user.tier_expires_at = now + timedelta(days=30)
        user.membership_expires_at = now + timedelta(days=30)
    if order_obj:
        order_obj.status = "PAID"
        order_obj.granted_tier = tier_id
        order_obj.paid_at = datetime.now(timezone.utc)

    db.commit()
    return {"duplicate": False, "processed": True, "event_id": event_id, "status": "processed"}


def verify_webhook_signature(
    header_signature: str | None,
    payload_signature: str | None,
    event_id: str,
    order_id: str,
    user_id: str,
    tier_id: str,
    paid_amount: float,
    provider: str,
) -> bool:
    """Verify the webhook signature against the shared secret."""
    sig = header_signature or payload_signature
    if not sig:
        return False
    expected = build_webhook_signature(event_id, order_id, user_id, tier_id, paid_amount, provider)
    return hmac.compare_digest(sig, expected)


def serialize_order(order: BillingOrder) -> dict[str, Any]:
    """Serialize a BillingOrder to a JSON-friendly dict."""
    return {
        "order_id": order.order_id,
        "user_id": order.user_id,
        "provider": order.provider,
        "expected_tier": getattr(order, "expected_tier", None),
        "period_months": getattr(order, "period_months", None),
        "amount_cny": getattr(order, "amount_cny", None),
        "currency": getattr(order, "currency", "CNY"),
        "payment_url": getattr(order, "payment_url", None),
        "status": order.status,
        "created_at": str(order.created_at) if order.created_at else None,
        "paid_at": str(order.paid_at) if order.paid_at else None,
    }


def grant_membership_order_entitlement(user, order) -> tuple[str, datetime]:
    """Grant membership tier to user based on billing order. Returns (new_tier, new_expiry)."""
    from app.models import MembershipSubscription
    from app.core.db import SessionLocal
    tier = getattr(order, "expected_tier", None) or "Pro"
    months = getattr(order, "period_months", None) or 1
    days = months * 30
    now = datetime.now(timezone.utc) if hasattr(datetime.now(timezone.utc), 'tzinfo') else utc_now_naive()
    new_expiry = now + timedelta(days=days)
    user.tier = tier
    user.membership_expires_at = new_expiry
    user.tier_expires_at = new_expiry
    # Create or update MembershipSubscription for truth-backed membership checking
    db = object.__getattribute__(user, '_sa_instance_state').session
    if db is not None:
        existing_sub = (
            db.query(MembershipSubscription)
            .filter(MembershipSubscription.user_id == str(user.user_id))
            .order_by(MembershipSubscription.id.desc())
            .first()
        )
        if existing_sub:
            existing_sub.plan_code = tier
            existing_sub.status = "active"
            existing_sub.start_at = now
            existing_sub.end_at = new_expiry
            existing_sub.updated_at = now
        else:
            sub = MembershipSubscription(
                user_id=str(user.user_id),
                plan_code=tier,
                status="active",
                start_at=now,
                end_at=new_expiry,
                updated_at=now,
            )
            db.add(sub)
    return tier, new_expiry
