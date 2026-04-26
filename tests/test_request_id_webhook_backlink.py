from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.core.config import settings
from app.core.request_context import reset_request_id, set_request_id
from app.core.security import hash_password
from app.models import Base, PaymentWebhookEvent, User
from app.services.membership import build_webhook_signature, create_order, handle_webhook

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _reset_request_id_context():
    reset_request_id()
    yield
    reset_request_id()


PAYMENT_WEBHOOK_WRITER_FILES = [
    ROOT / "app" / "api" / "routes_admin.py",
    ROOT / "app" / "services" / "membership.py",
]


def _login_and_create_order(client, create_user, monkeypatch) -> tuple[object, str]:
    monkeypatch.setattr(settings, "enable_mock_billing", True)
    monkeypatch.setattr(settings, "billing_webhook_secret", "task-d-webhook-secret")

    account = create_user(
        email=f"request-id-webhook-{uuid4().hex[:8]}@test.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": account["user"].email, "password": account["password"]},
    )
    assert login.status_code == 200
    token = login.json()["data"]["access_token"]

    create_order = client.post(
        "/billing/create_order",
        headers={"Authorization": f"Bearer {token}"},
        json={"tier_id": "Pro", "period_months": 1, "provider": "alipay"},
    )
    assert create_order.status_code == 201
    return account["user"], create_order.json()["data"]["order_id"]


@pytest.mark.parametrize(
    ("header_request_id", "expected_request_id"),
    [
        ("req-webhook-roundtrip", "req-webhook-roundtrip"),
        ("   ", None),
        (None, None),
    ],
    ids=["explicit-header", "blank-header-generated", "generated-in-context"],
)
def test_request_id_roundtrips_into_payment_webhook_event(
    client,
    create_user,
    db_session,
    monkeypatch,
    header_request_id: str | None,
    expected_request_id: str | None,
):
    user, order_id = _login_and_create_order(client, create_user, monkeypatch)
    event_id = f"evt-request-id-{uuid4().hex}"
    paid_amount = 29.9
    signature = build_webhook_signature(
        event_id,
        order_id,
        str(user.user_id),
        "Pro",
        paid_amount,
        "alipay",
    )

    headers = {"Webhook-Signature": signature}
    if header_request_id is not None:
        headers["X-Request-ID"] = header_request_id

    response = client.post(
        "/billing/webhook",
        headers=headers,
        json={
            "event_id": event_id,
            "order_id": order_id,
            "user_id": str(user.user_id),
            "tier_id": "Pro",
            "paid_amount": paid_amount,
            "provider": "alipay",
            "signature": signature,
        },
    )

    assert response.status_code == 200
    body = response.json()
    request_id = body["request_id"]
    assert request_id == response.headers["X-Request-ID"]
    if expected_request_id is not None:
        assert request_id == expected_request_id
    else:
        UUID(request_id)

    row = db_session.execute(
        select(PaymentWebhookEvent).where(PaymentWebhookEvent.event_id == event_id)
    ).scalars().one()
    assert row.request_id == request_id
    assert row.request_id not in {None, "", "missing-request-id"}
    assert row.order_id == order_id
    assert row.processing_succeeded is True


def test_request_id_duplicate_webhook_backfills_empty_event_request_id(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(settings, "enable_mock_billing", True)
    db_file = tmp_path / "duplicate-webhook.db"
    engine = create_engine(f"sqlite:///{db_file.as_posix()}")
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    db_session = Session()
    try:
        user = User(
            email=f"request-id-webhook-dup-{uuid4().hex[:8]}@test.com",
            password_hash=hash_password("Password123"),
            tier="Free",
            role="user",
            email_verified=True,
        )
        db_session.add(user)
        db_session.commit()
        db_session.refresh(user)

        order = create_order(
            db_session,
            user=user,
            tier_id="Pro",
            period_months=1,
            provider="alipay",
        )
        db_session.commit()
        order_id = order.order_id
        event_id = f"evt-duplicate-backfill-{uuid4().hex}"
        paid_amount = 29.9

        db_session.add(
            PaymentWebhookEvent(
                event_id=event_id,
                order_id=order_id,
                provider="alipay",
                event_type="PAYMENT_SUCCEEDED",
                payload_json={"source": "legacy-seed"},
                request_id=None,
                processing_succeeded=True,
                duplicate_count=0,
                received_at=datetime.now(timezone.utc),
                processed_at=datetime.now(timezone.utc),
            )
        )
        db_session.commit()

        token = set_request_id("req-webhook-backfill")
        try:
            result = handle_webhook(
                db_session,
                event_id=event_id,
                order_id=order_id,
                user_id=str(user.user_id),
                tier_id="Pro",
                paid_amount=paid_amount,
                provider="alipay",
                payload={"source": "duplicate-replay"},
            )
        finally:
            reset_request_id(token)

        assert result["duplicate"] is True
        row = db_session.execute(
            select(PaymentWebhookEvent).where(PaymentWebhookEvent.event_id == event_id)
        ).scalars().one()
        assert row.request_id == "req-webhook-backfill"
        assert row.duplicate_count == 1
        assert row.status_reason == "duplicate_event_ignored"
    finally:
        db_session.close()


def test_payment_webhook_event_writers_explicitly_pass_request_id():
    violations: list[str] = []
    for path in PAYMENT_WEBHOOK_WRITER_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Name) or node.func.id != "PaymentWebhookEvent":
                continue
            keyword_names = {keyword.arg for keyword in node.keywords if keyword.arg}
            if "request_id" not in keyword_names:
                violations.append(str(path.relative_to(ROOT)))

    assert not violations, (
        "PaymentWebhookEvent constructors in Task D scope must pass request_id explicitly:\n"
        + "\n".join(sorted(set(violations)))
    )
