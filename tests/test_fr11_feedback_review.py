from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.models import Base, Report
from app.services.feedback_ssot import (
    FeedbackServiceError,
    _dispatch_review_notification,
    submit_report_feedback,
)

pytestmark = [
    pytest.mark.feature("FR11-FEEDBACK-01"),
    pytest.mark.feature("FR11-FEEDBACK-02"),
]


def _auth_headers(client, create_user, email: str) -> tuple[str, dict[str, str]]:
    account = create_user(
        email=email,
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
    return account["user"].user_id, {"Authorization": f"Bearer {token}"}


def _feedback_count(db_session, *, user_id: str | None = None) -> int:
    feedback_table = Base.metadata.tables["report_feedback"]
    stmt = select(func.count()).select_from(feedback_table)
    if user_id is not None:
        stmt = stmt.where(feedback_table.c.user_id == user_id)
    return int(db_session.execute(stmt).scalar_one())


def test_fr11_feedback_requires_auth(client, db_session, seed_report_bundle):
    report = seed_report_bundle(review_flag="NONE")
    before_count = _feedback_count(db_session)

    response = client.post(
        f"/api/v1/reports/{report.report_id}/feedback",
        headers={"X-Request-ID": "req-fr11-auth"},
        json={"report_id": report.report_id, "feedback_type": "negative"},
    )

    assert response.status_code == 401
    body = response.json()
    assert body["success"] is False
    assert body["request_id"] == "req-fr11-auth"
    assert body["error_code"] == "UNAUTHORIZED"
    assert body["data"] is None
    assert _feedback_count(db_session) == before_count


def test_fr11_feedback_accepts_path_only_payload(client, db_session, create_user, seed_report_bundle):
    report = seed_report_bundle(review_flag="NONE")
    _, headers = _auth_headers(client, create_user, "fr11-path-only@example.com")

    response = client.post(
        f"/api/v1/reports/{report.report_id}/feedback",
        headers=headers,
        json={"feedback_type": "negative"},
    )
    assert response.status_code == 200

    body = response.json()["data"]
    assert body["feedback_type"] == "negative"
    assert body["negative_count"] == 1
    assert body["is_duplicate_negative"] is False
    assert body["review_flag"] == "NONE"
    assert body["review_event_enqueued"] is False


def test_fr11_feedback_rejects_conflicting_body_report_id(client, create_user, seed_report_bundle):
    report = seed_report_bundle(review_flag="NONE")
    _, headers = _auth_headers(client, create_user, "fr11-conflict@example.com")

    response = client.post(
        f"/api/v1/reports/{report.report_id}/feedback",
        headers=headers,
        json={"report_id": str(uuid4()), "feedback_type": "negative"},
    )

    assert response.status_code == 422
    body = response.json()
    assert body["success"] is False
    assert body["error_code"] == "INVALID_PAYLOAD"


def test_fr11_negative_dedup(client, db_session, create_user, seed_report_bundle):
    report = seed_report_bundle(review_flag="NONE")
    report_table = Base.metadata.tables["report"]
    feedback_table = Base.metadata.tables["report_feedback"]
    business_event_table = Base.metadata.tables["business_event"]
    outbox_table = Base.metadata.tables["outbox_event"]
    notification_table = Base.metadata.tables["notification"]

    _, user1_headers = _auth_headers(client, create_user, "fr11-user1@example.com")
    _, user2_headers = _auth_headers(client, create_user, "fr11-user2@example.com")
    _, user3_headers = _auth_headers(client, create_user, "fr11-user3@example.com")
    _, user4_headers = _auth_headers(client, create_user, "fr11-user4@example.com")

    first = client.post(
        f"/api/v1/reports/{report.report_id}/feedback",
        headers=user1_headers,
        json={"report_id": report.report_id, "feedback_type": "negative"},
    )
    assert first.status_code == 200
    first_body = first.json()["data"]
    assert first_body["feedback_type"] == "negative"
    assert first_body["negative_count"] == 1
    assert first_body["review_flag"] == "NONE"
    assert first_body["is_duplicate_negative"] is False
    assert first_body["review_event_enqueued"] is False

    duplicate = client.post(
        f"/api/v1/reports/{report.report_id}/feedback",
        headers=user1_headers,
        json={"report_id": report.report_id, "feedback_type": "negative"},
    )
    assert duplicate.status_code == 200
    duplicate_body = duplicate.json()["data"]
    assert duplicate_body["feedback_id"] == first_body["feedback_id"]
    assert duplicate_body["negative_count"] == 1
    assert duplicate_body["review_flag"] == "NONE"
    assert duplicate_body["is_duplicate_negative"] is True
    assert duplicate_body["review_event_enqueued"] is False

    positive = client.post(
        f"/api/v1/reports/{report.report_id}/feedback",
        headers=user1_headers,
        json={"report_id": report.report_id, "feedback_type": "positive"},
    )
    assert positive.status_code == 200
    positive_body = positive.json()["data"]
    assert positive_body["feedback_type"] == "positive"
    assert positive_body["negative_count"] == 1
    assert positive_body["review_flag"] == "NONE"
    assert positive_body["is_duplicate_negative"] is False
    assert positive_body["review_event_enqueued"] is False

    second_negative = client.post(
        f"/api/v1/reports/{report.report_id}/feedback",
        headers=user2_headers,
        json={"report_id": report.report_id, "feedback_type": "negative"},
    )
    assert second_negative.status_code == 200
    assert second_negative.json()["data"]["negative_count"] == 2
    assert second_negative.json()["data"]["review_flag"] == "NONE"

    third_negative = client.post(
        f"/api/v1/reports/{report.report_id}/feedback",
        headers=user3_headers,
        json={"report_id": report.report_id, "feedback_type": "negative"},
    )
    assert third_negative.status_code == 200
    third_body = third_negative.json()["data"]
    assert third_body["negative_count"] == 3
    assert third_body["review_flag"] == "PENDING_REVIEW"
    assert third_body["is_duplicate_negative"] is False
    assert third_body["review_event_enqueued"] is True

    fourth_negative = client.post(
        f"/api/v1/reports/{report.report_id}/feedback",
        headers=user4_headers,
        json={"report_id": report.report_id, "feedback_type": "negative"},
    )
    assert fourth_negative.status_code == 200
    assert fourth_negative.json()["data"]["negative_count"] == 4
    assert fourth_negative.json()["data"]["review_flag"] == "PENDING_REVIEW"
    assert fourth_negative.json()["data"]["review_event_enqueued"] is False

    report_row = db_session.execute(
        select(
            report_table.c.review_flag,
            report_table.c.negative_feedback_count,
        ).where(report_table.c.report_id == report.report_id)
    ).mappings().one()
    assert report_row["review_flag"] == "PENDING_REVIEW"
    assert report_row["negative_feedback_count"] == 4
    assert int(
        db_session.execute(
            select(func.count())
            .select_from(feedback_table)
            .where(feedback_table.c.report_id == report.report_id)
        ).scalar_one()
    ) == 5
    assert int(db_session.execute(select(func.count()).select_from(business_event_table)).scalar_one()) == 1
    assert int(db_session.execute(select(func.count()).select_from(outbox_table)).scalar_one()) == 1

    notification_row = db_session.execute(select(notification_table)).mappings().one()
    assert notification_row["event_type"] == "REPORT_PENDING_REVIEW"
    assert notification_row["recipient_scope"] == "admin"
    assert notification_row["recipient_key"] == "admin_global"
    assert notification_row["channel"] == "webhook"
    assert notification_row["status"] == "skipped"
    assert notification_row["status_reason"] == "admin_channel_not_configured"


def test_fr11_existing_skipped_notification_does_not_advance_last_sent_at(
    client,
    db_session,
    create_user,
    seed_report_bundle,
):
    report = seed_report_bundle(review_flag="NONE")
    business_event_table = Base.metadata.tables["business_event"]
    outbox_table = Base.metadata.tables["outbox_event"]
    cursor_table = Base.metadata.tables["event_projection_cursor"]
    notification_table = Base.metadata.tables["notification"]

    _, user1_headers = _auth_headers(client, create_user, "fr11-replay-1@example.com")
    _, user2_headers = _auth_headers(client, create_user, "fr11-replay-2@example.com")
    _, user3_headers = _auth_headers(client, create_user, "fr11-replay-3@example.com")

    for headers in (user1_headers, user2_headers, user3_headers):
        response = client.post(
            f"/api/v1/reports/{report.report_id}/feedback",
            headers=headers,
            json={"report_id": report.report_id, "feedback_type": "negative"},
        )
        assert response.status_code == 200

    event_row = db_session.execute(select(business_event_table)).mappings().one()
    outbox_row = db_session.execute(select(outbox_table)).mappings().one()
    cursor_row = db_session.execute(select(cursor_table)).mappings().one()
    notification_row = db_session.execute(select(notification_table)).mappings().one()

    assert notification_row["status"] == "skipped"
    assert notification_row["status_reason"] == "admin_channel_not_configured"
    assert cursor_row["last_sent_at"] is None

    db_session.execute(
        outbox_table.update()
        .where(outbox_table.c.outbox_event_id == outbox_row["outbox_event_id"])
        .values(
            dispatch_status="PENDING",
            claim_token=None,
            claimed_at=None,
            claimed_by=None,
            dispatched_at=None,
            updated_at=notification_row["created_at"],
        )
    )
    db_session.commit()

    _dispatch_review_notification(
        db_session,
        event_row["business_event_id"],
        current_time=notification_row["created_at"],
    )

    replayed_outbox = db_session.execute(select(outbox_table)).mappings().one()
    replayed_cursor = db_session.execute(select(cursor_table)).mappings().one()
    notifications = db_session.execute(select(notification_table)).mappings().all()

    assert replayed_outbox["dispatch_status"] == "DISPATCHED"
    assert replayed_outbox["status_reason"] == "admin_channel_not_configured"
    assert replayed_cursor["last_sent_at"] is None
    assert len(notifications) == 1


def test_fr11_feedback_rate_limit(client, db_session, create_user, seed_report_bundle, isolated_app):
    report = seed_report_bundle(review_flag="NONE")
    _, headers = _auth_headers(client, create_user, "fr11-limit@example.com")

    invalid_payload = client.post(
        f"/api/v1/reports/{report.report_id}/feedback",
        headers=headers,
        json={"report_id": report.report_id, "feedback_type": "negative", "reason": "too risky"},
    )
    assert invalid_payload.status_code == 422
    assert invalid_payload.json()["error_code"] == "INVALID_PAYLOAD"

    missing_report = client.post(
        f"/api/v1/reports/{uuid4()}/feedback",
        headers=headers,
        json={"report_id": str(uuid4()), "feedback_type": "negative"},
    )
    assert missing_report.status_code == 422
    assert missing_report.json()["error_code"] == "INVALID_PAYLOAD"

    unavailable_report_id = str(uuid4())
    unavailable = client.post(
        f"/api/v1/reports/{unavailable_report_id}/feedback",
        headers=headers,
        json={"report_id": unavailable_report_id, "feedback_type": "negative"},
    )
    assert unavailable.status_code == 404
    assert unavailable.json()["error_code"] == "REPORT_NOT_AVAILABLE"

    unpublished = seed_report_bundle(
        stock_code="688001.SH",
        published=False,
        publish_status="DRAFT_GENERATED",
        review_flag="NONE",
    )
    unpublished_response = client.post(
        f"/api/v1/reports/{unpublished.report_id}/feedback",
        headers=headers,
        json={"report_id": unpublished.report_id, "feedback_type": "negative"},
    )
    assert unpublished_response.status_code == 404
    assert unpublished_response.json()["error_code"] == "REPORT_NOT_AVAILABLE"

    first_twenty: list[Report] = []
    for idx in range(20):
        first_twenty.append(
            seed_report_bundle(
                stock_code=f"{600100 + idx:06d}.SH",
                review_flag="NONE",
            )
        )

    for seeded in first_twenty:
        response = client.post(
            f"/api/v1/reports/{seeded.report_id}/feedback",
            headers=headers,
            json={"report_id": seeded.report_id, "feedback_type": "negative"},
        )
        assert response.status_code == 200

    rate_limited_report = seed_report_bundle(stock_code="600199.SH", review_flag="NONE")
    rate_limited = client.post(
        f"/api/v1/reports/{rate_limited_report.report_id}/feedback",
        headers=headers,
        json={"report_id": rate_limited_report.report_id, "feedback_type": "negative"},
    )
    assert rate_limited.status_code == 429
    assert rate_limited.json()["error_code"] == "RATE_LIMITED"

    limit_user_id = db_session.execute(
        select(Base.metadata.tables["app_user"].c.user_id).where(
            Base.metadata.tables["app_user"].c.email == "fr11-limit@example.com"
        )
    ).scalar_one()
    assert _feedback_count(db_session, user_id=limit_user_id) == 20

    concurrent_user_id = create_user(
        email="fr11-concurrent@example.com",
        password="Password123",
        tier="Free",
        role="user",
        email_verified=True,
    )["user"].user_id
    concurrent_report_ids = [
        seed_report_bundle(
            stock_code=f"{601100 + idx:06d}.SH",
            review_flag="NONE",
        ).report_id
        for idx in range(50)
    ]

    def worker(report_id: str) -> str:
        session = isolated_app["sessionmaker"]()
        try:
            try:
                    submit_report_feedback(
                        session,
                        path_report_id=report_id,
                        report_id=report_id,
                        user_id=concurrent_user_id,
                        feedback_type="negative",
                    )
                    return "ok"
            except FeedbackServiceError as exc:
                return exc.error_code
        finally:
            session.close()

    with ThreadPoolExecutor(max_workers=12) as executor:
        outcomes = list(executor.map(worker, concurrent_report_ids))

    assert outcomes.count("ok") == 20
    assert outcomes.count("RATE_LIMITED") == 30
    assert _feedback_count(db_session, user_id=concurrent_user_id) == 20


def test_fr11_feedback_dispatch_retry_updates_existing_notification(client, db_session, create_user, seed_report_bundle, monkeypatch):
    from app.core.config import settings
    from app.services import notification as notification_service

    monkeypatch.setattr(settings, "admin_alert_webhook_url", "https://example.com/webhook")
    monkeypatch.setattr(notification_service, "send_admin_notification", lambda kind, payload: False)

    report = seed_report_bundle(review_flag="NONE")
    business_event_table = Base.metadata.tables["business_event"]
    outbox_table = Base.metadata.tables["outbox_event"]
    notification_table = Base.metadata.tables["notification"]

    _, user1_headers = _auth_headers(client, create_user, "fr11-fail-1@example.com")
    _, user2_headers = _auth_headers(client, create_user, "fr11-fail-2@example.com")
    _, user3_headers = _auth_headers(client, create_user, "fr11-fail-3@example.com")

    for headers in (user1_headers, user2_headers, user3_headers):
        response = client.post(
            f"/api/v1/reports/{report.report_id}/feedback",
            headers=headers,
            json={"report_id": report.report_id, "feedback_type": "negative"},
        )
        assert response.status_code == 200

    event_row = db_session.execute(select(business_event_table)).mappings().one()
    outbox_row = db_session.execute(select(outbox_table)).mappings().one()
    failed_rows = db_session.execute(
        select(notification_table).order_by(notification_table.c.created_at.asc(), notification_table.c.notification_id.asc())
    ).mappings().all()
    assert len(failed_rows) == 1
    assert failed_rows[0]["status"] == "failed"
    assert failed_rows[0]["status_reason"] == "admin_channel_send_failed"

    monkeypatch.setattr(notification_service, "send_admin_notification", lambda kind, payload: True)
    db_session.execute(
        outbox_table.update()
        .where(outbox_table.c.outbox_event_id == outbox_row["outbox_event_id"])
        .values(
            dispatch_status="PENDING",
            claim_token=None,
            claimed_at=None,
            claimed_by=None,
            dispatched_at=None,
        )
    )
    db_session.commit()

    _dispatch_review_notification(
        db_session,
        event_row["business_event_id"],
        current_time=failed_rows[0]["created_at"] + timedelta(minutes=1),
    )

    notifications = db_session.execute(
        select(notification_table).order_by(notification_table.c.created_at.asc(), notification_table.c.notification_id.asc())
    ).mappings().all()
    assert len(notifications) == 1
    assert notifications[0]["status"] == "sent"
    assert notifications[0]["status_reason"] is None
    replayed_outbox = db_session.execute(select(outbox_table)).mappings().one()
    assert replayed_outbox["dispatch_status"] == "DISPATCHED"
