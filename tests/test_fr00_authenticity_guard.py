from __future__ import annotations

from datetime import datetime

import pytest

from app.models import Base, Report


def _admin_headers(client, create_user):
    admin = create_user(
        email="admin-fr00@example.com",
        password="Password123",
        role="admin",
        email_verified=True,
    )
    login = client.post(
        "/auth/login",
        json={"email": admin["user"].email, "password": admin["password"]},
    )
    assert login.status_code == 200
    token = login.json()["data"]["access_token"]
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.feature("FR00-AUTH-02")
def test_fr00_citations_complete(client, seed_report_bundle):
    report = seed_report_bundle()

    response = client.get(f"/api/v1/reports/{report.report_id}")

    assert response.status_code == 200
    citations = response.json()["data"]["citations"]
    assert citations
    for citation in citations:
        assert citation["source_name"]
        assert citation["source_url"].startswith(("http://", "https://"))
        datetime.fromisoformat(citation["fetch_time"])


@pytest.mark.feature("FR00-AUTH-01")
def test_fr00_published_report_readonly(client, db_session, seed_report_bundle):
    report = seed_report_bundle(recommendation="BUY")
    before = db_session.get(Report, report.report_id)
    assert before is not None

    response = client.put(
        f"/api/v1/reports/{report.report_id}",
        json={"recommendation": "SELL"},
    )

    assert response.status_code in {403, 405}
    db_session.expire_all()
    after = db_session.get(Report, report.report_id)
    assert after is not None
    assert after.recommendation == "BUY"


@pytest.mark.feature("FR00-AUTH-03")
def test_fr00_publish_operation_audit_record(client, db_session, create_user, seed_report_bundle):
    report = seed_report_bundle(
        published=False,
        publish_status="DRAFT_GENERATED",
        review_flag="NONE",
    )
    headers = _admin_headers(client, create_user) | {"X-Request-ID": "req-fr00-publish"}

    response = client.patch(
        f"/api/v1/admin/reports/{report.report_id}",
        headers=headers,
        json={"published": True},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["request_id"] == "req-fr00-publish"
    assert body["data"]["published"] is True
    assert body["data"]["publish_status"] == "PUBLISHED"

    db_session.expire_all()
    updated = db_session.get(Report, report.report_id)
    assert updated is not None
    assert updated.published is True
    assert updated.publish_status == "PUBLISHED"

    admin_operation = Base.metadata.tables["admin_operation"]
    audit_log = Base.metadata.tables["audit_log"]

    operation_row = db_session.execute(
        admin_operation.select().where(
            admin_operation.c.target_table == "report",
            admin_operation.c.target_pk == report.report_id,
            admin_operation.c.request_id == "req-fr00-publish",
        )
    ).fetchone()
    assert operation_row is not None
    assert operation_row.action_type == "PATCH_REPORT"
    assert operation_row.status == "COMPLETED"
    assert operation_row.before_snapshot["published"] is False
    assert operation_row.after_snapshot["published"] is True

    audit_row = db_session.execute(
        audit_log.select().where(
            audit_log.c.target_table == "report",
            audit_log.c.target_pk == report.report_id,
            audit_log.c.request_id == "req-fr00-publish",
        )
    ).fetchone()
    assert audit_row is not None
    assert audit_row.action_type == "PUBLISH"
    assert audit_row.before_snapshot["published"] is False
    assert audit_row.after_snapshot["published"] is True
