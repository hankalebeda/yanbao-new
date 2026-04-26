from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select

from app.models import AdminOperation, AuditLog, Base
from tests.helpers_ssot import seed_generation_context

pytestmark = [pytest.mark.feature("FR06-LLM-01")]


@pytest.fixture(autouse=True)
def _reset_request_id_context():
    from app.core.request_context import reset_request_id
    reset_request_id()
    yield
    reset_request_id()

ROOT = Path(__file__).resolve().parents[1]
REQUEST_ID_SOURCE_FILES = [
    ROOT / "app" / "main.py",
    ROOT / "app" / "api" / "routes_auth.py",
    ROOT / "app" / "api" / "routes_admin.py",
    ROOT / "app" / "api" / "routes_billing.py",
    ROOT / "app" / "api" / "routes_business.py",
    ROOT / "app" / "api" / "routes_internal.py",
    ROOT / "app" / "api" / "routes_sim.py",
    ROOT / "app" / "core" / "request_context.py",
    ROOT / "app" / "core" / "response.py",
    ROOT / "app" / "services" / "admin_audit.py",
    ROOT / "app" / "services" / "membership.py",
    ROOT / "app" / "services" / "report_admin.py",
]


@pytest.mark.parametrize(
    ("header_request_id", "expected_request_id"),
    [
        ("req-backlink-explicit", "req-backlink-explicit"),
        ("   ", None),
        (None, None),
    ],
    ids=["explicit-header", "blank-header-generated", "generated-in-context"],
)
def test_request_id_roundtrips_into_report_generation_task(
    client,
    db_session,
    header_request_id: str | None,
    expected_request_id: str | None,
):
    trade_date = "2026-03-23"
    seed_generation_context(db_session, trade_date=trade_date)

    headers = {}
    if header_request_id is not None:
        headers["X-Request-ID"] = header_request_id

    response = client.post(
        "/api/v1/reports/generate",
        headers=headers,
        json={
            "stock_code": "600519.SH",
            "trade_date": trade_date,
            "source": "test",
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

    task_table = Base.metadata.tables["report_generation_task"]
    task_rows = db_session.execute(
        task_table.select().where(task_table.c.request_id == request_id)
    ).mappings().all()
    assert task_rows, "report_generation_task missing request_id backlink"
    latest = sorted(task_rows, key=lambda row: str(row["created_at"] or row["updated_at"] or ""), reverse=True)[0]
    assert latest["request_id"] == request_id
    assert latest["stock_code"] == "600519.SH"
    assert str(latest["trade_date"]) == trade_date

    bad_rows = db_session.execute(
        task_table.select().where(
            task_table.c.stock_code == "600519.SH",
            task_table.c.trade_date == trade_date,
            (task_table.c.request_id == "missing-request-id") | (task_table.c.request_id.is_(None)),
        )
    ).mappings().all()
    assert bad_rows == []


def test_request_id_sources_stay_context_driven_within_workflow_c_scope():
    direct_header_reads: list[str] = []
    forbidden_literals: list[str] = []
    for path in REQUEST_ID_SOURCE_FILES:
        text = path.read_text(encoding="utf-8-sig")
        if "missing-request-id" in text:
            forbidden_literals.append(f"{path.relative_to(ROOT)} contains missing-request-id")
        if 'request.headers.get("X-Request-ID")' in text and path.name != "main.py":
            direct_header_reads.append(str(path.relative_to(ROOT)))

    assert not direct_header_reads, (
        "workflow C request_id sources must stay inside request_context; "
        f"found direct header reads in {direct_header_reads}"
    )
    assert not forbidden_literals, "workflow C files must not synthesize fallback request_id literals:\n" + "\n".join(forbidden_literals)


@pytest.mark.feature("FR12-ADMIN-07")
@pytest.mark.parametrize(
    ("header_request_id", "expected_request_id"),
    [
        ("req-backlink-admin", "req-backlink-admin"),
        ("   ", None),
        (None, None),
    ],
    ids=["explicit-header", "blank-header-generated", "generated-in-context"],
)
def test_request_id_roundtrips_response_to_admin_operation_and_audit_log(
    client,
    create_user,
    db_session,
    header_request_id: str | None,
    expected_request_id: str | None,
):
    password = "Password123"
    suffix = uuid4().hex[:8]
    admin = create_user(
        email=f"request-id-admin-{suffix}@test.com",
        password=password,
        role="admin",
        email_verified=True,
    )["user"]
    target = create_user(
        email=f"request-id-target-{suffix}@test.com",
        password=password,
        tier="Free",
        role="user",
        email_verified=True,
    )["user"]
    login = client.post("/auth/login", json={"email": admin.email, "password": password})
    assert login.status_code == 200

    headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}
    if header_request_id is not None:
        headers["X-Request-ID"] = header_request_id

    response = client.patch(
        f"/api/v1/admin/users/{target.user_id}",
        headers=headers,
        json={"tier": "Pro"},
    )

    assert response.status_code == 200
    body = response.json()
    request_id = body["request_id"]
    assert request_id == response.headers["X-Request-ID"]
    if expected_request_id is not None:
        assert request_id == expected_request_id
    else:
        UUID(request_id)

    op = db_session.execute(
        select(AdminOperation)
        .where(
            AdminOperation.action_type == "PATCH_USER",
            AdminOperation.target_pk == str(target.user_id),
            AdminOperation.request_id == request_id,
        )
        .order_by(AdminOperation.created_at.desc())
    ).scalars().first()
    assert op is not None

    audit = db_session.execute(
        select(AuditLog)
        .where(
            AuditLog.action_type == "PATCH_USER",
            AuditLog.target_pk == str(target.user_id),
            AuditLog.request_id == request_id,
        )
        .order_by(AuditLog.created_at.desc())
    ).scalars().first()
    assert audit is not None


@pytest.mark.feature("FR12-ADMIN-07")
@pytest.mark.parametrize(
    ("header_request_id", "expected_request_id"),
    [
        ("req-backlink-admin-reject", "req-backlink-admin-reject"),
        (None, None),
    ],
    ids=["explicit-header", "generated-in-context"],
)
def test_request_id_roundtrips_into_rejected_admin_audit_chain(
    client,
    create_user,
    db_session,
    header_request_id: str | None,
    expected_request_id: str | None,
):
    password = "Password123"
    suffix = uuid4().hex[:8]
    admin = create_user(
        email=f"request-id-reject-admin-{suffix}@test.com",
        password=password,
        role="admin",
        email_verified=True,
    )["user"]
    target = create_user(
        email=f"request-id-reject-target-{suffix}@test.com",
        password=password,
        tier="Free",
        role="user",
        email_verified=True,
    )["user"]
    login = client.post("/auth/login", json={"email": admin.email, "password": password})
    assert login.status_code == 200

    headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}
    if header_request_id is not None:
        headers["X-Request-ID"] = header_request_id

    response = client.patch(
        f"/api/v1/admin/users/{target.user_id}",
        headers=headers,
        json={"tier": "VIP"},
    )

    assert response.status_code == 422
    body = response.json()
    request_id = body["request_id"]
    assert request_id == response.headers["X-Request-ID"]
    if expected_request_id is not None:
        assert request_id == expected_request_id
    else:
        UUID(request_id)
    assert body["error_code"] == "INVALID_PAYLOAD"

    op = db_session.execute(
        select(AdminOperation)
        .where(
            AdminOperation.action_type == "PATCH_USER",
            AdminOperation.target_pk == str(target.user_id),
            AdminOperation.request_id == request_id,
            AdminOperation.status == "REJECTED",
        )
        .order_by(AdminOperation.created_at.desc())
    ).scalars().first()
    assert op is not None

    audit = db_session.execute(
        select(AuditLog)
        .where(
            AuditLog.action_type == "PATCH_USER",
            AuditLog.target_pk == str(target.user_id),
            AuditLog.request_id == request_id,
            AuditLog.failure_category == "invalid_tier",
        )
        .order_by(AuditLog.created_at.desc())
    ).scalars().first()
    assert audit is not None
