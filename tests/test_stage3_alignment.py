from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.core.db import build_engine, ensure_sqlite_schema_alignment
from app.models import Base


ROOT = Path(__file__).resolve().parents[1]
PHASE3_GATE = "Phase-3 gate: docs/core/99_AI驱动系统开发与Skill转化指南.md 行动 17、行动 19、行动 21"
SKILL_ROOT = ROOT / ".cursor" / "skills"
RULE_ROOT = ROOT / ".cursor" / "rules"

SKILL_CONTRACTS = {
    "SC-FR00-authenticity-guard": "docs/core/01_需求基线.md §2 FR-00",
    "SC-FR01-pool-refresh": "docs/core/01_需求基线.md §2 FR-01",
    "SC-FR02-scheduler-ops": "docs/core/01_需求基线.md §2 FR-02",
    "SC-FR03-cookie-session": "docs/core/01_需求基线.md §2 FR-03",
    "SC-FR04-multisource-ingest": "docs/core/01_需求基线.md §2 FR-04",
    "SC-FR05-market-state": "docs/core/01_需求基线.md §2 FR-05",
    "SC-FR06-report-generate": "docs/core/01_需求基线.md §2 FR-06",
    "SC-FR07-settlement-run": "docs/core/01_需求基线.md §2 FR-07",
    "SC-FR08-sim-positioning": "docs/core/01_需求基线.md §2 FR-08",
    "SC-FR09-auth-billing": "docs/core/01_需求基线.md §2 FR-09",
    "SC-FR09B-cleanup-retention": "docs/core/01_需求基线.md §2 FR-09-b",
    "SC-FR10-site-dashboard": "docs/core/01_需求基线.md §2 FR-10",
    "SC-FR11-feedback-review": "docs/core/01_需求基线.md §2 FR-11",
    "SC-FR12-admin-ops": "docs/core/01_需求基线.md §2 FR-12",
    "SC-FR13-event-notify": "docs/core/01_需求基线.md §2 FR-13",
    "SC-NFR03-contract-guard": "docs/core/01_需求基线.md §3 NFR-03",
    "SC-NFR12-health-check": "docs/core/01_需求基线.md §3 NFR-12",
    "SC-NFR14-envelope-contract": "docs/core/01_需求基线.md §3 NFR-14",
    "SC-NFR16-security-redline": "docs/core/01_需求基线.md §3 NFR-16",
    "SC-NFR17-token-rotation": "docs/core/01_需求基线.md §3 NFR-17",
    "SC-NFR18-schema-contract-test": "docs/core/01_需求基线.md §3 NFR-18",
    "SC-NFR19-admin-audit": "docs/core/01_需求基线.md §3 NFR-19",
    "SC-STAGE123-audit-executor": "stage 1-3 governance fixes for `docs/core/01~05/99`, `.cursor/rules`, `.cursor/skills`, and `docs/提示词/18_全量自动化提示词.md`",
    "SC-STAGE123-optimization-suggester": "stage 1-3 governance assets (`docs/core/01~05/99`, `.cursor/rules`, `.cursor/skills`, `docs/提示词/18_全量自动化提示词.md`)",
}


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def _assert_skill_contract(skill_id: str, primary_scope: str) -> None:
    text = (SKILL_ROOT / skill_id / "SKILL.md").read_text(encoding="utf-8")
    assert f"Primary scope: {primary_scope}" in text
    assert "Phase-3 gate:" in text
    assert "docs/core/99_AI驱动系统开发与Skill转化指南.md" in text
    assert "Workflow:" in text
    assert "docs/core/01~05" in text
    assert "References:" in text
    assert "Verification:" in text
    if skill_id.startswith("SC-STAGE123-"):
        assert "python scripts/check_stage123_agents.py" in text
    else:
        assert "pytest tests/ -v -k " in text
    assert "Equivalent replay:" in text


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _build_runtime_engine(tmp_path: Path):
    engine = build_engine(f"sqlite:///{tmp_path / 'stage3_alignment.db'}")
    Base.metadata.create_all(engine)
    ensure_sqlite_schema_alignment(engine)
    return engine


def test_stage3_skill_inventory_is_fully_covered():
    actual_skill_ids = {
        path.parent.name
        for path in SKILL_ROOT.glob("*/SKILL.md")
    }
    assert actual_skill_ids == set(SKILL_CONTRACTS)


@pytest.mark.parametrize(
    ("skill_id", "primary_scope"),
    sorted(SKILL_CONTRACTS.items()),
)
def test_stage3_skill_contracts(skill_id: str, primary_scope: str):
    _assert_skill_contract(skill_id, primary_scope)


def test_nfr03_stage3_rule_inventory_and_auth_path_alignment():
    role_rules = sorted(path.name for path in RULE_ROOT.glob("role-*.mdc"))
    assert role_rules == [
        "role-commerce.mdc",
        "role-data-engineer.mdc",
        "role-frontend-ux.mdc",
        "role-report-engineer.mdc",
        "role-test-quality.mdc",
    ]

    auto_rule = _read(".cursor/rules/auto-multi-ai-analysis.mdc")
    assert "辅助自动化 Rule" in auto_rule
    assert "不计入角色 Rule 数量" in auto_rule

    auth_route_text = _read("app/api/routes_auth.py")
    oauth_service_text = _read("app/services/oauth_service.py")
    api_bridge_text = _read("app/web/api-bridge.js")
    assert 'APIRouter(tags=["auth"])' in auth_route_text
    assert "/api/v1/auth" not in auth_route_text
    assert "/api/v1/auth" not in oauth_service_text
    assert "/api/v1/auth" not in api_bridge_text
    assert "if (path.startsWith('/auth/') || path.startsWith('/billing/')) return path;" in api_bridge_text


def test_nfr18_stage3_schema_checks_reject_legacy_status_values(tmp_path):
    _assert_skill_contract("SC-NFR18-schema-contract-test", "docs/core/01_需求基线.md §3 NFR-18")

    engine = _build_runtime_engine(tmp_path)
    cookie_session = Base.metadata.tables["cookie_session"]
    report_data_usage = Base.metadata.tables["report_data_usage"]
    report_generation_task = Base.metadata.tables["report_generation_task"]

    with engine.begin() as conn:
        ddl = conn.execute(text("SELECT sql FROM sqlite_master WHERE name = 'cookie_session'")).scalar_one()
        assert "REFRESH_FAILED" in ddl
        assert "ck_cookie_session_status_enum" in ddl

        conn.execute(
            Base.metadata.tables["stock_pool_refresh_task"].insert().values(
                task_id="stage3-refresh-task",
                trade_date=date(2026, 3, 8),
                status="COMPLETED",
                pool_version=1,
                fallback_from=None,
                filter_params_json={"target_pool_size": 1},
                core_pool_size=1,
                standby_pool_size=0,
                evicted_stocks_json=[],
                status_reason=None,
                request_id="stage3-request",
                started_at=_now_utc(),
                finished_at=_now_utc(),
                updated_at=_now_utc(),
                created_at=_now_utc(),
            )
        )

        with pytest.raises(IntegrityError):
            conn.execute(
                cookie_session.insert().values(
                    cookie_session_id=str(uuid4()),
                    provider="xueqiu",
                    account_key="stage3-check",
                    status="unknown",
                    created_at=_now_utc(),
                    updated_at=_now_utc(),
                )
            )

        with pytest.raises(IntegrityError):
            conn.execute(
                report_data_usage.insert().values(
                    usage_id=str(uuid4()),
                    trade_date=date(2026, 3, 8),
                    stock_code="600519.SH",
                    dataset_name="market_snapshot",
                    source_name="eastmoney",
                    batch_id="stage3-batch",
                    fetch_time=_now_utc(),
                    status="READY",
                    created_at=_now_utc(),
                )
            )

        with pytest.raises(IntegrityError):
            conn.execute(
                report_generation_task.insert().values(
                    task_id=str(uuid4()),
                    trade_date=date(2026, 3, 8),
                    stock_code="600519.SH",
                    idempotency_key="daily:600519.SH:2026-03-08",
                    generation_seq=1,
                    status="SUCCEEDED",
                    retry_count=0,
                    quality_flag="ok",
                    llm_fallback_level="primary",
                    risk_audit_status="completed",
                    market_state_trade_date=date(2026, 3, 8),
                    refresh_task_id="stage3-refresh-task",
                    queued_at=_now_utc(),
                    updated_at=_now_utc(),
                    created_at=_now_utc(),
                )
            )

        fk_rows = conn.execute(text("PRAGMA foreign_key_list(report_generation_task)")).mappings().all()
        assert any(
            row["from"] == "refresh_task_id" and row["table"] == "stock_pool_refresh_task" and row["to"] == "task_id"
            for row in fk_rows
        )
        index_rows = conn.execute(text("PRAGMA index_list(outbox_event)")).mappings().all()
        assert any(row["name"] == "idx_outbox_event_dispatch" for row in index_rows)
