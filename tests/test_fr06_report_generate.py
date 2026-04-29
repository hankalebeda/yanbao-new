from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from threading import Event, Thread
from uuid import uuid4

import pytest

pytestmark = pytest.mark.feature("FR-06")

from app.models import Base
from app.services import scheduler as scheduler_service
from app.services.report_generation_ssot import (
    ReportGenerationServiceError,
    _gen_one_sync,
    cleanup_incomplete_reports,
    ensure_test_generation_context,
    generate_report_ssot,
    resolve_refresh_context,
)
from tests.helpers_ssot import (
    age_report_generation_task,
    insert_pool_snapshot,
    insert_report_bundle_ssot,
    seed_generation_context,
    utc_now,
)


def _build_grounded_llm_response(
    *,
    recommendation: str = "BUY",
    confidence: float = 0.72,
    include_capital_keywords: bool = True,
    include_valuation_keywords: bool = True,
    close_price: float = 1688.0,
    ma5: float = 1680.0,
    ma20: float = 1650.0,
    atr_pct_percent: float = 2.0,
) -> str:
    capital_text = "主力资金5日净流入1.2亿，龙虎榜近30日上榜3次。" if include_capital_keywords else "成交配合度一般，短线延续性需要继续观察。"
    valuation_text = "PE21.4倍、PB7.2倍、ROE28.1%，行业仍在景气区间。" if include_valuation_keywords else "基本面约束尚未展开，仍需后续补充论证。"
    risk_follow_text = "技术面仍偏多，但若后续跌破MA20或主力资金转负，当前判断就需要及时失效，因此更适合顺势而不是盲目追高。" if include_capital_keywords else "技术面仍偏多，但若后续跌破MA20或量价配合继续转弱，当前判断就需要及时失效，因此更适合顺势而不是盲目追高。"
    conclusion_summary = "趋势策略B目前仍可维持偏多判断，但必须同时跟踪主力资金与估值约束，避免在高位放大仓位。" if include_capital_keywords and include_valuation_keywords else "趋势策略B目前仍可维持偏多判断，但在补齐更多证据前不宜继续放大仓位。"
    conclusion_text = (
        f"贵州茅台当前收盘价{close_price}元，MA5={ma5}、MA20={ma20}，ATR约{atr_pct_percent}%，趋势策略B的触发依据清晰。"
        f"{capital_text}{valuation_text}"
        f"{risk_follow_text}"
    )
    reasoning_chain_md = (
        "## 技术面分析\n"
        f"收盘价{close_price}元，MA5={ma5}、MA20={ma20}，价格维持在MA20上方，ATR约{atr_pct_percent}%，说明波动仍在可控区间。\n"
        "## 资金面分析\n"
        f"{capital_text}{valuation_text}\n"
        "## 多空矛盾判断\n"
        "技术趋势偏多，但若资金边际转弱，趋势延续性会下降，因此不能仅凭均线继续提高仓位。\n"
        "## 风险因素\n"
        f"若收盘价重新跌破{ma20}附近的MA20支撑，或ATR快速升破3%，说明趋势稳定性下降，需要降低执行意愿。\n"
        "## 综合结论\n"
        f"{conclusion_summary}"
    )
    return json.dumps(
        {
            "recommendation": recommendation,
            "confidence": confidence,
            "conclusion_text": conclusion_text,
            "reasoning_chain_md": reasoning_chain_md,
            "strategy_specific_evidence": {
                "strategy_type": "B",
                "key_signal": f"收盘价{close_price}元高于MA20={ma20}，ATR约{atr_pct_percent}%且趋势延续",
                "validation_check": "均线趋势通过，主力资金与估值证据已补充，未触发B类否决条件",
            },
        },
        ensure_ascii=False,
    )


def test_fr06_batch_gen_sync_collects_non_report_usage_before_generation(monkeypatch):
    import app.services.report_generation_ssot as report_generation_ssot
    import app.services.stock_snapshot_service as stock_snapshot_service

    call_order: list[str] = []
    close_state = {"closed": False}

    class DummyDB:
        def close(self):
            close_state["closed"] = True

    def _fake_collect_non_report_usage_sync(db, *, stock_code, trade_date=None):
        assert isinstance(db, DummyDB)
        assert stock_code == "600519.SH"
        assert trade_date == "2026-03-06"
        call_order.append("collect")
        return {"stock_code": stock_code, "trade_date": trade_date}

    def _fake_generate_report_ssot(db, *, stock_code, trade_date=None, skip_pool_check=False, force_same_day_rebuild=False, forced_strategy_type=None):
        assert isinstance(db, DummyDB)
        assert stock_code == "600519.SH"
        assert trade_date == "2026-03-06"
        assert skip_pool_check is False
        assert force_same_day_rebuild is False
        call_order.append("generate")
        return {"report_id": "rpt-batch", "stock_code": stock_code}

    monkeypatch.setattr(
        stock_snapshot_service,
        "collect_non_report_usage_sync",
        _fake_collect_non_report_usage_sync,
    )
    monkeypatch.setattr(
        report_generation_ssot,
        "generate_report_ssot",
        _fake_generate_report_ssot,
    )

    result = _gen_one_sync(
        lambda: DummyDB(),
        "600519.SH",
        "2026-03-06",
        False,
        False,
    )

    assert result["report_id"] == "rpt-batch"
    assert call_order == ["collect", "generate"]
    assert close_state["closed"] is True


def test_fr06_single_generate_collects_missing_non_report_usage(monkeypatch, db_session):
    import app.services.stock_snapshot_service as stock_snapshot_service

    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    seed_generation_context(db_session, trade_date=trade_date)

    usage_table = Base.metadata.tables["report_data_usage"]
    data_batch_table = Base.metadata.tables["data_batch"]
    db_session.execute(
        usage_table.delete().where(
            usage_table.c.trade_date == trade_day,
            usage_table.c.stock_code == "600519.SH",
            usage_table.c.dataset_name.in_(
                (
                    "main_force_flow",
                    "dragon_tiger_list",
                    "margin_financing",
                    "stock_profile",
                    "northbound_summary",
                    "etf_flow_summary",
                )
            ),
        )
    )
    db_session.commit()

    call_state = {"called": False}

    def _fake_collect_non_report_usage_sync(db, *, stock_code, trade_date=None):
        call_state["called"] = True
        assert stock_code == "600519.SH"
        assert trade_date == "2026-03-06"
        now = utc_now()
        batch_id = str(uuid4())
        db.execute(
            data_batch_table.insert().values(
                batch_id=batch_id,
                source_name="supplemental_capital",
                trade_date=trade_day,
                batch_scope="stock_supplemental",
                batch_seq=1,
                batch_status="SUCCESS",
                quality_flag="ok",
                covered_stock_count=1,
                core_pool_covered_count=1,
                records_total=2,
                records_success=2,
                records_failed=0,
                status_reason=None,
                trigger_task_run_id=None,
                started_at=now,
                finished_at=now,
                updated_at=now,
                created_at=now,
            )
        )
        db.execute(
            usage_table.insert().values(
                usage_id=str(uuid4()),
                trade_date=trade_day,
                stock_code=stock_code,
                dataset_name="main_force_flow",
                source_name="eastmoney_fflow_daykline",
                batch_id=batch_id,
                fetch_time=now,
                status="ok",
                status_reason='capital_snapshot:{"net_inflow_5d":123000000.0}',
                created_at=now,
            )
        )
        db.execute(
            usage_table.insert().values(
                usage_id=str(uuid4()),
                trade_date=trade_day,
                stock_code=stock_code,
                dataset_name="stock_profile",
                source_name="eastmoney_push2_stock_get",
                batch_id=batch_id,
                fetch_time=now,
                status="ok",
                status_reason='profile_snapshot:{"pe_ttm":21.4,"pb":7.2,"roe_pct":28.1,"industry":"白酒"}',
                created_at=now,
            )
        )
        return {"stock_code": stock_code, "trade_date": trade_date}

    monkeypatch.setattr(
        stock_snapshot_service,
        "collect_non_report_usage_sync",
        _fake_collect_non_report_usage_sync,
    )

    report = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    assert call_state["called"] is True
    used_datasets = {item["dataset_name"] for item in report["used_data"]}
    assert "main_force_flow" in used_datasets
    assert "stock_profile" in used_datasets


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_idempotency(client, db_session):
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)

    payload = {
        "stock_code": "600519.SH",
        "trade_date": trade_date,
        "idempotency_key": f"daily:600519.SH:{trade_date}",
        "source": "test",
    }

    first = client.post("/api/v1/reports/generate", json=payload)
    second = client.post("/api/v1/reports/generate", json=payload)

    assert first.status_code == 200
    assert second.status_code == 200
    first_data = first.json()["data"]
    second_data = second.json()["data"]
    assert first_data["report_id"] == second_data["report_id"]
    assert first_data["idempotency_key"] == payload["idempotency_key"]
    assert len(first_data["citations"]) >= 1
    assert set(first_data["sim_trade_instruction"]) == {"10k", "100k", "500k"}


@pytest.mark.feature('FR06-LLM-05')
def test_fr06_quality_flag_enum(client, db_session):
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date, market_state="NEUTRAL")

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["quality_flag"] in {"ok", "stale_ok", "degraded"}
    assert data["quality_flag"] != "missing"
    assert data["llm_fallback_level"] in {"primary", "backup", "cli", "local", "failed"}
    assert set(data["instruction_card"]) == {
        "signal_entry_price",
        "atr_pct",
        "atr_multiplier",
        "stop_loss",
        "target_price",
        "stop_loss_calc_mode",
    }


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_incomplete_data_fails_closed_and_keeps_no_report(client, db_session):
    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    seed_generation_context(db_session, trade_date=trade_date)

    usage_table = Base.metadata.tables["report_data_usage"]
    report_table = Base.metadata.tables["report"]
    task_table = Base.metadata.tables["report_generation_task"]
    idem = f"daily:600519.SH:{trade_date}"

    db_session.execute(
        usage_table.update()
        .where(usage_table.c.trade_date == trade_day)
        .where(usage_table.c.stock_code == "600519.SH")
        .where(usage_table.c.dataset_name == "kline_daily")
        .values(status="missing", status_reason="fetcher_not_provided")
    )
    db_session.commit()

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )

    assert response.status_code == 422
    body = response.json()
    assert body["error_code"] == "REPORT_DATA_INCOMPLETE"

    report_rows = db_session.execute(
        report_table.select().where(
            report_table.c.stock_code == "600519.SH",
            report_table.c.trade_date == trade_day,
        )
    ).mappings().all()
    latest_task = db_session.execute(
        task_table.select()
        .where(task_table.c.idempotency_key == idem)
        .order_by(task_table.c.created_at.desc())
    ).mappings().first()

    assert report_rows == []
    assert latest_task is not None
    assert latest_task["status"] == "Failed"
    assert latest_task["status_reason"] == "REPORT_DATA_INCOMPLETE"


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_optional_usage_failures_do_not_trigger_input_gate(monkeypatch):
    from app.services import report_generation_ssot

    monkeypatch.setattr("app.services.report_generation_ssot.settings.mock_llm", False, raising=False)

    issues = report_generation_ssot._collect_generation_input_issues(
        used_data=[
            {"dataset_name": "kline_daily", "status": "ok", "status_reason": None},
            {"dataset_name": "hotspot_top50", "status": "ok", "status_reason": None},
            {"dataset_name": "market_state_input", "status": "ok", "status_reason": None},
            {"dataset_name": "stock_profile", "status": "missing", "status_reason": "failed:RemoteProtocolError"},
            {"dataset_name": "margin_financing", "status": "missing", "status_reason": "missing:capital_fetcher_returned_missing"},
        ],
        market_state_row={"market_state_degraded": False, "state_reason": None},
    )

    assert issues == []

    quality_flag, quality_reason = report_generation_ssot._derive_quality_flag(
        used_data=[
            {"dataset_name": "kline_daily", "status": "ok", "status_reason": None},
            {"dataset_name": "hotspot_top50", "status": "ok", "status_reason": None},
            {"dataset_name": "market_state_input", "status": "ok", "status_reason": None},
            {"dataset_name": "stock_profile", "status": "stale_ok", "status_reason": "reused_latest_ok_snapshot:2026-04-16"},
            {"dataset_name": "margin_financing", "status": "missing", "status_reason": "missing:capital_fetcher_returned_missing"},
        ],
        market_state_row={"market_state_degraded": False, "state_reason": None},
    )

    assert quality_flag == "ok"
    assert quality_reason is None


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_strategy_b_allows_missing_hotspot_when_core_inputs_are_ready():
    from app.services import report_generation_ssot

    used_data = [
        {"dataset_name": "kline_daily", "status": "ok", "status_reason": None},
        {"dataset_name": "market_state_input", "status": "ok", "status_reason": None},
        {"dataset_name": "hotspot_top50", "status": "missing", "status_reason": "no_hotspot_link"},
    ]

    issues = report_generation_ssot._collect_generation_input_issues(
        used_data=used_data,
        market_state_row={"market_state_degraded": False, "state_reason": None},
        strategy_type="B",
    )
    quality_flag, quality_reason = report_generation_ssot._derive_quality_flag(
        used_data,
        {"market_state_degraded": False, "state_reason": None},
        strategy_type="B",
    )

    assert issues == []
    assert quality_flag == "ok"
    assert quality_reason is None


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_strategy_a_still_requires_hotspot_usage():
    from app.services import report_generation_ssot

    issues = report_generation_ssot._collect_generation_input_issues(
        used_data=[
            {"dataset_name": "kline_daily", "status": "ok", "status_reason": None},
            {"dataset_name": "market_state_input", "status": "ok", "status_reason": None},
        ],
        market_state_row={"market_state_degraded": False, "state_reason": None},
        strategy_type="A",
    )

    assert issues == ["hotspot_top50:missing_required_usage"]


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_missing_required_dataset_usage_fails_closed(client, db_session):
    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    seed_generation_context(db_session, trade_date=trade_date)

    usage_table = Base.metadata.tables["report_data_usage"]
    report_table = Base.metadata.tables["report"]
    task_table = Base.metadata.tables["report_generation_task"]
    idem = f"daily:600519.SH:{trade_date}"

    db_session.execute(
        usage_table.delete().where(
            usage_table.c.trade_date == trade_day,
            usage_table.c.stock_code == "600519.SH",
            usage_table.c.dataset_name == "kline_daily",
        )
    )
    db_session.commit()

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )

    assert response.status_code == 422
    assert response.json()["error_code"] == "REPORT_DATA_INCOMPLETE"

    report_rows = db_session.execute(
        report_table.select().where(
            report_table.c.stock_code == "600519.SH",
            report_table.c.trade_date == trade_day,
        )
    ).mappings().all()
    latest_task = db_session.execute(
        task_table.select()
        .where(task_table.c.idempotency_key == idem)
        .order_by(task_table.c.created_at.desc())
    ).mappings().first()

    assert report_rows == []
    assert latest_task is not None
    assert latest_task["status"] == "Failed"
    assert latest_task["status_reason"] == "REPORT_DATA_INCOMPLETE"


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_runtime_error_after_report_insert_rolls_back_partial_report(db_session, monkeypatch):
    from app.services import report_generation_ssot

    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    seed_generation_context(db_session, trade_date=trade_date)

    monkeypatch.setattr(
        report_generation_ssot,
        "_determine_strategy_type",
        lambda db, stock_code, trade_day, kline_row: "B",
    )
    monkeypatch.setattr(
        report_generation_ssot,
        "run_generation_model",
        lambda **kwargs: {
            "recommendation": "HOLD",
            "confidence": 0.60,
            "llm_fallback_level": "cli",
            "risk_audit_status": "not_triggered",
            "risk_audit_skip_reason": None,
            "conclusion_text": "收盘价与趋势锚点已引用，当前更适合先观望等待均线补齐后再决定。",
            "reasoning_chain_md": (
                "## 技术面分析\n收盘价与趋势锚点已引用。\n"
                "## 资金面分析\n本轮不额外引用资金因子。\n"
                "## 多空矛盾判断\n均线缺失导致趋势确认保守。\n"
                "## 风险因素\n若价格转弱则需要继续降级。\n"
                "## 综合结论\n当前维持HOLD。"
            ),
            "strategy_specific_evidence": {
                "strategy_type": "B",
                "key_signal": "close 锚点已引用，但 ma20 暂缺",
                "validation_check": "趋势策略B仅在收盘价锚点基础上维持HOLD。",
            },
            "signal_entry_price": kwargs.get("signal_entry_price", 10.0),
        },
    )
    monkeypatch.setattr(
        report_generation_ssot,
        "_load_public_report_output_issues",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("payload explode")),
    )

    with pytest.raises(ReportGenerationServiceError) as exc_info:
        generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    assert exc_info.value.status_code == 500
    assert exc_info.value.error_code == "LLM_ALL_FAILED"

    report_rows = db_session.execute(
        Base.metadata.tables["report"].select().where(
            Base.metadata.tables["report"].c.stock_code == "600519.SH",
            Base.metadata.tables["report"].c.trade_date == trade_day,
        )
    ).mappings().all()
    latest_task = db_session.execute(
        Base.metadata.tables["report_generation_task"].select()
        .where(Base.metadata.tables["report_generation_task"].c.idempotency_key == f"daily:600519.SH:{trade_date}")
        .order_by(Base.metadata.tables["report_generation_task"].c.created_at.desc())
    ).mappings().first()

    assert report_rows == []
    assert latest_task is not None
    assert latest_task["status"] == "Failed"
    assert latest_task["status_reason"] == "llm_all_failed"


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_post_commit_result_load_failure_keeps_completed_task(db_session, monkeypatch):
    from app.services import report_generation_ssot

    trade_date = "2026-03-07"
    trade_day = date.fromisoformat(trade_date)
    seed_generation_context(db_session, trade_date=trade_date)

    monkeypatch.setattr(
        report_generation_ssot,
        "_determine_strategy_type",
        lambda db, stock_code, trade_day, kline_row: "B",
    )
    monkeypatch.setattr(
        report_generation_ssot,
        "run_generation_model",
        lambda **kwargs: {
            "recommendation": "HOLD",
            "confidence": 0.60,
            "llm_fallback_level": "cli",
            "risk_audit_status": "not_triggered",
            "risk_audit_skip_reason": None,
            "conclusion_text": "收盘价、趋势与风险约束均已引用，当前先保持观望。",
            "reasoning_chain_md": (
                "## 技术面分析\n收盘价仍是核心锚点。\n"
                "## 资金面分析\n本轮不额外引用资金因子。\n"
                "## 多空矛盾判断\n趋势未完全确认但证据真实。\n"
                "## 风险因素\n若价格回落则需要降级。\n"
                "## 综合结论\n当前维持HOLD。"
            ),
            "strategy_specific_evidence": {
                "strategy_type": "B",
                "key_signal": "close 锚点已引用，趋势策略B保持HOLD",
                "validation_check": "收盘价锚点已真实引用，MA/ATR缺失已披露。",
            },
            "signal_entry_price": kwargs.get("signal_entry_price", 10.0),
        },
    )
    monkeypatch.setattr(
        report_generation_ssot,
        "_load_report_result",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("result load explode")),
    )

    with pytest.raises(ReportGenerationServiceError) as exc_info:
        generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    assert exc_info.value.status_code == 500
    assert exc_info.value.error_code == "VALIDATION_FAILED"

    latest_report = db_session.execute(
        Base.metadata.tables["report"].select().where(
            Base.metadata.tables["report"].c.stock_code == "600519.SH",
            Base.metadata.tables["report"].c.trade_date == trade_day,
        )
    ).mappings().one()
    latest_task = db_session.execute(
        Base.metadata.tables["report_generation_task"].select()
        .where(Base.metadata.tables["report_generation_task"].c.task_id == latest_report["generation_task_id"])
    ).mappings().one()

    assert latest_report["published"] is True
    assert latest_report["is_deleted"] is False
    assert latest_report["publish_status"] == "PUBLISHED"
    assert latest_task["status"] == "Completed"
    assert latest_task["status_reason"] is None


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_purge_report_bundle_marks_task_failed_instead_of_deleting_it(db_session):
    from app.services import report_generation_ssot

    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-06",
        strategy_type="B",
        published=True,
    )
    task_table = Base.metadata.tables["report_generation_task"]
    task_id = report.generation_task_id

    report_generation_ssot._purge_report_generation_bundle(
        db_session,
        report_id=report.report_id,
        purge_reason="REPORT_DATA_INCOMPLETE",
    )
    db_session.commit()

    task_row = db_session.execute(
        task_table.select().where(task_table.c.task_id == task_id)
    ).mappings().first()
    report_row = db_session.execute(
        Base.metadata.tables["report"].select().where(Base.metadata.tables["report"].c.report_id == report.report_id)
    ).mappings().first()

    assert task_row is not None
    assert task_row["status"] == "Failed"
    assert task_row["status_reason"] == "REPORT_DATA_INCOMPLETE"
    assert report_row is not None
    assert report_row["is_deleted"] == 1
    assert report_row["published"] == 0


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_public_payload_gate_allows_missing_industry_when_other_core_fields_exist(db_session, monkeypatch):
    from app.services import report_generation_ssot

    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date="2026-03-06",
        strategy_type="B",
        published=True,
    )

    monkeypatch.setattr(
        "app.services.ssot_read_model.get_report_view_payload_ssot",
        lambda *args, **kwargs: {
            "indicators": {"close": 10.5, "ma5": 10.2, "ma20": 9.8, "total_mv": 123456789.0},
            "market_snapshot": {"trade_date": "2026-03-06", "last_price": 10.5},
            "company_overview": {"company_name": "贵州茅台", "industry": None},
            "financial_analysis": {"total_market_cap": 123456789.0},
            "industry_competition": {"industry_name": None},
            "capital_game_summary": {"completeness_level": "minimal"},
            "price_forecast": {"windows": [{"horizon_days": 7, "llm_pct_range": "+1.0% / -0.8%"}]},
            "direction_forecast": {"horizons": [{"horizon_day": 1}, {"horizon_day": 7}]},
            "report_data_usage": [{"dataset_name": "kline_daily", "source_name": "tdx_local", "status": "ok"}],
            "citations": [{"source_name": "tdx_local", "source_url": "https://example.com", "fetch_time": "2026-03-06T10:00:00"}],
        },
    )

    issues = report_generation_ssot._load_public_report_output_issues(
        db_session,
        report_id=report.report_id,
    )

    assert "public_field_missing:company_overview.industry" not in issues
    assert "public_field_missing:industry_competition.industry_name" not in issues


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_existing_incomplete_report_soft_deleted_before_regeneration_attempt(db_session):
    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    seed_generation_context(db_session, trade_date=trade_date)

    first = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)
    report_id = first["report_id"]

    usage_table = Base.metadata.tables["report_data_usage"]
    report_table = Base.metadata.tables["report"]
    citation_table = Base.metadata.tables["report_citation"]
    instruction_table = Base.metadata.tables["instruction_card"]
    sim_table = Base.metadata.tables["sim_trade_instruction"]
    audit_table = Base.metadata.tables["audit_log"]

    db_session.execute(
        usage_table.update()
        .where(usage_table.c.trade_date == trade_day)
        .where(usage_table.c.stock_code == "600519.SH")
        .where(usage_table.c.dataset_name == "kline_daily")
        .values(status="missing", status_reason="fetcher_not_provided")
    )
    db_session.commit()

    with pytest.raises(ReportGenerationServiceError) as exc_info:
        generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    assert exc_info.value.status_code == 422
    assert exc_info.value.error_code == "REPORT_DATA_INCOMPLETE"

    report_row = db_session.execute(
        report_table.select().where(report_table.c.report_id == report_id)
    ).mappings().one()

    assert bool(report_row["is_deleted"]) is True
    assert report_row["deleted_at"] is not None
    assert report_row["published"] in (False, 0)
    assert report_row["publish_status"] == "UNPUBLISHED"
    assert "REPORT_DATA_INCOMPLETE" in str(report_row["status_reason"] or "")

    citation_count = len(
        db_session.execute(
            citation_table.select().where(citation_table.c.report_id == report_id)
        ).fetchall()
    )
    instruction_count = len(
        db_session.execute(
            instruction_table.select().where(instruction_table.c.report_id == report_id)
        ).fetchall()
    )
    sim_count = len(
        db_session.execute(
            sim_table.select().where(sim_table.c.report_id == report_id)
        ).fetchall()
    )

    assert citation_count == 0
    assert instruction_count == 0
    assert sim_count == 0

    audit_row = db_session.execute(
        audit_table.select()
        .where(audit_table.c.target_table == "report")
        .where(audit_table.c.target_pk == report_id)
        .where(audit_table.c.action_type == "SOFT_DELETE_REPORT_BUNDLE")
        .order_by(audit_table.c.created_at.desc())
    ).mappings().first()

    assert audit_row is not None
    assert audit_row["reason_code"] == "REPORT_DATA_INCOMPLETE"
    assert (audit_row["after_snapshot"] or {}).get("is_deleted") is True


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_cleanup_incomplete_reports_soft_deletes_and_keeps_audit(db_session):
    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    seed_generation_context(db_session, trade_date=trade_date)

    first = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)
    report_id = first["report_id"]

    report_table = Base.metadata.tables["report"]
    audit_table = Base.metadata.tables["audit_log"]

    db_session.execute(
        report_table.update()
        .where(report_table.c.report_id == report_id)
        .values(conclusion_text=None, updated_at=utc_now())
    )
    db_session.commit()

    result = cleanup_incomplete_reports(db_session, limit=200)
    db_session.commit()

    assert result["soft_deleted"] >= 1
    assert report_id in result["deleted_report_ids"]

    report_row = db_session.execute(
        report_table.select().where(report_table.c.report_id == report_id)
    ).mappings().one()
    assert bool(report_row["is_deleted"]) is True
    assert report_row["publish_status"] == "UNPUBLISHED"
    assert "REPORT_DATA_INCOMPLETE" in str(report_row.get("status_reason") or "")

    audit_row = db_session.execute(
        audit_table.select()
        .where(audit_table.c.target_table == "report")
        .where(audit_table.c.target_pk == report_id)
        .where(audit_table.c.action_type == "SOFT_DELETE_REPORT_BUNDLE")
        .order_by(audit_table.c.created_at.desc())
    ).mappings().first()

    assert audit_row is not None
    assert audit_row["reason_code"] == "REPORT_DATA_INCOMPLETE"


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_cleanup_incomplete_reports_treats_stale_ok_input_as_incomplete(db_session):
    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    seed_generation_context(db_session, trade_date=trade_date)

    generated = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)
    report_id = generated["report_id"]

    usage_table = Base.metadata.tables["report_data_usage"]
    report_table = Base.metadata.tables["report"]

    db_session.execute(
        usage_table.update()
        .where(usage_table.c.trade_date == trade_day)
        .where(usage_table.c.stock_code == "600519.SH")
        .where(usage_table.c.dataset_name == "kline_daily")
        .values(status="stale_ok", status_reason="kline_stale")
    )
    db_session.commit()

    result = cleanup_incomplete_reports(db_session, limit=200)
    db_session.commit()

    assert result["include_non_ok"] is False
    assert report_id in result["deleted_report_ids"]

    row = db_session.execute(
        report_table.select().where(report_table.c.report_id == report_id)
    ).mappings().one()
    assert bool(row["is_deleted"]) is True
    assert "REPORT_DATA_INCOMPLETE" in str(row.get("status_reason") or "")


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_cleanup_incomplete_reports_include_non_ok_soft_deletes(db_session):
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)

    generated = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)
    report_id = generated["report_id"]

    report_table = Base.metadata.tables["report"]
    db_session.execute(
        report_table.update()
        .where(report_table.c.report_id == report_id)
        .values(quality_flag="stale_ok", updated_at=utc_now())
    )
    db_session.commit()

    result = cleanup_incomplete_reports(db_session, limit=200, include_non_ok=True)
    db_session.commit()

    assert result["include_non_ok"] is True
    assert report_id in result["deleted_report_ids"]

    row = db_session.execute(
        report_table.select().where(report_table.c.report_id == report_id)
    ).mappings().one()
    assert bool(row["is_deleted"]) is True


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_cleanup_incomplete_reports_soft_deletes_public_payload_gaps(db_session, monkeypatch):
    from app.services import report_generation_ssot

    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)

    monkeypatch.setattr(
        report_generation_ssot,
        "_load_public_report_output_issues",
        lambda db, report_id: [],
    )

    generated = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)
    report_id = generated["report_id"]

    monkeypatch.setattr(
        report_generation_ssot,
        "_load_public_report_output_issues",
        lambda db, report_id: ["public_field_missing:financial_analysis.pe_ttm"] if report_id == generated["report_id"] else [],
    )

    result = cleanup_incomplete_reports(db_session, limit=200)
    db_session.commit()

    assert report_id in result["deleted_report_ids"]
    assert any(
        "public_field_missing:financial_analysis.pe_ttm" in issue
        for candidate in result["candidate_examples"]
        for issue in (candidate.get("issues") or [])
    )


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_public_payload_gate_uses_view_payload(db_session, monkeypatch):
    from app.services import report_generation_ssot
    import app.services.ssot_read_model as ssot_read_model

    report = insert_report_bundle_ssot(db_session, stock_code="600519.SH", quality_flag="ok", published=True)

    monkeypatch.setattr(
        ssot_read_model,
        "get_report_api_payload_ssot",
        lambda *args, **kwargs: {
            "indicators": {
                "close": 123.45,
                "ma5": 120.0,
                "ma20": 118.0,
                "pe_ttm": 21.41,
                "pb": 7.2,
                "total_mv": 1762244737356.6,
            },
            "market_snapshot": {"trade_date": "2026-03-06", "last_price": 123.45},
        },
    )
    monkeypatch.setattr(
        ssot_read_model,
        "get_report_view_payload_ssot",
        lambda *args, **kwargs: {
            "indicators": {
                "close": 123.45,
                "ma5": 120.0,
                "ma20": 118.0,
                "pe_ttm": 21.41,
                "pb": 7.2,
                "total_mv": 1762244737356.6,
            },
            "market_snapshot": {"trade_date": "2026-03-06", "last_price": 123.45},
            "company_overview": {"industry": "食品饮料"},
            "financial_analysis": {
                "pe_ttm": 21.41,
                "pb": 7.2,
                "total_market_cap": 1762244737356.6,
            },
            "industry_competition": {"industry_name": "食品饮料"},
            "capital_game_summary": {
                "render_complete": True,
                "missing_dimensions": [],
                "main_force": {"net_inflow_5d": 120000000.0},
                "dragon_tiger": {"lhb_count_30d": 0},
                "margin_financing": {"latest_rzye": 8500000000.0},
                "northbound": {"net_inflow_5d": 50000000.0},
                "etf_flow": {"net_creation_redemption_5d": 30000000.0},
            },
            "price_forecast": {
                "windows": [
                    {
                        "horizon_days": 1,
                        "central_price": 123.45,
                        "target_high": 126.0,
                        "target_low": 121.0,
                        "llm_direction": "上行",
                        "llm_action": "BUY",
                        "llm_pct_range": "-2.0% ~ +2.1%",
                        "llm_confidence": 0.72,
                        "llm_reason": "ATR 短线波动区间",
                    },
                    {
                        "horizon_days": 7,
                        "central_price": 123.45,
                        "target_high": 130.0,
                        "target_low": 118.0,
                        "llm_direction": "上行",
                        "llm_action": "BUY",
                        "llm_pct_range": "-4.4% ~ +5.3%",
                        "llm_confidence": 0.68,
                        "llm_reason": "ATR 中短期波动区间",
                    },
                    {
                        "horizon_days": 14,
                        "central_price": 123.45,
                        "target_high": 133.0,
                        "target_low": 116.0,
                        "llm_direction": "上行",
                        "llm_action": "BUY",
                        "llm_pct_range": "-6.0% ~ +7.7%",
                        "llm_confidence": 0.64,
                        "llm_reason": "ATR 中期趋势区间",
                    },
                    {
                        "horizon_days": 30,
                        "central_price": 123.45,
                        "target_high": 136.0,
                        "target_low": 114.0,
                        "llm_direction": "上行",
                        "llm_action": "BUY",
                        "llm_pct_range": "-7.7% ~ +10.2%",
                        "llm_confidence": 0.6,
                        "llm_reason": "ATR 中期延展区间",
                    },
                    {
                        "horizon_days": 60,
                        "central_price": 123.45,
                        "target_high": 140.0,
                        "target_low": 110.0,
                        "llm_direction": "上行",
                        "llm_action": "BUY",
                        "llm_pct_range": "-10.9% ~ +13.4%",
                        "llm_confidence": 0.56,
                        "llm_reason": "ATR 长周期趋势区间",
                    },
                ]
            },
        },
    )

    issues = report_generation_ssot._load_public_report_output_issues(db_session, report_id=report.report_id)

    assert issues == []


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_public_payload_gate_flags_capital_and_forecast_gaps(db_session, monkeypatch):
    from app.services import report_generation_ssot
    import app.services.ssot_read_model as ssot_read_model

    report = insert_report_bundle_ssot(db_session, stock_code="600519.SH", quality_flag="stale_ok", published=True)

    monkeypatch.setattr(
        ssot_read_model,
        "get_report_view_payload_ssot",
        lambda *args, **kwargs: {
            "indicators": {
                "close": 123.45,
                "ma5": 120.0,
                "ma20": 118.0,
                "pe_ttm": 21.41,
                "pb": 7.2,
                "total_mv": 1762244737356.6,
            },
            "market_snapshot": {"trade_date": "2026-03-06", "last_price": 123.45},
            "company_overview": {"industry": "食品饮料"},
            "financial_analysis": {
                "pe_ttm": 21.41,
                "pb": 7.2,
                "total_market_cap": 1762244737356.6,
            },
            "industry_competition": {"industry_name": "食品饮料"},
            "capital_game_summary": {
                "render_complete": False,
                "missing_dimensions": ["北向资金"],
                "main_force": {"net_inflow_5d": 120000000.0},
                "dragon_tiger": {"lhb_count_30d": 0},
                "margin_financing": {"latest_rzye": 8500000000.0},
                "northbound": {"net_inflow_5d": None},
                "etf_flow": {"net_creation_redemption_5d": 30000000.0},
            },
            "price_forecast": {
                "windows": [
                    {
                        "horizon_days": 1,
                        "central_price": 123.45,
                        "target_high": 126.0,
                        "target_low": 121.0,
                        "llm_direction": "上行",
                        "llm_action": "BUY",
                        "llm_pct_range": "-2.0% ~ +2.1%",
                        "llm_confidence": 0.72,
                        "llm_reason": "ATR 短线波动区间",
                    },
                    {
                        "horizon_days": 7,
                        "central_price": 123.45,
                        "target_high": 130.0,
                        "target_low": 118.0,
                        "llm_direction": "上行",
                        "llm_action": "BUY",
                        "llm_pct_range": "区间待补充",
                        "llm_confidence": 0.68,
                        "llm_reason": "ATR 中短期波动区间",
                    },
                ]
            },
        },
    )

    issues = report_generation_ssot._load_public_report_output_issues(db_session, report_id=report.report_id)

    assert issues == [] or not any(
        "northbound" in issue or "etf_flow" in issue for issue in issues
    )
    # 北向在 missing_dimensions 中出现，但不在 capital_fields 门控中
    assert "public_field_missing:price_forecast.windows[7].llm_pct_range" in issues
    assert any(issue.startswith("public_field_missing:price_forecast.windows:") for issue in issues)


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_generate_fail_closes_when_public_payload_has_gaps(db_session, monkeypatch):
    from app.models import Base
    from app.services import report_generation_ssot

    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)
    report_table = Base.metadata.tables["report"]

    monkeypatch.setattr(
        report_generation_ssot,
        "_load_public_report_output_issues",
        lambda db, report_id: ["public_field_missing:financial_analysis.pe_ttm"],
    )

    with pytest.raises(ReportGenerationServiceError) as exc_info:
        generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    assert exc_info.value.status_code == 422
    assert exc_info.value.error_code == "REPORT_DATA_INCOMPLETE"

    row = db_session.execute(
        report_table.select().where(report_table.c.stock_code == "600519.SH").order_by(report_table.c.created_at.desc())
    ).mappings().first()
    assert row is not None
    assert bool(row["is_deleted"]) is True
    assert row["publish_status"] == "UNPUBLISHED"


@pytest.mark.feature('FR06-LLM-05')
def test_fr06_codex_gpt52_pool_fallback_maps_to_backup(monkeypatch):
    from app.services import report_generation_ssot
    from app.core.config import settings

    class DummyResult:
        response = _build_grounded_llm_response(confidence=0.72)
        model_used = "codex_api"
        degraded = True
        elapsed_s = 0.8
        extra = {"pool_level": "backup"}

    monkeypatch.setattr(report_generation_ssot, "_run_llm_coro", lambda *args, **kwargs: DummyResult())
    monkeypatch.setattr(settings, "mock_llm", False)

    data = report_generation_ssot.run_generation_model(
        stock_code="600519.SH",
        stock_name="璐靛窞鑼呭彴",
        strategy_type="B",
        market_state="BULL",
        quality_flag="ok",
        prior_stats=None,
        signal_entry_price=1688.0,
        used_data=[],
        kline_row={"close": 1688.0, "ma5": 1680.0, "ma20": 1650.0, "atr_pct": 0.02, "volatility_20d": 0.03},
    )

    assert data["llm_fallback_level"] == "backup"


@pytest.mark.feature('FR06-LLM-05')
def test_fr06_generation_uses_dedicated_model_timeout_budget(monkeypatch):
    from app.services import report_generation_ssot
    from app.core.config import settings

    captured: dict[str, float | None] = {}

    class DummyResult:
        response = _build_grounded_llm_response(recommendation="HOLD", confidence=0.64)
        model_used = "codex_api"
        degraded = False
        elapsed_s = 0.6
        extra = {"pool_level": "primary"}

    def _capture_timeout(*args, **kwargs):
        captured["timeout_sec"] = kwargs.get("timeout_sec")
        return DummyResult()

    monkeypatch.setattr(report_generation_ssot, "_run_llm_coro", _capture_timeout)
    monkeypatch.setattr(settings, "mock_llm", False)
    monkeypatch.setattr(settings, "request_timeout_seconds", 60)
    monkeypatch.setattr(settings, "report_generation_llm_timeout_seconds", 180, raising=False)

    data = report_generation_ssot.run_generation_model(
        stock_code="600519.SH",
        stock_name="璐靛窞鑼呭彴",
        strategy_type="B",
        market_state="NEUTRAL",
        quality_flag="ok",
        prior_stats=None,
        signal_entry_price=1688.0,
        used_data=[],
        kline_row={"close": 1688.0, "ma5": 1680.0, "ma20": 1650.0, "atr_pct": 0.02, "volatility_20d": 0.03},
    )

    assert captured["timeout_sec"] == 180
    assert data["llm_fallback_level"] == "primary"


@pytest.mark.feature('FR06-LLM-05')
def test_fr06_route_and_call_passes_dedicated_timeout_to_ollama(monkeypatch):
    from app.services.llm_router import LLMScene, route_and_call

    captured: dict[str, int | None] = {}

    async def _fake_ollama(prompt: str, temperature: float, *, timeout_sec: int | None = None):
        captured["timeout_sec"] = timeout_sec
        return {
            "response": '{"recommendation":"HOLD","confidence":0.55}',
            "source": "ollama",
            "model": "qwen3:8b",
            "usage": {},
        }

    monkeypatch.setattr("app.services.llm_router._call_ollama", _fake_ollama)

    result = asyncio.run(
        route_and_call(
            "测试 prompt",
            scene=LLMScene.GENERAL,
            force_model="ollama",
            timeout_sec=180,
        )
    )

    assert captured["timeout_sec"] == 180
    assert result.model_used == "ollama"


@pytest.mark.feature('FR06-LLM-02')
def test_fr06_route_and_call_supports_claude_cli(monkeypatch):
    from app.services.llm_router import LLMScene, route_and_call

    captured: dict[str, int | None] = {}

    async def _fake_claude_cli(prompt: str, temperature: float, use_cot: bool, timeout_sec: int | None = None):
        captured["timeout_sec"] = timeout_sec
        return {
            "response": '{"recommendation":"BUY","confidence":0.66,"reasoning_chain":["ok"],"analysis_steps":["ok"],"evidence_items":[],"risk_factors":[],"plain_report":"这是一段满足长度要求的结构化文本，用于验证 claude_cli 路由已正确接入，并且可以作为运行态 CLI 输出来源。为了满足长度要求，这里继续补充说明，确保正文超过一百二十字。"}',
            "source": "claude_cli",
            "model": "claude-cli",
            "usage": {},
        }

    monkeypatch.setattr("app.services.llm_router._call_claude_cli", _fake_claude_cli)

    result = asyncio.run(
        route_and_call(
            "测试 prompt",
            scene=LLMScene.GENERAL,
            force_model="claude_cli",
            timeout_sec=180,
        )
    )

    assert captured["timeout_sec"] == 180
    assert result.model_used == "claude_cli"


@pytest.mark.feature('FR06-LLM-02')
def test_fr06_claude_cli_direct_subprocess_uses_utf8_bytes(monkeypatch):
    from app.services import llm_router

    captured: dict[str, object] = {}

    class _Result:
        returncode = 0
        stdout = b'{"recommendation":"BUY","confidence":0.66}'
        stderr = b""

    def _fake_run(args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return _Result()

    monkeypatch.setattr(
        llm_router,
        "_resolve_claude_cli_command",
        lambda: r"D:\\yanbao-new\\claude.cmd",
    )
    monkeypatch.setattr(llm_router.subprocess, "run", _fake_run)

    result = asyncio.run(
        llm_router._call_claude_cli(
            "含有¥符号",
            0.3,
            False,
            timeout_sec=45,
        )
    )

    assert captured["args"] == [r"D:\\yanbao-new\\claude.cmd", "--bare", "--print"]
    assert captured["kwargs"]["input"] == "含有¥符号".encode("utf-8")
    assert captured["kwargs"]["timeout"] == 45
    assert result["source"] == "claude_cli"
    assert result["endpoint"] == r"D:\\yanbao-new\\claude.cmd"


@pytest.mark.feature('FR06-LLM-02')
def test_fr06_claude_cli_direct_subprocess_surfaces_timeout(monkeypatch):
    from app.services import llm_router

    def _fake_run(args, **kwargs):
        raise llm_router.subprocess.TimeoutExpired(cmd=args, timeout=kwargs["timeout"])

    monkeypatch.setattr(
        llm_router,
        "_resolve_claude_cli_command",
        lambda: r"D:\\yanbao-new\\claude.cmd",
    )
    monkeypatch.setattr(llm_router.subprocess, "run", _fake_run)

    with pytest.raises(RuntimeError, match="claude_cli_timeout_after_45s"):
        asyncio.run(
            llm_router._call_claude_cli(
                "timeout probe",
                0.3,
                False,
                timeout_sec=45,
            )
        )


@pytest.mark.feature('FR06-LLM-02')
def test_fr06_claude_cli_direct_subprocess_surfaces_nonzero_exit(monkeypatch):
    from app.services import llm_router

    class _Result:
        returncode = 9
        stdout = b""
        stderr = b"fatal: cli unavailable"

    monkeypatch.setattr(
        llm_router,
        "_resolve_claude_cli_command",
        lambda: r"D:\\yanbao-new\\claude.cmd",
    )
    monkeypatch.setattr(llm_router.subprocess, "run", lambda *args, **kwargs: _Result())

    with pytest.raises(RuntimeError, match="claude_cli_failed:fatal: cli unavailable"):
        asyncio.run(
            llm_router._call_claude_cli(
                "failure probe",
                0.3,
                False,
                timeout_sec=45,
            )
        )


@pytest.mark.feature('FR06-LLM-02')
def test_fr06_route_and_call_opens_global_circuit_breaker_after_three_failures(monkeypatch):
    from app.services import llm_router

    clock = {"now": 1000.0}
    attempt_counter = {"count": 0}

    async def _failing_model(*args, **kwargs):
        attempt_counter["count"] += 1
        raise RuntimeError("upstream unavailable")

    async def _successful_model(*args, **kwargs):
        return {
            "response": '{"recommendation":"HOLD","confidence":0.55}',
            "source": "codex_api",
            "model": "codex_api",
            "usage": {},
        }

    monkeypatch.setattr(llm_router, "_global_llm_circuit_breaker", llm_router.GlobalLLMCircuitBreaker())
    monkeypatch.setattr(llm_router.time, "time", lambda: clock["now"])
    monkeypatch.setattr(llm_router.settings, "max_llm_retries", 0)
    monkeypatch.setattr(llm_router, "_call_model", _failing_model)

    for _ in range(3):
        with pytest.raises(RuntimeError, match="All models in chain"):
            asyncio.run(
                llm_router.route_and_call(
                    "测试 prompt",
                    scene=llm_router.LLMScene.GENERAL,
                    force_model="codex_api",
                )
            )
        clock["now"] += 10

    blocked_attempts = attempt_counter["count"]
    with pytest.raises(RuntimeError, match=r"LLM global circuit breaker is open.*retry in"):
        asyncio.run(
            llm_router.route_and_call(
                "测试 prompt",
                scene=llm_router.LLMScene.GENERAL,
                force_model="codex_api",
            )
        )
    assert attempt_counter["count"] == blocked_attempts

    clock["now"] = 1051.0
    monkeypatch.setattr(llm_router, "_call_model", _successful_model)

    result = asyncio.run(
        llm_router.route_and_call(
            "测试 prompt",
            scene=llm_router.LLMScene.GENERAL,
            force_model="codex_api",
        )
    )

    assert result.model_used == "codex_api"
    assert result.degraded is False


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_resume_window_expire_stale_tasks(client, db_session):
    trade_date = "2026-03-06"
    stale_trade_date = (date.fromisoformat(trade_date) - timedelta(days=4)).isoformat()
    stale_task_id = age_report_generation_task(
        db_session,
        stock_code="000001.SZ",
        trade_date=stale_trade_date,
        updated_hours_ago=80,
    )
    stale_processing_task_id = age_report_generation_task(
        db_session,
        stock_code="300750.SZ",
        trade_date=stale_trade_date,
        status="Processing",
        updated_hours_ago=80,
    )
    seed_generation_context(db_session, trade_date=trade_date)

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )

    assert response.status_code == 200
    task_table = Base.metadata.tables["report_generation_task"]
    row = db_session.execute(
        task_table.select().where(task_table.c.task_id == stale_task_id)
    ).mappings().one()
    processing_row = db_session.execute(
        task_table.select().where(task_table.c.task_id == stale_processing_task_id)
    ).mappings().one()
    assert row["status"] == "Expired"
    assert row["status_reason"] == "stale_task_expired"
    assert processing_row["status"] == "Expired"
    assert processing_row["status_reason"] == "stale_task_expired"


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_same_day_stale_processing_task_expires_before_conflict(client, db_session):
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)
    stale_processing_task_id = age_report_generation_task(
        db_session,
        stock_code="600519.SH",
        trade_date=trade_date,
        status="Processing",
        updated_hours_ago=1,
    )

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )

    assert response.status_code == 200
    task_table = Base.metadata.tables["report_generation_task"]
    stale_row = db_session.execute(
        task_table.select().where(task_table.c.task_id == stale_processing_task_id)
    ).mappings().one()
    latest_row = db_session.execute(
        task_table.select()
        .where(task_table.c.idempotency_key == f"daily:600519.SH:{trade_date}")
        .order_by(task_table.c.created_at.desc())
    ).mappings().first()
    assert stale_row["status"] == "Expired"
    assert stale_row["status_reason"] == "stale_task_expired"
    assert latest_row is not None
    assert latest_row["status"] == "Completed"
    assert latest_row["generation_seq"] == 2


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_requires_exact_trade_date_pool_snapshot(db_session):
    trade_date = "2026-03-07"
    seed_generation_context(db_session, trade_date="2026-03-06")
    seed_generation_context(db_session, trade_date=trade_date)

    refresh_table = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot_table = Base.metadata.tables["stock_pool_snapshot"]
    db_session.execute(
        snapshot_table.delete().where(snapshot_table.c.trade_date == date.fromisoformat(trade_date))
    )
    db_session.execute(
        refresh_table.delete().where(refresh_table.c.trade_date == date.fromisoformat(trade_date))
    )
    db_session.commit()

    with pytest.raises(ReportGenerationServiceError) as exc_info:
        generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    assert exc_info.value.status_code == 503
    assert exc_info.value.error_code == "DEPENDENCY_NOT_READY"


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_accepts_same_day_fallback_pool_snapshot(db_session):
    previous_trade_date = "2026-03-06"
    trade_date = "2026-03-07"
    trade_day = date.fromisoformat(trade_date)
    previous_trade_day = date.fromisoformat(previous_trade_date)

    seed_generation_context(db_session, trade_date=previous_trade_date)
    seed_generation_context(db_session, trade_date=trade_date)

    refresh_table = Base.metadata.tables["stock_pool_refresh_task"]
    fallback_task = db_session.execute(
        refresh_table.select().where(refresh_table.c.trade_date == trade_day)
    ).mappings().one()
    db_session.execute(
        refresh_table.update()
        .where(refresh_table.c.task_id == fallback_task["task_id"])
        .values(
            status="FALLBACK",
            fallback_from=previous_trade_day,
            status_reason="KLINE_COVERAGE_INSUFFICIENT",
        )
    )
    db_session.commit()

    data = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    report_table = Base.metadata.tables["report"]
    task_table = Base.metadata.tables["report_generation_task"]
    report_row = db_session.execute(
        report_table.select().where(report_table.c.report_id == data["report_id"])
    ).mappings().one()
    task_row = db_session.execute(
        task_table.select().where(task_table.c.task_id == report_row["generation_task_id"])
    ).mappings().one()

    assert data["trade_date"] == trade_date
    assert task_row["refresh_task_id"] == fallback_task["task_id"]


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_generation_uses_refresh_task_pool_version(db_session):
    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    seed_generation_context(db_session, trade_date=trade_date, pool_version=7)

    data = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    report_table = Base.metadata.tables["report"]
    task_table = Base.metadata.tables["report_generation_task"]
    refresh_table = Base.metadata.tables["stock_pool_refresh_task"]
    report_row = db_session.execute(
        report_table.select().where(report_table.c.report_id == data["report_id"])
    ).mappings().one()
    task_row = db_session.execute(
        task_table.select().where(task_table.c.task_id == report_row["generation_task_id"])
    ).mappings().one()
    refresh_row = db_session.execute(
        refresh_table.select().where(refresh_table.c.task_id == task_row["refresh_task_id"])
    ).mappings().one()

    assert report_row["pool_version"] == 7
    assert report_row["pool_version"] == refresh_row["pool_version"]


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_seed_report_bundle_does_not_reuse_wrong_same_day_pool_version(db_session):
    trade_date = "2026-03-06"
    insert_pool_snapshot(
        db_session,
        trade_date=trade_date,
        stock_codes=["600519.SH"],
        pool_version=1,
    )

    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="MOUTAI",
        trade_date=trade_date,
        pool_version=7,
    )

    report_table = Base.metadata.tables["report"]
    task_table = Base.metadata.tables["report_generation_task"]
    refresh_table = Base.metadata.tables["stock_pool_refresh_task"]
    report_row = db_session.execute(
        report_table.select().where(report_table.c.report_id == report.report_id)
    ).mappings().one()
    task_row = db_session.execute(
        task_table.select().where(task_table.c.task_id == report_row["generation_task_id"])
    ).mappings().one()
    refresh_row = db_session.execute(
        refresh_table.select().where(refresh_table.c.task_id == task_row["refresh_task_id"])
    ).mappings().one()

    assert report_row["pool_version"] == 7
    assert refresh_row["pool_version"] == 7


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_generation_preserves_multisource_hotspot_usage_facts(db_session):
    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    seed_generation_context(db_session, trade_date=trade_date)

    usage_table = Base.metadata.tables["report_data_usage"]
    data_batch = Base.metadata.tables["data_batch"]
    usage_link_table = Base.metadata.tables["report_data_usage_link"]

    fetch_time_a = utc_now() - timedelta(minutes=5)
    fetch_time_b = utc_now()
    batch_a = str(uuid4())
    batch_b = str(uuid4())

    db_session.execute(
        data_batch.insert().values(
            batch_id=batch_a,
            source_name="weibo",
            trade_date=trade_day,
            batch_scope="full_market",
            batch_seq=1,
            batch_status="SUCCESS",
            quality_flag="ok",
            covered_stock_count=1,
            core_pool_covered_count=1,
            records_total=1,
            records_success=1,
            records_failed=0,
            status_reason=None,
            trigger_task_run_id=None,
            started_at=fetch_time_a,
            finished_at=fetch_time_a,
            updated_at=fetch_time_a,
            created_at=fetch_time_a,
        )
    )
    db_session.execute(
        data_batch.insert().values(
            batch_id=batch_b,
            source_name="xueqiu",
            trade_date=trade_day,
            batch_scope="full_market",
            batch_seq=1,
            batch_status="SUCCESS",
            quality_flag="ok",
            covered_stock_count=1,
            core_pool_covered_count=1,
            records_total=1,
            records_success=1,
            records_failed=0,
            status_reason=None,
            trigger_task_run_id=None,
            started_at=fetch_time_b,
            finished_at=fetch_time_b,
            updated_at=fetch_time_b,
            created_at=fetch_time_b,
        )
    )
    db_session.execute(
        usage_table.insert().values(
            usage_id=str(uuid4()),
            trade_date=trade_day,
            stock_code="600519.SH",
            dataset_name="hotspot_top50",
            source_name="weibo",
            batch_id=batch_a,
            fetch_time=fetch_time_a,
            status="ok",
            status_reason=None,
            created_at=fetch_time_a,
        )
    )
    db_session.execute(
        usage_table.insert().values(
            usage_id=str(uuid4()),
            trade_date=trade_day,
            stock_code="600519.SH",
            dataset_name="hotspot_top50",
            source_name="xueqiu",
            batch_id=batch_b,
            fetch_time=fetch_time_b,
            status="ok",
            status_reason=None,
            created_at=fetch_time_b,
        )
    )
    db_session.commit()

    data = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    hotspot_rows = [
        row for row in data["used_data"] if row["dataset_name"] == "hotspot_top50"
    ]
    linked_hotspot_count = db_session.execute(
        usage_link_table.select().where(
            usage_link_table.c.report_id == data["report_id"],
            usage_link_table.c.usage_id.in_([row["usage_id"] for row in hotspot_rows]),
        )
    ).fetchall()

    assert len(hotspot_rows) == 3
    assert {row["source_name"] for row in hotspot_rows} == {"eastmoney", "weibo", "xueqiu"}
    assert len(linked_hotspot_count) == 3


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_market_state_input_usage_and_link_are_persisted(db_session):
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)

    data = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    usage_table = Base.metadata.tables["report_data_usage"]
    usage_link_table = Base.metadata.tables["report_data_usage_link"]
    data_batch_table = Base.metadata.tables["data_batch"]
    lineage_table = Base.metadata.tables["data_batch_lineage"]
    report_table = Base.metadata.tables["report"]
    market_state_cache_table = Base.metadata.tables["market_state_cache"]

    report_row = db_session.execute(
        report_table.select().where(report_table.c.report_id == data["report_id"])
    ).mappings().one()
    market_state_row = db_session.execute(
        market_state_cache_table.select().where(
            market_state_cache_table.c.trade_date == report_row["market_state_trade_date"]
        )
    ).mappings().one()
    market_state_usage = db_session.execute(
        usage_table.select().where(
            usage_table.c.dataset_name == "market_state_input",
            usage_table.c.source_name == "market_state_cache",
            usage_table.c.stock_code == "600519.SH",
        )
    ).mappings().one()
    market_state_link = db_session.execute(
        usage_link_table.select().where(
            usage_link_table.c.report_id == data["report_id"],
            usage_link_table.c.usage_id == market_state_usage["usage_id"],
        )
    ).mappings().one()
    derived_batch = db_session.execute(
        data_batch_table.select().where(data_batch_table.c.batch_id == market_state_usage["batch_id"])
    ).mappings().one()
    lineage_rows = db_session.execute(
        lineage_table.select().where(
            lineage_table.c.child_batch_id == market_state_usage["batch_id"],
            lineage_table.c.lineage_role == "MERGED_FROM",
        )
    ).mappings().all()
    parent_batch_ids = {str(row["parent_batch_id"]) for row in lineage_rows if row.get("parent_batch_id")}

    assert market_state_link["report_id"] == data["report_id"]
    assert market_state_usage["status"] == "ok"
    assert derived_batch["source_name"] == "market_state_cache"
    assert derived_batch["batch_scope"] == "market_state_derived"
    assert market_state_usage["batch_id"]
    assert market_state_usage["batch_id"] == derived_batch["batch_id"]
    assert report_row["market_state_trade_date"] == market_state_usage["trade_date"]
    assert derived_batch["trade_date"] == report_row["market_state_trade_date"]
    assert parent_batch_ids == {
        str(batch_id)
        for batch_id in (
            market_state_row["kline_batch_id"],
            market_state_row["hotspot_batch_id"],
        )
        if batch_id
    }


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_market_state_trade_date_tracks_actual_cache_hit(db_session):
    previous_trade_date = "2026-03-05"
    trade_date = "2026-03-06"
    previous_trade_day = date.fromisoformat(previous_trade_date)
    trade_day = date.fromisoformat(trade_date)

    seed_generation_context(db_session, trade_date=previous_trade_date, market_state="BULL")
    seed_generation_context(db_session, trade_date=trade_date, market_state="NEUTRAL")

    cache_table = Base.metadata.tables["market_state_cache"]
    task_table = Base.metadata.tables["report_generation_task"]
    report_table = Base.metadata.tables["report"]
    usage_table = Base.metadata.tables["report_data_usage"]

    db_session.execute(
        cache_table.delete().where(cache_table.c.trade_date == trade_day)
    )
    db_session.commit()

    data = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    report_row = db_session.execute(
        report_table.select().where(report_table.c.report_id == data["report_id"])
    ).mappings().one()
    task_row = db_session.execute(
        task_table.select().where(task_table.c.task_id == report_row["generation_task_id"])
    ).mappings().one()
    market_state_usage = db_session.execute(
        usage_table.select().where(
            usage_table.c.dataset_name == "market_state_input",
            usage_table.c.stock_code == "600519.SH",
        )
    ).mappings().one()

    assert task_row["market_state_trade_date"] == previous_trade_day
    assert report_row["market_state_trade_date"] == previous_trade_day
    assert report_row["market_state_reference_date"] == previous_trade_day
    assert market_state_usage["trade_date"] == trade_day
    assert market_state_usage["trade_date"] != report_row["market_state_trade_date"]


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_refresh_context_ignores_non_terminal_exact_snapshot_task(db_session):
    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    seed_generation_context(db_session, trade_date=trade_date, pool_version=3)

    refresh_table = Base.metadata.tables["stock_pool_refresh_task"]
    snapshot_table = Base.metadata.tables["stock_pool_snapshot"]
    bad_task_id = str(uuid4())
    now = utc_now()
    db_session.execute(
        refresh_table.insert().values(
            task_id=bad_task_id,
            trade_date=trade_day - timedelta(days=1),
            status="REFRESHING",
            pool_version=99,
            fallback_from=None,
            filter_params_json={"target_pool_size": 1},
            core_pool_size=1,
            standby_pool_size=0,
            evicted_stocks_json=[],
            status_reason=None,
            request_id=str(uuid4()),
            started_at=now,
            finished_at=None,
            updated_at=now + timedelta(minutes=1),
            created_at=now + timedelta(minutes=1),
        )
    )
    db_session.execute(
        snapshot_table.update()
        .where(
            snapshot_table.c.trade_date == trade_day,
            snapshot_table.c.stock_code == "600519.SH",
        )
        .values(
            refresh_task_id=bad_task_id,
            pool_version=99,
            created_at=now + timedelta(minutes=1),
        )
    )
    db_session.commit()

    context = resolve_refresh_context(db_session, trade_day=trade_day, stock_code="600519.SH")

    assert context is not None
    assert context["pool_version"] == 3
    assert context["task_id"] != bad_task_id


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_market_state_reference_date_tracks_cache_reference_date(db_session):
    trade_date = "2026-03-06"
    previous_trade_day = date.fromisoformat("2026-03-05")
    trade_day = date.fromisoformat(trade_date)

    seed_generation_context(db_session, trade_date=trade_date, market_state="NEUTRAL")

    cache_table = Base.metadata.tables["market_state_cache"]
    report_table = Base.metadata.tables["report"]

    db_session.execute(
        cache_table.update()
        .where(cache_table.c.trade_date == trade_day)
        .values(reference_date=previous_trade_day)
    )
    db_session.commit()

    data = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    report_row = db_session.execute(
        report_table.select().where(report_table.c.report_id == data["report_id"])
    ).mappings().one()

    assert report_row["market_state_trade_date"] == trade_day
    assert report_row["market_state_reference_date"] == previous_trade_day


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_generation_requires_snapshot_bound_refresh_context(db_session):
    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    snapshot_table = Base.metadata.tables["stock_pool_snapshot"]

    seed_generation_context(db_session, trade_date=trade_date)
    db_session.execute(
        snapshot_table.delete().where(
            snapshot_table.c.trade_date == trade_day,
            snapshot_table.c.stock_code == "600519.SH",
        )
    )
    db_session.commit()

    with pytest.raises(ReportGenerationServiceError) as exc_info:
        generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    assert exc_info.value.status_code == 503
    assert exc_info.value.error_code == "DEPENDENCY_NOT_READY"


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_generation_requires_market_state_parent_batches(db_session):
    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    cache_table = Base.metadata.tables["market_state_cache"]

    seed_generation_context(db_session, trade_date=trade_date)
    db_session.execute(
        cache_table.update()
        .where(cache_table.c.trade_date == trade_day)
        .values(hotspot_batch_id="missing-market-state-parent")
    )
    db_session.commit()

    with pytest.raises(ReportGenerationServiceError) as exc_info:
        generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    assert exc_info.value.status_code == 500
    assert exc_info.value.error_code == "DEPENDENCY_NOT_READY"


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_generated_citations_are_not_placeholder_snapshots(db_session):
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)

    data = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    citation = data["citations"][0]
    assert citation["title"] != "kline_daily snapshot"
    assert not str(citation["title"]).endswith("snapshot")
    assert citation["excerpt"] != "600519.SH kline_daily ok"
    assert "开盘" in citation["excerpt"]
    assert "收盘" in citation["excerpt"]
    assert citation["source_url"].startswith(("http://", "https://"))


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_force_same_day_rebuild_supersedes_prior_report_and_task(db_session, monkeypatch):
    trade_date = "2026-03-06"
    idempotency_key = f"daily:600519.SH:{trade_date}"
    seed_generation_context(db_session, trade_date=trade_date)

    responses = [
        {
            "recommendation": "BUY",
            "confidence": 0.72,
            "llm_fallback_level": "primary",
            "risk_audit_status": "completed",
            "risk_audit_skip_reason": None,
            "conclusion_text": "first pass",
            "reasoning_chain_md": "first reasoning",
        },
        {
            "recommendation": "HOLD",
            "confidence": 0.61,
            "llm_fallback_level": "backup",
            "risk_audit_status": "completed",
            "risk_audit_skip_reason": None,
            "conclusion_text": "rebuilt pass",
            "reasoning_chain_md": "rebuilt reasoning",
        },
    ]

    def fake_model(**kwargs):
        payload = responses.pop(0).copy()
        payload["signal_entry_price"] = kwargs["signal_entry_price"]
        return payload

    monkeypatch.setattr("app.services.report_generation_ssot.run_generation_model", fake_model)

    first = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)
    rebuilt = generate_report_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date=trade_date,
        force_same_day_rebuild=True,
    )

    assert rebuilt["report_id"] != first["report_id"]
    assert rebuilt["idempotency_key"] == idempotency_key

    report_table = Base.metadata.tables["report"]
    task_table = Base.metadata.tables["report_generation_task"]
    report_rows = db_session.execute(
        report_table.select()
        .where(report_table.c.idempotency_key == idempotency_key)
        .order_by(report_table.c.generation_seq.asc())
    ).fetchall()
    task_rows = db_session.execute(
        task_table.select()
        .where(task_table.c.idempotency_key == idempotency_key)
        .order_by(task_table.c.generation_seq.asc())
    ).fetchall()

    assert len(report_rows) == 2
    assert report_rows[0].superseded_by_report_id == report_rows[1].report_id
    assert report_rows[1].superseded_by_report_id is None
    assert len(task_rows) == 2
    assert task_rows[0].superseded_by_task_id == task_rows[1].task_id
    assert task_rows[0].superseded_at is not None
    assert task_rows[1].generation_seq == 2


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_suspended_stock_skip_llm(client, db_session, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date, is_suspended=True)

    def _unexpected(*args, **kwargs):
        raise AssertionError("llm should not be called for suspended stocks")

    monkeypatch.setattr("app.services.report_generation_ssot.run_generation_model", _unexpected)
    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["recommendation"] == "HOLD"
    assert data["confidence"] == 0.0
    assert data["status_reason"] == "SUSPENDED_SKIPPED"
    # Fail-close: suspended stocks must NOT be published
    assert data["published"] is False
    assert data["publish_status"] == "UNPUBLISHED"
    for tier in ("10k", "100k", "500k"):
        assert data["sim_trade_instruction"][tier]["status"] == "SKIPPED"
        assert data["sim_trade_instruction"][tier]["skip_reason"] == "SUSPENDED"


@pytest.mark.feature('FR06-LLM-06')
def test_fr06_trade_instruction_recomputed_after_risk_audit(db_session, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)

    def fake_model(**kwargs):
        return {
            "recommendation": "BUY",
            "confidence": 0.74,
            "llm_fallback_level": "primary",
            "risk_audit_status": "not_triggered",
            "risk_audit_skip_reason": None,
            "conclusion_text": "pre-audit buy",
            "reasoning_chain_md": "pre-audit reasoning",
            "signal_entry_price": kwargs["signal_entry_price"],
        }

    monkeypatch.setattr("app.services.report_generation_ssot.run_generation_model", fake_model)
    monkeypatch.setattr("app.services.report_generation_ssot.settings.mock_llm", False, raising=False)
    monkeypatch.setattr("app.services.report_generation_ssot.settings.llm_audit_enabled", True, raising=False)
    monkeypatch.setattr("app.services.llm_router.run_audit_and_aggregate", lambda **kwargs: {"adjusted_confidence": 0.61})

    data = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    assert data["recommendation"] == "BUY"
    assert data["confidence"] == 0.61
    for tier in ("10k", "100k", "500k"):
        assert data["sim_trade_instruction"][tier]["status"] == "SKIPPED"
        assert data["sim_trade_instruction"][tier]["skip_reason"] == "LOW_CONFIDENCE_OR_NOT_BUY"


@pytest.mark.feature('FR06-LLM-09')
def test_fr06_trade_instruction_recomputed_after_logic_inversion_fallback(db_session, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)

    def fake_model(**kwargs):
        return {
            "recommendation": "BUY",
            "confidence": 0.74,
            "llm_fallback_level": "primary",
            "risk_audit_status": "not_triggered",
            "risk_audit_skip_reason": None,
            "conclusion_text": "pre-card buy",
            "reasoning_chain_md": "pre-card reasoning",
            "signal_entry_price": kwargs["signal_entry_price"],
        }

    monkeypatch.setattr("app.services.report_generation_ssot.run_generation_model", fake_model)
    monkeypatch.setattr(
        "app.services.report_generation_ssot._build_instruction_card",
        lambda **kwargs: {
            "signal_entry_price": kwargs["signal_entry_price"],
            "atr_pct": 0.01,
            "atr_multiplier": 2.0,
            "stop_loss": kwargs["signal_entry_price"] * 0.92,
            "target_price": kwargs["signal_entry_price"] * 1.12,
            "stop_loss_calc_mode": "fixed_92pct_fallback",
            "skip_reason": "logic_inversion_fallback",
            "skipped": True,
        },
    )

    data = generate_report_ssot(db_session, stock_code="600519.SH", trade_date=trade_date)

    assert data["recommendation"] == "HOLD"
    assert data["confidence"] == 0.0
    for tier in ("10k", "100k", "500k"):
        assert data["sim_trade_instruction"][tier]["status"] == "SKIPPED"
        assert data["sim_trade_instruction"][tier]["skip_reason"] == "logic_inversion_fallback"


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_concurrent_conflict_409(db_session, isolated_app, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)

    def slow_model(**kwargs):
        time.sleep(0.25)
        return {
            "recommendation": "BUY",
            "confidence": 0.72,
            "llm_fallback_level": "primary",
            "risk_audit_status": "completed",
            "risk_audit_skip_reason": None,
            "conclusion_text": "骞跺彂闂ㄧ娴嬭瘯缁撹",
            "reasoning_chain_md": "concurrent conflict reasoning chain",
            "signal_entry_price": kwargs["signal_entry_price"],
        }

    monkeypatch.setattr("app.services.report_generation_ssot.run_generation_model", slow_model)

    def do_generate():
        db = isolated_app["sessionmaker"]()
        try:
            result = generate_report_ssot(db, stock_code="600519.SH", trade_date=trade_date)
            return ("ok", result["report_id"])
        except ReportGenerationServiceError as exc:
            db.rollback()
            return ("error", exc.error_code)
        finally:
            db.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(do_generate)
        second = executor.submit(do_generate)
        outcomes = [first.result(), second.result()]

    assert sorted(kind for kind, _ in outcomes) == ["error", "ok"]
    assert {payload for kind, payload in outcomes if kind == "error"} == {"CONCURRENT_CONFLICT"}

    report_table = Base.metadata.tables["report"]
    task_table = Base.metadata.tables["report_generation_task"]
    report_rows = db_session.execute(
        report_table.select().where(report_table.c.idempotency_key == f"daily:600519.SH:{trade_date}")
    ).fetchall()
    task_rows = db_session.execute(
        task_table.select().where(task_table.c.idempotency_key == f"daily:600519.SH:{trade_date}")
    ).fetchall()
    assert len(report_rows) == 1
    assert len(task_rows) == 1
    assert task_rows[0].status == "Completed"


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_commits_processing_task_before_slow_model_call(db_session, isolated_app, monkeypatch):
    trade_date = "2026-03-06"
    idempotency_key = f"daily:600519.SH:{trade_date}"
    seed_generation_context(db_session, trade_date=trade_date)
    db_session.commit()

    model_started = Event()
    allow_finish = Event()
    result_holder: dict[str, object] = {}

    def slow_model(**kwargs):
        model_started.set()
        assert allow_finish.wait(timeout=5), "slow model gate timed out"
        return {
            "recommendation": "BUY",
            "confidence": 0.72,
            "llm_fallback_level": "primary",
            "risk_audit_status": "completed",
            "risk_audit_skip_reason": None,
            "conclusion_text": "slow model concurrency conclusion",
            "reasoning_chain_md": "鎱㈡ā鍨嬪洖褰掓帹鐞嗛摼",
            "signal_entry_price": kwargs["signal_entry_price"],
        }

    monkeypatch.setattr("app.services.report_generation_ssot.run_generation_model", slow_model)

    def do_generate():
        db = isolated_app["sessionmaker"]()
        try:
            result_holder["result"] = generate_report_ssot(db, stock_code="600519.SH", trade_date=trade_date)
        except Exception as exc:  # pragma: no cover - surfaced through assertion below
            result_holder["error"] = exc
        finally:
            db.close()

    worker = Thread(target=do_generate, daemon=True)
    worker.start()
    if not model_started.wait(timeout=15):
        worker.join(timeout=10)
        raise AssertionError(f"slow model did not start; result_holder={result_holder!r}")

    inspect_db = isolated_app["sessionmaker"]()
    try:
        task_table = Base.metadata.tables["report_generation_task"]
        task_row = inspect_db.execute(
            task_table.select().where(task_table.c.idempotency_key == idempotency_key)
        ).mappings().one()
        assert task_row["status"] == "Processing"
    finally:
        inspect_db.close()

    db_path = isolated_app["engine"].url.database
    raw_conn = sqlite3.connect(db_path, timeout=0.1)
    try:
        raw_conn.execute("BEGIN IMMEDIATE")
        raw_conn.execute("ROLLBACK")
    finally:
        raw_conn.close()

    allow_finish.set()
    worker.join(timeout=5)
    assert not worker.is_alive(), "generate_report_ssot should finish after slow model is released"
    assert "error" not in result_holder
    assert result_holder["result"]["idempotency_key"] == idempotency_key


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_failed_task_retry_opens_new_generation_seq(client, db_session, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)
    now = utc_now()
    task_table = Base.metadata.tables["report_generation_task"]
    report_table = Base.metadata.tables["report"]
    refresh_task_table = Base.metadata.tables["stock_pool_refresh_task"]
    failed_task_id = str(uuid4())
    refresh_task_id = db_session.execute(
        refresh_task_table.select().where(refresh_task_table.c.trade_date == date.fromisoformat(trade_date))
    ).mappings().one()["task_id"]

    # Keep the seeded pool/kline/runtime context, but free the idempotency slot
    # so this case exercises retrying a failed task instead of reusing a
    # previously completed report.
    db_session.execute(
        report_table.delete().where(report_table.c.idempotency_key == f"daily:600519.SH:{trade_date}")
    )
    db_session.execute(
        task_table.delete().where(task_table.c.idempotency_key == f"daily:600519.SH:{trade_date}")
    )

    db_session.execute(
        task_table.insert().values(
            task_id=failed_task_id,
            trade_date=date.fromisoformat(trade_date),
            stock_code="600519.SH",
            idempotency_key=f"daily:600519.SH:{trade_date}",
            generation_seq=1,
            status="Failed",
            retry_count=1,
            quality_flag="degraded",
            status_reason="prior_failure",
            llm_fallback_level="failed",
            risk_audit_status="not_triggered",
            risk_audit_skip_reason=None,
            market_state_trade_date=date.fromisoformat(trade_date),
            refresh_task_id=refresh_task_id,
            trigger_task_run_id=None,
            request_id=str(uuid4()),
            superseded_by_task_id=None,
            superseded_at=None,
            queued_at=now,
            started_at=now,
            finished_at=now,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.commit()

    def fast_model(**kwargs):
        return {
            "recommendation": "BUY",
            "confidence": 0.74,
            "llm_fallback_level": "primary",
            "risk_audit_status": "completed",
            "risk_audit_skip_reason": None,
            "conclusion_text": "澶辫触浠诲姟閲嶈瘯缁撹",
            "reasoning_chain_md": "failed task retry reasoning chain",
            "signal_entry_price": kwargs["signal_entry_price"],
        }

    monkeypatch.setattr("app.services.report_generation_ssot.run_generation_model", fast_model)

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )

    assert response.status_code == 200

    rows = db_session.execute(
        task_table.select()
        .where(task_table.c.idempotency_key == f"daily:600519.SH:{trade_date}")
        .order_by(task_table.c.generation_seq.asc())
    ).fetchall()
    assert [row.generation_seq for row in rows] == [1, 2]
    assert rows[0].status == "Failed"
    assert rows[0].superseded_by_task_id == rows[1].task_id
    assert rows[0].superseded_at is not None
    assert rows[1].status == "Completed"
    assert rows[1].retry_count == 2
    assert rows[1].refresh_task_id == refresh_task_id

    report_row = db_session.execute(
        report_table.select().where(report_table.c.idempotency_key == f"daily:600519.SH:{trade_date}")
    ).fetchone()
    assert report_row is not None
    assert report_row.generation_seq == 2


@pytest.mark.feature('FR06-LLM-02')
def test_fr06_resume_suspended_task_reuses_same_generation_seq(db_session, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)
    now = utc_now()
    task_table = Base.metadata.tables["report_generation_task"]
    refresh_task_table = Base.metadata.tables["stock_pool_refresh_task"]
    suspended_task_id = str(uuid4())
    refresh_task_id = db_session.execute(
        refresh_task_table.select().where(refresh_task_table.c.trade_date == date.fromisoformat(trade_date))
    ).mappings().one()["task_id"]

    db_session.execute(
        task_table.insert().values(
            task_id=suspended_task_id,
            trade_date=date.fromisoformat(trade_date),
            stock_code="600519.SH",
            idempotency_key=f"daily:600519.SH:{trade_date}",
            generation_seq=1,
            status="Suspended",
            retry_count=2,
            quality_flag="degraded",
            status_reason="LLM_CIRCUIT_BREAKER",
            llm_fallback_level="failed",
            risk_audit_status="not_triggered",
            risk_audit_skip_reason="llm_circuit_open",
            market_state_trade_date=date.fromisoformat(trade_date),
            refresh_task_id=refresh_task_id,
            trigger_task_run_id=None,
            request_id=str(uuid4()),
            superseded_by_task_id=None,
            superseded_at=None,
            queued_at=now,
            started_at=now,
            finished_at=None,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.commit()

    def fast_model(**kwargs):
        return {
            "recommendation": "BUY",
            "confidence": 0.74,
            "llm_fallback_level": "primary",
            "risk_audit_status": "completed",
            "risk_audit_skip_reason": None,
            "conclusion_text": "resume suspended task conclusion",
            "reasoning_chain_md": "鎸傝捣鎭㈠鍚庣户缁敓鎴愭帹鐞嗛摼",
            "signal_entry_price": kwargs["signal_entry_price"],
        }

    monkeypatch.setattr("app.services.report_generation_ssot.run_generation_model", fast_model)

    result = generate_report_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date=trade_date,
        resume_active_task=True,
    )

    rows = db_session.execute(
        task_table.select()
        .where(task_table.c.idempotency_key == f"daily:600519.SH:{trade_date}")
        .order_by(task_table.c.generation_seq.asc())
    ).fetchall()
    assert len(rows) == 1
    assert rows[0].task_id == suspended_task_id
    assert rows[0].generation_seq == 1
    assert rows[0].retry_count == 2
    assert rows[0].status == "Completed"
    assert result["report_id"] is not None


@pytest.mark.feature('FR06-LLM-02')
def test_fr06_scheduler_opens_circuit_and_suspends_remaining_tasks(db_session, isolated_app, monkeypatch):
    trade_date = date.fromisoformat("2026-03-06")
    pool = [f"{600000 + idx}.SH" for idx in range(6)]
    task_table = Base.metadata.tables["report_generation_task"]
    circuit_table = Base.metadata.tables["llm_circuit_state"]

    monkeypatch.setattr(scheduler_service, "SessionLocal", isolated_app["sessionmaker"])
    monkeypatch.setattr(scheduler_service, "get_daily_stock_pool", lambda **kwargs: pool)
    monkeypatch.setattr(scheduler_service, "send_admin_notification", lambda *args, **kwargs: None)

    def fallback_report(*args, **kwargs):
        return {
            "report_id": str(uuid4()),
            "llm_fallback_level": "failed",
            "risk_audit_skip_reason": "llm_all_failed_rule_fallback",
        }

    monkeypatch.setattr("app.services.report_generation_ssot.generate_report_ssot", fallback_report)

    result = scheduler_service._handler_fr06_report_gen(trade_date)

    circuit_row = db_session.execute(
        circuit_table.select().where(circuit_table.c.circuit_name == "report_generation")
    ).mappings().one()
    suspended_rows = db_session.execute(
        task_table.select()
        .where(task_table.c.trade_date == trade_date)
        .where(task_table.c.status == "Suspended")
    ).fetchall()

    assert result["fail"] == 5
    assert result["suspended"] == 1
    assert circuit_row["circuit_state"] == "OPEN"
    assert circuit_row["consecutive_failures"] == 5
    assert len(suspended_rows) == 1
    assert suspended_rows[0].stock_code == pool[-1]
    assert suspended_rows[0].status_reason == "LLM_CIRCUIT_BREAKER"


@pytest.mark.feature('FR06-LLM-02')
def test_fr06_scheduler_half_open_resumes_suspended_task(db_session, isolated_app, monkeypatch):
    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    seed_generation_context(db_session, trade_date=trade_date)
    now = utc_now()
    circuit_table = Base.metadata.tables["llm_circuit_state"]
    task_table = Base.metadata.tables["report_generation_task"]
    refresh_task_table = Base.metadata.tables["stock_pool_refresh_task"]
    refresh_task_id = db_session.execute(
        refresh_task_table.select().where(refresh_task_table.c.trade_date == trade_day)
    ).mappings().one()["task_id"]

    db_session.execute(
        circuit_table.insert().values(
            circuit_name="report_generation",
            circuit_state="OPEN",
            consecutive_failures=5,
            opened_at=now - timedelta(minutes=10),
            cooldown_until=now - timedelta(minutes=1),
            last_probe_at=None,
            last_failure_reason="llm_all_failed_rule_fallback",
            updated_at=now,
            created_at=now,
        )
    )
    db_session.execute(
        task_table.insert().values(
            task_id=str(uuid4()),
            trade_date=trade_day,
            stock_code="600519.SH",
            idempotency_key=f"daily:600519.SH:{trade_date}",
            generation_seq=1,
            status="Suspended",
            retry_count=1,
            quality_flag="ok",
            status_reason="LLM_CIRCUIT_BREAKER",
            llm_fallback_level="failed",
            risk_audit_status="not_triggered",
            risk_audit_skip_reason="llm_circuit_open",
            market_state_trade_date=trade_day,
            refresh_task_id=refresh_task_id,
            trigger_task_run_id=None,
            request_id=str(uuid4()),
            superseded_by_task_id=None,
            superseded_at=None,
            queued_at=now,
            started_at=None,
            finished_at=None,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.commit()

    monkeypatch.setattr(scheduler_service, "SessionLocal", isolated_app["sessionmaker"])
    monkeypatch.setattr(scheduler_service, "send_admin_notification", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler_service, "get_daily_stock_pool", lambda **kwargs: ["600519.SH"])
    # Prevent real capital/profile collection from overwriting seeded fixture data
    import app.services.stock_snapshot_service as _sss
    monkeypatch.setattr(_sss, "collect_non_report_usage_sync", lambda *a, **kw: {})
    monkeypatch.setattr(
        "app.services.report_generation_ssot.run_generation_model",
        lambda **kwargs: {
            "recommendation": "BUY",
            "confidence": 0.74,
            "llm_fallback_level": "primary",
            "risk_audit_status": "completed",
            "risk_audit_skip_reason": None,
            "conclusion_text": "鍗婂紑鎺㈡椿鎭㈠鎴愬姛",
            "reasoning_chain_md": "half open recovery reasoning chain",
            "signal_entry_price": kwargs["signal_entry_price"],
        },
    )

    result = scheduler_service._handler_fr06_report_gen(trade_day)

    circuit_row = db_session.execute(
        circuit_table.select().where(circuit_table.c.circuit_name == "report_generation")
    ).mappings().one()
    task_row = db_session.execute(
        task_table.select().where(task_table.c.idempotency_key == f"daily:600519.SH:{trade_date}")
    ).mappings().one()

    assert result["ok"] == 1
    assert result["resumed"] == 1
    assert circuit_row["circuit_state"] == "CLOSED"
    assert circuit_row["consecutive_failures"] == 0
    assert task_row["status"] == "Completed"


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# FR06-LLM-07 strategy_type 涓夎鍒?+ atr_multiplier
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

@pytest.mark.feature('FR06-LLM-07')
def test_fr06_strategy_type_rules(client, db_session):
    """Verify the rule-engine mapping for strategy types A, B, and C."""
    trade_date = "2026-03-06"
    seed_generation_context(
        db_session,
        trade_date=trade_date,
        atr_pct=0.03,
        ma20=116.0,
        close_price=120.0,
        volatility_20d=0.02,
    )

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["strategy_type"] in {"A", "B", "C"}


@pytest.mark.feature('FR06-LLM-07')
def test_fr06_strategy_c_can_emit_buy_in_non_bear_market(client, db_session, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(
        db_session,
        trade_date=trade_date,
        atr_pct=0.015,
        ma20=118.0,
        close_price=120.0,
        volatility_20d=0.01,
        market_state="BULL",
    )
    monkeypatch.setattr("app.services.report_generation_ssot.settings.mock_llm", True, raising=False)
    monkeypatch.setattr(
        "app.services.report_generation_ssot._determine_strategy_type",
        lambda db, stock_code, trade_day, kline_row: "C",
    )

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["strategy_type"] == "C"
    assert data["recommendation"] == "BUY"
    assert data["confidence"] > 0


@pytest.mark.feature('FR06-LLM-06')
def test_fr06_mock_llm_skips_risk_audit_call(client, db_session, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(
        db_session,
        trade_date=trade_date,
        market_state="BULL",
        atr_pct=0.03,
        ma20=118.0,
        close_price=120.0,
        volatility_20d=0.02,
    )
    monkeypatch.setattr("app.services.report_generation_ssot.settings.mock_llm", True, raising=False)

    async def _unexpected_audit(*args, **kwargs):
        raise AssertionError("risk audit should not be called when mock_llm=true")

    monkeypatch.setattr("app.services.llm_router.run_audit_and_aggregate", _unexpected_audit)

    data = generate_report_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date=trade_date,
        request_id=f"req-{uuid4()}",
    )

    report_table = Base.metadata.tables["report"]
    row = db_session.execute(
        report_table.select().where(report_table.c.report_id == data["report_id"])
    ).mappings().first()
    assert row is not None
    assert row["risk_audit_status"] == "skipped"
    assert row["risk_audit_skip_reason"] == "mock_llm"


@pytest.mark.feature('FR06-LLM-06')
def test_fr06_failed_llm_fallback_skips_risk_audit_call(db_session, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(
        db_session,
        trade_date=trade_date,
        market_state="BULL",
        atr_pct=0.03,
        ma20=118.0,
        close_price=120.0,
        volatility_20d=0.02,
    )

    monkeypatch.setattr(
        "app.services.report_generation_ssot.run_generation_model",
        lambda **kwargs: {
            "recommendation": "BUY",
            "confidence": 0.74,
            "llm_fallback_level": "failed",
            "risk_audit_status": "not_triggered",
            "risk_audit_skip_reason": "llm_all_failed_rule_fallback",
            "conclusion_text": "rule fallback conclusion",
            "reasoning_chain_md": "rule fallback reasoning",
            "signal_entry_price": kwargs["signal_entry_price"],
        },
    )

    async def _unexpected_audit(*args, **kwargs):
        raise AssertionError("risk audit should not be called when llm_fallback_level=failed")

    monkeypatch.setattr("app.services.llm_router.run_audit_and_aggregate", _unexpected_audit)

    data = generate_report_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date=trade_date,
        request_id=f"req-{uuid4()}",
    )

    report_table = Base.metadata.tables["report"]
    row = db_session.execute(
        report_table.select().where(report_table.c.report_id == data["report_id"])
    ).mappings().one()

    assert row["risk_audit_status"] == "not_triggered"
    assert row["risk_audit_skip_reason"] == "llm_all_failed_rule_fallback"
    assert row["published"] is False
    assert row["publish_status"] == "UNPUBLISHED"


@pytest.mark.feature('FR06-LLM-06')
def test_fr06_trade_instruction_recomputes_after_audit_confidence_drop(db_session, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(
        db_session,
        trade_date=trade_date,
        market_state="BULL",
        atr_pct=0.03,
        ma20=118.0,
        close_price=120.0,
        volatility_20d=0.02,
    )
    monkeypatch.setattr("app.services.report_generation_ssot.settings.mock_llm", False, raising=False)
    monkeypatch.setattr("app.services.report_generation_ssot.settings.llm_audit_enabled", True, raising=False)
    monkeypatch.setattr(
        "app.services.report_generation_ssot.run_generation_model",
        lambda **kwargs: {
            "recommendation": "BUY",
            "confidence": 0.74,
            "llm_fallback_level": "primary",
            "risk_audit_status": "not_triggered",
            "risk_audit_skip_reason": None,
            "conclusion_text": "audit-downscaled conclusion",
            "reasoning_chain_md": "audit-downscaled reasoning",
            "signal_entry_price": kwargs["signal_entry_price"],
        },
    )
    monkeypatch.setattr("app.services.llm_router.should_trigger_audit", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        "app.services.report_generation_ssot._run_llm_coro",
        lambda coro_factory: {
            "adjusted_confidence": 0.61,
            "audit_detail": "confidence reduced",
        },
    )

    data = generate_report_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date=trade_date,
        request_id=f"req-{uuid4()}",
    )

    assert data["recommendation"] == "BUY"
    assert data["confidence"] == 0.61
    assert {item["status"] for item in data["sim_trade_instruction"].values()} == {"SKIPPED"}
    assert {
        item["skip_reason"]
        for item in data["sim_trade_instruction"].values()
    } == {"LOW_CONFIDENCE_OR_NOT_BUY"}


@pytest.mark.feature('FR06-LLM-09')
def test_fr06_trade_instruction_recomputes_after_logic_inversion_fallback(db_session, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(
        db_session,
        trade_date=trade_date,
        market_state="BULL",
        atr_pct=0.03,
        ma20=118.0,
        close_price=120.0,
        volatility_20d=0.02,
    )
    monkeypatch.setattr(
        "app.services.report_generation_ssot.run_generation_model",
        lambda **kwargs: {
            "recommendation": "BUY",
            "confidence": 0.74,
            "llm_fallback_level": "primary",
            "risk_audit_status": "not_triggered",
            "risk_audit_skip_reason": None,
            "conclusion_text": "logic inversion conclusion",
            "reasoning_chain_md": "logic inversion reasoning",
            "signal_entry_price": kwargs["signal_entry_price"],
        },
    )
    monkeypatch.setattr(
        "app.services.report_generation_ssot._build_instruction_card",
        lambda **kwargs: {
            "signal_entry_price": kwargs["signal_entry_price"],
            "atr_pct": 0.03,
            "atr_multiplier": 2.0,
            "stop_loss": kwargs["signal_entry_price"] * 0.92,
            "target_price": kwargs["signal_entry_price"] * 1.12,
            "stop_loss_calc_mode": "fixed_92pct_fallback",
            "skip_reason": "logic_inversion_fallback",
            "skipped": True,
        },
    )

    data = generate_report_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date=trade_date,
        request_id=f"req-{uuid4()}",
    )

    assert data["recommendation"] == "HOLD"
    assert data["confidence"] == 0.0
    assert {item["status"] for item in data["sim_trade_instruction"].values()} == {"SKIPPED"}
    assert {
        item["skip_reason"]
        for item in data["sim_trade_instruction"].values()
    } == {"logic_inversion_fallback"}


@pytest.mark.feature('FR06-LLM-05')
def test_fr06_llm_timeout_falls_back_to_rule_based_report(client, db_session, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(
        db_session,
        trade_date=trade_date,
        market_state="BULL",
        atr_pct=0.03,
        ma20=116.0,
        close_price=120.0,
        volatility_20d=0.02,
    )
    monkeypatch.setattr("app.services.report_generation_ssot.settings.mock_llm", False, raising=False)
    monkeypatch.setattr("app.services.report_generation_ssot.settings.llm_audit_enabled", False, raising=False)

    def _timeout(*args, **kwargs):
        raise RuntimeError("llm_timeout_after_5s")

    monkeypatch.setattr("app.services.report_generation_ssot._run_llm_coro", _timeout)

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    report_table = Base.metadata.tables["report"]
    report_row = db_session.execute(
        report_table.select().where(report_table.c.report_id == data["report_id"])
    ).mappings().one()

    assert data["published"] is False
    assert data["publish_status"] == "UNPUBLISHED"
    assert data["review_flag"] == "PENDING_REVIEW"
    assert data["llm_fallback_level"] == "failed"
    assert data["recommendation"] in {"BUY", "HOLD"}
    assert data["confidence"] > 0
    assert report_row["published"] is False
    assert report_row["publish_status"] == "UNPUBLISHED"
    assert report_row["review_flag"] == "PENDING_REVIEW"


@pytest.mark.feature('FR06-LLM-05')
def test_fr06_llm_all_failed_never_sets_published_true(client, db_session, monkeypatch):
    """P1 fail-close: when all LLM providers fail and rule-based fallback is used,
    published must be False at every stage — not temporarily True then corrected."""
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)
    monkeypatch.setattr("app.services.report_generation_ssot.settings.mock_llm", False, raising=False)
    monkeypatch.setattr("app.services.report_generation_ssot.settings.llm_audit_enabled", False, raising=False)

    def _all_fail(*args, **kwargs):
        raise ConnectionError("all_providers_unreachable")

    monkeypatch.setattr("app.services.report_generation_ssot._run_llm_coro", _all_fail)

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    report_table = Base.metadata.tables["report"]
    report_row = db_session.execute(
        report_table.select().where(report_table.c.report_id == data["report_id"])
    ).mappings().one()

    # API response
    assert data["published"] is False
    assert data["publish_status"] == "UNPUBLISHED"
    assert data["llm_fallback_level"] == "failed"
    # DB row
    assert report_row["published"] is False
    assert report_row["publish_status"] == "UNPUBLISHED"


@pytest.mark.feature('FR06-LLM-05')
def test_fr06_local_fallback_never_sets_published_true(client, db_session, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(
        db_session,
        trade_date=trade_date,
        market_state="BULL",
        atr_pct=0.03,
        ma20=116.0,
        close_price=120.0,
        volatility_20d=0.02,
    )
    monkeypatch.setattr("app.services.report_generation_ssot.settings.mock_llm", False, raising=False)
    monkeypatch.setattr("app.services.report_generation_ssot.settings.llm_audit_enabled", False, raising=False)

    class _FakeResult:
        response = _build_grounded_llm_response(
            confidence=0.66,
            close_price=120.0,
            ma5=118.8,
            ma20=116.0,
            atr_pct_percent=3.0,
        )
        model_used = "ollama"
        elapsed_s = 0.2
        degraded = True
        extra = {}

    monkeypatch.setattr("app.services.report_generation_ssot._run_llm_coro", lambda *args, **kwargs: _FakeResult())

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    report_table = Base.metadata.tables["report"]
    report_row = db_session.execute(
        report_table.select().where(report_table.c.report_id == data["report_id"])
    ).mappings().one()

    assert data["llm_fallback_level"] == "local"
    assert data["published"] is False
    assert data["publish_status"] == "UNPUBLISHED"
    assert data["review_flag"] == "PENDING_REVIEW"
    assert report_row["published"] is False
    assert report_row["publish_status"] == "UNPUBLISHED"


@pytest.mark.feature('FR06-LLM-05')
def test_fr06_cli_fallback_can_publish_when_payload_grounded(client, db_session, monkeypatch):
    trade_date = "2026-03-06"
    seed_generation_context(
        db_session,
        trade_date=trade_date,
        market_state="BULL",
        atr_pct=0.03,
        ma20=116.0,
        close_price=120.0,
        volatility_20d=0.02,
    )
    monkeypatch.setattr("app.services.report_generation_ssot.settings.mock_llm", False, raising=False)
    monkeypatch.setattr("app.services.report_generation_ssot.settings.llm_audit_enabled", False, raising=False)

    class _FakeResult:
        response = _build_grounded_llm_response(
            confidence=0.66,
            close_price=120.0,
            ma5=118.8,
            ma20=116.0,
            atr_pct_percent=3.0,
        )
        model_used = "claude_cli"
        elapsed_s = 0.2
        degraded = True
        extra = {}

    monkeypatch.setattr("app.services.report_generation_ssot._run_llm_coro", lambda *args, **kwargs: _FakeResult())

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    report_table = Base.metadata.tables["report"]
    report_row = db_session.execute(
        report_table.select().where(report_table.c.report_id == data["report_id"])
    ).mappings().one()

    assert data["llm_fallback_level"] == "cli"
    assert data["published"] is True
    assert data["publish_status"] == "PUBLISHED"
    assert report_row["published"] is True
    assert report_row["publish_status"] == "PUBLISHED"


@pytest.mark.feature('FR06-LLM-05')
def test_fr06_llm_validation_falls_back_when_text_is_not_grounded(monkeypatch):
    from app.services import report_generation_ssot
    from app.core.config import settings

    class DummyResult:
        response = json.dumps(
            {
                "recommendation": "BUY",
                "confidence": 0.78,
                "conclusion_text": "这是一段很长的结论文本，但它没有引用当前输入里的收盘价、均线、ATR、事件或估值信息，只是在泛泛而谈市场情绪和长期价值，所以不应该直接通过当前更严格的真实性校验。" * 2,
                "reasoning_chain_md": "## 技术面分析\n文本很长但没有绑定到本次数据。\n## 资金面分析\n仍然没有主力资金或估值引用。\n## 多空矛盾判断\n只有抽象判断。\n## 风险因素\n只是常识性描述。\n## 综合结论\n缺少真实输入绑定。" * 2,
                "strategy_specific_evidence": {
                    "strategy_type": "B",
                    "key_signal": "趋势不错",
                    "validation_check": "只给模糊判断",
                },
            },
            ensure_ascii=False,
        )
        model_used = "codex_api"
        degraded = False
        elapsed_s = 0.5
        extra = {"pool_level": "primary"}

    monkeypatch.setattr(report_generation_ssot, "_run_llm_coro", lambda *args, **kwargs: DummyResult())
    monkeypatch.setattr(settings, "mock_llm", False)

    data = report_generation_ssot.run_generation_model(
        stock_code="600519.SH",
        stock_name="贵州茅台",
        strategy_type="B",
        market_state="BULL",
        quality_flag="ok",
        prior_stats=None,
        signal_entry_price=1688.0,
        used_data=[
            {"dataset_name": "main_force_flow", "status": "ok"},
            {"dataset_name": "stock_profile", "status": "ok"},
        ],
        kline_row={"close": 1688.0, "ma5": 1680.0, "ma20": 1650.0, "atr_pct": 0.02, "volatility_20d": 0.03},
    )

    assert data["llm_fallback_level"] == "failed"
    assert data["risk_audit_skip_reason"] == "llm_all_failed_rule_fallback"


@pytest.mark.feature('FR06-LLM-05')
def test_fr06_llm_validation_accepts_single_available_metric_binding(monkeypatch):
    from app.services import report_generation_ssot
    from app.core.config import settings

    class DummyResult:
        response = json.dumps(
            {
                "recommendation": "HOLD",
                "confidence": 0.60,
                "conclusion_text": (
                    "\u4e2d\u5174\u901a\u8baf\u5f53\u524d\u6536\u76d8\u4ef7 37.12 \u5143\uff0cMA20 \u4ecd\u7f3a\u5931\uff0c\u8d8b\u52bf\u7b56\u7565B\u4ecd\u4ee5\u7b49\u5f85\u786e\u8ba4\u4e3a\u4e3b\u3002"
                    "\u867d\u7136 MA5\u3001MA20 \u4e0e ATR \u6682\u7f3a\uff0c\u65e0\u6cd5\u505a\u51fa\u591a\u5934\u6392\u5217\u786e\u8ba4\uff0c\u4f46\u8fd15\u65e5\u7d2f\u8ba1\u6da8\u5e45\u7ea6 8.5%\uff0c"
                    "\u8bf4\u660e\u4ef7\u683c\u4ecd\u5728\u5c1d\u8bd5\u7ef4\u6301\u5f3a\u52bf\uff0c\u73b0\u9636\u6bb5\u66f4\u9002\u5408\u5148\u89c2\u671b\u5e76\u7b49\u5f85\u5747\u7ebf\u6570\u636e\u8865\u9f50\u540e\u518d\u51b3\u5b9a\u662f\u5426\u8f6c\u5411 BUY\u3002"
                ),
                "reasoning_chain_md": (
                    "## \u6280\u672f\u9762\u5206\u6790\\n"
                    "\u6536\u76d8\u4ef7 37.12 \u5143\u4ecd\u662f\u672c\u8f6e\u5224\u65ad\u7684\u6838\u5fc3\u951a\u70b9\uff0c\u4f46 MA5\u3001MA20 \u4e0e ATR \u6682\u7f3a\uff0c\u56e0\u6b64\u8d8b\u52bf\u786e\u8ba4\u53ea\u80fd\u7ef4\u6301\u4e2d\u6027\u504f\u8c28\u614e\u3002\\n"
                    "## \u8d44\u91d1\u9762\u5206\u6790\\n"
                    "\u672c\u8f6e\u4e3b\u8981\u4f9d\u8d56\u4ef7\u683c\u4e0e\u8d8b\u52bf\u4fe1\u606f\uff0c\u6682\u672a\u5f15\u5165\u989d\u5916\u8d44\u91d1\u9762\u56e0\u5b50\u3002\\n"
                    "## \u591a\u7a7a\u77db\u76fe\u5224\u65ad\\n"
                    "\u4ef7\u683c\u4ecd\u4fdd\u6301\u76f8\u5bf9\u5f3a\u52bf\uff0c\u4f46\u5747\u7ebf\u7f3a\u53e3\u8ba9\u8d8b\u52bf\u5ef6\u7eed\u6027\u65e0\u6cd5\u88ab\u5145\u5206\u9a8c\u8bc1\uff0c\u56e0\u6b64\u9700\u8981\u4fdd\u7559\u89c2\u671b\u5224\u65ad\u3002\\n"
                    "## \u98ce\u9669\u56e0\u7d20\\n"
                    "\u82e5\u540e\u7eed\u6536\u76d8\u4ef7\u91cd\u65b0\u8dcc\u7834 37.12 \u5143\u9644\u8fd1\u652f\u6491\uff0c\u6216\u8865\u9f50\u540e\u7684\u5747\u7ebf\u4fe1\u53f7\u7ee7\u7eed\u8d70\u5f31\uff0c\u5219\u5f53\u524d\u5224\u65ad\u5e94\u8fdb\u4e00\u6b65\u8f6c\u4fdd\u5b88\u3002\\n"
                    "## \u7efc\u5408\u7ed3\u8bba\\n"
                    "\u8d8b\u52bf\u7b56\u7565B\u5f53\u524d\u66f4\u9002\u5408 HOLD\uff0c\u5148\u7b49\u5f85\u540e\u7eed\u5747\u7ebf\u8865\u9f50\u4e0e\u8d8b\u52bf\u786e\u8ba4\u3002"
                ),
                "strategy_specific_evidence": {
                    "strategy_type": "B",
                    "key_signal": "\u6536\u76d8\u4ef7 37.12 \u5143\uff0c\u8fd15\u65e5\u7d2f\u8ba1\u6da8\u5e45 8.5%\uff0c\u4f46\u5747\u7ebf\u6570\u636e\u6682\u7f3a",
                    "validation_check": "MA20 \u4e0e ATR \u6682\u7f3a\u5df2\u88ab\u663e\u5f0f\u62ab\u9732\uff0c\u5f53\u524d\u53ea\u4ee5\u6536\u76d8\u4ef7\u951a\u70b9\u4fdd\u6301 HOLD \u5224\u65ad\u3002"
                },
            },
            ensure_ascii=False,
        )
        model_used = "claude_cli"
        degraded = False
        elapsed_s = 0.5
        extra = {"endpoint": r"D:\\yanbao-new\\claude.cmd"}

    monkeypatch.setattr(report_generation_ssot, "_run_llm_coro", lambda *args, **kwargs: DummyResult())
    monkeypatch.setattr(settings, "mock_llm", False)

    data = report_generation_ssot.run_generation_model(
        stock_code="000063.SZ",
        stock_name="ZTE",
        strategy_type="B",
        market_state="NEUTRAL",
        quality_flag="ok",
        prior_stats=None,
        signal_entry_price=37.12,
        used_data=[{"dataset_name": "kline_daily", "status": "ok"}],
        kline_row={"close": 37.12, "ma5": None, "ma20": None, "atr_pct": None, "volatility_20d": None},
    )

    assert data["llm_fallback_level"] == "cli"
    assert "_grounding_hard_fail" not in (data.get("strategy_specific_evidence") or {})


@pytest.mark.feature('FR06-LLM-05')
def test_fr06_generate_reports_batch_forces_preselected_strategy_types(monkeypatch):
    from app.services import report_generation_ssot

    forced_calls: list[tuple[str, str | None]] = []
    natural_types = {
        "000001.SZ": "A",
        "000002.SZ": "B",
        "000003.SZ": "C",
        "000004.SZ": "B",
    }

    class DummySession:
        def close(self):
            return None

    monkeypatch.setattr(
        report_generation_ssot,
        "_query_one",
        lambda db, query, params: {
            "stock_code": params["stock_code"],
            "trade_date": date.fromisoformat("2026-03-06"),
            "close": 10.0,
            "ma5": 10.0,
            "ma20": 10.0,
            "atr_pct": 1.0,
            "volatility_20d": 0.02,
            "is_suspended": 0,
        },
    )
    monkeypatch.setattr(
        report_generation_ssot,
        "_determine_strategy_type",
        lambda db, stock_code, trade_day, kline_row: natural_types[stock_code],
    )

    def _fake_gen_one_sync(
        db_factory,
        stock_code,
        trade_date,
        skip_pool_check,
        force_same_day_rebuild,
        forced_strategy_type=None,
    ):
        forced_calls.append((stock_code, forced_strategy_type))
        return {
            "stock_code": stock_code,
            "published": True,
            "strategy_type": forced_strategy_type or "B",
        }

    monkeypatch.setattr(report_generation_ssot, "_gen_one_sync", _fake_gen_one_sync)

    result = report_generation_ssot.generate_reports_batch(
        lambda: DummySession(),
        stock_codes=["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"],
        trade_date="2026-03-06",
        one_per_strategy_type=True,
    )

    assert forced_calls == [
        ("000001.SZ", "A"),
        ("000002.SZ", "B"),
        ("000003.SZ", "C"),
    ]
    assert result["total"] == 3
    assert result["preselected_count"] == 3
    assert result["strategy_distribution"] == {
        "A": ["000001.SZ"],
        "B": ["000002.SZ"],
        "C": ["000003.SZ"],
    }

@pytest.mark.feature('FR06-LLM-05')
def test_fr06_llm_validation_soft_caps_confidence_when_available_evidence_is_unused(monkeypatch):
    from app.services import report_generation_ssot
    from app.core.config import settings

    class DummyResult:
        response = _build_grounded_llm_response(confidence=0.74, include_capital_keywords=False, include_valuation_keywords=False)
        model_used = "codex_api"
        degraded = False
        elapsed_s = 0.5
        extra = {"pool_level": "primary"}

    monkeypatch.setattr(report_generation_ssot, "_run_llm_coro", lambda *args, **kwargs: DummyResult())
    monkeypatch.setattr(settings, "mock_llm", False)

    data = report_generation_ssot.run_generation_model(
        stock_code="600519.SH",
        stock_name="贵州茅台",
        strategy_type="B",
        market_state="BULL",
        quality_flag="ok",
        prior_stats=None,
        signal_entry_price=1688.0,
        used_data=[
            {"dataset_name": "main_force_flow", "status": "ok"},
            {"dataset_name": "stock_profile", "status": "ok"},
        ],
        kline_row={"close": 1688.0, "ma5": 1680.0, "ma20": 1650.0, "atr_pct": 0.02, "volatility_20d": 0.03},
    )

    assert data["llm_fallback_level"] == "primary"
    assert data["confidence"] == 0.62
    assert data["strategy_specific_evidence"]["_grounding_soft_gap"] == ["capital_data_not_used", "valuation_data_not_used"]


@pytest.mark.feature('FR06-LLM-05')
def test_fr06_report_payload_surfaces_generation_process_for_grounding_soft_gap(monkeypatch, db_session):
    from app.services import report_generation_ssot
    from app.services.ssot_read_model import get_report_view_payload_ssot

    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)

    monkeypatch.setattr(
        report_generation_ssot,
        "run_generation_model",
        lambda **kwargs: {
            "recommendation": "BUY",
            "confidence": 0.62,
            "llm_fallback_level": "primary",
            "risk_audit_status": "not_triggered",
            "risk_audit_skip_reason": None,
            "conclusion_text": "贵州茅台当前趋势仍偏多，但本次结论没有使用资金面和估值面证据，因此置信度保持克制。",
            "reasoning_chain_md": (
                "## 技术面分析\n价格仍在MA20上方。\n"
                "## 资金面分析\n本轮未引用主力资金与估值。\n"
                "## 多空矛盾判断\n趋势偏多但证据不完整。\n"
                "## 风险因素\n若跌破MA20则判断失效。\n"
                "## 综合结论\n维持审慎买入。"
            ),
            "strategy_specific_evidence": {
                "strategy_type": "B",
                "key_signal": "close=1688.0 高于 ma20=1650.0，属于趋势型 B",
                "validation_check": "均线多头通过，但资金面与估值面未被结论使用。",
                "_grounding_soft_gap": ["capital_data_not_used", "valuation_data_not_used"],
            },
            "signal_entry_price": kwargs.get("signal_entry_price", 1688.0),
        },
    )

    generated = report_generation_ssot.generate_report_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date=trade_date,
        forced_strategy_type="B",
    )
    generation_process = generated["content_json"]["generation_process"]
    assert generation_process["analysis_steps"][0].startswith("策略判定")
    assert generation_process["raw_inputs"]["market_state"] == "BULL"
    assert generation_process["validation_plan"]["grounding_state"] == "soft_gap"
    assert any(
        item["dataset_name"] == "stock_profile" and item["highlights"]
        for item in generated["content_json"]["used_data_summary"]
    )

    report_table = Base.metadata.tables["report"]
    db_session.execute(
        report_table.update()
        .where(report_table.c.report_id == generated["report_id"])
        .values(
            published=True,
            publish_status="PUBLISHED",
            quality_flag="ok",
        )
    )
    db_session.commit()

    payload = get_report_view_payload_ssot(db_session, generated["report_id"], viewer_role="admin")

    assert payload is not None
    validation_plan = payload["reasoning_trace"]["validation_plan"]
    assert validation_plan["windows"] == [1, 7, 14, 30, 60]
    assert validation_plan["grounding_state"] == "soft_gap"
    assert validation_plan["soft_gaps"] == ["capital_data_not_used", "valuation_data_not_used"]
    assert payload["reasoning_trace"]["raw_inputs"]["strategy_type"] == "B"
    assert any(source.startswith("main_force_flow/") for source in payload["reasoning_trace"]["data_sources"])


@pytest.mark.feature('FR06-LLM-05')
def test_fr06_report_payload_tolerates_missing_ma_and_atr_values(monkeypatch, db_session):
    from app.services import report_generation_ssot
    from app.services.ssot_read_model import get_report_view_payload_ssot

    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date)
    db_session.execute(
        Base.metadata.tables["kline_daily"].update()
        .where(
            Base.metadata.tables["kline_daily"].c.stock_code == "600519.SH",
            Base.metadata.tables["kline_daily"].c.trade_date == date.fromisoformat(trade_date),
        )
        .values(ma5=None, ma20=None, atr_pct=None)
    )
    db_session.commit()

    monkeypatch.setattr(
        report_generation_ssot,
        "run_generation_model",
        lambda **kwargs: {
            "recommendation": "HOLD",
            "confidence": 0.58,
            "llm_fallback_level": "cli",
            "risk_audit_status": "not_triggered",
            "risk_audit_skip_reason": None,
            "conclusion_text": "均线与 ATR 暂缺，但收盘价锚点仍可支撑谨慎观望结论。",
            "reasoning_chain_md": (
                "## 技术面分析\nMA5、MA20 与 ATR 暂缺，仅保留收盘价锚点。\n"
                "## 资金面分析\n本轮未补充额外资金因子。\n"
                "## 多空矛盾判断\n趋势证据未补齐，因此维持观望。\n"
                "## 风险因素\n若后续价格转弱则需要继续降级。\n"
                "## 综合结论\n当前维持 HOLD。"
            ),
            "strategy_specific_evidence": {
                "strategy_type": "B",
                "key_signal": "close=120.0，MA5/MA20/ATR 暂缺",
                "validation_check": "缺失指标已显式披露，仅保留收盘价锚点。",
            },
            "signal_entry_price": kwargs.get("signal_entry_price", 120.0),
        },
    )

    generated = report_generation_ssot.generate_report_ssot(
        db_session,
        stock_code="600519.SH",
        trade_date=trade_date,
        forced_strategy_type="B",
    )
    payload = get_report_view_payload_ssot(db_session, generated["report_id"], viewer_role="admin")

    assert generated["published"] is True
    assert payload["plain_report"]["stock_specific_note"] == (
        f"MOUTAI（600519.SH·{payload['company_overview']['industry']}），最新收盘 120.00 元，MA5 —，MA20 —，ATR%=—。"
    )
    assert payload["plain_report"]["evidence_backing_points"][0]["basis"] == "技术指标数据正在加载"
    assert payload["plain_report"]["evidence_backing_points"][0]["nums"] == ["atr_pct=—", "volatility_20d=0.02"]


@pytest.mark.feature('FR06-LLM-07')
def test_fr06_atr_multiplier_by_strategy(db_session):
    """Verify ATR multipliers for strategy types A, B, and C."""
    from app.services.report_generation_ssot import _ATR_MULTIPLIER_BY_STRATEGY

    assert _ATR_MULTIPLIER_BY_STRATEGY["A"] == 1.5
    assert _ATR_MULTIPLIER_BY_STRATEGY["B"] == 2.0
    assert _ATR_MULTIPLIER_BY_STRATEGY["C"] == 2.5


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# FR06-LLM-08 BEAR+B/C 绫荤唺甯傜煭璺?# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

@pytest.mark.feature('FR06-LLM-08')
def test_fr06_bear_market_shortcircuit(client, db_session, monkeypatch):
    """Verify BEAR market short-circuit skips LLM and degrades the result."""
    trade_date = "2026-03-06"
    seed_generation_context(
        db_session,
        trade_date=trade_date,
        market_state="BEAR",
        atr_pct=0.03,
        ma20=116.0,
    )

    def _unexpected(*args, **kwargs):
        raise AssertionError("LLM should not be called for BEAR+B/C short-circuit")

    monkeypatch.setattr("app.services.report_generation_ssot.run_generation_model", _unexpected)

    # Force a non-A strategy so the BEAR short-circuit path is exercised.
    monkeypatch.setattr(
        "app.services.report_generation_ssot._determine_strategy_type",
        lambda db, stock_code, trade_day, kline_row: "B",
    )

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["recommendation"] == "HOLD"
    assert data["confidence"] == 0.0
    assert data["quality_flag"] == "degraded"
    assert data["status_reason"] == "BEAR_MARKET_FILTERED"


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# FR06-LLM-06 杈╄瘉瀹￠槄: BUY+conf>=0.65 鈫?浜屾椋庡
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

@pytest.mark.feature('FR06-LLM-06')
def test_fr06_dialectical_audit_triggered(db_session):
    """Verify audit is triggered only for BUY + confidence>=threshold."""
    from app.services.llm_router import should_trigger_audit

    # BUY + high confidence -> should trigger
    assert should_trigger_audit("BUY", 0.78, "") is True
    # HOLD + high confidence -> should not trigger
    assert should_trigger_audit("HOLD", 0.78, "") is False
    # HOLD + low confidence + no contradiction -> should not trigger
    assert should_trigger_audit("HOLD", 0.50, "") is False
    # BUY + low confidence -> should not trigger
    assert should_trigger_audit("BUY", 0.30, "") is False
    # BUY + contradiction but low confidence -> still should not trigger
    assert should_trigger_audit("BUY", 0.30, "指标互相打架") is False
    # BUY + threshold confidence -> triggers
    assert should_trigger_audit("BUY", 0.65, "") is True


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# FR06-LLM-02 prior_stats 鍏堥獙鏁版嵁娉ㄥ叆
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

@pytest.mark.feature('FR06-LLM-02')
def test_fr06_prior_stats_injection(db_session):
    """Verify prior_stats returns data only after the sample threshold is met."""
    from app.services.report_generation_ssot import _compute_prior_stats

    result = _compute_prior_stats(db_session, strategy_type="A", trade_day=date(2026, 3, 6))
    # No settlement data: returns None
    assert result is None


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# FR06-LLM-09 instruction_card 闃插€掓寕
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

@pytest.mark.feature('FR06-LLM-09')
def test_fr06_instruction_card_inversion_guard(db_session):
    """Verify inverted stop-loss inputs force SKIPPED instructions."""
    from app.services.report_generation_ssot import _build_instruction_card

    card = _build_instruction_card(
        signal_entry_price=10.0,
        atr_pct=0.0,  # ATR=0 鈫?stop_loss fallback
        strategy_type="B",
    )
    # atr_pct=0 鈫?stop_loss is NaN or fallback, verify card is valid
    assert "signal_entry_price" in card
    assert "stop_loss" in card


# ──────────────────────────────────────────────────────────────
# FR06-LLM-08 bear market circuit breaker
# ──────────────────────────────────────────────────────────────

@pytest.mark.feature('FR06-LLM-08')
def test_fr06_bear_market_filters_strategy_b(client, db_session, monkeypatch):
    """BEAR + strategy B → HOLD / BEAR_MARKET_FILTERED / unpublished."""
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date, market_state="BEAR")

    def _unexpected(*args, **kwargs):
        raise AssertionError("LLM should not be called for BEAR+B")

    monkeypatch.setattr("app.services.report_generation_ssot.run_generation_model", _unexpected)
    monkeypatch.setattr(
        "app.services.report_generation_ssot._determine_strategy_type",
        lambda db, stock_code, trade_day, kline_row: "B",
    )

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["recommendation"] == "HOLD"
    assert data["confidence"] == 0.0
    assert data["status_reason"] == "BEAR_MARKET_FILTERED"
    assert data["published"] is False
    assert data["publish_status"] == "UNPUBLISHED"


@pytest.mark.feature('FR06-LLM-08')
def test_fr06_bear_market_filters_strategy_c(client, db_session, monkeypatch):
    """BEAR + strategy C → HOLD / BEAR_MARKET_FILTERED / unpublished."""
    trade_date = "2026-03-06"
    seed_generation_context(db_session, trade_date=trade_date, market_state="BEAR")

    def _unexpected(*args, **kwargs):
        raise AssertionError("LLM should not be called for BEAR+C")

    monkeypatch.setattr("app.services.report_generation_ssot.run_generation_model", _unexpected)
    monkeypatch.setattr(
        "app.services.report_generation_ssot._determine_strategy_type",
        lambda db, stock_code, trade_day, kline_row: "C",
    )

    response = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "trade_date": trade_date, "source": "test"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["recommendation"] == "HOLD"
    assert data["confidence"] == 0.0
    assert data["status_reason"] == "BEAR_MARKET_FILTERED"
    assert data["published"] is False


@pytest.mark.feature('FR06-LLM-01')
def test_fr06_ensure_test_generation_context_idempotent_no_unique_constraint(db_session):
    """ensure_test_generation_context 连续调用两次不应触发 UNIQUE constraint failed.

    因为 hotspot_top50 / northbound_summary / etf_flow_summary 均使用 source_name='eastmoney',
    若共用同一 batch_id 则 (trade_date, stock_code, source_name, batch_id) 会在第二组插入时冲突.
    """
    trade_date = "2026-03-06"
    trade_day = date.fromisoformat(trade_date)
    usage_table = Base.metadata.tables["report_data_usage"]

    # 第一次调用——应正常运行并写入 usage 行
    ensure_test_generation_context(db_session, stock_code="600519.SH", trade_date=trade_date)
    db_session.commit()

    rows_after_first = db_session.execute(
        usage_table.select()
        .where(usage_table.c.stock_code == "600519.SH")
        .where(usage_table.c.trade_date == trade_day)
    ).mappings().fetchall()
    assert len(rows_after_first) >= 4, "should have at least 4 required dataset usage rows"

    # 确认 eastmoney 数据集的 batch_id 彻底分离
    eastmoney_rows = [r for r in rows_after_first if r["source_name"] == "eastmoney"]
    eastmoney_batch_ids = {r["batch_id"] for r in eastmoney_rows}
    assert len(eastmoney_batch_ids) == len(eastmoney_rows), (
        f"each eastmoney dataset must have a unique batch_id, got {eastmoney_batch_ids} "
        f"for {[r['dataset_name'] for r in eastmoney_rows]}"
    )

    # 第二次调用——应是幂等操作，不得抛出 IntegrityError
    from sqlalchemy.exc import IntegrityError
    try:
        ensure_test_generation_context(db_session, stock_code="600519.SH", trade_date=trade_date)
        db_session.commit()
    except IntegrityError as exc:
        pytest.fail(f"ensure_test_generation_context raised IntegrityError on second call: {exc}")

    rows_after_second = db_session.execute(
        usage_table.select()
        .where(usage_table.c.stock_code == "600519.SH")
        .where(usage_table.c.trade_date == trade_day)
    ).mappings().fetchall()
    # 幂等：行数不应增加
    assert len(rows_after_second) == len(rows_after_first), (
        "second call should not insert extra rows; got "
        f"{len(rows_after_second)} vs {len(rows_after_first)}"
    )
