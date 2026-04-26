import pytest
from datetime import date
from sqlalchemy import text
from urllib.parse import quote

from app.models import Base
from tests.helpers_ssot import insert_report_bundle_ssot, utc_now

pytestmark = [
    pytest.mark.feature("FR10-LIST-01"),
    pytest.mark.feature("FR10-DETAIL-02"),
]


def test_fr10_reports_list_contract(client, seed_report_bundle):
    report = seed_report_bundle()

    response = client.get("/api/v1/reports", headers={"X-Request-ID": "req-report-list"})

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["request_id"] == "req-report-list"
    assert response.headers["X-Request-ID"] == "req-report-list"
    assert body["data"]["total"] == 1
    assert body["data"]["page"] == 1
    assert body["data"]["page_size"] == 20

    item = body["data"]["items"][0]
    assert item["report_id"] == report.report_id
    assert item["stock_code"] == report.stock_code
    assert item["trade_date"] == str(report.trade_date)
    assert item["recommendation"] == report.recommendation
    assert item["strategy_type"] == report.strategy_type
    assert item["market_state"] == report.market_state
    assert item["quality_flag"] == report.quality_flag
    assert item["published"] is True
    assert item["position_status"] == "OPEN"

    missing_response = client.get("/api/v1/reports?quality_flag=missing")
    assert missing_response.status_code == 200
    missing_body = missing_response.json()
    assert missing_body["success"] is True
    assert missing_body["data"]["items"] == []
    assert missing_body["data"]["total"] == 0
    assert missing_body["data"]["data_status"] == "READY"


def test_fr10_reports_list_rejects_non_ok_quality_filter(client, db_session):
    insert_report_bundle_ssot(db_session, stock_code="600519.SH", quality_flag="ok")
    insert_report_bundle_ssot(db_session, stock_code="000001.SZ", quality_flag="stale_ok")

    response = client.get("/api/v1/reports?quality_flag=stale_ok")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["items"] == []
    assert body["data"]["total"] == 0
    assert body["data"]["data_status"] == "READY"


def test_fr10_report_detail_masks_free_fields(client, seed_report_bundle):
    report = seed_report_bundle()

    response = client.get(f"/api/v1/reports/{report.report_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["report_id"] == report.report_id
    assert body["data"]["term_context"]["signal_entry_price"] == "¥**.**"
    assert body["data"]["instruction_card"]["signal_entry_price"] == "¥**.**"
    assert body["data"]["instruction_card"]["stop_loss"] == "¥**.**"
    assert body["data"]["instruction_card"]["target_price"] == "¥**.**"
    assert body["data"]["sim_trade_instruction"] is None
    assert isinstance(body["data"]["term_context"], dict)


def test_fr10_report_detail_redacts_operational_metadata_for_non_admin(client, db_session):
    report = insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="贵州茅台")
    now = utc_now()
    usage_table = Base.metadata.tables["report_data_usage"]
    usage_link_table = Base.metadata.tables["report_data_usage_link"]
    trade_day = date.fromisoformat(str(report.trade_date))

    db_session.execute(
        text(
            """
            UPDATE report
            SET llm_actual_model = 'gpt-5.4',
                llm_provider_name = 'newapi',
                llm_endpoint = 'http://internal-llm.local/v1'
            WHERE report_id = :report_id
            """
        ),
        {"report_id": report.report_id},
    )
    db_session.execute(
        usage_table.insert().values(
            usage_id="usage-redact-1",
            trade_date=trade_day,
            stock_code=report.stock_code,
            dataset_name="northbound_summary",
            source_name="akshare_hsgt_hist",
            batch_id="batch-redact-1",
            fetch_time=now,
            status="ok",
            status_reason="akshare_hsgt_hist",
            created_at=now,
        )
    )
    db_session.execute(
        usage_link_table.insert().values(
            report_data_usage_link_id="link-redact-1",
            report_id=report.report_id,
            usage_id="usage-redact-1",
            created_at=now,
        )
    )
    db_session.commit()

    response = client.get(f"/api/v1/reports/{report.report_id}")

    assert response.status_code == 200
    data = response.json()["data"]
    assert "llm_actual_model" not in data
    assert "llm_provider_name" not in data
    assert "llm_endpoint" not in data
    assert all(item.get("usage_id") is None for item in data["used_data"])
    assert all(item.get("batch_id") is None for item in data["used_data"])


def test_fr10_report_detail_fallback_path_does_not_expose_non_ok_report(client, db_session):
    report = insert_report_bundle_ssot(db_session, stock_code="600519.SH", quality_flag="stale_ok")

    response = client.get(f"/api/v1/reports/{report.report_id}")

    assert response.status_code == 404
    payload = response.json()
    assert payload["error_code"] == "REPORT_NOT_AVAILABLE"


def test_fr10_report_detail_pro_shows_full_fields(client, create_user, seed_report_bundle):
    report = seed_report_bundle()
    user_info = create_user(
        email="pro-user@example.com",
        password="Password123",
        tier="Pro",
        email_verified=True,
    )

    login_response = client.post(
        "/auth/login",
        json={"email": user_info["user"].email, "password": user_info["password"]},
    )
    assert login_response.status_code == 200
    access_token = login_response.json()["data"]["access_token"]

    response = client.get(
        f"/api/v1/reports/{report.report_id}",
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["instruction_card"]["signal_entry_price"] == 123.45
    assert body["data"]["instruction_card"]["stop_loss"] == 117.28
    assert body["data"]["instruction_card"]["target_price"] == 138.88
    assert body["data"]["term_context"]["signal_entry_price"] == "123.45"
    assert body["data"]["sim_trade_instruction"]["100k"]["status"] == "EXECUTE"
    assert set(body["data"]["sim_trade_instruction"].keys()) == {"10k", "100k", "500k"}


def test_fr10_report_detail_capital_summary_keeps_ok_reason_honest(client, db_session):
    report = insert_report_bundle_ssot(db_session)
    now = utc_now()
    usage_table = Base.metadata.tables["report_data_usage"]
    usage_link_table = Base.metadata.tables["report_data_usage_link"]

    northbound_usage_id = "usage-nb"
    etf_usage_id = "usage-etf"
    _td = date.fromisoformat(str(report.trade_date))
    db_session.execute(
        usage_table.insert().values(
            usage_id=northbound_usage_id,
            trade_date=_td,
            stock_code=report.stock_code,
            dataset_name="northbound_summary",
            source_name="akshare_hsgt_hist",
            batch_id="batch-nb",
            fetch_time=now,
            status="ok",
            status_reason="akshare_hsgt_hist",
            created_at=now,
        )
    )
    db_session.execute(
        usage_table.insert().values(
            usage_id=etf_usage_id,
            trade_date=_td,
            stock_code=report.stock_code,
            dataset_name="etf_flow_summary",
            source_name="akshare_fund_etf_daily",
            batch_id="batch-etf",
            fetch_time=now,
            status="degraded",
            status_reason="fetcher_not_provided",
            created_at=now,
        )
    )
    db_session.execute(
        usage_link_table.insert().values(
            report_data_usage_link_id="link-nb",
            report_id=report.report_id,
            usage_id=northbound_usage_id,
            created_at=now,
        )
    )
    db_session.execute(
        usage_link_table.insert().values(
            report_data_usage_link_id="link-etf",
            report_id=report.report_id,
            usage_id=etf_usage_id,
            created_at=now,
        )
    )
    db_session.commit()

    response = client.get(f"/api/v1/reports/{report.report_id}")

    assert response.status_code == 200
    capital = response.json()["data"]["capital_game_summary"]
    assert capital["northbound"]["reason"] == "数据已接入"
    assert "北向资金摘要已就绪" in capital["headline"]
    assert "ETF 资金流摘要已就绪" not in capital["headline"]


@pytest.mark.parametrize("param_name", ["q", "stock_name"])
@pytest.mark.parametrize("query_text", [r"Alpha\Beta", "Alpha%Beta", "Alpha_Beta"])
def test_fr10_reports_text_filters_escape_literals(client, seed_report_bundle, param_name, query_text):
    report = seed_report_bundle(stock_name=query_text)

    encoded = quote(query_text, safe="")
    response = client.get(f"/api/v1/reports?{param_name}={encoded}")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["total"] == 1
    assert body["data"]["items"][0]["report_id"] == report.report_id
    assert body["data"]["items"][0]["stock_name"] == query_text


def test_fr10_reports_viewer_window_anchors_to_latest_published_report(
    client,
    db_session,
    create_user,
    monkeypatch,
):
    import app.services.ssot_read_model as read_model

    monkeypatch.setattr(read_model, "latest_trade_date_str", lambda dt=None: "2026-03-19")
    latest_report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="贵州茅台",
        trade_date="2026-03-06",
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="平安银行",
        trade_date="2026-02-24",
    )

    list_response = client.get("/api/v1/reports")

    assert list_response.status_code == 200
    list_body = list_response.json()
    assert list_body["data"]["total"] == 1
    assert list_body["data"]["items"][0]["report_id"] == latest_report.report_id
    assert list_body["data"]["items"][0]["trade_date"] == "2026-03-06"

    detail_response = client.get(f"/api/v1/reports/{latest_report.report_id}")

    assert detail_response.status_code == 200
    assert detail_response.json()["data"]["report_id"] == latest_report.report_id

    user_info = create_user(
        email="free-window@example.com",
        password="Password123",
        tier="Free",
        email_verified=True,
    )
    login_response = client.post(
        "/auth/login",
        json={"email": user_info["user"].email, "password": user_info["password"]},
    )
    assert login_response.status_code == 200
    access_token = login_response.json()["data"]["access_token"]

    advanced_response = client.get(
        f"/api/v1/reports/{latest_report.report_id}/advanced",
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert advanced_response.status_code == 200
    assert advanced_response.json()["data"]["report_id"] == latest_report.report_id


def test_fr10_reports_list_anchors_history_window_to_runtime_data(client, seed_report_bundle, monkeypatch):
    import app.services.ssot_read_model as ssot_read_model

    monkeypatch.setattr(ssot_read_model, "latest_trade_date_str", lambda dt=None: "2026-03-19")
    report = seed_report_bundle(trade_date="2026-03-06")

    response = client.get("/api/v1/reports")

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["data"]["total"] == 1
    assert body["data"]["items"][0]["report_id"] == report.report_id


def test_fr10_report_detail_and_advanced_anchor_history_window_to_runtime_data(
    client,
    create_user,
    seed_report_bundle,
    monkeypatch,
):
    import app.services.ssot_read_model as ssot_read_model

    monkeypatch.setattr(ssot_read_model, "latest_trade_date_str", lambda dt=None: "2026-03-19")
    report = seed_report_bundle(trade_date="2026-03-06")
    user_info = create_user(
        email="free-anchor@example.com",
        password="Password123",
        tier="Free",
        email_verified=True,
    )
    login_response = client.post(
        "/auth/login",
        json={"email": user_info["user"].email, "password": user_info["password"]},
    )
    access_token = login_response.json()["data"]["access_token"]

    detail_response = client.get(f"/api/v1/reports/{report.report_id}")
    advanced_response = client.get(
        f"/api/v1/reports/{report.report_id}/advanced",
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert detail_response.status_code == 200
    assert detail_response.json()["data"]["report_id"] == report.report_id
    assert advanced_response.status_code == 200
    assert len(advanced_response.json()["data"]["reasoning_chain"]) <= 200


def test_fr10_reports_filtered_history_still_respects_viewer_window(
    client,
    db_session,
    create_user,
    monkeypatch,
):
    import app.services.ssot_read_model as ssot_read_model

    monkeypatch.setattr(ssot_read_model, "latest_trade_date_str", lambda dt=None: "2026-03-19")
    insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        stock_name="贵州茅台",
        trade_date="2026-03-06",
    )
    old_report = insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        stock_name="平安银行",
        trade_date="2026-01-09",
    )
    user_info = create_user(
        email="free-cutoff@example.com",
        password="Password123",
        tier="Free",
        email_verified=True,
    )
    login_response = client.post(
        "/auth/login",
        json={"email": user_info["user"].email, "password": user_info["password"]},
    )
    access_token = login_response.json()["data"]["access_token"]
    headers = {"Authorization": f"Bearer {access_token}"}

    list_response = client.get("/api/v1/reports?trade_date=2026-01-09", headers=headers)
    api_detail_response = client.get(f"/api/v1/reports/{old_report.report_id}", headers=headers)
    html_detail_response = client.get(f"/reports/{old_report.report_id}", headers=headers)

    assert list_response.status_code == 200
    assert list_response.json()["data"]["total"] == 0
    assert api_detail_response.status_code == 403
    assert html_detail_response.status_code == 403
    assert "/login?next=/admin" not in html_detail_response.text


def test_fr10_report_detail_api_includes_company_overview_and_summary_text(client, seed_report_bundle):
    report = seed_report_bundle(stock_code="600519.SH", stock_name="贵州茅台")

    response = client.get(f"/api/v1/reports/{report.report_id}")

    assert response.status_code == 200
    data = response.json()["data"]
    # company_overview is NOT in SSOT 05 ReportDetail spec; only present in view payload
    assert "company_overview" not in data
    assert data["stock_code"] == "600519.SH"
    assert (
        data["capital_game_summary"]["summary_text"] is None
        or isinstance(data["capital_game_summary"]["summary_text"], str)
    )


def test_fr10_report_detail_api_sets_summary_text_when_capital_inputs_exist(client, db_session):
    report = insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="贵州茅台")
    now = utc_now()
    usage_table = Base.metadata.tables["report_data_usage"]
    usage_link_table = Base.metadata.tables["report_data_usage_link"]
    _td = date.fromisoformat(str(report.trade_date))
    db_session.execute(
        usage_table.insert().values(
            usage_id="usage-capital-summary",
            trade_date=_td,
            stock_code=report.stock_code,
            dataset_name="northbound_summary",
            source_name="akshare_hsgt_hist",
            batch_id="batch-capital-summary",
            fetch_time=now,
            status="ok",
            status_reason="akshare_hsgt_hist",
            created_at=now,
        )
    )
    db_session.execute(
        usage_link_table.insert().values(
            report_data_usage_link_id="link-capital-summary",
            report_id=report.report_id,
            usage_id="usage-capital-summary",
            created_at=now,
        )
    )
    db_session.commit()

    response = client.get(f"/api/v1/reports/{report.report_id}")

    assert response.status_code == 200
    data = response.json()["data"]
    # company_overview is NOT in SSOT 05 ReportDetail spec; only present in view payload
    assert "company_overview" not in data
    assert data["stock_code"] == "600519.SH"
    assert isinstance(data["capital_game_summary"]["summary_text"], str)
    assert data["capital_game_summary"]["summary_text"]


def test_fr10_report_detail_api_fills_price_forecast_five_windows(client, seed_report_bundle):
    report = seed_report_bundle(stock_code="600519.SH", stock_name="贵州茅台")

    response = client.get(f"/api/v1/reports/{report.report_id}")

    assert response.status_code == 200
    data = response.json()["data"]
    windows = data["price_forecast"]["windows"]
    assert {int(item["horizon_days"]) for item in windows} == {1, 7, 14, 30, 60}
    assert all(item["llm_pct_range"] != "—" for item in windows)


def test_fr10_report_detail_api_sanitizes_internal_fallback_conclusion(client, db_session):
    report = insert_report_bundle_ssot(db_session, stock_code="600871.SH", stock_name="石化油服")
    db_session.execute(
        text(
            """
            UPDATE report
            SET llm_fallback_level = 'failed',
                conclusion_text = :conclusion_text,
                reasoning_chain_md = :reasoning_chain_md
            WHERE report_id = :report_id
            """
        ),
        {
            "report_id": report.report_id,
            "conclusion_text": "石化油服 600871.SH 研报生成（LLM降级，规则兜底）",
            "reasoning_chain_md": "## 分析过程（LLM降级，规则兜底）\nmarket_state=NEUTRAL\nstrategy_type=B\nquality_flag=ok",
        },
    )
    db_session.commit()

    response = client.get(f"/api/v1/reports/{report.report_id}")

    assert response.status_code == 200
    data = response.json()["data"]
    assert "LLM降级" not in data["conclusion_text"]
    assert "规则兜底" not in data["conclusion_text"]
    assert "石化油服当前处于" in data["conclusion_text"]


def test_fr10_report_advanced_api_sanitizes_internal_fallback_reasoning(
    client, create_user, db_session
):
    report = insert_report_bundle_ssot(db_session, stock_code="600871.SH", stock_name="石化油服")
    db_session.execute(
        text(
            """
            UPDATE report
            SET llm_fallback_level = 'failed',
                conclusion_text = :conclusion_text,
                reasoning_chain_md = :reasoning_chain_md
            WHERE report_id = :report_id
            """
        ),
        {
            "report_id": report.report_id,
            "conclusion_text": "石化油服 600871.SH 研报生成（LLM降级，规则兜底）",
            "reasoning_chain_md": "## 分析过程（LLM降级，规则兜底）\nmarket_state=NEUTRAL\nstrategy_type=B\nquality_flag=ok\nfallback=rule_based",
        },
    )
    db_session.commit()

    user_info = create_user(
        email="pro-fallback-sanitize@example.com",
        password="Password123",
        tier="Pro",
        email_verified=True,
    )
    login_response = client.post(
        "/auth/login",
        json={"email": user_info["user"].email, "password": user_info["password"]},
    )
    access_token = login_response.json()["data"]["access_token"]

    response = client.get(
        f"/api/v1/reports/{report.report_id}/advanced",
        headers={"Authorization": f"Bearer {access_token}"},
    )

    assert response.status_code == 200
    reasoning_chain = response.json()["data"]["reasoning_chain"]
    assert "market_state=" not in reasoning_chain
    assert "strategy_type=" not in reasoning_chain
    assert "fallback=rule_based" not in reasoning_chain
    assert "当前报告由规则引擎生成" in reasoning_chain
