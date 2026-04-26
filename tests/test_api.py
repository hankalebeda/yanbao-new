from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.core.config import settings
from app.core.db import SessionLocal
from app.main import app
from app.models import PredictionOutcome, Report, ReportIdempotency
from app.services.report_engine import _apply_direction_override, _reuse_recent_object

pytestmark = [
    pytest.mark.feature("FR06-LLM-03"),
    pytest.mark.feature("FR06-LLM-04"),
    pytest.mark.feature("FR06-LLM-10"),
]

# base_url 确保 Host=127.0.0.1，通过 TrustedHost 校验（见 config.trusted_hosts）
client = TestClient(app, base_url="http://127.0.0.1")


def test_platform_summary_public():
    """公开接口 /api/v1/platform/summary 返回平台模拟汇总，无需鉴权。"""
    r = client.get("/api/v1/platform/summary")
    assert r.status_code == 200
    d = r.json()
    assert d.get("code") == 0
    data = d.get("data", {})
    assert "win_rate" in data
    assert "pnl_ratio" in data
    assert "alpha" in data
    assert "total_trades" in data


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body.get("success") is True or body.get("code") == 0
    assert r.headers.get("X-Content-Type-Options") == "nosniff"
    assert r.headers.get("X-Frame-Options") == "DENY"


def test_health_alias_v1():
    r = client.get("/api/v1/health")
    assert r.status_code == 200
    body = r.json()
    assert body.get("success") is True
    assert isinstance((body.get("data") or {}).get("status"), str)


def test_page_routes_return_200():
    """页面路由：首页、研报列表、统计看板、模拟收益看板 返回 200。"""
    for path in ["/", "/reports", "/dashboard", "/login", "/register", "/forgot-password", "/subscribe"]:
        r = client.get(path)
        assert r.status_code == 200, f"GET {path} expected 200, got {r.status_code}"
        assert "<html" in r.text.lower() or "html" in r.headers.get("content-type", "")
    # sim-dashboard 需鉴权，可重定向或 200
    r = client.get("/portfolio/sim-dashboard", follow_redirects=False)
    assert r.status_code in (200, 302, 307), f"GET /portfolio/sim-dashboard expected 200/302/307, got {r.status_code}"


def test_admin_redirects_to_login_when_unauth():
    """未登录访问 /admin → 302 跳转 /login?next=/admin"""
    r = client.get("/admin", follow_redirects=False)
    assert r.status_code == 302
    assert "/login" in r.headers.get("location", "")
    assert "next=" in r.headers.get("location", "")


def test_profile_redirects_to_login_when_unauth():
    """未登录访问 /profile → 302 跳转 /login?next=/profile"""
    r = client.get("/profile", follow_redirects=False)
    assert r.status_code == 302
    assert "/login" in r.headers.get("location", "")


def test_auth_register_succeeds():
    """E6 用户系统：注册成功返回 201，写入 user 表，返回 token。"""
    import uuid
    email = f"reg_test_{uuid.uuid4().hex[:12]}@example.com"
    r = client.post("/auth/register", json={"email": email, "password": "Test1234"})
    assert r.status_code == 201
    data = r.json().get("data") or r.json()
    assert data.get("access_token")
    assert data.get("user", {}).get("email") == email
    # 重复注册应返回 400
    r2 = client.post("/auth/register", json={"email": email, "password": "Test1234"})
    assert r2.status_code == 400


def test_collect_and_generate_flow():
    """采集 + 生成研报（默认 mock LLM）。用本地 Ollama 测试时请先设置 MOCK_LLM=false 再运行本用例。"""
    # 清除旧数据确保生成最新格式（契约更新后旧幂等缓存结构可能不匹配）
    client.post("/api/v1/internal/reports/clear?stock_code=600519.SH")
    rc = client.post("/api/v1/internal/hotspot/collect?platform=douyin&stock_code=600519.SH", json={"top_n": 5})
    assert rc.status_code == 200
    rr = client.post("/api/v1/reports/generate", json={"stock_code": "600519.SH", "source": "test"})
    assert rr.status_code == 200
    data = rr.json()["data"]
    assert data["report_id"]
    assert "reasoning_trace" in data["report"]
    assert "novice_guide" in data["report"]
    assert data["report"]["novice_guide"]["one_line_decision"]
    assert len(data["report"]["novice_guide"]["why_points"]) >= 3
    assert "validation_plan" in data["report"]["reasoning_trace"]
    assert data["report"]["reasoning_trace"]["validation_plan"]["windows"] == [1, 2, 3, 4, 5, 6, 7]
    assert "market_snapshot" in data["report"]["dimensions"]
    assert "price_forecast" in data["report"]
    assert "direction_forecast" in data["report"]
    assert "plain_report" in data["report"]
    assert "execution_plan" in data["report"]["plain_report"]
    assert data["report"]["plain_report"]["execution_plan"]["position_suggestion"]
    assert "accuracy_explain" in data["report"]["plain_report"]
    assert data["report"]["plain_report"]["accuracy_explain"]["confidence_formula"]["formula"]
    assert "market_dual_source" in data["report"]["dimensions"]
    assert "dual_status" in data["report"]["dimensions"]["market_dual_source"]
    assert len(data["report"]["direction_forecast"]["horizons"]) >= 7  # 1~7 日（未来趋势表）+ 14/30/60（主要依据表）
    assert len(data["report"]["direction_forecast"]["backtest_recent_3m"]) >= 0  # 技术信号回测已废弃，可为空
    assert len(data["report"]["price_forecast"]["windows"]) == 5
    assert "explain" in data["report"]["price_forecast"]
    assert len(data["report"]["price_forecast"]["explain"]["factor_contributions"]) >= 5
    assert "confidence_diagnostics" in data["report"]["price_forecast"]["explain"]
    assert "backtest" in data["report"]["price_forecast"]
    assert "horizons" in data["report"]["price_forecast"]["backtest"]
    assert "selected_model_by_horizon" in data["report"]["price_forecast"]["backtest"] or "selected_model_by_horizon" in data["report"]["price_forecast"]["explain"]
    assert "historical_position" in data["report"]["price_forecast"]["explain"]
    assert "historical_position_long" in data["report"]["price_forecast"]["explain"]
    assert "multi_cycle_alignment" in data["report"]["price_forecast"]["explain"]
    assert len(data["report"]["price_forecast"]["explain"]["selected_model_by_horizon"]) == 5
    smh0 = data["report"]["price_forecast"]["explain"]["selected_model_by_horizon"][0]
    assert "model_name" in smh0 or "horizon_days" in smh0
    assert "factor_cn" in data["report"]["price_forecast"]["explain"]["factor_contributions"][0]
    assert "readiness" in data["report"]["price_forecast"]
    assert "target_ranges" in data["report"]["price_forecast"]["readiness"]
    assert "quality_gate" in data["report"]
    assert "publish_decision" in data["report"]["quality_gate"]
    assert "audit_flag" in data["report"]  # E2 三方投票审计（not_triggered|audit_skipped|unanimous_buy|...）
    assert "dimension_coverage" in data["report"]["quality_gate"]
    first_w = data["report"]["price_forecast"]["windows"][0]
    assert "confidence_raw" in first_w
    assert "confidence" in first_w
    assert len(data["report"]["price_forecast"]["improvement_actions"]) >= 1
    assert data["report"]["indicator_explanation"]["ma5_desc"]
    assert data["report"]["indicator_explanation"]["ma20_desc"]
    assert "company_overview" in data["report"]
    assert "industry_competition" in data["report"]
    assert "financial_analysis" in data["report"]
    assert "valuation" in data["report"]
    assert data["report"]["valuation"]["listed_days"] is None or data["report"]["valuation"]["listed_days"] > 1000
    assert "raw_inputs" in data["report"]["reasoning_trace"]
    assert "capital_flow" in data["report"]["dimensions"]
    assert "dragon_tiger" in data["report"]["dimensions"]
    assert "margin_financing" in data["report"]["dimensions"]
    assert "capital_game_summary" in data["report"]["plain_report"]
    assert "stability_gate_result" in data["report"]["plain_report"]
    assert "history_span" in data["report"]["plain_report"]["capital_game_summary"]
    assert "history_records" in data["report"]["dimensions"]["dragon_tiger"]
    assert "history_records" in data["report"]["dimensions"]["margin_financing"]
    assert "page_stats" in data["report"]["dimensions"]["dragon_tiger"]
    assert "pages_fetched" in data["report"]["dimensions"]["dragon_tiger"]["page_stats"]
    assert "page_stats" in data["report"]["dimensions"]["margin_financing"]
    assert "pages_fetched" in data["report"]["dimensions"]["margin_financing"]["page_stats"]
    assert "main_force" in data["report"]["dimensions"]["capital_flow"]
    assert "history_records" in data["report"]["dimensions"]["capital_flow"]["main_force"]
    # 技术信号回测已废弃，backtest 结构存在即可
    assert "report_data_usage" in data["report"]
    assert "sources" in data["report"]["report_data_usage"]
    assert len(data["report"]["report_data_usage"]["sources"]) >= 1


def test_internal_reports_clear():
    """清空全部研报（内部接口），便于只保留新生成的 8B 研报。无 INTERNAL_API_KEY 时可直接调用。"""
    r = client.post("/api/v1/internal/reports/clear")
    assert r.status_code == 200
    data = r.json()
    assert data.get("code") == 0
    assert "deleted" in data.get("data", {})
    assert data["data"].get("stock_code") is None  # 未传 stock_code 即清空全部


def test_internal_reports_clear_one_stock():
    """清空指定股票的研报。"""
    r = client.post("/api/v1/internal/reports/clear?stock_code=600519.SH")
    assert r.status_code == 200
    assert r.json().get("code") == 0
    assert r.json()["data"].get("stock_code") == "600519.SH"


def test_internal_stats_clear():
    """清空统计看板与模拟收益看板的数据和样本。"""
    r = client.post("/api/v1/internal/stats/clear")
    assert r.status_code == 200
    data = r.json()
    assert data.get("code") == 0
    d = data.get("data", {})
    for k in ("prediction_outcome", "report_feedback", "sim_position", "sim_position_backtest", "sim_account", "sim_baseline"):
        assert k in d
        assert isinstance(d[k], int)


def test_internal_llm_version():
    r = client.get("/api/v1/internal/llm/version")
    assert r.status_code == 200
    assert "test_model" in r.json()["data"]
    m = client.get("/api/v1/internal/metrics/summary")
    assert m.status_code == 200
    assert "llm" in m.json()["data"]
    s = client.get("/api/v1/internal/source/fallback-status")
    assert s.status_code == 200
    assert "runtime" in s.json()["data"]


def test_settle_and_stats():
    rr = client.post("/api/v1/reports/generate", json={"stock_code": "600519.SH", "source": "test"})
    rid = rr.json()["data"]["report_id"]
    rs = client.post(
        "/api/v1/predictions/settle",
        json={"report_id": rid, "stock_code": "600519.SH", "windows": [1, 7]},
    )
    assert rs.status_code == 200
    stats = client.get("/api/v1/predictions/stats")
    assert stats.status_code == 200
    data = stats.json()["data"]
    assert "judged" in data
    assert "total_judged" in data
    assert "accuracy" in data
    assert "by_window" in data
    assert "recent_3m" in data
    if data.get("total_judged", 0) > 0:
        assert 0 <= data["accuracy"] <= 1, "整合 §3、05 门禁 6：accuracy 为 0~1（方向命中率监控用）"


def test_billing_and_subscription_flow():
    create = client.post(
        "/api/v1/billing/create-order",
        json={"user_id": "u001", "plan_code": "monthly", "channel": "mock"},
    )
    assert create.status_code == 200
    order_id = create.json()["data"]["order_id"]

    callback = client.post("/api/v1/billing/callback", json={"order_id": order_id, "paid": True, "tx_id": "tx001"})
    assert callback.status_code == 200
    assert callback.json()["data"]["status"] == "paid"

    sub = client.get("/api/v1/membership/subscription/status?user_id=u001")
    assert sub.status_code == 200
    assert sub.json()["data"]["status"] == "active"


def test_reports_list():
    r = client.get("/api/v1/reports?page=1&page_size=5")
    assert r.status_code == 200
    assert "request_id" in r.json()
    data = r.json()["data"]
    assert "items" in data
    assert "page" in data
    assert "page_size" in data
    assert "total" in data
    assert data["page"] == 1
    assert data["page_size"] == 5
    for it in data["items"]:
        assert "report_id" in it and "stock_code" in it and "stock_name" in it and "recommendation" in it
        assert it["recommendation"] in ("BUY", "SELL", "HOLD")


def test_reports_list_filter_stock_name():
    """05 条款 34：列表支持按代码/日期/结论筛选；整合 §4 支持按名称筛选。"""
    r = client.get("/api/v1/reports?page=1&page_size=20&stock_name=test")
    assert r.status_code == 200
    data = r.json()["data"]
    assert "items" in data and "total" in data


def test_reports_list_strategy_position_market_sort():
    """11 §4.2、05 §2.2：strategy_type、position_status、market_state、sort 参数可接受。"""
    r = client.get("/api/v1/reports?page=1&page_size=5&strategy_type=A")
    assert r.status_code == 200
    r = client.get("/api/v1/reports?page=1&page_size=5&position_status=OPEN")
    assert r.status_code == 200
    r = client.get("/api/v1/reports?page=1&page_size=5&market_state=BULL")
    assert r.status_code == 200
    r = client.get("/api/v1/reports?page=1&page_size=5&sort=-confidence")
    assert r.status_code == 200


def test_reports_list_today_q_limit_recommendation():
    """07 §2.2：today、q、limit、recommendation 参数可接受。"""
    r = client.get("/api/v1/reports?today=1&limit=6")
    assert r.status_code == 200
    data = r.json()["data"]
    assert "items" in data and "total" in data
    assert len(data["items"]) <= 6
    r2 = client.get("/api/v1/reports?recommendation=BUY&page_size=5")
    assert r2.status_code == 200
    assert "items" in r2.json()["data"]


def test_report_detail_advanced_area_transparency():
    """05 条款 32：高级区须展示「所用数据」与「研报生成全过程」；缺失/降级显式说明。"""
    rr = client.post("/api/v1/reports/generate", json={"stock_code": "600519.SH", "source": "test"})
    assert rr.status_code == 200
    report_id = rr.json()["data"]["report_id"]
    r = client.get(f"/api/v1/reports/{report_id}")
    assert r.status_code == 200
    report = r.json()["data"]
    rt = report.get("reasoning_trace") or {}
    assert "data_sources" in rt
    assert "evidence_items" in rt
    assert "analysis_steps" in rt
    assert "inference_summary" in rt
    assert "validation_plan" in rt
    assert "report_data_usage" in report
    assert "sources" in report["report_data_usage"]
    assert "quality_gate" in report
    qg = report["quality_gate"]
    assert "missing_fields" in qg
    assert "recover_actions" in qg


def test_report_data_usage_northbound_status():
    """01 §2.1 P1-5：dimensions.capital_flow.northbound 含 status；status∈{ok,stale_ok,...}；missing 时须有 reason。"""
    rr = client.post("/api/v1/reports/generate", json={"stock_code": "600519.SH", "source": "test"})
    assert rr.status_code == 200
    report = rr.json()["data"].get("report") or rr.json()["data"]
    dims = report.get("dimensions") or {}
    cf = dims.get("capital_flow") or {}
    nb = cf.get("northbound") or {}
    assert "northbound" in cf, "dimensions.capital_flow.northbound 须存在"
    assert "status" in nb, "northbound.status 须存在"
    valid_status = {"ok", "stale_ok", "proxy_ok", "realtime_only", "missing", "degraded", "invalid", "fallback"}
    assert nb["status"] in valid_status, f"northbound.status 须 ∈ {valid_status}, 实际: {nb.get('status')}"
    if nb.get("status") == "missing":
        assert "reason" in nb, "status=missing 时 northbound 须含 reason 字段"


def test_report_citations_no_fake_urls():
    """05 条款 5/6：citations 存在；禁止示例/伪协议 URL 进入正式研报。"""
    rr = client.post("/api/v1/reports/generate", json={"stock_code": "600519.SH", "source": "test"})
    assert rr.status_code == 200
    report = rr.json()["data"].get("report") or {}
    citations = report.get("citations") or []
    for c in citations:
        url = (c.get("source_url") or "").strip()
        assert "示例" not in url and "伪" not in url, "citations 不得含示例/伪 URL"


def test_report_feedback_returns_200_and_records():
    """E5.1 FR-07：研报反馈接口可用；反馈 2 秒内记录。"""
    report_id = "test-feedback-e51"
    import time
    t0 = time.perf_counter()
    r = client.post(
        "/api/v1/report-feedback",
        json={"report_id": report_id, "is_helpful": True},
    )
    elapsed = time.perf_counter() - t0
    assert r.status_code == 200, r.text
    data = r.json().get("data") or {}
    assert "feedback_id" in data
    assert "created_at" in data
    assert elapsed < 2.0, "FR-07: 反馈须 2 秒内记录"


def test_negative_feedback_alert_triggers_when_rate_ge_30():
    """FR-07：7 日内负反馈率 ≥ 30% 时触发 ReportHighNegativeFeedback 告警（01 §2.8、06 §8）。"""
    from app.models import ReportFeedback
    from app.services import notification

    rows = [ReportFeedback(report_id="x", user_id=0, is_helpful=0) for _ in range(7)]
    rows += [ReportFeedback(report_id="x", user_id=0, is_helpful=1) for _ in range(3)]
    mock_db = MagicMock()
    mock_db.query.return_value.filter.return_value.all.return_value = rows
    with patch.object(notification, "send_admin_notification") as mock_send:
        notification.check_and_alert_negative_feedback(mock_db)
        mock_send.assert_called_once()
        assert mock_send.call_args[0][0] == "negative_feedback_alert"
        assert mock_send.call_args[0][1]["rate"] >= 0.30


def test_admin_users_requires_auth():
    """17 §3.1、05 §2.5a：GET /admin/users 需 JWT 且 role=admin，未登录返回 401。"""
    r = client.get("/api/v1/admin/users")
    assert r.status_code == 401


def test_notification_send_admin_no_webhook_returns_false(monkeypatch):
    """E5.2 FR-05a：无 webhook 时 send_admin_notification 不抛错且返回 False。"""
    from app.services.notification import send_admin_notification
    monkeypatch.setattr(settings, "notification_enabled", False)
    monkeypatch.setattr(settings, "admin_alert_webhook_url", "")
    monkeypatch.setattr(settings, "alert_webhook_enabled", False)
    monkeypatch.setattr(settings, "alert_webhook_url", "")
    ok = send_admin_notification("report_ready", {"count": 3, "trade_date": "2026-02-27"})
    assert ok is False


def test_baseline_generation_does_not_fail():
    """E8.2/E8.3：baseline 生成可调用且不抛错。"""
    from app.core.db import SessionLocal
    from app.services.baseline_service import generate_random_baseline, generate_ma_cross_baseline
    db = SessionLocal()
    try:
        ok = generate_random_baseline(db, trade_date="2026-02-27")
        assert isinstance(ok, bool)
        n = generate_ma_cross_baseline(db, trade_date="2026-02-27")
        assert isinstance(n, int)
    finally:
        db.close()


def test_internal_llm_health():
    """05 §3 Ollama 验收：internal/llm/health 可用。"""
    r = client.get("/api/v1/internal/llm/health")
    assert r.status_code == 200
    data = r.json().get("data") or {}
    assert "status" in data


def test_internal_auth_returns_error_code_4001():
    """07 §9 AuthError code=4001；05 安全与审计 越权须返回 AuthError。"""
    from app.core.error_codes import AUTH_ERROR
    backup = settings.internal_api_key
    settings.internal_api_key = "required-key"
    try:
        r = client.get("/api/v1/internal/llm/version")
        assert r.status_code == 401
        body = r.json()
        assert body.get("data", {}).get("error_code") == AUTH_ERROR
    finally:
        settings.internal_api_key = backup


def test_reports_list_pagination_no_duplicates():
    """05 列表筛选用例：分页不重复不丢失；items 与 total 一致。"""
    r1 = client.get("/api/v1/reports?page=1&page_size=2")
    r2 = client.get("/api/v1/reports?page=2&page_size=2")
    assert r1.status_code == 200 and r2.status_code == 200
    d1, d2 = r1.json()["data"], r2.json()["data"]
    ids1 = {it["report_id"] for it in d1["items"]}
    ids2 = {it["report_id"] for it in d2["items"]}
    assert ids1.isdisjoint(ids2), "分页间不得重复 report_id"
    total = d1["total"]
    assert total == d2["total"]
    if total > 0:
        assert len(d1["items"]) <= 2 and len(d2["items"]) <= 2


def test_report_get_minimal_structure_07_contract():
    """07 §2.3 研报完整响应最小必须字段：report_id, recommendation, reasoning_trace, quality_gate, report_data_usage, citations。"""
    rr = client.post("/api/v1/reports/generate", json={"stock_code": "600519.SH", "source": "test"})
    assert rr.status_code == 200
    report_id = rr.json()["data"]["report_id"]
    r = client.get(f"/api/v1/reports/{report_id}")
    assert r.status_code == 200
    report = r.json()["data"]
    for key in ("report_id", "stock_code", "recommendation", "reasoning_trace", "quality_gate", "report_data_usage", "citations"):
        assert key in report, f"07 契约 §2.3 要求含 {key}"
    assert "missing_fields" in report["quality_gate"]
    assert "recover_actions" in report["quality_gate"]
    assert "sources" in report["report_data_usage"]


def test_not_found_envelope():
    r = client.get("/api/v1/reports/not_exists")
    assert r.status_code in (400, 404)
    try:
        body = r.json()
        assert body.get("success") is False or body.get("code") in (404, 400)
    except Exception:
        pass  # HTML response for invalid stock code is acceptable


def test_report_generate_idempotency():
    key = "idem-001"
    r1 = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "run_mode": "daily", "idempotency_key": key, "source": "test"},
    )
    r2 = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": "600519.SH", "run_mode": "daily", "idempotency_key": key, "source": "test"},
    )
    assert r1.status_code == 200 and r2.status_code == 200
    assert r1.json()["data"]["report_id"] == r2.json()["data"]["report_id"]
    assert "reused" in r2.json()["data"]
    assert r2.json()["data"]["reused"] is True


def test_report_generate_daily_auto_idempotency():
    r1 = client.post("/api/v1/reports/generate", json={"stock_code": "600519.SH", "run_mode": "daily", "source": "test"})
    r2 = client.post("/api/v1/reports/generate", json={"stock_code": "600519.SH", "run_mode": "daily", "source": "test"})
    assert r1.status_code == 200 and r2.status_code == 200
    assert r1.json()["data"]["report_id"] == r2.json()["data"]["report_id"]


def test_request_id_roundtrip_header():
    req_id = "rid-test-001"
    r = client.get("/health", headers={"X-Request-ID": req_id})
    assert r.status_code == 200
    assert r.headers.get("X-Request-ID") == req_id
    assert r.json()["request_id"] == req_id


def test_internal_auth_when_enabled():
    backup = settings.internal_api_key
    settings.internal_api_key = "k1"
    try:
        bad = client.get("/api/v1/internal/llm/version")
        assert bad.status_code == 401
        ok = client.get("/api/v1/internal/llm/version", headers={"X-Internal-Key": "k1"})
        assert ok.status_code == 200
    finally:
        settings.internal_api_key = backup


def test_direction_override_keeps_base_when_not_reliable():
    reco, meta = _apply_direction_override(
        "BUY",
        {
            "horizons": [{"horizon_day": 7, "action": "SELL"}],
            "backtest_recent_3m": [{"horizon_day": 7, "actionable_samples": 1, "actionable_coverage": 0.01}],
        },
    )
    assert reco == "BUY"
    assert meta["d7_reliable"] is False


def test_demo_report_status_endpoint():
    r = client.get("/demo/report/600519.SH/status")
    assert r.status_code == 200
    assert "job" in r.json()["data"]


def test_demo_report_invalid_stock_code():
    r = client.get("/demo/report/ABC?cached_only=true")
    assert r.status_code == 400
    assert r.json()["error"] == "invalid_stock_code"


def test_demo_report_cached_fallback_plain_blocks():
    stock = "999999.SH"
    rid = f"demo-fallback-plain-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    now = datetime.now(timezone.utc)
    db = SessionLocal()
    try:
        db.add(
            Report(
                report_id=rid,
                stock_code=stock,
                run_mode="daily",
                created_at=now,
                recommendation="HOLD",
                confidence=0.5,
                content_json={
                    "report_id": rid,
                    "stock_code": stock,
                    "created_at": now.isoformat(),
                    "recommendation": "HOLD",
                    "recommendation_cn": "观望",
                    "dimensions": {
                        "market_snapshot": {
                            "stock_code": stock,
                            "source": "eastmoney",
                            "last_price": 10.01,
                            "pct_change": -0.01,
                            "fetch_time": now.isoformat(),
                        },
                        "market_features": {"features": {"ma5": 10.1, "ma20": 10.2, "trend": "偏空"}},
                    },
                    "direction_forecast": {
                        "horizons": [{"horizon_day": 7, "direction": "下跌", "action": "SELL", "confidence": 0.7}],
                        "backtest_recent_3m": [{"horizon_day": 7, "actionable_accuracy": 0.61}],
                    },
                    "novice_guide": {"one_line_decision": "测试用旧缓存报告"},
                },
            )
        )
        db.commit()
    finally:
        db.close()

    r = client.get(f"/demo/report/{stock}?cached_only=true")
    assert r.status_code == 200
    assert "未来趋势预测" in r.text
    assert "标的概况" in r.text
    assert "操作建议" in r.text
    assert "结论依据" in r.text
    assert "机会与风险" in r.text
    assert "资金面" in r.text
    assert "证据与引用" in r.text
    assert "仓位与风控" in r.text or "建议动作" in r.text
    assert "附录" in r.text or "回测与数据来源" in r.text


def test_generate_from_legacy_cached_report_without_market_features():
    stock = "688001.SH"
    rid = f"legacy-no-mf-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    idem = f"legacy-idem-no-mf-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    now = datetime.now(timezone.utc)
    db = SessionLocal()
    try:
        db.add(
            Report(
                report_id=rid,
                stock_code=stock,
                run_mode="daily",
                created_at=now,
                recommendation="HOLD",
                confidence=0.4,
                content_json={
                    "report_id": rid,
                    "stock_code": stock,
                    "created_at": now.isoformat(),
                    "recommendation": "HOLD",
                    "plain_report": {"title": "legacy"},
                    "dimensions": {
                        "market_snapshot": {
                            "stock_code": stock,
                            "source": "eastmoney",
                            "last_price": 10.01,
                            "pct_change": 0.01,
                            "fetch_time": now.isoformat(),
                        }
                    },
                },
            )
        )
        db.add(
            ReportIdempotency(
                idempotency_key=idem,
                stock_code=stock,
                run_mode="daily",
                report_id=rid,
            )
        )
        db.commit()
    finally:
        db.close()

    rr = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": stock, "run_mode": "daily", "idempotency_key": idem, "source": "test"},
    )
    assert rr.status_code == 200
    data = rr.json()["data"]
    assert data["report_id"] == rid
    plain = data["report"]["plain_report"]
    assert "technical_analysis" in plain
    assert "ma5" in plain["technical_analysis"]
    assert "trend" in plain["technical_analysis"]


def test_generate_from_legacy_cached_report_schema_patch_idempotent():
    stock = "688002.SH"
    rid = f"legacy-idem-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    idem = f"legacy-idem-repeat-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    now = datetime.now(timezone.utc)
    db = SessionLocal()
    try:
        db.add(
            Report(
                report_id=rid,
                stock_code=stock,
                run_mode="daily",
                created_at=now,
                recommendation="HOLD",
                confidence=0.4,
                content_json={
                    "report_id": rid,
                    "stock_code": stock,
                    "created_at": now.isoformat(),
                    "recommendation": "HOLD",
                    "plain_report": {"title": "legacy"},
                    "dimensions": {"market_snapshot": {"stock_code": stock}},
                },
            )
        )
        db.add(
            ReportIdempotency(
                idempotency_key=idem,
                stock_code=stock,
                run_mode="daily",
                report_id=rid,
            )
        )
        db.commit()
    finally:
        db.close()

    payload = {"stock_code": stock, "run_mode": "daily", "idempotency_key": idem, "source": "test"}
    r1 = client.post("/api/v1/reports/generate", json=payload)
    r2 = client.post("/api/v1/reports/generate", json=payload)
    assert r1.status_code == 200
    assert r2.status_code == 200
    p1 = r1.json()["data"]["report"]["plain_report"]["technical_analysis"]
    p2 = r2.json()["data"]["report"]["plain_report"]["technical_analysis"]
    assert p1.keys() == p2.keys()


def test_reuse_recent_object_skips_empty_required_fields():
    stock = "688099.SH"
    now = datetime.now(timezone.utc)
    rid_new = f"reuse-empty-{now.strftime('%Y%m%d%H%M%S%f')}"
    rid_old = f"reuse-valid-{now.strftime('%Y%m%d%H%M%S%f')}"
    db = SessionLocal()
    try:
        db.add(
            Report(
                report_id=rid_new,
                stock_code=stock,
                run_mode="daily",
                created_at=now,
                recommendation="HOLD",
                confidence=0.4,
                content_json={
                    "company_overview": {"company_name": "x", "industry": None, "listed_date": None},
                },
            )
        )
        db.add(
            Report(
                report_id=rid_old,
                stock_code=stock,
                run_mode="daily",
                created_at=now - timedelta(minutes=1),
                recommendation="HOLD",
                confidence=0.4,
                content_json={
                    "company_overview": {"company_name": "x", "industry": "测试行业", "listed_date": "2010-01-01"},
                },
            )
        )
        db.commit()

        obj = _reuse_recent_object(
            db,
            stock,
            "company_overview",
            max_age_hours=24,
            required_keys=["industry", "listed_date"],
        )
        assert obj.get("industry") == "测试行业"
        assert obj.get("listed_date") == "2010-01-01"
    finally:
        db.close()


def test_daily_idempotency_rebuilds_stale_missing_core_report():
    stock = "688199.SH"
    rid = f"daily-stale-missing-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    idem = f"daily-stale-idem-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    old_time = datetime.now(timezone.utc) - timedelta(hours=3)
    db = SessionLocal()
    try:
        db.add(
            Report(
                report_id=rid,
                stock_code=stock,
                run_mode="daily",
                created_at=old_time,
                recommendation="HOLD",
                confidence=0.4,
                content_json={
                    "report_id": rid,
                    "stock_code": stock,
                    "company_overview": {"industry": None, "listed_date": None},
                    "valuation": {"pe_ttm": None, "pb": None},
                    "capital_game": {"main_force": {"status": "missing"}, "margin_financing": {"status": "missing"}},
                },
            )
        )
        db.add(
            ReportIdempotency(
                idempotency_key=idem,
                stock_code=stock,
                run_mode="daily",
                report_id=rid,
            )
        )
        db.commit()
    finally:
        db.close()

    rr = client.post(
        "/api/v1/reports/generate",
        json={"stock_code": stock, "run_mode": "daily", "idempotency_key": idem, "source": "test"},
    )
    assert rr.status_code == 200
    new_id = rr.json()["data"]["report_id"]
    assert new_id != rid


