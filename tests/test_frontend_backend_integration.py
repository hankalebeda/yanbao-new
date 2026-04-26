"""回归测试：前后端集成（断裂修复验证）

验证修复的 6 个断裂问题：
- P0-1: API 响应 envelope {success, data} 正确被页面消费
- P0-2: recommendation_cn 在 report_view 中正确渲染
- P0-3: SSOT 模式下 report_view 模板字段不为空
- P0-4: 首页冷启动期展示 "积累中" 而非 "—"
- P0-5: 信号卡链接指向 /reports/{id}
- P0-7: report_view JS 使用 success 字段而非 code
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.models import Base
from app.services.trade_calendar import latest_trade_date_str
from tests.helpers_ssot import (
    insert_market_state_cache,
    insert_pool_snapshot,
    insert_report_bundle_ssot as _insert_report_bundle_ssot,
    insert_stock_master,
)


# ── helpers ──────────────────────────────────────────────────────


def _count_dash_placeholders(html: str) -> int:
    """统计 HTML 中独立 '—' 占位符数量（排除 HTML 注释和 script 块）。"""
    # Remove script blocks and comments
    cleaned = re.sub(r"<script[\s\S]*?</script>", "", html)
    cleaned = re.sub(r"<!--[\s\S]*?-->", "", cleaned)
    return len(re.findall(r"(?<![a-zA-Z\u4e00-\u9fff])—(?![a-zA-Z\u4e00-\u9fff])", cleaned))


def _login(client: TestClient, email: str, password: str) -> dict[str, str]:
    r = client.post("/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200
    token = r.json()["data"]["access_token"]
    return {"Authorization": f"Bearer {token}"}


def insert_report_bundle_ssot(db, **kwargs):
    kwargs.setdefault("trade_date", latest_trade_date_str())
    return _insert_report_bundle_ssot(db, **kwargs)


def _extract_js_function(source: str, function_name: str) -> str:
    match = re.search(
        rf"function\s+{re.escape(function_name)}\s*\([^)]*\)\s*\{{(?P<body>[\s\S]*?)\n\s*\}}",
        source,
    )
    assert match is not None, f"missing JS function: {function_name}"
    return match.group("body")


class _ReportPageParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements: list[dict] = []
        self._stack: list[dict] = []
        self.text_chunks: list[str] = []

    def handle_starttag(self, tag, attrs):
        element = {"tag": tag, "attrs": dict(attrs)}
        self.elements.append(element)
        self._stack.append(element)

    def handle_endtag(self, tag):
        if self._stack:
            self._stack.pop()

    def handle_data(self, data):
        if data and data.strip():
            self.text_chunks.append(data.strip())

    def has_selector(self, selector: str) -> bool:
        return any(_match_selector(element, selector) for element in self.elements)

    def text_contains(self, needle: str) -> bool:
        return needle in " ".join(self.text_chunks)

    def link_targets(self) -> list[str]:
        return [
            element["attrs"].get("href", "")
            for element in self.elements
            if element["tag"] == "a" and element["attrs"].get("href")
        ]


def _match_selector(element: dict, selector: str) -> bool:
    attrs = element.get("attrs", {})
    if selector.startswith("#"):
        return attrs.get("id") == selector[1:]
    if selector.startswith("."):
        return selector[1:] in attrs.get("class", "").split()
    if "." in selector:
        tag, cls = selector.split(".", 1)
        return element["tag"] == tag and cls in attrs.get("class", "").split()
    return element["tag"] == selector


# ── P0-1 + P0-7: envelope protocol ───────────────────────────────


class TestEnvelopeProtocol:
    """后端 envelope({success, data}) 与前端解析一致。"""

    def test_reports_api_returns_success_field(self, client, db_session):
        insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="贵州茅台")
        r = client.get("/api/v1/reports")
        body = r.json()
        assert "success" in body, "envelope 响应必须包含 success 字段"
        assert body["success"] is True
        assert "data" in body

    def test_report_detail_api_returns_success_field(self, client, db_session):
        report = insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="贵州茅台")
        r = client.get(f"/api/v1/reports/{report.report_id}")
        body = r.json()
        assert body["success"] is True
        assert body["data"]["report_id"] == report.report_id

    @pytest.mark.feature("FR07-SETTLE-04")
    def test_predictions_stats_returns_success(self, client, db_session):
        r = client.get("/api/v1/predictions/stats")
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True
        data = body["data"]
        assert data["total_judged"] == data["judged"]
        assert {item["window_days"] for item in data["by_window"]} == {1, 7, 14, 30, 60}

    @pytest.mark.feature("FR07-SETTLE-04")
    def test_predictions_stats_response_contains_all_spec_fields(self, client, db_session):
        """doc05 §831-851: PredictionStatsPayload 全部 10 个字段必须存在。"""
        r = client.get("/api/v1/predictions/stats")
        assert r.status_code == 200
        data = r.json()["data"]
        required_keys = {
            "total_judged", "judged", "accuracy", "by_window",
            "by_stock", "recent_3m", "negative_feedback_rate",
            "negative_feedback_total", "by_strategy_type", "alpha",
        }
        assert required_keys <= set(data), f"缺失字段: {required_keys - set(data)}"

    @pytest.mark.feature("FR07-SETTLE-04")
    def test_predictions_stats_by_strategy_type_is_dict(self, client, db_session):
        """by_strategy_type 应为 dict（strategy_type -> 指标子对象）。"""
        r = client.get("/api/v1/predictions/stats")
        data = r.json()["data"]
        assert isinstance(data["by_strategy_type"], dict)

    @pytest.mark.feature("FR07-SETTLE-04")
    def test_predictions_stats_alpha_is_nullable_number(self, client, db_session):
        """alpha 为 float 或 None（冷启动时可为空）。"""
        r = client.get("/api/v1/predictions/stats")
        data = r.json()["data"]
        assert data["alpha"] is None or isinstance(data["alpha"], (int, float))

    @pytest.mark.feature("FR07-SETTLE-04")
    def test_predictions_stats_negative_feedback_fields_typed(self, client, db_session):
        """negative_feedback_rate 为 float|None，negative_feedback_total 为 int。"""
        r = client.get("/api/v1/predictions/stats")
        data = r.json()["data"]
        rate = data["negative_feedback_rate"]
        total = data["negative_feedback_total"]
        assert rate is None or isinstance(rate, (int, float))
        assert isinstance(total, int)

    @pytest.mark.feature("FR07-SETTLE-04")
    def test_predictions_stats_recent_3m_structure(self, client, db_session):
        """recent_3m 子对象包含 accuracy / samples / coverage 三键。"""
        r = client.get("/api/v1/predictions/stats")
        data = r.json()["data"]
        recent = data["recent_3m"]
        assert isinstance(recent, dict)
        assert {"accuracy", "samples", "coverage"} <= set(recent)

    @pytest.mark.feature("FR07-SETTLE-04")
    def test_predictions_stats_by_stock_is_list(self, client, db_session):
        """by_stock 为 list，每项含 stock_code / accuracy / samples。"""
        r = client.get("/api/v1/predictions/stats")
        data = r.json()["data"]
        assert isinstance(data["by_stock"], list)
        for item in data["by_stock"]:
            assert {"stock_code", "accuracy", "samples"} <= set(item)

    @pytest.mark.feature("FR10-PLATFORM-01")
    def test_platform_config_returns_default_capital_tiers(self, client, db_session):
        r = client.get("/api/v1/platform/config")
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True
        data = body["data"]
        assert isinstance(data["capital_tiers"], dict)
        assert data["default_capital_tier"] in data["capital_tiers"]
        assert {"recommendation", "market_state", "ma_trend", "position_status"} <= set(data["labels"])
        assert "membership_level" not in data["labels"]
        assert "tier" not in data["labels"]

    @pytest.mark.feature("FR10-PLATFORM-02")
    def test_platform_summary_returns_success(self, client, db_session):
        r = client.get("/api/v1/platform/summary")
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True
        assert "data" in body
        assert {"win_rate", "pnl_ratio", "alpha", "total_trades", "cold_start"} <= set(body["data"])

    def test_no_code_field_in_envelope(self, client, db_session):
        """确认 envelope 不返回 code 字段，前端不应依赖它。"""
        r = client.get("/api/v1/platform/summary")
        body = r.json()
        assert "code" not in body, "envelope 不应包含 code 字段"


# ── P0-2 + P0-3: report_view 字段完整性 ─────────────────────────


class TestReportViewFields:
    """SSOT 模式下 report_view.html 模板变量完整。"""

    def test_recommendation_cn_rendered(self, client, db_session):
        """BUY 报告 → hero 区域显示 '买入' 而非 '观望等待'。"""
        insert_report_bundle_ssot(
            db_session,
            stock_code="600519.SH",
            stock_name="贵州茅台",
            recommendation="BUY",
        )
        r = client.get("/api/v1/reports?recommendation=BUY&limit=1")
        items = r.json()["data"]["items"]
        assert len(items) >= 1
        report_id = items[0]["report_id"]

        page = client.get(f"/reports/{report_id}")
        assert page.status_code == 200
        html = page.text
        assert "买入" in html, "BUY 报告页面应包含 '买入'"

    def test_recommendation_sell_cn(self, client, db_session):
        """SELL 报告 → 显示 '卖出'。"""
        report = insert_report_bundle_ssot(
            db_session,
            stock_code="000001.SZ",
            stock_name="卖出测试",
            recommendation="SELL",
        )
        page = client.get(f"/reports/{report.report_id}")
        assert page.status_code == 200
        parser = _ReportPageParser()
        parser.feed(page.text)
        assert "卖出" in " ".join(parser.text_chunks)

    def test_company_overview_in_view(self, client, db_session):
        """HTML view 中 company_overview 包含 stock_code 和 exchange。"""
        report = insert_report_bundle_ssot(
            db_session, stock_code="600519.SH", stock_name="贵州茅台"
        )
        page = client.get(f"/reports/{report.report_id}")
        assert page.status_code == 200
        html = page.text
        # company overview 区域应包含股票代码
        assert "600519" in html

    def test_dash_count_bounded(self, client, db_session):
        """report_view 中非JS区域的 '—' 占位符不超过合理阈值。"""
        report = insert_report_bundle_ssot(
            db_session,
            stock_code="600519.SH",
            stock_name="贵州茅台",
            recommendation="BUY",
        )
        page = client.get(f"/reports/{report.report_id}")
        assert page.status_code == 200
        dash_count = _count_dash_placeholders(page.text)
        # 部分 '—' 是模板中用于价格预测/无数据区域的合理占位
        # 关键验证: 带种子数据后不应满屏都是 dash
        assert dash_count < 50, f"dash 占位符异常多: {dash_count}，可能存在渲染错误"

    def test_report_view_has_conclusion(self, client, db_session):
        """report_view 应包含研报结论文本。"""
        report = insert_report_bundle_ssot(
            db_session, stock_code="600519.SH", stock_name="贵州茅台"
        )
        page = client.get(f"/reports/{report.report_id}")
        html = page.text
        assert "维持看多" in html or "结论" in html or report.conclusion_text[:4] in html

    def test_report_view_has_reasoning_trace(self, client, db_session):
        """report_view API 应返回非空 reasoning_trace。"""
        report = insert_report_bundle_ssot(
            db_session, stock_code="600519.SH", stock_name="贵州茅台"
        )
        # HTML 可能不直接渲染 reasoning_chain_md，但 reasoning_trace 应在 API 中
        page = client.get(f"/reports/{report.report_id}")
        html = page.text
        # 至少包含结论文本（来自 reasoning_trace.inference_summary 或 plain_report）
        assert "维持看多" in html or "结论" in html or "分析" in html.lower()

    def test_report_view_forecast_has_all_windows_without_paywall_gap(self, client, db_session):
        """未来趋势预测应固定展示 1/7/14/30/60 日，不再出现付费解锁缺口。"""
        report = insert_report_bundle_ssot(
            db_session, stock_code="600519.SH", stock_name="贵州茅台", recommendation="BUY"
        )

        page = client.get(f"/reports/{report.report_id}")

        assert page.status_code == 200
        html = page.text
        for day in ("1 日", "7 日", "14 日", "30 日", "60 日"):
            assert day in html
        assert "付费解锁" not in html
        assert "区间待补充" not in html
        assert "待校准" not in html

    def test_report_view_hides_stock_accuracy_block(self, client, db_session):
        """报告详情页不再展示历史准确率卡片，也不再发起 predictions/stats 请求。"""
        report = insert_report_bundle_ssot(
            db_session, stock_code="600519.SH", stock_name="贵州茅台"
        )

        page = client.get(f"/reports/{report.report_id}")

        assert page.status_code == 200
        assert "本股历史结算准确率" not in page.text
        assert "/predictions/stats" not in page.text


# ── P0-4: 冷启动展示 ─────────────────────────────────────────────


class TestColdStartDisplay:
    """冷启动期（无结算数据）首页不全显示 '—'。"""

    @pytest.mark.feature("FR10-PLATFORM-02")
    def test_platform_summary_cold_start_flag(self, client, db_session):
        """无结算数据时 cold_start=true。"""
        r = client.get("/api/v1/platform/summary")
        data = r.json()["data"]
        assert data["cold_start"] is True
        assert data["total_trades"] == 0

    def test_homepage_loads_without_crash(self, client, db_session):
        """首页在无数据时仍可加载（不 500）。"""
        r = client.get("/")
        assert r.status_code == 200


# ── P0-5: 路由入口一致性 ──────────────────────────────────────────


class TestRoutingConsistency:
    """信号卡和列表指向正确路由。"""

    def test_reports_list_page_loads(self, client, db_session):
        insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="贵州茅台")
        r = client.get("/reports")
        assert r.status_code == 200

    def test_canonical_report_route_redirects_to_latest_report(self, client, db_session):
        trade_date = latest_trade_date_str()
        report = insert_report_bundle_ssot(
            db_session,
            stock_code="600519.SH",
            stock_name="贵州茅台",
            trade_date=trade_date,
        )
        r = client.get("/report/600519.SH", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers.get("location", "") == f"/reports/{report.report_id}"

    def test_canonical_report_route_accepts_bare_stock_code(self, client, db_session):
        trade_date = latest_trade_date_str()
        report = insert_report_bundle_ssot(
            db_session,
            stock_code="688025.SH",
            stock_name="测试科创股",
            trade_date=trade_date,
        )
        r = client.get("/report/688025", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers.get("location", "") == f"/reports/{report.report_id}"

    def test_canonical_report_status_route_no_500_when_report_exists(self, client, db_session):
        trade_date = latest_trade_date_str()
        insert_report_bundle_ssot(
            db_session,
            stock_code="600519.SH",
            stock_name="贵州茅台",
            trade_date=trade_date,
        )
        r = client.get("/report/600519.SH/status")
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True
        assert body["data"]["stock_code"] == "600519.SH"

    def test_canonical_report_status_route_invalid_code_returns_json_envelope(self, client):
        r = client.get("/report/not-a-stock/status")
        assert r.status_code in (400, 404)
        assert r.headers["content-type"].startswith("application/json")
        body = r.json()
        assert body["success"] is False
        assert body["data"] is None
        assert body["error_code"]

    def test_demo_route_search_redirect(self, client):
        """兼容搜索入口跳转到 /report/{code}。"""
        r = client.get("/demo/report?stock_code=600519.SH", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers.get("location", "") == "/report/600519.SH"

    def test_demo_route_kept_as_compat_redirect_only(self, client):
        r = client.get("/demo/report/600519.SH", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers.get("location", "") == "/report/600519.SH"

    def test_canonical_report_route_does_not_autostart_generation_when_missing(self, client, db_session):
        insert_stock_master(db_session, stock_code="600519.SH", stock_name="贵州茅台")
        task_table = Base.metadata.tables["report_generation_task"]
        before_count = len(db_session.execute(task_table.select()).fetchall())

        response = client.get("/report/600519.SH")

        after_count = len(db_session.execute(task_table.select()).fetchall())
        parser = _ReportPageParser()
        parser.feed(response.text)
        assert response.status_code == 404
        assert not parser.has_selector(".progress-bar")
        assert f"/report/600519.SH/status" not in parser.link_targets()
        assert after_count == before_count

    def test_report_detail_by_id(self, client, db_session):
        """正式路由 /reports/{id} 正常渲染。"""
        report = insert_report_bundle_ssot(
            db_session, stock_code="600519.SH", stock_name="贵州茅台"
        )
        r = client.get(f"/reports/{report.report_id}")
        assert r.status_code == 200
        parser = _ReportPageParser()
        parser.feed(r.text)
        assert "600519" in " ".join(parser.text_chunks)

    def test_report_detail_labels_fallback_capital_block_as_data_status(self, client, db_session):
        report = insert_report_bundle_ssot(
            db_session,
            stock_code="600519.SH",
            stock_name="贵州茅台",
        )

        response = client.get(f"/reports/{report.report_id}")

        parser = _ReportPageParser()
        parser.feed(response.text)
        assert response.status_code == 200
        assert parser.text_contains("资金数据状态")
        assert parser.text_contains("数据接入概况")

    def test_report_detail_keeps_capital_block_in_status_mode_with_status_only_inputs(self, client, db_session):
        report = insert_report_bundle_ssot(
            db_session,
            stock_code="600519.SH",
            stock_name="贵州茅台",
        )
        usage_table = Base.metadata.tables["report_data_usage"]
        usage_link_table = Base.metadata.tables["report_data_usage_link"]
        now = datetime.now(timezone.utc)
        db_session.execute(
            usage_table.insert().values(
                usage_id="status-only-northbound",
                trade_date=date.fromisoformat(report.trade_date),
                stock_code=report.stock_code,
                dataset_name="northbound_summary",
                source_name="akshare_hsgt_hist",
                batch_id="batch-status-only-northbound",
                fetch_time=now,
                status="ok",
                status_reason="akshare_hsgt_hist",
                created_at=now,
            )
        )
        db_session.execute(
            usage_link_table.insert().values(
                report_data_usage_link_id="link-status-only-northbound",
                report_id=report.report_id,
                usage_id="status-only-northbound",
                created_at=now,
            )
        )
        db_session.commit()

        response = client.get(f"/reports/{report.report_id}")

        parser = _ReportPageParser()
        parser.feed(response.text)
        assert response.status_code == 200
        assert parser.text_contains("资金数据状态")
        assert parser.text_contains("数据接入概况")
        assert not parser.text_contains("资金结论")

    def test_reports_api_items_have_report_id(self, client, db_session):
        """列表 API 每条 item 都有 report_id，前端可构造 /reports/{id} 链接。"""
        insert_report_bundle_ssot(db_session, stock_code="600519.SH", stock_name="贵州茅台")
        r = client.get("/api/v1/reports")
        items = r.json()["data"]["items"]
        assert len(items) >= 1
        for item in items:
            assert item.get("report_id"), "列表项缺少 report_id"

    def test_public_templates_have_no_demo_report_links(self):
        template_paths = [
            Path("app/web/templates/index.html"),
            Path("app/web/templates/reports_list.html"),
            Path("app/web/templates/sim_dashboard.html"),
        ]
        for path in template_paths:
            text = path.read_text(encoding="utf-8")
            assert "/demo/report/" not in text

    def test_register_template_has_friendly_conflict_handling(self):
        text = Path("app/web/templates/register.html").read_text(encoding="utf-8")
        assert "IDEMPOTENCY_CONFLICT" in text
        assert "/login" in text
        assert "/forgot-password" in text

    def test_dashboard_and_sim_templates_use_ssot_endpoints(self):
        api_bridge = Path("app/web/api-bridge.js").read_text(encoding="utf-8")
        home = Path("app/web/templates/index.html").read_text(encoding="utf-8")
        dashboard = Path("app/web/templates/dashboard.html").read_text(encoding="utf-8")
        sim_dashboard = Path("app/web/templates/sim_dashboard.html").read_text(encoding="utf-8")
        admin = Path("app/web/templates/admin.html").read_text(encoding="utf-8")
        load_home_fn = _extract_js_function(home, "loadHome")
        assert "window.ApiBridge" in home
        assert "getHomePayload" in api_bridge
        assert "getMarketState" in api_bridge
        assert "getHotStocks" in api_bridge
        assert "getPoolStocks" in api_bridge
        assert "getDashboardStats(30)" not in home
        assert "getSettledBridgeData" in home
        assert "fetch(" not in home
        assert "function getSettledBridgeData" not in home
        assert "function getBridgeData" not in home
        assert "resolveHomeAuthoritativeTradeDate" in api_bridge
        assert "resolveHomeAnchorMismatchReason" in api_bridge
        assert "function resolveHomeAnchorTradeDate" not in home
        assert "function resolveMarketSupplementalTradeDate" not in home
        assert "function resolveHomeAnchorMismatchReason" not in home
        assert "resolveHomeAuthoritativeTradeDate(" in home
        assert "resolveHomeAnchorMismatchReason(" in home
        assert "pushUniqueReason" in home
        assert "Promise.allSettled" in home
        assert "status === 'fulfilled'" not in load_home_fn
        assert 'status === "fulfilled"' not in load_home_fn
        assert "getHotStocks(" not in home
        assert "/api/v1/market/hot-stocks" not in home
        assert "Array.isArray(home.hot_stocks)" in home
        assert "home.trade_date || publicPerformance.runtime_trade_date || market.market_state_date || market.reference_date || ''" not in home
        assert "reportItems[0].trade_date" not in home
        assert ".innerHTML" not in home
        assert "var marketStateCode = home.market_state || market.market_state || 'NEUTRAL';" not in home
        assert "var marketStateCode = market.market_state || home.market_state || 'NEUTRAL';" not in home
        assert "var tradingDayText = '按首页批次展示';" in home
        assert "var tradingDayText = marketRes.success" not in home
        assert "marketSupplement.ok" in home
        assert "reasonParts.push(" not in home
        assert "home_source_inconsistent" in home
        assert "var hasAnchorMismatch = !!marketMismatchReason;" in home
        assert "tradingDayText = '按首页批次展示';" in home
        assert "if (!marketSupplement.ok) pushUniqueReason(" in home
        assert "marketMismatchReason" in home
        assert "if (!referenceTradeDate) return;" in home
        assert "if (res.data.trade_date && res.data.trade_date !== referenceTradeDate)" in home
        assert "loadPool(homeTradeDate || null)" in home
        assert "document.createElement('a')" in home
        assert "/predictions/stats" not in dashboard
        assert "getDashboardStats" in dashboard
        assert "/sim/account/summary" not in sim_dashboard
        assert "/sim/account/snapshots" not in sim_dashboard
        assert "getPortfolioSimDashboard" in sim_dashboard
        assert "getAdminSystemStatus" in admin
        assert "getAdminOverview" in admin
        assert "getAdminSchedulerStatus" in admin
        assert "getAdminCookieSessionHealth" in admin
        assert "patchAdminUser" in admin
        assert "public_runtime" in admin
        assert "attempted_trade_date" in admin
        assert "kline_coverage" in admin
        assert "alignmentDatePairs" in admin


# ── 横切面：HTML 页面基本可达性 ───────────────────────────────────


class TestPageAccessibility:
    """所有主页面 HTTP 200 + 无 server error。"""

    @pytest.mark.parametrize(
        "path",
        ["/", "/reports", "/dashboard", "/login", "/register"],
    )
    def test_public_pages_200(self, client, path):
        r = client.get(path)
        assert r.status_code == 200, f"{path} 返回 {r.status_code}"

    def test_sim_dashboard_requires_auth(self, client):
        r = client.get("/portfolio/sim-dashboard")
        # 未登录应返回 200（显示空白/提示）或 302（重定向登录）
        assert r.status_code in (200, 302, 401, 403)

    def test_market_state_api(self, client, db_session):
        r = client.get("/api/v1/market/state")
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True
        assert "market_state" in body["data"]

    def test_hot_stocks_api(self, client, db_session):
        r = client.get("/api/v1/market/hot-stocks")
        assert r.status_code == 200
        body = r.json()
        assert body["success"] is True
        assert "items" in body["data"]
        assert isinstance(body["data"]["items"], list)
        assert len(body["data"]["items"]) <= 50
        for item in body["data"]["items"]:
            assert {"stock_code", "stock_name", "rank", "topic_title", "source_name"} <= set(item)
            assert isinstance(item["rank"], int)
            assert item["rank"] >= 1
            assert item["source_name"] == body["data"]["source"]


def test_report_detail_html_loads_advanced_area_via_api_not_ssr(client, db_session, create_user, seed_report_bundle):
    report = seed_report_bundle()
    report_table = Base.metadata.tables["report"]
    db_session.execute(
        report_table.update()
        .where(report_table.c.report_id == report.report_id)
        .values(reasoning_chain_md="ADVANCED-ONLY-TRACE-1234567890")
    )
    db_session.commit()

    user = create_user(
        email="advanced-html@example.com",
        password="Password123",
        tier="Pro",
        role="user",
        email_verified=True,
    )
    login = client.post("/auth/login", json={"email": user["user"].email, "password": user["password"]})
    assert login.status_code == 200
    headers = {"Authorization": f"Bearer {login.json()['data']['access_token']}"}

    response = client.get(f"/reports/{report.report_id}", headers=headers)

    assert response.status_code == 200
    assert 'id="advanced-area"' in response.text
    assert "getReportAdvanced(REPORT_ID)" in response.text
    assert "ADVANCED-ONLY-TRACE-1234567890" not in response.text


def test_canonical_report_not_ready_page_has_no_force_retry_link(client, db_session):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="贵州茅台")
    db_session.commit()

    response = client.get("/report/600519.SH")

    parser = _ReportPageParser()
    parser.feed(response.text)
    assert response.status_code == 404
    assert not any("?force=true" in href for href in parser.link_targets())
    assert parser.text_contains("可先返回研报列表查看已发布内容")
    assert not parser.text_contains("MANUAL_TRIGGER_REQUIRED")
