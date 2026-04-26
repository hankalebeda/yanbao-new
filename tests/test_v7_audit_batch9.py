from html.parser import HTMLParser

from sqlalchemy import text

from tests.helpers_ssot import insert_report_bundle_ssot, insert_stock_master


class _PageParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements: list[dict] = []
        self.text_chunks: list[str] = []

    def handle_starttag(self, tag, attrs):
        self.elements.append({"tag": tag, "attrs": dict(attrs)})

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


def _parse_html(text: str) -> _PageParser:
    parser = _PageParser()
    parser.feed(text)
    return parser


def test_v7_invalid_stock_code_html_hides_internal_error_code(client):
    response = client.get("/report?stock_code=bad-code")

    dom = _parse_html(response.text)
    assert response.status_code == 400
    assert not dom.text_contains("invalid_stock_code")
    assert dom.text_contains("请输入正确的股票代码")


def test_v7_invalid_activation_token_renders_html_page(client):
    response = client.get("/auth/activate?token=bad-token", headers={"Accept": "text/html"})

    dom = _parse_html(response.text)
    assert response.status_code == 400
    assert "text/html" in response.headers["content-type"]
    assert dom.text_contains("激活链接已失效或无效")
    assert not dom.text_contains("INVALID_PAYLOAD")


def test_v7_canonical_report_not_ready_page_hides_internal_reason(client, db_session):
    insert_stock_master(db_session, stock_code="600519.SH", stock_name="贵州茅台")
    db_session.commit()

    response = client.get("/report/600519.SH")

    dom = _parse_html(response.text)
    assert response.status_code == 404
    assert not dom.text_contains("MANUAL_TRIGGER_REQUIRED")
    assert not dom.text_contains("GET 自动")
    assert dom.text_contains("可先返回研报列表查看已发布内容")
    assert "/reports" in dom.link_targets()


def test_v7_report_detail_html_hides_internal_skip_reason(client, create_user, db_session):
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="600888.SH",
        stock_name="测试股份",
        trade_instructions={
            "10k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
            "100k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
            "500k": {"status": "SKIPPED", "position_ratio": 0.0, "skip_reason": "LOW_CONFIDENCE_OR_NOT_BUY"},
        },
    )
    db_session.execute(
        text(
            """
            UPDATE report
            SET llm_fallback_level = 'failed',
                reasoning_chain_md = :reasoning_chain_md
            WHERE report_id = :report_id
            """
        ),
        {
            "report_id": report.report_id,
            "reasoning_chain_md": "## 分析过程（LLM降级，规则兜底）\nmarket_state=NEUTRAL\nstrategy_type=B",
        },
    )
    db_session.commit()

    user = create_user(
        email="v7-report-html@example.com",
        password="Password123",
        tier="Pro",
        email_verified=True,
    )
    login_response = client.post(
        "/auth/login",
        json={"email": user["user"].email, "password": user["password"]},
    )
    assert login_response.status_code == 200

    response = client.get(f"/reports/{report.report_id}")

    dom = _parse_html(response.text)
    assert response.status_code == 200
    assert dom.has_selector("main")
    assert not dom.text_contains("LOW_CONFIDENCE_OR_NOT_BUY")
    assert dom.text_contains("当前暂无明确买入信号")
    assert not dom.text_contains("策略 B")


def test_v7_404_page_has_no_joke_copy(client):
    response = client.get("/path-that-does-not-exist")

    dom = _parse_html(response.text)
    assert response.status_code == 404
    assert dom.has_selector(".error-page")
    assert not dom.text_contains("开个玩笑")
    assert dom.text_contains("页面不存在")
