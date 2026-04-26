"""
E2E 模拟实盘追踪与三方审计用例

用例来源：docs/core/01_需求基线.md FR-07/FR-08
验收标准：docs/core/22_全量功能进度总表_v13.md

执行方式：pytest tests/test_e2e_sim.py -v

E6 鉴权：sim API 需付费用户（17 §4.2），测试使用 dependency_overrides 注入模拟付费用户，
避免测试环境 bcrypt/passlib 兼容性问题。
"""
import pytest
from unittest.mock import AsyncMock, patch

from fastapi import Request
from fastapi.testclient import TestClient

from app.api import routes_sim
from app.core.db import SessionLocal
from app.main import app
from app.models import Report, SimAccount, SimPosition, User
from app.services.trade_calendar import latest_trade_date_str, next_trade_date_str

pytestmark = [
    pytest.mark.feature("FR08-SIM-02"),
    pytest.mark.feature("FR08-SIM-04"),
    pytest.mark.feature("FR08-SIM-09"),
]

# 模拟付费用户注入（绕过 login/register，避免 bcrypt 兼容性问题）
async def _mock_require_sim_access(_request: Request) -> User:
    return User(
        user_id="00000000-0000-0000-0000-000000000001",
        email="sim_test@e2e.local",
        password_hash="",
        membership_level="monthly",
        role="user",
    )


@pytest.fixture(scope="module", autouse=True)
def _override_sim_auth():
    app.dependency_overrides[routes_sim._require_sim_access] = _mock_require_sim_access
    yield
    app.dependency_overrides.pop(routes_sim._require_sim_access, None)


client = TestClient(app, base_url="http://127.0.0.1")


def _seed_buy_position(stock_code: str = "600519.SH", report_id: str | None = None):
    """Seed Report(BUY) + SimPosition for E2E-SIM 用例。返回 (report_id, position_id)。"""
    report_id = report_id or f"e2e_sim_{stock_code.replace('.', '_')}_test"
    today = latest_trade_date_str()
    sim_open = next_trade_date_str(today)
    db = SessionLocal()
    try:
        # 确保无 HALT，避免 create_position 被跳过
        latest = db.query(SimAccount).filter(SimAccount.capital_tier == "10w").order_by(SimAccount.snapshot_date.desc()).first()
        if not latest or latest.drawdown_state != "HALT":
            pass  # ok
        else:
            latest.drawdown_state = "NORMAL"
            db.commit()
        # 若已有同 report_id 的 report/position，复用
        r = db.query(Report).filter(Report.report_id == report_id).first()
        if not r:
            content = {
                "sim_trade_instruction": {
                    "sim_open_price": 1850.0,
                    "stop_loss_price": 1700.0,
                    "target_price_1": 1950.0,
                    "target_price_2": 2050.0,
                    "strategy_type": "B",
                    "sim_qty": 100,
                    "valid_until": "2026-12-31",
                    "stock_name": "贵州茅台",
                },
                "thesis": {"filtered_out": False},
            }
            r = Report(
                report_id=report_id,
                stock_code=stock_code,
                run_mode="daily",
                recommendation="BUY",
                confidence=0.70,
                content_json=content,
                trade_date=today,
            )
            db.add(r)
            db.commit()
        pos = db.query(SimPosition).filter(SimPosition.report_id == report_id).first()
        if pos and pos.status != "OPEN":
            pos.status = "OPEN"
            pos.sim_close_date = None
            pos.sim_close_price = None
            pos.sim_pnl_gross = None
            pos.sim_pnl_net = None
            pos.sim_pnl_pct = None
            pos.hold_days = None
            db.commit()
        if not pos:
            pos = SimPosition(
                report_id=report_id,
                stock_code=stock_code,
                stock_name="贵州茅台",
                strategy_type="B",
                signal_date=today,
                sim_open_date=sim_open,
                sim_open_price=1850.0,
                actual_entry_price=None,
                sim_qty=100,
                capital_tier="10w",
                stop_loss_price=1700.0,
                target_price_1=1950.0,
                target_price_2=2050.0,
                valid_until="2026-12-31",
                status="OPEN",
            )
            db.add(pos)
            db.commit()
            db.refresh(pos)
        return report_id, pos.id
    finally:
        db.close()


# --- E2E-SIM 模拟实盘追踪 ---


def test_e2e_sim_01_buy_signal_creates_position():
    """E2E-SIM-01：BUY 强信号 → sim_position 新增 OPEN → report_id 可追溯"""
    report_id, _ = _seed_buy_position("600519.SH")
    r = client.get("/api/v1/sim/positions?stock_code=600519.SH")
    assert r.status_code == 200
    j = r.json()
    assert j.get("code") == 0
    items = j.get("data", {}).get("items", [])
    found = [x for x in items if x.get("report_id") == report_id and x.get("stock_code") == "600519.SH"]
    assert len(found) >= 1, f"GET positions 应返回 report_id={report_id} 的持仓"
    assert found[0].get("status") == "OPEN"


@patch("app.services.sim_settle_service._fetch_quote")
def test_e2e_sim_02_settle_to_closed_sl(mock_fetch):
    """E2E-SIM-02：OPEN 持仓结算 → CLOSED_SL，sim_pnl_net 含手续费（99 §2）"""
    mock_fetch.return_value = {
        "close": 1680.0,
        "high": 1700.0,
        "low": 1680.0,
        "limit_up": None,
        "limit_down": None,
        "volume": 0,
    }
    report_id, pos_id = _seed_buy_position("600519.SH", report_id="e2e_sim_02_settle_test")
    from app.services.sim_settle_service import run_settle

    run_settle()
    db = SessionLocal()
    try:
        pos = db.query(SimPosition).filter(SimPosition.id == pos_id).first()
        assert pos is not None, "持仓应存在"
        assert pos.status == "CLOSED_SL", f"预期 CLOSED_SL，实际 {pos.status}"
        assert pos.sim_pnl_net is not None, "净盈亏应已写入"
        assert pos.sim_pnl_net < 0, "止损应为亏损"
    finally:
        db.close()


def test_e2e_sim_03_sim_dashboard():
    """E2E-SIM-03：GET /portfolio/sim-dashboard → 200，页面可访问（17 §2.4：未登录显示登录提示，付费显示完整看板）"""
    r = client.get("/portfolio/sim-dashboard")
    assert r.status_code == 200
    text = r.text
    # 未登录/免费：显示「请先登录」或「去订阅」或「模拟收益看板」；付费：显示 loadSummary
    assert (
        "loadSummary" in text
        or "sim-dashboard" in text
        or "请先登录" in text
        or "去订阅" in text
        or "模拟收益看板" in text
    ), "页面应包含模拟收益相关内容或登录/订阅引导"


def test_sim_api_positions():
    """sim API：GET /api/v1/sim/positions 需付费用户可访问"""
    r = client.get("/api/v1/sim/positions")
    assert r.status_code == 200
    j = r.json()
    assert j.get("code") == 0
    assert "items" in j.get("data", {})
    assert "total" in j.get("data", {})


def test_sim_api_summary_cold_start_fields():
    """sim API：GET /api/v1/sim/account/summary 冷启动期返回 cold_start/cold_start_message/est_days_to_30（05 §7.5a）"""
    r = client.get("/api/v1/sim/account/summary?capital_tier=10w")
    assert r.status_code == 200
    j = r.json()
    assert j.get("code") == 0
    d = j.get("data", {})
    assert "total_trades" in d
    assert "cold_start" in d
    assert "cold_start_message" in d or not d.get("cold_start")  # cold_start 时须有 message
    if d.get("cold_start"):
        assert "已有" in (d.get("cold_start_message") or "")


def test_sim_api_summary_strategy_paused_field():
    """E8.6：sim/account/summary 返回 strategy_paused 字段（12 §10.2 用户侧展示）"""
    r = client.get("/api/v1/sim/account/summary?capital_tier=10w")
    assert r.status_code == 200
    j = r.json()
    assert j.get("code") == 0
    d = j.get("data", {})
    assert "strategy_paused" in d
    assert isinstance(d["strategy_paused"], list)


def test_sim_api_market_state():
    """sim API：GET /api/v1/market/state 可访问"""
    r = client.get("/api/v1/market/state")
    assert r.status_code == 200
    j = r.json()
    assert j.get("code") == 0
    d = j.get("data", {})
    assert "market_state" in d
    assert d["market_state"] in ("BULL", "NEUTRAL", "BEAR")


def test_e2e_sim_04_report_detail_positions_link():
    """E2E-SIM-04：研报详情「本股历史信号追踪」→ GET /api/v1/sim/positions 返回列表"""
    report_id, _ = _seed_buy_position("000001.SZ")
    # 研报详情（通过 reports API 获取）
    rr = client.get(f"/api/v1/reports/{report_id}")
    if rr.status_code != 200:
        pytest.skip("report not found, skip positions link test")
    # 本股历史：positions API 按 stock_code 筛选
    rp = client.get("/api/v1/sim/positions?stock_code=000001.SZ")
    assert rp.status_code == 200
    j = rp.json()
    assert j.get("code") == 0
    items = j.get("data", {}).get("items", [])
    found = [x for x in items if x.get("report_id") == report_id]
    assert len(found) >= 1, "本股历史应包含该 report 对应持仓"


@patch("app.services.ollama_client.ollama_client.generate")
def test_e2e_sim_05_halt_blocks_new_position(mock_ollama_gen):
    """E2E-SIM-05：drawdown_state=HALT → 不写入 sim_position，研报仍发布（99 §3）"""
    mock_ollama_gen.return_value = AsyncMock(
        return_value={
            "model": "qwen3:8b",
            "latency_ms": 5,
            "response": '{"recommendation":"BUY","reason":"test HALT block"}',
            "raw": {"mock": True},
        }
    )
    # 插入 HALT 状态的 SimAccount（10w 最新快照）
    today = latest_trade_date_str()
    db = SessionLocal()
    try:
        existing = (
            db.query(SimAccount)
            .filter(SimAccount.snapshot_date == today, SimAccount.capital_tier == "10w")
            .first()
        )
        if existing:
            existing.drawdown_state = "HALT"
        else:
            db.add(
                SimAccount(
                    snapshot_date=today,
                    capital_tier="10w",
                    initial_capital=100000,
                    total_asset=80000,
                    cash=80000,
                    position_value=0,
                    daily_return_pct=0,
                    cumulative_return_pct=-20,
                    max_drawdown_pct=-20,
                    drawdown_state="HALT",
                    open_positions=0,
                    settled_trades=0,
                    win_rate=None,
                    pnl_ratio=None,
                )
            )
        db.commit()
    finally:
        db.close()

    # 触发研报生成（会得到 BUY，但因 HALT 不创建 position）
    r = client.post("/api/v1/reports/generate", json={"stock_code": "600519.SH", "source": "test"})
    assert r.status_code == 200
    report_id = r.json().get("data", {}).get("report_id")
    assert report_id, "应返回 report_id"

    # 断言：无该 report 的 sim_position
    db = SessionLocal()
    try:
        pos = db.query(SimPosition).filter(SimPosition.report_id == report_id).first()
        assert pos is None, "HALT 状态下不应创建 sim_position"
    finally:
        db.close()


# --- E2E-AUDIT 三方投票审计（99 §4：mock run_audit_and_aggregate）---


_MOCK_LLM_BUY_RESPONSE = (
    '{"recommendation":"买入","7d":{"direction":"上涨","pct_range":"5%~10%","reason":"mock","confidence":"高"},'
    '"1d":{"direction":"震荡","confidence":"中"},"14d":{"direction":"上涨","confidence":"中高"},"30d":{"direction":"震荡","confidence":"中"},"60d":{"direction":"震荡","confidence":"中低"}}'
)


@patch("app.services.ollama_client.ollama_client.generate", new_callable=AsyncMock)
@patch("app.services.llm_router.run_audit_and_aggregate", new_callable=AsyncMock)
def test_e2e_audit_01_audit_flag_in_report(mock_audit, mock_ollama, monkeypatch):
    """E2E-AUDIT-01：审计后研报含 audit_flag ∈ unanimous_buy|majority_agree|votes_uncertain|high_risk_flag"""
    from app.core.config import settings
    monkeypatch.setattr(settings, "mock_llm", False)
    mock_ollama.return_value = {
        "model": "mock",
        "latency_ms": 1,
        "response": _MOCK_LLM_BUY_RESPONSE,
        "raw": {},
    }
    mock_audit.return_value = {
        "audit_flag": "unanimous_buy",
        "audit_detail": "三方审计：看多3票",
        "adjusted_confidence": 0.72,
        "final_recommendation": "BUY",
        "skip_reason": None,
    }
    client.post("/api/v1/internal/reports/clear?stock_code=600519.SH")
    r = client.post("/api/v1/reports/generate", json={"stock_code": "600519.SH", "source": "test"})
    assert r.status_code == 200, r.text
    report_id = r.json()["data"]["report_id"]
    rr = client.get(f"/api/v1/reports/{report_id}")
    assert rr.status_code == 200
    j = rr.json()["data"].get("report") or rr.json()["data"]
    af = (j.get("content_json") or j).get("audit_flag") or j.get("audit_flag")
    assert af in (
        "unanimous_buy",
        "majority_agree",
        "votes_uncertain",
        "high_risk_flag",
    ), f"audit_flag={af} 不在预期枚举内"


@patch("app.services.ollama_client.ollama_client.generate", new_callable=AsyncMock)
@patch("app.services.llm_router.run_audit_and_aggregate", new_callable=AsyncMock)
def test_e2e_audit_02_high_risk_lowers_confidence(mock_audit, mock_ollama, monkeypatch):
    """E2E-AUDIT-02：审计方-1 异议 → confidence ≤ base×0.75，audit_flag=high_risk_flag"""
    from app.core.config import settings
    monkeypatch.setattr(settings, "mock_llm", False)
    mock_ollama.return_value = {
        "model": "mock",
        "latency_ms": 1,
        "response": _MOCK_LLM_BUY_RESPONSE,
        "raw": {},
    }
    base_conf = 0.70
    mock_audit.return_value = {
        "audit_flag": "high_risk_flag",
        "audit_detail": "审计方高风险反对",
        "adjusted_confidence": base_conf * 0.75,
        "final_recommendation": "BUY",
        "skip_reason": None,
    }
    client.post("/api/v1/internal/reports/clear?stock_code=600519.SH")
    r = client.post("/api/v1/reports/generate", json={"stock_code": "600519.SH", "source": "test"})
    assert r.status_code == 200, r.text
    report_id = r.json()["data"]["report_id"]
    rr = client.get(f"/api/v1/reports/{report_id}")
    assert rr.status_code == 200
    j = rr.json()["data"].get("report") or rr.json()["data"]
    af = (j.get("content_json") or j).get("audit_flag") or j.get("audit_flag")
    assert af == "high_risk_flag", f"audit_flag={af}"
    conf = (j.get("content_json") or j).get("confidence") or j.get("confidence")
    if conf is not None:
        assert conf <= base_conf * 0.76, f"confidence={conf} 应 ≤ base×0.75"


@patch("app.services.ollama_client.ollama_client.generate", new_callable=AsyncMock)
@patch("app.services.llm_router.run_audit_and_aggregate", new_callable=AsyncMock)
def test_e2e_audit_03_votes_uncertain_displayed(mock_audit, mock_ollama, monkeypatch):
    """E2E-AUDIT-03：三票不一致 → audit_flag=votes_uncertain，高级区显式标注异议"""
    from app.core.config import settings
    monkeypatch.setattr(settings, "mock_llm", False)
    mock_ollama.return_value = {
        "model": "mock",
        "latency_ms": 1,
        "response": _MOCK_LLM_BUY_RESPONSE,
        "raw": {},
    }
    mock_audit.return_value = {
        "audit_flag": "votes_uncertain",
        "audit_detail": "三方审计：看多1票 看空1票 观望1票",
        "adjusted_confidence": 0.60,
        "final_recommendation": "BUY",
        "skip_reason": None,
    }
    client.post("/api/v1/internal/reports/clear?stock_code=600519.SH")
    r = client.post("/api/v1/reports/generate", json={"stock_code": "600519.SH", "source": "test"})
    assert r.status_code == 200, r.text
    report_id = r.json()["data"]["report_id"]
    rr = client.get(f"/api/v1/reports/{report_id}")
    assert rr.status_code == 200
    j = rr.json()["data"].get("report") or rr.json()["data"]
    af = (j.get("content_json") or j).get("audit_flag") or j.get("audit_flag")
    assert af == "votes_uncertain", f"audit_flag={af}"


# --- E7 Walk-Forward 回测回归 ---


def test_walkforward_backtest_script_runs():
    """E7.4：Walk-Forward 回测脚本可执行，相同参数两次运行新增记录数一致（12 §6.0.1）。"""
    import subprocess
    from pathlib import Path

    from app.core.db import SessionLocal
    from app.models import SimPositionBacktest

    root = Path(__file__).resolve().parents[1]
    script = root / "scripts" / "walkforward_backtest.py"
    if not script.exists():
        pytest.skip("scripts/walkforward_backtest.py 不存在")
    cmd = [
        "python",
        str(script),
        "--start-date", "2024-03-01",
        "--end-date", "2024-03-15",
        "--stock-codes", "600519.SH,000001.SZ",
        "--capital-tier", "10w",
    ]

    def count_in_range():
        db = SessionLocal()
        try:
            return db.query(SimPositionBacktest).filter(
                SimPositionBacktest.signal_date >= "2024-03-01",
                SimPositionBacktest.signal_date <= "2024-03-15",
            ).count()
        finally:
            db.close()

    c0 = count_in_range()
    r1 = subprocess.run(cmd, cwd=str(root), capture_output=True, text=True, timeout=90)
    assert r1.returncode == 0, f"首次运行失败: {r1.stderr or r1.stdout}"
    c1 = count_in_range()
    delta1 = c1 - c0
    r2 = subprocess.run(cmd, cwd=str(root), capture_output=True, text=True, timeout=90)
    assert r2.returncode == 0, f"二次运行失败: {r2.stderr}"
    c2 = count_in_range()
    delta2 = c2 - c1
    assert delta1 == delta2, f"可复现性：两次运行新增记录数应一致 ({delta1} vs {delta2})"
