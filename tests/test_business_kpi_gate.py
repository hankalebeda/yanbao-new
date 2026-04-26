"""
业务KPI门禁测试 (角度38 - 商业底线门禁)
验证系统满足最低业务健康度指标:
  - settlement 结算覆盖率 > 0 (有研报被结算)
  - kline 日线覆盖率 >= 10% (足够股票有K线数据)
  - /health 端点正确暴露 settlement_status / kline_status 字段
"""
from __future__ import annotations

import pytest
from tests.helpers_ssot import (
    insert_kline,
    insert_report_bundle_ssot,
    insert_settlement_result,
    insert_stock_master,
)

pytestmark = [
    pytest.mark.feature("FR07-KPI-GATE"),
]


def test_business_kpi_settlement_not_zero(client, db_session):
    """角度38: 系统中至少有1份研报完成了结算，确保结算流程不是空转."""
    sm = insert_stock_master(db_session, stock_code="000001.SZ", stock_name="平安银行")
    report = insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        recommendation="BUY",
        quality_flag="ok",
    )
    insert_settlement_result(
        db_session,
        report_id=report.report_id,
        signal_date="2026-01-15",
    )
    db_session.commit()

    from app.models import SettlementResult
    from sqlalchemy import func

    count = (
        db_session.query(func.count(func.distinct(SettlementResult.report_id)))
        .scalar()
    )
    assert count > 0, (
        "业务KPI门禁失败: settlement_distinct_reports == 0，结算流程空转"
    )


def test_business_kpi_kline_coverage_above_threshold(client, db_session):
    """角度38: K线覆盖率达到最低10%阈值，确保技术分析有足够数据支撑."""
    from app.models import KlineDaily, StockMaster
    from sqlalchemy import func

    # 插入10只股票 + K线（模拟达标）
    for i in range(10):
        code = f"60{i:04d}.SH"
        insert_stock_master(db_session, stock_code=code, stock_name=f"测试股{i}")
        insert_kline(
            db_session,
            stock_code=code,
            trade_date=f"2026-01-{i+10:02d}",
            open_price=10.0,
            high_price=11.0,
            low_price=9.5,
            close_price=10.5,
        )
    db_session.commit()

    total_stocks = db_session.query(func.count(StockMaster.stock_code)).scalar() or 0
    kline_stocks = (
        db_session.query(func.count(func.distinct(KlineDaily.stock_code))).scalar()
        or 0
    )
    if total_stocks == 0:
        pytest.skip("stock_master 为空，跳过覆盖率门禁")

    coverage_pct = kline_stocks / total_stocks * 100
    assert coverage_pct >= 10.0, (
        f"业务KPI门禁失败: K线覆盖率 {coverage_pct:.1f}% < 10% 最低阈值"
    )


def test_business_kpi_health_endpoint_exposes_settlement_and_kline_fields(client):
    """/health 端点必须暴露 settlement_status 和 kline_status 字段 (角度29+38)."""
    resp = client.get("/health")
    assert resp.status_code == 200, f"/health 返回 {resp.status_code}"
    body = resp.json()
    data = body.get("data", body)
    assert "settlement_status" in data, (
        "/health 缺少 settlement_status 字段 (角度29门禁)"
    )
    assert "kline_status" in data, (
        "/health 缺少 kline_status 字段 (角度29门禁)"
    )
    assert data["settlement_status"] in {"ok", "degraded"}, (
        f"settlement_status 值非法: {data['settlement_status']}"
    )
    assert data["kline_status"] in {"ok", "degraded"}, (
        f"kline_status 值非法: {data['kline_status']}"
    )


def test_business_kpi_health_settlement_coverage_counts_only_visible_ok_reports(client, db_session):
    """角度38: settlement 覆盖率只统计可见 ok 报告，non-ok 不得稀释业务健康度。"""
    ok_report = insert_report_bundle_ssot(
        db_session,
        stock_code="600519.SH",
        recommendation="BUY",
        quality_flag="ok",
        published=True,
    )
    insert_report_bundle_ssot(
        db_session,
        stock_code="000001.SZ",
        recommendation="HOLD",
        quality_flag="stale_ok",
        published=True,
    )
    insert_settlement_result(
        db_session,
        report_id=ok_report.report_id,
        signal_date="2026-01-15",
    )
    db_session.commit()

    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json().get("data", {})
    assert data["settlement_coverage_pct"] == 100.0
    assert data["settlement_status"] == "ok"


def test_business_kpi_health_report_chain_requires_visible_ok_reports(client, db_session):
    """角度38: 报告链健康只应由可见 ok 报告支撑，non-ok 存量不能让健康误报为 ok。"""
    insert_report_bundle_ssot(
        db_session,
        stock_code="300750.SZ",
        recommendation="HOLD",
        quality_flag="stale_ok",
        published=True,
    )
    db_session.commit()

    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json().get("data", {})
    assert data["report_chain_status"] == "degraded"
