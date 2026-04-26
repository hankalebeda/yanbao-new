"""FR06 质量管线契约回归测试

覆盖报告生成质量管线的两条契约：
- C2: backtest_recent_3m 在样本数 < 10 时必须将 actionable_accuracy 置 null
- C9: direction_forecast.horizons 必须覆盖 1/7/14/30/60 五个窗口，且长周期 fallback 必须可展示

依赖 SSOT 真实代码路径（不通过 HTTP 端点），保持轻量、与认证/迁移环境解耦。
"""
from __future__ import annotations

from app.services.report_generation_ssot import _estimate_future_direction


def _kline_uptrend() -> dict:
    return {"close": 100.0, "ma5": 95.0, "ma20": 90.0}


def test_c2_low_samples_suppresses_accuracy():
    """样本数 < 10 时 actionable_accuracy 必须为 None，避免 1 样本 0% 误导。"""
    forecast = _estimate_future_direction(
        kline_row=_kline_uptrend(),
        recommendation="BUY",
        accuracy_7d=0.0,  # 1 样本错误 → 假准确率 0%
        samples_7d=1,
    )
    bt = forecast["backtest_recent_3m"][0]
    assert bt["actionable_accuracy"] is None, "样本不足时不应展示准确率"
    assert bt["actionable_samples"] == 1
    assert bt["samples_sufficient"] is False
    assert bt["min_samples_required"] == 10
    assert bt["actionable_coverage"] == 0.0


def test_c2_sufficient_samples_keeps_accuracy():
    """样本数 >= 10 时保留真实准确率。"""
    forecast = _estimate_future_direction(
        kline_row=_kline_uptrend(),
        recommendation="BUY",
        accuracy_7d=0.62,
        samples_7d=15,
    )
    bt = forecast["backtest_recent_3m"][0]
    assert bt["actionable_accuracy"] == 0.62
    assert bt["actionable_samples"] == 15
    assert bt["samples_sufficient"] is True
    assert bt["actionable_coverage"] == 1.0


def test_c2_zero_samples_returns_null_accuracy():
    """无样本时既无准确率也无 coverage。"""
    forecast = _estimate_future_direction(
        kline_row=_kline_uptrend(),
        recommendation="HOLD",
        accuracy_7d=None,
        samples_7d=0,
    )
    bt = forecast["backtest_recent_3m"][0]
    assert bt["actionable_accuracy"] is None
    assert bt["actionable_coverage"] == 0.0


def test_c9_direction_forecast_covers_five_windows():
    """direction_forecast.horizons 必须包含 1/7/14/30/60 五个 horizon_day。"""
    forecast = _estimate_future_direction(
        kline_row=_kline_uptrend(),
        recommendation="BUY",
        accuracy_7d=0.55,
        samples_7d=20,
    )
    horizons = forecast["horizons"]
    days = {int(h["horizon_day"]) for h in horizons}
    assert days == {1, 7, 14, 30, 60}, f"必须覆盖 5 窗口，实际={sorted(days)}"

    # 14/30/60 由 SSOT fallback 产生，必须给出可展示方向/动作，避免前端横杠缺口
    long_horizons = [h for h in horizons if int(h["horizon_day"]) in (14, 30, 60)]
    assert len(long_horizons) == 3
    for h in long_horizons:
        assert h["direction"] == "UP"
        assert h["status"] == "derived_fallback"
        assert h["action"] == "BUY"


def test_c9_short_horizons_use_kline_signal():
    """1d/7d 必须基于 K线 MA 结构给出明确方向，不能 None。"""
    forecast = _estimate_future_direction(
        kline_row=_kline_uptrend(),
        recommendation="BUY",
        accuracy_7d=0.55,
        samples_7d=20,
    )
    short_map = {int(h["horizon_day"]): h for h in forecast["horizons"] if int(h["horizon_day"]) in (1, 7)}
    assert short_map[1]["direction"] == "UP"
    assert short_map[7]["direction"] == "UP"
    assert short_map[7]["action"] == "BUY"
