from __future__ import annotations

from typing import Any


STATUS_REASON_CN = {
    "stats_not_ready": "统计数据仍在汇总中。",
    "home_snapshot_not_ready": "今日批次已确定，公开数据仍在补齐。",
    "home_reports_not_ready": "最新研报仍在生成中。",
    "home_source_inconsistent": "来源口径暂未完全对齐，页面已回退到稳定批次。",
    "equity_curve_empty": "收益曲线仍在生成中。",
    "sim_snapshot_lagging": "模拟收益快照正在追赶最新批次。",
    "sim_dashboard_not_ready": "模拟收益看板数据准备中。",
    "SIM_DASHBOARD_NOT_READY": "模拟收益看板数据准备中。",
    "baseline_pending": "基线对照数据计算中。",
    "settlement_pipeline_not_completed": "结算链路仍在处理中。",
    "settlement_pipeline_failed": "结算链路未成功完成，当前展示仍在降级。",
    "settlement_materialization_pending": "结算结果已生成，快照与物化仍在补齐中。",
    "sample_lt_30": "样本积累中。",
    "market_state_degraded": "市场状态数据暂时降级，已自动展示可用结果。",
    "reference_date_missing;market_state_degraded=true": "市场状态参考交易日暂不可用，已自动降级展示。",
    "reference_metrics_unavailable;market_state_degraded=true": "市场状态参考指标暂不可用，已自动降级展示。",
    "reference metrics unavailable;market state degraded=true": "市场状态参考指标暂不可用，已自动降级展示。",
    "reference date missing;market state degraded=true": "市场状态参考交易日暂不可用，已自动降级展示。",
    "COLD_START_FALLBACK": "系统处于冷启动阶段，当前展示最近可用结果。",
    "non_trade_day": "当前为非交易日，页面展示最近有效批次。",
    "no_etf_data_available": "ETF 资金流数据暂不可用。",
    "fetcher_not_provided": "对应数据源暂未配置。",
    "northbound_not_ok": "北向资金数据暂不可用。",
    "etf_flow_not_ok": "ETF 资金流数据暂不可用。",
    "REPORT_DATA_INCOMPLETE": "研报依赖数据不完整，系统已清除本次不合格结果。",
    "stale_ok": "数据正在更新中。",
    "fallback_t_minus_1": "已回退到最近一个交易日的稳定数据。",
    "KLINE_COVERAGE_INSUFFICIENT": "当日行情覆盖不足，系统已回退到稳定批次。",
    "stats_history_truncated": "历史窗口覆盖不足",
    "stats_source_degraded": "部分历史批次未完整回补，沿用当前统计快照",
}

ROLE_CN = {
    "user": "普通用户",
    "admin": "管理员",
    "super_admin": "超级管理员",
}

STRATEGY_TYPE_CN = {
    "A": "事件驱动",
    "B": "趋势跟踪",
    "C": "低波套利",
}

MARKET_STATE_CN = {
    "BULL": "偏强",
    "NEUTRAL": "震荡",
    "BEAR": "偏弱",
    "VOLATILE": "波动较大",
    "UNKNOWN": "待确认",
}

QUALITY_FLAG_CN = {
    "ok": "数据正常",
    "stale_ok": "数据延迟，已回退至可用批次",
    "degraded": "部分数据降级展示",
    "failed": "数据暂不可用",
    "llm_degraded": "深度分析补充中",
    "rule_fallback": "当前以基础信号解读为主",
}

RISK_AUDIT_STATUS_CN = {
    "completed": "已完成风险检查",
    "skipped": "风险补充信息稍后更新",
    "not_triggered": "风险补充信息准备中",
}

RISK_AUDIT_SKIP_REASON_CN = {
    "llm_circuit_open": "风险补充信息稍后更新",
    "llm_all_failed_rule_fallback": "风险补充信息稍后更新",
    "mock_llm": "当前仅展示基础风险提示",
    "manual_review_pending": "风险补充信息准备中",
}

FEEDBACK_TYPE_CN = {
    "positive": "有帮助",
    "negative": "没帮助",
}

PUBLIC_ERROR_DETAIL_CN = {
    "invalid_stock_code": "请输入正确的股票代码，例如 600519.SH。",
    "INVALID_STOCK_CODE": "请输入正确的股票代码，例如 600519.SH。",
    "INVALID_PAYLOAD": "当前请求缺少必要信息或格式不正确，请检查后重试。",
    "RESET_TOKEN_EXPIRED": "重置链接已失效，请重新申请新的密码重置链接。",
}

REPORT_UNAVAILABLE_COPY = {
    "DEPENDENCY_NOT_READY": {
        "title": "研报准备中",
        "message": "这只股票的行情与研究资料仍在补齐，暂时还不能生成新的公开研报。",
    },
    "NOT_IN_CORE_POOL": {
        "title": "当前暂无公开研报",
        "message": "这只股票目前不在公开研究池范围内，因此暂未提供公开研报展示。",
    },
    "MANUAL_TRIGGER_REQUIRED": {
        "title": "当前暂无可展示研报",
        "message": "这只股票目前还没有可直接展示的公开研报，请稍后再来查看。",
    },
    "recent_report_not_ready": {
        "title": "最新研报准备中",
        "message": "最新一期研报仍在整理中，请稍后刷新页面查看。",
    },
}

KNOWN_OK_REASON_PREFIXES = (
    "akshare_",
    "eastmoney_",
    "tdx_",
    "manual_",
    "runtime_",
)


def humanize_status_reason(value: Any, default: str = "当前数据暂不可用，系统已自动降级展示。") -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw in STATUS_REASON_CN:
        return STATUS_REASON_CN[raw]
    normalized = raw.replace("market_state_degraded=true", "").strip("; ")
    if normalized in STATUS_REASON_CN:
        return STATUS_REASON_CN[normalized]
    if any(raw.startswith(prefix) for prefix in KNOWN_OK_REASON_PREFIXES):
        return "数据已接入"
    if any(token in raw for token in ("_", ";", ":")):
        return default
    return raw


def humanize_role(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "—"
    return ROLE_CN.get(raw, raw)


def humanize_strategy_type(value: Any, default: str = "策略框架") -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return default
    return STRATEGY_TYPE_CN.get(raw, default)


def humanize_market_state(value: Any, default: str = "市场环境待确认") -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return default
    return MARKET_STATE_CN.get(raw, default)


def humanize_quality_flag(value: Any, default: str = "数据处理中") -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    return QUALITY_FLAG_CN.get(raw, default)


def humanize_risk_audit_status(value: Any, default: str = "风险补充信息准备中") -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    return RISK_AUDIT_STATUS_CN.get(raw, default)


def humanize_risk_audit_skip_reason(value: Any, default: str = "风险补充信息准备中") -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return RISK_AUDIT_SKIP_REASON_CN.get(raw, humanize_status_reason(raw, default=default))


def humanize_feedback_type(value: Any, default: str = "未分类") -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    return FEEDBACK_TYPE_CN.get(raw, default)


def humanize_public_error_detail(value: Any, *, path: str | None = None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "当前请求缺少必要信息或格式不正确，请检查后重试。"
    normalized_path = str(path or "").strip()
    if normalized_path == "/auth/activate" and raw == "INVALID_PAYLOAD":
        return "激活链接已失效或无效，请重新申请新的激活邮件。"
    if normalized_path in {"/report", "/demo/report"} and raw == "INVALID_PAYLOAD":
        return PUBLIC_ERROR_DETAIL_CN["INVALID_STOCK_CODE"]
    if raw in PUBLIC_ERROR_DETAIL_CN:
        return PUBLIC_ERROR_DETAIL_CN[raw]
    if raw.isupper() or "_" in raw:
        return "当前请求暂时无法处理，请稍后重试。"
    return raw


def humanize_report_unavailable(value: Any) -> dict[str, str]:
    raw = str(value or "").strip()
    if raw in REPORT_UNAVAILABLE_COPY:
        return dict(REPORT_UNAVAILABLE_COPY[raw])
    return {
        "title": "研报暂时不可用",
        "message": "当前这只股票的研报仍在准备中，请稍后刷新页面或先查看其他已发布内容。",
    }
