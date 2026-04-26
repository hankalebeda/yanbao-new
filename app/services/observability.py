from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.models import ModelRunLog, PredictionOutcome, Report
from app.services.report_engine import prediction_stats
from app.services.source_state import get_source_runtime_status
from app.services.dashboard_query import get_dashboard_stats_payload_ssot  # noqa: F401 — re-export for admin monkeypatch
from app.services.runtime_anchor_service import RuntimeAnchorService
from app.services.settlement_ssot import get_settlement_pipeline_status


def prediction_stats_ssot(db: Session) -> dict:
    """Alias used by admin system-status endpoint."""
    stats = prediction_stats(db)
    judged = stats.get("total_judged", 0)
    return {
        "total_judged": judged,
        "accuracy": stats.get("accuracy", 0.0),
        "by_window": stats.get("by_window"),
        "recent_3m": stats.get("recent_3m"),
    }


def runtime_metrics_summary(db: Session, **_kwargs) -> dict:
    import json as _json
    from datetime import date as _date_type

    def _make_json_safe(obj):
        """Recursively convert datetime/date objects to ISO strings."""
        if isinstance(obj, dict):
            return {k: _make_json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_make_json_safe(v) for v in obj]
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, _date_type):
            return obj.isoformat()
        return obj

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=24)

    logs = db.query(ModelRunLog).filter(ModelRunLog.created_at >= since).all()
    total_llm = len(logs)
    ok_llm = len([x for x in logs if x.status == "ok"])
    timeout_llm = len([x for x in logs if (x.error_type or "") == "timeout"])
    oom_llm = len([x for x in logs if "oom" in (x.error_type or "").lower()])
    avg_latency = round(sum(x.latency_ms for x in logs) / total_llm, 2) if total_llm else 0

    reports_24h = db.query(Report).filter(Report.created_at >= since).count()
    report_rows_24h = db.query(Report).filter(Report.created_at >= since).all()
    degraded_reports = 0
    core_field_coverage_values = []
    conflict_count = 0
    for r in report_rows_24h:
        payload = r.content_json or {}
        qg = (payload.get("quality_gate") or {})
        if qg.get("publish_decision") != "publish":
            degraded_reports += 1
        co = payload.get("company_overview") or {}
        val = payload.get("valuation") or {}
        dims = payload.get("dimensions") or {}
        cap = (dims.get("capital_flow") or {}).get("main_force") or {}
        margin = dims.get("margin_financing") or {}
        checks = [
            1.0 if co.get("industry") else 0.0,
            1.0 if co.get("listed_date") else 0.0,
            1.0 if val.get("pe_ttm") is not None else 0.0,
            1.0 if val.get("pb") is not None else 0.0,
            1.0 if cap.get("status") in {"ok", "stale_ok"} else 0.0,
            1.0 if margin.get("status") in {"ok", "stale_ok"} else 0.0,
        ]
        core_field_coverage_values.append(sum(checks) / len(checks))
        if ((payload.get("thesis") or {}).get("event_conflict_flag")) is True:
            conflict_count += 1
    degraded_rate = round(degraded_reports / max(1, len(report_rows_24h)), 4) if report_rows_24h else 0.0
    core_field_coverage = (
        round(sum(core_field_coverage_values) / max(1, len(core_field_coverage_values)), 4)
        if core_field_coverage_values
        else 0.0
    )
    decision_conflict_rate = round(conflict_count / max(1, len(report_rows_24h)), 4) if report_rows_24h else 0.0
    stats = prediction_stats(db)
    judged = stats.get("total_judged") or 0
    accuracy = stats.get("accuracy") or 0

    timeout_rate = round(timeout_llm / total_llm, 4) if total_llm else 0
    error_distribution = {}
    for x in logs:
        k = (x.error_type or "none").lower()
        error_distribution[k] = error_distribution.get(k, 0) + 1
    degraded = timeout_rate > 0.05

    # service health: analyse source_runtime for unhealthy sources
    source_rt = get_source_runtime_status()
    flags: list[str] = []
    unhealthy_sources: list[str] = []
    for kind in ("market", "hotspot"):
        for src, info in (source_rt.get(kind) or {}).items():
            if isinstance(info, dict) and info.get("circuit_open"):
                unhealthy_sources.append(f"{kind}:{src}")
    if unhealthy_sources:
        flags.append("source_runtime_abnormal")
    if degraded:
        flags.append("llm_timeout_elevated")

    anchor = RuntimeAnchorService(db)
    runtime_anchors = anchor.runtime_anchor_dates()
    public_runtime = anchor.public_runtime_status()
    dashboard_30d = get_dashboard_stats_payload_ssot(
        db,
        window_days=30,
        runtime_anchor_service=anchor,
    )
    runtime_trade_date = (
        dashboard_30d.get("runtime_trade_date")
        or public_runtime.get("trade_date")
        or runtime_anchors.get("runtime_trade_date")
    )
    if runtime_trade_date:
        settlement_pipeline = get_settlement_pipeline_status(
            db,
            trade_date=runtime_trade_date,
            target_scope="all",
        )
    else:
        settlement_pipeline = {
            "pipeline_name": None,
            "trade_date": None,
            "pipeline_status": "NOT_RUN",
            "degraded": False,
            "status_reason": None,
            "started_at": None,
            "finished_at": None,
            "updated_at": None,
            "pipeline_run_total": 0,
            "matching_pipeline_run_total": 0,
        }

    data_quality_flags: list[str] = []
    if str(public_runtime.get("data_status") or "READY").upper() != "READY":
        data_quality_flags.append("public_runtime_degraded")
    if str(settlement_pipeline.get("pipeline_status") or "NOT_RUN").upper() != "COMPLETED":
        data_quality_flags.append("settlement_pipeline_not_completed")
    if str(dashboard_30d.get("status_reason") or "") == "stats_not_ready":
        data_quality_flags.append("dashboard_stats_not_ready")

    runtime_flags = [flag for flag in data_quality_flags if flag in {"public_runtime_degraded", "sim_snapshot_missing"}]
    if unhealthy_sources:
        runtime_flags.append("source_circuit_open")

    business_flags: list[str] = []
    if degraded_reports > 0:
        business_flags.append("report_output_degraded")
    if judged and accuracy is not None and accuracy < 0.55:
        business_flags.append("prediction_accuracy_below_target")

    result = {
        "window": "24h",
        "llm": {
            "total": total_llm,
            "success": ok_llm,
            "timeout": timeout_llm,
            "timeout_rate": timeout_rate,
            "oom_count_24h": oom_llm,
            "avg_latency_ms": avg_latency,
            "error_distribution": error_distribution,
        },
        "report": {
            "generated_24h": reports_24h,
            "degraded_24h": degraded_reports,
            "degraded_rate": degraded_rate,
            "core_field_coverage": core_field_coverage,
            "decision_conflict_rate": decision_conflict_rate,
        },
        "prediction": {
            "judged_total": judged,
            "accuracy": accuracy,
            "by_window": stats.get("by_window"),
            "recent_3m": stats.get("recent_3m"),
        },
        "dashboard_30d": dashboard_30d,
        "settlement_pipeline": settlement_pipeline,
        "public_runtime": public_runtime,
        "runtime_anchors": runtime_anchors,
        "runtime_flags": runtime_flags,
        "runtime_state": "degraded" if degraded or data_quality_flags or runtime_flags else "normal",
        "source_runtime": source_rt,
        "service_health": {
            "status": "degraded" if unhealthy_sources or degraded else "normal",
            "flags": flags,
            "unhealthy_source_count": len(unhealthy_sources),
            "unhealthy_sources": unhealthy_sources,
        },
        "business_health": {
            "status": "degraded" if business_flags else "normal",
            "flags": business_flags,
        },
        "data_quality": {
            "status": "degraded" if data_quality_flags else "normal",
            "flags": data_quality_flags,
            "dashboard_status_reason": dashboard_30d.get("status_reason"),
            "pipeline_run_total": settlement_pipeline.get("pipeline_run_total", 0),
            "matching_pipeline_run_total": settlement_pipeline.get("matching_pipeline_run_total", 0),
        },
    }
    return _make_json_safe(result)
