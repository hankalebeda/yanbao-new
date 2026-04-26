"""Report view service — extracted from main.py to keep route handlers thin.

Provides:
- build_report_template_context_for_user(request, view_report) -> dict
- load_report_view_payload(report_payload, stock_code) -> dict
- latest_report_id_for_code(db, stock_code) -> str | None
- report_status_payload(stock_code, db) -> dict
- ensure_demo_job(stock_code) -> None
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from fastapi import Request

from app.core.db import SessionLocal
from app.models import Report
from app.services.market_data import fetch_quote_snapshot
from app.services.membership import subscription_status
from app.services.report_engine import trade_date_str
from app.services.report_generation_ssot import ensure_non_report_usage_collected_if_needed, generate_report_ssot

_STOCK_CODE_RE = re.compile(r"^\d{6}\.(SH|SZ)$")
_demo_jobs: dict[str, dict] = {}
_demo_jobs_lock = Lock()
logger = logging.getLogger(__name__)


def is_admin_role(role: Any) -> bool:
    return str(role or "").strip().lower() in {"admin", "super_admin"}


def effective_viewer_tier_from_subscription_state(user, subscription_state: dict | None) -> str:
    if user is None:
        return "Free"
    if is_admin_role(getattr(user, "role", None)):
        return "Enterprise"

    if isinstance(subscription_state, dict):
        tier = subscription_state.get("tier") or subscription_state.get("membership_level")
        if tier:
            return str(tier)
        plan_code = str(subscription_state.get("plan_code") or "").strip().lower()
        if plan_code in {"monthly", "quarterly", "yearly", "annual"}:
            return str(getattr(user, "tier", None) or "Pro")

    return str(getattr(user, "tier", None) or getattr(user, "membership_level", None) or "Free")


def has_active_paid_membership_from_subscription_state(user, subscription_state: dict | None) -> bool:
    if user is None:
        return False
    if is_admin_role(getattr(user, "role", None)):
        return True

    tier = effective_viewer_tier_from_subscription_state(user, subscription_state)
    if tier.strip().lower() not in {"", "free"}:
        if not isinstance(subscription_state, dict):
            return True
        status = str(subscription_state.get("status") or "").strip().lower()
        return status in {"active", "paid", "granted", "trialing", "unknown", ""}
    return False


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_utc(dt) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Demo job management
# ---------------------------------------------------------------------------

async def _run_demo_generation(stock_code: str):
    db = SessionLocal()
    try:
        target_trade_date = trade_date_str()
        await ensure_non_report_usage_collected_if_needed(
            db,
            stock_code=stock_code,
            trade_date=target_trade_date,
        )
        result = generate_report_ssot(db, stock_code=stock_code)
        with _demo_jobs_lock:
            _demo_jobs[stock_code] = {
                "status": "done",
                "started_at": _demo_jobs.get(stock_code, {}).get("started_at") or _now_utc().isoformat(),
                "updated_at": _now_utc().isoformat(),
                "report_id": result["report_id"],
                "error": None,
            }
    except Exception as exc:
        with _demo_jobs_lock:
            _demo_jobs[stock_code] = {
                "status": "failed",
                "started_at": _demo_jobs.get(stock_code, {}).get("started_at") or _now_utc().isoformat(),
                "updated_at": _now_utc().isoformat(),
                "report_id": None,
                "error": str(exc),
            }
    finally:
        db.close()


def ensure_demo_job(stock_code: str):
    with _demo_jobs_lock:
        job = _demo_jobs.get(stock_code)
        if job and job.get("status") in {"running", "queued"}:
            return
        _demo_jobs[stock_code] = {
            "status": "running",
            "started_at": _now_utc().isoformat(),
            "updated_at": _now_utc().isoformat(),
            "report_id": None,
            "error": None,
        }
    asyncio.create_task(_run_demo_generation(stock_code))


def _demo_status_snapshot(stock_code: str) -> dict:
    with _demo_jobs_lock:
        return dict(_demo_jobs.get(stock_code) or {"status": "idle"})


# ---------------------------------------------------------------------------
# Plain-report fallback builder
# ---------------------------------------------------------------------------

def _build_market_dual_source_from_snapshot(market_snapshot: dict) -> dict:
    return {
        "dual_status": market_snapshot.get("dual_status"),
        "selected_by_order": market_snapshot.get("source_selected_by_order"),
        "comparison": market_snapshot.get("dual_comparison"),
        "sources": market_snapshot.get("dual_sources"),
    }


def _build_plain_fallback(report: dict, market_snapshot: dict, market_features: dict, dual_source_market: dict) -> dict:
    recommendation_cn = report.get("recommendation_cn") or {"BUY": "买入", "SELL": "卖出", "HOLD": "观望等待"}.get(
        report.get("recommendation"), "观望等待"
    )
    ng = report.get("novice_guide") or {}
    df = report.get("direction_forecast") or {}
    pf = report.get("price_forecast") or {}
    d7 = next((x for x in (df.get("horizons") or []) if x.get("horizon_day") == 7), {})
    b7 = next((x for x in (df.get("backtest_recent_3m") or []) if x.get("horizon_day") == 7), {})
    p7 = next((x for x in (pf.get("windows") or []) if x.get("horizon_days") == 7), {})
    bsum3 = ((pf.get("backtest") or {}).get("summary_recent_3m") or {})
    readi = pf.get("readiness") or {}
    target = df.get("target") or {}
    dsum = df.get("summary") or {}
    feature_values = (market_features or {}).get("features") or {}
    chain = [
        {"step": 1, "title": "先看真实价格", "fact": f"当前价 {market_snapshot.get('last_price')}，涨跌幅 {market_snapshot.get('pct_change')}。", "impact": "价格是所有判断的基础。"},
        {"step": 2, "title": "再看趋势", "fact": f"MA5={feature_values.get('ma5')}，MA20={feature_values.get('ma20')}，趋势={feature_values.get('trend')}。", "impact": "趋势用于确认短中期方向是否一致。"},
        {"step": 3, "title": "最后给动作", "fact": f"7天方向={d7.get('direction')}，动作={d7.get('action')}。", "impact": "方向和稳定性一起决定是否执行。"},
    ]
    execution_plan = {
        "position_suggestion": f"建议仓位：{'0%~20%' if recommendation_cn == '卖出' else ('20%~40%' if recommendation_cn == '观望' else '40%~70%')}。",
        "risk_line": "风险线：若价格连续两天走弱并跌破关键均线，优先防守。",
        "next_checklist": [f"检查7天方向与动作：{d7.get('direction')} / {d7.get('action')}", "检查是否有重大公告或政策变化"],
        "execution_note": "旧缓存已自动补齐，建议重新生成可获得更完整执行建议。",
    }
    return {
        "title": f"{report.get('stock_code')} 白话研报",
        "action_now": recommendation_cn,
        "one_sentence": f"结论：{recommendation_cn}。先看真实双源行情，再看1~7天方向和回测稳定性。",
        "execution_plan": execution_plan,
        "what_to_do_now": [f"当前建议：{recommendation_cn}。", ng.get("uncertainty") or "优先看7天信号稳定性（样本和覆盖率）。", "若有重大公告/政策变化，请重新生成。"],
        "key_numbers": [
            {"name": "当前价格", "value": market_snapshot.get("last_price"), "why": "决定当前买卖位置。"},
            {"name": "7天方向", "value": d7.get("direction"), "why": "用于判断一周主方向。"},
            {"name": "7天动作", "value": d7.get("action"), "why": "用于执行建议。"},
        ],
        "accuracy_explain": {
            "headline": (
                f"7天可操作准确率={b7.get('actionable_accuracy')}，样本={b7.get('actionable_samples')}，覆盖率={b7.get('actionable_coverage')}。"
                if (b7 and (b7.get("actionable_accuracy") is not None or b7.get("actionable_samples")))
                else "预测由 AI 综合多维度数据完成，置信度见各窗口。"
            ),
            "current_metrics": {
                "horizon_7d_actionable_accuracy": b7.get("actionable_accuracy"),
                "horizon_7d_samples": b7.get("actionable_samples"),
                "horizon_7d_coverage": b7.get("actionable_coverage"),
                "backtest_overall_3m_accuracy": bsum3.get("overall_accuracy"),
                "backtest_overall_3m_samples": bsum3.get("samples"),
                "readiness_score": readi.get("score"),
                "ready_for_use": readi.get("ready_for_use"),
            },
            "confidence_formula": {"formula": "final_confidence = raw_confidence*(1-alpha) + empirical_accuracy*alpha", "raw_name": "confidence_raw", "empirical_name": "confidence_empirical_accuracy", "alpha_name": "alpha", "calibrated_name": "confidence"},
            "confidence_case_7d": {"raw_confidence": p7.get("confidence_raw"), "empirical_accuracy": p7.get("confidence_empirical_accuracy"), "alpha": None, "reliability": None, "calibrated_confidence": p7.get("confidence"), "sample_scope": p7.get("confidence_sample_scope")},
            "target_explain": {"target_accuracy": target.get("target_accuracy"), "min_actionable_samples": target.get("min_actionable_samples"), "min_actionable_coverage": target.get("min_actionable_coverage"), "target_met_days": dsum.get("target_met_days") or [], "target_failed_days": dsum.get("target_failed_days") or []},
            "window_accuracy_board": [{"horizon_days": x.get("horizon_days"), "accuracy_3m": x.get("accuracy"), "samples_3m": x.get("samples"), "coverage_3m": x.get("coverage")} for x in (((pf.get("backtest") or {}).get("horizons_recent_3m")) or [])],
            "plain_steps": ["confidence 为 LLM 判断与历史准确率融合后的置信度。", "预测由 AI 综合多维度数据完成。"],
        },
        "cause_effect_chain": chain,
        "dual_source_market": {
            "selected_source": market_snapshot.get("source"),
            "selected_price": market_snapshot.get("last_price"),
            "eastmoney_price": (((dual_source_market or {}).get("sources") or {}).get("eastmoney") or {}).get("last_price"),
            "tdx_price": (((dual_source_market or {}).get("sources") or {}).get("tdx") or {}).get("last_price"),
            "diff_abs": ((dual_source_market or {}).get("comparison") or {}).get("last_price_diff_abs"),
            "diff_pct": ((dual_source_market or {}).get("comparison") or {}).get("last_price_diff_pct"),
            "price_consistent": ((dual_source_market or {}).get("comparison") or {}).get("price_consistent"),
            "dual_status": (dual_source_market or {}).get("dual_status"),
        },
        "terms": [
            {"term": "MA5", "plain_explain": "近5个交易日平均价格，反映短期方向。"},
            {"term": "MA20", "plain_explain": "近20个交易日平均价格，反映中期方向。"},
            {"term": "覆盖率", "plain_explain": "模型给出明确动作的样本占比。"},
        ],
    }


# ---------------------------------------------------------------------------
# View payload enrichment
# ---------------------------------------------------------------------------

def _ensure_list_for_view(val, *, split_lines: bool = False):
    if isinstance(val, list):
        return val
    if val is None:
        return []
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return []
        if split_lines:
            return [line.strip() for line in s.split("\n") if line.strip()]
        return [s]
    return []


def _plain_need_fallback(plain: dict) -> bool:
    for key in ("what_to_do_now", "key_numbers", "cause_effect_chain", "terms"):
        v = plain.get(key)
        if not isinstance(v, list) or len(v) == 0:
            return True
    return False


async def load_report_view_payload(report_payload: dict, stock_code: str) -> dict:
    report = dict(report_payload or {})
    report["stock_code"] = report.get("stock_code") or stock_code
    dims = dict(report.get("dimensions") or {})
    market_snapshot = dict(dims.get("market_snapshot") or {})
    market_features = dict(dims.get("market_features") or {})
    dual_source = dict(dims.get("market_dual_source") or {})

    if not dual_source.get("dual_status"):
        from_snapshot = _build_market_dual_source_from_snapshot(market_snapshot)
        dual_source = {**dual_source, **from_snapshot}

    need_live_quote = not dual_source.get("dual_status") or not dual_source.get("sources")
    if need_live_quote:
        try:
            live = await fetch_quote_snapshot(stock_code)
            if live:
                old_price = market_snapshot.get("last_price")
                live_price = live.get("last_price")
                replace_snapshot = old_price is None and live_price is not None
                if isinstance(old_price, (int, float)) and isinstance(live_price, (int, float)) and live_price > 0:
                    replace_snapshot = abs(old_price - live_price) / live_price > 0.2
                if replace_snapshot:
                    market_snapshot = live
                dual_source = _build_market_dual_source_from_snapshot(live)
        except Exception as exc:
            logger.warning("live_quote_refresh_failed stock=%s err=%s", stock_code, str(exc) or exc.__class__.__name__)

    dims["market_snapshot"] = market_snapshot
    dims["market_features"] = market_features
    dims["market_dual_source"] = dual_source
    report["dimensions"] = dims

    plain = dict(report.get("plain_report") or {})
    fallback_plain = _build_plain_fallback(report, market_snapshot, market_features, dual_source)
    if _plain_need_fallback(plain):
        plain = fallback_plain
    else:
        for list_key in ("key_numbers", "cause_effect_chain", "terms", "what_to_do_now"):
            if list_key in plain and not isinstance(plain[list_key], list):
                plain[list_key] = []
        if plain.get("evidence_backing_points") is not None and not isinstance(plain["evidence_backing_points"], list):
            plain["evidence_backing_points"] = _ensure_list_for_view(plain["evidence_backing_points"], split_lines=True)
    if not plain.get("execution_plan"):
        plain["execution_plan"] = fallback_plain.get("execution_plan")
    if not plain.get("accuracy_explain"):
        plain["accuracy_explain"] = fallback_plain.get("accuracy_explain")
    report["plain_report"] = plain

    if not report.get("direction_forecast") and report.get("price_forecast"):
        report["direction_forecast"] = (report.get("price_forecast") or {}).get("direction_forecast")

    rt = dict(report.get("reasoning_trace") or {})
    if rt.get("data_sources") is not None and not isinstance(rt["data_sources"], list):
        rt["data_sources"] = _ensure_list_for_view(rt["data_sources"], split_lines=True)
    if rt.get("analysis_steps") is not None and not isinstance(rt["analysis_steps"], list):
        rt["analysis_steps"] = _ensure_list_for_view(rt["analysis_steps"], split_lines=True)
    if rt.get("evidence_items") is not None and not isinstance(rt["evidence_items"], list):
        raw = rt["evidence_items"]
        if isinstance(raw, str) and raw.strip():
            rt["evidence_items"] = [{"title": raw.strip(), "summary": raw.strip()}]
        else:
            rt["evidence_items"] = []
    report["reasoning_trace"] = rt

    qg = dict(report.get("quality_gate") or {})
    if not isinstance(qg.get("missing_fields"), list):
        qg["missing_fields"] = []
    if not isinstance(qg.get("recover_actions"), list):
        qg["recover_actions"] = []
    report["quality_gate"] = qg

    _ic_defaults = {"signal_entry_price": None, "atr_pct": None, "atr_multiplier": None, "stop_loss": None, "target_price": None, "stop_loss_calc_mode": None}
    ic = report.get("instruction_card")
    if not ic or not isinstance(ic, dict):
        report["instruction_card"] = dict(_ic_defaults)
    else:
        for k, v in _ic_defaults.items():
            ic.setdefault(k, v)

    return report


# ---------------------------------------------------------------------------
# Template context builder
# ---------------------------------------------------------------------------

async def build_report_template_context_for_user(request: Request, view_report: dict) -> dict:
    from app.core.security import get_current_user_optional
    from app.services.strategy_failure import get_strategy_paused
    from app.core.display_text import humanize_status_reason

    user = await get_current_user_optional(request)
    subscription_state = _load_subscription_state(user) if user else None
    level = effective_viewer_tier_from_subscription_state(user, subscription_state) if user else "Free"
    is_admin = user is not None and is_admin_role(getattr(user, "role", None))
    is_paid = has_active_paid_membership_from_subscription_state(user, subscription_state)
    can_see_full = user and (is_paid or is_admin)
    return {
        "report": view_report,
        "current_user": user,
        "membership_level": level if user else None,
        "can_see_instruction_full": can_see_full,
        "can_see_forecast_14_60": can_see_full,
        "can_see_advanced": can_see_full,
        "strategy_paused": get_strategy_paused(),
        "humanize_status_reason": humanize_status_reason,
    }


def _load_subscription_state(user) -> dict | None:
    if user is None:
        return None
    from app.core.db import SessionLocal as _SL
    db = _SL()
    try:
        return subscription_status(db, str(user.user_id))
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Latest report ID / status
# ---------------------------------------------------------------------------

def latest_report_id_for_code(db, stock_code: str) -> str | None:
    row = (
        db.query(Report)
        .filter(Report.stock_code == stock_code)
        .order_by(Report.created_at.desc())
        .first()
    )
    return row.report_id if row else None


def report_status_payload(stock_code: str, db=None) -> dict:
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    try:
        target_trade_date = trade_date_str()
        rows = (
            db.query(Report)
            .filter(Report.stock_code == stock_code, Report.run_mode == "daily")
            .order_by(Report.created_at.desc())
            .limit(20)
            .all()
        )
        cached = None
        for row in rows:
            created_at = _to_utc(row.created_at)
            if not created_at:
                continue
            if trade_date_str(created_at) == target_trade_date:
                cached = row
                break
        status = _demo_status_snapshot(stock_code)
        if cached:
            status.update({"status": "done", "report_id": cached.report_id, "ready": True})
        else:
            status.update({"ready": False})
        return {"stock_code": stock_code, "job": status}
    finally:
        if close_db:
            db.close()
