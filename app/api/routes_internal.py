from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path

from fastapi import APIRouter, Body, Depends, HTTPException, Path as FastApiPath, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.core.response import envelope
from app.core.security import internal_auth
from app.models import (
    Base,
    CookieSession,
    DataSourceCircuitState,
    HotspotRaw,
    MarketHotspotItemSource,
    PredictionOutcome,
    Report,
    ReportGenerationTask,
    ReportFeedback,
    ReportIdempotency,
    SimAccount,
    SimBaseline,
    SimPosition,
    SimPositionBacktest,
)
from app.schemas import HotspotCollectRequest, LLMGenerateRequest
from app.services.hotspot import enrich_topic, fetch_douyin_hot, fetch_weibo_hot
from app.services.observability import runtime_metrics_summary
from app.services.ollama_client import ollama_client
from app.services.report_engine import collect_topics, run_regression
from app.services.source_state import get_source_runtime_status

try:
    from app.services.llm_router import get_primary_status
except ImportError:  # pragma: no cover
    def get_primary_status() -> str:  # type: ignore[misc]
        return "unknown"

try:
    from app.services.autonomy_loop_runtime import get_autonomy_loop_runtime
except ImportError:  # pragma: no cover
    get_autonomy_loop_runtime = None  # type: ignore[assignment]

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared artifact paths & snapshot for runtime/gates
# ---------------------------------------------------------------------------
_SHARED_ARTIFACT_PATHS: dict[str, Path] = {
    "junit": Path("output/junit.xml"),
    "catalog_snapshot": Path("output/catalog_snapshot.json"),
    "blind_spot_audit": Path("output/blind_spot_audit.json"),
    "continuous_audit": Path("output/latest_run.json"),
}


def _shared_artifact_snapshot() -> dict:
    """Read governance artifacts and return a summary dict."""
    result: dict[str, dict] = {}
    for key, path in _SHARED_ARTIFACT_PATHS.items():
        if not path.exists():
            result[key] = {"exists": False}
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            result[key] = {"exists": False}
            continue

        if key == "junit":
            import xml.etree.ElementTree as ET
            try:
                tree = ET.parse(path)
                root_el = tree.getroot()
                result[key] = {
                    "exists": True,
                    "failures": int(root_el.attrib.get("failures", 0)),
                    "errors": int(root_el.attrib.get("errors", 0)),
                }
            except Exception:
                result[key] = {"exists": False}
        elif key == "catalog_snapshot":
            result[key] = {
                "exists": True,
                "test_result_freshness": raw.get("test_result_freshness", "unknown"),
                "total_collected": raw.get("total_collected", 0),
            }
        elif key == "blind_spot_audit":
            # Support both new (summary.fake_count) and legacy (FAKE) shapes
            summary = raw.get("summary", {})
            if summary:
                result[key] = {
                    "exists": True,
                    "fake": summary.get("fake_count", 0),
                    "hollow": summary.get("hollow_count", 0),
                    "weak": summary.get("weak_count", 0),
                    "guarded": summary.get("guarded_assertions", 0),
                }
            else:
                result[key] = {
                    "exists": True,
                    "fake": raw.get("FAKE", 0),
                    "hollow": raw.get("HOLLOW", 0),
                    "weak": raw.get("WEAK", 0),
                    "guarded": raw.get("GUARDED", 0),
                }
        elif key == "continuous_audit":
            stats = raw.get("registry_stats", {})
            shared = raw.get("shared_artifact_status", {})
            result[key] = {
                "exists": True,
                "status": raw.get("status", "unknown"),
                "finding_count": len(raw.get("findings", [])),
                "warn_features": stats.get("warn_features", 0),
                "mismatch_count": stats.get("mismatch_count", 0),
                "catalog_total_collected": shared.get("catalog_total_collected", 0),
            }
        else:
            result[key] = {"exists": True}
    return result


def submit_report_generation_task(db, *, stock_code: str, trade_date: str | None = None,
                                   idempotency_key: str | None = None,
                                   request_id: str | None = None,
                                   force: bool = False, run_inline: bool = False) -> dict:
    """Submit a report generation task (stub — used via monkeypatch in tests)."""
    from uuid import uuid4 as _uuid4
    task_id = str(_uuid4())
    return {"task_id": task_id, "stock_code": stock_code, "status": "PENDING"}


router = APIRouter(prefix="/api/v1/internal", tags=["internal"], dependencies=[Depends(internal_auth)])


@router.post("/hotspot/collect")
async def hotspot_collect(
    payload: HotspotCollectRequest,
    platform: str = Query(..., pattern="^(weibo|douyin)$"),
    stock_code: str = Query("600519.SH"),
    db: Session = Depends(get_db),
):
    from app.services.ssot_read_model import report_storage_mode

    if report_storage_mode(db) == "ssot":
        raise HTTPException(status_code=404, detail="NOT_FOUND")
    topics = await (fetch_weibo_hot(payload.top_n) if platform == "weibo" else fetch_douyin_hot(payload.top_n))
    collect_topics(db, stock_code=stock_code, raw_topics=topics)
    return envelope(data={"count": len(topics), "platform": platform})


@router.post("/hotspot/enrich")
async def hotspot_enrich(db: Session = Depends(get_db)):
    hotspot_item = Base.metadata.tables.get("market_hotspot_item")
    if hotspot_item is None:
        return envelope(data={"total_candidates": 0, "enriched": 0, "items": []})

    rows = (
        db.execute(
            hotspot_item.select()
            .where(hotspot_item.c.fetch_time > datetime.now(timezone.utc) - timedelta(hours=24))
            .order_by(hotspot_item.c.merged_rank.asc(), hotspot_item.c.created_at.desc())
        )
        .mappings()
        .all()
    )

    items = []
    for row in rows:
        payload = {
            "topic_id": str(row.get("hotspot_item_id") or ""),
            "title": row.get("topic_title") or "",
            "heat_score": float(max(0, 100 - int(row.get("merged_rank") or 100))),
            "fetch_time": row.get("fetch_time"),
            "industry": "产业链",
            "stock_code": None,
            "stock_name": None,
            "last_price": None,
            "prev_close": None,
            "circulating_shares": None,
        }
        enriched = enrich_topic(payload)
        items.append(
            {
                "hotspot_item_id": str(row.get("hotspot_item_id") or ""),
                "sentiment_score": enriched.get("sentiment_score"),
                "event_type": enriched.get("event_type"),
                "decay_weight": enriched.get("decay_weight"),
            }
        )

    return envelope(data={"total_candidates": len(rows), "enriched": len(items), "items": items})


@router.get("/hotspot/health")
async def hotspot_health(db: Session = Depends(get_db)):
    last = db.query(HotspotRaw).order_by(HotspotRaw.fetch_time.desc()).first()
    is_degraded = last is None
    sources: list[dict] = []
    # Check MarketHotspotItemSource for per-source freshness
    now = datetime.now(timezone.utc)
    seen: dict[str, dict] = {}
    for row in db.query(MarketHotspotItemSource).order_by(MarketHotspotItemSource.fetch_time.desc()).all():
        src = row.source_name
        if src in seen:
            continue
        ft = row.fetch_time
        if ft and ft.tzinfo is None:
            ft = ft.replace(tzinfo=timezone.utc)
        age_h = round((now - ft).total_seconds() / 3600, 2) if ft else None
        freshness = "fresh" if age_h is not None and age_h < 1 else ("degraded" if age_h is not None and age_h < 24 else "stale")
        seen[src] = {"source_name": src, "freshness": freshness, "age_hours": age_h, "last_fetch": ft.isoformat() if ft else None}
    sources = list(seen.values())
    if not sources and last is None:
        is_degraded = True
    elif any(s["freshness"] != "fresh" for s in sources):
        is_degraded = True
    return envelope(
        data={"status": "degraded" if is_degraded else "ok", "last_fetch": getattr(last, "fetch_time", None), "sources": sources},
        degraded=is_degraded,
        degraded_reason="cold_start_no_hotspot_data" if (last is None and not sources) else None,
    )


@router.post("/stocks/{stock_code}/non-report-data/collect")
async def collect_non_report_data(
    stock_code: str = FastApiPath(..., pattern=r"^\d{6}\.(SH|SZ|BJ)$"),
    trade_date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    db: Session = Depends(get_db),
):
    from app.services.stock_snapshot_service import collect_non_report_usage

    payload = await collect_non_report_usage(
        db,
        stock_code=stock_code,
        trade_date=trade_date,
    )
    return envelope(data=payload)


@router.post("/cookie/refresh")
async def cookie_refresh(platform: str = Query(..., pattern="^(weibo|douyin|eastmoney)$"), db: Session = Depends(get_db)):
    from app.services.cookie_session_ssot import execute_cookie_probe
    probe = execute_cookie_probe(db, login_source=platform)
    if probe.get("outcome") == "skipped" and probe.get("reason") == "no_session":
        return envelope(data={"platform": platform, "status": "SKIPPED", "status_reason": "no_session"})
    # Re-read the session row for response fields
    row = db.query(CookieSession).filter(CookieSession.provider == platform).order_by(CookieSession.updated_at.desc()).first()
    status = row.status if row else "unknown"
    status_reason = row.status_reason if row else None
    last_probe_at = row.last_probe_at.isoformat() if row and row.last_probe_at else None
    last_refresh_at = row.last_refresh_at.isoformat() if row and row.last_refresh_at else None
    return envelope(data={
        "platform": platform,
        "status": status,
        "status_reason": status_reason,
        "last_probe_at": last_probe_at,
        "last_refresh_at": last_refresh_at,
        "expires_at": row.expires_at.isoformat() if row and row.expires_at else None,
    })


@router.get("/source/fallback-status")
async def source_fallback_status(db: Session = Depends(get_db)):
    rows = db.query(DataSourceCircuitState).all()
    runtime = get_source_runtime_status()
    is_degraded = len(rows) == 0
    circuits = []
    for r in rows:
        item: dict = {
            "source_name": r.source_name,
            "circuit_state": r.circuit_state,
            "consecutive_failures": r.consecutive_failures,
            "cooldown_until": r.cooldown_until.isoformat() if r.cooldown_until else None,
        }
        for kind in ("market", "hotspot"):
            bucket = runtime.get(kind, {})
            if r.source_name in bucket:
                rt = bucket[r.source_name]
                item["runtime_source_kind"] = kind
                item["runtime_circuit_open"] = rt.get("circuit_open", False)
                item["runtime_last_error"] = rt.get("last_error")
                if rt.get("circuit_open"):
                    is_degraded = True
                break
        circuits.append(item)
    return envelope(
        data={
            "hotspot_chain": "public+browser_fallback",
            "market_chain": "eastmoney+tdx+fallback",
            "status": "degraded" if is_degraded else "normal",
            "status_reason": "cold_start_no_circuit_rows" if len(rows) == 0 else None,
            "circuits": circuits,
            "runtime": runtime,
        },
        degraded=is_degraded,
        degraded_reason="cold_start_no_circuit_rows" if len(rows) == 0 else None,
    )


@router.post("/llm/generate")
async def llm_generate(payload: LLMGenerateRequest):
    raise HTTPException(status_code=410, detail="ROUTE_RETIRED")


@router.get("/llm/health")
async def llm_health():
    # Ollama health (original behavior, wrapped with error handling)
    try:
        data = await ollama_client.health()
    except Exception as exc:
        data = {"status": "degraded", "error": str(exc), "models": [], "tags": []}

    is_degraded = data.get("status") == "degraded"
    return envelope(
        data={"status": data.get("status", "ok"), "tags": data.get("models", []) or data.get("tags", [])},
        degraded=is_degraded if is_degraded else None,
        degraded_reason=data.get("error") if is_degraded else None,
    )


@router.get("/llm/version")
async def llm_version():
    return envelope(data={"test_model": ollama_client.model_name(False), "prod_model": ollama_client.model_name(True)})


def _iso_datetime_or_none(value):
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _iso_date_or_none(value):
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


@router.post("/reports/generate")
async def internal_reports_generate(payload: dict = Body(...), db: Session = Depends(get_db)):
    del payload, db
    raise HTTPException(status_code=410, detail="ROUTE_RETIRED")


@router.post("/reports/generate-batch")
async def internal_reports_generate_batch(payload: dict = Body(...), db: Session = Depends(get_db)):
    """Batch-generate reports for multiple stock codes concurrently.

    Uses NewAPI 12-relay token pool with configurable concurrency.
    Request: {"stock_codes": ["600519.SH", ...], "trade_date": "2026-04-12", "force": false}
    """
    from app.services.report_generation_ssot import (
        REPORT_GENERATION_ROUND_LIMIT,
        cleanup_incomplete_reports,
        generate_reports_batch,
    )
    from app.core.db import SessionLocal

    stock_codes = payload.get("stock_codes")
    if not stock_codes or not isinstance(stock_codes, list):
        raise HTTPException(status_code=400, detail="INVALID_PAYLOAD: stock_codes must be a non-empty list")
    if len(stock_codes) > REPORT_GENERATION_ROUND_LIMIT:
        raise HTTPException(
            status_code=400,
            detail=f"INVALID_PAYLOAD: max {REPORT_GENERATION_ROUND_LIMIT} stock_codes per round",
        )

    normalized = [str(c).strip().upper() for c in stock_codes if str(c).strip()]
    if not normalized:
        raise HTTPException(status_code=400, detail="INVALID_PAYLOAD: no valid stock_codes")

    cleanup_summary = None
    if bool(payload.get("cleanup_incomplete_before_batch", True)):
        cleanup_limit = int(payload.get("cleanup_limit") or 500)
        cleanup_include_non_ok = bool(payload.get("include_non_ok", True))
        cleanup_summary = cleanup_incomplete_reports(
            db,
            limit=cleanup_limit,
            include_non_ok=cleanup_include_non_ok,
        )

    result = generate_reports_batch(
        db_factory=SessionLocal,
        stock_codes=normalized,
        trade_date=payload.get("trade_date"),
        skip_pool_check=bool(payload.get("skip_pool_check", False)),
        force_same_day_rebuild=bool(payload.get("force", False)),
        max_concurrent_override=payload.get("max_concurrent"),
        # Internal batch generation is hard-limited to one stock per strategy type
        # so each run can produce at most 3 reports (A/B/C one each).
        one_per_strategy_type=True,
        # OPT-12: 支持按股票代码强制策略类型（{"600519.SH": "A"} 等）
        strategy_type_override_map=payload.get("strategy_type_override_map"),
    )
    if cleanup_summary is not None:
        result = dict(result or {})
        result["cleanup_incomplete_before_batch"] = cleanup_summary
    return JSONResponse(status_code=202, content=jsonable_encoder(envelope(data=result, code=202, message="batch_accepted")))


@router.post("/reports/cleanup-incomplete")
async def internal_reports_cleanup_incomplete(payload: dict = Body(default={}), db: Session = Depends(get_db)):
    from app.services.report_generation_ssot import cleanup_incomplete_reports

    limit = int(payload.get("limit") or 500)
    dry_run = bool(payload.get("dry_run", False))
    kwargs = {
        "limit": limit,
        "dry_run": dry_run,
        "include_non_ok": bool(payload.get("include_non_ok", True)),
    }
    result = cleanup_incomplete_reports(db, **kwargs)
    if not dry_run and int(result.get("soft_deleted") or 0) > 0:
        db.commit()
    else:
        db.rollback()
    return envelope(data=result)


@router.post("/reports/cleanup-incomplete-all")
async def internal_reports_cleanup_incomplete_all(payload: dict = Body(default={}), db: Session = Depends(get_db)):
    from app.services.report_generation_ssot import cleanup_incomplete_reports_until_clean

    batch_limit = int(payload.get("batch_limit") or 500)
    max_batches = int(payload.get("max_batches") or 20)
    dry_run = bool(payload.get("dry_run", False))
    kwargs = {
        "batch_limit": batch_limit,
        "max_batches": max_batches,
        "dry_run": dry_run,
        "include_non_ok": bool(payload.get("include_non_ok", True)),
    }
    result = cleanup_incomplete_reports_until_clean(db, **kwargs)
    if not dry_run and int(result.get("total_soft_deleted") or 0) > 0:
        db.commit()
    else:
        db.rollback()
    return envelope(data=result)


@router.get("/reports/incomplete-status")
async def internal_reports_incomplete_status(limit: int = Query(500, ge=1, le=5000), db: Session = Depends(get_db)):
    from app.services.report_generation_ssot import cleanup_incomplete_reports

    result = cleanup_incomplete_reports(db, limit=limit, dry_run=True, include_non_ok=True)
    db.rollback()
    data = {
        "all_reports_complete": int(result.get("candidates") or 0) == 0,
        "incomplete_candidates": int(result.get("candidates") or 0),
        "scanned": int(result.get("scanned") or 0),
        "reason": result.get("reason"),
        "candidate_examples": result.get("candidate_examples") or [],
    }
    return envelope(data=data)


@router.get("/reports/tasks/{task_id}")
async def internal_report_task_status(task_id: str, db: Session = Depends(get_db)):
    task = db.get(ReportGenerationTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="TASK_NOT_FOUND")

    report = (
        db.query(Report)
        .filter(Report.generation_task_id == task_id)
        .order_by(Report.created_at.desc())
        .first()
    )
    return envelope(
        data={
            "task_id": task.task_id,
            "trade_date": _iso_date_or_none(task.trade_date),
            "stock_code": task.stock_code,
            "idempotency_key": task.idempotency_key,
            "generation_seq": task.generation_seq,
            "status": task.status,
            "retry_count": task.retry_count,
            "quality_flag": task.quality_flag,
            "status_reason": task.status_reason,
            "llm_fallback_level": task.llm_fallback_level,
            "risk_audit_status": task.risk_audit_status,
            "risk_audit_skip_reason": task.risk_audit_skip_reason,
            "report_id": getattr(report, "report_id", None),
            "request_id": task.request_id,
            "queued_at": _iso_datetime_or_none(task.queued_at),
            "started_at": _iso_datetime_or_none(task.started_at),
            "finished_at": _iso_datetime_or_none(task.finished_at),
            "updated_at": _iso_datetime_or_none(task.updated_at),
        }
    )


@router.post("/eval/run-regression")
async def eval_regression(db: Session = Depends(get_db)):
    from app.services.ssot_read_model import report_storage_mode

    if report_storage_mode(db) == "ssot":
        raise HTTPException(status_code=404, detail="NOT_FOUND")
    row = run_regression(db)
    return envelope(data={"experiment_id": row.id, "decision": row.decision})


@router.get("/metrics/summary")
async def metrics_summary(db: Session = Depends(get_db)):
    return envelope(data=runtime_metrics_summary(db))


@router.post("/reports/clear")
async def reports_clear():
    """Retired route — no longer available."""
    raise HTTPException(status_code=410, detail="ROUTE_RETIRED")


@router.post("/stats/clear")
async def stats_clear():
    """Retired route — no longer available."""
    raise HTTPException(status_code=410, detail="ROUTE_RETIRED")


@router.get("/runtime/gates")
async def runtime_gates(db: Session = Depends(get_db)):
    """Composite gate check for autonomous loop / promote decisions."""
    metrics = runtime_metrics_summary(db)
    artifacts = _shared_artifact_snapshot()
    llm_status = get_primary_status()

    # --- runtime live recovery gate ---
    runtime_flags = metrics.get("runtime_flags", [])
    blocking_flags = [f for f in runtime_flags if f in (
        "sim_snapshot_missing", "public_runtime_degraded",
        "market_state_stale", "source_circuit_open",
    )]
    runtime_allowed = len(blocking_flags) == 0

    # --- shared artifact promote gate ---
    blind = artifacts.get("blind_spot_audit", {})
    blind_clean = (
        blind.get("exists", False)
        and blind.get("fake", 0) == 0
        and blind.get("hollow", 0) == 0
        and blind.get("weak", 0) == 0
        and blind.get("guarded", 0) == 0
    )
    audit = artifacts.get("continuous_audit", {})
    audit_complete = (
        audit.get("exists", False)
        and audit.get("status") == "completed"
        and audit.get("warn_features", 0) == 0
        and audit.get("mismatch_count", 0) == 0
    )
    catalog_snap = artifacts.get("catalog_snapshot", {})
    catalog_total = catalog_snap.get("total_collected", 0)
    audit_catalog_total = audit.get("catalog_total_collected", 0)
    same_round = catalog_total > 0 and catalog_total == audit_catalog_total
    promote_allowed = blind_clean and audit_complete and same_round and runtime_allowed

    # --- overall status ---
    if not runtime_allowed:
        overall = "blocked"
    elif not promote_allowed:
        overall = "degraded"
    else:
        overall = "ready"

    degraded = overall != "ready"
    degraded_reason = "runtime_live_recovery_blocked" if overall == "blocked" else None

    return envelope(
        data={
            "status": overall,
            "runtime_live_recovery": {
                "allowed": runtime_allowed,
                "blocking_flags": blocking_flags,
            },
            "shared_artifact_promote": {
                "allowed": promote_allowed,
                "blind_spot_clean": blind_clean,
                "continuous_audit_complete": audit_complete,
                "artifacts_same_round": same_round,
            },
            "llm_router": {
                "ready": llm_status == "ok",
                "status": llm_status,
            },
            "artifacts": artifacts,
        },
        degraded=degraded if degraded else None,
        degraded_reason=degraded_reason,
    )


# ── Audit context ────────────────────────────────────────

_LOOP_CONTROLLER_STATE_PATH = Path("runtime/loop_controller/state.json")
_ISSUE_MESH_RUNTIME_ROOT = Path("runtime/issue_mesh")
_CODE_FIX_RUNTIME_ROOT = Path("runtime/issue_mesh/promote_prep/code_fix")


def _read_json_safe(p: Path) -> dict | None:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _latest_issue_mesh_snapshot(run_id: str | None) -> dict | None:
    if not run_id:
        return None
    run_dir = _ISSUE_MESH_RUNTIME_ROOT / run_id
    status = _read_json_safe(run_dir / "status.json")
    if status is None:
        return None
    bundle = _read_json_safe(run_dir / "bundle.json") or {}
    return {
        "run_id": run_id,
        "status": status.get("status"),
        "finding_count": bundle.get("finding_count", 0),
    }


def _latest_code_fix_snapshot(fix_wave_id: str | None) -> dict | None:
    if not fix_wave_id:
        return None
    wave_dir = _CODE_FIX_RUNTIME_ROOT / fix_wave_id
    patches = _read_json_safe(wave_dir / "patches.json")
    if patches is None:
        return None
    pytest_result = _read_json_safe(wave_dir / "pytest_result.json") or {}
    return {
        "fix_run_id": fix_wave_id,
        "patch_count": patches.get("patch_count", 0),
        "pytest_passed": pytest_result.get("passed"),
    }


@router.get("/audit/context")
async def audit_context(db: Session = Depends(get_db)):
    """Consolidated runtime + doc + automation snapshot for autonomous audit."""
    metrics = runtime_metrics_summary(db)
    artifacts = _shared_artifact_snapshot()
    llm_status = get_primary_status()

    # --- reuse runtime_gates logic ---
    runtime_flags = metrics.get("runtime_flags", [])
    blocking_flags = [f for f in runtime_flags if f in (
        "sim_snapshot_missing", "public_runtime_degraded",
        "market_state_stale", "source_circuit_open",
    )]
    runtime_allowed = len(blocking_flags) == 0

    blind = artifacts.get("blind_spot_audit", {})
    blind_clean = (
        blind.get("exists", False)
        and blind.get("fake", 0) == 0
        and blind.get("hollow", 0) == 0
        and blind.get("weak", 0) == 0
        and blind.get("guarded", 0) == 0
    )
    audit_complete = artifacts.get("continuous_audit", {}).get("status") == "completed"
    catalog_a = artifacts.get("catalog_snapshot", {}).get("total_collected", 0)
    catalog_b = artifacts.get("continuous_audit", {}).get("catalog_total_collected", 0)
    same_round = catalog_a > 0 and catalog_a == catalog_b
    promote_allowed = blind_clean and audit_complete and same_round and runtime_allowed

    if not runtime_allowed:
        overall = "blocked"
    elif not promote_allowed:
        overall = "degraded"
    else:
        overall = "ready"

    # --- public runtime status ---
    runtime_state = metrics.get("runtime_state", "unknown")
    anchors = metrics.get("runtime_anchors", {})
    latest_published = anchors.get("latest_published_report_trade_date")

    # --- automation snapshots ---
    loop_state = _read_json_safe(_LOOP_CONTROLLER_STATE_PATH)
    automation: dict = {}
    if loop_state:
        automation["loop_controller"] = {
            "phase": loop_state.get("phase"),
            "mode": loop_state.get("mode"),
            "current_round_id": loop_state.get("current_round_id"),
            "latest_audit_run_id": loop_state.get("last_audit_run_id"),
            "latest_fix_wave_id": loop_state.get("last_fix_wave_id"),
        }
        automation["latest_issue_mesh_run"] = _latest_issue_mesh_snapshot(
            loop_state.get("last_audit_run_id")
        )
        automation["latest_code_fix_wave"] = _latest_code_fix_snapshot(
            loop_state.get("last_fix_wave_id")
        )

    # --- promote readiness ---
    promote_status = "promote_ready" if overall == "ready" else "blocked"
    automation["promote_readiness"] = {"status": promote_status}

    degraded = overall != "ready"
    degraded_reason = "runtime_live_recovery_blocked" if overall == "blocked" else None

    return envelope(
        data={
            "runtime_gates": {
                "status": overall,
                "runtime_live_recovery": {
                    "allowed": runtime_allowed,
                    "blocking_flags": blocking_flags,
                },
                "shared_artifact_promote": {
                    "allowed": promote_allowed,
                },
                "llm_router": {"status": llm_status},
            },
            "latest_published_report_trade_date": latest_published,
            "public_runtime_status": runtime_state,
            "docs": {
                "progress_doc_path": "docs/core/22_全量功能进度总表_v7_精审.md",
                "analysis_lens_doc_path": "docs/core/25_系统问题分析角度清单.md",
            },
            "automation": automation,
        },
        degraded=degraded if degraded else None,
        degraded_reason=degraded_reason,
    )


# ---------------------------------------------------------------------------
# Autonomy loop control routes
# ---------------------------------------------------------------------------

def _autonomy_runtime():
    """Get or raise 503 if autonomy loop runtime is unavailable."""
    if get_autonomy_loop_runtime is None:
        raise HTTPException(status_code=503, detail="Autonomy loop not available")
    return get_autonomy_loop_runtime()


@router.get("/autonomy/loop")
async def autonomy_loop_status():
    runtime = _autonomy_runtime()
    return envelope(data=runtime.status())


@router.get("/autonomy/loop/state")
async def autonomy_loop_state():
    runtime = _autonomy_runtime()
    return envelope(data=runtime.status())


@router.post("/autonomy/loop/start")
async def autonomy_loop_start(body: dict = Body(default={})):
    runtime = _autonomy_runtime()
    payload = runtime.start(
        mode=body.get("mode"),
        fix_goal=body.get("fix_goal"),
        force_new_round=body.get("force_new_round", False),
        reason=body.get("reason", "api_start"),
    )
    return envelope(data=payload)


@router.post("/autonomy/loop/stop")
async def autonomy_loop_stop(body: dict = Body(default={})):
    runtime = _autonomy_runtime()
    payload = runtime.stop(reason=body.get("reason", "manual_stop"))
    return envelope(data=payload)


@router.get("/autonomy/loop/await-round")
async def autonomy_loop_await_round(timeout_seconds: int = Query(600)):
    runtime = _autonomy_runtime()
    payload = runtime.await_round(timeout_seconds=timeout_seconds)
    return envelope(data=payload)


def _add_legacy_aliases(data: dict) -> dict:
    """Map modern field names to legacy aliases."""
    out = dict(data)
    out.setdefault("success_round_streak", data.get("goal_progress_count"))
    out.setdefault("success_round_goal", data.get("fix_goal"))
    out.setdefault("total_rounds", data.get("round_history_size"))
    out.setdefault("round_seq", data.get("round_history_size"))
    out.setdefault("fixed_problem_ids", data.get("fixed_problems"))
    return out


@router.get("/automation/fix-loop/state")
async def legacy_fix_loop_state():
    runtime = _autonomy_runtime()
    return envelope(data=_add_legacy_aliases(runtime.status()))


@router.post("/automation/fix-loop/start")
async def legacy_fix_loop_start(body: dict = Body(default={})):
    runtime = _autonomy_runtime()
    payload = runtime.start(
        mode=body.get("mode"),
        fix_goal=body.get("success_round_goal"),
        force_new_round=body.get("force_new_round", False),
        reason="legacy_internal_api_start",
    )
    result = _add_legacy_aliases(payload)
    result["success_round_goal"] = body.get("success_round_goal") or result.get("fix_goal")
    return envelope(data=result)


@router.post("/automation/fix-loop/force-round")
async def legacy_fix_loop_force_round(body: dict = Body(default={})):
    runtime = _autonomy_runtime()
    payload = runtime.force_new_round(
        mode=body.get("mode"),
        fix_goal=body.get("success_round_goal"),
        reason="legacy_internal_api_force_round",
    )
    result = _add_legacy_aliases(payload)
    result["success_round_goal"] = body.get("success_round_goal") or result.get("fix_goal")
    return envelope(data=result)


@router.get("/automation/fix-loop/await-round")
async def legacy_fix_loop_await_round(timeout_seconds: int = Query(600)):
    runtime = _autonomy_runtime()
    payload = runtime.await_round(timeout_seconds=timeout_seconds)
    return envelope(data=payload)


@router.post("/automation/fix-loop/stop")
async def legacy_fix_loop_stop(body: dict = Body(default={})):
    runtime = _autonomy_runtime()
    payload = runtime.stop(reason=body.get("reason", "manual_stop"))
    result = _add_legacy_aliases(payload)
    return envelope(data=result)


# ── Internal Settlement Routes ───────────────────────────

class _InternalSettlementRunRequest(BaseModel):
    trade_date: str
    window_days: int
    target_scope: str
    target_report_id: str | None = None
    target_stock_code: str | None = None
    force: bool = False


@router.post("/settlement/run", status_code=202)
async def internal_settlement_run(
    payload: _InternalSettlementRunRequest,
    db: Session = Depends(get_db),
):
    """Internal settlement trigger (no admin auth required, uses internal_auth)."""
    from app.services.settlement_ssot import (
        SettlementServiceError,
        get_settlement_task_status,
        submit_settlement_task,
    )

    try:
        data = submit_settlement_task(
            db,
            trade_date=payload.trade_date,
            window_days=payload.window_days,
            target_scope=payload.target_scope,
            target_report_id=payload.target_report_id,
            target_stock_code=payload.target_stock_code,
            force=payload.force,
            request_id=None,
            requested_by_user_id=None,
            run_inline=False,
        )
        db.commit()
        return envelope(data=data)
    except SettlementServiceError as exc:
        db.rollback()
        raise HTTPException(status_code=exc.status_code, detail=exc.error_code) from exc


@router.get("/settlement/tasks/{task_id}")
async def internal_settlement_task_status(
    task_id: str,
    db: Session = Depends(get_db),
):
    """Query settlement task status."""
    from app.services.settlement_ssot import SettlementServiceError, get_settlement_task_status

    try:
        task = get_settlement_task_status(db, task_id=task_id)
    except SettlementServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.error_code) from exc
    if not task:
        raise HTTPException(status_code=404, detail="NOT_FOUND")
    return envelope(data=task)
