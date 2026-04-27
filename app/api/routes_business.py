from datetime import date
from time import monotonic

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import and_, func, or_, text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.db import get_db
from app.core.request_context import get_request_id
from app.core.response import envelope
from app.core.security import get_current_user_optional
from app.models import Report, ReportFeedback, User
from app.schemas import (
    BillingCallbackRequest,
    BillingCreateOrderRequest,
    GenerateReportRequest,
    PredictionSettleRequest,
    ReportFeedbackRequest,
)
from app.services.membership import create_order, get_plans_config, handle_callback, subscription_status
from app.services.reports_query import (
    get_report_access_state_ssot,
    get_report_advanced_payload_ssot,
    get_report_api_payload_ssot,
    get_report_view_payload_ssot,
    list_report_summaries_ssot,
)
from app.services.report_engine import prediction_stats, settle_prediction
from app.services.report_generation_ssot import (
    ReportGenerationServiceError,
    ensure_non_report_usage_collected_if_needed,
    ensure_test_generation_context,
    generate_report_ssot,
)
from app.services.stock_pool import get_daily_stock_pool
from app.services.home_query import get_home_payload_ssot
from app.services.ssot_read_model import (
    count_reports_ssot,
    get_public_pool_snapshot_ssot,
    report_storage_mode,
)
try:
    from app.services.runtime_anchor_service import RuntimeAnchorService
except ImportError:
    RuntimeAnchorService = None  # type: ignore[assignment,misc]

router = APIRouter(prefix="/api/v1", tags=["business"])

_HOME_CACHE_TTL = 300  # seconds (5 min)
_home_cache = {"data": None, "cache_key": None, "ts": 0.0}
# CR-20260416-08: protect module-level cache from concurrent mutation (uvicorn threaded routes).
import threading as _threading
_home_cache_lock = _threading.Lock()


def _redact_public_report_operational_fields(payload: dict, *, viewer_role: str | None) -> dict:
    role = (viewer_role or "").strip().lower()
    if role in {"admin", "super_admin"}:
        return payload

    redacted = dict(payload)
    for field_name in ("llm_actual_model", "llm_provider_name", "llm_endpoint"):
        redacted.pop(field_name, None)

    if isinstance(redacted.get("used_data"), list):
        redacted["used_data"] = [
            {
                key: value
                for key, value in dict(item or {}).items()
                if key not in {"usage_id", "batch_id"}
            }
            for item in redacted["used_data"]
        ]
    return redacted


def get_home_payload_cached(
    db: Session,
    *,
    viewer_tier: str | None = None,
    viewer_role: str | None = None,
    window_days: int = 30,
) -> dict:
    runtime_anchor_service = RuntimeAnchorService(db) if RuntimeAnchorService is not None else None
    if runtime_anchor_service is not None:
        cache_key = runtime_anchor_service.home_cache_key(
            viewer_tier=viewer_tier,
            viewer_role=viewer_role,
            window_days=window_days,
        )
    else:
        cache_key = (
            "home-payload",
            str(viewer_tier or "Free"),
            str(viewer_role or "").lower(),
            window_days,
        )

    now = monotonic()
    with _home_cache_lock:
        if (
            _home_cache.get("cache_key") == cache_key
            and _home_cache.get("data") is not None
            and now - float(_home_cache.get("ts") or 0.0) < _HOME_CACHE_TTL
        ):
            return _home_cache["data"]

    payload = get_home_payload_ssot(
        db,
        viewer_tier=viewer_tier,
        viewer_role=viewer_role,
        runtime_anchor_service=runtime_anchor_service,
    )
    with _home_cache_lock:
        _home_cache.update({"data": payload, "cache_key": cache_key, "ts": now})
    return payload


def _report_list_item(row: Report) -> dict:
    c = row.content_json or {}
    status = "degraded" if c.get("degraded_mode") or c.get("status") == "degraded" else "normal"
    co = c.get("company_overview") or {}
    stock_name = co.get("company_name") or getattr(row, "stock_name_snapshot", None) or row.stock_code
    sti = c.get("sim_trade_instruction") or {}
    strategy_type = getattr(row, "strategy_type", None) or sti.get("strategy_type")
    market_state = getattr(row, "market_state", None) or sti.get("market_state")
    quality_flag = getattr(row, "quality_flag", None) or "ok"
    return {
        "report_id": row.report_id,
        "stock_code": row.stock_code,
        "stock_name": stock_name,
        "trade_date": row.trade_date or c.get("trade_date", ""),
        "run_mode": row.run_mode,
        "recommendation": row.recommendation,
        "confidence": row.confidence,
        "created_at": row.created_at.isoformat() if row.created_at else "",
        "status": status,
        "strategy_type": strategy_type,
        "market_state": market_state,
        "quality_flag": quality_flag,
        "published": getattr(row, "published", None),
    }


def _escape_like(s: str) -> str:
    """Escape % and _ for LIKE to avoid injection."""
    return (s or "").replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _find_active_report_id(
    db: Session,
    *,
    stock_code: str,
    trade_date: str,
    idempotency_key: str,
) -> str | None:
    row = db.execute(
        text(
            """
            SELECT report_id
            FROM report
            WHERE is_deleted = 0
              AND superseded_by_report_id IS NULL
              AND (idempotency_key = :idempotency_key OR (stock_code = :stock_code AND trade_date = :trade_date))
            ORDER BY created_at DESC
            LIMIT 1
            """
        ),
        {
            "idempotency_key": idempotency_key,
            "stock_code": stock_code,
            "trade_date": trade_date,
        },
    ).mappings().first()
    return str(row["report_id"]) if row else None


@router.get("/reports")
async def reports_list(
    request: Request,
    db: Session = Depends(get_db),
    stock_code: str | None = Query(None, pattern=r"^\d{6}\.(SH|SZ)$"),
    stock_name: str | None = Query(None, max_length=64),
    trade_date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    today: int | None = Query(None, ge=0, le=1),
    date_from: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    date_to: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    conclusion: str | None = Query(None, pattern="^(BUY|SELL|HOLD)$"),
    recommendation: str | None = Query(None, pattern="^(BUY|SELL|HOLD)$"),
    strategy_type: str | None = Query(None, pattern="^(A|B|C)$"),
    position_status: str | None = Query(None, pattern="^(OPEN|CLOSED_SL|CLOSED_T1|CLOSED_T2|CLOSED_EXPIRED)$"),
    market_state: str | None = Query(None, pattern="^(BULL|NEUTRAL|BEAR)$"),
    search: str | None = Query(None, alias="q", max_length=128),
    limit: int | None = Query(None, ge=1, le=500),
    run_mode: str | None = Query(None, pattern="^(daily|hourly)$"),
    in_pool: int | None = Query(None, ge=0, le=1, description="1=仅返回股票池内研报（24 §7）"),
    exclude_test: int = Query(1, ge=0, le=1, description="1=默认排除测试样本（source=test）"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    sort: str = Query("-created_at", pattern="^(-)?(created_at|confidence)$"),
    quality_flag: str | None = Query(None, max_length=32),
):
    from app.services.trade_calendar import latest_trade_date_str

    if conclusion and recommendation and conclusion != recommendation:
        raise HTTPException(status_code=422, detail="VALIDATION_FAILED")
    concl = conclusion or recommendation
    if today == 1 and trade_date is None:
        trade_date = latest_trade_date_str()
    current_user = await get_current_user_optional(request)
    pool_codes = get_daily_stock_pool() if in_pool == 1 else None
    normalized_sort = {
        "-created_at": "-trade_date",
        "created_at": "trade_date",
    }.get(sort, sort)

    if run_mode:
        query = db.query(Report).filter(Report.run_mode == run_mode)
        report_ids = [row.report_id for row in query.all()]
        if not report_ids:
            return envelope(data={"items": [], "page": page, "page_size": 0 if limit is not None else page_size, "total": 0, "data_status": "READY"})
        ssot_payload = list_report_summaries_ssot(
            db,
            viewer_tier=getattr(current_user, "tier", None) if current_user else None,
            viewer_role=getattr(current_user, "role", None) if current_user else None,
            stock_code=stock_code,
            stock_name=stock_name,
            trade_date=trade_date,
            date_from=date_from,
            date_to=date_to,
            recommendation=concl,
            strategy_type=strategy_type,
            position_status=position_status,
            market_state=market_state,
            quality_flag=quality_flag,
            search=search,
            limit=limit,
            in_pool_codes=pool_codes,
            page=page,
            page_size=page_size,
            sort=normalized_sort,
        )
        filtered_items = [item for item in ssot_payload["items"] if item["report_id"] in report_ids]
        return envelope(data={
            "items": filtered_items,
            "page": 1 if limit is not None else page,
            "page_size": len(filtered_items) if limit is not None else page_size,
            "total": len(filtered_items),
            "data_status": ssot_payload["data_status"],
        })

    ssot_payload = list_report_summaries_ssot(
        db,
        viewer_tier=getattr(current_user, "tier", None) if current_user else None,
        viewer_role=getattr(current_user, "role", None) if current_user else None,
        stock_code=stock_code,
        stock_name=stock_name,
        trade_date=trade_date,
        date_from=date_from,
        date_to=date_to,
        recommendation=concl,
        strategy_type=strategy_type,
        position_status=position_status,
        market_state=market_state,
        quality_flag=quality_flag,
        search=search,
        limit=limit,
        in_pool_codes=pool_codes,
        page=page,
        page_size=page_size,
        sort=normalized_sort,
    )
    items = ssot_payload["items"]
    if exclude_test == 1:
        items = [item for item in items if item.get("report_id")]
    return envelope(data={
        "items": items,
        "page": 1 if limit is not None else page,
        "page_size": len(items) if limit is not None else page_size,
        "total": ssot_payload["total"],
        "data_status": ssot_payload["data_status"],
    })


def _build_hot_stock_items(db: Session, *, limit: int) -> tuple[list[dict], str]:
    pool_codes = get_daily_stock_pool() or []
    source = "pool"
    codes = pool_codes[:limit]
    items: list[dict] = []
    for idx, stock_code in enumerate(codes):
        row = db.query(Report).filter(Report.stock_code == stock_code).order_by(Report.created_at.desc()).first()
        stock_name = stock_code
        if row and row.stock_name_snapshot:
            stock_name = row.stock_name_snapshot
        items.append(
            {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "rank": idx + 1,
                "source_name": source,
            }
        )
    return items, source


@router.get("/reports/featured")
async def reports_featured(
    request: Request,
    db: Session = Depends(get_db),
    limit: int = Query(6, ge=1, le=50),
):
    """兼容入口：默认仅返回 quality=ok 的精选研报列表。"""
    current_user = await get_current_user_optional(request)
    ssot_payload = list_report_summaries_ssot(
        db,
        viewer_tier=getattr(current_user, "tier", None) if current_user else None,
        viewer_role=getattr(current_user, "role", None) if current_user else None,
        in_pool_codes=get_daily_stock_pool(),
        limit=limit,
        page=1,
        page_size=limit,
        sort="-trade_date",
    )
    return envelope(
        data={
            "items": ssot_payload.get("items", []),
            "total": len(ssot_payload.get("items", [])),
            "data_status": ssot_payload.get("data_status"),
        }
    )


@router.get("/stocks")
async def stocks_search(
    db: Session = Depends(get_db),
    q: str = Query(None, min_length=1, max_length=20, description="股票代码或名称前缀"),
    limit: int = Query(10, ge=1, le=50),
):
    """股票搜索/自动补全：支持代码前缀（000001）或名称前缀，自动补全 .SH/.SZ 后缀。"""
    from app.models import StockMaster  # noqa: PLC0415
    q_strip = (q or "").strip()
    if not q_strip:
        return envelope(data={"items": []})
    # Try exact code or prefix match with exchange suffix completion
    safe_q = q_strip.replace("%", "").replace("_", "")
    like_code = safe_q + "%"          # 代码前缀匹配（精确前缀）
    like_name = "%" + safe_q + "%"    # 股票名称子串匹配（含中间字）
    # Remove .SH/.SZ from query for flexible match
    bare = q_strip.upper().rstrip(".SH").rstrip(".SZ")
    # Search by code prefix or stock_name substring
    rows = db.execute(
        text(
            """
            SELECT stock_code, stock_name, exchange
            FROM stock_master
            WHERE (is_delisted = 0 OR is_delisted IS NULL)
              AND (
                    stock_code LIKE :code_pfx ESCAPE '\\'
                    OR UPPER(SUBSTR(stock_code, 1, 6)) = :bare6
                    OR stock_name LIKE :name_pfx ESCAPE '\\'
              )
            ORDER BY
                CASE WHEN REPLACE(REPLACE(stock_code,'.SH',''),'.SZ','') = :bare6 THEN 0 ELSE 1 END,
                CASE WHEN stock_name LIKE :name_exact_pfx ESCAPE '\\' THEN 0 ELSE 1 END,
                stock_code
            LIMIT :lim
            """
        ),
        {"code_pfx": like_code, "bare6": bare[:6], "name_pfx": like_name, "name_exact_pfx": safe_q + "%", "lim": limit},
    ).fetchall()
    items = [
        {
            "stock_code": r[0],
            "stock_name": r[1] or r[0],
            "exchange": r[2],
            "display": f"{r[0]} {r[1] or ''}".strip(),
        }
        for r in rows
    ]
    return envelope(data={"items": items, "total": len(items)})


@router.get("/stocks/autocomplete")
async def stocks_autocomplete(
    db: Session = Depends(get_db),
    q: str = Query(None, min_length=1, max_length=20),
    limit: int = Query(8, ge=1, le=30),
):
    """股票代码自动补全，前端搜索框使用。返回带完整 .SH/.SZ 后缀的匹配结果。"""
    return await stocks_search(db=db, q=q, limit=limit)


@router.get("/stocks/{stock_code}/snapshot")
async def stock_snapshot(
    stock_code: str = Path(..., pattern=r"^\d{6}\.(SH|SZ|BJ)$"),
    trade_date: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    db: Session = Depends(get_db),
):
    """独立返回个股非研报数据快照，覆盖公司、估值、资金、新闻、热点与市场上下文。"""
    from app.services.stock_snapshot_service import build_stock_snapshot_payload

    payload = await build_stock_snapshot_payload(
        db,
        stock_code=stock_code,
        trade_date=trade_date,
    )
    degraded = payload.get("data_status") != "ok"
    return envelope(
        data=payload,
        degraded=degraded,
        degraded_reason=None if not degraded else f"non_report_data_{payload.get('data_status')}",
    )


@router.get("/user/favorites")
async def user_favorites_get(request: Request):
    """获取当前用户收藏列表（需登录）。"""
    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="未登录")
    user_id = str(getattr(user, "user_id", None) or user.id)
    return envelope(data={"user_id": user_id, "items": [], "total": 0, "message": "favorites_feature_coming_soon"})


@router.post("/user/favorites/{report_id}")
async def user_favorites_add(report_id: str, request: Request):
    """添加收藏（需登录）。"""
    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="未登录")
    return envelope(data={"report_id": report_id, "action": "added", "message": "favorites_feature_coming_soon"})


@router.delete("/user/favorites/{report_id}")
async def user_favorites_remove(report_id: str, request: Request):
    """取消收藏（需登录）。"""
    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="未登录")
    return envelope(data={"report_id": report_id, "action": "removed", "message": "favorites_feature_coming_soon"})


@router.get("/market/hotspots")
async def market_hotspots(
    db: Session = Depends(get_db),
    limit: int = Query(20, ge=1, le=100),
):
    """返回最新批次市场热点条目列表（降级友好：无数据时返回空列表+DEGRADED状态）。"""
    from app.models import MarketHotspotItem  # noqa: PLC0415
    rows = db.execute(
        text(
            """
            SELECT h.hotspot_item_id, h.topic_title, h.merged_rank, h.source_name,
                   h.news_event_type, h.fetch_time, h.quality_flag
            FROM market_hotspot_item h
            WHERE h.quality_flag != 'DROPPED'
            ORDER BY h.fetch_time DESC, h.merged_rank ASC
            LIMIT :lim
            """
        ),
        {"lim": limit},
    ).fetchall()
    items = [
        {
            "hotspot_item_id": r[0],
            "topic_title": r[1],
            "merged_rank": r[2],
            "source_name": r[3],
            "news_event_type": r[4],
            "fetch_time": str(r[5]) if r[5] else None,
            "quality_flag": r[6],
        }
        for r in rows
    ]
    data_status = "READY" if items else "DEGRADED"
    status_reason = None if items else "hotspot_source_unavailable(external_blocked)"
    return envelope(
        data={"items": items, "total": len(items), "data_status": data_status},
        degraded=(data_status == "DEGRADED"),
        degraded_reason=status_reason,
    )


@router.get("/search")
async def global_search(
    db: Session = Depends(get_db),
    q: str = Query(None, min_length=1, max_length=20, description="股票代码或名称前缀"),
    limit: int = Query(10, ge=1, le=50),
):
    """全局搜索：按股票代码或名称前缀搜索（复用 /stocks 逻辑）。"""
    return await stocks_search(db=db, q=q, limit=limit)


@router.get("/hot-stocks")
async def hot_stocks_compat(db: Session = Depends(get_db), limit: int = Query(6, ge=1, le=50)):
    """兼容入口：桥接到当前热股输出结构。"""
    items, source = _build_hot_stock_items(db, limit=limit)
    return envelope(data={"items": items, "source": source})


@router.get("/market-overview")
async def market_overview_compat(request: Request, db: Session = Depends(get_db)):
    """兼容入口：聚合首页核心概览字段。"""
    current_user = await get_current_user_optional(request)
    payload = get_home_payload_cached(
        db,
        viewer_tier=getattr(current_user, "tier", None) if current_user else None,
        viewer_role=getattr(current_user, "role", None) if current_user else None,
    )
    return envelope(
        data={
            "market_state": payload.get("market_state"),
            "pool_size": payload.get("pool_size", 0),
            "today_report_count": payload.get("today_report_count", 0),
            "data_status": payload.get("data_status"),
            "status_reason": payload.get("status_reason"),
            "hot_stocks": payload.get("hot_stocks", []),
        }
    )


@router.post("/reports/generate")
async def reports_generate(payload: GenerateReportRequest, db: Session = Depends(get_db)):
    from app.services.trade_calendar import latest_trade_date_str

    if payload.source == "test" and not settings.mock_llm:
        payload = payload.model_copy(update={"source": "real"})  # 非 mock 环境忽略 source=test
    target_trade_date = payload.trade_date or latest_trade_date_str()
    normalized_key = payload.idempotency_key or f"daily:{payload.stock_code}:{target_trade_date}"
    if payload.source == "test":
        existing_usage = db.execute(
            text(
                """
                SELECT 1
                FROM report_data_usage
                WHERE stock_code = :stock_code AND trade_date = :trade_date
                LIMIT 1
                """
            ),
            {
                "stock_code": payload.stock_code,
                "trade_date": date.fromisoformat(target_trade_date),
            },
        ).first()
        if existing_usage is None:
            ensure_test_generation_context(db, stock_code=payload.stock_code, trade_date=target_trade_date)
    existing_report_id = _find_active_report_id(
        db,
        stock_code=payload.stock_code,
        trade_date=target_trade_date,
        idempotency_key=normalized_key,
    )
    await ensure_non_report_usage_collected_if_needed(
        db,
        stock_code=payload.stock_code,
        trade_date=target_trade_date,
    )
    try:
        result = generate_report_ssot(
            db,
            stock_code=payload.stock_code,
            trade_date=target_trade_date,
            idempotency_key=payload.idempotency_key,
            request_id=get_request_id(),
        )
    except ReportGenerationServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.error_code) from exc

    report_payload = get_report_view_payload_ssot(db, result["report_id"]) or get_report_api_payload_ssot(db, result["report_id"]) or {}
    report_payload = dict(report_payload)
    return envelope(
        data={
            **result,
            "reused": existing_report_id == result["report_id"],
            "market_state": report_payload.get("market_state"),
            "review_flag": result.get("review_flag") or report_payload.get("review_flag") or "NONE",
            "publish_status": result.get("publish_status") or report_payload.get("publish_status") or "DRAFT_GENERATED",
            "report": report_payload,
        }
    )


@router.get("/reports/{report_id}")
async def reports_get(report_id: str, request: Request, db: Session = Depends(get_db)):
    user = await get_current_user_optional(request)
    viewer_role = getattr(user, "role", None) if user else None
    access_state = get_report_access_state_ssot(
        db,
        report_id,
        viewer_tier=getattr(user, "tier", None) if user else None,
        viewer_role=viewer_role,
    )
    if access_state == "hidden_by_viewer_cutoff":
        raise HTTPException(status_code=403, detail="REPORT_NOT_AVAILABLE")
    ssot_payload = get_report_api_payload_ssot(
        db,
        report_id,
        viewer_tier=getattr(user, "tier", None) if user else None,
        viewer_role=viewer_role,
    )
    if ssot_payload:
        return envelope(data=_redact_public_report_operational_fields(ssot_payload, viewer_role=viewer_role))

    row = db.get(Report, report_id)
    if not row:
        raise HTTPException(status_code=404, detail="REPORT_NOT_AVAILABLE")
    if bool(getattr(row, "is_deleted", False)):
        raise HTTPException(status_code=404, detail="REPORT_NOT_AVAILABLE")
    if str(getattr(row, "quality_flag", "ok") or "ok").strip().lower() != "ok":
        raise HTTPException(status_code=404, detail="REPORT_NOT_AVAILABLE")
    # N-08: 未发布研报仅 admin/super_admin 可访问
    if not bool(getattr(row, "published", False)):
        _viewer_role = getattr(user, "role", None) if user else None
        if _viewer_role not in ("admin", "super_admin"):
            raise HTTPException(status_code=404, detail="REPORT_NOT_AVAILABLE")
    data = dict(row.content_json or {})
    # 07 §2.3：响应根级必须含 report_id, stock_code, trade_date, created_at, recommendation, confidence 等
    data.setdefault("report_id", row.report_id)
    data.setdefault("stock_code", row.stock_code)
    data.setdefault("trade_date", row.trade_date or data.get("trade_date"))
    data.setdefault("created_at", row.created_at.isoformat() if row.created_at else data.get("created_at"))
    data.setdefault("recommendation", row.recommendation)
    data.setdefault("confidence", row.confidence)
    data.setdefault("run_mode", row.run_mode)
    return envelope(data=_redact_public_report_operational_fields(data, viewer_role=viewer_role))


@router.get("/reports/{report_id}/advanced")
async def reports_advanced(report_id: str, request: Request, db: Session = Depends(get_db)):
    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")

    data = get_report_advanced_payload_ssot(
        db,
        report_id,
        viewer_tier=getattr(user, "tier", None),
        viewer_role=getattr(user, "role", None),
    )
    if not data:
        raise HTTPException(status_code=404, detail="REPORT_NOT_AVAILABLE")

    compatible = dict(data)
    compatible.setdefault("reasoning_chain_md", compatible.get("reasoning_chain"))
    return envelope(data=compatible)


class ReportFeedbackPathRequest(BaseModel):
    model_config = {"extra": "forbid"}
    report_id: str | None = None
    feedback_type: str


@router.post("/reports/{report_id}/feedback")
async def report_feedback_by_path(
    report_id: str,
    payload: ReportFeedbackPathRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    """FR-11: 用户研报反馈（路径参数版）"""
    user = await get_current_user_optional(request)
    if not user:
        return JSONResponse(
            status_code=401,
            content={
                "success": False,
                "error_code": "UNAUTHORIZED",
                "error_message": "未登录",
                "data": None,
                "request_id": get_request_id(),
            },
        )
    body_report_id = payload.report_id or report_id
    if payload.report_id and payload.report_id != report_id:
        return JSONResponse(
            status_code=422,
            content={
                "success": False,
                "error_code": "INVALID_PAYLOAD",
                "error_message": "body report_id conflicts with path",
                "data": None,
                "request_id": get_request_id(),
            },
        )
    from app.services.feedback_ssot import FeedbackServiceError, submit_report_feedback
    try:
        result = submit_report_feedback(
            db,
            path_report_id=report_id,
            report_id=body_report_id,
            user_id=str(user.user_id),
            feedback_type=payload.feedback_type,
        )
    except FeedbackServiceError as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "success": False,
                "error_code": exc.error_code,
                "error_message": exc.error_code,
                "data": None,
                "request_id": get_request_id(),
            },
        )
    return envelope(data=result)


@router.post("/predictions/extract")
async def predictions_extract(report_id: str, db: Session = Depends(get_db)):
    row = db.get(Report, report_id)
    if not row:
        raise HTTPException(status_code=404, detail="REPORT_NOT_AVAILABLE")
    return envelope(data={"report_id": report_id, "prediction": row.recommendation})


@router.post("/predictions/settle")
async def predictions_settle(payload: PredictionSettleRequest, db: Session = Depends(get_db)):
    if report_storage_mode(db) == "ssot":
        raise HTTPException(status_code=404, detail="NOT_FOUND")
    rows = await settle_prediction(db, payload.report_id, payload.stock_code, payload.windows)
    return envelope(data={"count": len(rows), "report_id": payload.report_id})


@router.get("/predictions/stats", tags=["frozen-v1"])
async def predictions_stats(db: Session = Depends(get_db)):
    return envelope(data=prediction_stats(db))


@router.post("/billing/create-order")
async def billing_create_order(payload: BillingCreateOrderRequest, db: Session = Depends(get_db)):
    order = create_order(db, user_id=payload.user_id, plan_code=payload.plan_code, channel=payload.channel)
    return envelope(
        data={
            "order_id": order.order_id,
            "user_id": order.user_id,
            "plan_code": order.plan_code,
            "amount": order.amount,
            "status": order.status,
            "channel": order.channel,
        }
    )


@router.post("/billing/callback")
async def billing_callback(payload: BillingCallbackRequest, db: Session = Depends(get_db)):
    order = handle_callback(db, payload.order_id, payload.paid, payload.tx_id)
    if not order:
        raise HTTPException(status_code=404, detail="NOT_FOUND")
    return envelope(data={"order_id": order.order_id, "status": order.status})


@router.get("/platform/config", tags=["frozen-v1"])
async def platform_config():
    """前端用配置：API 基址、资金档位、枚举标签。无需登录。"""
    import json
    tiers = {}
    try:
        tiers = json.loads(getattr(settings, "capital_tiers", "{}") or "{}")
    except Exception:
        pass
    if not tiers:
        tiers = {"10k": {"label": "1 万档", "amount": 10000}, "100k": {"label": "10 万档", "amount": 100000}, "500k": {"label": "50 万档", "amount": 500000}}
    default_tier = "100k" if "100k" in tiers else (list(tiers.keys())[0] if tiers else "100k")
    return envelope(data={
        "api_base": settings.api_prefix,
        "capital_tiers": tiers,
        "default_capital_tier": default_tier,
        "labels": {
            "recommendation": {"BUY": "买入", "SELL": "卖出", "HOLD": "观望"},
            "market_state": {"BULL": "积极", "NEUTRAL": "谨慎", "BEAR": "防御"},
            "ma_trend": {"UP": "向上", "DOWN": "向下", "FLAT": "持平"},
            "position_status": {"OPEN": "持有中", "CLOSED_T1": "已止盈", "CLOSED_T2": "已止盈", "CLOSED_SL": "已止损", "CLOSED_EXPIRED": "已过期"},
        },
    })


@router.get("/platform/plans", tags=["frozen-v1"])
async def platform_plans():
    """订阅套餐列表（价格、功能）。无需登录。"""
    return envelope(data={"plans": get_plans_config()})


@router.get("/membership/subscription/status")
async def membership_subscription_status():
    raise HTTPException(status_code=410, detail="ROUTE_RETIRED")


@router.post("/report-feedback")
async def report_feedback(payload: ReportFeedbackRequest, db: Session = Depends(get_db)):
    """用户研报反馈 (05 §10, FR-07)"""
    row = ReportFeedback(
        report_id=payload.report_id,
        user_id=0,  # 演示模式，JWT启用后从token解析
        is_helpful=1 if payload.is_helpful else 0,
        feedback_type=payload.feedback_type,
        comment=payload.comment[:200] if payload.comment else None,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return envelope(data={"feedback_id": str(row.id), "created_at": row.created_at.isoformat() if row.created_at else None})


async def _require_admin(request: Request):
    """17 §3.1、05 §2.5a：admin API 需 JWT 且 role=admin；非 admin 返回 403（code=4001）。"""
    from app.core.security import get_current_user_optional
    from app.models import User

    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    if (user.role or "").lower() not in {"admin", "super_admin"}:
        raise HTTPException(status_code=403, detail="FORBIDDEN")
    return user


@router.get("/admin/users")
async def admin_users_list(
    request: Request,
    _: User = Depends(_require_admin),
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    tier: str | None = Query(None, pattern=r"^(Free|Pro|Enterprise)$"),
    sort: str | None = Query(None),
):
    """对齐 FR12 正式 admin/users 实现，避免 legacy 路由截获。"""
    from app.api import routes_admin

    return await routes_admin.admin_users(
        request=request,
        tier=tier,
        page=page,
        page_size=page_size,
        sort=sort,
        db=db,
        _=_,
    )


@router.patch("/admin/users/{user_id}")
async def admin_users_patch(
    user_id: str,
    payload: dict = Body(default={}),
    request: Request = None,
    current_admin: User = Depends(_require_admin),
    db: Session = Depends(get_db),
):
    """对齐 FR12 正式 admin/users/{id} 实现，避免 legacy 路由截获。"""
    from pydantic import ValidationError
    from app.api import routes_admin

    try:
        parsed_payload = routes_admin.UserPatchRequest.model_validate(payload if isinstance(payload, dict) else {})
    except ValidationError as exc:
        routes_admin._record_admin_precheck_rejection(
            db,
            actor_user_id=str(current_admin.user_id),
            action_type="PATCH_USER",
            target_table="app_user",
            target_pk=str(user_id),
            request_id=str(get_request_id() or ""),
            status_reason="invalid_tier",
            error_code="INVALID_PAYLOAD",
            reason_code="admin_manual_edit",
        )
        raise HTTPException(status_code=422, detail="INVALID_PAYLOAD") from exc

    return await routes_admin.admin_patch_user(
        user_id=user_id,
        payload=parsed_payload,
        request=request,
        actor=current_admin,
        db=db,
    )


@router.get("/admin/system-status")
async def admin_system_status(_: User = Depends(_require_admin), db: Session = Depends(get_db)):
    """17 §3.2：系统状态聚合 metrics、source、hotspot、counts、stock_pool、tasks"""
    from datetime import datetime, timezone
    from app.models import HotspotRaw, Report
    from app.services.observability import runtime_metrics_summary, prediction_stats_ssot, get_source_runtime_status, get_dashboard_stats_payload_ssot
    from app.services.stock_pool import get_daily_stock_pool
    from app.services.runtime_anchor_service import RuntimeAnchorService
    from app.services.runtime_truth_guard import truth_counters
    from app.services.membership import (
        audit_membership_provider_truth,
        get_payment_capability,
        payment_browser_checkout_ready,
    )
    from app.core.config import settings as _settings

    metrics = runtime_metrics_summary(db)

    # Layered prediction stats — use prediction_stats_ssot so tests can monkeypatch
    pred = prediction_stats_ssot(db)
    metrics["prediction"] = {
        "judged_total": pred.get("total_judged", 0),
        "accuracy": pred.get("accuracy"),
        "by_window": pred.get("by_window"),
        "recent_3m": pred.get("recent_3m"),
    }

    source_status = get_source_runtime_status()
    hotspot_last = db.query(HotspotRaw).order_by(HotspotRaw.fetch_time.desc()).first()
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if report_storage_mode(db) == "ssot":
        reports_today = count_reports_ssot(db, created_at_from=today_start)
    else:
        reports_today = db.query(Report).filter(Report.created_at >= today_start).count()
    metric_report = metrics.get("report") if isinstance(metrics, dict) else {}
    canonical_today_reports = metric_report.get("today_reports") if isinstance(metric_report, dict) else None
    if not isinstance(canonical_today_reports, int):
        canonical_today_reports = reports_today
    pool = get_daily_stock_pool()

    # ---- Layered health assessment ----
    anchor = RuntimeAnchorService(db)

    # service_health
    service_flags = []
    source_states = source_status if isinstance(source_status, dict) else {}
    for _src_name, _src_info in source_states.items():
        if isinstance(_src_info, dict) and _src_info.get("state") not in ("NORMAL", "OK", None):
            service_flags.append("source_runtime_abnormal")
            break
    service_status = "degraded" if service_flags else "normal"

    # business_health
    biz_flags = []
    accuracy = pred.get("accuracy")
    target = getattr(_settings, "forecast_target_accuracy", 0.55)
    if accuracy is not None and accuracy < target:
        biz_flags.append("prediction_accuracy_below_target")
    try:
        pool_snap = anchor.public_pool_snapshot()
        pool_size = pool_snap.get("pool_size", 0) if pool_snap else 0
    except Exception:
        pool_snap = None
        pool_size = 0
    batch_target = 0.8
    batch_rate = reports_today / max(pool_size, 1) if pool_size else 0.0
    if batch_rate < batch_target:
        biz_flags.append("today_report_progress_low")
    biz_status = "degraded" if biz_flags else "normal"

    # data_quality
    dq_flags = []
    try:
        anchor_dates = anchor.runtime_anchor_dates()
    except Exception:
        anchor_dates = {}
    runtime_td = anchor_dates.get("runtime_trade_date")
    stats_snap_date = anchor_dates.get("stats_snapshot_date")
    sim_snap_date = anchor_dates.get("sim_snapshot_date")
    if stats_snap_date is None:
        dq_flags.append("stats_snapshot_missing")
    if sim_snap_date and runtime_td and str(sim_snap_date) < str(runtime_td):
        dq_flags.append("sim_snapshot_lagging")
    try:
        dash_stats = get_dashboard_stats_payload_ssot(db)
    except Exception:
        dash_stats = {}
    if isinstance(dash_stats, dict) and dash_stats.get("data_status") in ("DEGRADED", "NOT_READY"):
        dq_flags.append("dashboard_stats_not_ready")
    try:
        mkt_row = anchor.runtime_market_state_row()
    except Exception:
        mkt_row = None
    if mkt_row is None:
        dq_flags.append("market_state_snapshot_missing")
    dq_status = "degraded" if dq_flags else "normal"

    overall = "degraded" if service_status == "degraded" or biz_status == "degraded" or dq_status == "degraded" else "normal"
    metrics["runtime_state"] = overall
    metrics["service_health"] = {"status": service_status, "flags": service_flags}
    metrics["business_health"] = {
        "status": biz_status,
        "flags": biz_flags,
        "batch_completion_rate": round(batch_rate, 4),
        "batch_completion_target": batch_target,
        "pool_size": pool_size,
    }
    metrics["data_quality"] = {"status": dq_status, "flags": dq_flags}

    # runtime_anchors inside metrics AND at top level
    try:
        runtime_anchors = anchor.runtime_anchor_dates()
    except Exception:
        runtime_anchors = {}
    metrics["runtime_anchors"] = runtime_anchors

    try:
        public_runtime = anchor.merged_public_runtime_payload(window_days=30)
    except Exception:
        public_runtime = {
            "runtime_trade_date": runtime_anchors.get("runtime_trade_date"),
            "snapshot_date": runtime_anchors.get("public_pool_trade_date"),
            "data_status": "COMPUTING",
            "status_reason": "home_snapshot_not_ready",
            "display_hint": None,
            "attempted_trade_date": runtime_anchors.get("latest_published_report_trade_date"),
            "fallback_from": None,
            "task_status": None,
            "kline_coverage": None,
        }

    pool_count = int(pool_size or 0)
    masked_stock_pool = {
        "count": pool_count,
        "sample": list(pool[:10]) if runtime_anchors.get("public_pool_trade_date") else [],
    }

    truth_gaps = truth_counters(db)
    payment_capability_raw = get_payment_capability() or {}
    payment_capability = {
        "provider_status": str(
            payment_capability_raw.get("provider_status")
            or ("provider-configured" if payment_capability_raw.get("enabled") else "provider-not-configured")
        ),
        "browser_checkout_ready": bool(payment_browser_checkout_ready()),
        "enabled": bool(payment_capability_raw.get("enabled", False)),
        "mock_billing": bool(payment_capability_raw.get("mock_billing", False)),
        "providers": payment_capability_raw.get("providers") or [],
    }
    membership_truth = audit_membership_provider_truth(db, apply_safe_repairs=False)

    return envelope(data={
        "metrics": metrics,
        "source_dates": runtime_anchors,
        "public_runtime": public_runtime,
        "runtime_anchors": runtime_anchors,
        "source_runtime": source_status,
        "hotspot": {"last_fetch": hotspot_last.fetch_time.isoformat() if hotspot_last and hotspot_last.fetch_time else None},
        "counts": {"reports": db.query(Report).count(), "users": db.query(User).count(), "reports_today": canonical_today_reports},
        "stock_pool": masked_stock_pool,
        "tasks": {"reports_today": canonical_today_reports, "note": "日更研报今日已生成数量"},
        "truth_gaps": truth_gaps,
        "payment_capability": payment_capability,
        "membership_truth": membership_truth,
    })

