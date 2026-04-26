"""模拟实盘追踪 API（07 契约 §2.10，17 §4.2 需付费用户）"""
import math
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.core.response import envelope
from app.models import Base, MarketStateCache, Report, SimAccount, SimBaseline, SimPosition, SimPositionBacktest, User
from app.services.stock_pool import get_daily_stock_pool
from app.services.sim_settle_service import HOLD_DAYS_MAX
from app.services.strategy_failure import get_strategy_paused
from app.services.trade_calendar import latest_trade_date_str

router = APIRouter(prefix="/api/v1", tags=["sim"])

_SIM_TIER_ALIASES = {
    "1w": "10k",
    "10w": "100k",
    "50w": "500k",
    "10k": "10k",
    "100k": "100k",
    "500k": "500k",
}
_SIM_TIER_PATTERN = "^(1w|10w|50w|10k|100k|500k)$"
_SIM_TIER_CANONICAL_TO_LEGACY = {
    "10k": "1w",
    "100k": "10w",
    "500k": "50w",
}


def _normalize_sim_tier(capital_tier: str | None, default: str = "100k") -> str:
    if not capital_tier:
        return default
    return _SIM_TIER_ALIASES.get(capital_tier.lower(), default)


def _sim_tier_candidates(capital_tier: str | None, default: str = "100k") -> tuple[str, ...]:
    ssot_tier = _normalize_sim_tier(capital_tier, default=default)
    legacy_tier = _SIM_TIER_CANONICAL_TO_LEGACY.get(ssot_tier)
    if legacy_tier:
        return (ssot_tier, legacy_tier)
    return (ssot_tier,)


async def _require_sim_access(request: Request) -> User:
    """模拟收益 API 需付费用户或管理员（17 §2.4：未登录401，免费且非管理员403）"""
    from app.core.security import get_current_user_optional

    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    role = (user.role or "").strip().lower()
    is_admin = role in {"admin", "super_admin"}

    now = datetime.now(timezone.utc)
    is_paid = False
    tier = str(getattr(user, "tier", "Free") or "Free")
    tier_expires_at = getattr(user, "tier_expires_at", None)
    if tier in {"Pro", "Enterprise"} and tier_expires_at is not None:
        expiry = tier_expires_at
        if getattr(expiry, "tzinfo", None) is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        is_paid = expiry > now
    if not is_paid:
        membership_level = str(getattr(user, "membership_level", "") or "").lower()
        membership_expires_at = getattr(user, "membership_expires_at", None)
        if membership_level in {"monthly", "annual"} and membership_expires_at is not None:
            membership_expiry = membership_expires_at
            if getattr(membership_expiry, "tzinfo", None) is None:
                membership_expiry = membership_expiry.replace(tzinfo=timezone.utc)
            is_paid = membership_expiry > now

    if not is_paid and not is_admin:
        raise HTTPException(status_code=403, detail="TIER_NOT_AVAILABLE")
    return user


def _position_to_dict(p: SimPosition | SimPositionBacktest, today: str) -> dict:
    hold_days_max = HOLD_DAYS_MAX.get(p.strategy_type, 15)
    hold_days = p.hold_days or 0
    days_until_expire = 0
    valid_until = p.valid_until or "9999-12-31"
    try:
        from datetime import date
        d0 = date.fromisoformat(p.sim_open_date)
        d1 = date.fromisoformat(today)
        hold_days = max(0, (d1 - d0).days)
        if p.status == "OPEN":
            de = date.fromisoformat(valid_until)
            days_until_expire = max(0, (de - d1).days)
    except Exception:
        pass
    return {
        "position_id": str(p.id),
        "report_id": p.report_id,
        "stock_code": p.stock_code,
        "stock_name": p.stock_name or p.stock_code,
        "strategy_type": p.strategy_type,
        "signal_date": p.signal_date,
        "sim_open_date": p.sim_open_date,
        "sim_open_price": float(p.sim_open_price) if p.sim_open_price is not None else 0,
        "actual_entry_price": float(p.actual_entry_price) if p.actual_entry_price is not None else None,
        "sim_qty": p.sim_qty,
        "capital_tier": p.capital_tier,
        "stop_loss_price": float(p.stop_loss_price) if p.stop_loss_price is not None else 0,
        "target_price_1": float(p.target_price_1) if p.target_price_1 is not None else None,
        "target_price_2": float(p.target_price_2) if p.target_price_2 is not None else None,
        "valid_until": p.valid_until,
        "status": p.status,
        "sim_close_date": p.sim_close_date,
        "sim_close_price": float(p.sim_close_price) if p.sim_close_price is not None else None,
        "sim_pnl_gross": float(p.sim_pnl_gross) if p.sim_pnl_gross is not None else None,
        "sim_pnl_net": float(p.sim_pnl_net) if p.sim_pnl_net is not None else None,
        "sim_pnl_pct": float(p.sim_pnl_pct) if p.sim_pnl_pct is not None else None,
        "hold_days": hold_days,
        "hold_days_max": hold_days_max,
        "days_until_expire": days_until_expire,
        "execution_blocked": bool(p.execution_blocked),
    }


@router.get("/sim/positions")
async def sim_positions(
    request: Request,
    _: User = Depends(_require_sim_access),
    db: Session = Depends(get_db),
    source: str | None = Query(None, pattern="^(live|backtest)$"),
    stock_code: str | None = Query(None, pattern=r"^\d{6}\.(SH|SZ)$"),
    status: str | None = Query(None, pattern="^(OPEN|CLOSED_SL|CLOSED_T1|CLOSED_T2|CLOSED_EXPIRED)$"),
    date_from: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    date_to: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
):
    """GET /api/v1/sim/positions 查询模拟持仓；source=backtest 时查 Walk-Forward 回测（12 §6.0.1）"""
    from app.services.ssot_read_model import list_sim_positions_ssot
    return envelope(data=list_sim_positions_ssot(
        db,
        stock_code=stock_code,
        status=status,
        date_from=date_from,
        date_to=date_to,
        source=source,
        page=page,
        page_size=page_size,
    ))


@router.get("/sim/positions/{position_id}")
async def sim_position_detail(
    request: Request,
    position_id: str,
    _: User = Depends(_require_sim_access),
    db: Session = Depends(get_db),
):
    """GET /api/v1/sim/positions/{position_id} 单笔持仓详情"""
    from app.services.ssot_read_model import get_sim_position_ssot
    data = get_sim_position_ssot(db, position_id=str(position_id))
    if not data:
        raise HTTPException(status_code=404, detail="NOT_FOUND")
    return envelope(data=data)


@router.get("/sim/positions/by-report/{report_id}")
async def sim_positions_by_report(
    request: Request,
    report_id: str,
    _: User = Depends(_require_sim_access),
    db: Session = Depends(get_db),
):
    """GET /api/v1/sim/positions/by-report/{report_id} 研报对应的持仓"""
    from app.services.ssot_read_model import list_sim_positions_by_report_ssot
    return envelope(data=list_sim_positions_by_report_ssot(db, report_id))


@router.get("/sim/account/snapshots")
async def sim_account_snapshots(
    request: Request,
    _: User = Depends(_require_sim_access),
    db: Session = Depends(get_db),
    capital_tier: str | None = Query(None, pattern=_SIM_TIER_PATTERN),
    date_from: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    date_to: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=200),
):
    """GET /api/v1/sim/account/snapshots 模拟账户日度快照"""
    from app.services.ssot_read_model import list_sim_account_snapshots_ssot
    return envelope(data=list_sim_account_snapshots_ssot(
        db,
        capital_tier=capital_tier,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
    ))


def _compute_summary(db: Session, capital_tier: str = "100k", source: str = "live") -> dict:
    from app.core.config import settings

    ssot_tier = _normalize_sim_tier(capital_tier)
    tier_candidates = _sim_tier_candidates(ssot_tier)
    today = latest_trade_date_str()
    Model = SimPositionBacktest if source == "backtest" else SimPosition
    AccountModel = None if source == "backtest" else SimAccount
    positions = db.query(Model).filter(Model.capital_tier.in_(tier_candidates)).all()
    closed = [p for p in positions if p.status and p.status.startswith("CLOSED")]
    total = len(closed)
    wins = [p for p in closed if p.sim_pnl_net is not None and p.sim_pnl_net > 0]
    win_rate = len(wins) / total if total else None
    avg_win = sum(float(p.sim_pnl_net) for p in wins) / len(wins) if wins else 0
    losses = [p for p in closed if p.sim_pnl_net is not None and p.sim_pnl_net <= 0]
    avg_loss = abs(sum(float(p.sim_pnl_net) for p in losses) / len(losses)) if losses else 1
    pnl_ratio = avg_win / avg_loss if avg_loss and total else None

    snaps = None
    drawdown_state = "NORMAL"
    dates = []
    if AccountModel:
        snaps = db.query(SimAccount).filter(SimAccount.capital_tier.in_(tier_candidates)).order_by(SimAccount.snapshot_date.desc()).limit(1).first()
        drawdown_state = snaps.drawdown_state if snaps else "NORMAL"
        dates = [r[0] for r in db.query(SimAccount.snapshot_date).filter(SimAccount.capital_tier.in_(tier_candidates)).distinct().all()]
    if source == "backtest" and positions:
        dates = [p.signal_date for p in positions if p.signal_date] + [p.sim_close_date for p in positions if p.sim_close_date]
    period_start = min(dates) if dates else today
    period_end = max(dates) if dates else today

    by_type = {}
    for st in ("A", "B", "C"):
        st_closed = [p for p in closed if p.strategy_type == st]
        st_total = len(st_closed)
        st_wins = [p for p in st_closed if p.sim_pnl_net is not None and p.sim_pnl_net > 0]
        st_win_rate = len(st_wins) / st_total if st_total else None
        st_avg_win = sum(float(p.sim_pnl_net) for p in st_wins) / len(st_wins) if st_wins else 0
        st_losses = [p for p in st_closed if p.sim_pnl_net is not None and p.sim_pnl_net <= 0]
        st_avg_loss = abs(sum(float(p.sim_pnl_net) for p in st_losses) / len(st_losses)) if st_losses else 1
        st_pnl = st_avg_win / st_avg_loss if st_avg_loss and st_total else None
        by_type[st] = {"total": st_total, "win_rate": st_win_rate, "pnl_ratio": st_pnl, "note": "样本<30笔时为null" if st_total < 30 else None}

    blocked = sum(1 for p in positions if p.execution_blocked)

    # 冷启动期展示：样本<30时返回约需交易日（05 §7.5a）
    cold_start = total < 30
    daily_avg = getattr(settings, "cold_start_daily_signal_avg", 1.5)
    est_days_to_30 = max(0, math.ceil((30 - total) / daily_avg)) if cold_start and daily_avg > 0 else 0

    # annualized_return: avg pct-return × (252 / avg holding days)
    _pct_vals = [float(p.sim_pnl_pct) for p in closed if p.sim_pnl_pct is not None]
    _hold_vals = [int(p.hold_days) for p in closed if p.hold_days and int(p.hold_days) > 0]
    if total >= 5 and _pct_vals:
        _avg_pct = sum(_pct_vals) / len(_pct_vals)
        _avg_hold = sum(_hold_vals) / len(_hold_vals) if _hold_vals else 7
        _annualized_return = round(_avg_pct * (252 / max(_avg_hold, 1)), 2)
    else:
        _annualized_return = None

    if source == "backtest":
        data_disclaimer = "历史回测数据，与实盘分离，仅供参考"
    else:
        data_disclaimer = "以上统计基于模拟持仓，非真实交易收益"

    # E8.4 对照组基线胜率（样本≥30 后展示）
    baseline_comparison = None
    if source == "live" and total >= 30:
        bl_rows = db.query(SimBaseline).filter(SimBaseline.pnl_pct.isnot(None)).all()
        by_bl = {}
        for bt in ("random", "ma_cross"):
            subset = [r for r in bl_rows if r.baseline_type == bt]
            if len(subset) < 10:
                continue
            bl_wins = [r for r in subset if r.pnl_pct and r.pnl_pct > 0]
            bl_wr = len(bl_wins) / len(subset) if subset else None
            by_bl[bt] = {"total": len(subset), "win_rate": round(bl_wr, 4) if bl_wr is not None else None}
        if by_bl:
            baseline_comparison = by_bl

    return {
        "capital_tier": ssot_tier,
        "period_start": period_start,
        "period_end": period_end,
        "total_trades": total,
        "win_trades": len(wins),
        "win_rate": round(win_rate, 4) if win_rate is not None else None,
        "avg_win_pnl": round(avg_win, 2) if wins else 0,
        "avg_loss_pnl": round(-sum(p.sim_pnl_net for p in losses) / len(losses), 2) if losses else 0,
        "pnl_ratio": round(pnl_ratio, 4) if pnl_ratio else None,
        "annualized_return": _annualized_return,
        "hs300_return": None,
        "alpha": None,
        "max_drawdown": float(snaps.max_drawdown_pct) if snaps and snaps.max_drawdown_pct else 0,
        "drawdown_state": drawdown_state,
        "execution_blocked_count": blocked,
        "by_strategy_type": by_type,
        "data_disclaimer": data_disclaimer,
        "cold_start": cold_start,
        "est_days_to_30": est_days_to_30,
        "cold_start_message": f"样本积累中：已有 {total} 笔，约需 {est_days_to_30} 个交易日达 30 笔" if cold_start and est_days_to_30 else (f"样本积累中：已有 {total} 笔" if cold_start else None),
        "source": source,
        "baseline_comparison": baseline_comparison,
        "strategy_paused": get_strategy_paused() if source == "live" else [],
    }


@router.get("/platform/summary")
async def platform_summary(db: Session = Depends(get_db)):
    """公开：平台模拟汇总（10万档），用于首页 Hero 展示胜率/盈亏比/年化 Alpha，无需鉴权。"""
    from app.services.ssot_read_model import get_public_performance_payload_ssot

    public_performance = get_public_performance_payload_ssot(db, window_days=30)
    total = public_performance.get("total_settled") or 0
    date_range = public_performance.get("date_range") or {}
    data_status = public_performance.get("data_status")
    cold_start = str(data_status or "").upper() != "READY"
    # DEGRADED with stats_source_degraded means data exists but some sources incomplete
    # — not a cold start situation (reports, stats snapshots all present)
    if cold_start and str(data_status or "").upper() == "DEGRADED":
        status_reason = public_performance.get("status_reason") or ""
        if status_reason == "stats_source_degraded":
            cold_start = False
    display_hint = public_performance.get("display_hint")
    return envelope(data={
        "win_rate": public_performance.get("overall_win_rate"),
        "pnl_ratio": public_performance.get("overall_profit_loss_ratio"),
        "alpha": public_performance.get("alpha_vs_baseline"),
        "baseline_random": public_performance.get("baseline_random"),
        "baseline_ma_cross": public_performance.get("baseline_ma_cross"),
        "total_trades": total,
        "period_start": date_range.get("from"),
        "period_end": date_range.get("to"),
        "data_status": data_status,
        "status_reason": public_performance.get("status_reason"),
        "display_hint": display_hint,
        "runtime_trade_date": public_performance.get("runtime_trade_date"),
        "snapshot_date": public_performance.get("snapshot_date"),
        "cold_start": cold_start,
        "cold_start_message": display_hint if cold_start else None,
    })


@router.get("/sim/account/summary")
async def sim_account_summary(
    request: Request,
    _: User = Depends(_require_sim_access),
    db: Session = Depends(get_db),
    capital_tier: str = Query("100k", pattern=_SIM_TIER_PATTERN),
    source: str | None = Query(None, pattern="^(live|backtest)$"),
):
    """GET /api/v1/sim/account/summary 模拟账户汇总；source=backtest 时返回 Walk-Forward 回测统计"""
    src = source or "live"
    data = _compute_summary(db, _normalize_sim_tier(capital_tier), source=src)
    return envelope(data=data)


@router.get("/market/hot-stocks")
async def market_hot_stocks(db: Session = Depends(get_db), limit: int = Query(5, ge=1, le=50)):
    """GET /api/v1/market/hot-stocks 首页「常用」股票快捷入口，取自股票池前 N 只，名称来自最新研报"""
    pool = get_daily_stock_pool()
    codes = pool[:limit] if pool else []
    source = "pool"
    # Try hotspot items first
    hotspot_link = Base.metadata.tables.get("market_hotspot_item_stock_link")
    hotspot_item = Base.metadata.tables.get("market_hotspot_item")
    hotspot_map: dict[str, dict] = {}
    if hotspot_link is not None and hotspot_item is not None:
        try:
            rows = db.execute(
                hotspot_link.join(hotspot_item, hotspot_link.c.hotspot_item_id == hotspot_item.c.hotspot_item_id)
                .select()
                .use_labels()
            ).mappings().all()
            for r in rows:
                sc = r.get("market_hotspot_item_stock_link_stock_code") or r.get("stock_code")
                if sc:
                    hotspot_map[sc] = {
                        "topic_title": r.get("market_hotspot_item_topic_title") or r.get("topic_title"),
                        "heat_score": r.get("market_hotspot_item_stock_link_match_confidence") or r.get("match_confidence"),
                    }
        except Exception:
            pass
    if hotspot_map:
        source = "hotspot"
        if not codes:
            codes = list(hotspot_map.keys())[:limit]
    out: list[dict] = []
    for idx, sc in enumerate(codes):
        row = db.query(Report).filter(Report.stock_code == sc).order_by(Report.created_at.desc()).first()
        name = sc
        if row and row.content_json:
            co = (row.content_json or {}).get("company_overview") or {}
            name = co.get("company_name") or name
        if row:
            name = row.stock_name_snapshot or name
        hot = hotspot_map.get(sc, {})
        out.append({
            "stock_code": sc,
            "stock_name": name,
            "rank": idx + 1,
            "topic_title": hot.get("topic_title"),
            "source_name": source,
            "heat_score": hot.get("heat_score"),
        })
    return envelope(data={"items": out, "source": source})


@router.get("/market/state")
async def market_state(db: Session = Depends(get_db)):
    """GET /api/v1/market/state 当日市场状态（07 §2.10）"""
    import app.services.market_state as market_state_service

    now_cn = market_state_service._now_cn()
    service_date = now_cn.date()
    today = service_date.isoformat()
    row = db.get(MarketStateCache, service_date)
    response_trade_date = today

    state = "NEUTRAL"
    reference_date = None
    cache_status = "FRESH"
    market_state_degraded = False
    state_reason = None

    if row is not None:
        state = str(row.market_state or "NEUTRAL")
        reference_date = row.reference_date
        cache_status = str(row.cache_status or "FRESH").upper()
        market_state_degraded = bool(row.market_state_degraded)
        state_reason = row.state_reason
    elif now_cn.hour < 9:
        fallback_row = market_state_service._latest_cache_before(db, today)
        if fallback_row is not None:
            state = str(fallback_row.market_state or "NEUTRAL")
            reference_date = fallback_row.reference_date
            cache_status = str(fallback_row.cache_status or "FRESH").upper()
            market_state_degraded = bool(fallback_row.market_state_degraded)
            state_reason = fallback_row.state_reason
            response_trade_date = fallback_row.trade_date.isoformat()
        else:
            state = "NEUTRAL"
            cache_status = "COLD_START"
            market_state_degraded = True
            state_reason = "COLD_START_FALLBACK"
    else:
        reference_date = market_state_service._previous_trade_date(service_date)
        metrics = market_state_service._load_reference_metrics(db, reference_date) if reference_date else None
        if metrics is None:
            state = "NEUTRAL"
            cache_status = "DEGRADED_NEUTRAL"
            market_state_degraded = True
            ref_text = reference_date.isoformat() if reference_date else "none"
            state_reason = f"computed_from_reference_date={ref_text};market_state_degraded=true"
        else:
            state = market_state_service.classify_market_state(metrics)
            cache_status = "FRESH"
            market_state_degraded = False
            state_reason = f"computed_from_reference_date={reference_date.isoformat()};cache_status=FRESH"

        db_now = datetime.now(timezone.utc).replace(tzinfo=None)
        row = MarketStateCache(
            trade_date=service_date,
            market_state=state,
            cache_status=cache_status,
            state_reason=state_reason,
            reference_date=reference_date,
            market_state_degraded=market_state_degraded,
            computed_at=db_now,
            created_at=db_now,
        )
        db.add(row)

    if row is not None and not state_reason:
        if reference_date is not None:
            state_reason = f"computed_from_reference_date={reference_date.isoformat()};cache_status={cache_status}"
        elif market_state_degraded:
            state_reason = "COLD_START_FALLBACK"
        else:
            state_reason = f"cache_status={cache_status}"

    if row is not None:
        row.market_state = state
        row.cache_status = cache_status
        row.reference_date = reference_date
        row.market_state_degraded = market_state_degraded
        row.state_reason = state_reason
        response_trade_date = row.trade_date.isoformat()

    db.commit()

    pos = {
        "A_type_pct": 10 if state == "BULL" else (7 if state == "NEUTRAL" else 3),
        "B_type_pct": 8 if state == "BULL" else (5 if state == "NEUTRAL" else 0),
        "C_type_pct": 5 if state == "BULL" else (3 if state == "NEUTRAL" else 0),
    }
    _is_trade = market_state_service.is_trade_day()

    return envelope(data={
        "trade_date": response_trade_date,
        "market_state_date": response_trade_date,
        "market_state": state,
        "reference_date": reference_date.isoformat() if reference_date else None,
        "state_reason": state_reason,
        "sh_index_close": 0,
        "sh_ma20": 0,
        "sh_ma20_trend": "FLAT",
        "hs300_20d_return_pct": 0.0,
        "volume_ratio_20d": 1.0,
        "position_advice": pos,
        "is_trading_day": _is_trade,
        "data_freshness": f"{response_trade_date} 09:00:00",
        "cache_ttl_seconds": 86400,
    })
