from __future__ import annotations

from sqlalchemy.orm import Session

from app.services import ssot_read_model as shared
from app.services.runtime_anchor_service import RuntimeAnchorService


def get_sim_dashboard_payload_ssot(
    db: Session,
    *,
    capital_tier: str = "100k",
    runtime_anchor_service: RuntimeAnchorService | None = None,
) -> dict:
    from app.services.dashboard_query import get_public_performance_payload_ssot

    service = runtime_anchor_service or RuntimeAnchorService(db)
    ssot_tier = shared.normalize_capital_tier(capital_tier)
    public_performance = get_public_performance_payload_ssot(
        db,
        window_days=30,
        runtime_anchor_service=service,
    )
    closed_trade_count, open_position_count = shared._sim_runtime_source_state(db, capital_tier=ssot_tier)
    snapshot = shared._execute_mappings(
        db,
        """
        SELECT
            capital_tier,
            snapshot_date,
            data_status,
            status_reason,
            total_return_pct,
            win_rate,
            profit_loss_ratio,
            alpha_annual,
            max_drawdown_pct,
            sample_size,
            display_hint,
            is_simulated_only
        FROM sim_dashboard_snapshot
        WHERE capital_tier = :capital_tier
        ORDER BY snapshot_date DESC
        LIMIT 1
        """,
        {"capital_tier": ssot_tier},
    ).first()
    if not snapshot and closed_trade_count <= 0 and open_position_count <= 0:
        return {
            "capital_tier": ssot_tier,
            "snapshot_date": None,
            "runtime_trade_date": service.runtime_trade_date(),
            "data_status": "COMPUTING",
            "status_reason": "sim_dashboard_not_ready",
            "is_simulated_only": True,
            "drawdown_state": "NORMAL",
            "equity_curve": [],
            "total_return_pct": 0.0,
            "win_rate": None,
            "profit_loss_ratio": None,
            "alpha_annual": None,
            "max_drawdown_pct": None,
            "sample_size": 0,
            "signal_validity_warning": False,
            "display_hint": None,
            "baseline_random": None,
            "baseline_ma_cross": None,
            "open_positions": [],
            "public_performance": public_performance,
        }

    open_positions = shared._build_open_positions_payload(db, capital_tier=ssot_tier)
    equity_curve = shared._load_equity_curve_points(db, capital_tier=ssot_tier)
    baseline_random = shared._sanitize_baseline_curve_points(
        shared._load_equity_curve_points(db, capital_tier=ssot_tier, baseline_type="baseline_random")
    )
    baseline_ma_cross = shared._sanitize_baseline_curve_points(
        shared._load_equity_curve_points(db, capital_tier=ssot_tier, baseline_type="baseline_ma_cross")
    )
    account_row = shared._load_sim_account_snapshot(db, capital_tier=ssot_tier) or {}
    live_returns = shared._load_closed_sim_returns(db, capital_tier=ssot_tier)
    live_sample_size = len(live_returns)
    initial_cash = shared._to_float(account_row.get("initial_cash")) or 0.0
    latest_equity = shared._to_float(equity_curve[-1].get("equity")) if equity_curve else None
    total_asset = latest_equity if latest_equity is not None else (shared._to_float(account_row.get("total_asset")) or initial_cash)
    live_total_return_pct = round(((total_asset / initial_cash) - 1.0), 6) if initial_cash > 0 else 0.0
    live_win_rate = shared._win_rate_from_returns(live_returns)
    live_profit_loss_ratio = shared._profit_loss_ratio_from_returns(live_returns)
    live_ai_annualized = shared._annualized_return_from_curve_points(equity_curve)
    live_baseline_annualized = shared._annualized_return_from_curve_points(baseline_random)
    live_alpha_annual = (
        round(live_ai_annualized - live_baseline_annualized, 6)
        if live_ai_annualized is not None and live_baseline_annualized is not None
        else None
    )
    live_max_drawdown_pct = shared._max_drawdown_from_curve_points(equity_curve)
    if live_max_drawdown_pct is None:
        live_max_drawdown_pct = shared._to_float(account_row.get("max_drawdown_pct")) or 0.0
    latest_curve_row = shared._execute_mappings(
        db,
        """
        SELECT drawdown_state
        FROM sim_equity_curve_point
        WHERE capital_tier = :capital_tier
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        {"capital_tier": ssot_tier},
    ).first()
    runtime_trade_date = service.runtime_trade_date()
    snapshot_date = shared._iso_date((snapshot or {}).get("snapshot_date"))
    display_hint = (snapshot or {}).get("display_hint")
    if (not baseline_random or not baseline_ma_cross) and str((snapshot or {}).get("data_status") or "").upper() == "READY":
        display_hint = "基线对照数据计算中"

    if live_sample_size <= 0 and open_position_count <= 0:
        return {
            "capital_tier": ssot_tier,
            "snapshot_date": snapshot_date,
            "runtime_trade_date": runtime_trade_date,
            "data_status": "COMPUTING",
            "status_reason": "sim_dashboard_not_ready",
            "is_simulated_only": shared._to_bool((snapshot or {}).get("is_simulated_only")) or True,
            "drawdown_state": "NORMAL",
            "equity_curve": [],
            "total_return_pct": 0.0,
            "win_rate": None,
            "profit_loss_ratio": None,
            "alpha_annual": None,
            "max_drawdown_pct": 0.0,
            "sample_size": 0,
            "signal_validity_warning": False,
            "display_hint": None,
            "baseline_random": None,
            "baseline_ma_cross": None,
            "open_positions": [],
            "public_performance": public_performance,
        }

    effective_data_status = (snapshot or {}).get("data_status") or "COMPUTING"
    effective_status_reason = (snapshot or {}).get("status_reason")
    if effective_status_reason is None and effective_data_status != "READY":
        effective_status_reason = "sim_dashboard_not_ready"
    use_live_metrics = snapshot is None
    if not equity_curve and (shared._to_int((snapshot or {}).get("sample_size")) or 0) == 0:
        effective_data_status = "COMPUTING"
        effective_status_reason = "equity_curve_empty"
        use_live_metrics = True
    elif snapshot_date and runtime_trade_date and snapshot_date < runtime_trade_date and effective_data_status == "READY":
        effective_data_status = "DEGRADED"
        effective_status_reason = "sim_snapshot_lagging"
        display_hint = display_hint or (f"模拟收益快照截至 {snapshot_date}，已切换实时口径" if snapshot_date else "模拟收益快照日期缺失，已切换实时口径")
        use_live_metrics = True
    elif snapshot is not None and (shared._to_int(snapshot.get("sample_size")) or 0) != live_sample_size:
        effective_data_status = "DEGRADED"
        effective_status_reason = "sim_snapshot_lagging"
        display_hint = display_hint or "模拟收益快照与实时持仓不一致，已切换实时口径"
        use_live_metrics = True
    elif effective_data_status == "READY":
        effective_status_reason = None

    signal_validity_warning = shared._sim_signal_warning_from_curves(equity_curve, baseline_random)

    if use_live_metrics:
        total_return_pct = live_total_return_pct
        win_rate = live_win_rate
        profit_loss_ratio = live_profit_loss_ratio
        alpha_annual = live_alpha_annual
        max_drawdown_pct = live_max_drawdown_pct
        sample_size = live_sample_size
        if display_hint is None and 0 < sample_size < 30:
            display_hint = "样本积累中"
    else:
        total_return_pct = shared._to_float(snapshot.get("total_return_pct")) or 0.0
        win_rate = shared._to_float(snapshot.get("win_rate"))
        profit_loss_ratio = shared._to_float(snapshot.get("profit_loss_ratio"))
        alpha_annual = shared._to_float(snapshot.get("alpha_annual"))
        max_drawdown_pct = shared._to_float(snapshot.get("max_drawdown_pct"))
        sample_size = shared._to_int(snapshot.get("sample_size")) or 0

    return {
        "capital_tier": ssot_tier,
        "snapshot_date": snapshot_date,
        "runtime_trade_date": runtime_trade_date,
        "data_status": effective_data_status,
        "status_reason": effective_status_reason,
        "is_simulated_only": shared._to_bool((snapshot or {}).get("is_simulated_only")) or True,
        "drawdown_state": (latest_curve_row or {}).get("drawdown_state") or "NORMAL",
        "equity_curve": equity_curve,
        "total_return_pct": total_return_pct,
        "win_rate": win_rate,
        "profit_loss_ratio": profit_loss_ratio,
        "alpha_annual": alpha_annual,
        "max_drawdown_pct": max_drawdown_pct,
        "sample_size": sample_size,
        "signal_validity_warning": signal_validity_warning,
        "display_hint": display_hint,
        "baseline_random": baseline_random or None,
        "baseline_ma_cross": baseline_ma_cross or None,
        "open_positions": open_positions,
        "public_performance": public_performance,
    }


def sim_summary_ssot(
    db: Session,
    *,
    capital_tier: str = "100k",
    source: str = "live",
) -> dict:
    ssot_tier = shared.normalize_capital_tier(capital_tier)
    if shared.sim_storage_mode(db) != "ssot" or source == "backtest":
        return {
            "capital_tier": shared.compat_capital_tier(ssot_tier),
            "period_start": shared.latest_trade_date_str(),
            "period_end": shared.latest_trade_date_str(),
            "total_trades": 0,
            "win_trades": 0,
            "win_rate": None,
            "avg_win_pnl": 0,
            "avg_loss_pnl": 0,
            "pnl_ratio": None,
            "annualized_return": None,
            "hs300_return": None,
            "alpha": None,
            "max_drawdown": None,
            "drawdown_state": "NORMAL",
            "execution_blocked_count": 0,
            "by_strategy_type": {},
            "data_disclaimer": "当前无可用模拟收益快照",
            "cold_start": True,
            "est_days_to_30": 0,
            "cold_start_message": None,
            "source": source,
            "baseline_comparison": None,
            "strategy_paused": [],
            "data_status": "COMPUTING",
            "status_reason": "SIM_DASHBOARD_NOT_READY",
        }

    dashboard = get_sim_dashboard_payload_ssot(db, capital_tier=ssot_tier)
    equity_curve = dashboard.get("equity_curve") or []
    baseline_random = dashboard.get("baseline_random") or []
    baseline_ma_cross = dashboard.get("baseline_ma_cross") or []
    rows = shared._execute_mappings(
        db,
        """
        SELECT p.net_return_pct
        FROM sim_position p
        WHERE p.capital_tier = :capital_tier
          AND p.position_status IN ('TAKE_PROFIT', 'STOP_LOSS', 'TIMEOUT', 'DELISTED_LIQUIDATED')
        """,
        {"capital_tier": ssot_tier},
    ).all()
    returns = [shared._to_float(row.get("net_return_pct")) for row in rows]
    returns = [value for value in returns if value is not None]
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    total_trades = len(returns)
    cold_start = total_trades < 30
    est_days_to_30 = max(0, 30 - total_trades)
    period_start = equity_curve[0]["date"] if equity_curve else shared.latest_trade_date_str()
    period_end = equity_curve[-1]["date"] if equity_curve else shared.latest_trade_date_str()
    baseline_comparison = None
    if baseline_random or baseline_ma_cross:
        baseline_comparison = {}
        if baseline_random:
            baseline_comparison["random"] = {"total": len(baseline_random), "win_rate": None}
        if baseline_ma_cross:
            baseline_comparison["ma_cross"] = {"total": len(baseline_ma_cross), "win_rate": None}

    return {
        "capital_tier": shared.compat_capital_tier(ssot_tier),
        "capital_tier_raw": ssot_tier,
        "period_start": period_start,
        "period_end": period_end,
        "total_trades": total_trades,
        "win_trades": len(wins),
        "win_rate": dashboard.get("win_rate"),
        "avg_win_pnl": round(sum(wins) / len(wins), 6) if wins else 0,
        "avg_loss_pnl": round(sum(losses) / len(losses), 6) if losses else 0,
        "pnl_ratio": dashboard.get("profit_loss_ratio"),
        "annualized_return": dashboard.get("total_return_pct"),
        "hs300_return": None,
        "alpha": dashboard.get("alpha_annual"),
        "max_drawdown": dashboard.get("max_drawdown_pct"),
        "drawdown_state": dashboard.get("drawdown_state") or "NORMAL",
        "execution_blocked_count": 0,
        "by_strategy_type": shared._load_sim_strategy_breakdown(db, capital_tier=ssot_tier),
        "data_disclaimer": "以上统计基于模拟持仓，非真实交易收益",
        "cold_start": cold_start,
        "est_days_to_30": est_days_to_30,
        "cold_start_message": (
            f"样本积累中：已有 {total_trades} 笔，约需 {est_days_to_30} 个交易日达到 30 笔"
            if cold_start and est_days_to_30
            else (f"样本积累中：已有 {total_trades} 笔" if cold_start else None)
        ),
        "source": source,
        "baseline_comparison": baseline_comparison,
        "strategy_paused": [],
        "data_status": dashboard.get("data_status"),
        "status_reason": dashboard.get("status_reason"),
        "display_hint": dashboard.get("display_hint"),
        "snapshot_date": dashboard.get("snapshot_date"),
        "runtime_trade_date": dashboard.get("runtime_trade_date"),
        "is_simulated_only": dashboard.get("is_simulated_only"),
    }
