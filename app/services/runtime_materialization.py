from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Iterable
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models import Base
from app.services.fr07_metrics import FR07_MIN_SAMPLE_SIZE
from app.services.trade_calendar import trade_days_in_range

SSOT_TIERS = ("10k", "100k", "500k")
INITIAL_CASH_BY_TIER = {"10k": 10_000.0, "100k": 100_000.0, "500k": 500_000.0}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _query_one(db: Session, sql_text: str, params: dict[str, Any]) -> dict[str, Any] | None:
    row = db.execute(text(sql_text), params).mappings().first()
    return dict(row) if row else None


def _query_all(db: Session, sql_text: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in db.execute(text(sql_text), params).mappings().all()]


def _canonical_trade_days(days: Iterable[date]) -> list[date]:
    ordered_days = sorted({day for day in days})
    if not ordered_days:
        return []
    valid_trade_days = set(trade_days_in_range(ordered_days[0].isoformat(), ordered_days[-1].isoformat()))
    return [day for day in ordered_days if day.isoformat() in valid_trade_days]


def _latest_existing_baseline_equity(
    db: Session,
    *,
    capital_tier: str,
    baseline_type: str,
    before_trade_date: date,
) -> float | None:
    row = _query_one(
        db,
        """
        SELECT equity
        FROM baseline_equity_curve_point
        WHERE capital_tier = :capital_tier
          AND baseline_type = :baseline_type
          AND trade_date < :before_trade_date
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        {
            "capital_tier": capital_tier,
            "baseline_type": baseline_type,
            "before_trade_date": before_trade_date,
        },
    )
    return _to_float((row or {}).get("equity"))


def _weighted_metric(rows: list[dict[str, Any]], field_name: str) -> float | None:
    weighted_sum = 0.0
    total_weight = 0
    for row in rows:
        metric_value = _to_float(row.get(field_name))
        weight = _to_int(row.get("sample_size"))
        if metric_value is None or weight <= 0:
            continue
        weighted_sum += metric_value * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return round(weighted_sum / total_weight, 6)


def _annualized_return_from_curve(rows: list[dict[str, Any]]) -> float | None:
    if len(rows) < 2:
        return None
    start_equity = _to_float(rows[0].get("equity"))
    end_equity = _to_float(rows[-1].get("equity"))
    if not start_equity or end_equity is None or start_equity <= 0 or end_equity <= 0:
        return None
    trade_day_count = len(rows)
    if trade_day_count <= 1:
        return None
    return round((end_equity / start_equity) ** (252 / trade_day_count) - 1.0, 6)


def _max_drawdown_from_curve(rows: list[dict[str, Any]]) -> float | None:
    equities = [_to_float(row.get("equity")) for row in rows]
    equities = [value for value in equities if value is not None and value > 0]
    if not equities:
        return None
    peak = equities[0]
    max_drawdown = 0.0
    for equity in equities:
        peak = max(peak, equity)
        if peak <= 0:
            continue
        max_drawdown = min(max_drawdown, (equity / peak) - 1.0)
    return round(max_drawdown, 6)


def _win_rate_from_returns(returns: list[float]) -> float | None:
    if not returns:
        return None
    win_count = sum(1 for value in returns if value > 0)
    return round(win_count / len(returns), 6)


def _profit_loss_ratio_from_returns(returns: list[float]) -> float | None:
    wins = [value for value in returns if value > 0]
    losses = [abs(value) for value in returns if value < 0]
    if not wins or not losses:
        return None
    avg_win = sum(wins) / len(wins)
    avg_loss = sum(losses) / len(losses)
    if avg_loss <= 0:
        return None
    return round(avg_win / avg_loss, 6)


def _curve_rows_for_tier(
    db: Session,
    *,
    capital_tier: str,
    baseline_type: str | None = None,
    snapshot_date: str | date | None = None,
) -> list[dict[str, Any]]:
    snapshot_day = _as_date(snapshot_date) if snapshot_date is not None else None
    if baseline_type is None:
        sql_text = """
            SELECT trade_date, equity
            FROM sim_equity_curve_point
            WHERE capital_tier = :capital_tier
              AND (:snapshot_date IS NULL OR trade_date <= :snapshot_date)
            ORDER BY trade_date ASC
        """
        params = {"capital_tier": capital_tier, "snapshot_date": snapshot_day}
    else:
        sql_text = """
            SELECT trade_date, equity
            FROM baseline_equity_curve_point
            WHERE capital_tier = :capital_tier
              AND baseline_type = :baseline_type
              AND (:snapshot_date IS NULL OR trade_date <= :snapshot_date)
            ORDER BY trade_date ASC
        """
        params = {"capital_tier": capital_tier, "baseline_type": baseline_type, "snapshot_date": snapshot_day}
    return _query_all(db, sql_text, params)


def ensure_sim_accounts(db: Session) -> None:
    table = Base.metadata.tables["sim_account"]
    now = utc_now()
    for capital_tier in SSOT_TIERS:
        exists = _query_one(
            db,
            """
            SELECT capital_tier
            FROM sim_account
            WHERE capital_tier = :capital_tier
            LIMIT 1
            """,
            {"capital_tier": capital_tier},
        )
        if exists:
            continue
        initial_cash = INITIAL_CASH_BY_TIER[capital_tier]
        db.execute(
            table.insert().values(
                capital_tier=capital_tier,
                initial_cash=initial_cash,
                cash_available=initial_cash,
                total_asset=initial_cash,
                peak_total_asset=initial_cash,
                max_drawdown_pct=0.0,
                drawdown_state="NORMAL",
                drawdown_state_factor=1.0,
                active_position_count=0,
                last_reconciled_trade_date=None,
                updated_at=now,
                created_at=now,
            )
        )
    db.flush()


def materialize_baseline_equity_curve_points(
    db: Session,
    *,
    snapshot_date: str | date,
    start_date: str | date | None = None,
    purge_existing: bool = False,
) -> list[dict[str, Any]]:
    snapshot_day = _as_date(snapshot_date)
    start_day = _as_date(start_date) if start_date is not None else None
    ensure_sim_accounts(db)
    table = Base.metadata.tables["baseline_equity_curve_point"]
    metric_rows = _query_all(
        db,
        """
        SELECT snapshot_date, baseline_type, cumulative_return_pct
        FROM baseline_metric_snapshot
        WHERE window_days = 30
          AND (:start_date IS NULL OR snapshot_date >= :start_date)
          AND snapshot_date <= :snapshot_date
        ORDER BY snapshot_date ASC, baseline_type ASC
        """,
        {"snapshot_date": snapshot_day, "start_date": start_day},
    )
    if purge_existing:
        purge_start = start_day
        if purge_start is None and metric_rows:
            purge_start = min(_as_date(row["snapshot_date"]) for row in metric_rows)
        if purge_start is not None:
            db.execute(
                table.delete().where(
                    (table.c.trade_date >= purge_start)
                    & (table.c.trade_date <= snapshot_day)
                )
            )
    if not metric_rows:
        db.flush()
        return []

    by_type: dict[str, dict[str, float]] = {}
    for row in metric_rows:
        baseline_type = str(row.get("baseline_type") or "")
        if not baseline_type:
            continue
        by_type.setdefault(baseline_type, {})
        cumulative_return = _to_float(row.get("cumulative_return_pct"))
        if cumulative_return is None:
            continue
        by_type[baseline_type][_as_date(row["snapshot_date"]).isoformat()] = cumulative_return

    results: list[dict[str, Any]] = []
    now = utc_now()
    for capital_tier in SSOT_TIERS:
        initial_cash = INITIAL_CASH_BY_TIER[capital_tier]
        for baseline_type, returns_by_day in by_type.items():
            if not returns_by_day:
                continue
            first_snapshot = date.fromisoformat(min(returns_by_day))
            rebuild_start = start_day or first_snapshot
            trade_days = trade_days_in_range(rebuild_start.isoformat(), snapshot_day.isoformat())
            if not purge_existing:
                db.execute(
                    table.delete().where(
                        (table.c.capital_tier == capital_tier)
                        & (table.c.baseline_type == baseline_type)
                        & (table.c.trade_date >= rebuild_start)
                        & (table.c.trade_date <= snapshot_day)
                    )
                )
            last_equity = (
                _latest_existing_baseline_equity(
                    db,
                    capital_tier=capital_tier,
                    baseline_type=baseline_type,
                    before_trade_date=rebuild_start,
                )
                or initial_cash
            )
            inserted = 0
            for trade_day_text in trade_days:
                cumulative_return = returns_by_day.get(trade_day_text)
                if cumulative_return is not None:
                    last_equity = round(initial_cash * (1.0 + cumulative_return), 2)
                db.execute(
                    table.insert().values(
                        baseline_equity_curve_point_id=str(uuid4()),
                        capital_tier=capital_tier,
                        baseline_type=baseline_type,
                        trade_date=_as_date(trade_day_text),
                        equity=last_equity,
                        created_at=now,
                    )
                )
                inserted += 1
            results.append(
                {
                    "capital_tier": capital_tier,
                    "baseline_type": baseline_type,
                    "points": inserted,
                }
            )
    db.flush()
    return results


def materialize_sim_dashboard_snapshots(
    db: Session,
    *,
    snapshot_date: str | date,
    capital_tiers: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    snapshot_day = _as_date(snapshot_date)
    target_tiers = tuple(capital_tiers or SSOT_TIERS)
    ensure_sim_accounts(db)

    account_rows = {
        str(row["capital_tier"]): row
        for row in _query_all(
            db,
            """
            SELECT
                capital_tier,
                initial_cash,
                cash_available,
                total_asset,
                peak_total_asset,
                max_drawdown_pct,
                drawdown_state,
                active_position_count
            FROM sim_account
            """,
            {},
        )
    }
    open_position_counts = {
        str(row["capital_tier"]): _to_int(row.get("open_count"))
        for row in _query_all(
            db,
            """
            SELECT capital_tier, COUNT(*) AS open_count
            FROM sim_position
            WHERE COALESCE(entry_date, signal_date) <= :snapshot_date
              AND (exit_date IS NULL OR exit_date > :snapshot_date)
            GROUP BY capital_tier
            """,
            {"snapshot_date": snapshot_day},
        )
    }
    closed_position_rows = _query_all(
        db,
        """
        SELECT
            capital_tier,
            position_status,
            net_return_pct
        FROM sim_position
        WHERE position_status IN ('TAKE_PROFIT', 'STOP_LOSS', 'TIMEOUT', 'DELISTED_LIQUIDATED')
          AND exit_date IS NOT NULL
          AND exit_date <= :snapshot_date
        """,
        {"snapshot_date": snapshot_day},
    )
    closed_returns_by_tier: dict[str, list[float]] = {tier: [] for tier in SSOT_TIERS}
    for row in closed_position_rows:
        capital_tier = str(row.get("capital_tier") or "")
        if capital_tier not in closed_returns_by_tier:
            continue
        net_return_pct = _to_float(row.get("net_return_pct"))
        if net_return_pct is None:
            continue
        closed_returns_by_tier[capital_tier].append(net_return_pct)

    table = Base.metadata.tables["sim_dashboard_snapshot"]
    results: list[dict[str, Any]] = []
    now = utc_now()
    for capital_tier in target_tiers:
        account = account_rows.get(capital_tier)
        if not account:
            continue
        initial_cash = _to_float(account.get("initial_cash")) or INITIAL_CASH_BY_TIER.get(capital_tier, 0.0)
        actual_open_positions = open_position_counts.get(capital_tier, 0)
        closed_returns = closed_returns_by_tier.get(capital_tier, [])
        sample_size = len(closed_returns)
        equity_curve_rows = _curve_rows_for_tier(db, capital_tier=capital_tier, snapshot_date=snapshot_day)
        baseline_random_rows = _curve_rows_for_tier(
            db,
            capital_tier=capital_tier,
            baseline_type="baseline_random",
            snapshot_date=snapshot_day,
        )
        baseline_ma_cross_rows = _curve_rows_for_tier(
            db,
            capital_tier=capital_tier,
            baseline_type="baseline_ma_cross",
            snapshot_date=snapshot_day,
        )
        latest_equity = _to_float(equity_curve_rows[-1].get("equity")) if equity_curve_rows else None
        total_asset = latest_equity if latest_equity is not None else (_to_float(account.get("total_asset")) or initial_cash)
        total_return_pct = ((total_asset / initial_cash) - 1.0) if initial_cash else 0.0
        win_rate = _win_rate_from_returns(closed_returns)
        profit_loss_ratio = _profit_loss_ratio_from_returns(closed_returns)
        ai_annualized = _annualized_return_from_curve(equity_curve_rows)
        baseline_random_annualized = _annualized_return_from_curve(baseline_random_rows)
        alpha_annual = (
            round(ai_annualized - baseline_random_annualized, 6)
            if ai_annualized is not None and baseline_random_annualized is not None
            else None
        )
        max_drawdown_pct = _max_drawdown_from_curve(equity_curve_rows)
        if max_drawdown_pct is None:
            max_drawdown_pct = _to_float(account.get("max_drawdown_pct"))
        no_real_sim_data = sample_size <= 0 and actual_open_positions <= 0 and not equity_curve_rows
        data_status = "READY"
        status_reason = None
        display_hint = None
        if no_real_sim_data:
            total_asset = initial_cash
            total_return_pct = 0.0
            max_drawdown_pct = 0.0
            data_status = "COMPUTING"
            status_reason = "sim_dashboard_not_ready"
        elif not equity_curve_rows:
            data_status = "COMPUTING"
            status_reason = "equity_curve_empty"
        elif sample_size < FR07_MIN_SAMPLE_SIZE:
            data_status = "DEGRADED"
            status_reason = "sim_sample_lt_30"
        if display_hint is None and 0 < sample_size < FR07_MIN_SAMPLE_SIZE:
            display_hint = "sample_lt_30"
        if data_status == "READY" and (not baseline_random_rows or not baseline_ma_cross_rows):
            data_status = "DEGRADED"
            status_reason = status_reason or "sim_baseline_pending"
        if display_hint is None and data_status == "DEGRADED" and (not baseline_random_rows or not baseline_ma_cross_rows):
            display_hint = "baseline_pending"

        existing = _query_one(
            db,
            """
            SELECT dashboard_snapshot_id
            FROM sim_dashboard_snapshot
            WHERE capital_tier = :capital_tier
              AND snapshot_date = :snapshot_date
            LIMIT 1
            """,
            {"capital_tier": capital_tier, "snapshot_date": snapshot_day},
        )
        values = {
            "capital_tier": capital_tier,
            "snapshot_date": snapshot_day,
            "data_status": data_status,
            "status_reason": status_reason,
            "total_return_pct": round(total_return_pct, 6),
            "win_rate": win_rate,
            "profit_loss_ratio": profit_loss_ratio,
            "alpha_annual": alpha_annual,
            "max_drawdown_pct": max_drawdown_pct,
            "sample_size": sample_size,
            "display_hint": display_hint,
            "is_simulated_only": True,
            "created_at": now,
        }
        if existing:
            db.execute(
                table.update()
                .where(table.c.dashboard_snapshot_id == existing["dashboard_snapshot_id"])
                .values(
                    data_status=values["data_status"],
                    status_reason=values["status_reason"],
                    total_return_pct=values["total_return_pct"],
                    win_rate=values["win_rate"],
                    profit_loss_ratio=values["profit_loss_ratio"],
                    alpha_annual=values["alpha_annual"],
                max_drawdown_pct=values["max_drawdown_pct"],
                sample_size=values["sample_size"],
                display_hint=values["display_hint"],
                is_simulated_only=values["is_simulated_only"],
            )
            )
            snapshot_id = existing["dashboard_snapshot_id"]
        else:
            snapshot_id = str(uuid4())
            db.execute(
                table.insert().values(
                    dashboard_snapshot_id=snapshot_id,
                    **values,
                )
            )
        results.append(
            {
                "dashboard_snapshot_id": snapshot_id,
                "capital_tier": capital_tier,
                "snapshot_date": snapshot_day.isoformat(),
                "data_status": data_status,
                "status_reason": status_reason,
                "sample_size": sample_size,
                "display_hint": display_hint,
                "signal_validity_warning": False,
            }
        )
    db.flush()
    return results


def materialize_sim_dashboard_snapshot_history(
    db: Session,
    *,
    capital_tiers: Iterable[str] | None = None,
    snapshot_dates: Iterable[str | date] | None = None,
    prune_missing_dates: bool = False,
) -> list[dict[str, Any]]:
    target_tiers = tuple(capital_tiers or SSOT_TIERS)
    if snapshot_dates is None:
        snapshot_days = _canonical_trade_days(
            _as_date(row["trade_date"])
            for row in _query_all(
                db,
                """
                SELECT DISTINCT trade_date
                FROM sim_equity_curve_point
                ORDER BY trade_date ASC
                """,
                {},
            )
        )
    else:
        snapshot_days = _canonical_trade_days(_as_date(value) for value in snapshot_dates if value is not None)

    if prune_missing_dates and snapshot_days:
        table = Base.metadata.tables["sim_dashboard_snapshot"]
        min_day = snapshot_days[0]
        max_day = snapshot_days[-1]
        db.execute(
            table.delete().where(
                table.c.capital_tier.in_(target_tiers),
                table.c.snapshot_date >= min_day,
                table.c.snapshot_date <= max_day,
                table.c.snapshot_date.notin_(snapshot_days),
            )
        )

    results: list[dict[str, Any]] = []
    for snapshot_day in snapshot_days:
        results.extend(
            materialize_sim_dashboard_snapshots(
                db,
                snapshot_date=snapshot_day,
                capital_tiers=target_tiers,
            )
        )
    db.flush()
    return results


def rebuild_runtime_sim_history(
    db: Session,
    *,
    snapshot_dates: Iterable[str | date],
    capital_tiers: Iterable[str] | None = None,
    purge_existing: bool = True,
    prune_missing_dates: bool = True,
) -> dict[str, Any]:
    snapshot_days = _canonical_trade_days(_as_date(value) for value in snapshot_dates if value is not None)
    ensure_sim_accounts(db)
    if not snapshot_days:
        return {
            "snapshot_dates": [],
            "baseline_points": 0,
            "dashboard_snapshots": 0,
            "latest_snapshot_rows": 0,
        }

    baseline_points = materialize_baseline_equity_curve_points(
        db,
        snapshot_date=snapshot_days[-1],
        start_date=snapshot_days[0],
        purge_existing=purge_existing,
    )
    dashboard_snapshots = materialize_sim_dashboard_snapshot_history(
        db,
        capital_tiers=capital_tiers,
        snapshot_dates=snapshot_days,
        prune_missing_dates=prune_missing_dates,
    )
    latest_snapshot_rows = materialize_sim_dashboard_snapshots(
        db,
        snapshot_date=snapshot_days[-1],
        capital_tiers=capital_tiers,
    )
    db.flush()
    return {
        "snapshot_dates": [value.isoformat() for value in snapshot_days],
        "baseline_points": len(baseline_points),
        "dashboard_snapshots": len(dashboard_snapshots),
        "latest_snapshot_rows": len(latest_snapshot_rows),
    }
