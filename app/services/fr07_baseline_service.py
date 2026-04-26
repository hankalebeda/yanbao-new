from __future__ import annotations

import random
from collections import defaultdict
from datetime import date, datetime
from statistics import median
from typing import Any

from sqlalchemy import bindparam, select
from sqlalchemy.orm import Session

from app.models import Base
from app.services.fr07_metrics import (
    FR07_MIN_SAMPLE_SIZE,
    FR07_SAMPLE_ACCUMULATING_HINT,
    build_metric_payload,
)
from app.services.trade_calendar import trade_date_after_n_days

RANDOM_BASELINE_RUNS = 500


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def _net_return_pct(entry_price: float, exit_price: float, shares: int = 100) -> float:
    buy_amount = entry_price * shares
    sell_amount = exit_price * shares
    buy_commission = max(buy_amount * 0.00025, 5.0)
    sell_commission = max(sell_amount * 0.00025, 5.0)
    stamp_duty = sell_amount * 0.0005
    buy_slippage_cost = buy_amount * 0.0005
    sell_slippage_cost = sell_amount * 0.0005
    buy_paid = buy_amount + buy_commission + buy_slippage_cost
    sell_get = sell_amount - sell_commission - stamp_duty - sell_slippage_cost
    return round((sell_get - buy_paid) / buy_paid, 6) if buy_paid else 0.0


def _normalize_truth_rows(
    truth_rows: list[dict[str, Any]],
    *,
    trade_day: date,
    window_days: int,
) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    for index, row in enumerate(truth_rows):
        signal_day = _as_date(row.get("signal_date") or row.get("trade_date"))
        if signal_day is None:
            continue
        exit_trade_day = _as_date(row.get("exit_trade_date"))
        if exit_trade_day is None:
            exit_trade_day = date.fromisoformat(
                trade_date_after_n_days(signal_day.isoformat(), int(window_days))
            )
        if exit_trade_day > trade_day:
            continue
        normalized_rows.append(
            {
                "template_index": index,
                "report_id": row.get("report_id"),
                "signal_date": signal_day,
                "exit_trade_date": exit_trade_day,
            }
        )
    return normalized_rows


def _complete_public_signal_dates(db: Session, *, signal_dates: set[str]) -> dict[str, bool]:
    from app.services.ssot_read_model import _has_complete_public_batch_trace

    return {
        signal_date: _has_complete_public_batch_trace(db, trade_date=signal_date)
        for signal_date in signal_dates
    }


def _load_close_matrix(
    db: Session,
    *,
    relevant_dates: set[date],
) -> dict[str, dict[str, float]]:
    if not relevant_dates:
        return {}

    kline_table = Base.metadata.tables["kline_daily"]
    stock_table = Base.metadata.tables["stock_master"]
    rows = db.execute(
        select(
            kline_table.c.stock_code,
            kline_table.c.trade_date,
            kline_table.c.close,
        )
        .select_from(kline_table.join(stock_table, stock_table.c.stock_code == kline_table.c.stock_code))
        .where(
            kline_table.c.trade_date.in_(sorted(relevant_dates)),
            stock_table.c.is_delisted == 0,
            stock_table.c.list_date <= kline_table.c.trade_date,
        )
        .order_by(kline_table.c.trade_date.asc(), kline_table.c.stock_code.asc())
    ).mappings().all()

    close_by_date: dict[str, dict[str, float]] = {}
    for row in rows:
        trade_day = _as_date(row.get("trade_date"))
        stock_code = str(row.get("stock_code") or "")
        close_value = float(row.get("close") or 0.0)
        if trade_day is None or not stock_code or close_value <= 0:
            continue
        close_by_date.setdefault(trade_day.isoformat(), {})[stock_code] = close_value
    return close_by_date


def load_random_baseline_market_returns(
    db: Session,
    *,
    trade_day: date,
    window_days: int,
    truth_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized_rows = _normalize_truth_rows(
        truth_rows,
        trade_day=trade_day,
        window_days=window_days,
    )
    if not normalized_rows:
        return []

    signal_dates = {row["signal_date"].isoformat() for row in normalized_rows}
    complete_public_signal_dates = _complete_public_signal_dates(
        db,
        signal_dates=signal_dates,
    )
    close_by_date = _load_close_matrix(
        db,
        relevant_dates={
            row["signal_date"]
            for row in normalized_rows
        }
        | {
            row["exit_trade_date"]
            for row in normalized_rows
        },
    )

    candidate_rows: list[dict[str, Any]] = []
    for row in normalized_rows:
        signal_day = row["signal_date"]
        exit_trade_day = row["exit_trade_date"]
        if not complete_public_signal_dates.get(signal_day.isoformat(), False):
            continue

        signal_closes = close_by_date.get(signal_day.isoformat(), {})
        exit_closes = close_by_date.get(exit_trade_day.isoformat(), {})
        for stock_code in sorted(set(signal_closes).intersection(exit_closes)):
            entry_price = float(signal_closes.get(stock_code) or 0.0)
            exit_price = float(exit_closes.get(stock_code) or 0.0)
            if entry_price <= 0 or exit_price <= 0:
                continue
            candidate_rows.append(
                {
                    "template_index": row["template_index"],
                    "report_id": row.get("report_id"),
                    "stock_code": stock_code,
                    "signal_date": signal_day,
                    "exit_trade_date": exit_trade_day,
                    "net_return_pct": _net_return_pct(entry_price, exit_price),
                }
            )
    return candidate_rows


def summarize_random_baseline_candidates(
    candidate_rows: list[dict[str, Any]],
    *,
    window_days: int,
    trade_day: date | None = None,
) -> dict[str, Any]:
    grouped_returns: dict[int, list[float]] = defaultdict(list)
    for row in candidate_rows:
        template_index = int(row.get("template_index") or 0)
        grouped_returns[template_index].append(float(row.get("net_return_pct") or 0.0))

    ordered_template_indexes = sorted(grouped_returns)
    sample_size = len(ordered_template_indexes)
    if sample_size <= 0:
        return {
            "baseline_type": "baseline_random",
            "simulation_runs": RANDOM_BASELINE_RUNS,
            "sample_size": 0,
            "win_rate": None,
            "profit_loss_ratio": None,
            "alpha_annual": None,
            "max_drawdown_pct": None,
            "cumulative_return_pct": None,
            "display_hint": FR07_SAMPLE_ACCUMULATING_HINT,
            "window_days": window_days,
        }

    # Seed is deterministic per (trade_day, window_days) so each evaluation
    # context produces an independent but reproducible simulation run.
    seed_material = f"{trade_day or ''}:{window_days}:42"
    rng = random.Random(seed_material)
    run_payloads: list[dict[str, Any]] = []
    for _ in range(RANDOM_BASELINE_RUNS):
        sampled_returns = [
            rng.choice(grouped_returns[template_index])
            for template_index in ordered_template_indexes
        ]
        run_payloads.append(
            build_metric_payload(
                sampled_returns,
                trade_day_count=max(int(window_days), 1),
                sample_size=len(sampled_returns),
            )
        )

    cumulative_values = [
        float(payload["cumulative_return_pct"])
        for payload in run_payloads
        if payload.get("cumulative_return_pct") is not None
    ]
    if sample_size < FR07_MIN_SAMPLE_SIZE:
        cumulative_return = round(float(median(cumulative_values)), 6) if cumulative_values else None
        return {
            "baseline_type": "baseline_random",
            "simulation_runs": RANDOM_BASELINE_RUNS,
            "sample_size": sample_size,
            "win_rate": None,
            "profit_loss_ratio": None,
            "alpha_annual": None,
            "max_drawdown_pct": None,
            "cumulative_return_pct": cumulative_return,
            "display_hint": FR07_SAMPLE_ACCUMULATING_HINT,
            "window_days": window_days,
        }

    def _median_payload_value(field_name: str) -> float | None:
        values = [
            float(payload[field_name])
            for payload in run_payloads
            if payload.get(field_name) is not None
        ]
        return round(float(median(values)), 6) if values else None

    return {
        "baseline_type": "baseline_random",
        "simulation_runs": RANDOM_BASELINE_RUNS,
        "sample_size": sample_size,
        "win_rate": _median_payload_value("win_rate"),
        "profit_loss_ratio": _median_payload_value("profit_loss_ratio"),
        "alpha_annual": _median_payload_value("alpha_annual"),
        "max_drawdown_pct": _median_payload_value("max_drawdown_pct"),
        "cumulative_return_pct": _median_payload_value("cumulative_return_pct"),
        "display_hint": None,
        "window_days": window_days,
    }
