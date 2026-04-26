from __future__ import annotations

import math
from typing import Any

ZERO_RETURN_THRESHOLD = 0.0001
FR07_MIN_SAMPLE_SIZE = 30
FR07_SAMPLE_ACCUMULATING_HINT = "样本积累中"
MAX_REASONABLE_ANNUALIZED_RETURN = 10.0


def path_cumulative_return_pct(returns: list[float]) -> float | None:
    if not returns:
        return None
    equity = 1.0
    for value in returns:
        equity *= 1.0 + value
    return round(equity - 1.0, 6)


def max_drawdown_pct_from_return_path(returns: list[float]) -> float | None:
    if not returns:
        return None
    equity = 1.0
    peak = 1.0
    worst_drawdown = 0.0
    for value in returns:
        equity *= 1.0 + value
        peak = max(peak, equity)
        drawdown = (equity / peak - 1.0) if peak else 0.0
        worst_drawdown = min(worst_drawdown, drawdown)
    return round(worst_drawdown, 6)


def annualized_return_from_cumulative(
    cumulative_return: float | None,
    *,
    trade_day_count: int,
) -> float | None:
    if cumulative_return is None or trade_day_count <= 0 or cumulative_return <= -1.0:
        return None
    try:
        annualized_return = (1.0 + cumulative_return) ** (252 / max(trade_day_count, 1)) - 1.0
    except OverflowError:
        return None
    if not math.isfinite(annualized_return) or abs(annualized_return) > MAX_REASONABLE_ANNUALIZED_RETURN:
        return None
    return round(annualized_return, 6)


def build_metric_payload(
    returns: list[float],
    *,
    trade_day_count: int,
    sample_size: int | None = None,
) -> dict[str, Any]:
    effective_sample_size = int(sample_size if sample_size is not None else len(returns))
    if effective_sample_size <= 0 or not returns:
        return {
            "win_rate": None,
            "profit_loss_ratio": None,
            "alpha_annual": None,
            "max_drawdown_pct": None,
            "cumulative_return_pct": None,
            "display_hint": None,
        }

    wins = [value for value in returns if value > ZERO_RETURN_THRESHOLD]
    losses = [abs(value) for value in returns if value < -ZERO_RETURN_THRESHOLD]
    non_zero = len(wins) + len(losses)
    cumulative_return = path_cumulative_return_pct(returns)

    if effective_sample_size < FR07_MIN_SAMPLE_SIZE:
        return {
            "win_rate": None,
            "profit_loss_ratio": None,
            "alpha_annual": None,
            "max_drawdown_pct": None,
            "cumulative_return_pct": cumulative_return,
            "display_hint": FR07_SAMPLE_ACCUMULATING_HINT,
        }

    avg_win = (sum(wins) / len(wins)) if wins else None
    avg_loss = (sum(losses) / len(losses)) if losses else None
    return {
        "win_rate": round(len(wins) / non_zero, 6) if non_zero > 0 else None,
        "profit_loss_ratio": round(avg_win / avg_loss, 6) if avg_win is not None and avg_loss is not None and avg_loss != 0 else None,
        "alpha_annual": annualized_return_from_cumulative(
            cumulative_return,
            trade_day_count=trade_day_count,
        ),
        "max_drawdown_pct": max_drawdown_pct_from_return_path(returns),
        "cumulative_return_pct": cumulative_return,
        "display_hint": None,
    }
