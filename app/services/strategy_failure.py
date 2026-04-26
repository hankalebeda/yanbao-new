"""E8.5 策略失效监测（12 §10.2）：滚动胜率低于 MA 基线或连续净亏时告警/暂停。"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from app.core.config import settings
from app.models import Report, SimBaseline, SimPosition

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

PAUSED_CACHE_PATH = Path("data/strategy_paused.json")
STRATEGY_LABELS = {"A": "事件驱动", "B": "趋势跟踪", "C": "低波套利"}


def get_strategy_paused() -> list[str]:
    """读取当前暂停的策略类型列表（供 API 与前端使用）。"""
    if not PAUSED_CACHE_PATH.exists():
        return []
    try:
        data = json.loads(PAUSED_CACHE_PATH.read_text(encoding="utf-8"))
        return list(data.get("paused", []))
    except Exception:
        return []


def _write_paused(paused: list[str]) -> None:
    PAUSED_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    from datetime import datetime, timezone
    PAUSED_CACHE_PATH.write_text(
        json.dumps({"paused": paused, "updated_at": datetime.now(timezone.utc).isoformat()}, ensure_ascii=False),
        encoding="utf-8",
    )


def _ma_baseline_win_rate(db: Session) -> float | None:
    """MA 金叉基线胜率（同期 sim_baseline 中 ma_cross 类型）。"""
    rows = db.query(SimBaseline).filter(
        SimBaseline.baseline_type == "ma_cross",
        SimBaseline.pnl_pct.isnot(None),
    ).all()
    if len(rows) < 10:
        return None
    wins = sum(1 for r in rows if r.pnl_pct and float(r.pnl_pct) > 0)
    return wins / len(rows)


def check_and_update_strategy_paused(db: Session) -> list[str]:
    """
    E8.5 策略失效检测：按类型统计滚动胜率，与 MA 基线对比；超阈值时加入 strategy_paused。
    12 §10.2：窗口1=最近20笔，窗口2=第21～40笔；两窗口胜率均 < MA 基线 或 连续10笔净亏 即触发。
    """
    if not getattr(settings, "strategy_failure_alert_enabled", True):
        return get_strategy_paused()

    n = getattr(settings, "strategy_failure_rolling_window", 20)
    continuous = getattr(settings, "strategy_failure_continuous_loss", 10)
    auto_pause = getattr(settings, "strategy_failure_auto_pause", False)

    baseline_wr = _ma_baseline_win_rate(db)
    if baseline_wr is None:
        return get_strategy_paused()

    current_paused = set(get_strategy_paused())
    newly_paused: list[str] = []

    for st in ("A", "B", "C"):
        closed = (
            db.query(SimPosition)
            .join(Report, Report.report_id == SimPosition.report_id)
            .filter(
                Report.strategy_type == st,
                SimPosition.position_status.in_(("CLOSED_SL", "CLOSED_T1", "CLOSED_T2", "CLOSED_EXPIRED")),
            )
            .order_by(SimPosition.exit_date.desc(), SimPosition.position_id.desc())
            .limit(n * 2 + 10)
            .all()
        )

        if len(closed) < n:
            continue

        # 连续 N 笔净亏
        recent = closed[:continuous]
        if len(recent) >= continuous and all(p.net_return_pct is not None and float(p.net_return_pct) <= 0 for p in recent):
            if st not in current_paused and auto_pause:
                newly_paused.append(st)
                logger.warning("strategy_failure_trigger type=%s reason=continuous_loss count=%d", st, continuous)
            elif st not in current_paused:
                logger.info("strategy_failure_alert type=%s reason=continuous_loss count=%d (auto_pause=False)", st, continuous)
            continue

        # 两窗口胜率均 < MA 基线
        if len(closed) < n * 2:
            continue
        w1 = closed[:n]
        w2 = closed[n : n * 2]
        wr1 = sum(1 for p in w1 if p.net_return_pct and float(p.net_return_pct) > 0) / n
        wr2 = sum(1 for p in w2 if p.net_return_pct and float(p.net_return_pct) > 0) / n
        if wr1 < baseline_wr and wr2 < baseline_wr:
            if st not in current_paused and auto_pause:
                newly_paused.append(st)
                logger.warning(
                    "strategy_failure_trigger type=%s reason=under_baseline wr1=%.2f wr2=%.2f baseline=%.2f",
                    st, wr1, wr2, baseline_wr,
                )
            elif st not in current_paused:
                logger.info(
                    "strategy_failure_alert type=%s reason=under_baseline wr1=%.2f wr2=%.2f (auto_pause=False)",
                    st, wr1, wr2,
                )

    if newly_paused:
        updated = list(current_paused | set(newly_paused))
        _write_paused(updated)
        return updated
    return get_strategy_paused()
