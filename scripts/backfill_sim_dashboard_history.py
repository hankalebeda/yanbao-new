from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.db import SessionLocal, engine
from app.services.runtime_materialization import (
    materialize_baseline_equity_curve_points,
    materialize_sim_dashboard_snapshot_history,
)
from app.services.settlement_ssot import backfill_baseline_snapshot_history
from app.services.ssot_read_model import _latest_runtime_trade_date
from app.services.trade_calendar import trade_days_in_range
from scripts.repair_runtime_history import _runtime_history_anchor_trade_dates


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill sim dashboard history on repaired runtime anchor dates."
    )
    parser.add_argument("--trade-date", default=None, help="Anchor runtime trade date. Defaults to latest runtime trade date.")
    parser.add_argument(
        "--history-days",
        type=int,
        default=None,
        help="Optional natural-day filter applied to the repaired anchor date set.",
    )
    parser.add_argument(
        "--skip-baseline-backfill",
        action="store_true",
        help="Only rebuild sim dashboard snapshots without rebuilding baseline history first.",
    )
    return parser.parse_args()


def _target_trade_dates(db, *, trade_date_value: str, history_days: int | None) -> list[str]:
    trade_dates = _runtime_history_anchor_trade_dates(
        db,
        trade_date_value=trade_date_value,
    )
    if history_days is None:
        return trade_dates

    window_start = (
        date.fromisoformat(trade_date_value) - timedelta(days=max(history_days - 1, 0))
    ).isoformat()
    expected_trade_dates = set(trade_days_in_range(window_start, trade_date_value))
    return [value for value in trade_dates if value in expected_trade_dates]


def main() -> int:
    args = parse_args()
    db = SessionLocal()
    try:
        trade_date_value = args.trade_date or _latest_runtime_trade_date(db)
        if trade_date_value is None:
            raise RuntimeError("runtime_trade_date is unavailable")

        trade_dates = _target_trade_dates(
            db,
            trade_date_value=trade_date_value,
            history_days=args.history_days,
        )
        if not args.skip_baseline_backfill:
            backfill_baseline_snapshot_history(
                db,
                trade_dates=trade_dates,
                window_days=30,
                prune_missing_dates=True,
            )
            if trade_dates:
                materialize_baseline_equity_curve_points(
                    db,
                    snapshot_date=trade_date_value,
                    start_date=trade_dates[0],
                    purge_existing=True,
                )

        results = materialize_sim_dashboard_snapshot_history(
            db,
            snapshot_dates=trade_dates,
            prune_missing_dates=True,
        )
        db.commit()
        counts = Counter((item["capital_tier"], item["data_status"]) for item in results)
        print(f"trade_date={trade_date_value}")
        print(f"anchor_dates={len(trade_dates)}")
        print(f"materialized_rows={len(results)}")
        for (capital_tier, data_status), count in sorted(counts.items()):
            print(f"{capital_tier}.{data_status}={count}")
        return 0
    finally:
        db.close()
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
