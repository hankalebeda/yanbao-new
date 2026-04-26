from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.db import SessionLocal
from app.services.settlement_ssot import VALID_WINDOWS, rebuild_fr07_snapshot_history
from app.services.trade_calendar import trade_days_in_range


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Purge invalid FR-07 history and rebuild truth-aligned snapshots.")
    parser.add_argument("--trade-date", help="Single snapshot trade date in YYYY-MM-DD format.")
    parser.add_argument("--start-date", help="Start trade date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", help="End trade date in YYYY-MM-DD format.")
    parser.add_argument(
        "--window-days",
        dest="window_days",
        type=int,
        nargs="+",
        default=sorted(VALID_WINDOWS),
        help="One or more FR-07 windows to rebuild. Defaults to all valid windows.",
    )
    parser.add_argument(
        "--skip-purge",
        action="store_true",
        help="Skip the initial invalid-settlement purge.",
    )
    return parser.parse_args()


def _validate_window_days(window_days: list[int]) -> list[int]:
    invalid_windows = sorted({window for window in window_days if window not in VALID_WINDOWS})
    if invalid_windows:
        raise SystemExit(f"Unsupported window_days: {invalid_windows}")
    return list(dict.fromkeys(window_days))


def _resolve_trade_dates(args: argparse.Namespace) -> list[date]:
    if args.trade_date:
        return [date.fromisoformat(args.trade_date)]
    if args.start_date and args.end_date:
        return [date.fromisoformat(value) for value in trade_days_in_range(args.start_date, args.end_date)]
    raise SystemExit("Either --trade-date or both --start-date/--end-date must be provided.")


def main() -> None:
    args = _parse_args()
    window_days = _validate_window_days(args.window_days)

    snapshot_days = _resolve_trade_dates(args)

    db = SessionLocal()
    try:
        summary = rebuild_fr07_snapshot_history(
            db,
            trade_days=snapshot_days,
            window_days_list=window_days,
            purge_invalid=not args.skip_purge,
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    print(
        json.dumps(
            {
                "snapshot_dates": [item.isoformat() for item in snapshot_days],
                "window_days": window_days,
                "rebuilt": summary["rebuilt"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
