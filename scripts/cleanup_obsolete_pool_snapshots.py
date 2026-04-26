from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from app.core.db import SessionLocal


def _parse_trade_date(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Delete obsolete stock_pool_snapshot rows for a trade date while keeping the latest task.pool_version."
    )
    parser.add_argument("--trade-date", required=True, help="Trade date in YYYY-MM-DD.")
    parser.add_argument("--dry-run", action="store_true", help="Show counts only.")
    args = parser.parse_args()

    trade_date = _parse_trade_date(args.trade_date)
    db = SessionLocal()
    try:
        latest_task = db.execute(
            text(
                """
                SELECT task_id, pool_version, status
                FROM stock_pool_refresh_task
                WHERE trade_date = :trade_date
                ORDER BY updated_at DESC, created_at DESC, task_id DESC
                LIMIT 1
                """
            ),
            {"trade_date": trade_date},
        ).mappings().first()
        if not latest_task or latest_task.get("pool_version") is None:
            print(f"trade_date={trade_date} latest_task_missing=true")
            return 1

        latest_pool_version = int(latest_task["pool_version"])
        counts = db.execute(
            text(
                """
                SELECT pool_version, pool_role, COUNT(*) AS row_count
                FROM stock_pool_snapshot
                WHERE trade_date = :trade_date
                GROUP BY pool_version, pool_role
                ORDER BY pool_version ASC, pool_role ASC
                """
            ),
            {"trade_date": trade_date},
        ).mappings().all()
        delete_count = db.execute(
            text(
                """
                SELECT COUNT(*) AS row_count
                FROM stock_pool_snapshot
                WHERE trade_date = :trade_date
                  AND pool_version != :pool_version
                """
            ),
            {"trade_date": trade_date, "pool_version": latest_pool_version},
        ).mappings().first()
        print(f"trade_date={trade_date} latest_pool_version={latest_pool_version} delete_rows={int(delete_count['row_count'] or 0)}")
        for row in counts:
            print(f"pool_version={row['pool_version']} pool_role={row['pool_role']} row_count={row['row_count']}")

        if args.dry_run:
            db.rollback()
            return 0

        db.execute(
            text(
                """
                DELETE FROM stock_pool_snapshot
                WHERE trade_date = :trade_date
                  AND pool_version != :pool_version
                """
            ),
            {"trade_date": trade_date, "pool_version": latest_pool_version},
        )
        db.commit()
        print("cleanup_applied=true")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
