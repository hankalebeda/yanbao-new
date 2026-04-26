from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.db import SessionLocal
from app.services.cleanup_service import run_cleanup


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run FR-09-b cleanup maintenance.")
    parser.add_argument("--cleanup-date", default=None, help="Cleanup date in YYYY-MM-DD format. Defaults to today.")
    parser.add_argument(
        "--purge-test-account-pollution",
        action="store_true",
        help="Also purge shared runtime-test account pollution.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db = SessionLocal()
    try:
        result = run_cleanup(
            db,
            cleanup_date=args.cleanup_date,
            purge_test_account_pollution=args.purge_test_account_pollution,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
