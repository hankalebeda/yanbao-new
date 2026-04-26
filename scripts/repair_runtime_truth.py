from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.db import SessionLocal
from app.services.runtime_truth_guard import (
    normalize_snapshot_truth,
    soft_delete_stray_unpublished_reports,
    truth_counters,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair runtime truth by isolating stray reports and normalizing snapshot states.")
    parser.add_argument("--dry-run", action="store_true", help="Inspect only; do not commit changes.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db = SessionLocal()
    try:
        before = truth_counters(db)
        deleted = soft_delete_stray_unpublished_reports(db)
        normalized = normalize_snapshot_truth(db)
        after = truth_counters(db)
        payload = {
            "dry_run": args.dry_run,
            "before": before,
            "deleted_stray_reports": deleted,
            "normalized_snapshots": normalized,
            "after": after,
        }
        if args.dry_run:
            db.rollback()
        else:
            db.commit()
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
