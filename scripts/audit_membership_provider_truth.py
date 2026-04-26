from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.db import SessionLocal
from app.services.membership import audit_membership_provider_truth


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit membership/provider truth and classify paid-null-expiry records."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply only strictly repairable tier_expires_at fixes derived from local truth.",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indentation level.",
    )
    args = parser.parse_args()

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    db = SessionLocal()
    try:
        payload = audit_membership_provider_truth(db, apply_safe_repairs=args.apply)
        if args.apply:
            db.commit()
        else:
            db.rollback()
    finally:
        db.close()

    print(json.dumps(payload, ensure_ascii=False, indent=args.indent, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
