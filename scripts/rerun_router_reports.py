from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import bindparam, text

from app.core.config import settings
from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_report_ssot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rerun router-backed report generation for local/failed reports.")
    parser.add_argument("--trade-date", required=True, help="Trade date to rebuild, e.g. 2026-04-03")
    parser.add_argument("--limit", type=int, default=0, help="Optional max report count. 0 means all.")
    parser.add_argument("--include-failed", action="store_true", help="Also rerun llm_fallback_level=failed reports.")
    parser.add_argument("--disable-audit", action="store_true", help="Temporarily disable audit for faster reruns.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db = SessionLocal()
    try:
        fallback_levels = ["local"]
        if args.include_failed:
            fallback_levels.append("failed")
        stmt = text(
            """
            SELECT stock_code, llm_fallback_level
            FROM report
            WHERE is_deleted = 0
              AND trade_date = :trade_date
              AND llm_fallback_level IN :levels
            ORDER BY stock_code ASC
            """
        ).bindparams(bindparam("levels", expanding=True))
        rows = db.execute(
            stmt,
            {"trade_date": args.trade_date, "levels": fallback_levels},
        ).mappings().all()
        if args.limit > 0:
            rows = rows[: args.limit]

        original_audit_flag = settings.llm_audit_enabled
        if args.disable_audit:
            settings.llm_audit_enabled = False
        try:
            results: list[dict[str, object]] = []
            failures: list[dict[str, object]] = []
            for row in rows:
                stock_code = str(row["stock_code"])
                t0 = time.time()
                try:
                    result = generate_report_ssot(
                        db,
                        stock_code=stock_code,
                        trade_date=args.trade_date,
                        force_same_day_rebuild=True,
                    )
                    db.commit()
                    results.append(
                        {
                            "stock_code": stock_code,
                            "elapsed_s": round(time.time() - t0, 2),
                            "llm_fallback_level": result.get("llm_fallback_level"),
                            "publish_status": result.get("publish_status"),
                            "confidence": result.get("confidence"),
                        }
                    )
                except Exception as exc:  # pragma: no cover - operational path
                    db.rollback()
                    failures.append(
                        {
                            "stock_code": stock_code,
                            "error": str(exc),
                        }
                    )
        finally:
            settings.llm_audit_enabled = original_audit_flag

        print(
            json.dumps(
                {
                    "trade_date": args.trade_date,
                    "requested_levels": fallback_levels,
                    "processed": len(rows),
                    "succeeded": len(results),
                    "failed": len(failures),
                    "results": results,
                    "failures": failures,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
