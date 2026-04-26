"""v20 backfill — regenerate all visible reports that fail hard-gate (mainly llm_fallback_level!='primary')

Strategy: collect failing (stock_code, trade_date) tuples, regen with force_same_day_rebuild=True.
Each successful regen supersedes the old report. Afterwards, run strict sweep to remove any
remaining non-compliant visibles.

Usage: python scripts/phase3_backfill_v20.py [--concurrent 6] [--max 200]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("NO_PROXY", "*")
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402
from app.core.db import SessionLocal  # noqa: E402
from app.services.report_generation_ssot import (  # noqa: E402
    ReportGenerationServiceError,
    generate_report_ssot,
)


def collect_targets(db, limit: int) -> list[dict]:
    rows = [
        dict(r._mapping)
        for r in db.execute(
            text(
                """
                SELECT report_id, stock_code, trade_date, quality_flag, llm_fallback_level
                FROM report
                WHERE (is_deleted=0 OR is_deleted IS NULL)
                  AND published=1
                  AND (quality_flag <> 'ok' OR llm_fallback_level <> 'primary'
                       OR quality_flag IS NULL OR llm_fallback_level IS NULL)
                ORDER BY trade_date DESC, stock_code ASC
                LIMIT :lim
                """
            ),
            {"lim": limit},
        ).fetchall()
    ]
    return rows


def regen_one_sync(stock_code: str, trade_date: str) -> dict:
    db = SessionLocal()
    try:
        res = generate_report_ssot(
            db,
            stock_code=stock_code,
            trade_date=trade_date,
            force_same_day_rebuild=True,
            skip_pool_check=True,
        )
        db.commit()
        return {
            "stock_code": stock_code,
            "trade_date": trade_date,
            "status": "ok",
            "llm_fallback_level": res.get("llm_fallback_level"),
            "quality_flag": res.get("quality_flag"),
            "new_report_id": res.get("report_id"),
        }
    except ReportGenerationServiceError as exc:
        db.rollback()
        return {
            "stock_code": stock_code,
            "trade_date": trade_date,
            "status": "error",
            "error_code": getattr(exc, "error_code", str(exc)),
        }
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        return {
            "stock_code": stock_code,
            "trade_date": trade_date,
            "status": "error",
            "error_code": f"{type(exc).__name__}: {str(exc)[:200]}",
        }
    finally:
        db.close()


async def run_batch(targets: list[dict], concurrent: int) -> list[dict]:
    sem = asyncio.Semaphore(concurrent)
    loop = asyncio.get_running_loop()
    results: list[dict] = []
    total = len(targets)

    async def _one(idx: int, t: dict) -> None:
        async with sem:
            res = await loop.run_in_executor(
                None, regen_one_sync, t["stock_code"], str(t["trade_date"])
            )
            results.append(res)
            if res.get("status") == "ok" and res.get("llm_fallback_level") == "primary":
                tag = "OK "
            elif res.get("status") == "ok":
                tag = "DEG"
            else:
                tag = "ERR"
            print(
                f"[{idx + 1:3d}/{total}] {tag} {res['stock_code']} {res['trade_date']} "
                f"status={res.get('status')} "
                f"lvl={res.get('llm_fallback_level', '-')} "
                f"q={res.get('quality_flag', '-')} "
                f"err={res.get('error_code', '-')}"
            )

    await asyncio.gather(*[_one(i, t) for i, t in enumerate(targets)])
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrent", type=int, default=6)
    parser.add_argument("--max", dest="max_count", type=int, default=500)
    args = parser.parse_args()

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = ROOT / "output" / f"backfill_v20_{ts}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    db = SessionLocal()
    try:
        targets = collect_targets(db, args.max_count)
    finally:
        db.close()

    print(f"[info] targets={len(targets)} concurrent={args.concurrent}")
    if not targets:
        print("[ok] nothing to backfill")
        return 0

    t0 = time.time()
    results = asyncio.run(run_batch(targets, args.concurrent))
    elapsed = round(time.time() - t0, 2)

    status_counts: dict[str, int] = defaultdict(int)
    level_counts: dict[str, int] = defaultdict(int)
    for r in results:
        status_counts[r.get("status", "unknown")] += 1
        lvl = r.get("llm_fallback_level") or "-"
        level_counts[lvl] += 1

    summary = {
        "schema": "v20",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "targets": len(targets),
        "concurrent": args.concurrent,
        "elapsed_s": elapsed,
        "status_counts": dict(status_counts),
        "level_counts": dict(level_counts),
        "primary_rate": round(level_counts.get("primary", 0) / max(len(results), 1), 4),
        "results": results,
    }
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(
        f"[done] elapsed={elapsed}s status={dict(status_counts)} level={dict(level_counts)} "
        f"primary_rate={summary['primary_rate']}"
    )
    print(f"  out={out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
