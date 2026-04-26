"""Trigger settlement for the 9 BUY+ok+published reports on 2026-04-03."""
import os
import sys
import json
from datetime import datetime
from pathlib import Path

sys.path.insert(0, r"D:\yanbao-new")
os.environ["SETTLEMENT_INLINE_EXECUTION"] = "true"

import app.models  # noqa: F401
from sqlalchemy import text
from app.core.db import SessionLocal
from app.services.settlement_ssot import submit_settlement_task, SettlementServiceError

OUT = Path(__file__).resolve().parent.parent / "output" / "phase3_settlement.json"


def main() -> int:
    db = SessionLocal()
    try:
        buys = db.execute(text("""
            SELECT report_id, stock_code, trade_date
            FROM report
            WHERE (is_deleted IS NULL OR is_deleted=0)
              AND quality_flag='ok'
              AND published=1
              AND recommendation='BUY'
            ORDER BY trade_date, stock_code
        """)).mappings().all()
        buys = [dict(b) for b in buys]
        print(f"BUY+ok+published candidates: {len(buys)}")

        results = []
        for b in buys:
            try:
                task = submit_settlement_task(
                    db,
                    trade_date=str(b["trade_date"]),
                    window_days=7,
                    target_scope="report_id",
                    target_report_id=b["report_id"],
                    force=True,
                    run_inline=True,
                )
                db.commit()
                status = task.get("task", {}).get("status") if isinstance(task, dict) else "?"
                results.append({"stock": b["stock_code"], "trade_date": str(b["trade_date"]),
                                "report_id": b["report_id"], "status": status})
                print(f"  {b['stock_code']} {b['trade_date']} -> {status}")
            except SettlementServiceError as e:
                results.append({"stock": b["stock_code"], "report_id": b["report_id"],
                                "status": f"err:{e.args}"})
                print(f"  {b['stock_code']} FAIL {e}")
            except Exception as e:
                results.append({"stock": b["stock_code"], "report_id": b["report_id"],
                                "status": f"exc:{type(e).__name__}:{str(e)[:200]}"})
                print(f"  {b['stock_code']} EXC {type(e).__name__}:{str(e)[:200]}")

        # Final summary
        count_sr = db.execute(text("SELECT COUNT(*) FROM settlement_result")).scalar()
        count_pr = db.execute(text("SELECT COUNT(*) FROM prediction_outcome")).scalar()
        print(f"\nafter: settlement_result={count_sr} prediction_outcome={count_pr}")

        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps({"results": results, "settlement_result_count": count_sr,
                                   "prediction_outcome_count": count_pr,
                                   "at": datetime.utcnow().isoformat()},
                                  ensure_ascii=False, indent=2),
                       encoding="utf-8")
        print(f"wrote {OUT}")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
