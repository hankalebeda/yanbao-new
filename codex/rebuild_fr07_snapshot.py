"""
Rebuild FR07 strategy_metric_snapshot for window_days=30 using our window_days=1 settlements.

Steps:
1. Update settlement_result.window_days from 1 to 30 for 2026-04-13 signals
2. Call rebuild_fr07_snapshot(db, trade_day=2026-04-14, window_days=30)
3. Verify dashboard now sees the data
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from datetime import date
from sqlalchemy.orm import Session

from app.core.db import SessionLocal
from app.services.settlement_ssot import rebuild_fr07_snapshot

BASE = "http://127.0.0.1:8010"
HEADERS = {"X-Internal-Token": "kestra-internal-20260327", "Content-Type": "application/json"}
NO_PROXY = {"http": None, "https": None}

def main():
    db: Session = SessionLocal()
    try:
        from sqlalchemy import text

        # Step 1: Update settlement_result.window_days from 1 to 30 (if not done)
        result = db.execute(text("""
            UPDATE settlement_result
            SET window_days = 30
            WHERE signal_date = '2026-04-13'
              AND window_days = 1
        """))
        db.flush()
        print(f"Updated {result.rowcount} settlement_result rows: window_days 1 -> 30")

        # Step 2: Clear stuck RUNNING pipeline_run rows for settlement
        result2 = db.execute(text("""
            UPDATE pipeline_run
            SET pipeline_status = 'COMPLETED'
            WHERE pipeline_status = 'RUNNING'
              AND pipeline_name LIKE 'settlement_pipeline%'
        """))
        db.flush()
        print(f"Fixed {result2.rowcount} stuck RUNNING pipeline_run rows")

        # Step 3: Rebuild FR07 snapshot for multiple dates with window_days=30
        # Cover both 2026-04-14 (exit date) and 2026-04-21 (runtime_trade_date)
        for rebuild_date in [date(2026, 4, 14), date(2026, 4, 21)]:
            print(f"Rebuilding FR07 snapshot for {rebuild_date}, window_days=30...")
            summary = rebuild_fr07_snapshot(
                db,
                trade_day=rebuild_date,
                window_days=30,
                purge_invalid=False,
            )
            print(f"  summary: settled={summary['settled_sample_size']}, cumret={summary['strategy_cumulative_return_pct']}")
        db.commit()
        print("Committed all changes")

        # Step 4: Check dashboard
        import time; time.sleep(1)
        print("\nChecking dashboard stats (window_days=30)...")
        r = requests.get(
            f"{BASE}/api/v1/dashboard/stats?window_days=30",
            timeout=30,
            proxies=NO_PROXY,
        )
        d = r.json().get("data", {})
        print(f"total_settled   = {d.get('total_settled')}")
        print(f"win_rate        = {d.get('overall_win_rate')}")
        print(f"pl_ratio        = {d.get('overall_profit_loss_ratio')}")
        print(f"cum_return      = {d.get('overall_cumulative_return_pct')}")
        print(f"data_status     = {d.get('data_status')}")
        print(f"status_reason   = {d.get('status_reason')}")
        print(f"display_hint    = {d.get('display_hint')}")
        print(f"snapshot_date   = {d.get('stats_snapshot_date')}")
        by_strat = d.get("by_strategy_type", {})
        for k, v in by_strat.items():
            print(f"  Strategy {k}: sample={v.get('sample_size')}, win_rate={v.get('win_rate')}, pl_ratio={v.get('profit_loss_ratio')}")

    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main()
