"""Regenerate 3 reports by calling generate_report_ssot directly.

Bypasses API-level pool check since the pool is FALLBACK status.
"""
import os
import sys

os.environ["MOCK_LLM"] = "false"
os.environ["ENABLE_SCHEDULER"] = "false"
sys.path.insert(0, "d:\\yanbao-new")

from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_report_ssot

stocks = ["600519.SH", "002594.SZ", "000858.SZ"]
trade_date = "2026-04-16"

for stock in stocks:
    print(f"\n{'='*60}")
    print(f"Generating report for {stock} trade_date={trade_date}...")
    db = SessionLocal()
    try:
        result = generate_report_ssot(
            db,
            stock_code=stock,
            trade_date=trade_date,
            skip_pool_check=True,
            force_same_day_rebuild=True,
        )
        print(f"  report_id: {result.get('report_id', '?')}")
        print(f"  recommendation: {result.get('recommendation', '?')}")
        print(f"  llm_level: {result.get('llm_fallback_level', '?')}")
        print(f"  quality_flag: {result.get('quality_flag', '?')}")
        print(f"  published: {result.get('publish_status', '?')}")

        # Quick content_json check
        from app.models import Report
        report = db.get(Report, result["report_id"])
        if report and report.content_json:
            cj = report.content_json
            cj_keys = list(cj.keys())
            print(f"  content_json keys ({len(cj_keys)}): {cj_keys}")
            # Check new snapshot fields
            for k in ["capital_game_snapshot", "stock_profile_snapshot", "market_state_snapshot", "kline_snapshot", "data_completeness"]:
                v = cj.get(k)
                if v is None:
                    print(f"  MISSING: {k}")
                elif isinstance(v, dict) and not v:
                    print(f"  EMPTY: {k}")
                else:
                    print(f"  OK: {k}")
            # Check data_completeness
            dc = cj.get("data_completeness", {})
            print(f"  data_completeness: {dc.get('total_ok', 0)}/{dc.get('total_required', 0)} all_complete={dc.get('all_complete', False)}")
        else:
            print(f"  WARNING: content_json is empty!")
    except Exception as e:
        print(f"  ERROR: {e}")
    finally:
        db.close()
    print()

print("All done!")
