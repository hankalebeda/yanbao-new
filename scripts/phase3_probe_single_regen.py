"""Regenerate one report and show llm_fallback_level to validate the end-to-end path."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

os.environ.setdefault("NO_PROXY", "*")
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_report_ssot

if __name__ == "__main__":
    code = sys.argv[1] if len(sys.argv) > 1 else "000519.SZ"
    trade = sys.argv[2] if len(sys.argv) > 2 else "2026-04-03"
    db = SessionLocal()
    try:
        res = generate_report_ssot(
            db,
            stock_code=code,
            trade_date=trade,
            force_same_day_rebuild=True,
            skip_pool_check=True,
        )
        db.commit()
        print(
            f"stock={code} trade={trade} "
            f"quality_flag={res.get('quality_flag')} "
            f"llm_fallback_level={res.get('llm_fallback_level')} "
            f"rec={res.get('recommendation')} "
            f"conf={res.get('confidence')}"
        )
        print("citations:", len(res.get("citations") or []))
        print("used_data:", [u.get("dataset_name") for u in (res.get("used_data") or [])])
    finally:
        db.close()
