"""
生成策略 A/B/C 各一份研报（MOCK_LLM 模式），记录 report_id。
"""
import json, os
os.environ.setdefault("MOCK_LLM", "true")
os.environ.setdefault("ENABLE_SCHEDULER", "false")

from app.core.db import SessionLocal
from app.services.report_generation_ssot import (
    ensure_test_generation_context,
    generate_report_ssot,
)

STOCK = "688001.SH"
TRADE_DATE = "2026-04-22"  # 有 kline 数据的最新日期

results = {}
for strategy in ["A", "B", "C"]:
    db = SessionLocal()
    try:
        ensure_test_generation_context(db, stock_code=STOCK, trade_date=TRADE_DATE)
        db.commit()
        r = generate_report_ssot(
            db,
            stock_code=STOCK,
            trade_date=TRADE_DATE,
            skip_pool_check=True,
            force_same_day_rebuild=True,
            forced_strategy_type=strategy,
        )
        results[strategy] = {
            "report_id": r["report_id"],
            "strategy_type": r["strategy_type"],
            "recommendation": r["recommendation"],
            "confidence": r["confidence"],
            "quality_flag": r["quality_flag"],
            "conclusion_len": len(r.get("conclusion_text", "") or ""),
            "reasoning_len": len(r.get("reasoning_chain_md", "") or ""),
        }
        print(f"[{strategy}] OK  report_id={r['report_id']}  rec={r['recommendation']}  conf={r['confidence']}  quality={r['quality_flag']}")
    except Exception as e:
        import traceback
        results[strategy] = {"error": str(e)}
        print(f"[{strategy}] ERROR: {e}")
        traceback.print_exc()
    finally:
        db.close()

print()
print("=== Summary ===")
for s, v in results.items():
    print(f"  Strategy {s}: {json.dumps(v, ensure_ascii=False)}")
