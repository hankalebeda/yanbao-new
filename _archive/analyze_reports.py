"""Analyze the 3 active reports for data completeness."""
import json
from app.core.db import SessionLocal
from app.models import Report

db = SessionLocal()
for row in db.query(Report).filter(Report.is_deleted == False).order_by(Report.created_at.desc()).limit(3).all():
    print(f"=== {row.stock_code} {row.stock_name_snapshot} ===")
    print(f"report_id: {row.report_id}")
    print(f"recommendation: {row.recommendation}")
    print(f"confidence: {row.confidence}")
    print(f"strategy_type: {row.strategy_type}")
    print(f"quality_flag: {row.quality_flag}")
    cj = row.content_json or {}
    print(f"content_json keys: {list(cj.keys())}")

    check_keys = [
        "capital_game_summary", "evidence_items", "analysis_steps",
        "kline_data", "hotspot_data", "northbound_data", "etf_flow_data",
        "stock_profile", "main_force_flow", "dragon_tiger_list",
        "margin_financing", "market_state", "recommendation",
        "confidence", "strategy_type", "risk_level", "target_price",
        "stop_loss", "entry_price", "position_ratio", "holding_period",
        "review_flag", "data_usage", "llm_model", "llm_level",
    ]
    for k in check_keys:
        v = cj.get(k)
        if v is None:
            print(f"  MISSING: {k}")
        elif isinstance(v, list) and len(v) == 0:
            print(f"  EMPTY_LIST: {k}")
        elif isinstance(v, dict) and not v:
            print(f"  EMPTY_DICT: {k}")
        elif isinstance(v, str) and not v.strip():
            print(f"  EMPTY_STR: {k}")
        else:
            vlen = len(v) if hasattr(v, "__len__") else "N/A"
            print(f"  OK: {k} (type={type(v).__name__}, len={vlen})")
    print()

    # Dump full content for detailed analysis
    fname = f"_archive/report_{row.stock_code.replace('.', '_')}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(cj, f, ensure_ascii=False, indent=2, default=str)
    print(f"  -> Dumped to {fname}")
    print()

db.close()
