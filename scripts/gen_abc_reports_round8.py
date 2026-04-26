"""
Round 8 报告生成脚本: 2026-04-16 A/B/C 各1份（MOCK_LLM）
- 目标日期: 2026-04-16（数据完整: kline=ok, hotspot=ok, market_state=ok）
- 策略A: 600519.SH, 策略B: 300750.SZ, 策略C: 000001.SZ（沿用Round 6分配）
- force_same_day_rebuild=True: 替换现有 llm_fallback_level=failed 的空报告
- MOCK_LLM=true: 产出完整规则化内容（推理链/结论/策略证据/置信度）
- 结果: published=false（mock模式设计约束），llm_fallback_level=local
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ["MOCK_LLM"] = "true"
os.environ["ENABLE_SCHEDULER"] = "false"
os.environ["SETTLEMENT_INLINE_EXECUTION"] = "false"

import asyncio
from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_reports_batch

TARGET_DATE = "2026-04-16"
# 按 Round 6 的策略分配: A→600519, B→300750, C→000001
STOCKS = ["600519.SH", "000001.SZ", "300750.SZ"]
STRATEGY_OVERRIDE = {
    "600519.SH": "A",
    "000001.SZ": "C",
    "300750.SZ": "B",
}


def main():
    print(f"[Gen] target_date={TARGET_DATE} stocks={STOCKS}")
    print(f"[Gen] strategy_override={STRATEGY_OVERRIDE}")
    print("[Gen] MOCK_LLM=true (rule-based content, published=false by design)")
    print()

    result = generate_reports_batch(
        SessionLocal,
        stock_codes=STOCKS,
        trade_date=TARGET_DATE,
        skip_pool_check=False,
        force_same_day_rebuild=True,
        max_concurrent_override=1,
        one_per_strategy_type=True,
        strategy_type_override_map=STRATEGY_OVERRIDE,
    )

    print(f"[Result] total={result.get('total')} succeeded={result.get('succeeded')} failed={result.get('failed')}")
    print()
    for item in result.get("results", []):
        code = item.get("stock_code")
        status = item.get("status")
        if status == "ok":
            r = item.get("result", {})
            print(f"  ✓ {code}: report_id={str(r.get('report_id',''))[:8]} strategy={r.get('strategy_type')} "
                  f"published={r.get('published')} fallback={r.get('llm_fallback_level')} quality={r.get('quality_flag')}")
        else:
            print(f"  ✗ {code}: {item.get('error_code','')}")


if __name__ == "__main__":
    main()
