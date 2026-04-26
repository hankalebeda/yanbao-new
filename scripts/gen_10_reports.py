"""批量生成 10 只股票的研报 — Phase 4"""
import os
import sys
import time
import traceback

# 确保 MOCK_LLM=false
os.environ["MOCK_LLM"] = "false"
os.environ.setdefault("DATABASE_URL", "sqlite:///./data/app.db")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_report_ssot

TARGET_STOCKS = [
    "688702.SH",  # 盛科通信
    "688048.SH",  # 长光华芯
    "603618.SH",  # 杭电股份
    "688629.SH",  # 华丰科技
    "688167.SH",  # 炬光科技
    "601869.SH",  # 长飞光纤
    "688503.SH",  # 聚和材料
    "688025.SH",  # 杰普特
    "603629.SH",  # 利通电子
    "688519.SH",  # 南亚新材
]

TRADE_DATE = "2026-03-10"


def main():
    ok = 0
    fail = 0
    results = []

    for i, code in enumerate(TARGET_STOCKS, 1):
        print(f"\n[{i}/{len(TARGET_STOCKS)}] 生成研报: {code}", flush=True)
        db = SessionLocal()
        t0 = time.time()
        try:
            result = generate_report_ssot(
                db,
                stock_code=code,
                trade_date=TRADE_DATE,
            )
            elapsed = time.time() - t0
            report_id = result.get("report_id", "?")
            direction = result.get("direction", "?")
            confidence = result.get("confidence", "?")
            print(f"  OK  report_id={report_id}  direction={direction}  confidence={confidence}  {elapsed:.1f}s")
            results.append({"code": code, "status": "OK", "direction": direction, "confidence": confidence, "report_id": report_id})
            ok += 1
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  FAIL  {e}  {elapsed:.1f}s")
            traceback.print_exc()
            results.append({"code": code, "status": "FAIL", "error": str(e)})
            fail += 1
        finally:
            db.close()

    print(f"\n{'='*60}")
    print(f"总计: {ok} 成功, {fail} 失败")
    print(f"{'='*60}")
    for r in results:
        s = f"  {r['code']}  {r['status']}"
        if r['status'] == 'OK':
            s += f"  {r['direction']}  conf={r['confidence']}  id={r['report_id']}"
        else:
            s += f"  {r.get('error', '')}"
        print(s)


if __name__ == "__main__":
    main()
