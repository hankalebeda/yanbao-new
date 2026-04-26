"""v24 一次性样本采集+生成脚本：
1. 对 1 只股票（600519.SH）+ 1 个日期（2026-04-16）采集 3 个 capital 数据集
2. 通过 HTTP 调用 generate-batch 生成研报
3. 打印结果让用户确认

严苛：只允许 remote-ok 的真实数据；任一 proxy/cache → 硬闸 422。
"""
import asyncio, sys, json, httpx
sys.path.insert(0, r"d:\yanbao-new")

from app.core.db import SessionLocal
from app.services.capital_usage_collector import persist_capital_usage

STOCK = "600519.SH"
TRADE_DATE = "2026-04-16"
TOKEN = "kestra-internal-20260327"
URL = "http://127.0.0.1:8000/api/v1/internal/reports/generate-batch"


async def collect():
    db = SessionLocal()
    try:
        summary = await persist_capital_usage(db, stock_code=STOCK, trade_date=TRADE_DATE)
        return summary
    finally:
        db.close()


def main():
    print(f"[sample] 1/3 collect capital usage for {STOCK} @ {TRADE_DATE} ...")
    summary = asyncio.run(collect())
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    all_ok = all(v["persisted_status"] == "ok" for v in summary["per_dataset"].values())
    print(f"[sample] all 3 capital datasets ok? {all_ok}")

    print(f"\n[sample] 2/3 invoke generate-batch ...")
    with httpx.Client(trust_env=False, timeout=180.0) as c:
        r = c.post(
            URL,
            headers={"X-Internal-Token": TOKEN, "Content-Type": "application/json"},
            json={
                "stock_codes": [STOCK],
                "trade_date": TRADE_DATE,
                "force": True,
                "skip_pool_check": False,
                "cleanup_incomplete_before_batch": False,
            },
        )
    print(f"HTTP {r.status_code}")
    body = r.json()
    print(json.dumps(body, ensure_ascii=False, indent=2, default=str)[:2000])

    details = body.get("data", {}).get("details", [{}])[0]
    if details.get("status") == "success":
        print(f"\n[sample] 3/3 fetch new report")
        rid = details.get("report_id")
        if rid:
            with httpx.Client(trust_env=False, timeout=30.0) as c:
                r = c.get(f"http://127.0.0.1:8000/api/v1/reports/{rid}")
            print(f"GET report HTTP {r.status_code}")
            data = r.json().get("data", {})
            cgs = data.get("capital_game_summary", {}) or {}
            print("capital_game_summary.has_real_conclusion:", cgs.get("has_real_conclusion"))
            print("  main_force.status:", (cgs.get("main_force") or {}).get("status"))
            print("  dragon_tiger.lhb_count_30d:", (cgs.get("dragon_tiger") or {}).get("lhb_count_30d"))
            print("  margin_financing.status:", (cgs.get("margin_financing") or {}).get("status"))
            with open(rf"d:\yanbao-new\_archive\audit_v24_phase1_evidence\sample_{STOCK}_{TRADE_DATE}.json", "w", encoding="utf-8") as f:
                json.dump(r.json(), f, ensure_ascii=False, indent=2, default=str)
            print(f"[sample] saved → _archive/audit_v24_phase1_evidence/sample_{STOCK}_{TRADE_DATE}.json")


if __name__ == "__main__":
    main()
