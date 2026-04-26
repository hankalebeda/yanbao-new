"""v25 research-report matrix sample generator.

Per user request (2026-04-18):
  - gpt-5.4 primary LLM
  - one sample per category, diversified stocks
  - strictest remote-real-time data (v25 hard-gate: 9 datasets per stock)
  - stock_profile + main_force/dragon_tiger/margin_financing + kline/hotspot/northbound/etf/market_state

Picks 3 stocks from different sectors/sizes expected to yield diverse recommendations:
  600519.SH 贵州茅台 (白酒, 大盘蓝筹)
  002594.SZ 比亚迪 (新能源车)
  000858.SZ 五粮液 (白酒, 大盘蓝筹)
"""
from __future__ import annotations
import asyncio
import json
import sys
import time

import httpx

sys.path.insert(0, r"d:\yanbao-new")

from app.core.db import SessionLocal
from app.services.capital_usage_collector import persist_capital_usage
from app.services.stock_profile_collector import persist_stock_profile

STOCKS = ["600519.SH", "002594.SZ", "000858.SZ"]
TRADE_DATE = "2026-04-16"
TOKEN = "kestra-internal-20260327"
BATCH_URL = "http://127.0.0.1:8000/api/v1/internal/reports/generate-batch"
REPORT_URL = "http://127.0.0.1:8000/api/v1/reports/{rid}"
VIEW_URL = "http://127.0.0.1:8000/reports/{rid}"

OUT = r"d:\yanbao-new\_archive\audit_v24_phase1_evidence\v25_matrix_results.json"


async def _collect_capital(stock: str) -> dict:
    db = SessionLocal()
    try:
        return await persist_capital_usage(db, stock_code=stock, trade_date=TRADE_DATE)
    finally:
        db.close()


def _collect_profile(stock: str) -> dict:
    db = SessionLocal()
    try:
        return persist_stock_profile(db, stock_code=stock, trade_date=TRADE_DATE)
    finally:
        db.close()


def main() -> None:
    results: dict = {"stocks": {}, "trade_date": TRADE_DATE}
    # Phase 1: backfill data
    for stock in STOCKS:
        print(f"[v25] backfill {stock} ...")
        cap = asyncio.run(_collect_capital(stock))
        prof = _collect_profile(stock)
        cap_ok = all(v.get("persisted_status") == "ok" for v in cap.get("per_dataset", {}).values())
        prof_ok = prof.get("persisted_status") == "ok"
        print(f"  capital_all_ok={cap_ok}  profile_ok={prof_ok}  profile={prof.get('snapshot')}")
        results["stocks"][stock] = {
            "capital_summary": {k: v.get("persisted_status") for k, v in cap.get("per_dataset", {}).items()},
            "profile_status": prof.get("persisted_status"),
            "profile_snapshot": prof.get("snapshot"),
            "profile_reason": prof.get("reason"),
        }

    # Phase 2: generate reports
    print(f"\n[v25] invoke generate-batch for {len(STOCKS)} stocks ...")
    t0 = time.time()
    with httpx.Client(trust_env=False, timeout=600.0) as c:
        r = c.post(
            BATCH_URL,
            headers={"X-Internal-Token": TOKEN, "Content-Type": "application/json"},
            json={
                "stock_codes": STOCKS,
                "trade_date": TRADE_DATE,
                "force": True,
                "skip_pool_check": True,
                "cleanup_incomplete_before_batch": False,
            },
        )
    print(f"HTTP {r.status_code}  elapsed={time.time()-t0:.1f}s")
    body = r.json() if r.status_code < 500 else {"raw": r.text[:4000]}
    results["batch_http_status"] = r.status_code
    results["batch_body"] = body
    details = (body.get("data") or {}).get("details") or []
    for d in details:
        sc = d.get("stock_code")
        if sc in results["stocks"]:
            results["stocks"][sc]["gen_status"] = d.get("status")
            results["stocks"][sc]["report_id"] = d.get("report_id")
            results["stocks"][sc]["error"] = d.get("error") or d.get("error_code")

    # Phase 3: fetch view-payload key fields
    print("\n[v25] fetch generated reports ...")
    with httpx.Client(trust_env=False, timeout=30.0) as c:
        for stock, info in results["stocks"].items():
            rid = info.get("report_id")
            if not rid:
                continue
            rj = c.get(REPORT_URL.format(rid=rid)).json()
            data = rj.get("data") or rj
            ms = (data.get("dimensions") or {}).get("market_snapshot") or {}
            val = data.get("valuation") or {}
            co = data.get("company_overview") or {}
            cgs = ((data.get("plain_report") or {}).get("capital_game_summary")) or data.get("capital_game_summary") or {}
            info["view_url"] = VIEW_URL.format(rid=rid)
            info["recommendation"] = data.get("recommendation") or data.get("recommendation_cn")
            info["confidence"] = data.get("confidence")
            info["summary_fields"] = {
                "last_price": ms.get("last_price"),
                "pct_change": ms.get("pct_change"),
                "ma5": ms.get("ma5"),
                "ma20": ms.get("ma20"),
                "pe_ttm": val.get("pe_ttm"),
                "pb": val.get("pb"),
                "total_mv": val.get("total_mv"),
                "region": val.get("region"),
                "list_date": co.get("list_date"),
                "industry": co.get("industry"),
                "main_force_5d": (cgs.get("main_force") or {}).get("net_inflow_5d_fmt"),
                "lhb_count_30d": (cgs.get("dragon_tiger") or {}).get("lhb_count_30d"),
                "margin_5d": (cgs.get("margin_financing") or {}).get("rzye_delta_5d_fmt"),
            }
            print(f"\n[{stock}] reco={info['recommendation']}  url={info['view_url']}")
            print(f"  {json.dumps(info['summary_fields'], ensure_ascii=False)}")

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[v25] saved → {OUT}")


if __name__ == "__main__":
    main()
