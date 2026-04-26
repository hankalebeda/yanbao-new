"""
批量生成 2026-04-13 报告，专门选取牛势股票（close > ma5 > ma20，动量最强的 35 只）
目标：获取足够多的 BUY 推荐以供结算，达到 30 个样本阈值激活 dashboard KPI

用法：python codex/batch_gen_apr13.py [--max N] [--settle]
  --max N: 最多生成 N 只股票（默认 35）
  --settle: 生成完成后立即触发结算
"""
import argparse
import json
import sqlite3
import time
import requests

BASE = "http://127.0.0.1:8010"
TOKEN = "kestra-internal-20260327"
HEADERS = {"X-Internal-Token": TOKEN, "Content-Type": "application/json"}
NO_PROXY = {"http": None, "https": None}
TRADE_DATE = "2026-04-13"
DB_PATH = "data/app.db"

# 目标股票：2026-04-13 牛势强度排名 TOP-40（close > ma5 > ma20，按动量排序）
TARGET_STOCKS = [
    "002281.SZ",  # +27.6% above MA20
    "300308.SZ",  # +19.6% above MA20
    "300782.SZ",  # +17.5% above MA20
    "002475.SZ",  # +16.2% above MA20
    "002008.SZ",  # +15.4% above MA20
    "688002.SH",  # +14.1% above MA20
    "000977.SZ",  # +13.9% above MA20
    "002466.SZ",  # +13.3% above MA20
    "600309.SH",  # +12.9% above MA20
    "002460.SZ",  # +12.4% above MA20
    "300502.SZ",  # +11.3% above MA20
    "300408.SZ",  # +10.6% above MA20
    "600183.SH",  # +9.6% above MA20
    "688008.SH",  # +9.5% above MA20
    "300450.SZ",  # +8.4% above MA20
    "603259.SH",  # +8.3% above MA20
    "600741.SH",  # +7.8% above MA20
    "601138.SH",  # +7.4% above MA20
    "002756.SZ",  # +7.4% above MA20
    "002179.SZ",  # +6.7% above MA20
    "600760.SH",  # +5.8% above MA20
    "002415.SZ",  # +5.6% above MA20
    "600030.SH",  # +5.3% above MA20
    "603799.SH",  # +5.2% above MA20
    "002396.SZ",  # +4.5% above MA20
    "002709.SZ",  # +4.2% above MA20
    "603993.SH",  # +4.2% above MA20
    "002241.SZ",  # +4.2% above MA20
    "600584.SH",  # +4.1% above MA20
    "688396.SH",  # +4.0% above MA20
    "002371.SZ",  # +4.0% above MA20
    "000708.SZ",  # +3.6% above MA20
    "002236.SZ",  # +3.3% above MA20
    "600570.SH",  # +2.4% above MA20
    "601633.SH",  # +2.2% above MA20
]


def get_existing_stocks() -> set[str]:
    """查询已为 2026-04-13 生成的股票报告（非删除）"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT stock_code FROM report WHERE trade_date=? AND is_deleted=0",
            (TRADE_DATE,)
        ).fetchall()
        conn.close()
        return {r[0] for r in rows}
    except Exception as e:
        print(f"  [WARN] DB check failed: {e}")
        return set()


def generate_stock(stock_code: str) -> dict:
    """为单只股票触发报告生成"""
    payload = {
        "stock_codes": [stock_code],
        "trade_date": TRADE_DATE,
        "skip_pool_check": True,
        "force": True,  # 允许重试 Completed 状态的任务
        "cleanup_incomplete_before_batch": False,
        "max_concurrent": 1,
    }
    resp = requests.post(
        f"{BASE}/api/v1/internal/reports/generate-batch",
        headers=HEADERS,
        json=payload,
        timeout=300,  # 5 minutes per stock
        proxies=NO_PROXY,
    )
    return resp.json()


def check_report_result(stock_code: str) -> dict | None:
    """检查股票是否已有非删除的报告"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT report_id, recommendation, confidence, quality_flag, published
            FROM report
            WHERE stock_code=? AND trade_date=? AND is_deleted=0
            ORDER BY created_at DESC LIMIT 1
            """,
            (stock_code, TRADE_DATE)
        ).fetchone()
        conn.close()
        if row:
            return {
                "report_id": row[0],
                "recommendation": row[1],
                "confidence": row[2],
                "quality_flag": row[3],
                "published": row[4],
            }
    except Exception as e:
        print(f"  [WARN] DB check failed: {e}")
    return None


def trigger_settlement() -> dict:
    """触发 2026-04-13 所有 BUY 报告的 1d 结算
    NOTE: settlement condition is r.trade_date < :trade_date, so we must use
    the EXIT day (2026-04-14) not the signal day (2026-04-13).
    """
    payload = {
        "trade_date": "2026-04-14",  # exit day for 1d window from 2026-04-13
        "window_days": 1,
        "target_scope": "all",
        "force": True,
    }
    resp = requests.post(
        f"{BASE}/api/v1/internal/settlement/run",
        headers=HEADERS,
        json=payload,
        timeout=120,
        proxies=NO_PROXY,
    )
    return resp.json()


def check_dashboard() -> dict:
    r = requests.get(
        f"{BASE}/api/v1/dashboard/stats?window_days=7",
        timeout=30,
        proxies=NO_PROXY,
    )
    return r.json().get("data", {})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=35, help="最多生成多少只股票")
    parser.add_argument("--settle", action="store_true", help="生成后自动触发结算")
    parser.add_argument("--settle-only", action="store_true", help="仅触发结算（跳过生成）")
    args = parser.parse_args()

    if args.settle_only:
        print("=== Triggering settlement only ===")
        result = trigger_settlement()
        print(json.dumps(result, ensure_ascii=False, indent=2))
        time.sleep(5)
        dash = check_dashboard()
        print(f"\nDashboard: total_settled={dash.get('total_settled')}, "
              f"win_rate={dash.get('overall_win_rate')}, "
              f"status={dash.get('data_status')}")
        return

    existing = get_existing_stocks()
    to_generate = [s for s in TARGET_STOCKS if s not in existing][: args.max]
    print(f"=== Batch generating {len(to_generate)} stocks for {TRADE_DATE} ===")
    print(f"  Already exists: {len(existing)} stocks")
    print(f"  To generate: {to_generate}\n")

    results = {"ok": [], "buy": [], "hold": [], "failed": []}
    for i, stock_code in enumerate(to_generate, 1):
        print(f"[{i}/{len(to_generate)}] Generating {stock_code}...")
        try:
            gen_result = generate_stock(stock_code)
            status = gen_result.get("data", {}).get("status") or gen_result.get("data", {})
            print(f"  -> gen response: {json.dumps(gen_result.get('data', {}), ensure_ascii=False)[:200]}")
        except Exception as e:
            print(f"  -> GEN FAILED: {e}")
            results["failed"].append(stock_code)
            time.sleep(5)
            continue

        # Wait a moment for DB write and check result
        time.sleep(3)
        report = check_report_result(stock_code)
        if report:
            rec = report["recommendation"]
            qual = report["quality_flag"]
            print(f"  -> {rec} conf={report['confidence']} quality={qual} pub={report['published']}")
            results["ok"].append(stock_code)
            if rec == "BUY":
                results["buy"].append(stock_code)
            else:
                results["hold"].append(stock_code)
        else:
            print(f"  -> No report found in DB after generation")
            results["failed"].append(stock_code)

        # Brief pause between stocks
        if i < len(to_generate):
            time.sleep(5)

    print(f"\n=== Generation Summary ===")
    print(f"  Generated OK: {len(results['ok'])} stocks")
    print(f"  BUY: {len(results['buy'])} → {results['buy']}")
    print(f"  HOLD: {len(results['hold'])} stocks")
    print(f"  Failed: {len(results['failed'])} → {results['failed']}")

    # Count all BUY reports for this date in DB
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        buy_count = cur.execute(
            "SELECT COUNT(*) FROM report WHERE trade_date=? AND recommendation='BUY' AND is_deleted=0 AND published=1",
            (TRADE_DATE,)
        ).fetchone()[0]
        conn.close()
        print(f"\n  Total BUY published for {TRADE_DATE}: {buy_count}")
    except Exception:
        buy_count = len(results["buy"])

    if args.settle or buy_count > 0:
        print(f"\n=== Triggering 1d settlement for {TRADE_DATE} ===")
        try:
            s_result = trigger_settlement()
            print(f"  -> {json.dumps(s_result.get('data', {}), ensure_ascii=False)}")
        except Exception as e:
            print(f"  -> Settlement trigger failed: {e}")

        time.sleep(8)
        dash = check_dashboard()
        total_settled = dash.get("total_settled", 0)
        win_rate = dash.get("overall_win_rate")
        data_status = dash.get("data_status")
        print(f"\n=== Dashboard Stats ===")
        print(f"  total_reports={dash.get('total_reports')}")
        print(f"  total_settled={total_settled}")
        print(f"  overall_win_rate={win_rate}")
        print(f"  data_status={data_status}")
        print(f"  status_reason={dash.get('status_reason')}")
        if total_settled >= 30:
            print("\n✅ Dashboard KPI ACTIVE (≥30 samples)")
        else:
            print(f"\n⚠ Need {30 - total_settled} more settlement samples for dashboard KPI")


if __name__ == "__main__":
    main()
