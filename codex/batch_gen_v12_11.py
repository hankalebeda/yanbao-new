"""
Batch generate reports using real LLM (CLIProxyAPI gpt-5.4).
Targets stocks with recent K-line data.
"""
import requests
import json
import time

BASE = "http://127.0.0.1:8010"
TOKEN = "kestra-internal-20260327"
HEADERS = {"X-Internal-Token": TOKEN, "Content-Type": "application/json"}
TRADE_DATE = "2026-04-22"

# Stocks with recent K-line data and known history
BATCH_STOCKS = [
    # Round 1 - latest K-line (2026-04-22)
    ["688002.SH", "600519.SH", "000858.SZ", "002594.SZ", "000001.SZ"],
    # Round 2
    ["000002.SZ", "000069.SZ", "000100.SZ", "000157.SZ", "000301.SZ"],
    # Round 3
    ["000333.SZ", "000400.SZ", "000423.SZ", "000538.SZ", "600036.SH"],
    # Round 4 - 688 series
    ["688001.SH", "688003.SH", "688005.SH", "688006.SH", "688007.SH"],
    # Round 5
    ["600000.SH", "600016.SH", "600030.SH", "600050.SH", "600104.SH"],
    # Round 6
    ["600887.SH", "601318.SH", "601336.SH", "000568.SZ", "002415.SZ"],
]

all_results = []

for round_num, batch in enumerate(BATCH_STOCKS, 1):
    print(f"\n=== Round {round_num}/{len(BATCH_STOCKS)} | stocks: {batch} ===")
    
    payload = {
        "stock_codes": batch,
        "trade_date": TRADE_DATE,
        "force": False,
        "skip_pool_check": True,  # Skip pool check since pool_task table doesn't exist
        "cleanup_incomplete_before_batch": True,
    }
    
    try:
        resp = requests.post(
            f"{BASE}/api/v1/internal/reports/generate-batch",
            headers=HEADERS,
            json=payload,
            timeout=600,  # 10 min timeout for real LLM calls
        )
        
        if resp.status_code in (200, 202):
            data = resp.json()
            result_data = data.get("data", {})
            succeeded = result_data.get("succeeded", 0)
            failed = result_data.get("failed", 0)
            total = result_data.get("total", 0)
            elapsed = result_data.get("elapsed_s", 0)
            print(f"  ✅ total={total}, succeeded={succeeded}, failed={failed}, elapsed={elapsed:.1f}s")
            
            # Print detail of each report
            for detail in (result_data.get("details") or []):
                sc = detail.get("stock_code", "?")
                status = detail.get("status", "?")
                r = detail.get("result", {})
                if r:
                    rec = r.get("recommendation", "?")
                    conf = r.get("confidence", 0)
                    qf = r.get("quality_flag", "?")
                    pub = r.get("published", False)
                    strategy = r.get("strategy_type", "?")
                    fallback = r.get("llm_fallback_level", "?")
                    print(f"    {sc} {status}: {rec} conf={conf} quality={qf} published={pub} strategy={strategy} llm={fallback}")
                else:
                    err = detail.get("error", "unknown")
                    print(f"    {sc} {status}: ERROR={err}")
            
            all_results.append({
                "round": round_num,
                "batch": batch,
                "succeeded": succeeded,
                "failed": failed,
                "details": result_data.get("details", []),
            })
        else:
            print(f"  ❌ HTTP {resp.status_code}: {resp.text[:300]}")
            all_results.append({"round": round_num, "batch": batch, "http_error": resp.status_code})
    
    except requests.exceptions.Timeout:
        print(f"  ⏰ Timeout after 600s")
        all_results.append({"round": round_num, "batch": batch, "error": "timeout"})
    except Exception as e:
        print(f"  💥 Exception: {e}")
        all_results.append({"round": round_num, "batch": batch, "error": str(e)})
    
    # Brief pause between rounds to avoid overwhelming the LLM service
    if round_num < len(BATCH_STOCKS):
        print(f"  Waiting 5s before next round...")
        time.sleep(5)

# Final summary
print("\n" + "="*60)
print("BATCH GENERATION SUMMARY")
print("="*60)
total_succeeded = sum(r.get("succeeded", 0) for r in all_results)
total_failed = sum(r.get("failed", 0) for r in all_results)
print(f"Total succeeded: {total_succeeded}")
print(f"Total failed: {total_failed}")

# Check DB state
import sqlite3
db = sqlite3.connect(r'd:\yanbao-new\data\app.db')
c = db.cursor()
c.execute('SELECT COUNT(*) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND published=1')
pub = c.fetchone()[0]
c.execute('SELECT COUNT(*) FROM report WHERE is_deleted=0 OR is_deleted IS NULL')
alive = c.fetchone()[0]
c.execute('SELECT trade_date, COUNT(*), GROUP_CONCAT(recommendation) FROM report WHERE (is_deleted=0 OR is_deleted IS NULL) AND published=1 GROUP BY trade_date ORDER BY trade_date DESC')
td_dist = c.fetchall()
db.close()

print(f"\nDB state after generation:")
print(f"  alive={alive}, published={pub}")
print(f"  trade_date distribution (published):", td_dist)
