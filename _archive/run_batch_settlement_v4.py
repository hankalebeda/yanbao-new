"""
Phase 4b-v4: 以最新 K 线日期作为统一 settlement 执行日
覆盖所有已到期的报告（trade_date + 7 trading_days <= kline_max_date）
不再使用 force=True 删除历史; 对未结算的报告再补充一遍
"""
import os, sys, sqlite3, logging
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

os.environ.setdefault("MOCK_LLM", "true")
os.environ.setdefault("ENABLE_SCHEDULER", "false")
os.environ.setdefault("SETTLEMENT_INLINE_EXECUTION", "true")

logging.basicConfig(level=logging.WARNING)

from app.core.db import SessionLocal
from app.services.settlement_ssot import submit_settlement_task, SettlementServiceError

DB_PATH = ROOT / "data" / "app.db"
WINDOW_DAYS = 7


def get_kline_max():
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("SELECT MAX(trade_date) FROM kline_daily")
    d = c.fetchone()[0]
    conn.close()
    return d


def get_eligible_reports_count():
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("SELECT COUNT(1) FROM report WHERE published=1 AND is_deleted=0 AND quality_flag IN ('ok','stale_ok')")
    n = c.fetchone()[0]
    conn.close()
    return n


def main():
    kline_max = get_kline_max()
    eligible = get_eligible_reports_count()
    
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("SELECT COUNT(1) FROM settlement_result")
    before = c.fetchone()[0]
    conn.close()

    print(f"K线最大日期: {kline_max}")
    print(f"合格研报数: {eligible}")
    print(f"结算开始前 settlement_result 数量: {before}")
    print(f"\n以 {kline_max} 作为统一 settlement 执行日...")

    db = SessionLocal()
    try:
        result = submit_settlement_task(
            db,
            trade_date=kline_max,
            window_days=WINDOW_DAYS,
            target_scope="all",
            force=True,  # 重新计算所有记录
            run_inline=True,
        )
        db.commit()
        print(f"Settlement 状态: {result.get('status')}")
        print(f"详情: processed={result.get('processed')}, skipped={result.get('skipped')}, failed={result.get('failed')}")
    except SettlementServiceError as e:
        print(f"SettlementError: {e}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback; traceback.print_exc()
    finally:
        db.close()

    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("SELECT COUNT(1) FROM settlement_result")
    after = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT stock_code) FROM settlement_result")
    stocks = c.fetchone()[0]
    c.execute("SELECT MIN(exit_trade_date), MAX(exit_trade_date) FROM settlement_result")
    date_range = c.fetchone()
    c.execute("SELECT settlement_status, COUNT(1) FROM settlement_result GROUP BY settlement_status")
    status_dist = c.fetchall()
    conn.close()

    print(f"\n=== 结果 ===")
    print(f"结算记录: {after} / 合格研报: {eligible} = {round(after*100/max(eligible,1),1)}%")
    print(f"已结算股票数: {stocks}")
    print(f"退出日期范围: {date_range[0]} ~ {date_range[1]}")
    print(f"状态分布: {status_dist}")


if __name__ == "__main__":
    main()
