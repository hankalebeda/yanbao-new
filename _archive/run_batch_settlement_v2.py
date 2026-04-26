"""
Phase 4b-v2: 批量结算（包含 stale_ok 报告）
修复 FR07EligibleReportFilter 扩展后，重新对所有 trade_date 结算
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


def get_all_settlement_dates():
    """返回所有有 K 线且已发布研报（ok + stale_ok）的 trade_date 列表"""
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("""
        SELECT DISTINCT r.trade_date
        FROM report r
        WHERE r.published = 1
          AND r.is_deleted = 0
          AND r.quality_flag IN ('ok', 'stale_ok')
          AND r.trade_date IS NOT NULL
          AND EXISTS (SELECT 1 FROM kline_daily k WHERE k.stock_code = r.stock_code)
        ORDER BY r.trade_date
    """)
    dates = [row[0] for row in c.fetchall()]
    conn.close()
    return dates


def main():
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("SELECT COUNT(1) FROM settlement_result")
    before = c.fetchone()[0]
    conn.close()
    print(f"结算开始前 settlement_result 数量: {before}")

    dates = get_all_settlement_dates()
    print(f"待处理 trade_date 数量: {len(dates)}")
    if not dates:
        print("无需结算")
        return

    processed = 0
    skipped = 0
    errors = 0

    for i, trade_date in enumerate(dates):
        if i % 50 == 0:
            print(f"  进度: {i}/{len(dates)} (成功={processed}, 跳过={skipped}, 错误={errors})")

        db = SessionLocal()
        try:
            result = submit_settlement_task(
                db,
                trade_date=str(trade_date),
                window_days=7,
                target_scope="all",
                force=True,
                run_inline=True,
            )
            db.commit()
            status = result.get("status", "unknown")
            if status in ("SKIPPED", "skipped", "NO_ELIGIBLE_REPORTS"):
                skipped += 1
            else:
                processed += 1
        except SettlementServiceError as e:
            if e.status_code == 409:
                skipped += 1
            else:
                errors += 1
                if errors <= 5:
                    print(f"    SettlementError [{trade_date}]: {e}")
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"    Error [{trade_date}]: {e}")
        finally:
            db.close()

    print(f"\n=== 批量结算完成 ===")
    print(f"  成功: {processed}, 跳过: {skipped}, 错误: {errors}")

    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("SELECT COUNT(1) FROM settlement_result")
    after = c.fetchone()[0]
    c.execute("SELECT COUNT(1) FROM report WHERE published=1 AND is_deleted=0 AND quality_flag IN ('ok','stale_ok')")
    eligible = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT stock_code) FROM settlement_result")
    stocks = c.fetchone()[0]
    conn.close()
    print(f"\n结算记录: {after} / 合格研报: {eligible} = {round(after*100/max(eligible,1),1)}%")
    print(f"已结算股票数: {stocks}")


if __name__ == "__main__":
    main()
