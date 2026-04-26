"""
批量触发结算 — Phase 4b
直接调用 settlement_ssot.submit_settlement_task，绕过 HTTP 层（无需 INTERNAL_TOKEN）
目标：把有 K 线数据的已发布研报全部结算完毕
"""
import os
import sys
import sqlite3
import logging
from pathlib import Path

# 把项目根目录加到 path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

os.environ.setdefault("MOCK_LLM", "true")
os.environ.setdefault("ENABLE_SCHEDULER", "false")
os.environ.setdefault("SETTLEMENT_INLINE_EXECUTION", "true")

logging.basicConfig(level=logging.WARNING)

from app.core.db import SessionLocal
from app.services.settlement_ssot import submit_settlement_task, SettlementServiceError

DB_PATH = ROOT / "data" / "app.db"


def get_unsettled_trade_dates():
    """返回有 K 线数据且有未结算已发布研报的 trade_date 列表"""
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("""
        SELECT DISTINCT r.trade_date
        FROM report r
        WHERE r.published = 1
          AND EXISTS (SELECT 1 FROM kline_daily k WHERE k.stock_code = r.stock_code)
          AND NOT EXISTS (SELECT 1 FROM settlement_result s WHERE s.report_id = r.report_id)
        ORDER BY r.trade_date
    """)
    dates = [row[0] for row in c.fetchall()]
    conn.close()
    return dates


def main():
    dates = get_unsettled_trade_dates()
    print(f"待结算 trade_date 数量: {len(dates)}")
    if not dates:
        print("无需结算")
        return

    processed = 0
    skipped = 0
    errors = 0

    for i, trade_date in enumerate(dates):
        if i % 20 == 0:
            print(f"  处理进度: {i}/{len(dates)} (已结算: {processed}, 跳过: {skipped}, 错误: {errors})")
        
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
            if status in ("COMPLETED", "completed", "SUCCESS"):
                processed += 1
            elif status in ("SKIPPED", "skipped", "NO_ELIGIBLE_REPORTS"):
                skipped += 1
            else:
                processed += 1  # 接受其他成功状态
        except SettlementServiceError as e:
            if e.status_code == 409:  # CONCURRENT_CONFLICT / already processed
                skipped += 1
            else:
                errors += 1
                print(f"    ERROR {trade_date}: {e.error_code}")
        except Exception as e:
            errors += 1
            print(f"    EXCEPTION {trade_date}: {e}")
        finally:
            db.close()

    print(f"\n=== 结算批次完成 ===")
    print(f"  成功: {processed}")
    print(f"  跳过: {skipped}")
    print(f"  错误: {errors}")

    # 最终统计
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("SELECT COUNT(1) FROM settlement_result")
    total_settled = c.fetchone()[0]
    c.execute("SELECT COUNT(1) FROM report WHERE published=1")
    total_published = c.fetchone()[0]
    conn.close()
    pct = total_settled * 100 // total_published
    print(f"\n结算记录: {total_settled} / 已发布研报: {total_published} = {pct}%")


if __name__ == "__main__":
    main()
