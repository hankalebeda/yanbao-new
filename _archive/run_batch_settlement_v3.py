"""
Phase 4b-v3: 正确的批量结算
settlement的 trade_date 参数 = 退出日期（settlement执行日）
对每份报告应传入 report.trade_date + window_days 个交易日后的日期
"""
import os, sys, sqlite3, logging
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

os.environ.setdefault("MOCK_LLM", "true")
os.environ.setdefault("ENABLE_SCHEDULER", "false")
os.environ.setdefault("SETTLEMENT_INLINE_EXECUTION", "true")

logging.basicConfig(level=logging.WARNING)

from app.core.db import SessionLocal
from app.services.settlement_ssot import submit_settlement_task, SettlementServiceError
from app.services.trade_calendar import trade_date_after_n_days

DB_PATH = ROOT / "data" / "app.db"

WINDOW_DAYS = 7  # 结算窗口


def get_settlement_execution_dates():
    """
    返回正确的 settlement 执行日期列表：
    对每个 report trade_date，计算该日 + WINDOW_DAYS 个交易日后的退出日
    """
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("""
        SELECT DISTINCT r.trade_date
        FROM report r
        WHERE r.published = 1
          AND r.is_deleted = 0
          AND r.quality_flag IN ('ok', 'stale_ok')
          AND r.trade_date IS NOT NULL
        ORDER BY r.trade_date
    """)
    report_dates = [row[0] for row in c.fetchall()]
    
    # K线最大日期（不能超过这个日期）
    c.execute("SELECT MAX(trade_date) FROM kline_daily")
    kline_max = c.fetchone()[0]
    conn.close()

    execution_dates = set()
    for rd in report_dates:
        exit_date = trade_date_after_n_days(rd, WINDOW_DAYS)
        if exit_date and exit_date <= kline_max:
            execution_dates.add(exit_date)
        elif kline_max:
            # 如果退出日超出 K 线范围，使用 K 线最大日期（对最近的报告）
            execution_dates.add(kline_max)
    
    return sorted(execution_dates), kline_max


def main():
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    c.execute("SELECT COUNT(1) FROM settlement_result")
    before = c.fetchone()[0]
    conn.close()
    print(f"结算开始前 settlement_result 数量: {before}")

    exec_dates, kline_max = get_settlement_execution_dates()
    print(f"K线最大日期: {kline_max}")
    print(f"待处理 settlement execution date 数量: {len(exec_dates)}")
    if not exec_dates:
        print("无需结算")
        return

    processed = 0
    skipped = 0
    errors = 0

    for i, exec_date in enumerate(exec_dates):
        if i % 5 == 0:
            print(f"  进度: {i}/{len(exec_dates)} (成功={processed}, 跳过={skipped}, 错误={errors})")

        db = SessionLocal()
        try:
            result = submit_settlement_task(
                db,
                trade_date=str(exec_date),
                window_days=WINDOW_DAYS,
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
            elif e.status_code == 500 and "DEPENDENCY" in str(e):
                skipped += 1  # no eligible reports for this date
            else:
                errors += 1
                if errors <= 5:
                    print(f"    SettlementError [{exec_date}]: {e}")
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"    Error [{exec_date}]: {e}")
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
    c.execute("SELECT MIN(exit_trade_date), MAX(exit_trade_date) FROM settlement_result")
    date_range = c.fetchone()
    conn.close()
    print(f"\n结算记录: {after} / 合格研报: {eligible} = {round(after*100/max(eligible,1),1)}%")
    print(f"已结算股票数: {stocks}")
    print(f"结算日期范围: {date_range[0]} ~ {date_range[1]}")


if __name__ == "__main__":
    main()
