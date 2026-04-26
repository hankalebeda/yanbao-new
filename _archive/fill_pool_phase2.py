"""
Phase 2: 重建 stock_pool_snapshot（利用新填充的 kline_daily 数据）
对 2026-04-17 到 2026-04-24 执行 refresh_stock_pool
"""
import os
import sys
from pathlib import Path

# 设置项目路径
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault('MOCK_LLM', 'true')
os.environ.setdefault('ENABLE_SCHEDULER', 'false')

from app.core.db import SessionLocal, Base, engine
import app.models  # noqa: F401 - ensure all ORM models register with metadata
# Ensure tables exist in SQLite (also ensures metadata is populated)
Base.metadata.create_all(bind=engine)
from app.services.stock_pool import refresh_stock_pool
from app.services.trade_calendar import latest_trade_date_str
import logging

logging.basicConfig(level=logging.WARNING)

def run_refresh_for_dates():
    # 需要重建的日期列表（从新到旧）
    target_dates = [
        '2026-04-24',
        '2026-04-23',
        '2026-04-22',
        '2026-04-21',
        '2026-04-20',
        '2026-04-17',
    ]
    
    results = []
    for td in target_dates:
        print(f'\n=== 重建 stock_pool for {td} ===')
        db = SessionLocal()
        try:
            result = refresh_stock_pool(
                db,
                trade_date=td,
                force_rebuild=True,  # 强制重建
            )
            db.commit()
            print(f'  Status: {result.get("status")}')
            print(f'  Core pool: {result.get("core_pool_size")} stocks')
            print(f'  Standby pool: {result.get("standby_pool_size")} stocks')
            if result.get("status_reason"):
                print(f'  Reason: {result.get("status_reason")}')
            results.append({'date': td, 'success': True, 'result': result})
        except Exception as e:
            print(f'  ERROR: {e}')
            results.append({'date': td, 'success': False, 'error': str(e)})
        finally:
            db.close()
    
    print('\n=== 汇总 ===')
    for r in results:
        if r['success']:
            res = r['result']
            print(f"  {r['date']}: {res.get('status')} (core={res.get('core_pool_size')}, standby={res.get('standby_pool_size')})")
        else:
            print(f"  {r['date']}: FAILED - {r['error']}")
    
    return results

if __name__ == '__main__':
    run_refresh_for_dates()
