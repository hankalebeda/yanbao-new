"""测试 eastmoney kline 是否可访问，然后批量补采200股核心池"""
import asyncio
import os
os.environ['NO_PROXY'] = '*'

import sys
sys.path.insert(0, 'D:/yanbao-new')

async def test_one():
    from app.services.market_data import fetch_recent_klines
    result = await fetch_recent_klines('601888.SH', limit=5)
    print('601888.SH kline result (latest 5):', result[:2] if result else 'EMPTY/FAILED')
    return result

if __name__ == '__main__':
    r = asyncio.run(test_one())
    if r:
        print(f'SUCCESS: got {len(r)} records')
        print(f'Latest: {r[0]}')
    else:
        print('FAILED: no data returned')
