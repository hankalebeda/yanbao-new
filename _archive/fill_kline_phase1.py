"""
Phase 1: 用 TDX 本地文件全量补充 kline_daily（历史数据）
覆盖 stock_master 中所有 5194 只活跃股票
日期范围：2024-12-17 至 2026-03-13（TDX本地最后日期）
"""
import sqlite3, struct, uuid, sys, os
sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime
from pathlib import Path

DB_PATH = 'data/app.db'
TDX_VIPDOC = Path('C:/new_tdx/vipdoc')
DATE_FROM = '2024-12-17'
TDX_LAST_DATE = '2026-03-13'
SOURCE_BATCH_ID = 'tdx_bulk_fill_' + datetime.now().strftime('%Y%m%d')
BATCH_SIZE = 2000

print('Starting TDX Phase 1 fill...', flush=True)
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx
import sqlite3

# 配置
DB_PATH = 'data/app.db'
TENCENT_KLINE_URL = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

def _to_tencent_symbol(stock_code: str) -> str:
    code = stock_code.split('.')[0]
    if stock_code.endswith('.SH') or code.startswith('6') or code.startswith('9'):
        return 'sh' + code
    return 'sz' + code


async def fetch_tencent_kline(stock_code: str, start_date: str, end_date: str, limit: int = 60) -> list[dict]:
    """获取腾讯K线数据，返回 [{trade_date, open, high, low, close, volume, amount}]"""
    sym = _to_tencent_symbol(stock_code)
    params = {
        'param': f'{sym},day,,{end_date},{limit},qfq',
        '_var': 'kline_day'
    }
    try:
        async with httpx.AsyncClient(timeout=15, trust_env=False) as c:
            r = await c.get(TENCENT_KLINE_URL, params=params, headers=HEADERS)
            text = r.text
            if text.startswith('kline_day='):
                text = text[10:]
            data = json.loads(text)
            if data.get('code') != 0:
                return []
            stock_data = data['data'].get(sym, {})
            kline = stock_data.get('qfqday', stock_data.get('day', []))
            out = []
            for row in kline:
                # [date, open, close, high, low, volume_lots]
                if len(row) < 5:
                    continue
                td = row[0]
                if td < start_date or td > end_date:
                    continue
                try:
                    open_p = float(row[1])
                    close_p = float(row[2])
                    high_p = float(row[3])
                    low_p = float(row[4])
                    vol_lots = float(row[5]) if len(row) > 5 else 0
                    volume = vol_lots * 100  # lots to shares
                    # amount = volume * avg_price (approximate)
                    avg_price = (open_p + close_p + high_p + low_p) / 4
                    amount = volume * avg_price
                    out.append({
                        'trade_date': td,
                        'open': round(open_p, 4),
                        'high': round(high_p, 4),
                        'low': round(low_p, 4),
                        'close': round(close_p, 4),
                        'volume': round(volume, 2),
                        'amount': round(amount, 2),
                    })
                except Exception:
                    continue
            return out
    except Exception as e:
        print(f'  Tencent kline fetch failed for {stock_code}: {e}')
        return []


def compute_ma(closes: list[float], n: int) -> float | None:
    if len(closes) < n:
        return None
    return round(sum(closes[-n:]) / n, 4)


def insert_kline_rows(conn, stock_code: str, rows: list[dict], batch_id: str) -> int:
    """插入K线数据，跳过已存在的记录"""
    cur = conn.cursor()
    inserted = 0
    # 获取该股票已有的trade_date集合
    cur.execute("SELECT trade_date FROM kline_daily WHERE stock_code=?", (stock_code,))
    existing_dates = set(r[0] for r in cur.fetchall())
    
    # 获取历史收盘价序列用于计算MA
    cur.execute("SELECT close FROM kline_daily WHERE stock_code=? ORDER BY trade_date ASC", (stock_code,))
    hist_closes = [float(r[0]) for r in cur.fetchall()]
    
    for row in sorted(rows, key=lambda x: x['trade_date']):
        td = row['trade_date']
        if td in existing_dates:
            print(f'    skip existing: {stock_code} {td}')
            continue
        
        hist_closes.append(row['close'])
        ma5 = compute_ma(hist_closes, 5)
        ma10 = compute_ma(hist_closes, 10)
        ma20 = compute_ma(hist_closes, 20)
        ma60 = compute_ma(hist_closes, 60)
        
        kline_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO kline_daily 
            (kline_id, stock_code, trade_date, open, high, low, close, volume, amount,
             adjust_type, ma5, ma10, ma20, ma60, is_suspended, source_batch_id, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            kline_id, stock_code, td,
            row['open'], row['high'], row['low'], row['close'],
            row['volume'], row['amount'],
            'qfq',
            ma5, ma10, ma20, ma60,
            0,
            batch_id,
            datetime.now(timezone.utc).isoformat()
        ))
        existing_dates.add(td)
        inserted += 1
    
    return inserted


async def fill_kline_for_stocks(stock_codes: list[str], start_date: str = '2026-04-17', end_date: str = '2026-04-24'):
    """批量填充股票K线数据"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    batch_id = 'kline_tencent_fill_' + datetime.now().strftime('%Y%m%d_%H%M%S')
    
    total_inserted = 0
    results = []
    
    for sc in stock_codes:
        print(f'Fetching kline for {sc}...')
        rows = await fetch_tencent_kline(sc, start_date, end_date, limit=30)
        if rows:
            print(f'  Got {len(rows)} rows: {[r["trade_date"] for r in rows]}')
            inserted = insert_kline_rows(conn, sc, rows, batch_id)
            print(f'  Inserted {inserted} new rows')
            total_inserted += inserted
            results.append({'stock_code': sc, 'fetched': len(rows), 'inserted': inserted})
        else:
            print(f'  No data returned')
            results.append({'stock_code': sc, 'fetched': 0, 'inserted': 0})
        await asyncio.sleep(0.2)  # Rate limit
    
    conn.commit()
    conn.close()
    print(f'\nTotal inserted: {total_inserted} kline rows')
    return results, total_inserted


async def main():
    # 目标：2026-04-16 pool的204只股票 + 当前pool的8只股票
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    
    # 获取需要填充的股票列表
    cur.execute("SELECT DISTINCT stock_code FROM stock_pool_snapshot WHERE trade_date='2026-04-16'")
    pool_0416 = [r[0] for r in cur.fetchall()]
    
    cur.execute("SELECT DISTINCT stock_code FROM stock_pool_snapshot WHERE trade_date='2026-04-24'")
    pool_0424 = [r[0] for r in cur.fetchall()]
    
    # 合并两个列表（去重）
    target_stocks = list(set(pool_0416 + pool_0424))
    target_stocks.sort()
    
    # 找出哪些股票在2026-04-17后有数据缺口
    gaps = []
    for sc in target_stocks:
        cur.execute("SELECT MAX(trade_date) FROM kline_daily WHERE stock_code=?", (sc,))
        row = cur.fetchone()
        latest = row[0] if row[0] else '2000-01-01'
        if latest < '2026-04-17':
            gaps.append(sc)
    
    conn.close()
    
    print(f'总共 {len(target_stocks)} 只股票，其中 {len(gaps)} 只需要补充 2026-04-17+ 的K线数据')
    print(f'需补充股票: {gaps[:20]}...' if len(gaps) > 20 else f'需补充股票: {gaps}')
    
    if not gaps:
        print('No gaps to fill!')
        return
    
    print('\n=== 开始填充K线数据 ===')
    results, total = await fill_kline_for_stocks(gaps, start_date='2026-04-01', end_date='2026-04-25')
    
    # 打印汇总
    success = [r for r in results if r['inserted'] > 0]
    failed = [r for r in results if r['inserted'] == 0]
    print(f'\n=== 完成 ===')
    print(f'成功填充: {len(success)} 只股票，共 {total} 条K线')
    print(f'无数据: {len(failed)} 只股票')
    if failed:
        print(f'无数据股票: {[r["stock_code"] for r in failed[:20]]}')

asyncio.run(main())
