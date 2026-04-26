"""
Step 1: 用腾讯实时行情 API 获取流通市值，补充 stock_master.circulating_shares
然后清理 bad task entries，重建 stock_pool_snapshot
"""
import sqlite3
import httpx
import time
import sys

DB_PATH = 'data/app.db'

def tencent_sym(stock_code: str) -> str:
    code, exchange = stock_code.split('.')
    if exchange == 'SH':
        return f'sh{code}'
    else:
        return f'sz{code}'

def fetch_rt_batch(sym_list: list[str]) -> dict[str, dict]:
    """Fetch real-time quote from Tencent, extract 流通市值"""
    query = ','.join(sym_list)
    url = f'https://qt.gtimg.cn/q={query}'
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.qq.com'}
    with httpx.Client(timeout=15, trust_env=False) as c:
        r = c.get(url, headers=headers)
    result = {}
    for line in r.text.split('\n'):
        line = line.strip()
        if not line or '~' not in line:
            continue
        # v_sh600519="1~贵州茅台~600519~1419~..."
        try:
            key_part, val_part = line.split('=', 1)
            sym = key_part.strip().replace('v_', '')
            val = val_part.strip().strip('"').strip(';')
            fields = val.split('~')
            if len(fields) < 45:
                continue
            close_price = float(fields[3]) if fields[3] else 0
            # field 38 is 流通市值 in 亿元 (some sources) or 万元?
            # Let's print all known market cap fields
            circ_mktcap_wan = float(fields[44]) if len(fields) > 44 and fields[44] else 0  # field 44: 流通市值
            total_mktcap_wan = float(fields[45]) if len(fields) > 45 and fields[45] else 0  # field 45: 总市值
            result[sym] = {
                'close': close_price,
                'circ_mktcap_wan': circ_mktcap_wan,
                'total_mktcap_wan': total_mktcap_wan,
                'fields_sample': fields[38:50],
            }
        except Exception as e:
            pass
    return result

# Test with a few stocks
test_stocks = ['sh600519', 'sz000001', 'sz300750', 'sh688008']
print("Testing Tencent RT API for field discovery...")
data = fetch_rt_batch(test_stocks)
for sym, info in data.items():
    if info['close'] > 0:
        circ_shares_calc = info['circ_mktcap_wan'] * 10000 / info['close'] if info['close'] > 0 else 0
        print(f"{sym}: close={info['close']}, circ_mktcap_wan={info['circ_mktcap_wan']}, "
              f"total_mktcap_wan={info['total_mktcap_wan']}, "
              f"calc_shares={circ_shares_calc:.0f}")
    print(f"  fields[38:50]={info['fields_sample']}")
