"""
补充所有 stock_master active 股票的 circulating_shares
使用腾讯 RT API 批量获取 流通市值，计算 circulating_shares
"""
import sqlite3, httpx, time, sys, os
sys.stdout.reconfigure(line_buffering=True)

DB_PATH = 'data/app.db'

def tencent_sym(code: str, exch: str) -> str:
    if exch == 'SH': return f'sh{code}'
    if exch == 'SZ': return f'sz{code}'
    return f'bj{code}'

def fetch_rt_batch(syms: list[str]) -> dict:
    """Returns {sym: circulating_shares}"""
    url = f'https://qt.gtimg.cn/q={",".join(syms)}'
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.qq.com'}
    with httpx.Client(timeout=20, trust_env=False) as c:
        r = c.get(url, headers=headers)
    result = {}
    for line in r.text.split('\n'):
        line = line.strip()
        if not line or '~' not in line:
            continue
        try:
            key_part, val_part = line.split('=', 1)
            sym = key_part.strip().replace('v_', '')
            val = val_part.strip().strip('"').strip(';')
            fields = val.split('~')
            if len(fields) < 45:
                continue
            close_p = float(fields[3]) if fields[3] else 0
            circ_mktcap_b = float(fields[44]) if len(fields) > 44 and fields[44] else 0
            if close_p > 0 and circ_mktcap_b > 0:
                circ_shares = circ_mktcap_b * 1e8 / close_p
                result[sym] = int(circ_shares)
        except:
            pass
    return result

# Load all active stocks needing circulating_shares
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("""SELECT stock_code FROM stock_master 
    WHERE (is_delisted=0 OR is_delisted IS NULL)
    ORDER BY stock_code""")
all_stocks = [r[0] for r in cur.fetchall()]

cur.execute("SELECT COUNT(*) FROM stock_master WHERE circulating_shares IS NOT NULL AND circulating_shares > 0")
existing_count = cur.fetchone()[0]
conn.close()
print(f'Active stocks: {len(all_stocks)}, already have circulating_shares: {existing_count}', flush=True)

# Build sym list
sym_map = {}  # sym -> stock_code
for sc in all_stocks:
    parts = sc.split('.')
    if len(parts) != 2: continue
    code, exch = parts
    sym = tencent_sym(code, exch)
    sym_map[sym] = sc

all_syms = list(sym_map.keys())
print(f'Total syms to fetch: {len(all_syms)}', flush=True)

# Fetch in batches of 100
results = {}  # stock_code -> circulating_shares
for i in range(0, len(all_syms), 100):
    batch = all_syms[i:i+100]
    try:
        fetched = fetch_rt_batch(batch)
        for sym, shares in fetched.items():
            sc = sym_map.get(sym)
            if sc:
                results[sc] = shares
    except Exception as e:
        print(f'Batch {i//100+1} failed: {e}', flush=True)
    
    if (i//100+1) % 10 == 0:
        print(f'  Fetched {i+len(batch)}/{len(all_syms)}, got {len(results)} results', flush=True)
    
    if i + 100 < len(all_syms):
        time.sleep(0.3)

print(f'\nTotal circulating_shares fetched: {len(results)}', flush=True)

# Update stock_master
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
updated = 0
for sc, shares in results.items():
    if shares > 0:
        cur.execute("""UPDATE stock_master SET circulating_shares=?
            WHERE stock_code=? AND (circulating_shares IS NULL OR circulating_shares=0)""",
            (shares, sc))
        if cur.rowcount > 0:
            updated += 1

# Also update even if already set (fresh data)
forced = 0
for sc, shares in results.items():
    if shares > 0:
        cur.execute("""UPDATE stock_master SET circulating_shares=?
            WHERE stock_code=? AND (circulating_shares IS NULL OR circulating_shares=0 OR abs(circulating_shares - ?) / ? > 0.2)""",
            (shares, sc, shares, shares))
        if cur.rowcount > 0:
            forced += 1

conn.commit()

cur.execute("SELECT COUNT(*) FROM stock_master WHERE circulating_shares IS NOT NULL AND circulating_shares > 0")
total_ok = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM stock_master WHERE circulating_shares IS NULL OR circulating_shares = 0")
total_null = cur.fetchone()[0]
conn.close()

print(f'Updated: {updated} new + {forced} forced updates', flush=True)
print(f'After update: {total_ok} have circulating_shares, {total_null} still NULL', flush=True)
