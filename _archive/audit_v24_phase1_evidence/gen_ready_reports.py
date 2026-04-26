"""Get list of stocks with full ok coverage for each date - ready for report generation"""
import sqlite3, json, requests

c = sqlite3.connect('data/app.db')
cur = c.cursor()

with open('_archive/audit_v24_phase1_evidence/core_pool.json') as f:
    pool = json.load(f)['core_stocks']
pool_str = ','.join([f"'{s}'" for s in pool])

target_dates = ['2026-04-07','2026-04-08','2026-04-09','2026-04-10',
                '2026-04-13','2026-04-14','2026-04-15','2026-04-16']

REQUIRED = {'kline_daily', 'northbound_summary', 'etf_flow_summary', 'hotspot_top50'}

ready_stocks_per_date = {}

for td in target_dates:
    # Get stocks that have ok records for all 4 required datasets (market_state_input auto-created)
    cur.execute(f"""
    SELECT stock_code, count(DISTINCT dataset_name) as ds_count
    FROM report_data_usage 
    WHERE trade_date='{td}' AND status='ok' AND dataset_name IN ('kline_daily','northbound_summary','etf_flow_summary','hotspot_top50')
    AND stock_code IN ({pool_str})
    GROUP BY stock_code
    HAVING ds_count = 4
    """)
    ready_stocks = [r[0] for r in cur.fetchall()]
    ready_stocks_per_date[td] = ready_stocks
    print(f"{td}: {len(ready_stocks)} stocks ready for report gen")

# Save for use in generation
with open('_archive/audit_v24_phase1_evidence/ready_stocks.json', 'w') as f:
    json.dump(ready_stocks_per_date, f, indent=2)
print('\nSaved to ready_stocks.json')

# Generate reports for dates with ready stocks
API_BASE = 'http://localhost:8000'
TOKEN = 'phase1-audit-token-20260417'

total_ok = 0
total_fail = 0

for td, stocks in ready_stocks_per_date.items():
    if not stocks:
        continue
    print(f'\nGenerating reports for {td} ({len(stocks)} stocks)...')
    
    # Split into batches of 50
    chunks = [stocks[i:i+50] for i in range(0, len(stocks), 50)]
    for i, chunk in enumerate(chunks):
        try:
            resp = requests.post(
                f'{API_BASE}/api/v1/internal/reports/generate-batch',
                headers={'X-Internal-Token': TOKEN, 'Content-Type': 'application/json'},
                json={'stock_codes': chunk, 'trade_date': td, 'force': True, 'skip_pool_check': True},
                timeout=600
            )
            if resp.status_code == 200:
                data = resp.json()
                ok = data.get('success_count', 0)
                fail = data.get('failed_count', 0)
                total_ok += ok
                total_fail += fail
                print(f'  Batch{i+1}: ok={ok}, fail={fail}')
            else:
                print(f'  Batch{i+1}: HTTP {resp.status_code}: {resp.text[:100]}')
                total_fail += len(chunk)
        except Exception as e:
            print(f'  Batch{i+1}: ERROR: {e}')
            total_fail += len(chunk)

print(f'\nTotal: ok={total_ok}, fail={total_fail}')
