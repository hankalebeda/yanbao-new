"""并行生成研报 - 跳过已完成的2026-04-07, 并行8个日期的批次"""
import sqlite3, json, asyncio
import httpx

with open('_archive/audit_v24_phase1_evidence/ready_stocks.json') as f:
    ready = json.load(f)

API_BASE = 'http://localhost:8000'
TOKEN = 'phase1-audit-token-20260417'

# Skip 04-07 which already has 197 reports
TARGETS = ['2026-04-08','2026-04-09','2026-04-10','2026-04-13',
           '2026-04-14','2026-04-15','2026-04-16']

# Check which stocks need generation (not already have reports)
c = sqlite3.connect('data/app.db')
cur = c.cursor()

pending = {}  # date -> stocks needing gen
for td in TARGETS:
    stocks = ready.get(td, [])
    existing = set(r[0] for r in cur.execute(
        "SELECT stock_code FROM report WHERE trade_date=? AND is_deleted=0", (td,)).fetchall())
    pending_stocks = [s for s in stocks if s not in existing]
    pending[td] = pending_stocks
    print(f"{td}: {len(pending_stocks)} pending (existing={len(existing)})")
c.close()

# Parallel generation with concurrency
async def generate_batch(client, td, chunk, batch_idx):
    try:
        resp = await client.post(
            f'{API_BASE}/api/v1/internal/reports/generate-batch',
            headers={'X-Internal-Token': TOKEN, 'Content-Type': 'application/json'},
            json={'stock_codes': chunk, 'trade_date': td, 'force': False, 'skip_pool_check': True},
            timeout=600
        )
        if resp.status_code in (200, 202):
            data = resp.json().get('data', {})
            ok = data.get('succeeded', 0)
            fail = data.get('failed', 0)
            return (td, batch_idx, ok, fail, None)
        else:
            return (td, batch_idx, 0, len(chunk), f"HTTP {resp.status_code}")
    except Exception as e:
        return (td, batch_idx, 0, len(chunk), str(e))


async def main():
    # Build task list
    tasks_info = []
    for td in TARGETS:
        stocks = pending[td]
        if not stocks: continue
        chunks = [stocks[i:i+50] for i in range(0, len(stocks), 50)]
        for idx, chunk in enumerate(chunks):
            tasks_info.append((td, chunk, idx+1))
    
    print(f"\n总共 {len(tasks_info)} 批次需生成")
    
    # Run with limited concurrency
    semaphore = asyncio.Semaphore(3)  # 3 concurrent batches
    
    async def bounded_task(client, td, chunk, idx):
        async with semaphore:
            print(f"  [START] {td} batch{idx} ({len(chunk)} stocks)")
            result = await generate_batch(client, td, chunk, idx)
            td_r, idx_r, ok, fail, err = result
            print(f"  [DONE] {td_r} batch{idx_r}: ok={ok} fail={fail}" + (f" err={err}" if err else ""))
            return result
    
    async with httpx.AsyncClient(timeout=900, trust_env=False) as client:
        results = await asyncio.gather(*[bounded_task(client, td, chunk, idx) for td, chunk, idx in tasks_info])
    
    # Aggregate
    by_date = {}
    for td, idx, ok, fail, err in results:
        by_date.setdefault(td, {'ok':0, 'fail':0})
        by_date[td]['ok'] += ok
        by_date[td]['fail'] += fail
    
    print("\n=== 汇总 ===")
    for td in sorted(by_date):
        print(f"  {td}: ok={by_date[td]['ok']} fail={by_date[td]['fail']}")
    print(f"  Total: ok={sum(v['ok'] for v in by_date.values())} fail={sum(v['fail'] for v in by_date.values())}")


if __name__ == '__main__':
    asyncio.run(main())
