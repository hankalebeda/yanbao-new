import os, json
cache_dir = 'data/capital_cache'
files = sorted(os.listdir(cache_dir))
print(f'Total capital_cache files: {len(files)}')

# Check one file in detail
path = os.path.join(cache_dir, '600519_SH.json')
if not os.path.exists(path):
    path = os.path.join(cache_dir, files[0])

with open(path, encoding='utf-8') as fp:
    data = json.load(fp)

print(f'\nFile: {os.path.basename(path)}')
print(f'updated_at: {data.get("updated_at")}')
rows = data.get('capital_flow_rows', [])
print(f'capital_flow_rows count: {len(rows)}')
if rows:
    # Show last 3 rows (most recent)
    for r in rows[-3:]:
        print(f'  {r}')
    # Check what fields exist
    if rows:
        print(f'\nSample row keys: {list(rows[-1].keys()) if isinstance(rows[-1], dict) else "not dict"}')

# Check all files for date coverage
print('\n=== Date coverage of capital_flow_rows ===')
for fname in files[:10]:
    fpath = os.path.join(cache_dir, fname)
    try:
        with open(fpath, encoding='utf-8') as fp:
            d = json.load(fp)
        rows = d.get('capital_flow_rows', [])
        if rows:
            dates = sorted(set(r.get('trade_date', r.get('date', '?')) for r in rows if isinstance(r, dict)))
            last_date = dates[-1] if dates else '?'
        else:
            last_date = '(empty)'
        print(f'  {fname}: last_date={last_date}, rows={len(rows)}')
    except Exception as e:
        print(f'  {fname}: ERROR {e}')

