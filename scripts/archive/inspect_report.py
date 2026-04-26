import sys, sqlite3, json
sys.stdout.reconfigure(encoding='utf-8')

conn = sqlite3.connect('data/app.db')
row = conn.execute(
    "SELECT id, content_json FROM reports WHERE stock_code='600519.SH' ORDER BY created_at DESC LIMIT 1"
).fetchone()

if not row:
    print("No report found for 600519.SH")
    sys.exit(1)

print("report_id:", row[0])
content = json.loads(row[1]) if row[1] else {}

pf = content.get('price_forecast', {})
print("price_forecast keys:", list(pf.keys()))

# --- backtest (technical) ---
bt = pf.get('backtest', {})
print("\n=== backtest.summary ===")
print(json.dumps(bt.get('summary'), ensure_ascii=False, indent=2))
print("\n=== backtest.summary_recent_3m ===")
print(json.dumps(bt.get('summary_recent_3m'), ensure_ascii=False, indent=2))
sm = bt.get('selected_model_by_horizon') or []
if sm:
    print("\n=== selected_model_by_horizon (first 3) ===")
    for x in sm[:3]:
        print(json.dumps(x, ensure_ascii=False))

# --- llm_backtest ---
print("\n=== llm_backtest ===")
print(json.dumps(pf.get('llm_backtest'), ensure_ascii=False, indent=2))

# --- direction_forecast ---
df = content.get('direction_forecast', {})
print("\n=== direction_forecast keys ===", list((df or {}).keys()))
horizons = (df or {}).get('horizons') or []
print("horizons count:", len(horizons))
for h in horizons[:3]:
    print(json.dumps(h, ensure_ascii=False))

# --- price_forecast windows ---
windows = pf.get('windows') or []
print("\n=== price_forecast.windows (first 2) ===")
for w in windows[:2]:
    print(json.dumps(w, ensure_ascii=False, indent=2)[:400])

# --- reasoning_trace ---
rt = content.get('reasoning_trace', {})
print("\n=== reasoning_trace keys ===", list((rt or {}).keys()))
print("inference_summary:", (rt or {}).get('inference_summary', '')[:200])
print("data_sources:", (rt or {}).get('data_sources'))
