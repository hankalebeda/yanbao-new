import sys, json
from pathlib import Path
_root = Path(__file__).resolve().parent.parent.parent  # scripts/archive -> project root
sys.path.insert(0, str(_root))
from app.services.tdx_local_data import load_tdx_day_records
from app.services.report_engine import _backtest_forecast_model

records = load_tdx_day_records('600519.SH', limit=1000)
tdx_local = {'series': records}

result = _backtest_forecast_model(tdx_local, stock_code='600519.SH')

print('=== quanls summary ===')
s = result['summary']
print(f"  overall_accuracy: {s.get('overall_accuracy')}")
print(f"  samples: {s.get('samples')}")

print('=== jin3yue summary ===')
s3 = result['summary_recent_3m']
print(f"  overall_accuracy: {s3.get('overall_accuracy')}")
print(f"  samples: {s3.get('samples')}")

print()
print('=== ge chuangkou quan lishi ===')
for h in result['horizons']:
    print(f"  {h['horizon_days']}ri: acc={h['accuracy']}, samples={h['samples']}, model={h['model_name']}")

print()
print('=== ge chuangkou jin3yue ===')
for h in result['horizons_recent_3m']:
    print(f"  {h['horizon_days']}ri: acc={h['accuracy']}, samples={h['samples']}, model={h['model_name']}")

print()
print('=== houbu moxing zuigao acc (jin3yue) ===')
best = []
for m in result.get('model_candidates', []):
    s3m = m.get('summary_recent_3m', {})
    best.append((m['model_name'], s3m.get('overall_accuracy'), s3m.get('samples', 0)))
best.sort(key=lambda x: x[1] or 0, reverse=True)
for name, acc, samp in best[:12]:
    print(f"  {name}: acc={acc}, samples={samp}")

print()
print('=== per horizon per model (jin3yue) ===')
for m in result.get('model_candidates', []):
    print(f"Model: {m['model_name']}")
    for h in m.get('horizons_recent_3m', []):
        print(f"  h={h['horizon_days']}: acc={h.get('accuracy')}, samples={h.get('samples')}")
