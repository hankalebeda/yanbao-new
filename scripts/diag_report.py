"""第0轮诊断脚本：分析最新研报结构与准确率"""
import sys
import os
import json

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root)
os.chdir(root)

from app.core.db import SessionLocal, Base, engine, ensure_report_trade_date_column

Base.metadata.create_all(bind=engine)
ensure_report_trade_date_column()
db = SessionLocal()

from app.models import Report

row = db.query(Report).filter(Report.stock_code == "600519.SH").order_by(Report.created_at.desc()).first()
db.close()

if not row:
    print("No report found for 600519.SH")
    sys.exit(1)

d = row.content_json
print(f"Report ID: {row.report_id}")
print(f"Created: {row.created_at}")
print(f"Trade date: {row.trade_date}")
print(f"Recommendation: {d.get('recommendation')} confidence={d.get('confidence')}")
print(f"\n=== TOP-LEVEL KEYS ===")
print(list(d.keys()))

# price_forecast windows
pf = d.get("price_forecast") or {}
wins = pf.get("windows") or []
print(f"\n=== price_forecast.windows count={len(wins)} ===")
for w in wins:
    print(f"  horizon={w.get('horizon_days')} price={w.get('predicted_price')} ret={w.get('predicted_return')} "
          f"conf_raw={w.get('confidence_raw')} conf={w.get('confidence')} conf_gap={w.get('confidence_gap')}")

# price_forecast backtest
bt = pf.get("backtest") or {}
print(f"\n=== price_forecast.backtest.horizons ===")
for h in bt.get("horizons") or []:
    print(f"  day={h.get('horizon_days')} acc={h.get('accuracy')} samples={h.get('samples')} cov={h.get('coverage')}")

print(f"\n=== price_forecast.backtest.horizons_recent_3m ===")
for h in bt.get("horizons_recent_3m") or []:
    print(f"  day={h.get('horizon_days')} acc={h.get('accuracy')} samples={h.get('samples')} cov={h.get('coverage')}")

s3m = bt.get("summary_recent_3m") or {}
print(f"summary_recent_3m: acc={s3m.get('overall_accuracy')} samples={s3m.get('samples')} "
      f"start={s3m.get('start_date')} end={s3m.get('end_date')}")

rd = pf.get("readiness") or {}
print(f"\n=== readiness ===")
print(f"  score={rd.get('score')} ready={rd.get('ready_for_use')}")
print(f"  reasons={rd.get('reasons')}")

# direction_forecast
df = d.get("direction_forecast") or {}
horizons = df.get("horizons") or []
print(f"\n=== direction_forecast.horizons count={len(horizons)} ===")
for h in horizons:
    print(f"  day={h.get('horizon_day')} dir={h.get('direction')} action={h.get('action')} conf={h.get('confidence')}")

bt3m = df.get("backtest_recent_3m") or []
print(f"\n=== direction_forecast.backtest_recent_3m count={len(bt3m)} ===")
for b in bt3m:
    print(f"  day={b.get('horizon_day')} acc={b.get('actionable_accuracy')} "
          f"samples={b.get('actionable_samples')} cov={b.get('actionable_coverage')}")

target = df.get("target") or {}
print(f"target: acc={target.get('target_accuracy')} min_samples={target.get('min_actionable_samples')} "
      f"min_cov={target.get('min_actionable_coverage')}")

# quality_gate
qg = d.get("quality_gate") or {}
print(f"\n=== quality_gate ===")
print(f"  publish={qg.get('publish_decision')} score={qg.get('coverage_score')}")
print(f"  missing={qg.get('missing_fields')}")
print(f"  recover={qg.get('recover_actions')}")

# plain_report accuracy_explain
pr = d.get("plain_report") or {}
ae = pr.get("accuracy_explain") or {}
print(f"\n=== plain_report.accuracy_explain ===")
print(f"  headline: {ae.get('headline')}")
cm = ae.get("current_metrics") or {}
print(f"  7d_acc={cm.get('horizon_7d_actionable_accuracy')} samples={cm.get('horizon_7d_samples')} cov={cm.get('horizon_7d_coverage')}")
print(f"  3m_acc={cm.get('backtest_overall_3m_accuracy')} 3m_samples={cm.get('backtest_overall_3m_samples')}")
print(f"  readiness_score={cm.get('readiness_score')} ready={cm.get('ready_for_use')}")

# citations
cits = d.get("citations") or []
print(f"\n=== citations count={len(cits)} ===")
for c in cits[:5]:
    print(f"  [{c.get('source_name')}] {c.get('source_url','')[:80]}")

# report_data_usage
rdu = d.get("report_data_usage") or {}
sources = rdu.get("sources") or []
print(f"\n=== report_data_usage.sources count={len(sources)} ===")
for s in sources:
    print(f"  {s.get('name')} | status={s.get('status')} | records={s.get('record_count')}")

# reasoning_trace check
rt = d.get("reasoning_trace") or {}
print(f"\n=== reasoning_trace ===")
print(f"  data_sources count={len(rt.get('data_sources') or [])}")
print(f"  evidence_items count={len(rt.get('evidence_items') or [])}")
print(f"  analysis_steps count={len(rt.get('analysis_steps') or [])}")
print(f"  inference_summary len={len(rt.get('inference_summary') or '')}")

# novice_guide
ng = d.get("novice_guide") or {}
print(f"\n=== novice_guide ===")
print(f"  one_line={ng.get('one_line_decision')}")
print(f"  risk_level={ng.get('risk_level')}")
print(f"  why_points count={len(ng.get('why_points') or [])}")
