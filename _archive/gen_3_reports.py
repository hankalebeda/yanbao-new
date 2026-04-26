"""
生成 3 只股票的真实研报，用于展示 BUY/HOLD/SELL 三种类型。
股票：000858.SZ (五粮液)，002594.SZ (比亚迪)，600519.SH (贵州茅台)
日期：2026-04-16
"""
import sys, json, logging
sys.path.insert(0, 'd:/yanbao-new')
logging.basicConfig(level=logging.WARNING)

from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_report_ssot

stocks = [
    ('000858.SZ', '五粮液'),
    ('002594.SZ', '比亚迪'),
    ('600519.SH', '贵州茅台'),
]
trade_date = '2026-04-16'
results = {}

for sc, name in stocks:
    print(f'\n{"="*60}')
    print(f'生成研报: {sc} {name} ({trade_date})')
    print('='*60)
    db = SessionLocal()
    try:
        result = generate_report_ssot(
            db,
            stock_code=sc,
            trade_date=trade_date,
            skip_pool_check=True,   # 这3只已确认在pool snapshot中
            force_same_day_rebuild=True,
        )
        results[sc] = result
        rec = result.get('recommendation', 'N/A')
        conf = result.get('confidence', 0)
        llm_level = result.get('llm_fallback_level', 'N/A')
        model = result.get('llm_actual_model', 'N/A')
        published = result.get('publish_status', 'N/A')
        print(f'✓ 推荐: {rec}  置信度: {conf:.0%}  LLM: {llm_level}  模型: {model}')
        print(f'  发布状态: {published}')
        print(f'  结论: {result.get("conclusion_text", "")[:200]}')
    except Exception as e:
        results[sc] = {'error': str(e)}
        print(f'✗ 错误: {e}')
    finally:
        db.close()

print('\n\n' + '='*60)
print('=== 完整研报内容 ===')
print('='*60)
for sc, name in stocks:
    r = results.get(sc, {})
    if 'error' in r:
        print(f'\n{sc} {name}: ERROR - {r["error"]}')
        continue
    print(f'\n{"─"*60}')
    print(f'【{r.get("recommendation","?")}】{sc} {name}  置信度={r.get("confidence",0):.0%}')
    print(f'交易日: {r.get("trade_date")}  发布: {r.get("publish_status")}  LLM: {r.get("llm_actual_model","规则")}')
    print(f'\n--- 分析结论 ---')
    print(r.get('conclusion_text', ''))
    print(f'\n--- 推理链 ---')
    print(r.get('reasoning_chain_md', '')[:800])
    cj = r.get('content_json') or {}
    if isinstance(cj, str):
        try: cj = json.loads(cj)
        except: cj = {}
    if cj:
        print(f'\n--- 方向预测 ---')
        df = cj.get('direction_forecast', {})
        for h in df.get('horizons', []):
            print(f'  {h.get("horizon_day")}日: {h.get("direction")} ({h.get("action")})')
        print(f'\n--- 价格预测 ---')
        for w in (cj.get('price_forecast') or {}).get('windows', []):
            print(f'  {w.get("horizon_days")}日: 中枢={w.get("central_price")} 止盈={w.get("target_high")} 止损={w.get("target_low")}')
        print(f'\n--- 证据维度 ---')
        for ep in cj.get('evidence_backing_points', []):
            badge_icon = {'up': '↑', 'down': '↓', 'warn': '⚠', 'flat': '─'}.get(ep.get('badge_type'), '')
            print(f'  {badge_icon} {ep.get("title")}: {ep.get("badge")} | {" / ".join(ep.get("nums",[])[:2])}')
