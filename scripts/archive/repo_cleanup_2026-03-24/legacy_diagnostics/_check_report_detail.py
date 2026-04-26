#!/usr/bin/env python3
"""检查研报详情页HTML渲染内容"""
import httpx, sys
sys.stdout.reconfigure(encoding='utf-8')

BASE = "http://127.0.0.1:8099"

with httpx.Client(follow_redirects=True, timeout=15) as c:
    # 设置cookie登录
    r = c.post(BASE + "/auth/login", json={"email": "admin@example.com", "password": "Qwer1234.."})
    token = r.json()["data"]["access_token"]
    hdrs = {"Authorization": f"Bearer {token}"}
    
    # 设置cookie session
    c.post(BASE + "/api/v1/admin/cookie-session", headers=hdrs)
    
    # 获取Pro用户
    rp = c.post(BASE + "/auth/login", json={"email": "v79_pro@test.com", "password": "TestPro123!"})
    pro_token = rp.json()["data"]["access_token"]
    pro_hdrs = {"Authorization": f"Bearer {pro_token}"}
    
    # 获取研报列表
    r2 = c.get(BASE + "/api/v1/reports?page=1", headers=pro_hdrs)
    rpt_id = r2.json()["data"]["items"][0]["report_id"]
    rpt_code = r2.json()["data"]["items"][0]["stock_code"]
    print(f"Testing report: {rpt_id} ({rpt_code})")
    
    # 获取HTML研报详情页
    r3 = c.get(f"{BASE}/reports/{rpt_id}", headers=pro_hdrs)
    html = r3.text
    print(f"HTML page status: {r3.status_code}, len={len(html)}")
    
    # 检查关键内容
    checks = {
        "stock_name显示": rpt_code in html or "德业股份" in html or "股份" in html,
        "analysis_steps/推理步骤": ("数据解读" in html or "价格行为" in html or "分析步骤" in html),
        "BUY/SELL/HOLD": any(x in html for x in ["买入", "卖出", "观望", "BUY", "SELL", "HOLD"]),
        "citations展示": ("来源" in html or "source" in html.lower() or "引用" in html),
        "止损/目标价": ("止损" in html or "目标价" in html or "target" in html.lower()),
        "degraded_banner": ("降级" in html or "数据略旧" in html or "T-1" in html or "stale" in html.lower()),
        "trading_date": "2026-03" in html,
        "strategy_type": any(x in html for x in ["策略A", "策略B", "策略C", "strategy"]),
        "confidence": "置信度" in html or "confidence" in html.lower(),
    }
    
    print("\n研报详情页内容检查:")
    for k, v in checks.items():
        print(f"  {'[OK]' if v else '[MISSING]'} {k}")
    
    # 检查是否有原始内部字段暴露给用户
    raw_fields = ["stale_ok", "fallback_t_minus_1", "tdx_local", "kline_daily:"]
    print("\n内部字段暴露检查:")
    for f in raw_fields:
        if f in html:
            # 找到出现位置
            idx = html.find(f)
            context = html[max(0,idx-50):idx+100]
            print(f"  [WARNING] '{f}' 出现在HTML: ...{context.strip()[:100]}...")
        else:
            print(f"  [OK] '{f}' 未暴露")
    
    # 检查 analysis_steps 实际内容
    # 找 <li style="line-height:1.7"> 
    import re
    steps = re.findall(r'<li style="line-height:1\.7">(.*?)</li>', html, re.DOTALL)
    print(f"\n分析步骤数量: {len(steps)}")
    for s in steps[:3]:
        print(f"  - {s[:80]}")
    
    # 报告的JSON API响应
    r4 = c.get(f"{BASE}/api/v1/reports/{rpt_id}", headers=pro_hdrs)
    api_data = r4.json()["data"]
    print(f"\nJSON API fields: {list(api_data.keys())}")
    print(f"stock_name in API: {'stock_name' in api_data}")
    print(f"analysis_steps in API: {'analysis_steps' in api_data}")
    print(f"conclusion_text preview: {(api_data.get('conclusion_text') or '')[:100]}")
