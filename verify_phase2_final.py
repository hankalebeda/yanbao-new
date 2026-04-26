# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
Phase 2 改进验证脚本 - 检�?missing_reasons 字段是否存在和结构是否正�?
"""

import json
import sqlite3
import sys
from pathlib import Path

# 添加工作目录
sys.path.insert(0, str(Path(__file__).parent))

# 导入必要模块
from app.services.ssot_read_model import _load_ssot_report_bundle, _build_report_payload

def verify_missing_reasons():
    """验证 missing_reasons 字段"""
    
    # 连接数据�?
    conn = sqlite3.connect("data/app.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 获取最近的报告
    cursor.execute("""
        SELECT report_id, stock_code, stock_name
        FROM report
        WHERE quality_flag != 'excluded'
        ORDER BY created_at DESC
        LIMIT 1
    """)
    
    row = cursor.fetchone()
    if not row:
        print("�?数据库中没有报告")
        conn.close()
        return False
    
    report_id = row['report_id']
    stock_code = row['stock_code']
    
    print(f"📝 检查报�? {stock_code} (Report ID: {report_id})")
    
    # 加载报告 bundle
    try:
        bundle = _load_ssot_report_bundle(report_id)
        if not bundle:
            print("�?无法加载报告 bundle")
            conn.close()
            return False
    except Exception as e:
        print(f"�?加载 bundle 失败: {e}")
        conn.close()
        return False
    
    # 构建 API 响应 (为公众可�?
    try:
        payload = _build_report_payload(bundle, can_see_full=False, for_view=True)
    except Exception as e:
        print(f"�?构建 API 响应失败: {e}")
        conn.close()
        return False
    
    # 检�?capital_game_summary
    capital_summary = payload.get("capital_game_summary", {})
    
    print("\n📊 Capital Game Summary 字段检�?")
    print(f"  - headline: {'�? if capital_summary.get('headline') else '�?}")
    print(f"  - summary_text: {'�? if capital_summary.get('summary_text') else '�?}")
    print(f"  - has_real_conclusion: {'�? if 'has_real_conclusion' in capital_summary else '�?}")
    print(f"  - missing_dimensions: {'�? if capital_summary.get('missing_dimensions') is not None else '�?}")
    print(f"  - completeness_level: {'�? if capital_summary.get('completeness_level') else '�?}")
    
    # 🔑 关键检查：missing_reasons
    missing_reasons = capital_summary.get('missing_reasons')
    
    if not missing_reasons:
        print(f"  - missing_reasons: �?字段不存在或为空")
        print(f"\n⚠️  PHASE 2 实施不完整：missing_reasons 字段未出现在 API 响应�?)
        conn.close()
        return False
    
    print(f"  - missing_reasons: �?字段存在")
    
    # 验证 missing_reasons 结构
    print(f"\n📋 Missing Reasons 详细内容:")
    all_valid = True
    for dimension, info in missing_reasons.items():
        if not isinstance(info, dict):
            print(f"  �?{dimension}: 值类型错�?(不是 dict)")
            all_valid = False
            continue
        
        has_reason = 'reason' in info
        has_remediation = 'remediation' in info
        has_status = 'status' in info
        
        status_icon = "�? if (has_reason and has_remediation and has_status) else "⚠️"
        print(f"  {status_icon} {dimension}:")
        print(f"     reason: {'�? if has_reason else '�?} {info.get('reason', '')[:50]}...")
        print(f"     remediation: {'�? if has_remediation else '�?} {info.get('remediation', '')[:50]}...")
        print(f"     status: {'�? if has_status else '�?} {info.get('status', '')}")
        
        if not (has_reason and has_remediation and has_status):
            all_valid = False
    
    # 验证公司简�?
    print(f"\n📌 Company Introduction 检�?")
    company_intro = payload.get("company_introduction", {})
    company_brief = company_intro.get("company_brief")
    
    if company_brief:
        print(f"  �?company_brief: {company_brief[:80]}...")
    else:
        print(f"  ⚠️  company_brief: 仍为空或缺失")
    
    intro_text = company_intro.get("company_intro_text", "")
    if intro_text:
        print(f"  �?company_intro_text: {intro_text[:80]}...")
    else:
        print(f"  ⚠️  company_intro_text: 为空")
    
    # 综合判定
    print(f"\n{'='*70}")
    success = bool(missing_reasons) and all_valid
    print(f"PHASE 2 验证结果: {'�?PASSED - missing_reasons 已正确实�? if success else '⚠️  PARTIAL - missing_reasons 存在但结构可能不完整'}")
    print(f"{'='*70}")
    
    conn.close()
    return success

if __name__ == "__main__":
    success = verify_missing_reasons()
    sys.exit(0 if success else 1)