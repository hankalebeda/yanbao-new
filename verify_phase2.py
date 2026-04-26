#!/usr/bin/env python3
"""
Phase 2 改进验证脚本：检查是否包含 missing_reasons 字段
"""

import os
import sys
import sqlite3
import json
from pathlib import Path

# 添加工作目录
sys.path.insert(0, str(Path(__file__).parent))

# 配置环境
os.environ['NO_PROXY'] = '*'

# 导入所需模块
from app.services.ssot_read_model import report_detail_to_api_response
from app.services.report_generation_ssot import get_report_by_stock_date

def test_missing_reasons_in_response():
    """验证 missing_reasons 字段是否在 capital_game_summary 中"""
    
    conn = sqlite3.connect("data/app.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 查询最新的报告
    cursor.execute("""
        SELECT report_id, stock_code, stock_name, content_json 
        FROM reports 
        WHERE quality_flag != 'excluded'
        ORDER BY created_at DESC 
        LIMIT 1
    """)
    
    row = cursor.fetchone()
    if not row:
        print("❌ 数据库中没有报告")
        return False
    
    report_id = row['report_id']
    stock_code = row['stock_code']
    
    print(f"📝 检查报告: {stock_code} (Report ID: {report_id})")
    
    # 获取原始报告数据
    report_dict = get_report_by_stock_date(stock_code=stock_code)
    if not report_dict:
        print("❌ 无法获取报告数据")
        return False
    
    # 转换为 API 响应格式
    try:
        api_response = report_detail_to_api_response(report_dict)
    except Exception as e:
        print(f"❌ 转换 API 响应失败: {e}")
        return False
    
    # 检查 capital_game_summary 结构
    capital_summary = api_response.get("data", {}).get("capital_game_summary", {})
    
    print("\n📊 Capital Game Summary 结构:")
    print(f"  - headline: {capital_summary.get('headline', 'N/A')[:50]}...")
    print(f"  - has_real_conclusion: {capital_summary.get('has_real_conclusion', 'N/A')}")
    print(f"  - completeness_level: {capital_summary.get('completeness_level', 'N/A')}")
    print(f"  - missing_dimensions: {capital_summary.get('missing_dimensions', [])}")
    
    # ✅ 关键检查：missing_reasons 是否存在
    missing_reasons = capital_summary.get('missing_reasons')
    if not missing_reasons:
        print("\n❌ PHASE 2 FAILED: missing_reasons 字段不存在")
        return False
    
    print(f"\n✅ missing_reasons 字段已找到！")
    print(f"\n📋 Missing Reasons 详细内容:")
    for dimension, info in missing_reasons.items():
        print(f"\n  {dimension}:")
        print(f"    reason: {info.get('reason', 'N/A')}")
        print(f"    remediation: {info.get('remediation', 'N/A')}")
        print(f"    status: {info.get('status', 'N/A')}")
    
    # 检查公司简介
    company_intro = api_response.get("data", {}).get("company_introduction", {})
    company_brief = company_intro.get("company_brief")
    
    print(f"\n📌 Company Introduction:")
    if company_brief:
        print(f"  ✅ company_brief: {company_brief[:80]}...")
    else:
        print(f"  ⚠️  company_brief: 仍然为空或缺失")
    
    print(f"\n  company_intro_text: {company_intro.get('company_intro_text', '')[:80]}...")
    
    # 综合判定
    success = bool(missing_reasons)
    print(f"\n{'='*60}")
    print(f"PHASE 2 验证结果: {'✅ PASSED' if success else '❌ FAILED'}")
    print(f"{'='*60}")
    
    conn.close()
    return success

if __name__ == "__main__":
    success = test_missing_reasons_in_response()
    sys.exit(0 if success else 1)
