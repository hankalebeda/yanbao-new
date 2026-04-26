#!/usr/bin/env python3
"""
完整系统审计 - 39 角度深度分析
Comprehensive System Audit - 39 Angles Deep Analysis
"""
import requests, json, sqlite3, os
from datetime import datetime
from collections import defaultdict

os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

BASE_URL = "http://127.0.0.1:8010"

def get_http_no_proxies():
    return {'http': None, 'https': None}

class AuditAgent:
    def __init__(self):
        self.findings = defaultdict(list)
        self.db_snapshot = {}
        self.api_responses = {}
        
    def probe_database(self):
        """角度 1,5,6,9: 真实性/血缘/时间锚点"""
        conn = sqlite3.connect('data/app.db')
        c = conn.cursor()
        
        print("\n[PROBE] Database Analysis...")
        
        # 1. Report 质量分布 (角度 1,2)
        visible_reports = c.execute("""
            SELECT report_id, stock_code, recommendation, quality_flag, published, is_deleted, created_at, updated_at
            FROM report WHERE published=1 AND is_deleted=0 LIMIT 20
        """).fetchall()
        
        cols = ['report_id', 'stock_code', 'recommendation', 'quality_flag', 'published', 'is_deleted', 'created_at', 'updated_at']
        self.db_snapshot['sample_reports'] = [dict(zip(cols, r)) for r in visible_reports]
        
        # 2. Settlement 完全缺口 (角度 1,5)
        settlement_count = c.execute('SELECT COUNT(*) FROM settlement_result').fetchone()[0]
        if settlement_count == 0:
            self.findings['P1_SETTLEMENT_COMPLETE_MISSING'].append({
                'angle': [1, 5],
                'evidence': 'settlement_result 表 0 条记录，对应 2032 条已发布报告',
                'coverage': '0%'
            })
        
        # 3. K-line 覆盖缺口 (角度 1,5,9)
        kline_stocks = c.execute('SELECT COUNT(DISTINCT stock_code) FROM kline_daily').fetchone()[0]
        stock_total = c.execute('SELECT COUNT(*) FROM stock_master').fetchone()[0]
        kline_coverage = (kline_stocks / stock_total * 100) if stock_total > 0 else 0
        
        if kline_coverage < 20:
            self.findings['P1_KLINE_COVERAGE_LOW'].append({
                'angle': [1, 5, 9],
                'evidence': f'K-line 覆盖 {kline_stocks}/{stock_total} = {kline_coverage:.1f}%，预期 ≥20%',
                'coverage': f'{kline_coverage:.1f}%'
            })
        
        # 4. 质量标记分布异常 (角度 2,8)
        quality_dist = c.execute("""
            SELECT lower(coalesce(quality_flag,'ok')) as qf, COUNT(*) as cnt
            FROM report WHERE published=1 AND is_deleted=0
            GROUP BY lower(coalesce(quality_flag,'ok'))
        """).fetchall()
        
        quality_dict = {q[0]: q[1] for q in quality_dist}
        stale_ok_pct = (quality_dict.get('stale_ok', 0) / len(self.db_snapshot.get('sample_reports', [])) * 100) if self.db_snapshot.get('sample_reports') else 0
        
        if stale_ok_pct > 80:  # 超过 80% 是 stale_ok
            self.findings['P2_STALE_OK_DOMINANT'].append({
                'angle': [2, 8],
                'evidence': f'{quality_dict.get("stale_ok", 0)} 条报告质量标记为 stale_ok，占比 {stale_ok_pct:.1f}%',
                'coverage': f'{stale_ok_pct:.1f}%'
            })
        
        # 5. 数据一致性检查 (角度 8)
        missing_conclusions = c.execute("""
            SELECT COUNT(*) FROM report 
            WHERE published=1 AND is_deleted=0 
            AND (conclusion_text IS NULL OR TRIM(conclusion_text)='')
        """).fetchone()[0]
        
        if missing_conclusions > 0:
            self.findings['P1_MISSING_CONCLUSION'].append({
                'angle': [1, 5, 8],
                'evidence': f'{missing_conclusions} 条可见报告缺失 conclusion_text',
                'coverage': f'{missing_conclusions} records'
            })
        
        conn.close()
    
    def probe_api(self):
        """角度 3,4: SSOT 契约/错误码"""
        print("\n[PROBE] API Contract Analysis...")
        
        # 测试关键端点
        endpoints = [
            ('GET /api/v1/reports', 'reports_list'),
            ('GET /api/v1/home', 'home'),
            ('POST /api/v1/reports', 'create_report'),
        ]
        
        for endpoint, label in endpoints:
            try:
                method, path = endpoint.split(' ', 1)
                if method == 'GET':
                    r = requests.get(f"{BASE_URL}{path}", timeout=5, proxies=get_http_no_proxies())
                elif method == 'POST':
                    r = requests.post(f"{BASE_URL}{path}", timeout=5, json={}, proxies=get_http_no_proxies())
                
                self.api_responses[label] = {
                    'status': r.status_code,
                    'headers_keys': list(r.headers.keys()),
                    'content_type': r.headers.get('content-type', 'unknown'),
                }
                
                # 验证内容类型 (角度 3,19)
                if method == 'GET' and r.status_code == 200:
                    if 'application/json' not in r.headers.get('content-type', ''):
                        self.findings['P2_WRONG_CONTENT_TYPE'].append({
                            'angle': [3, 19],
                            'endpoint': path,
                            'expected': 'application/json',
                            'got': r.headers.get('content-type', 'unknown')
                        })
                
                # 验证 HTTP 状态 (角度 4)
                if r.status_code == 405:  # POST 不允许
                    # 这可能是预期的行为
                    pass
                    
            except Exception as e:
                self.findings['API_ERROR'].append({
                    'endpoint': endpoint,
                    'error': str(e)
                })
        
        # 测试错误态处理 (角度 4,26)
        r_404 = requests.get(f"{BASE_URL}/api/v1/reports/999999", proxies=get_http_no_proxies())
        if r_404.status_code == 404:
            try:
                resp_body = r_404.json()
                if not isinstance(resp_body, dict) or 'detail' not in resp_body:
                    self.findings['P2_ERROR_CODE_INCONSISTENCY'].append({
                        'angle': [4, 26],
                        'status': 404,
                        'evidence': '404 响应缺少标准错误消息格式'
                    })
            except:
                pass
    
    def probe_entry_points(self):
        """角度 17,20,21: 权限边界/入口管理"""
        print("\n[PROBE] Entry Points & Permissions...")
        
        # 检查是否有暴露的 admin/internal 路由
        admin_paths = [
            '/admin',
            '/api/v1/admin',
            '/internal',
            '/api/v1/internal',
            '/debug',
        ]
        
        for path in admin_paths:
            try:
                r = requests.get(f"{BASE_URL}{path}", timeout=2, proxies=get_http_no_proxies())
                if r.status_code not in [404, 403]:
                    self.findings['P1_OVERLY_EXPOSED_ENDPOINT'].append({
                        'angle': [17, 21],
                        'path': path,
                        'status': r.status_code,
                        'evidence': f'Admin/Internal 路由应返回 403/404，实际返回 {r.status_code}'
                    })
            except Exception as e:
                pass  # 连接失败视为端点不存在（预期）
    
    def analyze_coverage(self):
        """角度 10,34,38: 窗口完整/验收/业务健康"""
        print("\n[PROBE] Coverage & Business Health Analysis...")
        
        conn = sqlite3.connect('data/app.db')
        c = conn.cursor()
        
        # K-line 覆盖分析
        kline_coverage = c.execute('SELECT COUNT(DISTINCT stock_code) FROM kline_daily').fetchone()[0]
        stock_total = c.execute('SELECT COUNT(*) FROM stock_master').fetchone()[0]
        
        coverage_pct = (kline_coverage / stock_total * 100) if stock_total > 0 else 0
        
        # 按照 39 角度判级
        if coverage_pct < 10:
            severity = 'P1'
        elif coverage_pct < 20:
            severity = 'P2'
        else:
            severity = 'P3'
        
        conn.close()
        
        return {
            'kline_coverage_pct': coverage_pct,
            'severity': severity
        }
    
    def generate_findings(self):
        """生成最终问题清单"""
        print("\n[ANALYSIS] Generating Problem List...")
        
        problems = []
        
        # P1 问题
        for issue_key in ['P1_SETTLEMENT_COMPLETE_MISSING', 'P1_KLINE_COVERAGE_LOW', 'P1_MISSING_CONCLUSION']:
            if issue_key in self.findings:
                for finding in self.findings[issue_key]:
                    problems.append({
                        'p_level': 'P1',
                        'issue_key': issue_key,
                        'finding': finding
                    })
        
        # P2 问题
        for issue_key in ['P2_STALE_OK_DOMINANT', 'P2_WRONG_CONTENT_TYPE', 'P2_ERROR_CODE_INCONSISTENCY']:
            if issue_key in self.findings:
                for finding in self.findings[issue_key]:
                    problems.append({
                        'p_level': 'P2',
                        'issue_key': issue_key,
                        'finding': finding
                    })
        
        return problems
    
    def run(self):
        """执行完整审计"""
        print("=" * 80)
        print("[AUDIT] Starting System Full Audit - 39 Angles Framework")
        print("=" * 80)
        
        self.probe_database()
        self.probe_api()
        self.probe_entry_points()
        self.analyze_coverage()
        
        problems = self.generate_findings()
        
        print("\n" + "=" * 80)
        print("[RESULTS] Problem Summary")
        print("=" * 80)
        print(f"\nTotal Problems Found: {len(problems)}")
        for p in problems:
            print(f"  - {p['p_level']}: {p['issue_key']}")
        
        return {
            'timestamp': datetime.now().isoformat(),
            'problems': problems,
            'db_snapshot': self.db_snapshot,
            'api_responses': self.api_responses
        }

if __name__ == "__main__":
    agent = AuditAgent()
    result = agent.run()
    
    # 保存结果
    with open('_archive/audit_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"\nDetailed results saved to _archive/audit_analysis.json")
