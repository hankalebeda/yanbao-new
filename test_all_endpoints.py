#!/usr/bin/env python3
"""
全系统API测试 - 验证39个分析角度对应的功能点
"""

import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:8000"
test_results = []

def test_endpoint(name, method, endpoint, **kwargs):
    """测试单个端点"""
    try:
        if method == "GET":
            resp = requests.get(f"{BASE_URL}{endpoint}", timeout=10, **kwargs)
        elif method == "POST":
            resp = requests.post(f"{BASE_URL}{endpoint}", timeout=10, **kwargs)
        else:
            resp = requests.request(method, f"{BASE_URL}{endpoint}", timeout=10, **kwargs)
        
        status = "✅" if resp.status_code < 400 else "⚠️" if resp.status_code < 500 else "❌"
        result = {
            "name": name,
            "endpoint": endpoint,
            "method": method,
            "status_code": resp.status_code,
            "status": status,
            "error": None
        }
        
        # 尝试解析响应
        try:
            data = resp.json()
            result["response_type"] = type(data).__name__
            if isinstance(data, dict) and "data" in data:
                result["has_data"] = True
            else:
                result["has_data"] = False
        except:
            result["response_type"] = "text"
            result["has_data"] = False
            
        test_results.append(result)
        return result
    except Exception as e:
        test_results.append({
            "name": name,
            "endpoint": endpoint,
            "status": "❌",
            "error": str(e)
        })
        return None

print("=" * 100)
print("系统API功能点测试 - 基于39角度分析")
print("=" * 100)
print(f"测试时间: {datetime.now().isoformat()}")
print(f"基础URL: {BASE_URL}")
print()

# =========================================================================
# 第一组：事实、契约、数据相关端点
# =========================================================================

print("\n【第一组】事实、契约、数据 - API测试")
print("-" * 100)

print("\n角度1-3：真实性/契约 - 核心路径")
test_endpoint("首页", "GET", "/")
test_endpoint("首页重定向到/index.html", "GET", "/index.html")
test_endpoint("API Health检查", "GET", "/health")
test_endpoint("内部Health聚合", "GET", "/internal/health/aggregate")

print("\n角度4-6：错误码/契约/证据链")
test_endpoint("404错误端点", "GET", "/api/v1/reports/nonexistent")
test_endpoint("已发布报告列表", "GET", "/api/v1/reports?status=published")
test_endpoint("研报详情获取", "GET", "/api/v1/reports/list?limit=1")

print("\n角度5-7：数据血缘/样本独立")
test_endpoint("数据使用链接", "GET", "/api/v1/reports/usage-lineage")
test_endpoint("交易指令列表", "GET", "/api/v1/trading-instructions")

# =========================================================================  
# 第二组：时间、运行态、恢复相关
# =========================================================================

print("\n\n【第二组】时间、运行态、恢复 - API测试")
print("-" * 100)

print("\n角度9-14：时间锚点/任务生命周期")
test_endpoint("市场状态查询", "GET", "/api/v1/market-state")
test_endpoint("调度器状态", "GET", "/api/v1/scheduler/status")
test_endpoint("生成任务列表", "GET", "/api/v1/generation-tasks")
test_endpoint("股票池快照", "GET", "/api/v1/stock-pool")

print("\n角度15-16：降级策略/缓存")
test_endpoint("缓存状态", "GET", "/internal/cache-status")
test_endpoint("修复脚本状态", "GET", "/internal/repair-scripts")

# =========================================================================
# 第三组：安全、权限、边界相关
# =========================================================================

print("\n\n【第三组】安全、权限、边界 - API测试")
print("-" * 100)

print("\n角度17-22：权限/审计/依赖")
test_endpoint("用户认证状态", "GET", "/api/v1/auth/me")
test_endpoint("审计日志查询", "GET", "/api/v1/admin/audit-logs")
test_endpoint("后台管理面板", "GET", "/admin/dashboard")
test_endpoint("功能注册表", "GET", "/api/v1/features")

# =========================================================================
# 第四组：展示、桥接、观测相关
# =========================================================================

print("\n\n【第四组】展示、桥接、观测 - API测试")
print("-" * 100)

print("\n角度25-29：页面/显示/观测")
test_endpoint("首页数据", "GET", "/api/v1/dashboard/overview")
test_endpoint("统计看板", "GET", "/api/v1/dashboard/statistics")
test_endpoint("模拟看板", "GET", "/api/v1/dashboard/simulation")
test_endpoint("平台配置", "GET", "/api/v1/platform-config")
test_endpoint("平台汇总", "GET", "/api/v1/platform-summary")

# =========================================================================
# 第五组：测试、治理、决策相关
# =========================================================================

print("\n\n【第五组】测试、治理、决策 - API测试")
print("-" * 100)

print("\n角度30-39：门禁/治理/决策")
test_endpoint("系统状态汇总", "GET", "/api/v1/system-status")
test_endpoint("特性地图", "GET", "/api/v1/feature-map")
test_endpoint("问题列表", "GET", "/api/v1/issues")
test_endpoint("测试覆盖率", "GET", "/api/v1/test-coverage")

# =========================================================================
# 核心业务路径
# =========================================================================

print("\n\n【核心业务路径完整性检查】")
print("-" * 100)

print("\n FR-01 股票池")
test_endpoint("核心池查询", "GET", "/api/v1/stock-pool/latest")
test_endpoint("手动刷新", "POST", "/api/v1/stock-pool/refresh", json={})

print("\n FR-04 热点数据")
test_endpoint("热股查询", "GET", "/api/v1/hotspot/top")
test_endpoint("热点健康检查", "GET", "/internal/hotspot/health")

print("\n FR-07 结算/绩效")
test_endpoint("结算列表", "GET", "/api/v1/settlement/list")
test_endpoint("预测统计", "GET", "/api/v1/settlement/stats")

print("\n FR-08 模拟仓位")
test_endpoint("模拟账户", "GET", "/api/v1/sim/accounts")
test_endpoint("模拟持仓", "GET", "/api/v1/sim/positions")

print("\n FR-10 站点/仪表盘")
test_endpoint("首页", "GET", "/")
test_endpoint("报告列表", "GET", "/api/v1/reports/list")
test_endpoint("报告详情", "GET", "/api/v1/reports/detail")

print("\n FR-12 后台管理")
test_endpoint("用户管理", "GET", "/api/v1/admin/users")
test_endpoint("报告管理", "GET", "/api/v1/admin/reports")

# =========================================================================
# 汇总
# =========================================================================

print("\n\n【测试结果汇总】")
print("=" * 100)

passed = sum(1 for r in test_results if r.get("status_code", 500) < 400)
failed = sum(1 for r in test_results if r.get("status_code", 500) >= 500)
warned = sum(1 for r in test_results if 400 <= r.get("status_code", 500) < 500)
errors = sum(1 for r in test_results if r.get("error"))

print(f"\n总测试数: {len(test_results)}")
print(f"✅ 成功 (2xx): {passed}")
print(f"⚠️  警告 (4xx): {warned}")
print(f"❌ 失败 (5xx): {failed}")
print(f"💥 错误连接: {errors}")

print(f"\n成功率: {passed}/{len(test_results)} = {100*passed/len(test_results):.1f}%")

print("\n\n详细结果:")
print("-" * 100)
for r in test_results:
    status_code = r.get("status_code", "ERR")
    endpoint = r.get("endpoint", "N/A")
    name = r.get("name", "Unknown")
    error = r.get("error")
    
    if error:
        print(f"{r['status']} {name:30} {endpoint:40} ERROR: {error}")
    else:
        print(f"{r['status']} {name:30} {endpoint:40} [{status_code}]")

print("\n" + "=" * 100)
