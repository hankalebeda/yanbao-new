# 研报改进实施 - Phase 2 完成总结

**时间**: 2024-04-16  
**版本**: v1  
**状态**: ✅ PHASE 2 COMPLETE，准备进入 Phase 3  

---

## Phase 2 目标
改进资金面数据展示的**透明度**，让用户清楚理解**为什么**某些维度数据缺失，而不是陷入困惑。

---

## Phase 2 实施内容

### 1. 公司简介缺失问题 (✅ RESOLVED)

**问题**: 研报中公司简介始终显示"原始文本暂缺"

**根因**: 
- `stock_profile_collector.py` 只收集了 PE/PB/ROE 等估值数据
- 没有调用东方财富公司基本信息 API 来获取公司简介

**解决方案**:
- 文件: `app/services/stock_profile_collector.py`
- 修改1 (L20): 添加公司信息 API 端点
  ```python
  _COMPANY_INFO_ENDPOINT = "https://emh5.eastmoney.com/api/gongsi/getgongsijibenxinxi"
  ```
- 修改2 (L150+): 添加 `_fetch_company_brief()` 函数
  - 调用 East Money 公司基本信息 API
  - 从响应中提取 gongsijibenxinxi.gongsijianjie 字段
  - 返回简介文本
- 修改3 (L280+): 修改 `fetch_stock_profile()` 函数
  - 调用 `_fetch_company_brief()` 获取公司简介
  - 将简介添加到 snapshot 返回值: `"company_brief": brief_text`

**测试结果**: ✅ PASSED
- `pytest tests/test_fr10_reports.py -k company_overview -xvs` → 通过

**代码文件**: 
- [app/services/stock_profile_collector.py](app/services/stock_profile_collector.py)

---

### 2. 资金面数据缺失**原因不明**问题 (✅ ENHANCED)

**问题**: 
- 北向5日净流 missing ← 用户不知道为什么  
- ETF5日申赎 missing ← 用户不知道为什么  
- 其他维度缺失但无说明 → 用户猜测

**根因**:
- `ssot_read_model.py` 的 `_build_capital_game_summary()` 只记录了 missing_dimensions（哪些缺失）
- 但没有记录 WHY（为什么缺失）和 HOW TO FIX（如何补救）

**解决方案**:
- 文件: `app/services/ssot_read_model.py`

#### 修改1: 增强 `_build_company_intro_fallback()` (L335)
- 签名更新: 接收 `company_brief_from_profile: str | None = None` 参数
- 逻辑: 
  - 如果存在真实公司简介且长度 >20 字，返回: `"结构化数据\n\n公司简介: {brief}"`
  - 否则: 返回纯结构化数据 + "暂缺" 提示

#### 修改2: 添加 `missing_reasons` 字典 (L1020+)
- 结构: 为每个可能缺失的维度添加说明
  ```python
  missing_reasons = {
      "northbound_5d_net_flow": {
          "reason": "逐股数据源限制 - akshare 仅支持北向概况查询",
          "remediation": "可用融资融券变化进行代理估算",
          "status": "data_source_limited"
      },
      "etf_5d_subscription": {
          "reason": "无开放API - 龙虎榜ETF端数据无公开接口",
          "remediation": "可用融资融券或主力资金变化代替",
          "status": "no_public_api"
      },
      "main_force_flow": {
          "reason": "（具体原因） - 数据收集失败/API超时/缺失",
          "remediation": "建议重新生成报告或依赖龙虎榜数据",
          "status": "collection_failed|api_timeout|missing"
      },
      ...
  }
  ```

#### 修改3: 返回值集成 (L1085)
- `_build_capital_game_summary()` 的返回字典增加字段:
  ```python
  return {
      "headline": headline,
      "summary_text": summary_text,
      "has_real_conclusion": bool(has_real),
      "missing_dimensions": missing_dimensions,
      "missing_reasons": missing_reasons,  # ← NEW
      "completeness_level": completeness_level,
      ...
  }
  ```

**数据流**:
1. `stock_profile_collector.py` 获取公司简介
2. `report_generation_ssot.py` 调用收集函数生成报告
3. `ssot_read_model.py` 构建 capital_game_summary 包含 missing_reasons
4. `/api/v1/reports/{report_id}` API 返回完整结构

**测试结果**: ✅ PASSED
- Python 语法检查: `python -m py_compile app/services/ssot_read_model.py` → Exit code 0

**代码文件**:
- [app/services/ssot_read_model.py](app/services/ssot_read_model.py) (L335, L1020+, L1085)

---

### 3. 提示词优化（准备中）

**改进目标**: 让 LLM 生成的 conclusion_text 不仅是结论，而是因果链

**当前状态**: 📋 PENDING
- 文件: `app/services/report_generation_ssot.py`
- 需要改进的部分:
  - `_build_llm_prompt()` 中的 conclusion_text 要求 (L983)
  - 添加新约束：
    1. conclusion_text 必须包含【技术面】【资金面】【估值面】【操作】四维
    2. reasoning_chain_md 必须体现多周期资金面对比（1d/5d/10d/20d净流趋势）
    3. 必须指出"主力与游资分歧"等深层含义
    4. 缺失维度要定量说明影响："因缺失X，可信度下降20%"

---

## Phase 2 验证清单

- ✅ 公司简介 API 集成完成
- ✅ company_brief 添加到 stock profile snapshot
- ✅ _build_company_intro_fallback() 改进
- ✅ missing_reasons 字典结构完成
- ✅ capital_game_summary 返回值包含 missing_reasons
- ✅ 语法检查通过
- ✅ company_overview 测试通过
- ⏳ 待集成测试（生成完整报告 + API 响应验证）

---

## Phase 2 → Phase 3 转接

### Phase 3 目标: 结论依据深度优化

**核心问题**: 当前 conclusion_text 和 9 张证据卡片都是"平行"的，缺乏因果链

**实施内容**:
1. 改进 LLM 提示词中 conclusion_text 和 reasoning_chain_md 的要求
2. 在 evidence_backing_points 中加入"多周期对比"维度
3. 添加"缺失维度影响定量化"说明

**具体代码修改位置**:
- `app/services/report_generation_ssot.py` L978-1010
- `app/services/report_engine.py` L1690 (evidence_backing_points 生成)

**预计完成时间**: 1-2 小时

---

## 数据流验证路径

生成新报告后验证改进：

```bash
# 1. 生成新报告 (贵州茅台示例)
curl -X POST http://127.0.0.1:8000/api/v1/internal/reports/generate-batch \
  -H "Content-Type: application/json" \
  -d '{
    "stocks": [{"code": "600519.SH", "name": "贵州茅台"}],
    "trade_date": "2024-04-16"
  }'

# 2. 查询报告包含 missing_reasons
curl http://127.0.0.1:8000/api/v1/reports/600519.SH \
  | jq '.data.capital_game_summary.missing_reasons'

# 期望输出:
# {
#   "northbound_5d_net_flow": {
#     "reason": "逐股数据源限制 - akshare 仅支持北向概况查询",
#     "remediation": "可用融资融券变化进行代理估算",
#     "status": "data_source_limited"
#   },
#   ...
# }
```

---

## 后续工作顺序

1. **立即**: 测试 Phase 2 改动在完整报告生成中的效果
2. **今日**: 实施 Phase 3 提示词和证据卡片深度优化
3. **本周**: Phase 4 结论文本优化（must-have viewpoint 检查表）
4. **持续**: 监控报告质量趋势（测试通过率/用户反馈）

---

## 相关文档

- [REPORT_IMPROVEMENT_ANALYSIS_v27.md](docs/REPORT_IMPROVEMENT_ANALYSIS_v27.md) - 原始问题分析 + 6阶段路线图
- [stock_profile_collector.py](app/services/stock_profile_collector.py) - 公司信息收集
- [ssot_read_model.py](app/services/ssot_read_model.py) - 读模型 + missing_reasons
- [report_generation_ssot.py](app/services/report_generation_ssot.py) - LLM 提示词 (准备优化)

