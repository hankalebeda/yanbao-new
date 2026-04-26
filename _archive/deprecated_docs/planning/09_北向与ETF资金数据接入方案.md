# 北向资金与 ETF 资金数据接入方案

> **状态**：✅ **已解决**（方案与接口已验证，见 §6 实测）  
> **目标**：解决 `capital_flow.py` 中 `northbound` 与 `etf_flow` 长期 `status: missing` 的问题  
> **依据**：`14_数据全景分析.md` §8.1、`08_数据扩展路线图.md`、GitHub 开源库调研  
> **最后更新**：2026-02-25

---

## 一、现状与缺口（已解决）

| 维度 | 原状态 | 解决方式 | 验证 |
|------|--------|----------|------|
| **北向资金（个股）** | `status: missing` | akshare `stock_hsgt_individual_em` | ✅ 北向个股 600519 已验证（见 §6） |
| **ETF 资金/申赎** | `status: missing` | akshare `fund_portfolio_hold_em`、`fund_etf_scale_sse/szse` | ✅ ETF 持仓 510050 已验证；份额接口视 akshare 版本 |

---

## 二、北向资金——可解决

### 2.1 推荐方案：AKShare（免费、稳定）

**接口**：`ak.stock_hsgt_individual_em(symbol="600519")`

- **来源**：东方财富网-沪深港通持股-具体股票  
- **数据**：持股日期、当日收盘价、持股数量、持股市值、**今日增持股数**、**今日增持资金**、今日持股市值变化  
- **覆盖**：全历史，单次返回该股票所有北向持股记录

```python
import akshare as ak

# 获取贵州茅台北向持股历史
df = ak.stock_hsgt_individual_em(symbol="600519")
# 列：持股日期、当日收盘价、当日涨跌幅、持股数量、持股市值、
#     持股数量占A股百分比、今日增持股数、今日增持资金、今日持股市值变化
```

**注意**：symbol 需为 6 位 A 股代码（如 `600519`、`000858`），无需市场前缀。

**衍生指标计算**：
- `net_inflow_1d` = 最近 1 日「今日增持资金」
- `net_inflow_5d` = 最近 5 日「今日增持资金」累计
- `streak_days` = 连续净流入/流出天数（按「今日增持资金」正负判断）

### 2.2 备选方案 1：TT_Fund（Scrapy 爬虫）

- **仓库**：<https://github.com/CBJerry993/TT_Fund>
- **爬虫**：`beixiang_10stock.py`，爬取北向资金每日**前 20 大交易股**（沪股通 10 + 深股通 10）
- **字段**：date_time、code、name、net_in（当日净流入）、in（流入）、out（流出）、total（成交额）
- **局限**：仅覆盖上榜的 20 只股票，其他个股无数据
- **适用**：仅需关注热门北向标的时

### 2.3 备选方案 2：TuShare Pro（付费）

- **接口**：`pro.ccass_hold()` 中央结算持股（含北向）
- **限制**：需 Token，且北向持仓 2024-08 起改为**按季度**发布，时效性下降

### 2.4 代理环境说明（必读）

**现象**：本机使用代理（如 Clash、V2Ray）时，访问东方财富、上交所、深交所等国内站点易报 `ConnectionResetError(10054)`，即远程主机关闭连接。

**原因**：代理通常面向境外访问，对国内站点可能拦截、重置或路由异常，导致请求失败。

**处理方式**：与项目现有 `capital_flow`、`market_data` 一致，**绕过代理直连**：

| 组件 | 处理方式 |
|------|----------|
| **httpx** | 创建 Client 时传入 `trust_env=False`，不读取系统代理 |
| **akshare / requests** | 调用前设置 `os.environ["NO_PROXY"] = "*"`，并清除 `HTTP_PROXY`、`HTTPS_PROXY`、`http_proxy`、`https_proxy` |

**实现示例**（已用于验证脚本）：

```python
def _bypass_proxy() -> None:
    """绕过系统代理直连国内站点"""
    os.environ["NO_PROXY"] = "*"
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(k, None)

# 在发起请求前调用
_bypass_proxy()

# httpx 示例
httpx.get(url, ..., trust_env=False)

# akshare 会使用 requests，需在 import 前或调用前执行 _bypass_proxy()
```

**实施要求**：新增 `northbound_data.py`、`etf_flow_service.py` 等模块时，必须在发起 akshare/东方财富相关请求前调用上述逻辑，否则在代理环境下会持续失败。`_bypass_proxy` 可抽取到 `app/core/` 或 `app/services/` 公共模块供复用。

### 2.5 实施建议（北向）

| 步骤 | 动作 |
|------|------|
| 1 | 在 `requirements.txt` 中取消 `akshare` 注释并锁定版本 |
| 2 | 新增 `app/services/northbound_data.py`，封装 `stock_hsgt_individual_em` 调用；**在发起 akshare 请求前调用 §2.4 的 `_bypass_proxy()`** |
| 3 | 在 `capital_flow.py` 的 `_build_capital_flow_summary` 中，用 akshare 数据填充 `northbound` |
| 4 | 增加异常与超时处理，失败时保持 `status: missing` 并打日志 |
| 5 | 更新 `report_engine.py` 中「北向逐股数据暂不支持」的文案 |

---

## 三、ETF 资金——可解决（两层含义）

### 3.1 含义区分

- **A. ETF 层面**：单只 ETF 的申赎/份额变化、资金净流入  
- **B. 个股层面**：某只股票被多少 ETF 持有，以及相关 ETF 的资金变动对个股的间接影响  

本系统当前 `etf_flow` 定义为「对个股的影响」，因此需要 A + B 组合。

### 3.2 ETF 申赎/份额（AKShare）

**接口**：
- `ak.fund_etf_scale_sse(date="20250225")` — 上交所 ETF 基金份额  
- `ak.fund_etf_scale_szse()` — 深交所 ETF 基金份额  

通过**相邻交易日份额变化**可估算申赎净额：
- 份额增加 ≈ 申购大于赎回
- 份额减少 ≈ 赎回大于申购

**局限**：仅能拿到 ETF 维度数据，不能直接得到「某只个股对应的 ETF 资金流」。

**已知问题**（2026-02-25 验证）：akshare 1.18.24 下 `fund_etf_scale_sse`、`fund_etf_scale_szse` 存在解析异常（列索引或字节流处理），详见 §6.2。阶段 1 实施时可优先尝试东方财富直接接口（§3.6）或跟进 akshare 后续版本。

### 3.3 ETF 持仓（个股↔ETF 映射）

**接口**：`ak.fund_portfolio_hold_em(symbol="510050", date="2024")`

- **说明**：天天基金-基金档案-投资组合-基金持仓  
- **返回**：股票代码、股票名称、占净值比例、持股数、持仓市值、季度  
- **用途**：得到 ETF 的股票持仓，建立「ETF↔个股」映射

### 3.4 个股的 ETF 资金影响——推导思路

1. 确定该股票被哪些 ETF 重仓：`fund_portfolio_hold_em` 遍历主要宽基/行业 ETF，筛选含该股的 ETF  
2. 获取这些 ETF 的份额变化：`fund_etf_scale_sse` / `fund_etf_scale_szse`  
3. 按持仓权重加权：`etf_flow_contribution ≈ Σ(ETF 份额变化 × 该股占 ETF 比例)`  

**复杂度**：需维护 ETF 列表、定期更新持仓，计算量较大。可先做**简化版**。

### 3.5 简化方案（推荐先实现）

**阶段 1：仅补 ETF 市场整体信号**

- 使用 `fund_etf_scale_sse` + `fund_etf_scale_szse` 计算**全市场 ETF 份额日变动**
- 在研报中增加「ETF 市场整体申赎」指标（如近 5 日、20 日净申赎）
- 不区分个股，先解决「有无」问题

**阶段 2：个股相关 ETF 资金（可选）**

- 预置核心宽基 ETF 列表（如 510050、510300、159915 等）
- 定期拉取 `fund_portfolio_hold_em` 建立成分股表
- 对该股所在 ETF 的份额变化做加权汇总，写入 `etf_flow`

### 3.6 东方财富直接接口（需自行抓包验证）

东方财富数据中心有 ETF 资金流向页面，可尝试抓取其接口：

- 板块/ETF 资金流向：`https://push2.eastmoney.com/api/qt/clist/get`（需确认 ETF 相关参数）
- 具体接口需通过浏览器开发者工具抓包确认

### 3.7 实施建议（ETF）

| 步骤 | 动作 |
|------|------|
| 1 | 实现阶段 1：优先尝试 `fund_etf_scale_sse/szse`；若 akshare 解析异常，改用东方财富直接接口（§3.6）；**调用前执行 §2.4 的 `_bypass_proxy()`** |
| 2 | 在 `capital_flow.py` 增加 `etf_market_summary`（或扩展 `etf_flow`），先填市场级指标 |
| 3 | 阶段 2：新增 `app/services/etf_flow_service.py`，用 `fund_portfolio_hold_em` 建立个股↔ETF 映射（已验证可用） |
| 4 | 在 `report_data_usage.etf_flow` 中标注 `scope: market` 或 `scope: stock` |

---

## 四、依赖与版本

```txt
# requirements.txt 建议
akshare>=1.12.0  # 取消注释，与东方财富互为备份
```

安装较慢时可使用国内镜像：

```bash
pip install akshare -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com
```

---

## 五、数据源汇总

| 数据类型 | 推荐方案 | 接口/项目 | 是否免费 | 备注 |
|----------|----------|-----------|----------|------|
| 北向个股净买入 | AKShare | `stock_hsgt_individual_em` | ✅ | 东方财富源，覆盖全历史 |
| 北向前 20 交易股 | TT_Fund | `beixiang_10stock.py` | ✅ | 仅覆盖 20 只 |
| ETF 份额/申赎 | AKShare | `fund_etf_scale_sse/szse` | ✅ | 交易所官方 |
| ETF 持仓成分 | AKShare | `fund_portfolio_hold_em` | ✅ | 季度更新 |
| 北向持仓（港交所） | TuShare Pro | `ccass_hold` | ❌ 需 Token | 现为季度频率 |

**备注**：ETF 份额接口 `fund_etf_scale_sse/szse` 在 akshare 1.18.24 下存在解析异常，见 §6.2；阶段 2 的 `fund_portfolio_hold_em` 已验证可用。

---

## 六、验证脚本与测试结果

### 6.1 验证脚本

已实现 `scripts/verify_northbound_etf.py`，覆盖：

| 测试项 | 接口 | 方案需求 |
|--------|------|----------|
| 网络可达性 | httpx 请求东方财富 push2 API | 与 capital_flow 同源，验证直连可用 |
| 北向个股 | `stock_hsgt_individual_em("600519")` | 需含「今日增持资金」等列 |
| ETF 份额-上交所 | `fund_etf_scale_sse(date)` | 需含「基金份额」列 |
| ETF 份额-深交所 | `fund_etf_scale_szse()` | 同上 |
| ETF 持仓 | `fund_portfolio_hold_em("510050", "2024")` | 阶段 2 个股↔ETF 映射 |

**代理处理**：脚本启动时自动执行 `_bypass_proxy()`，绕过系统代理直连，详见 §2.4。

**运行**：`python scripts/verify_northbound_etf.py`，结果写入 `docs/core/test_results/verify_northbound_etf_*.json`。

### 6.2 验证结论（2026-02-25 运行，绕过代理后）

| 测试项 | 结果 | 说明 |
|--------|------|------|
| 东方财富 push2 可达 | ✅ 通过 | 与 capital_flow 同源，可访问 |
| akshare 已安装 | ✅ 通过 | 版本 1.18.24 |
| 北向个股 600519 | ✅ 通过 | 行数 1683，含「今日增持资金」「持股日期」等 9 列，可计算 net_inflow_1d/5d、streak_days |
| ETF 份额-上交所 | ❌ 失败 | akshare 解析异常：`None of [Index([...])] are in the [columns]`，疑为 akshare 列名变更或接口变更 |
| ETF 份额-深交所 | ❌ 失败 | akshare 解析异常：`Expected file path name or file-like object, got <class 'bytes'>` |
| ETF 持仓 510050 | ✅ 通过 | 行数 225，含「股票代码」「股票名称」「占净值比例」「持仓市值」等列 |

**代理问题**：未绕过代理时全部失败（ConnectionResetError）；执行 §2.4 中 `_bypass_proxy()` 后，北向与 ETF 持仓均通过，证实为代理导致。

**ETF 份额接口**：失败为 akshare 内部解析问题，与代理无关；阶段 2 依赖的 `fund_portfolio_hold_em` 已通过，阶段 1 可考虑东方财富直接接口或后续 akshare 版本适配。

**ETF 份额兜底规则**：当 akshare `fund_etf_scale_sse`/`fund_etf_scale_szse` 解析异常时，可改用东方财富直接接口（见 §3.6 抓包验证后的 URL）；在 `etf_flow_service.py` 中实现 `try akshare except 解析异常 -> 东财 direct API` 的 fallback 逻辑；`report_data_usage.etf_flow` 标注 `status: ok`（东财直连）或 `status: degraded`（akshare 失败且东财也失败时）。

### 6.3 满足方案需求判断

| 需求 | 是否满足 | 依据 |
|------|----------|------|
| 北向个股净买入 1d/5d/10d/20d、连续流入天数 | ✅ 已验证 | `stock_hsgt_individual_em` 返回「今日增持资金」，验证通过 |
| ETF 市场整体申赎（阶段 1） | ⚠️ 待验证 | `fund_etf_scale_sse/szse` 在 akshare 1.18.24 下解析异常，可尝试东方财富直接接口 |
| ETF 个股映射（阶段 2） | ✅ 已验证 | `fund_portfolio_hold_em` 返回持仓，验证通过 |

---

## 七、实施前检查清单

| 检查项 | 说明 |
|--------|------|
| 代理环境 | 本机若使用代理，必须在请求前执行 `_bypass_proxy()`（§2.4），否则北向/ETF 接口将报 ConnectionResetError |
| akshare 版本 | 建议 `akshare>=1.12.0`，当前验证使用 1.18.24 |
| ETF 份额接口 | `fund_etf_scale_sse/szse` 存在解析异常；可先实现阶段 2（ETF 持仓/个股映射），或改用东方财富直接接口完成阶段 1 |
| 运行验证脚本 | 实施前运行 `python scripts/verify_northbound_etf.py` 确认本地环境可达 |

---

*本方案基于 GitHub、AKShare 文档及东方财富公开接口调研整理，实施时需结合实际网络环境与接口变更情况调整。*
