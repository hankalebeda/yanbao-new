# E2E Mock 设计规格

> **文档编号**：`docs/guides/99_E2E_Mock设计规格.md`（guides-99）  
> **用途**：为 E2E-SIM-02/05、E2E-AUDIT-01～03 提供 mock 注入点与 pytest 实现规格，供代码生成与验收对齐。  
> **依据**：`docs/guides/01_测试验收计划.md` §2.3～§2.5；用例骨架 `tests/test_e2e_sim.py`。  
> **更新**：2026-02-25

---

## 1. 总览

| 用例 ID | 依赖 | Mock 注入点 | Mock 方式 |
|---------|------|-------------|-----------|
| E2E-SIM-02 | 行情触发止损 / `_sim_settle_job` | `sim_settle_service.run_settle` 内行情获取 | `unittest.mock.patch` 行情 API 返回跌破止损价 |
| E2E-SIM-05 | `drawdown_state=HALT` + BUY 发布 | `sim_account` 查询 / `report_engine` 开仓前检查 | patch `sim_position_service` 或 DB 返回 HALT |
| E2E-AUDIT-01 | `aggregate_audit_votes` 返回值 | `llm_router.aggregate_audit_votes` | patch 返回 `{"audit_flag": "unanimous_buy", ...}` |
| E2E-AUDIT-02 | 审计方返回 severity=high | 同上 | patch 返回 `{"audit_flag": "high_risk_flag", ...}` |
| E2E-AUDIT-03 | 三票不一致 | 同上 | patch 返回 `{"audit_flag": "votes_uncertain", ...}` |

---

## 2. E2E-SIM-02：OPEN → CLOSED_SL

### 2.1 目标

对 seeded OPEN 持仓执行结算逻辑，使当日行情跌破 `stop_loss_price`，验证状态变为 `CLOSED_SL`，`sim_pnl_net` 含手续费。

### 2.2 Mock 注入点

- **位置**：`app.services.sim_settle_service._fetch_quote`（当前实现中行情由该函数获取，无 `_get_close_price`）
- **方式**：`patch('app.services.sim_settle_service._fetch_quote')`
- **返回值**：对 seeded 持仓的 `stock_code`，返回 dict 含 `close/high/low` 且 `low <= stop_loss_price` 触发止损（如 stop_loss=1700，返回 `{"close":1680,"high":1700,"low":1680,"limit_up":None,"limit_down":None,"volume":0}`）

### 2.3 实现步骤（建议）

1. `_seed_buy_position()` 创建 OPEN 持仓，`stop_loss_price=1700`
2. `patch` `_fetch_quote` 返回 `{"close":1680,"high":1700,"low":1680,"limit_up":None,"limit_down":None,"volume":0}`
3. 调用 `sim_settle_service.run_settle()`（无参数，内部使用 `latest_trade_date_str()` 和 `SessionLocal()`）
4. 断言：`db.query(SimPosition).filter(...).first().status == "CLOSED_SL"`
5. 断言：`sim_pnl_net` 为负，且 `abs(sim_pnl_net - gross) <= 手续费+印花税`（万三+0.05%）

### 2.4 pytest 骨架

```python
@patch("app.services.sim_settle_service._fetch_quote")
def test_e2e_sim_02_settle_to_closed_sl(mock_fetch):
    report_id, pos_id = _seed_buy_position("600519.SH")
    mock_fetch.return_value = {"close": 1680, "high": 1700, "low": 1680, "limit_up": None, "limit_down": None, "volume": 0}
    from app.services.sim_settle_service import run_settle
    run_settle()
    db = SessionLocal()
    pos = db.query(SimPosition).filter(SimPosition.id == pos_id).first()
    assert pos.status == "CLOSED_SL"
    assert pos.sim_pnl_net is not None
    # 净盈亏 = 毛盈亏 - 手续费 - 印花税
```

---

## 3. E2E-SIM-05：HALT 阻止新开仓

### 3.1 目标

当 `drawdown_state=HALT` 时，生成 BUY 研报不写入 `sim_position`，研报仍发布。

### 3.2 Mock 注入点

- **位置**：`report_engine` 开仓前读取 `sim_account.drawdown_state`；或 `sim_position_service.create_position` 内检查
- **方式**：在调用 `report_engine.generate_report` 前，向 DB 写入 `SimAccount(snapshot_date=今日, drawdown_state="HALT", ...)`，或 patch `_get_latest_drawdown_state` 返回 `"HALT"`
- **判定**：生成 BUY 研报后，`sim_position` 表无该 `report_id` 对应记录

### 3.3 实现步骤（建议）

1. 写入或 patch `sim_account` 最新快照 `drawdown_state="HALT"`
2. 使用 MOCK_LLM 或 seeded 数据触发 `POST /api/v1/reports/generate`，得到 BUY 研报
3. 断言：`GET /api/v1/sim/positions?report_id=xxx` 或查询 `sim_position` 表，无该 report 的持仓
4. 断言：研报 `content_json.sim_trade_instruction` 可为空或标注「回撤保护中」

---

## 4. E2E-AUDIT-01～03：三方投票审计

### 4.1 目标

Mock `aggregate_audit_votes` 返回值，验证研报 JSON 中 `audit_flag`、`confidence` 符合预期。

### 4.2 Mock 注入点

- **位置**：E2 实装后，`report_engine` 会调用审计整合；**patch 使用处**：`app.services.report_engine.aggregate_audit_votes`（若 report_engine 从 llm_router 导入该函数，则 patch 后者会无效，必须 patch report_engine 内的引用）
- **方式**：`patch('app.services.report_engine.aggregate_audit_votes')`（报告生成入口在 report_engine，patch 使用处即可）
- **返回值结构**：`{"audit_flag": str, "audit_detail": {...}, "confidence_adjustment": float}`

### 4.3 各用例 Mock 值

| 用例 | audit_flag | confidence 预期 |
|------|------------|-----------------|
| E2E-AUDIT-01 | `unanimous_buy` | 可选提升 ≤0.05 |
| E2E-AUDIT-02 | `high_risk_flag` | `confidence ≤ base × 0.75` |
| E2E-AUDIT-03 | `votes_uncertain` | 不变；高级区须含「存在审计异议」|

### 4.4 pytest 骨架

```python
@patch("app.services.report_engine.aggregate_audit_votes")
def test_e2e_audit_01_audit_flag_in_report(mock_audit):
    mock_audit.return_value = {"audit_flag": "unanimous_buy", "audit_detail": {}}
    # 触发 BUY 研报生成
    r = client.post("/api/v1/reports/generate", json={"stock_code": "600519.SH"})
    report_id = r.json()["data"]["report_id"]
    rr = client.get(f"/api/v1/reports/{report_id}")
    j = rr.json()["data"]
    assert j.get("content_json", {}).get("audit_flag") in (
        "unanimous_buy", "majority_agree", "votes_uncertain", "high_risk_flag"
    )
```

---

## 5. 回归入口

- **E2E-SIM-02/05**：已实现（2026-02-25），已移除 skip
- **E2E-AUDIT**：待 E2 实装后移除 `@pytest.mark.skip`
- 建议与主回归同轮执行：`pytest tests/test_api.py tests/test_trade_calendar.py tests/test_e2e_sim.py -v`

---

## 6. FR-05a / FR-07 Mock 验收规格（待实现）

### 6.1 FR-05a 通知发送 Mock

| 目标 | Mock 方式 | 断言 |
|------|-----------|------|
| 通知发送成功率 ≥95% | `patch` 通知发送函数（如 `_send_webhook`），返回 200；再 patch 10% 返回 非200，统计成功率 | 成功率 = 成功数/总数 ≥ 0.95；失败时须有重试记录 |
| 失败重试 1 次 | patch 首次返回 500，第二次返回 200 | 最终记录为成功，retry_count=1 |
| 免费用户模糊化 | 抽检通知 payload，不含完整止损/目标价 | 免费 tier 时 instruction 字段模糊或缺失 |

**建议用例**：`test_notification_send_success_rate_mock`、`test_notification_retry_on_failure`。

### 6.2 FR-07 反馈 2 秒内 Mock

| 目标 | Mock 方式 | 断言 |
|------|-----------|------|
| 2 秒内写入 | 调用 `POST /api/v1/report-feedback`，记录请求开始与响应时间 | `response_time_ms < 2000`；DB 中 `report_feedback` 表有对应记录 |
| 负反馈率 ≥30% 告警 | mock 插入多条 `is_helpful=0`，使 7 日内负反馈率 ≥30% | 触发 `ReportHighNegativeFeedback` 或等效日志/打点 |
| 单报告负反馈 ≥3 次 | 需 user 维度（每用户每报告 1 次），多用户 mock 插入 3 条负反馈 | 人工复审标记或等效逻辑可测 |

**建议用例**：`test_report_feedback_within_2s`（需 JWT mock 或测试环境放行匿名）。

## 7. 关联文档

| 文档 | 用途 |
|------|------|
| `docs/guides/01_测试验收计划.md` §2.3～§2.5 | 用例定义与判定 |
| `tests/test_e2e_sim.py` | 用例骨架与 seeded 数据 |
| `docs/core/13_多模型路由设计.md` §10 | 审计触发条件与投票整合规则 |
