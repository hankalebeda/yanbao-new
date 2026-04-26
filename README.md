# A股研报平台（日级单次版）

## 文档入口（先看这里，避免口径重复）
- `docs/索引.md`
- **AI 接入策略**：`docs/core/08_AI接入策略.md`（本部署仅用 ai-api 四个 Web API + Codex/Gemini CLI，**不使用** API Key）
- 目标/范围/底线：`docs/core/07_系统目标与范围整合.md`
- API/Schema/错误码：`docs/core/05_API与数据契约.md`
- 需求基线：`docs/core/01_需求基线.md`
- 全量功能进度：`docs/core/22_全量功能进度总表_v14.md`

## 1. 安装
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

## 2. 环境变量（可选）
建议用模板快速落地：复制 `.env.example -> .env`，按需覆盖。**本部署不使用** `DEEPSEEK_API_KEY`、`GEMINI_API_KEY` 等 AI API Key，见 `docs/core/08_AI接入策略.md`。
- `OLLAMA_BASE_URL` 默认 `http://127.0.0.1:11434`
- `OLLAMA_MODEL_TEST` 默认 `deepseek-r1:8b`
- `OLLAMA_MODEL_PROD` 默认 `deepseek-r1:14b`
- `USE_TEST_MODEL=true|false` 默认 `true`
- `MOCK_LLM=true|false` 默认 `false`
- `STOCK_POOL=600519.SH,000001.SZ,300750.SZ`
- `ENABLE_SCHEDULER=true|false` 默认 `true`
- `MARKET_PROVIDER_ORDER=eastmoney,tdx`（按顺序尝试）
- `SOURCE_FAIL_OPEN_THRESHOLD=3`（连续失败熔断阈值）
- `SOURCE_RECOVER_SUCCESS_THRESHOLD=2`（连续成功恢复阈值）
- `INTERNAL_API_KEY=...`（设置后内部接口需 `X-Internal-Key`）
- `STRICT_REAL_DATA=true|false`（默认 `true`，拿不到真实行情则直接报错，不返回演示数据）
- `TRUSTED_HOSTS=127.0.0.1,localhost,testserver`（Host 白名单）
- `EXPOSE_ERROR_DETAILS=true|false`（默认 `false`，生产建议关闭）

## 3. 运行
建议固定端口避免冲突（示例用 `8010`）：
uvicorn app.main:app --host 127.0.0.1 --port 8010

访问：
- Web: `http://127.0.0.1:8010/`
- Health: `http://127.0.0.1:8010/health`
- Metrics: `http://127.0.0.1:8010/api/v1/internal/metrics/summary`

单股页面（异步）：
- `http://127.0.0.1:8010/demo/report/600519.SH` 会先返回加载页，再后台生成并自动跳转
- 状态接口：`/demo/report/{stock_code}/status`

## 4. 测试
pytest -q

## 5. 端到端烟测
python runtime/smoke_web.py

## 6. 当前已实现链路
1. 热搜采集（微博/抖音）
2. 实时行情双源接入（东方财富 + 通达信 mootdx 0.11.7）并输出价差一致性
3. 白话研报模板（先给结论与因果链，再给高级细节）
4. 预测结算（1/5/20/60 日窗口）
5. 回归评估与发布/回滚决策字段
6. 日级调度（按股票池批量执行），每只股票每日仅生成一次正式报告
7. 会员闭环（创建订单、支付回调、订阅状态）
8. 报告幂等键（`idempotency_key`）避免重复生成
9. `request_id` 全链路透传（请求头 `X-Request-ID`）
10. 真实行情双源（东财 + 通达信 mootdx）

## 7. 白话模板说明
页面顺序固定为：
1. 可直接执行（仓位建议 + 风险线 + 检查清单）
2. 你现在该怎么做
3. 3个关键数字
4. 股票基础信息（公司概况 + 财务与估值）
5. 预测准确率怎么来的（口径 + 公式 + 样本 + 覆盖率）
6. 研报附录（AI推理全过程、数据来源）
7. 模型分析链路（数据来源 -> 分析步骤 -> 推理摘要 -> 回验计划）
8. 为什么是这个结论（因果链）
9. 实时行情是否可信（双来源一致性）
10. 名词解释
11. 高级细节（折叠）

## 8. 真实可用标准
1. 页面任何核心模块不得出现空白块（即使旧缓存报告缺字段，也必须回退生成可读内容）。
2. 实时行情必须展示双来源状态：`ok_both / ok_eastmoney_only / ok_tdx_only / missing`。
3. `ok_both` 时必须展示东财价、通达信价、价差、一致性。
4. 历史缓存报告若缺少 `plain_report` 或 `market_dual_source`，渲染前必须自动补齐（不依赖人工重跑）。
5. `STRICT_REAL_DATA=true` 时，核心行情不可用必须报错，不得伪造。
6. 接口默认返回安全响应头（`nosniff`、`DENY`、`Referrer-Policy`、`Permissions-Policy`）。
7. 股票代码必须符合 `^\d{6}\.(SH|SZ)$`，非法参数返回 `400 invalid_stock_code`。

## 9. 准确率透明化口径
1. 页面必须同时展示两种“准确率”：
   - `方向可操作准确率`：来自 `direction_forecast.backtest_recent_3m[*].actionable_accuracy`，只统计触发动作的样本。
   - `窗口历史准确率`：来自 `price_forecast.backtest.horizons_recent_3m[*].accuracy`，统计对应窗口方向命中。
2. 页面必须同时展示 `样本数` 与 `覆盖率`，禁止只展示百分比。
3. `confidence` 必须说明来源：`confidence_raw` 与 `confidence_empirical_accuracy` 的校准融合结果。
4. 若样本不足（如 7 天样本 `<5` 或覆盖率 `<5%`），页面必须显示“稳定性偏弱”，并在执行建议中降级仓位。
5. **绩效与统计口径**：以 `docs/core/07_系统目标与范围整合.md` §3 为准；页面需同时展示准确率、样本数、覆盖率与口径简述，避免“只给一个数字”。



