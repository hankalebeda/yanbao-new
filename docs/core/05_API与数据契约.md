# API 与数据契约

> **文档编号**：05  
> **版本**：v3.0  
> **最后更新**：2026-04-22（全量重写，从代码逆向完整规格）  
> **SSOT 角色**：全接口 Schema 唯一来源；所有路由文件必须与本文档保持一致  
> **主要变更**：填写全部章节（原内容均为占位符）；补充 §12–§16 及附录

---

## 目录

| 章节 | 内容 |
|------|------|
| §0 | 通用约定（Base URL · 认证 · 响应包装 · 中间件） |
| §1 | 基础设施 API（健康检查 · 运行时锚点） |
| §2 | 业务核心 API（研报 · 股票 · 预测 · 平台配置 · 用户收藏） |
| §3 | 认证 API（登录 · 注册 · 刷新 · OAuth） |
| §4 | 看板与首页 API（首页 · Dashboard 统计 · 股票池 · 模拟看板） |
| §5 | 管理员 API（需 admin/super_admin 角色） |
| §6 | 内部 API（服务间调用，X-Internal-Token 认证） |
| §7 | 计费 API（订单 · Webhook） |
| §8 | 模拟仓 API（持仓 · 账户快照 · 绩效摘要） |
| §9 | 治理与特征目录 API |
| §10 | 类型冻结（所有枚举） |
| §11 | 业务对象契约（Report · User · SimPosition 完整 Schema） |
| §12 | 证据与推理链契约 |
| §13 | 指标与预测契约 |
| §14 | 响应约束（分页 · 排序 · 过滤规则） |
| §15 | 错误码词典（HTTP + 业务码） |
| §16 | Web 页面路由（HTML 入口） |

---

## §0 通用约定

### §0.1 基础信息

| 项目 | 值 |
|------|----|
| 开发环境 Base URL | `http://localhost:8010` |
| API 版本前缀 | `/api/v1/` |
| 服务端口 | `8010`（由 `settings.port` 控制） |
| 字符编码 | UTF-8 |
| 日期格式 | ISO-8601 `YYYY-MM-DD` |
| 时间格式 | ISO-8601 `YYYY-MM-DDTHH:MM:SS+00:00`（UTC） |

### §0.2 认证机制

平台支持三种认证方式，优先级：`Authorization` Header > Cookie：

#### a) Cookie 认证（浏览器端）

```
Cookie: access_token=<JWT>
```

- Cookie 名：`access_token`
- 属性：`HttpOnly; SameSite=Lax; Path=/`
- 生产环境额外加 `Secure`

#### b) Bearer Token 认证（API 客户端）

```
Authorization: Bearer <JWT>
```

#### c) 内部服务认证（仅 `/api/v1/internal/*`）

```
X-Internal-Token: <internal_token>
```

值来自 `settings.internal_api_token`，不等同于 JWT。

#### JWT Payload 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `sub` | string | 用户 UUID |
| `email` | string | 用户邮箱 |
| `role` | string | `user \| admin \| super_admin` |
| `tier` | string | `Free \| Pro \| Enterprise` |
| `membership_level` | string | `free \| monthly \| annual` |
| `exp` | int | Unix 时间戳，过期时间 |
| `iat` | int | Unix 时间戳，签发时间 |
| `jti` | string | JWT 唯一 ID（用于撤销） |
| `sid` | string | 会话 ID（access token 专有） |
| `type` | string | `access` 或 `refresh` |

#### Access Token 有效期

默认 `settings.jwt_access_token_expire_hours`（默认 24h）；Refresh Token 默认 7 天。

### §0.3 统一响应包装

所有 API 响应均由 `app/core/response.py::envelope()` 包装：

```json
{
  "success": true,
  "message": "ok",
  "data": "<any>",
  "request_id": "uuid-v4"
}
```

**错误响应（额外字段）**：

```json
{
  "success": false,
  "message": "<error_code>",
  "data": null,
  "request_id": "uuid-v4",
  "error_code": "<SCREAMING_SNAKE_CASE>",
  "error_message": "<人类可读说明>"
}
```

**降级响应（额外字段）**：

```json
{
  "success": true,
  "data": { "...": "..." },
  "degraded": true,
  "degraded_reason": "<降级原因>"
}
```

> 降级时 `success` 仍可为 `true`，但 `degraded=true` 表示数据可能不完整。

### §0.4 请求 ID 传播

- 客户端可在请求头携带 `X-Request-ID: <uuid>`
- 服务器若未收到则自动生成
- 响应头始终返回 `X-Request-ID`
- 响应体中的 `request_id` 与响应头保持一致

### §0.5 安全响应头（全局中间件）

所有响应自动附加：

```
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
Referrer-Policy: no-referrer
Permissions-Policy: camera=(), microphone=(), geolocation=()
```

### §0.6 分页约定

凡带分页的接口，`data` 字段包含：

```json
{
  "items": ["..."],
  "total": 100,
  "page": 1,
  "page_size": 20,
  "pages": 5
}
```

默认 `page=1, page_size=20`，最大 `page_size=100`（部分接口有差异，见各节说明）。

---

## §1 基础设施 API

### §1.1 系统健康检查

```
GET /health
GET /api/v1/health   （alias）
```

**认证**：无需  
**响应**：`200 OK`（即使内部降级也返回 200；通过 `status` 字段判断健康度）

```json
{
  "success": true,
  "data": {
    "status": "ok | degraded",
    "database_status": "ok | degraded",
    "scheduler_status": "ok | degraded | disabled",
    "llm_router_status": "ok | degraded | unconfigured",
    "hotspot_status": "ok | degraded",
    "report_chain_status": "ok | degraded",
    "settlement_status": "ok | degraded",
    "settlement_coverage_pct": 85.3,
    "kline_status": "ok | degraded",
    "kline_coverage_pct": 92.1,
    "checked_at": "2026-04-22T10:00:00+00:00"
  }
}
```

**降级判断逻辑**：

- `overall=degraded`：`database_status != ok` 或 `scheduler_status == degraded` 或 (`hotspot_status == degraded` 且 `report_chain_status == degraded`) 或 `settlement_status == degraded`
- `kline_status=degraded`：K 线覆盖率 < 10%
- `llm_router_status=unconfigured` 不影响 overall

### §1.2 市场状态

```
GET /api/v1/market/state
```

**认证**：无需

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "trade_date": "2026-04-22",
    "market_state": "BULL | NEUTRAL | BEAR",
    "confidence": 0.72,
    "trend_label": "震荡上行",
    "data_status": "READY | COMPUTING | DEGRADED",
    "status_reason": null
  }
}
```

---

## §2 业务核心 API

### §2.1 研报列表

```
GET /api/v1/reports
```

**认证**：可选（未登录可访问，viewer_tier 为 Free 级别）

**查询参数**：

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `stock_code` | string | — | 格式 `^\d{6}\.(SH\|SZ)$` |
| `stock_name` | string | — | 模糊匹配股票名 |
| `trade_date` | string | — | 精确匹配 `YYYY-MM-DD` |
| `today` | bool | — | `true` 仅返回最新交易日研报 |
| `date_from` | string | — | 交易日下界（含） |
| `date_to` | string | — | 交易日上界（含） |
| `conclusion` | string | — | `BUY\|SELL\|HOLD` |
| `recommendation` | string | — | 同 `conclusion`（别名） |
| `strategy_type` | string | — | `A\|B\|C` |
| `position_status` | string | — | 同步过滤 sim_position 状态 |
| `market_state` | string | — | `BULL\|NEUTRAL\|BEAR` |
| `q` | string | — | 全文搜索（股票代码 / 名称） |
| `limit` | int | 20 | 兼容旧参数；优先使用 `page_size` |
| `run_mode` | string | — | `daily\|hourly` |
| `in_pool` | bool | — | 是否在当日股票池内 |
| `exclude_test` | int | 1 | `1` 排除 `source=test` 的研报 |
| `page` | int | 1 | 页码（≥1） |
| `page_size` | int | 20 | 每页条数（1–100） |
| `sort` | string | `-created_at` | 前缀 `-` 降序；允许字段见 §14 |
| `quality_flag` | string | — | `ok\|stale_ok\|missing\|degraded` |

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "items": [
      {
        "report_id": "rpt-uuid",
        "stock_code": "600519.SH",
        "stock_name": "贵州茅台",
        "trade_date": "2026-04-22",
        "recommendation": "BUY",
        "confidence": 0.85,
        "strategy_type": "A",
        "market_state": "BULL",
        "quality_flag": "ok",
        "run_mode": "daily",
        "source": "real",
        "published": true,
        "publish_status": "PUBLISHED",
        "created_at": "2026-04-22T06:30:00+00:00",
        "conclusion_text": "短期看多，突破压力位...",
        "in_pool": true
      }
    ],
    "total": 42,
    "page": 1,
    "page_size": 20,
    "pages": 3
  }
}
```

### §2.2 精选研报

```
GET /api/v1/reports/featured?limit=5
```

**认证**：无需  
**查询参数**：`limit`（默认 5，最大 20）  
**响应**：`data` 为研报简要对象数组（字段同 §2.1 列表条目）

### §2.3 生成研报

```
POST /api/v1/reports/generate
```

**认证**：无需（内部服务可直接调用）  
**请求体**：

```json
{
  "stock_code": "600519.SH",
  "run_mode": "daily",
  "trade_date": "2026-04-22",
  "idempotency_key": "client-uuid-optional",
  "source": "real"
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `stock_code` | StockCode | ✅ | `^\d{6}\.(SH\|SZ)$` |
| `run_mode` | string | — | `daily`（默认）或 `hourly` |
| `trade_date` | string | — | `YYYY-MM-DD`；默认最新交易日 |
| `idempotency_key` | string | — | 幂等键，相同键不重复生成 |
| `source` | string | — | `real`（默认）或 `test` |

**响应** `200 OK`（幂等重用）或 `202 Accepted`（新任务提交）：

```json
{
  "success": true,
  "data": {
    "report_id": "rpt-uuid",
    "task_id": "task-uuid",
    "status": "PENDING | RUNNING | COMPLETED | FAILED",
    "quality_flag": "ok",
    "stock_code": "600519.SH",
    "trade_date": "2026-04-22"
  }
}
```

**错误**：

| HTTP | error_code | 条件 |
|------|-----------|------|
| 422 | `INVALID_PAYLOAD` | stock_code 格式错误 |
| 409 | `IDEMPOTENCY_CONFLICT` | 相同幂等键正在进行中 |
| 503 | `NOT_IN_CORE_POOL` | 股票不在核心池且 skip_pool_check=false |
| 503 | `DEPENDENCY_NOT_READY` | 上游数据未就绪 |

### §2.4 研报详情

```
GET /api/v1/reports/{report_id}
```

**认证**：可选  
**路径参数**：`report_id`（UUID 字符串）

**访问控制**：

- `access_state=not_found` → 404
- `access_state=hidden_by_viewer_cutoff` → 403 `REPORT_NOT_AVAILABLE`（Free 用户只能看最近 N 天）
- `access_state=visible` → 200

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "report_id": "rpt-uuid",
    "stock_code": "600519.SH",
    "stock_name": "贵州茅台",
    "trade_date": "2026-04-22",
    "recommendation": "BUY",
    "confidence": 0.85,
    "strategy_type": "A",
    "market_state": "BULL",
    "quality_flag": "ok",
    "run_mode": "daily",
    "source": "real",
    "published": true,
    "publish_status": "PUBLISHED",
    "created_at": "2026-04-22T06:30:00+00:00",
    "conclusion_text": "短期看多...",
    "reasoning_chain_md": "## 分析过程\n...",
    "llm_actual_model": "gpt-4o",
    "llm_provider_name": "newapi",
    "risk_audit_status": "PASS | WARN | BLOCK | SKIPPED",
    "risk_audit_skip_reason": null,
    "content_json": {},
    "evidence_items": ["..."],
    "analysis_steps": ["..."],
    "direction_forecast": {},
    "performance_kpi": {},
    "sim_positions": ["..."]
  }
}
```

> `evidence_items` 结构见 §12.1；`analysis_steps` 见 §12.2；`direction_forecast` 见 §13.1；`performance_kpi` 见 §13.2。

**已知问题（N-08）**：UNPUBLISHED 研报当前可通过此端点访问（200 返回，字段为空），应返回 403/404，待修复。

### §2.5 研报高级区

```
GET /api/v1/reports/{report_id}/advanced
```

**认证**：必须登录（未登录 → 401 `UNAUTHORIZED`）  
**访问控制**：Free 用户可查看摘要，高级区内容按 tier 控制

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "report_id": "rpt-uuid",
    "advanced_available": true,
    "data_lineage": ["..."],
    "prompt_snapshot": "...",
    "generation_log": ["..."],
    "degraded_datasets": [],
    "llm_token_usage": {
      "prompt_tokens": 3200,
      "completion_tokens": 800,
      "total_tokens": 4000
    }
  }
}
```

### §2.6 研报反馈（新版）

```
POST /api/v1/reports/{report_id}/feedback
```

**认证**：可选  
**请求体**：

```json
{
  "feedback_type": "direction | data | logic | other",
  "is_helpful": true,
  "comment": "可选，最多200字"
}
```

**响应** `200 OK`：

```json
{
  "success": true,
  "data": { "feedback_id": "fb-uuid", "report_id": "rpt-uuid" }
}
```

### §2.7 股票搜索

```
GET /api/v1/stocks?q=茅台&limit=20
GET /api/v1/stocks/autocomplete?q=600519&limit=10
```

**认证**：无需

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "items": [
      {
        "stock_code": "600519.SH",
        "stock_name": "贵州茅台",
        "market": "SH",
        "in_pool": true
      }
    ],
    "total": 1
  }
}
```

### §2.8 热门股票

```
GET /api/v1/hot-stocks?limit=10
```

**认证**：无需  
**查询参数**：`limit`（默认 10，最大 50）

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "items": [
      {
        "rank": 1,
        "stock_code": "600519.SH",
        "stock_name": "贵州茅台",
        "heat_score": 9.8,
        "source": "weibo | douyin | xueqiu | kuaishou"
      }
    ],
    "trade_date": "2026-04-22",
    "total": 10
  }
}
```

### §2.9 预测提取

```
POST /api/v1/predictions/extract
```

**认证**：无需  
**请求体**：

```json
{
  "report_id": "rpt-uuid",
  "stock_code": "600519.SH"
}
```

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "report_id": "rpt-uuid",
    "stock_code": "600519.SH",
    "direction_forecast": {},
    "extracted_at": "2026-04-22T06:30:00+00:00"
  }
}
```

### §2.10 预测结算

```
POST /api/v1/predictions/settle
```

**请求体**：

```json
{
  "report_id": "rpt-uuid",
  "stock_code": "600519.SH",
  "windows": [1, 7, 14, 30, 60]
}
```

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "report_id": "rpt-uuid",
    "settled_windows": [1, 7],
    "skipped_windows": [14, 30, 60],
    "results": {
      "1": { "actual_return_pct": 2.3, "direction_hit": true },
      "7": { "actual_return_pct": -1.1, "direction_hit": false }
    }
  }
}
```

### §2.11 预测统计

```
GET /api/v1/predictions/stats
```

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "total_predictions": 120,
    "direction_hit_rate": 0.62,
    "avg_return_1d": 0.8,
    "avg_return_7d": 2.1,
    "sample_size_note": "基于120笔已结算预测"
  }
}
```

### §2.12 平台配置

```
GET /api/v1/platform/config
GET /api/v1/platform/plans
```

**认证**：无需

**`/platform/config` 响应**：

```json
{
  "success": true,
  "data": {
    "app_name": "A股研报平台",
    "features": { "sim_enabled": true, "billing_enabled": false },
    "capital_tiers": { "10k": {}, "100k": {}, "500k": {} }
  }
}
```

**`/platform/plans` 响应**：

```json
{
  "success": true,
  "data": {
    "plans": [
      { "tier_id": "Pro", "name": "专业版", "price_monthly": 99, "features": ["..."] }
    ]
  }
}
```

### §2.13 用户收藏（占位功能）

```
GET    /api/v1/user/favorites
POST   /api/v1/user/favorites/{id}
DELETE /api/v1/user/favorites/{id}
```

**状态**：所有端点返回 `feature_coming_soon`，暂未实现实际逻辑。

### §2.14 市场概览（兼容旧路由）

```
GET /api/v1/market-overview
```

兼容旧路由，返回与 `GET /api/v1/market/state` 相同数据结构。

### §2.15 研报反馈（LEGACY）

```
POST /api/v1/report-feedback
```

LEGACY 路由，功能与 §2.6 相同；接受相同请求体，返回相同结构。

---

## §3 认证 API

前缀无 `/api/v1/`，直接挂在 `/auth/*`。

### §3.1 登录

```
POST /auth/login
```

**认证**：无需  
**限流**：5 次失败 / 10 分钟 / IP  
**请求体**：

```json
{
  "account": "user@example.com 或 13800138000",
  "email": "同 account（别名）",
  "password": "plaintext-password"
}
```

> `account` 与 `email` 任选其一；支持邮箱或11位手机号。

**成功响应** `200 OK`（同时设置 `access_token` Cookie）：

```json
{
  "success": true,
  "data": {
    "access_token": "<JWT>",
    "refresh_token": "<JWT>",
    "expires_in": 86400,
    "user_id": "user-uuid",
    "id": "user-uuid",
    "email": "user@example.com",
    "phone": "13800138000",
    "nickname": "张三",
    "role": "user",
    "tier": "Free",
    "membership_level": "free",
    "tier_expires_at": null,
    "membership_expires_at": null,
    "email_verified": true,
    "permissions": { "can_view_sim": false, "can_export": false }
  }
}
```

**错误**：

| HTTP | error_code | 条件 |
|------|-----------|------|
| 401 | `UNAUTHORIZED` | 账号或密码错误 |
| 401 | `EMAIL_NOT_VERIFIED` | 邮箱未激活（启用邮箱验证时） |
| 429 | `RATE_LIMITED` | 登录尝试过于频繁 |
| 503 | `UPSTREAM_TIMEOUT` | 数据库不可用 |

### §3.2 注册

```
POST /auth/register
```

**认证**：无需  
**请求体**：

```json
{
  "account": "user@example.com 或 13800138000",
  "email": "同 account（别名）",
  "password": "password123",
  "nickname": "张三"
}
```

**密码规则**：长度 ≥ 8 位，且同时含字母和数字

**成功响应** `201 Created`（同时设置 Cookie）：

```json
{
  "success": true,
  "data": {
    "access_token": "<JWT>",
    "refresh_token": "<JWT>",
    "expires_in": 86400,
    "user_id": "user-uuid",
    "id": "user-uuid",
    "email": "user@example.com",
    "phone": null,
    "nickname": "张三",
    "role": "user",
    "tier": "Free",
    "membership_level": "free",
    "tier_expires_at": null,
    "membership_expires_at": null,
    "email_verified": false,
    "permissions": { "can_view_sim": false },
    "message": "注册成功，请完成邮箱激活后再登录。"
  }
}
```

**错误**：

| HTTP | 说明 |
|------|------|
| 400 | 邮箱/手机号格式错误 |
| 400 | 密码强度不足 |
| 409 | 邮箱已被注册 |

### §3.3 登出

```
POST /auth/logout
```

**认证**：可选（无 token 也可调用）

清除 Cookie，内存撤销 access + refresh token。

**响应** `200 OK`：

```json
{ "success": true, "data": { "ok": true } }
```

### §3.4 刷新 Token

```
POST /auth/refresh
```

**认证**：无需（以 refresh_token 换取新 token pair）  
**请求体**：

```json
{ "refresh_token": "<refresh-JWT>" }
```

**Token Rotation 机制**：

- 每个 refresh_token 只能使用一次
- 重复使用（replay）在 60s 宽限期内：返回 401，不撤销
- 超过 60s：撤销该用户所有 token（全设备登出）+ 返回 401
- 成功后自动设置新 Cookie

**成功响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "access_token": "<new-JWT>",
    "refresh_token": "<new-refresh-JWT>",
    "expires_in": 86400
  }
}
```

**错误**：`401 UNAUTHORIZED`（token 无效/已使用/已过期）

### §3.5 邮箱激活

```
GET /auth/activate?token=<activation-token>
```

**认证**：无需

- 成功（Accept: text/html）：302 重定向到 `/login?activated=1`
- 成功（Accept: application/json）：`{"data": {"message": "email_activated"}}`
- 失败：400 `INVALID_PAYLOAD`

### §3.6 当前用户信息

```
GET /auth/me
```

**认证**：必须登录（未登录 → 401）

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "id": "user-uuid",
    "user_id": "user-uuid",
    "email": "user@example.com",
    "nickname": "张三",
    "role": "user",
    "tier": "Free",
    "membership_level": "free",
    "tier_expires_at": null,
    "membership_expires_at": null,
    "email_verified": true,
    "permissions": { "can_view_sim": false, "can_export": false }
  }
}
```

### §3.7 OAuth 相关

```
GET /auth/oauth/providers
GET /auth/oauth/authorize?provider=qq|wechat
POST /auth/oauth/{provider}/start
GET /auth/oauth/exchange     → 410 ROUTE_RETIRED
```

**`/auth/oauth/providers` 响应**：

```json
{
  "success": true,
  "data": {
    "providers": [
      {
        "id": "qq",
        "name": "QQ 登录",
        "start_path": "/auth/oauth/qq/start",
        "method": "POST"
      }
    ],
    "has_providers": true
  }
}
```

> `/auth/oauth/exchange` 已废弃，返回 410 `ROUTE_RETIRED`。

---

## §4 看板与首页 API

前缀：`/api/v1`

### §4.1 首页综合数据

```
GET /api/v1/home
```

**认证**：可选

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "trade_date": "2026-04-22",
    "market_state": "BULL",
    "featured_reports": ["..."],
    "hot_stocks": ["..."],
    "pool_size": 50,
    "today_reports": 42,
    "data_status": "READY | COMPUTING | DEGRADED",
    "status_reason": null
  }
}
```

**已知问题（N-07）**：`trade_date` 可能指向旧缓存日期（如 4-07），与 `/market/state` 的 `trade_date` 不同步，待修复。

### §4.2 Dashboard 统计

```
GET /api/v1/dashboard/stats?window_days=30
```

**认证**：无需  
**查询参数**：`window_days`（枚举：`1 | 7 | 14 | 30 | 60 | 90`；其他值 → 422）

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "window_days": 30,
    "total_reports": 120,
    "buy_signals": 48,
    "sell_signals": 12,
    "hold_signals": 60,
    "direction_hit_rate": 0.63,
    "avg_confidence": 0.74,
    "quality_breakdown": {
      "ok": 110,
      "stale_ok": 8,
      "degraded": 2,
      "missing": 0
    },
    "generated_at": "2026-04-22T10:00:00+00:00"
  }
}
```

**已知问题（N-09）**：当前 `total_reports` 可能返回 0，与 `/api/v1/reports` 列表数量不一致，待修复。

### §4.3 股票池列表

```
GET /api/v1/pool/stocks?trade_date=2026-04-22
```

**认证**：无需  
**查询参数**：`trade_date`（可选，默认最新已完成任务对应日期）

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "trade_date": "2026-04-22",
    "total": 50,
    "items": [
      { "stock_code": "600519.SH", "stock_name": "贵州茅台" }
    ]
  }
}
```

### §4.4 模拟仓看板

```
GET /api/v1/portfolio/sim-dashboard?capital_tier=100k
```

**认证**：必须登录 + 付费用户（Free → 403 `TIER_NOT_AVAILABLE`；admin 豁免）  
**查询参数**：`capital_tier`（`10k | 100k | 500k`，默认 `100k`）

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "capital_tier": "100k",
    "summary": {
      "total_closed": 80,
      "win_rate": 0.61,
      "pnl_ratio": 1.82,
      "open_positions": 5,
      "drawdown_state": "NORMAL | DRAWDOWN | MAX_DRAWDOWN",
      "period_start": "2026-01-01",
      "period_end": "2026-04-22",
      "cold_start": false,
      "est_days_to_30": 0
    },
    "by_strategy": {
      "A": { "total": 30, "win_rate": 0.67, "pnl_ratio": 2.1, "note": null },
      "B": { "total": 25, "win_rate": 0.56, "pnl_ratio": 1.6, "note": null },
      "C": { "total": 25, "win_rate": 0.60, "pnl_ratio": 1.7, "note": null }
    },
    "open_positions": ["..."],
    "recent_closed": ["..."]
  }
}
```

---

## §5 管理员 API

前缀：`/api/v1/admin`  
**认证**：所有端点需 `role=admin` 或 `role=super_admin`；未登录 → 401；权限不足 → 403

### §5.1 管理员概览

```
GET /api/v1/admin/overview
```

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "pool_size": 50,
    "today_reports": 42,
    "today_buy_signals": 18,
    "pending_review": 3,
    "active_positions": { "10k": 5, "100k": 8, "500k": 3 },
    "scheduler_last_run": "2026-04-22T06:00:00+00:00",
    "scheduler_last_run_status": "SUCCESS",
    "latest_trade_date": "2026-04-22",
    "pipeline_stages": {
      "fr01_stock_pool": { "status": "SUCCESS", "started_at": "...", "completed_at": "...", "error": null },
      "fr04_data_collect": { "status": "SUCCESS", "started_at": "...", "completed_at": "...", "error": null },
      "fr05_market_state": { "status": "SUCCESS", "started_at": "...", "completed_at": "...", "error": null },
      "fr06_report_gen": { "status": "RUNNING", "started_at": "...", "completed_at": null, "error": null },
      "fr07_settlement": { "status": "NOT_RUN", "started_at": null, "completed_at": null, "error": null },
      "fr08_sim_trade": { "status": "NOT_RUN", "started_at": null, "completed_at": null, "error": null },
      "fr13_event_notify": { "status": "NOT_RUN", "started_at": null, "completed_at": null, "error": null }
    },
    "report_generation": {
      "total": 42,
      "pool_size": 50,
      "progress_pct": 84.0,
      "by_strategy": { "A": 20, "B": 12, "C": 10 }
    },
    "source_dates": {
      "runtime_trade_date": "2026-04-22",
      "kline_trade_date": "2026-04-21",
      "market_state_trade_date": "2026-04-22"
    },
    "data_freshness": {
      "latest_kline_date": "2026-04-21",
      "latest_market_state_date": "2026-04-22"
    },
    "llm_health": { "status": "ok", "provider": "codex_api", "reason": null },
    "db_statistics": { "total_tables": 32, "app_user_count": 15, "report_count": 280 }
  }
}
```

### §5.2 股票池刷新

```
POST /api/v1/admin/pool/refresh
```

**请求体**：

```json
{
  "trade_date": "2026-04-22",
  "force_rebuild": false
}
```

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "task_id": "task-uuid",
    "status": "COMPLETED | FALLBACK | COLD_START_BLOCKED",
    "pool_size": 50,
    "trade_date": "2026-04-22"
  }
}
```

**错误**：

| HTTP | error_code | 条件 |
|------|-----------|------|
| 409 | `CONCURRENT_CONFLICT` | 另一个刷新任务正在运行 |
| 500 | `COLD_START_ERROR` | 冷启动数据不可用 |

### §5.3 调度器状态

```
GET /api/v1/admin/scheduler/status?page=1&page_size=20
```

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "items": [
      {
        "run_id": "run-uuid",
        "task_name": "fr01_stock_pool",
        "trade_date": "2026-04-22",
        "status": "SUCCESS | RUNNING | FAILED",
        "started_at": "2026-04-22T06:00:00+00:00",
        "finished_at": "2026-04-22T06:05:00+00:00",
        "error_message": null
      }
    ],
    "total": 120,
    "page": 1,
    "page_size": 20,
    "pages": 6
  }
}
```

### §5.4 Cookie 会话管理

```
POST /api/v1/admin/cookie-session          → 201
GET  /api/v1/admin/cookie-session/health
```

**POST 请求体**：

```json
{
  "login_source": "weibo | douyin | xueqiu | kuaishou",
  "cookie_string": "raw-cookie-header-value"
}
```

**POST 响应** `201 Created`：

```json
{
  "success": true,
  "data": {
    "cookie_session_id": "cs-uuid",
    "login_source": "weibo",
    "status": "ACTIVE",
    "created_at": "2026-04-22T10:00:00+00:00"
  }
}
```

**GET 查询参数**：`login_source`（必填，`weibo|douyin|xueqiu|kuaishou`）、`session_id`（可选）

**GET 响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "login_source": "weibo",
    "status": "ACTIVE | EXPIRING | EXPIRED | REFRESH_FAILED | SKIPPED",
    "last_probe_at": "2026-04-22T09:50:00+00:00",
    "last_refresh_at": "2026-04-22T08:00:00+00:00",
    "expires_at": "2026-04-25T00:00:00+00:00"
  }
}
```

**错误**：`404 NOT_FOUND`（会话不存在）

### §5.5 结算任务提交

```
POST /api/v1/admin/settlement/run
```

**请求体**：

```json
{
  "trade_date": "2026-04-22",
  "window_days": 14,
  "target_scope": "all | single_report | single_stock",
  "target_report_id": "rpt-uuid",
  "target_stock_code": "600519.SH",
  "force": false
}
```

**响应** `202 Accepted`：

```json
{
  "success": true,
  "data": {
    "task_id": "settle-uuid",
    "status": "ACCEPTED | RUNNING",
    "trade_date": "2026-04-22",
    "target_scope": "all"
  }
}
```

### §5.6 研报字段修改（PATCH）

```
PATCH /api/v1/admin/reports/{report_id}
```

**请求体**（所有字段可选，仅修改指定字段）：

```json
{
  "published": true,
  "publish_status": "PUBLISHED | UNPUBLISHED | DRAFT_GENERATED",
  "quality_flag": "ok | stale_ok | degraded | missing",
  "review_flag": "PENDING_REVIEW | REVIEWED | FLAGGED",
  "risk_audit_status": "PASS | WARN | BLOCK | SKIPPED",
  "conclusion_text": "修订后的结论文本"
}
```

**响应** `200 OK`：修改后的研报摘要对象  
**错误**：`404 NOT_FOUND` / `422 INVALID_PAYLOAD`

### §5.7 强制重新生成（super_admin）

```
POST /api/v1/admin/reports/{report_id}/force-regenerate
```

**认证**：需精确 `role=super_admin`

**请求体**：

```json
{
  "force_regenerate": true,
  "reason_code": "data_correction"
}
```

> `force_regenerate` 必须字面量 `true`，否则 422。

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "old_report_id": "rpt-old-uuid",
    "new_report_id": "rpt-new-uuid",
    "status": "COMPLETED",
    "reason_code": "data_correction"
  }
}
```

**错误**：

| HTTP | error_code | 条件 |
|------|-----------|------|
| 409 | `REPORT_ALREADY_REFERENCED_BY_SIM` | 研报已被模拟仓或结算引用 |
| 503 | `DEPENDENCY_NOT_READY` | 生成依赖数据不可用 |

### §5.8 计费对账（exact_admin）

```
POST /api/v1/admin/billing/orders/{order_id}/reconcile
```

**认证**：需精确 `role=admin`（super_admin **不满足**此条件）

**请求体**：

```json
{
  "provider": "wechat | alipay | mock",
  "order_id": "ord-uuid",
  "expected_tier": "Pro | Enterprise",
  "reason_code": "manual_payment_confirm"
}
```

> `body.order_id` 必须与路径参数 `order_id` 一致，否则 422。

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "order_id": "ord-uuid",
    "order_status": "PAID",
    "granted_tier": "Pro",
    "membership_status": "active",
    "user_tier": "Pro",
    "user_tier_expires_at": "2027-04-22T00:00:00+00:00"
  }
}
```

---

## §6 内部 API

前缀：`/api/v1/internal`  
**认证**：`X-Internal-Token: <token>` 头（值来自 `settings.internal_api_token`）  
**用途**：仅供服务间调用，不对外暴露

### §6.1 热点采集

```
POST /api/v1/internal/hotspot/collect
POST /api/v1/internal/hotspot/enrich
GET  /api/v1/internal/hotspot/health
```

**`/hotspot/collect` 请求体**：

```json
{ "source": "weibo | douyin | xueqiu | kuaishou", "force": false }
```

**`/hotspot/health` 响应**：

```json
{
  "success": true,
  "data": {
    "status": "ok | degraded",
    "last_fetch": "2026-04-22T09:30:00+00:00",
    "sources": [
      {
        "source_name": "weibo",
        "freshness": "fresh | degraded | stale",
        "age_hours": 0.5,
        "last_fetch": "2026-04-22T09:30:00+00:00"
      }
    ]
  }
}
```

### §6.1-b 个股非研报补采

```
POST /api/v1/internal/stocks/{stock_code}/non-report-data/collect?trade_date=YYYY-MM-DD
```

**用途**：只补 truth-layer 与 lineage，**不生成研报**、不创建结算。  
**路径参数**：`stock_code` 必须匹配 `^\d{6}\.(SH|SZ|BJ)$`。  
**查询参数**：`trade_date` 可选；不传则使用系统最新交易日。

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "stock_code": "600519.SH",
    "trade_date": "2026-04-24",
    "market_state_input": {
      "usage_id": "uuid",
      "batch_id": "uuid",
      "status": "ok | degraded",
      "reason": null,
      "market_state_trade_date": "2026-04-24"
    },
    "capital_usage": {
      "batch_id": "uuid",
      "per_dataset": {
        "main_force_flow": { "persisted_status": "ok | proxy_ok | stale_ok | missing" },
        "dragon_tiger_list": { "persisted_status": "ok | missing" },
        "margin_financing": { "persisted_status": "ok | proxy_ok | realtime_only | stale_ok | missing" }
      }
    },
    "stock_profile": {
      "batch_id": "uuid",
      "persisted_status": "ok | stale_ok | missing",
      "reason": null
    },
    "northbound_summary": {
      "status": "ok | missing | degraded",
      "reason": "akshare_stock_hsgt_individual_em | northbound_data_unavailable | ...",
      "batch_id": "uuid"
    },
    "etf_flow_summary": {
      "status": "ok | missing | degraded",
      "reason": "akshare_fund_etf_daily | no_etf_data_available | ...",
      "batch_id": "uuid"
    }
  }
}
```

**边界说明**：

- 该接口的职责是补齐 `data_batch / data_batch_lineage / report_data_usage / data_usage_fact`；
- `report_data_usage_link` 只会在真正生成了 `report` 后出现，当前库若 `report=0` 则 `link=0` 属真实状态；
- `market_snapshot/company_overview/valuation/news_policy` 等详情页 read-through 数据不属于本接口返回的 truth-layer 落库范围。
- 核心池批量回放当前走服务/调度 handler：`materialize_non_report_usage_for_pool()` / `scheduler._handler_fr05_non_report_truth_materialize()`；**当前没有单独的 pool-level HTTP API**，避免误解为“调用该接口会顺带生成研报”。

### §6.2 Cookie 刷新

```
POST /api/v1/internal/cookie/refresh?platform=weibo|douyin|eastmoney
```

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "platform": "weibo",
    "status": "ACTIVE | REFRESH_FAILED | SKIPPED",
    "status_reason": null,
    "last_probe_at": "2026-04-22T09:50:00+00:00",
    "last_refresh_at": "2026-04-22T08:00:00+00:00",
    "expires_at": "2026-04-25T00:00:00+00:00"
  }
}
```

### §6.3 数据源熔断状态

```
GET /api/v1/internal/source/fallback-status
```

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "hotspot_chain": "public+browser_fallback",
    "market_chain": "eastmoney+tdx+fallback",
    "status": "normal | degraded",
    "status_reason": null,
    "circuits": [
      {
        "source_name": "eastmoney",
        "circuit_state": "CLOSED | OPEN | HALF_OPEN",
        "consecutive_failures": 0,
        "cooldown_until": null,
        "runtime_circuit_open": false
      }
    ],
    "runtime": {}
  }
}
```

### §6.4 LLM 健康检查与版本

```
GET /api/v1/internal/llm/health
GET /api/v1/internal/llm/version
```

**`/llm/health` 响应**：

```json
{
  "success": true,
  "data": {
    "status": "ok | degraded",
    "tags": ["gpt-4o", "gpt-3.5-turbo"]
  }
}
```

**`/llm/version` 响应**：

```json
{
  "success": true,
  "data": {
    "test_model": "gpt-3.5-turbo",
    "prod_model": "gpt-4o"
  }
}
```

### §6.5 批量生成研报

```
POST /api/v1/internal/reports/generate-batch
```

**请求体**：

```json
{
  "stock_codes": ["600519.SH", "000858.SZ"],
  "trade_date": "2026-04-22",
  "skip_pool_check": false,
  "force": false,
  "cleanup_incomplete_before_batch": true,
  "cleanup_limit": 500,
  "include_non_ok": true,
  "max_concurrent": null
}
```

> `stock_codes` 最多 50 个。

**响应** `202 Accepted`：

```json
{
  "success": true,
  "message": "batch_accepted",
  "data": {
    "submitted": 2,
    "succeeded": 0,
    "failed": 0,
    "tasks": [
      { "stock_code": "600519.SH", "task_id": "task-uuid", "status": "PENDING" }
    ],
    "cleanup_incomplete_before_batch": { "soft_deleted": 0, "candidates": 0 }
  }
}
```

### §6.6 清理不完整研报

```
POST /api/v1/internal/reports/cleanup-incomplete
POST /api/v1/internal/reports/cleanup-incomplete-all
GET  /api/v1/internal/reports/incomplete-status?limit=500
```

**`/cleanup-incomplete` 请求体**：

```json
{
  "limit": 500,
  "dry_run": false,
  "include_non_ok": true
}
```

**`/incomplete-status` 响应**：

```json
{
  "success": true,
  "data": {
    "all_reports_complete": true,
    "incomplete_candidates": 0,
    "scanned": 1500,
    "reason": null,
    "candidate_examples": []
  }
}
```

### §6.7 任务状态查询

```
GET /api/v1/internal/reports/tasks/{task_id}
```

**响应** `200 OK`（字段见 §11.5）  
**错误**：`404 TASK_NOT_FOUND`

### §6.8 运行时指标

```
GET /api/v1/internal/metrics/summary
```

返回 LLM 调用量、错误率、各数据源成功率等运行时监控指标。

### §6.9 内部控制平面：运行时门禁

`GET /api/v1/internal/runtime/gates`

**用途**：给自治修复、共享产物晋级与 Ralph/Issue Mesh 控制平面提供同一份运行态门禁事实。
**认证**：`X-Internal-Token`；仅内部服务调用。

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "status": "ready | degraded | blocked",
    "runtime_live_recovery": {
      "allowed": true,
      "blocking_flags": []
    },
    "shared_artifact_promote": {
      "allowed": true,
      "blind_spot_clean": true,
      "continuous_audit_complete": true,
      "artifacts_same_round": true
    },
    "llm_router": {
      "ready": true,
      "status": "ok | degraded | unconfigured"
    },
    "artifacts": {}
  },
  "degraded": false,
  "degraded_reason": null
}
```

**字段约束**：

- `data.status: string`：`ready` 表示 runtime 与共享产物均可晋级；`degraded` 表示 runtime 可用但共享产物未完全满足；`blocked` 表示运行态恢复门禁阻塞。
- `data.runtime_live_recovery` / `data.runtime_live_recovery: object`：运行态恢复门禁，`allowed=false` 时必须给出 `blocking_flags`。
- `data.shared_artifact_promote` / `data.shared_artifact_promote: object`：共享产物晋级门禁，必须同时覆盖盲点审计、连续审计、同轮产物一致性与 runtime gate。
- `data.llm_router` / `data.llm_router: object`：LLM 路由健康快照，`ready=true` 仅代表主路由状态为 `ok`。
- `degraded_reason=runtime_live_recovery_blocked`：仅当 `data.status=blocked` 时出现。

### §6.10 内部控制平面：审计上下文

`GET /api/v1/internal/audit/context`

**用途**：为自治审计与修复链路提供运行态门禁、文档锚点、Issue Mesh/Code Fix 最近运行快照。
**认证**：`X-Internal-Token`；仅内部服务调用。

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "runtime_gates": {
      "status": "ready | degraded | blocked",
      "runtime_live_recovery": { "allowed": true, "blocking_flags": [] },
      "shared_artifact_promote": { "allowed": true },
      "llm_router": { "status": "ok | degraded | unconfigured" }
    },
    "latest_published_report_trade_date": "2026-04-22",
    "public_runtime_status": "ok | degraded | unknown",
    "docs": {
      "progress_doc_path": "docs/core/22_全量功能进度总表_v7_精审.md",
      "analysis_lens_doc_path": "docs/core/25_系统问题分析角度清单.md"
    },
    "automation": {
      "loop_controller": {},
      "latest_issue_mesh_run": null,
      "latest_code_fix_wave": null,
      "promote_readiness": { "status": "promote_ready | blocked" }
    }
  },
  "degraded": false,
  "degraded_reason": null
}
```

**字段约束**：

- `data.runtime_gates: object`：与 `GET /api/v1/internal/runtime/gates` 的核心门禁语义一致，但为审计上下文压缩形态。
- `data.public_runtime_status: string|null`：公开运行态状态，来自 runtime metrics；不可用时允许为 `null` 或 `unknown`。
- `progress_doc_path: string`：当前进度 SSOT 文档路径。
- `analysis_lens_doc_path: string`：问题分析角度 SSOT 文档路径。
- `data.automation.promote_readiness.status: string`：`ready` 门禁映射为 `promote_ready`，其他状态映射为 `blocked`。

### §6.11 已废弃路由（410）

```
POST /api/v1/internal/llm/generate         → 410 ROUTE_RETIRED
POST /api/v1/internal/reports/generate     → 410 ROUTE_RETIRED
POST /api/v1/internal/reports/clear        → 410 ROUTE_RETIRED
POST /api/v1/internal/stats/clear          → 410 ROUTE_RETIRED
```

---

## §7 计费 API

前缀：`/billing`（不含 `/api/v1/`）

### §7.1 创建订单

```
POST /billing/create_order
```

**认证**：必须登录  
**请求体**：

```json
{
  "tier_id": "Pro",
  "period_months": 1,
  "provider": "wechat | alipay | mock"
}
```

**响应** `201 Created`：

```json
{
  "success": true,
  "data": {
    "order_id": "ord-uuid",
    "tier_id": "Pro",
    "period_months": 1,
    "amount": 99.00,
    "currency": "CNY",
    "provider": "wechat",
    "pay_url": "weixin://...",
    "status": "PENDING",
    "expires_at": "2026-04-22T10:30:00+00:00"
  }
}
```

**错误**：

| HTTP | error_code | 条件 |
|------|-----------|------|
| 409 | `TIER_ALREADY_ACTIVE` | 用户当前 tier 未过期 |
| 503 | `PAYMENT_PROVIDER_NOT_CONFIGURED` | 支付提供商未配置 |

### §7.2 支付回调 Webhook

```
POST /billing/webhook
```

**认证**：`Webhook-Signature` 头签名校验

**请求体**：

```json
{
  "event_id": "evt-uuid",
  "order_id": "ord-uuid",
  "user_id": "user-uuid",
  "tier_id": "Pro",
  "paid_amount": 99.00,
  "provider": "wechat",
  "signature": "hex-string"
}
```

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "event_id": "evt-uuid",
    "order_id": "ord-uuid",
    "processed": true
  }
}
```

**错误**：

| HTTP | error_code | 条件 |
|------|-----------|------|
| 400 | `PAYMENT_SIGNATURE_INVALID` | 签名校验失败 |
| 422 | `INVALID_PAYLOAD` | 字段校验失败 |

### §7.3 已废弃路由（410）

```
GET  /billing/mock-pay/{order_id}         → 410 ROUTE_RETIRED
POST /billing/mock-pay/{order_id}/confirm → 410 ROUTE_RETIRED
```

---

## §8 模拟仓 API

前缀：`/api/v1/sim`  
**认证**：所有端点需登录 + 付费会员；admin/super_admin 豁免  
**Free 用户**：→ 403 `TIER_NOT_AVAILABLE`

### §8.1 持仓查询

```
GET /api/v1/sim/positions
```

**查询参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `source` | string | `live`（默认）或 `backtest` |
| `stock_code` | string | 精确股票代码 |
| `status` | string | `OPEN\|CLOSED_SL\|CLOSED_T1\|CLOSED_T2\|CLOSED_EXPIRED` |
| `date_from` | string | 开仓日期下界 `YYYY-MM-DD` |
| `date_to` | string | 开仓日期上界 `YYYY-MM-DD` |
| `page` | int | 默认 1 |
| `page_size` | int | 默认 50，最大 100 |

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "items": [
      {
        "position_id": "pos-uuid",
        "report_id": "rpt-uuid",
        "stock_code": "600519.SH",
        "stock_name": "贵州茅台",
        "strategy_type": "A",
        "signal_date": "2026-04-15",
        "sim_open_date": "2026-04-16",
        "sim_open_price": 1750.00,
        "actual_entry_price": 1752.50,
        "sim_qty": 57,
        "capital_tier": "100k",
        "stop_loss_price": 1700.00,
        "target_price_1": 1820.00,
        "target_price_2": 1900.00,
        "valid_until": "2026-04-30",
        "status": "OPEN",
        "sim_close_date": null,
        "sim_close_price": null,
        "sim_pnl_gross": null,
        "sim_pnl_net": null,
        "sim_pnl_pct": null,
        "hold_days": 6,
        "hold_days_max": 15,
        "days_until_expire": 14,
        "execution_blocked": false
      }
    ],
    "total": 5,
    "page": 1,
    "page_size": 50,
    "pages": 1
  }
}
```

### §8.2 单笔持仓详情

```
GET /api/v1/sim/positions/{position_id}
```

**响应**：同 §8.1 条目字段；**错误**：`404 NOT_FOUND`

### §8.3 研报对应持仓

```
GET /api/v1/sim/positions/by-report/{report_id}
```

**响应**：持仓数组（同 §8.1 条目）

### §8.4 账户日度快照

```
GET /api/v1/sim/account/snapshots
```

**查询参数**：`capital_tier`（默认 `100k`）、`date_from`、`date_to`、`page`（默认 1）、`page_size`（默认 100，最大 200）

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "items": [
      {
        "snapshot_date": "2026-04-22",
        "capital_tier": "100k",
        "nav": 105200.00,
        "cash": 42000.00,
        "position_value": 63200.00,
        "drawdown_state": "NORMAL",
        "open_positions": 3,
        "total_pnl_net": 5200.00
      }
    ],
    "total": 90,
    "page": 1,
    "page_size": 100,
    "pages": 1
  }
}
```

---

## §9 治理与特征目录 API

### §9.1 特征目录（admin）

```
GET /api/v1/features/catalog
GET /api/v1/governance/catalog   （alias）
```

**认证**：需 admin  
**查询参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `fr_id` | string | FR 编号过滤（如 `FR-06`） |
| `visibility` | string | `public\|internal\|deprecated` |
| `status` | string | `active\|warn\|missing` |
| `source` | string | `live`（从代码实时计算）或 `snapshot`（从 JSON 缓存） |

**响应** `200 OK`：

```json
{
  "success": true,
  "data": {
    "catalog": ["..."],
    "status_summary": { "active": 120, "warn": 56, "missing": 5 },
    "denominator_summary": { "total": 181, "covered": 176 },
    "feature_traceability_summary": {},
    "generated_at": "2026-04-22T10:00:00+00:00"
  }
}
```

---

## §10 类型冻结（所有枚举）

> 以下枚举值为系统唯一来源，代码修改前必须先更新本节。

### §10.1 研报相关枚举

| 枚举名 | 允许值 | 说明 |
|--------|--------|------|
| `Recommendation` | `BUY \| SELL \| HOLD` | 研报结论 |
| `StrategyType` | `A \| B \| C` | 策略类型（A=成长，B=价值，C=趋势） |
| `RunMode` | `daily \| hourly` | 研报生成频率 |
| `Source` | `real \| test` | 数据来源类型 |
| `QualityFlag` | `ok \| stale_ok \| missing \| degraded` | 研报质量标志 |
| `PublishStatus` | `DRAFT_GENERATED \| PUBLISHED \| UNPUBLISHED` | 发布状态 |
| `ReviewFlag` | `PENDING_REVIEW \| REVIEWED \| FLAGGED` | 人工审核状态 |
| `RiskAuditStatus` | `PASS \| WARN \| BLOCK \| SKIPPED` | 风险审计结果 |

### §10.2 市场状态枚举

| 枚举名 | 允许值 |
|--------|--------|
| `MarketState` | `BULL \| NEUTRAL \| BEAR` |
| `DataStatus` | `READY \| COMPUTING \| DEGRADED` |

### §10.3 用户枚举

| 枚举名 | 允许值 | 说明 |
|--------|--------|------|
| `UserRole` | `user \| admin \| super_admin` | 角色 |
| `UserTier` | `Free \| Pro \| Enterprise` | 会员层级（首字母大写） |
| `MembershipLevel` | `free \| monthly \| annual` | 会员类型（全小写） |

### §10.4 模拟仓枚举

| 枚举名 | 允许值 |
|--------|--------|
| `CapitalTier` | `10k \| 100k \| 500k`（兼容别名：`1w \| 10w \| 50w`） |
| `SimPositionStatus` | `OPEN \| CLOSED_SL \| CLOSED_T1 \| CLOSED_T2 \| CLOSED_EXPIRED` |
| `DrawdownState` | `NORMAL \| DRAWDOWN \| MAX_DRAWDOWN` |

### §10.5 任务与基础设施枚举

| 枚举名 | 允许值 |
|--------|--------|
| `TaskStatus` | `PENDING \| RUNNING \| COMPLETED \| FAILED \| EXPIRED` |
| `LLMFallbackLevel` | `primary \| backup \| cli \| local \| failed` |
| `CookieSessionStatus` | `ACTIVE \| EXPIRING \| EXPIRED \| REFRESH_FAILED \| SKIPPED` |
| `CircuitState` | `CLOSED \| OPEN \| HALF_OPEN` |
| `PoolRefreshStatus` | `COMPLETED \| FALLBACK \| COLD_START_BLOCKED` |
| `PipelineStatus` | `NOT_RUN \| PENDING \| RUNNING \| SUCCESS \| PARTIAL_SUCCESS \| FAILED` |

### §10.6 计费枚举

| 枚举名 | 允许值 |
|--------|--------|
| `OrderStatus` | `PENDING \| PAID \| FAILED \| CANCELLED \| REFUNDED` |
| `PaymentProvider` | `wechat \| alipay \| mock` |

---

## §11 业务对象契约

### §11.1 Report（研报完整 Schema）

> 对应数据库表 `report`

| 字段 | 类型 | 说明 |
|------|------|------|
| `report_id` | UUID (PK) | 主键 |
| `stock_code` | string | `^\d{6}\.(SH\|SZ\|BJ)$` |
| `stock_name_snapshot` | string \| null | 生成时股票名快照 |
| `trade_date` | date \| null | 可空（旧研报无此字段） |
| `recommendation` | Recommendation | 研报结论 |
| `confidence` | float | 置信度 0.0–1.0 |
| `quality_flag` | QualityFlag | 质量标志 |
| `status_reason` | string \| null | 状态原因 |
| `published` | bool | 是否已发布 |
| `publish_status` | PublishStatus | 发布状态 |
| `run_mode` | RunMode | 生成模式 |
| `source` | Source | 数据来源 |
| `strategy_type` | StrategyType | 策略类型 |
| `market_state` | MarketState \| null | 市场状态 |
| `content_json` | JSON \| null | 研报完整 JSON 内容 |
| `conclusion_text` | string \| null | 结论摘要文本 |
| `reasoning_chain_md` | string \| null | Markdown 格式推理链 |
| `is_deleted` | bool | 软删除标志（default false） |
| `deleted_at` | datetime \| null | 软删除时间 |
| `superseded_by_report_id` | UUID \| null | 被哪个新研报替代 |
| `llm_actual_model` | string \| null | 实际使用的 LLM 模型名 |
| `llm_provider_name` | string \| null | LLM 提供商名 |
| `llm_endpoint` | string \| null | LLM 端点 URL |
| `risk_audit_status` | RiskAuditStatus \| null | 风险审计结果 |
| `review_flag` | ReviewFlag \| null | 人工审核标志 |
| `generation_task_id` | UUID \| null | 关联生成任务 ID |
| `created_at` | datetime (UTC) | 创建时间 |
| `updated_at` | datetime (UTC) | 更新时间 |

### §11.2 User（用户完整 Schema）

> 对应数据库表 `app_user`

| 字段 | 类型 | 说明 |
|------|------|------|
| `user_id` | UUID (PK) | 主键；`.id` 属性是别名 |
| `email` | string (unique) | 邮箱（手机注册时为 `<phone>@phone.local`） |
| `phone` | string \| null (unique) | 11位手机号 |
| `password_hash` | string | bcrypt 哈希，72字节截断 |
| `nickname` | string \| null | 昵称 |
| `role` | UserRole | 角色（default: `user`） |
| `tier` | UserTier | 会员层级（default: `Free`） |
| `tier_expires_at` | datetime \| null | tier 过期时间 |
| `membership_level` | MembershipLevel | 会员类型（default: `free`） |
| `membership_expires_at` | datetime \| null | 会员过期时间 |
| `email_verified` | bool | 邮箱是否已激活（default: false） |
| `created_at` | datetime (UTC) | 注册时间 |
| `updated_at` | datetime (UTC) | 更新时间 |

> 注意：`user.id` 属性等同于 `user.user_id`，代码中两者均有使用。

### §11.3 SimPosition（模拟仓位 Schema）

> 对应数据库表 `sim_position`；回测版对应 `sim_position_backtest`

| 字段 | 类型 | 说明 |
|------|------|------|
| `position_id` | UUID (PK) | 主键；`.id` 属性是别名 |
| `report_id` | UUID (FK→report) | 关联研报 |
| `stock_code` | string | 股票代码 |
| `stock_name` | string \| null | 股票名 |
| `strategy_type` | StrategyType | 策略类型 |
| `capital_tier` | CapitalTier | 资金档位 |
| `signal_date` | date | 信号日 |
| `sim_open_date` | date | 模拟开仓日 |
| `sim_open_price` | decimal | 模拟开仓价 |
| `actual_entry_price` | decimal \| null | 实际成交价（回填） |
| `sim_qty` | int | 模拟持仓数量（股） |
| `stop_loss_price` | decimal | 止损价 |
| `target_price_1` | decimal \| null | 目标价1 |
| `target_price_2` | decimal \| null | 目标价2 |
| `valid_until` | date | 持仓有效期（到期强制平仓） |
| `position_status` | SimPositionStatus | 持仓状态；`.status` 属性是别名 |
| `sim_close_date` | date \| null | 模拟平仓日 |
| `sim_close_price` | decimal \| null | 模拟平仓价 |
| `sim_pnl_gross` | decimal \| null | 毛盈亏（元） |
| `sim_pnl_net` | decimal \| null | 净盈亏（含手续费，元） |
| `sim_pnl_pct` | decimal \| null | 净盈亏百分比（小数，如 0.023） |
| `hold_days` | int \| null | 持仓天数 |
| `execution_blocked` | bool | 执行是否被阻断（default: false） |
| `source` | string \| null | 仅 backtest 表有此字段 |
| `created_at` | datetime (UTC) | — |
| `updated_at` | datetime (UTC) | — |

### §11.4 BillingOrder（计费订单）

| 字段 | 类型 | 说明 |
|------|------|------|
| `order_id` | UUID (PK) | 主键 |
| `user_id` | UUID (FK→app_user) | 用户 |
| `provider` | PaymentProvider | 支付提供商 |
| `tier_id` | string | 目标 tier 名 |
| `expected_tier` | UserTier | 期望提升到的 tier |
| `granted_tier` | UserTier \| null | 实际授予的 tier |
| `period_months` | int | 购买月数 |
| `amount` | decimal | 支付金额（元） |
| `status` | OrderStatus | 订单状态 |
| `event_id` | string \| null | webhook 唯一事件 ID |
| `paid_at` | datetime \| null | 支付时间 |
| `created_at` | datetime (UTC) | 创建时间 |

### §11.5 ReportGenerationTask（研报生成任务）

| 字段 | 类型 | 说明 |
|------|------|------|
| `task_id` | UUID (PK) | 主键 |
| `stock_code` | string | 股票代码 |
| `trade_date` | date | 交易日 |
| `idempotency_key` | string \| null | 幂等键 |
| `generation_seq` | int | 同一 stock_code+trade_date 的序号 |
| `status` | TaskStatus | 任务状态 |
| `retry_count` | int | 重试次数（default: 0） |
| `quality_flag` | QualityFlag \| null | 生成结果质量 |
| `status_reason` | string \| null | 状态原因 |
| `llm_fallback_level` | LLMFallbackLevel \| null | LLM 降级级别 |
| `risk_audit_status` | RiskAuditStatus \| null | 风险审计结果 |
| `risk_audit_skip_reason` | string \| null | 跳过审计的原因 |
| `request_id` | UUID | 关联请求 ID |
| `queued_at` | datetime \| null | 入队时间 |
| `started_at` | datetime \| null | 开始执行时间 |
| `finished_at` | datetime \| null | 完成时间 |
| `updated_at` | datetime (UTC) | 更新时间 |

---

## §12 证据与推理链契约

### §12.1 evidence_items（数据证据列表）

`content_json.evidence_items` 为数组，每条记录必须包含：

```json
{
  "dataset_name": "kline_daily",
  "display_name": "日线K线",
  "data_status": "READY | DEGRADED | MISSING",
  "freshness": "fresh | stale | missing",
  "trade_date": "2026-04-22",
  "sample_rows": 240,
  "key_metrics": {
    "close": 1750.0,
    "volume": 12800000,
    "ma5": 1720.0,
    "ma20": 1680.0
  },
  "degraded_reason": null
}
```

**必填字段**：`dataset_name`、`data_status`

**常见 dataset_name 值**：

| 值 | 说明 |
|----|------|
| `kline_daily` | 日线 K 线 |
| `kline_weekly` | 周线 K 线 |
| `kline_monthly` | 月线 K 线 |
| `market_state` | 市场状态 |
| `hot_stocks` | 热门股票数据 |
| `fundamentals` | 基本面数据 |
| `report_history` | 历史研报 |

### §12.2 analysis_steps（推理链步骤）

`content_json.analysis_steps` 为数组，每条记录结构：

```json
{
  "step_no": 1,
  "step_name": "技术面分析",
  "conclusion": "60日均线上方，MACD金叉，短期趋势偏强",
  "confidence": 0.78,
  "data_sources": ["kline_daily", "kline_weekly"],
  "weight": 0.35,
  "degraded": false,
  "degraded_reason": null
}
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `step_no` | int | 步骤编号，从 1 开始 |
| `step_name` | string | 步骤中文名称 |
| `conclusion` | string | 该步骤结论（中文） |
| `confidence` | float | 该步骤置信度 0.0–1.0 |
| `data_sources` | string[] | 使用的数据集列表 |
| `weight` | float | 该步骤在综合判断中的权重 |
| `degraded` | bool | 该步骤是否因数据降级影响结论 |
| `degraded_reason` | string \| null | 降级原因描述 |

**降级展示规则**：若 `degraded=true`，前端必须显示明确的降级说明文本，不得隐藏或替换为"无数据"。

---

## §13 指标与预测契约

### §13.1 direction_forecast（方向预测）

```json
{
  "direction": "UP | DOWN | NEUTRAL",
  "confidence": 0.72,
  "predicted_return_pct": 3.5,
  "time_horizon_days": 14,
  "basis": "技术面突破+市场环境偏多",
  "stop_loss_trigger_pct": -5.0,
  "upside_target_pct": 8.0
}
```

> **注意**：`direction` 仅作监控指标，不作为交易信号。交易信号使用 `recommendation`（§10.1）。

### §13.2 performance_kpi（四维度绩效）

```json
{
  "win_rate": 0.61,
  "win_rate_sample_size": 80,
  "win_rate_coverage_pct": 89.0,
  "pnl_ratio": 1.82,
  "pnl_ratio_sample_size": 80,
  "annual_alpha": 0.128,
  "annual_alpha_benchmark": "hs300",
  "direction_hit_rate": 0.63,
  "direction_hit_rate_note": "仅监控，非交易信号",
  "cold_start": false,
  "cold_start_note": null,
  "computed_at": "2026-04-22T10:00:00+00:00"
}
```

**商业底线要求**（达标才能对外展示）：

| 指标 | 最低要求 |
|------|---------|
| `win_rate` | ≥ 55%（且样本量 ≥ 30 笔） |
| `pnl_ratio` | ≥ 1.5 |
| `annual_alpha` | ≥ 10%（0.10） |

**冷启动期（`cold_start=true`）**：样本量 < 30 时，`win_rate`、`pnl_ratio` 显示为 `null`，前端展示冷启动提示，禁止显示虚假统计数据。

---

## §14 响应约束

### §14.1 分页规则

- 参数：`page`（≥1，默认 1）、`page_size`（1–100，默认 20）
- 响应体包含：`items[]`、`total`、`page`、`page_size`、`pages`（总页数）
- 超出 total 的页码：返回空 `items[]`，不报错
- 兼容旧参数 `limit`（当 `page_size` 未指定时作为 `page_size` 处理）

### §14.2 排序规则

- 参数：`sort=field`（升序）或 `sort=-field`（降序）
- 研报列表允许排序字段：`created_at`（默认 `-created_at`）、`trade_date`、`confidence`、`recommendation`
- 非白名单字段 → 422 `INVALID_PAYLOAD`

### §14.3 过滤规则

- 字符串过滤：精确匹配（`stock_code`、`trade_date`）或模糊匹配（`q`、`stock_name`）
- 日期区间：`date_from`（含）+ `date_to`（含）
- 枚举过滤：必须是 §10 中定义的合法枚举值
- **超额参数保护**：接口内部使用 `_ensure_allowed_query_params()` 机制，不在白名单内的查询参数 → 422 `INVALID_PAYLOAD`

---

## §15 错误码词典

### §15.1 HTTP 状态码语义

| HTTP | 含义 |
|------|------|
| `200 OK` | 成功 |
| `201 Created` | 资源创建成功 |
| `202 Accepted` | 异步任务已提交 |
| `204 No Content` | 成功但无响应体 |
| `302 Found` | 重定向 |
| `400 Bad Request` | 请求参数格式错误 |
| `401 Unauthorized` | 未认证或 token 无效 |
| `403 Forbidden` | 已认证但权限不足 |
| `404 Not Found` | 资源不存在 |
| `409 Conflict` | 冲突（重复/并发/引用约束） |
| `410 Gone` | 路由已废弃 |
| `422 Unprocessable Entity` | 请求体语义校验失败 |
| `429 Too Many Requests` | 请求频率超限 |
| `500 Internal Server Error` | 服务器内部未预期错误 |
| `503 Service Unavailable` | 上游依赖不可用 |

### §15.2 业务错误码（error_code 字段）

| error_code | 典型 HTTP | 触发场景 |
|-----------|----------|---------|
| `UNAUTHORIZED` | 401 | 未登录、token 无效、账号密码错误 |
| `EMAIL_NOT_VERIFIED` | 401 | 邮箱未激活即尝试登录 |
| `FORBIDDEN` | 403 | 无操作权限（角色不符） |
| `TIER_NOT_AVAILABLE` | 403 | 付费功能，当前 tier 不满足 |
| `REPORT_NOT_AVAILABLE` | 403/404 | 研报存在但 viewer 无权访问 |
| `NOT_FOUND` | 404 | 通用资源不存在 |
| `TASK_NOT_FOUND` | 404 | 任务 ID 不存在 |
| `IDEMPOTENCY_CONFLICT` | 409 | 相同幂等键的任务正在进行 |
| `CONCURRENT_CONFLICT` | 409 | 同类任务并发冲突 |
| `TIER_ALREADY_ACTIVE` | 409 | 用户已有未过期的相同 tier |
| `REPORT_ALREADY_REFERENCED_BY_SIM` | 409 | 研报已被模拟仓/结算引用，不可重生成 |
| `ROUTE_RETIRED` | 410 | 路由已废弃 |
| `INVALID_PAYLOAD` | 422 | 请求体字段校验失败 |
| `INVALID_STOCK_CODE` | 400/422 | 股票代码格式错误 |
| `INVALID_PROVIDER` | 400 | OAuth provider 不合法 |
| `RATE_LIMITED` | 429 | 超过请求频率限制（登录/API） |
| `DATA_SOURCE_UNAVAILABLE` | 503 | 外部数据源不可用 |
| `LLM_ALL_FAILED` | 503 | 所有 LLM 后端均失败 |
| `DEPENDENCY_NOT_READY` | 503 | 上游数据依赖未就绪 |
| `NOT_IN_CORE_POOL` | 503 | 股票不在核心池，无法生成研报 |
| `COLD_START_ERROR` | 500 | 冷启动时缺少必要数据 |
| `UPSTREAM_TIMEOUT` | 503 | 上游服务（DB/外部API）超时 |
| `CIRCUIT_BREAKER_OPEN` | 503 | 熔断器开启，拒绝请求 |
| `STALE_TASK_EXPIRED` | 409 | 任务已过期，需重新提交 |
| `PAYMENT_SIGNATURE_INVALID` | 400 | 支付 Webhook 签名校验失败 |
| `PAYMENT_PROVIDER_NOT_CONFIGURED` | 503 | 支付提供商未配置密钥 |
| `VALIDATION_FAILED` | 422 | 通用数据校验失败 |
| `INTERNAL_ERROR` | 500 | 未预期的服务器内部错误 |

---

## §16 Web 页面路由（HTML 入口）

以下路由返回 HTML，不包含 JSON 响应体。

| 路径 | 说明 |
|------|------|
| `/` | 首页（`index.html`） |
| `/reports` | 研报列表页（`reports_list.html`） |
| `/reports/list` | 旧路径，302 重定向到 `/reports` |
| `/reports/{report_id}` | 研报详情页（`report_view.html`） |
| `/login` | 登录页 |
| `/register` | 注册页 |
| `/dashboard` | 数据看板页 |
| `/admin` | 管理后台页 |
| `/report/{stock_code}` | 旧路径：有研报则 302 重定向，否则 404 HTML |
| `/report/实时研报/{stock_code}` | 旧路径（GBK 路径兼容），重定向到详情页 |
| `/report/{code}/status` | LEGACY JSON 端点：返回研报状态 |
| `/demo-pages/*` | 静态 Demo 页面（`/demo/` 目录） |
| `/static/*` | 静态资源（CSS、JS、图片） |
| `/favicon.ico` | 204 No Content |

---

## 附录 A：Pydantic Schema 参考

### A.1 StockCode

```python
StockCode = Annotated[str, StringConstraints(pattern=r"^\d{6}\.(SH|SZ)$")]
# 内部路由（BJ 北交所）: pattern=r"^\d{6}\.(SH|SZ|BJ)$"
```

### A.2 GenerateReportRequest

```python
class GenerateReportRequest(BaseModel):
    stock_code: StockCode
    run_mode: Literal["daily", "hourly"] = "daily"
    trade_date: str | None = None         # YYYY-MM-DD
    idempotency_key: str | None = None
    source: Literal["real", "test"] = "real"
```

### A.3 ReportFeedbackRequest

```python
class ReportFeedbackRequest(BaseModel):
    report_id: str | None = None
    is_helpful: bool
    feedback_type: Literal["direction", "data", "logic", "other"] | None = None
    comment: str | None = Field(None, max_length=200)
```

### A.4 PredictionSettleRequest

```python
class PredictionSettleRequest(BaseModel):
    report_id: str
    stock_code: StockCode
    windows: list[int] = [1, 7, 14, 30, 60]
```

### A.5 BillingCreateOrderV2Request

```python
class BillingCreateOrderV2Request(BaseModel):
    tier_id: str
    period_months: int
    provider: str
```

### A.6 BillingWebhookRequest

```python
class BillingWebhookRequest(BaseModel):
    event_id: str
    order_id: str
    user_id: str
    tier_id: str
    paid_amount: float
    provider: str
    signature: str | None = None
```

### A.7 ForceRegenerateRequest

```python
class ForceRegenerateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    force_regenerate: bool = Field(...)   # 必须字面量 True
    reason_code: str = Field(..., min_length=1)
```

### A.8 BillingReconcileRequest

```python
class BillingReconcileRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    provider: str
    order_id: str   # 必须与路径参数一致
    expected_tier: str = Field(..., pattern=r"^(Pro|Enterprise)$")
    reason_code: str = Field(..., min_length=1)
```

---

## 附录 B：AI 网关路由

以下路由挂载自 `ai-api/` 子目录，提供 Web-based AI 接口代理（无需 API Key）：

| 前缀 | 模块 | 说明 |
|------|------|------|
| `/gemini/*` | `gemini_web.py` | Google Gemini Web API |
| `/chatgpt/*` | `chatgpt_web.py` | ChatGPT/NewAPI Web API |
| `/deepseek/*` | `deepseek_web.py` | DeepSeek Web API |
| `/qwen/*` | `qwen_web.py` | 通义千问 Web API |
| `/webai/*` | `webai.py` | 通用 Web AI 路由 |

> 详见 `docs/core/08_AI接入策略.md`。

---

*本文档从 `app/api/routes_*.py`、`app/core/security.py`、`app/main.py`、`app/models.py`、`app/schemas.py` 逆向整理生成。如发现与代码不一致，以代码为准并及时更新本文档。*
