# Gemini Web — AI API 模块

通过 Playwright 驱动本机已登录的 Chrome，调用 Gemini 网页端做联网股票分析。

## 目录结构

```
ai-api/gemini_web/
├── __init__.py     对外暴露：router / GeminiWebClient / shutdown
├── client.py       核心：GeminiWebClient 单例 + 浏览器自动化
├── schemas.py      Pydantic 请求体：AnalyzeRequest / BatchRequest
├── router.py       FastAPI APIRouter（prefix: /api/v1/gemini）
├── test.py         集成测试（需要本机 Chrome 已登录 Gemini）
└── README.md       本文档
```

## 环境要求

```bash
pip install playwright
playwright install chrome
# 本机 Chrome 已登录 gemini.google.com（必须）
```

可选 `.env` 配置：

| 变量 | 默认 | 说明 |
|------|------|------|
| `GEMINI_CHROME_USER_DATA` | 当前用户 Chrome 目录 | Chrome User Data 路径 |
| `GEMINI_CHROME_PROFILE` | `Default` | Profile 子目录名 |
| `GEMINI_MAX_CONCURRENCY` | `5` | 最大并发标签数 |

## HTTP 接口

### POST `/api/v1/gemini/analyze` — 单条分析

```json
// 请求
{
  "prompt": "请联网搜索贵州茅台（600519）今日最新股价和重要新闻，给出来源链接。",
  "timeout_s": 120
}

// 响应 200
{
  "code": 0, "message": "ok",
  "data": {
    "response": "截至 2026年2月24日收盘，贵州茅台 1485.30 元...",
    "elapsed_s": 9.63,
    "has_citation": true
  }
}
```

### POST `/api/v1/gemini/analyze/batch` — 并发批量（最多 5 只）

```json
// 请求
{
  "stocks": [
    {"code": "600519", "name": "贵州茅台", "prompt": "..."},
    {"code": "000858", "name": "五粮液",   "prompt": "..."},
    {"code": "300750", "name": "宁德时代", "prompt": "..."}
  ],
  "timeout_s": 120
}

// 响应 200
{
  "code": 0, "message": "ok",
  "data": {
    "count": 3,
    "results": [
      {"code": "600519", "name": "贵州茅台", "response": "...", "elapsed_s": 18.4, "has_citation": true},
      {"code": "000858", "name": "五粮液",   "response": "...", "elapsed_s": 16.7, "has_citation": true},
      {"code": "300750", "name": "宁德时代", "response": "...", "elapsed_s": 18.7, "has_citation": true}
    ]
  }
}
```

> 某只失败时，对应结果含 `"error": "..."` 字段，不影响其他股票。

### DELETE `/api/v1/gemini/session` — 关闭会话

释放后台 Chrome 进程，下次调用时自动重新初始化。

## 错误码

| HTTP | 场景 | 处理 |
|------|------|------|
| `503` | Cookie 过期 / Google 拒绝登录 | 在本机 Chrome 重新登录 Gemini，调 `DELETE /session` 再重试 |
| `500` | Playwright 内部异常 / 页面结构变化 | 查服务日志 `gemini_web \|` |
| `422` | 参数校验失败 | `prompt` 长度 1~4000，`stocks` 数量 1~5 |

## 性能参考

| 场景 | 耗时 |
|------|------|
| 首次初始化（复制 profile + 启动 Chrome） | ~10s |
| 单条查询（含联网搜索） | 9~12s |
| 3 只并发批量 | ~19s |

## Python 内部调用

```python
from ai_api.gemini_web import GeminiWebClient

# 单条
client = await GeminiWebClient.get()
result = await client.analyze("请联网搜索贵州茅台今日消息")
print(result["response"])

# 批量并发
results = await client.analyze_batch([
    {"code": "600519", "name": "贵州茅台", "prompt": "..."},
    {"code": "300750", "name": "宁德时代", "prompt": "..."},
])
```

## 运行测试

```
python ai-api/gemini_web/test.py
```
