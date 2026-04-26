# Web AI 统一调用说明

本文定义 `chatgpt_web`、`deepseek_web`、`gemini_web`、`qwen_web` 的统一接口，供本机其他模块直接调用。

> 本部署策略：仅使用 `ai-api` 下四个 Web API + 手动登录的 Codex/Gemini CLI，不使用任何官方 API Key。

## 1. 统一入口

- 基础路径：`http://127.0.0.1:8000/api/v1/webai`
- 支持 provider：`chatgpt` / `deepseek` / `gemini` / `qwen`

## 2. 接口列表

1. `GET /api/v1/webai/providers`
2. `POST /api/v1/webai/analyze`
3. `POST /api/v1/webai/analyze/batch`
4. `GET /api/v1/webai/session/status`
5. `GET /api/v1/webai/session/status/{provider}`
6. `DELETE /api/v1/webai/session/{provider}`
7. `DELETE /api/v1/webai/session`

## 3. 请求结构

### 3.1 单条分析

`POST /api/v1/webai/analyze`

```json
{
  "provider": "chatgpt",
  "prompt": "你只输出：CASE-A001|OK",
  "timeout_s": 120
}
```

### 3.2 批量分析

`POST /api/v1/webai/analyze/batch`

```json
{
  "provider": "qwen",
  "stocks": [
    {
      "code": "600519",
      "name": "MOUTAI",
      "prompt": "你只输出：CASE-B001|MOUTAI|OK"
    },
    {
      "code": "000858",
      "name": "WULIANGYE",
      "prompt": "你只输出：CASE-B002|WULIANGYE|OK"
    }
  ],
  "timeout_s": 120
}
```

约束：
- `timeout_s`: `10..600`
- `stocks` 长度：`1..5`

## 4. 响应结构

统一 envelope：

```json
{
  "code": 0,
  "message": "ok",
  "data": {}
}
```

### 4.1 单条分析返回（`data`）

```json
{
  "provider": "chatgpt",
  "response": "CASE-A001|OK",
  "elapsed_s": 3.42,
  "has_citation": false
}
```

### 4.2 批量分析返回（`data`）

```json
{
  "provider": "qwen",
  "count": 2,
  "results": [
    {
      "code": "600519",
      "name": "MOUTAI",
      "response": "CASE-B001|MOUTAI|OK",
      "elapsed_s": 4.9,
      "has_citation": false
    },
    {
      "code": "000858",
      "name": "WULIANGYE",
      "response": "CASE-B002|WULIANGYE|OK",
      "elapsed_s": 5.1,
      "has_citation": false
    }
  ]
}
```

### 4.3 会话状态（`data`）

`GET /session/status/{provider}`：

```json
{
  "provider": "chatgpt",
  "initialized": true,
  "ready": true,
  "model_probe": {
    "raw": "GPT-5.2",
    "is_5x": true
  }
}
```

## 5. 登录与会话复用

建议：

1. 正常情况下登录一次可连续使用多天到数周，不需要每次请求都登录。
2. 仅在以下场景重新登录：
   - 返回 `503` 且提示登录态失效
   - 页面跳转到登录页
   - 输出连续出现登录/落地页文本
3. 出现异常可先调用：
   - `DELETE /api/v1/webai/session/{provider}`
   - 再重新调用 `analyze`

## 6. 持久登录（推荐，避免反复登录）

DeepSeek / Qwen 支持持久登录目录。配置后服务直接使用该目录，不再拷贝临时 profile，重启后仍可复用登录态。

`.env` 推荐：

```env
DEEPSEEK_CHROME_SERVICE_USER_DATA="C:/Users/Administrator/Desktop/AI/yanbao/runtime/web_login_profiles/deepseek"
QWEN_SERVICE_USER_DATA="C:/Users/Administrator/Desktop/AI/yanbao/runtime/web_login_profiles/qwen"
```

首次登录流程：

1. 运行：
   - `python scripts/manual_login_then_test.py --base-url http://127.0.0.1:8000 --providers deepseek,qwen`
2. 手工登录后关闭浏览器窗口。
3. 脚本会自动执行低并发真实测试。

## 7. 代理与浏览器策略

- 四个 provider 使用 Chrome。
- `qwen` / `deepseek` 默认直连优先（不走本地代理），可通过配置覆盖。
- `chatgpt` / `gemini` 保持各模块原有配置。

## 8. Python 调用示例

```python
import requests

base = "http://127.0.0.1:8000/api/v1/webai"

resp = requests.post(
    f"{base}/analyze",
    json={
        "provider": "chatgpt",
        "prompt": "你只输出：CASE-DEMO|OK",
        "timeout_s": 120,
    },
    timeout=180,
)
print(resp.json())
```
