# ChatGPT Web — AI API 模块

通过 Playwright 驱动本机已登录的 Chrome，调用 ChatGPT 网页端做联网查询和内容分析。

> **注意：测试模型要求**  
> 按照需求，如果需要确保模型是 ChatGPT 5.x 版本或最新的旗舰版，建议您在本机开启 Chrome 访问 `https://chatgpt.com/`，将顶部模型切换按钮（如下拉框）手动选至 `GPT-5` 或最新模型即可（客户端默认会继承该界面状态使用当前选择的模型；如尚未发布GPT-5，请选用最新版的旗舰模型GPT-4/GPT-4o等）。`test.py` 内部也会通过 Prompt 询问模型其自己的真实版本号并打印以供检查确认。

## 目录结构

```
ai-api/chatgpt_web/
├── __init__.py     对外暴露：router / ChatGPTWebClient / shutdown
├── client.py       核心：ChatGPTWebClient 单例 + 浏览器自动化
├── schemas.py      Pydantic 请求体：AnalyzeRequest / BatchRequest
├── router.py       FastAPI APIRouter（prefix: /api/v1/chatgpt）
├── test.py         集成测试（需要本机 Chrome 已登录 ChatGPT）
└── README.md       本文档
```

## 环境要求

与 gemini_web 同理：
```bash
pip install playwright
playwright install chrome
# 本机 Chrome 已登录 chatgpt.com（必须）
```

可选 `.env` 配置：
| 变量 | 默认 | 说明 |
|------|------|------|
| `CHATGPT_CHROME_USER_DATA` | 继承 gemini 或 当前用户 Chrome 目录 | Chrome User Data 路径 |
| `CHATGPT_CHROME_PROFILE` | `Default` | Profile 子目录名 |
| `CHATGPT_MAX_CONCURRENCY` | `5` | 最大并发标签数 |

## HTTP 接口

与 `/api/v1/gemini` 保持一致，但前缀改为 `/api/v1/chatgpt`：
- `POST /api/v1/chatgpt/analyze` (单条分析)
- `POST /api/v1/chatgpt/analyze/batch` (并发批量)
- `DELETE /api/v1/chatgpt/session` (关闭会话释放资源)

## 运行测试

打开 CMD 直接运行以下集成测试脚本。它会在控制台展示模型自我识别的版本回答：
```
python ai-api/chatgpt_web/test.py
```
