"""
ai-api/gemini_web
=================
通过 Playwright 驱动本机已登录的 Chrome，调用 Gemini 网页端做股票分析。

对外暴露：
  router          — FastAPI APIRouter，挂载到 main.py
  GeminiWebClient — 可直接在其他 Service 里 await GeminiWebClient.get()
  shutdown        — FastAPI lifespan shutdown 钩子
"""

from .client import GeminiWebClient
from .client import shutdown as shutdown
from .router import router

__all__ = ["router", "GeminiWebClient", "shutdown"]
