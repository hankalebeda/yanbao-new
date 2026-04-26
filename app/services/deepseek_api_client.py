"""DeepSeek 官方 API 客户端（替代 Playwright Web 自动化）

使用 OpenAI 兼容接口，稳定性 >99.5%，成本约 ¥160/月（180M tokens）。
优先级：deepseek_api > gemini_api > ollama（本地兜底）
"""
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_DEEPSEEK_API_BASE = "https://api.deepseek.com/v1"
_DEFAULT_MODEL = "deepseek-chat"  # DeepSeek-V3
_TIMEOUT = 60.0


class DeepSeekAPIClient:
    """轻量级 DeepSeek 官方 API 客户端，OpenAI 兼容格式。"""

    def __init__(self, api_key: str, base_url: str = _DEEPSEEK_API_BASE, model: str = _DEFAULT_MODEL) -> None:
        if not api_key:
            raise ValueError("DeepSeek API key is required. Set DEEPSEEK_API_KEY in .env")
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._model = model
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(_TIMEOUT, connect=10.0),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )

    async def analyze(
        self,
        prompt: str,
        system_prompt: str = "你是A股金融分析专家，请基于提供的数据进行严谨的推理分析，输出结构化JSON。",
        temperature: float = 0.3,
        max_tokens: int = 2048,
    ) -> dict[str, Any]:
        """发送单次分析请求，返回与 Web 客户端兼容的 dict 格式。"""
        t0 = time.time()
        payload = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
        }

        try:
            resp = await self._client.post(f"{self._base_url}/chat/completions", json=payload)
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {})
            elapsed = round(time.time() - t0, 2)
            logger.info(
                "deepseek_api | ok elapsed=%.1fs in=%d out=%d",
                elapsed,
                usage.get("prompt_tokens", 0),
                usage.get("completion_tokens", 0),
            )
            return {
                "response": content,
                "elapsed_s": elapsed,
                "has_citation": False,
                "model": self._model,
                "source": "deepseek_api",
                "usage": usage,
            }
        except httpx.HTTPStatusError as exc:
            logger.error("deepseek_api | HTTP %s: %s", exc.response.status_code, exc.response.text[:200])
            raise RuntimeError(f"DeepSeek API error {exc.response.status_code}: {exc.response.text[:200]}") from exc
        except Exception as exc:
            logger.error("deepseek_api | request failed: %s", exc)
            raise RuntimeError(f"DeepSeek API request failed: {exc}") from exc

    async def analyze_with_chain_of_thought(
        self,
        prompt: str,
        system_prompt: str = "你是A股金融分析专家，请基于提供的数据进行严谨的推理分析，输出结构化JSON。",
        temperature: float = 0.3,
        max_tokens: int = 3000,
    ) -> dict[str, Any]:
        """启用 DeepSeek 推理模式（R1 系列适用，V3 也支持 chain-of-thought prompt）。"""
        cot_system = (
            system_prompt
            + "\n\n请先用<reasoning>标签展示推理过程，再输出最终JSON。"
            "推理链格式：数据解读→逻辑推断→风险评估→结论。"
        )
        return await self.analyze(prompt, cot_system, temperature, max_tokens)

    async def close(self) -> None:
        await self._client.aclose()


_instance: DeepSeekAPIClient | None = None


def get_deepseek_api_client() -> DeepSeekAPIClient:
    """获取单例客户端（懒加载）。若未配置 API key 则抛出 RuntimeError。"""
    global _instance
    if _instance is None:
        from app.core.config import settings
        if not settings.deepseek_api_key:
            raise RuntimeError(
                "DEEPSEEK_API_KEY not configured. "
                "申请地址：https://platform.deepseek.com/ "
                "费用约¥2/1M输出tokens（DeepSeek-V3）"
            )
        _instance = DeepSeekAPIClient(
            api_key=settings.deepseek_api_key,
            base_url=settings.deepseek_api_base_url,
            model=settings.deepseek_api_model,
        )
    return _instance
