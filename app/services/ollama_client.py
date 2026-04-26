import asyncio
import time
from typing import Any

import httpx

from app.core.config import settings


class OllamaClient:
    def __init__(self):
        self.base_url = settings.ollama_base_url.rstrip("/")

    def model_name(self, use_prod: bool = False) -> str:
        if use_prod:
            return settings.ollama_model_prod
        return settings.ollama_model_test if settings.use_test_model else settings.ollama_model_prod

    async def health(self) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{self.base_url}/api/tags")
            resp.raise_for_status()
            return resp.json()

    async def generate(
        self,
        prompt: str,
        use_prod: bool = False,
        options: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        model = self.model_name(use_prod=use_prod)
        if settings.mock_llm:
            return {
                "model": model,
                "latency_ms": 5,
                "response": "HOLD - mock llm response",
                "raw": {"mock": True},
            }
        body: dict[str, Any] = {"model": model, "prompt": prompt, "stream": False}
        if options:
            body["options"] = options
        effective_timeout = timeout or settings.request_timeout_seconds
        last_error = None
        for attempt in range(settings.max_llm_retries + 1):
            start = time.perf_counter()
            try:
                async with httpx.AsyncClient(timeout=effective_timeout) as client:
                    resp = await client.post(f"{self.base_url}/api/generate", json=body)
                    resp.raise_for_status()
                    payload = resp.json()
                    latency_ms = int((time.perf_counter() - start) * 1000)
                    return {
                        "model": model,
                        "latency_ms": latency_ms,
                        "response": payload.get("response", ""),
                        "raw": payload,
                    }
            except Exception as exc:
                last_error = str(exc)
                if attempt < settings.max_llm_retries:
                    await asyncio.sleep(2**attempt)
        raise RuntimeError(f"ollama_generate_failed: {last_error}")


ollama_client = OllamaClient()
