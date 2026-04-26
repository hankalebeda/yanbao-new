from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.11+ ships tomllib
    import tomli as tomllib  # type: ignore[no-redef]

from app.core.config import settings

logger = logging.getLogger(__name__)

_TIMEOUT = 300.0
_RETRYABLE_REASONING_STATUS_CODES = {400, 422}
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _parse_sse_chat_response(body: str) -> dict[str, Any] | None:
    """Parse SSE (Server-Sent Events) streaming response from chat completions.

    Some gateways return SSE streaming format even when stream=False is requested.
    This function reconstitutes a standard chat completion object from SSE chunks.
    Returns None if parsing fails or body is not SSE format.
    """
    if not body.strip().startswith("data:"):
        return None
    content_parts: list[str] = []
    reasoning_parts: list[str] = []
    usage: dict[str, Any] = {}
    model: str = ""
    finish_reason: str | None = None
    for raw_line in body.split("\n"):
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        data_str = line[5:].strip()
        if data_str == "[DONE]":
            break
        try:
            chunk = json.loads(data_str)
        except (json.JSONDecodeError, ValueError):
            continue
        if not model:
            model = chunk.get("model", "")
        if chunk.get("usage"):
            usage = chunk["usage"]
        for choice in chunk.get("choices", []):
            delta = choice.get("delta", {})
            if delta.get("content"):
                content_parts.append(delta["content"])
            if delta.get("reasoning") or delta.get("reasoning_content"):
                reasoning_parts.append(delta.get("reasoning") or delta.get("reasoning_content") or "")
            if choice.get("finish_reason"):
                finish_reason = choice["finish_reason"]
    content = "".join(content_parts) or "".join(reasoning_parts)
    if not content and finish_reason not in ("stop", "length", None):
        return None
    return {
        "choices": [{"message": {"content": content}, "finish_reason": finish_reason}],
        "model": model,
        "usage": usage,
    }


@dataclass(frozen=True, slots=True)
class CodexProviderSpec:
    provider_name: str
    base_url: str
    api_key: str
    model: str
    review_model: str | None = None
    fallback_model: str | None = None
    fallback_models: tuple[str, ...] = ()
    wire_api: str = "responses"
    reasoning_effort: str = "high"


def _normalize_provider_name(name: str | None) -> str:
    return "".join(ch for ch in str(name or "").strip().lower() if ch.isalnum())


def _normalize_provider_endpoint(value: str | None) -> str:
    parsed = urlsplit(str(value or "").strip())
    return _normalize_provider_name(parsed.netloc or parsed.path or value)


def _normalize_model_name(model: str | None) -> str:
    return str(model or "").strip().lower()


def _is_reasoning_model(model: str | None) -> bool:
    return _normalize_model_name(model).startswith("gpt-5")


# v26 P0: 严格允许名单。生成研报只允许 gpt-5.x 族，
# 拒绝 deepseek/qwen/glm/kimi 等其他模型垃圾进入高品质研报主链。
_ALLOWED_MODEL_PREFIXES = ("gpt-5.4", "gpt-5.3", "gpt-5.2", "gpt-5")


def _model_in_allowlist(model: str | None) -> bool:
    n = _normalize_model_name(model)
    if not n:
        return False
    return any(n.startswith(p) for p in _ALLOWED_MODEL_PREFIXES)


def _provider_priority(spec: CodexProviderSpec) -> tuple[int, str]:
    normalized = _normalize_model_name(spec.model)
    if normalized.startswith("gpt-5.4"):
        return 0, spec.provider_name.lower()
    if normalized.startswith("gpt-5.3"):
        return 1, spec.provider_name.lower()
    if normalized.startswith("gpt-5.2"):
        return 2, spec.provider_name.lower()
    return 3, spec.provider_name.lower()


def _codex_pool_level(model: str | None) -> str:
    normalized = _normalize_model_name(model)
    if normalized.startswith("gpt-5.4"):
        return "primary"
    if normalized.startswith("gpt-5"):
        return "backup"
    return "primary"


def _resolve_provider_root(root: str | Path | None = None) -> Path:
    raw = Path(root or settings.codex_provider_root)
    return raw if raw.is_absolute() else (_REPO_ROOT / raw).resolve()


def _load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning("codex_api | failed to parse %s: %s", path, exc)
        return {}


def _load_toml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning("codex_api | failed to parse %s: %s", path, exc)
        return {}


def _load_api_key(provider_dir: Path) -> str:
    key_path = provider_dir / "key.txt"
    if key_path.exists():
        lines = [line.strip() for line in key_path.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
        if lines:
            if len(lines) >= 2 and lines[0].startswith("http"):
                return lines[1]
            return lines[0]

    auth = _load_json_file(provider_dir / "auth.json")
    for key_name in ("OPENAI_API_KEY", "api_key", "apiKey"):
        value = str(auth.get(key_name) or "").strip()
        if value:
            return value
    return ""


def _load_key_base_url(provider_dir: Path) -> str:
    key_path = provider_dir / "key.txt"
    if not key_path.exists():
        return ""
    lines = [line.strip() for line in key_path.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
    if len(lines) >= 2 and lines[0].startswith("http"):
        return lines[0].rstrip("/")
    return ""


def _normalize_fallback_models(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    normalized: list[str] = []
    for item in value:
        clean = str(item or "").strip()
        if clean and clean not in normalized:
            normalized.append(clean)
    return tuple(normalized)


def _build_provider_spec(provider_dir: Path) -> CodexProviderSpec | None:
    metadata = _load_json_file(provider_dir / "provider.json")
    if metadata and metadata.get("enabled") is False:
        return None

    config = _load_toml_file(provider_dir / "config.toml")
    provider_cfg = config.get("model_providers", {}).get("OpenAI", {})

    base_url = str(provider_cfg.get("base_url") or _load_key_base_url(provider_dir) or metadata.get("endpoint") or "").strip().rstrip("/")
    api_key = _load_api_key(provider_dir)
    # Prefer the provider's local config.toml model selection over provider.json.
    # Some relay directories intentionally keep provider.json as a generic/default
    # fallback while config.toml carries the operator-approved live model.
    model = str(config.get("model") or metadata.get("model") or "").strip()
    review_model = str(config.get("review_model") or metadata.get("review_model") or "").strip() or None
    fallback_model = str(settings.codex_api_fallback_model or "").strip() or None
    fallback_models = _normalize_fallback_models(metadata.get("fallback_models") or config.get("fallback_models"))
    wire_api = str(settings.codex_wire_api or provider_cfg.get("wire_api") or "responses").strip() or "responses"
    reasoning_effort = str(config.get("model_reasoning_effort") or "high").strip().lower() or "high"
    provider_name = str(metadata.get("name") or provider_dir.name).strip() or provider_dir.name

    if not (base_url and api_key and model):
        return None

    return CodexProviderSpec(
        provider_name=provider_name,
        base_url=base_url,
        api_key=api_key,
        model=model,
        review_model=review_model,
        fallback_model=fallback_model,
        fallback_models=fallback_models,
        wire_api=wire_api,
        reasoning_effort=reasoning_effort,
    )


def discover_codex_provider_specs(root: str | Path | None = None) -> list[CodexProviderSpec]:
    provider_root = _resolve_provider_root(root)
    if not provider_root.exists():
        return []

    specs: list[CodexProviderSpec] = []
    for provider_dir in provider_root.iterdir():
        if not provider_dir.is_dir():
            continue
        if provider_dir.name.startswith("portable_") or provider_dir.name == "__pycache__":
            continue
        spec = _build_provider_spec(provider_dir)
        if spec is not None:
            specs.append(spec)
    specs.sort(key=_provider_priority)
    return specs


def discover_audit_codex_provider_specs(root: str | Path | None = None) -> list[CodexProviderSpec]:
    discovered = discover_codex_provider_specs(root)
    if not settings.codex_audit_gateway_only:
        return _merge_with_settings_fallback(discovered)

    canonical = _normalize_provider_name(settings.codex_canonical_provider)
    if not canonical:
        return _merge_with_settings_fallback(discovered)

    filtered = [
        spec
        for spec in discovered
        if _normalize_provider_name(spec.provider_name) == canonical
        or _normalize_provider_endpoint(spec.base_url) == canonical
    ]
    return filtered


def _settings_fallback_provider() -> CodexProviderSpec | None:
    base_url = str(settings.codex_api_base_url or "").strip().rstrip("/")
    api_key = str(settings.codex_api_key or "").strip()
    model = str(settings.codex_api_model or "").strip()
    if not (base_url and api_key and model):
        return None
    return CodexProviderSpec(
        provider_name="settings_fallback",
        base_url=base_url,
        api_key=api_key,
        model=model,
        review_model=None,
        fallback_model=str(settings.codex_api_fallback_model or "").strip() or None,
        fallback_models=(),
        wire_api=str(settings.codex_wire_api or "responses").strip() or "responses",
        reasoning_effort=str(settings.codex_api_reasoning_effort or "high").strip().lower() or "high",
    )


def _merge_with_settings_fallback(discovered: list[CodexProviderSpec]) -> list[CodexProviderSpec]:
    fallback = _settings_fallback_provider()
    if fallback is None:
        return discovered

    fallback_key = (fallback.base_url.rstrip("/"), _normalize_model_name(fallback.model))
    # v20: put settings_fallback AFTER discovered provider specs so healthy
    # cliproxy-style discovered providers are tried first; settings_fallback
    # (often pointing at a legacy/dead gateway via env defaults) only runs if
    # all discovered providers fail. Deduplicate on (base_url, model) so we
    # don't double-register the same endpoint.
    merged: list[CodexProviderSpec] = []
    for spec in discovered:
        spec_key = (spec.base_url.rstrip("/"), _normalize_model_name(spec.model))
        if spec_key == fallback_key:
            continue
        merged.append(spec)
    merged.append(fallback)
    return merged


def model_candidates(
    primary_model: str,
    *,
    review_model: str | None = None,
    fallback_models: tuple[str, ...] = (),
    fallback_model: str | None = None,
) -> list[str]:
    candidates: list[str] = []
    ordered = [primary_model, review_model, *fallback_models, fallback_model]
    for model in ordered:
        normalized = str(model or "").strip()
        if not normalized or normalized in candidates:
            continue
        # v26 P0: 拒绝进入不在允许名单的模型（如 deepseek/qwen/glm/kimi）
        if not _model_in_allowlist(normalized):
            logger.warning("codex_api | model_blocked=%s (not in gpt-5 allowlist)", normalized)
            continue
        candidates.append(normalized)
    return candidates


def reasoning_effort_candidates(model: str, configured_effort: str | None = None) -> list[str]:
    normalized = _normalize_model_name(model)
    configured = str(configured_effort or "").strip().lower()
    candidates: list[str] = []

    if (
        normalized.startswith("gpt-5.4")
        or normalized.startswith("gpt-5.3")
        or normalized.startswith("gpt-5.2")
    ):
        # Respect explicitly configured effort (e.g. "low" for batch mode).
        # Only escalate to xhigh/high when no effort is configured.
        if configured and configured not in ("xhigh", "high"):
            # Explicit low/medium: use it first, then escalate as fallback
            for effort in (configured, "high", "xhigh"):
                if effort and effort not in candidates:
                    candidates.append(effort)
        else:
            for effort in ("xhigh", "high", configured):
                if effort and effort not in candidates:
                    candidates.append(effort)
        return candidates or ["high"]

    if configured:
        candidates.append(configured)
    if "high" not in candidates:
        candidates.append("high")
    return candidates


class CodexAPIClient:
    """OpenAI-compatible client for the local ai-api/codex provider pool."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str | None = None,
        wire_api: str | None = None,
        provider_root: str | Path | None = None,
        provider_specs: list[CodexProviderSpec] | None = None,
    ) -> None:
        if provider_specs is not None:
            self._providers = provider_specs
        elif api_key and base_url:
            self._providers = [
                CodexProviderSpec(
                    provider_name="manual_override",
                    base_url=base_url.rstrip("/"),
                    api_key=api_key,
                    model=model or settings.codex_api_model,
                    review_model=None,
                    fallback_model=str(settings.codex_api_fallback_model or "").strip() or None,
                    fallback_models=(),
                    wire_api=wire_api or getattr(settings, "codex_wire_api", "responses"),
                    reasoning_effort=str(settings.codex_api_reasoning_effort or "high").strip().lower() or "high",
                )
            ]
        else:
            discovered = discover_codex_provider_specs(provider_root)
            self._providers = _merge_with_settings_fallback(discovered)

        if not self._providers:
            raise ValueError(
                "No Codex provider is available. "
                "Expected ai-api/codex/* provider dirs or CODEX_API_* fallback settings."
            )

        self._clients: dict[str, httpx.AsyncClient] = {}
        self._provider_cursor_by_priority: dict[int, int] = {}
        self._provider_cooldown_until: dict[str, float] = {}

    @property
    def providers(self) -> list[CodexProviderSpec]:
        return list(self._providers)

    def _provider_bucket(self, provider: CodexProviderSpec) -> int:
        return _provider_priority(provider)[0]

    def _provider_in_cooldown(self, provider: CodexProviderSpec, now: float | None = None) -> bool:
        current = time.monotonic() if now is None else now
        return self._provider_cooldown_until.get(provider.provider_name, 0.0) > current

    def _mark_provider_failed(self, provider: CodexProviderSpec) -> None:
        cooldown_seconds = max(0, int(settings.codex_provider_failure_cooldown_seconds))
        if cooldown_seconds <= 0:
            return
        self._provider_cooldown_until[provider.provider_name] = time.monotonic() + cooldown_seconds

    def _mark_provider_healthy(self, provider: CodexProviderSpec) -> None:
        self._provider_cooldown_until.pop(provider.provider_name, None)

    def _provider_order(self) -> list[CodexProviderSpec]:
        buckets: dict[int, list[CodexProviderSpec]] = {}
        for provider in self._providers:
            bucket = self._provider_bucket(provider)
            buckets.setdefault(bucket, []).append(provider)

        now = time.monotonic()
        fresh_order: list[CodexProviderSpec] = []
        cooled_order: list[CodexProviderSpec] = []
        any_fresh = False

        for bucket in sorted(buckets):
            providers = buckets[bucket]
            start = self._provider_cursor_by_priority.get(bucket, 0) % len(providers)
            rotated = providers[start:] + providers[:start]
            self._provider_cursor_by_priority[bucket] = (start + 1) % len(providers)

            fresh = [provider for provider in rotated if not self._provider_in_cooldown(provider, now)]
            cooled = [provider for provider in rotated if self._provider_in_cooldown(provider, now)]
            if fresh:
                any_fresh = True
                fresh_order.extend(fresh)
            cooled_order.extend(cooled)

        if any_fresh:
            return fresh_order + cooled_order

        ordered: list[CodexProviderSpec] = []
        for bucket in sorted(buckets):
            providers = buckets[bucket]
            start = (self._provider_cursor_by_priority.get(bucket, 1) - 1) % len(providers)
            ordered.extend(providers[start:] + providers[:start])
        return ordered

    def _get_client(self, provider: CodexProviderSpec) -> httpx.AsyncClient:
        client = self._clients.get(provider.provider_name)
        if client is None or client.is_closed:
            timeout_seconds = max(30.0, float(getattr(settings, "codex_api_timeout_seconds", _TIMEOUT)))
            client = httpx.AsyncClient(
                timeout=httpx.Timeout(timeout_seconds, connect=15.0),
                trust_env=False,  # 强制直连，绕过系统代理（如 10808）
                headers={
                    "Authorization": f"Bearer {provider.api_key}",
                    "Content-Type": "application/json",
                },
            )
            self._clients[provider.provider_name] = client
        return client

    async def analyze(
        self,
        prompt: str,
        system_prompt: str = (
            "你是A股金融分析专家，请基于提供的数据进行严谨的推理分析，"
            "并输出结构化 JSON。"
        ),
        temperature: float = 0.3,
        max_tokens: int = 4096,
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        providers = self._provider_order()
        logger.info(
            "codex_api | provider_order=%s",
            [provider.provider_name for provider in providers],
        )
        for provider in providers:
            try:
                if provider.wire_api == "responses":
                    result = await self._analyze_responses(provider, prompt, system_prompt, temperature, max_tokens)
                else:
                    result = await self._analyze_chat(provider, prompt, system_prompt, temperature, max_tokens)
                self._mark_provider_healthy(provider)
                return result
            except Exception as exc:
                last_error = exc
                self._mark_provider_failed(provider)
                logger.warning(
                    "codex_api | provider=%s model=%s failed: %s",
                    provider.provider_name,
                    provider.model,
                    exc,
                )
        raise RuntimeError(f"All Codex providers failed: {last_error}")

    async def _analyze_chat(
        self,
        provider: CodexProviderSpec,
        prompt: str,
        system_prompt: str,
        temperature: float,
        max_tokens: int,
    ) -> dict[str, Any]:
        client = self._get_client(provider)
        t0 = time.time()
        last_exc: Exception | None = None

        for current_model in model_candidates(
            provider.model,
            review_model=provider.review_model,
            fallback_models=provider.fallback_models,
            fallback_model=provider.fallback_model,
        ):
            for effort in reasoning_effort_candidates(current_model, provider.reasoning_effort):
                payload: dict[str, Any] = {
                    "model": current_model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": max_tokens,
                    "stream": True,
                }
                if _is_reasoning_model(current_model):
                    payload["reasoning_effort"] = effort
                else:
                    payload["temperature"] = temperature

                try:
                    # Ensure base_url includes /v1 for chat completions
                    chat_base = provider.base_url
                    if not chat_base.rstrip("/").endswith("/v1"):
                        chat_base = chat_base.rstrip("/") + "/v1"
                    resp = await client.post(f"{chat_base}/chat/completions", json=payload)
                    resp.raise_for_status()
                    # Always try SSE parsing first (stream=True returns SSE chunks).
                    # Some gateways may still return JSON even with stream=True.
                    data = _parse_sse_chat_response(resp.text)
                    if data is None:
                        try:
                            data = resp.json()
                        except ValueError:
                            raise RuntimeError(f"Codex API response not parseable from {provider.provider_name}")
                    if not data.get("choices"):
                        raise RuntimeError(f"Codex API returned empty choices from {provider.provider_name}")
                    msg = data["choices"][0]["message"]
                    content = msg.get("content")
                    # Reasoning models (glm-5, kimi, etc.) put output in 'reasoning' or
                    # 'reasoning_content' when content is empty – use it as fallback.
                    if not content:
                        content = msg.get("reasoning") or msg.get("reasoning_content") or ""
                    usage = data.get("usage", {})
                    elapsed = round(time.time() - t0, 2)
                    resolved_model = data.get("model", current_model)
                    # If content is empty AND elapsed > 35s (not a hard gateway timeout),
                    # retry with lower effort. Short empty responses (< 35s) are hard timeouts
                    # from the gateway and retrying won't help.
                    if not content and elapsed > 35:
                        logger.warning(
                            "codex_api | provider=%s model=%s reasoning=%s returned empty content after %.1fs, trying lower effort",
                            provider.provider_name,
                            current_model,
                            effort,
                            elapsed,
                        )
                        continue
                    logger.info(
                        "codex_api | provider=%s chat ok elapsed=%.1fs model=%s reasoning=%s in=%d out=%d",
                        provider.provider_name,
                        elapsed,
                        resolved_model,
                        effort,
                        usage.get("prompt_tokens", 0),
                        usage.get("completion_tokens", 0),
                    )
                    return {
                        "response": content,
                        "elapsed_s": elapsed,
                        "has_citation": False,
                        "model": resolved_model,
                        "source": "codex_api",
                        "usage": usage,
                        "provider_name": provider.provider_name,
                        "endpoint": provider.base_url,
                        "reasoning_effort": effort,
                        "pool_level": _codex_pool_level(resolved_model),
                    }
                except ValueError as exc:
                    last_exc = RuntimeError(f"Codex API response decode failed: {exc}")
                    logger.warning(
                        "codex_api | provider=%s model=%s chat decode failed, trying next model candidate if available: %s | body=%s",
                        provider.provider_name,
                        current_model,
                        exc,
                        resp.text[:300] if 'resp' in locals() else "",
                    )
                    break
                except httpx.HTTPStatusError as exc:
                    last_exc = RuntimeError(f"Codex API error {exc.response.status_code}")
                    if exc.response.status_code in _RETRYABLE_REASONING_STATUS_CODES and effort != "high":
                        logger.warning(
                            "codex_api | provider=%s model=%s chat rejected reasoning=%s with HTTP %s, retrying lower effort",
                            provider.provider_name,
                            current_model,
                            effort,
                            exc.response.status_code,
                        )
                        continue
                    logger.warning(
                        "codex_api | provider=%s model=%s chat HTTP %s, trying next model candidate if available: %s",
                        provider.provider_name,
                        current_model,
                        exc.response.status_code,
                        exc.response.text[:300],
                    )
                    break
                except Exception as exc:
                    last_exc = RuntimeError(f"Codex API request failed: {exc}")
                    logger.warning(
                        "codex_api | provider=%s model=%s chat request failed, trying next model candidate if available: %s",
                        provider.provider_name,
                        current_model,
                        exc,
                    )
                    break

        raise last_exc or RuntimeError("Codex API request failed")

    async def _analyze_responses(
        self,
        provider: CodexProviderSpec,
        prompt: str,
        system_prompt: str,
        temperature: float,
        max_tokens: int,
    ) -> dict[str, Any]:
        client = self._get_client(provider)
        t0 = time.time()
        last_exc: Exception | None = None

        for current_model in model_candidates(
            provider.model,
            review_model=provider.review_model,
            fallback_models=provider.fallback_models,
            fallback_model=provider.fallback_model,
        ):
            for effort in reasoning_effort_candidates(current_model, provider.reasoning_effort):
                payload: dict[str, Any] = {
                    "model": current_model,
                    "input": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt},
                    ],
                    "max_output_tokens": max_tokens,
                    "store": False,
                }
                if _is_reasoning_model(current_model):
                    payload["reasoning"] = {"effort": effort}
                else:
                    payload["temperature"] = temperature

                try:
                    resp = await client.post(f"{provider.base_url}/responses", json=payload)
                    resp.raise_for_status()
                    data = resp.json()
                    content = _extract_response_text(data)
                    usage = data.get("usage", {})
                    elapsed = round(time.time() - t0, 2)
                    resolved_model = data.get("model", current_model)
                    logger.info(
                        "codex_api | provider=%s responses ok elapsed=%.1fs model=%s reasoning=%s in=%d out=%d",
                        provider.provider_name,
                        elapsed,
                        resolved_model,
                        effort,
                        usage.get("input_tokens", 0),
                        usage.get("output_tokens", 0),
                    )
                    return {
                        "response": content,
                        "elapsed_s": elapsed,
                        "has_citation": False,
                        "model": resolved_model,
                        "source": "codex_api",
                        "usage": usage,
                        "provider_name": provider.provider_name,
                        "endpoint": provider.base_url,
                        "reasoning_effort": effort,
                        "pool_level": _codex_pool_level(resolved_model),
                    }
                except ValueError as exc:
                    last_exc = RuntimeError(f"Codex API response decode failed: {exc}")
                    logger.warning(
                        "codex_api | provider=%s model=%s responses decode failed, trying next model candidate if available: %s | body=%s",
                        provider.provider_name,
                        current_model,
                        exc,
                        resp.text[:300] if 'resp' in locals() else "",
                    )
                    break
                except httpx.HTTPStatusError as exc:
                    last_exc = RuntimeError(f"Codex API error {exc.response.status_code}")
                    if exc.response.status_code in _RETRYABLE_REASONING_STATUS_CODES and effort != "high":
                        logger.warning(
                            "codex_api | provider=%s model=%s responses rejected reasoning=%s with HTTP %s, retrying lower effort",
                            provider.provider_name,
                            current_model,
                            effort,
                            exc.response.status_code,
                        )
                        continue
                    logger.warning(
                        "codex_api | provider=%s model=%s responses HTTP %s, trying next model candidate if available: %s",
                        provider.provider_name,
                        current_model,
                        exc.response.status_code,
                        exc.response.text[:300],
                    )
                    break
                except Exception as exc:
                    last_exc = RuntimeError(f"Codex API request failed: {exc}")
                    logger.warning(
                        "codex_api | provider=%s model=%s responses request failed, trying next model candidate if available: %s",
                        provider.provider_name,
                        current_model,
                        exc,
                    )
                    break

        raise last_exc or RuntimeError("Codex API request failed")

    async def close(self) -> None:
        for provider_name, client in list(self._clients.items()):
            if client.is_closed:
                continue
            try:
                await client.aclose()
            except RuntimeError as exc:
                if "Event loop is closed" not in str(exc):
                    raise
                logger.warning("codex_api | suppress close after loop shutdown: %s", exc)
            except asyncio.CancelledError:
                raise
            finally:
                self._clients.pop(provider_name, None)


def _extract_response_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for item in payload.get("output", []):
        if item.get("type") != "message":
            continue
        for part in item.get("content", []):
            if part.get("type") == "output_text":
                parts.append(str(part.get("text") or ""))
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        parts.append(output_text)
    return "".join(parts).strip()


_client: CodexAPIClient | None = None
_audit_client: CodexAPIClient | None = None


def get_codex_client() -> CodexAPIClient:
    global _client
    if _client is None:
        _client = CodexAPIClient()
    return _client


def get_audit_codex_client() -> CodexAPIClient:
    global _audit_client
    if _audit_client is None:
        specs = discover_audit_codex_provider_specs()
        if not specs:
            raise ValueError(
                "No Codex audit provider is available. "
                "Expected the canonical gateway provider to exist under ai-api/codex."
            )
        _audit_client = CodexAPIClient(provider_specs=specs)
    return _audit_client


async def shutdown_codex_client() -> None:
    global _client, _audit_client
    if _client is not None:
        try:
            await _client.close()
        except RuntimeError as exc:
            if "Event loop is closed" not in str(exc):
                raise
            logger.warning("codex_api | suppress shutdown after loop shutdown: %s", exc)
        finally:
            _client = None
    if _audit_client is not None:
        try:
            await _audit_client.close()
        except RuntimeError as exc:
            if "Event loop is closed" not in str(exc):
                raise
            logger.warning("codex_api | suppress audit shutdown after loop shutdown: %s", exc)
        finally:
            _audit_client = None
