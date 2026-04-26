from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import settings
from app.services.codex_client import CodexProviderSpec, discover_codex_provider_specs


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Low-cost Codex/NewAPI live gateway probe.")
    parser.add_argument("--provider-root", default=None, help="Optional provider root override.")
    parser.add_argument("--timeout-seconds", type=float, default=5.0, help="HTTP timeout for the probe.")
    parser.add_argument("--provider", default=None, help="Optional provider name to probe first.")
    return parser.parse_args()


def _direct_provider_from_settings() -> CodexProviderSpec | None:
    base_url = str(settings.codex_api_base_url or "").strip().rstrip("/")
    api_key = str(settings.codex_api_key or "").strip()
    model = str(settings.codex_api_model or "").strip()
    if not base_url or not api_key or not model:
        return None
    return CodexProviderSpec(
        provider_name="settings-direct",
        base_url=base_url,
        api_key=api_key,
        model=model,
        fallback_model=str(settings.codex_api_fallback_model or "").strip() or None,
        wire_api=str(settings.codex_wire_api or "responses"),
        reasoning_effort=str(settings.codex_api_reasoning_effort or "high"),
    )


def resolve_probe_targets(*, provider_root: str | None = None, preferred_provider: str | None = None) -> list[CodexProviderSpec]:
    direct = _direct_provider_from_settings()
    discovered = discover_codex_provider_specs(provider_root)
    providers = [direct] + discovered if direct else discovered
    providers = [provider for provider in providers if provider and provider.base_url and provider.api_key]
    if preferred_provider:
        preferred = str(preferred_provider).strip().lower()
        providers.sort(key=lambda provider: (0 if provider.provider_name.lower() == preferred else 1, provider.provider_name.lower()))
    return providers


def _extract_completion_signal(response: httpx.Response) -> tuple[bool, str | None]:
    content_type = (response.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        body = response.json()
        if isinstance(body, dict):
            output = body.get("output")
            if isinstance(output, list):
                for item in output:
                    if not isinstance(item, dict):
                        continue
                    for content in item.get("content") or []:
                        if not isinstance(content, dict):
                            continue
                        if str(content.get("type") or "").lower() == "output_text" and str(content.get("text") or "").strip():
                            return True, None
            choices = body.get("choices")
            if isinstance(choices, list) and choices:
                return True, None
            usage = body.get("usage") or {}
            if isinstance(usage, dict) and int(usage.get("completion_tokens") or 0) > 0:
                return True, None
        return False, "empty_completion_payload"

    text = response.text
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if payload == "[DONE]" or not payload:
            continue
        try:
            body = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(body, dict):
            choices = body.get("choices")
            if isinstance(choices, list) and choices:
                return True, None
            usage = body.get("usage") or {}
            if isinstance(usage, dict) and int(usage.get("completion_tokens") or 0) > 0:
                return True, None
    return False, "empty_completion_stream"


def probe_provider_completion(provider: CodexProviderSpec, *, timeout_seconds: float) -> dict:
    wire_api = str(provider.wire_api or "responses").strip().lower()
    if wire_api == "responses":
        url = f"{provider.base_url.rstrip('/')}/responses"
        payload = {
            "model": provider.model,
            "input": "Return exactly OK",
            "max_output_tokens": 16,
        }
    else:
        url = f"{provider.base_url.rstrip('/')}/chat/completions"
        payload = {
            "model": provider.model,
            "messages": [{"role": "user", "content": "Return exactly OK"}],
            "max_tokens": 8,
            "temperature": 0,
        }
    headers = {
        "Authorization": f"Bearer {provider.api_key}",
        "Content-Type": "application/json",
    }
    timeout = httpx.Timeout(timeout_seconds)
    with httpx.Client(timeout=timeout) as client:
        response = client.post(url, headers=headers, json=payload)
    ok, error = _extract_completion_signal(response)
    return {
        "probe_url": url,
        "status_code": response.status_code,
        "ok": response.status_code == 200 and ok,
        "error": error,
        "wire_api": wire_api,
    }


def probe_provider(provider: CodexProviderSpec, *, timeout_seconds: float) -> dict:
    url = f"{provider.base_url.rstrip('/')}/models"
    headers = {
        "Authorization": f"Bearer {provider.api_key}",
        "Accept": "application/json",
    }
    timeout = httpx.Timeout(timeout_seconds)
    with httpx.Client(timeout=timeout) as client:
        response = client.get(url, headers=headers)
    body = response.json() if "json" in (response.headers.get("content-type") or "").lower() else {}
    models = body.get("data") if isinstance(body, dict) else None
    model_count = len(models) if isinstance(models, list) else None
    models_ok = response.status_code == 200 and model_count is not None
    completion_probe = probe_provider_completion(provider, timeout_seconds=timeout_seconds)
    ok = models_ok and completion_probe["ok"]
    return {
        "provider_name": provider.provider_name,
        "base_url": provider.base_url,
        "probe_url": url,
        "status_code": response.status_code,
        "ok": ok,
        "model_count": model_count,
        "wire_api": provider.wire_api,
        "models_ok": models_ok,
        "completion_probe": completion_probe,
    }


def main() -> int:
    args = _parse_args()
    targets = resolve_probe_targets(
        provider_root=args.provider_root,
        preferred_provider=args.provider,
    )
    if not targets:
        print(json.dumps({"ok": False, "error": "no_probe_targets"}, ensure_ascii=False))
        return 2

    failures: list[dict] = []
    for provider in targets:
        try:
            result = probe_provider(provider, timeout_seconds=args.timeout_seconds)
        except Exception as exc:  # pragma: no cover - operational path
            failures.append(
                {
                    "provider_name": provider.provider_name,
                    "base_url": provider.base_url,
                    "ok": False,
                    "error": str(exc),
                }
            )
            continue
        if result["ok"]:
            print(json.dumps(result, ensure_ascii=False))
            return 0
        failures.append(result)

    print(json.dumps({"ok": False, "failures": failures}, ensure_ascii=False))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
