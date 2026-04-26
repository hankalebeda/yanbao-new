from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import httpx


DEFAULT_MODEL = "gpt-5.4"
DEFAULT_PROMPT = "Reply with exactly: LIVE_OK"
DEFAULT_TIMEOUT = 40.0
KEY_PATH = Path(__file__).with_name("key.txt")


@dataclass(frozen=True)
class CredentialPair:
    index: int
    base_url: str
    api_key: str

    @property
    def models_url(self) -> str:
        return self.base_url.rstrip("/") + "/v1/models"

    @property
    def responses_url(self) -> str:
        return self.base_url.rstrip("/") + "/v1/responses"

    @property
    def masked_key(self) -> str:
        if len(self.api_key) <= 10:
            return "***"
        return f"{self.api_key[:6]}...{self.api_key[-4:]}"


def load_pairs(path: Path) -> list[CredentialPair]:
    raw_lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    lines = [line.strip() for line in raw_lines if line.strip()]
    if len(lines) % 2 != 0:
        raise ValueError(f"Expected URL/key pairs in {path}, got {len(lines)} non-empty lines")

    pairs: list[CredentialPair] = []
    for offset in range(0, len(lines), 2):
        base_url = lines[offset]
        api_key = lines[offset + 1]
        pair_index = offset // 2 + 1
        if not base_url.startswith(("http://", "https://")):
            raise ValueError(f"Pair {pair_index}: invalid base URL")
        if not api_key.startswith("sk-"):
            raise ValueError(f"Pair {pair_index}: invalid API key prefix")
        pairs.append(CredentialPair(index=pair_index, base_url=base_url, api_key=api_key))
    return pairs


def _headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _extract_models(payload: dict) -> list[str]:
    data = payload.get("data") or []
    result: list[str] = []
    for item in data:
        model_id = item.get("id")
        if isinstance(model_id, str):
            result.append(model_id)
    return result


def _extract_text(payload: dict) -> str:
    chunks: list[str] = []
    for item in payload.get("output") or []:
        for content in item.get("content") or []:
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                chunks.append(text.strip())
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        chunks.append(output_text.strip())
    return "\n".join(chunks).strip()


def probe_models(pair: CredentialPair, requested_model: str, timeout: float) -> tuple[bool, dict]:
    response = httpx.get(pair.models_url, headers=_headers(pair.api_key), timeout=timeout)
    summary = {
        "pair": pair.index,
        "base_url": pair.base_url,
        "models_url": pair.models_url,
        "status_code": response.status_code,
    }
    if response.status_code != 200:
        summary["error"] = response.text[:300]
        return False, summary

    payload = response.json()
    models = _extract_models(payload)
    summary["model_count"] = len(models)
    summary["contains_requested_model"] = requested_model in models
    summary["sample_models"] = models[:10]
    return True, summary


def call_responses_api(pair: CredentialPair, model: str, prompt: str, timeout: float) -> tuple[bool, dict]:
    payload = {
        "model": model,
        "input": prompt,
        "max_output_tokens": 32,
    }
    response = httpx.post(
        pair.responses_url,
        headers=_headers(pair.api_key),
        json=payload,
        timeout=timeout,
    )
    summary = {
        "pair": pair.index,
        "base_url": pair.base_url,
        "responses_url": pair.responses_url,
        "status_code": response.status_code,
    }
    if response.status_code != 200:
        summary["error"] = response.text[:500]
        return False, summary

    body = response.json()
    summary["resolved_model"] = body.get("model")
    summary["response_id"] = body.get("id")
    summary["status"] = body.get("status")
    summary["output_text"] = _extract_text(body)
    return True, summary


def iter_target_pairs(pairs: Iterable[CredentialPair], pair_index: int | None) -> list[CredentialPair]:
    if pair_index is None:
        return list(pairs)
    selected = [pair for pair in pairs if pair.index == pair_index]
    if not selected:
        raise ValueError(f"Pair index {pair_index} not found in {KEY_PATH}")
    return selected


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Live probe for Codex provider models using ai-api/codex/key.txt"
    )
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Model name to call")
    parser.add_argument("--prompt", default=DEFAULT_PROMPT, help="Prompt to send")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="HTTP timeout in seconds")
    parser.add_argument("--pair-index", type=int, default=None, help="Only test one URL/key pair from key.txt")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pairs = load_pairs(KEY_PATH)
    failures: list[dict] = []

    for pair in iter_target_pairs(pairs, args.pair_index):
        print(json.dumps({
            "pair": pair.index,
            "base_url": pair.base_url,
            "api_key": pair.masked_key,
            "step": "probe_start",
        }, ensure_ascii=False))

        try:
            models_ok, models_summary = probe_models(pair, args.model, args.timeout)
            print(json.dumps(models_summary, ensure_ascii=False))
            if not models_ok:
                failures.append(models_summary)
                continue

            call_ok, call_summary = call_responses_api(pair, args.model, args.prompt, args.timeout)
            print(json.dumps(call_summary, ensure_ascii=False))
            if not call_ok:
                failures.append(call_summary)
                continue

            print(json.dumps({
                "pair": pair.index,
                "base_url": pair.base_url,
                "requested_model": args.model,
                "resolved_model": call_summary.get("resolved_model"),
                "output_text": call_summary.get("output_text"),
                "result": "LIVE_CALL_OK",
            }, ensure_ascii=False))
            return 0
        except Exception as exc:  # pragma: no cover - live probe fallback
            failure = {
                "pair": pair.index,
                "base_url": pair.base_url,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
            failures.append(failure)
            print(json.dumps(failure, ensure_ascii=False))

    print(json.dumps({
        "requested_model": args.model,
        "result": "LIVE_CALL_FAILED",
        "failures": failures,
    }, ensure_ascii=False))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
