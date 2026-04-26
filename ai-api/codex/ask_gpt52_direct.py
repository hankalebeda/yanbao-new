from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import httpx


KEY_PATH = Path(__file__).with_name("key.txt")
DEFAULT_MODEL = "gpt-5.4"


def load_pair(pair_index: int = 2) -> tuple[str, str]:
    lines = [line.strip() for line in KEY_PATH.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
    pairs = [(lines[i], lines[i + 1]) for i in range(0, len(lines), 2)]
    if pair_index < 1 or pair_index > len(pairs):
        raise ValueError(f"pair_index out of range: {pair_index}")
    return pairs[pair_index - 1]


def extract_text(payload: dict) -> str:
    parts: list[str] = []
    for item in payload.get("output") or []:
        for content in item.get("content") or []:
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        parts.append(output_text.strip())
    return "\n".join(parts).strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Direct Codex provider caller. Defaults to gpt-5.4 for long-context providers."
    )
    parser.add_argument("prompt", nargs="?", help="Prompt to send. If omitted, reads from stdin.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Model name to call. Use gpt-5.2 for fallback providers.")
    parser.add_argument("--pair-index", type=int, default=2)
    parser.add_argument("--json", action="store_true", help="Print raw JSON summary instead of plain text.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    prompt = args.prompt if args.prompt is not None else sys.stdin.read().strip()
    if not prompt:
        raise SystemExit("prompt is required")

    base_url, api_key = load_pair(args.pair_index)
    response = httpx.post(
        base_url.rstrip("/") + "/v1/responses",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": args.model,
            "input": prompt,
            "max_output_tokens": 2048,
        },
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    text = extract_text(payload)

    if args.json:
        print(json.dumps({
            "base_url": base_url,
            "requested_model": args.model,
            "resolved_model": payload.get("model"),
            "response_id": payload.get("id"),
            "status": payload.get("status"),
            "text": text,
        }, ensure_ascii=False))
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
