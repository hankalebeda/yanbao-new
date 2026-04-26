#!/usr/bin/env python3
"""
Fake Codex CLI for mesh_runner integration.

Impersonates `codex exec` by directly calling the OpenAI-compatible API
instead of using the real Codex CLI (which has auth issues in automated context).

Usage (same as codex CLI):
  python fake_codex.py exec [args...] -
  # Reads prompt from stdin, writes JSON events to stdout,
  # writes last message to --output-last-message path.

Required env vars:
  OPENAI_API_KEY    API key for the target provider
  OPENAI_BASE_URL   Base URL (e.g., https://api.einzieg.site/v1)

Optional env vars:
  FAKE_CODEX_MODEL          Model name (default: gpt-5.4)
  FAKE_CODEX_MAX_TOKENS     Max output tokens (default: 4096)
  FAKE_CODEX_TIMEOUT        Request timeout in seconds (default: 300)
"""
from __future__ import annotations

import json
import os
import sys
import uuid
import time
import argparse
import traceback
from pathlib import Path
from urllib.request import build_opener, ProxyHandler, Request
from urllib.error import HTTPError, URLError


def _log(msg: str) -> None:
    print(json.dumps({"type": "agent_reasoning", "text": msg}), flush=True)


def _emit(event: dict) -> None:
    print(json.dumps(event, ensure_ascii=False), flush=True)


def _call_api(
    base_url: str,
    api_key: str,
    model: str,
    prompt: str,
    max_tokens: int,
    timeout: int,
) -> str:
    """Call /v1/responses endpoint and return the assistant message text."""
    url = base_url.rstrip("/") + "/responses"
    payload = json.dumps({
        "model": model,
        "input": prompt,
        "max_output_tokens": max_tokens,
    }).encode("utf-8")
    req = Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    # Always bypass system proxies to avoid local proxy hijack in automation.
    opener = build_opener(ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="replace"))
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        raise RuntimeError(f"HTTP {exc.code} from {url}: {error_body[:500]}") from exc
    except URLError as exc:
        raise RuntimeError(f"Connection error to {url}: {exc.reason}") from exc

    # Extract text from response output
    output = body.get("output") or []
    for item in output:
        if isinstance(item, dict) and item.get("role") == "assistant":
            content_list = item.get("content") or []
            for content_item in content_list:
                if isinstance(content_item, dict) and content_item.get("type") == "output_text":
                    return str(content_item.get("text") or "")
            # fallback: join all text parts
            parts = []
            for content_item in content_list:
                if isinstance(content_item, dict):
                    parts.append(str(content_item.get("text") or ""))
            if parts:
                return "\n".join(parts)
    # fallback: look for any text output
    if isinstance(body.get("output_text"), str):
        return body["output_text"]
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("subcommand", nargs="?", default="")
    parser.add_argument("--output-last-message", default=None)
    parser.add_argument("--json", action="store_true", default=True)
    parser.add_argument("--cd", default=None)
    parser.add_argument("--color", default="never")
    parser.add_argument("--skip-git-repo-check", action="store_true")
    parser.add_argument("--disable", default=None)
    parser.add_argument("--enable", default=None)
    parser.add_argument("--dangerously-bypass-approvals-and-sandbox", action="store_true")
    parser.add_argument("--ephemeral", action="store_true")
    parser.add_argument("-s", "--sandbox", default=None)
    parser.add_argument("-", dest="stdin_marker", action="store_true", default=False)
    args, _ = parser.parse_known_args()

    thread_id = str(uuid.uuid4())
    _emit({"type": "thread.started", "thread_id": thread_id})
    _emit({"type": "turn.started"})

    # Read prompt from stdin
    prompt = sys.stdin.read().strip()
    if not prompt:
        _emit({"type": "error", "message": "No prompt provided via stdin"})
        _emit({"type": "turn.failed", "error": {"message": "No prompt"}})
        return 1

    # Get API credentials
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    base_url = os.environ.get("OPENAI_BASE_URL", "").strip()
    model = (
        os.environ.get("OPENAI_MODEL", "").strip()
        or os.environ.get("FAKE_CODEX_MODEL", "gpt-5.4").strip()
    )
    max_tokens = int(os.environ.get("FAKE_CODEX_MAX_TOKENS", "4096"))
    timeout = int(os.environ.get("FAKE_CODEX_TIMEOUT", "120"))

    if not api_key:
        # Try to read from portable home config
        userprofile = os.environ.get("USERPROFILE") or os.environ.get("HOME") or ""
        auth_file = Path(userprofile) / ".codex" / "auth.json"
        if auth_file.exists():
            try:
                auth_data = json.loads(auth_file.read_text(encoding="utf-8"))
                api_key = str(auth_data.get("OPENAI_API_KEY") or "").strip()
            except Exception:
                pass
        if not api_key:
            msg = "OPENAI_API_KEY not set and not found in auth.json"
            _emit({"type": "error", "message": msg})
            _emit({"type": "turn.failed", "error": {"message": msg}})
            return 1

    if not base_url:
        # Try to read from portable home config.toml
        userprofile = os.environ.get("USERPROFILE") or os.environ.get("HOME") or ""
        config_file = Path(userprofile) / ".codex" / "config.toml"
        if config_file.exists():
            try:
                cfg_text = config_file.read_text(encoding="utf-8")
                for line in cfg_text.splitlines():
                    if "base_url" in line and "=" in line:
                        parts = line.split("=", 1)
                        base_url = parts[1].strip().strip('"').strip("'")
                        break
            except Exception:
                pass
        if not base_url:
            base_url = "https://api.openai.com/v1"

    _log(f"fake_codex: using base_url={base_url} model={model}")

    try:
        result_text = _call_api(
            base_url=base_url,
            api_key=api_key,
            model=model,
            prompt=prompt,
            max_tokens=max_tokens,
            timeout=timeout,
        )
    except Exception as exc:
        error_msg = str(exc)
        _emit({"type": "error", "message": f"API call failed: {error_msg}"})
        _emit({"type": "turn.failed", "error": {"message": error_msg}})
        # Write empty last_message if needed
        if args.output_last_message:
            out_path = Path(args.output_last_message)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            if not out_path.exists():
                out_path.write_text("", encoding="utf-8")
        sys.stderr.write(f"Warning: no last agent message; wrote empty content to {args.output_last_message}\n")
        return 1

    # Write result to output-last-message
    if args.output_last_message:
        out_path = Path(args.output_last_message)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(result_text, encoding="utf-8")
        if not result_text:
            sys.stderr.write(f"Warning: no last agent message; wrote empty content to {args.output_last_message}\n")

    # Emit completion events
    _emit({
        "type": "message",
        "role": "assistant",
        "content": [{"type": "output_text", "text": result_text}]
    })
    _emit({"type": "turn.completed"})
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
