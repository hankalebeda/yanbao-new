from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parent
DEFAULT_WIRE_API = "responses"
DEFAULT_SANDBOX = "elevated"
DEFAULT_REASONING_EFFORT = "high"
DEFAULT_CONTEXT_WINDOW = 400000
DEFAULT_AUTO_COMPACT_TOKEN_LIMIT = 320000


def resolve_context_limits(model: str) -> tuple[int, int]:
    normalized = model.strip().lower()
    if normalized.startswith("gpt-5.4"):
        return 1000000, 900000
    if normalized.startswith(("gpt-5.2", "gpt-5.2-codex", "gpt-5.3-codex")):
        return 400000, 320000
    return DEFAULT_CONTEXT_WINDOW, DEFAULT_AUTO_COMPACT_TOKEN_LIMIT


def _single_value(values: dict[str, list[str]], key: str) -> str:
    items = values.get(key)
    if not items or not items[0]:
        raise ValueError(f"Missing required query parameter: {key}")
    return items[0]


def _normalize_endpoint(endpoint: str) -> tuple[str, str]:
    parsed = urlparse(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"Invalid endpoint: {endpoint}")

    base_path = parsed.path.rstrip("/")
    if not base_path.endswith("/v1"):
        if not base_path:
            base_path = "/v1"
        else:
            base_path = f"{base_path}/v1"

    normalized = parsed._replace(path=base_path, params="", query="", fragment="").geturl()
    return parsed.netloc, normalized.rstrip("/")


def _parse_enabled(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_ccswitch_uri(uri: str) -> dict[str, object]:
    parsed = urlparse(uri)
    if parsed.scheme != "ccswitch":
        raise ValueError("URI must start with ccswitch://")
    if parsed.netloc != "v1" or parsed.path != "/import":
        raise ValueError("Unsupported CCSwitch path, expected ccswitch://v1/import")

    query = parse_qs(parsed.query, keep_blank_values=False)
    if _single_value(query, "resource") != "provider":
        raise ValueError("Only resource=provider is supported")
    if _single_value(query, "app") != "codex":
        raise ValueError("Only app=codex is supported")

    raw_endpoint = _single_value(query, "endpoint")
    provider_dir, endpoint = _normalize_endpoint(raw_endpoint)
    model = _single_value(query, "model")
    api_key = _single_value(query, "apiKey")
    homepage = _single_value(query, "homepage")

    return {
        "provider_dir": provider_dir,
        "name": _single_value(query, "name"),
        "endpoint": endpoint,
        "model": model,
        "api_key": api_key,
        "homepage": homepage,
        "enabled": _parse_enabled(_single_value(query, "enabled")),
        "resource": "provider",
        "app": "codex",
    }


def build_config_toml(model: str, endpoint: str, wire_api: str, sandbox: str) -> str:
    context_window, auto_compact_token_limit = resolve_context_limits(model)
    return "\n".join(
        [
            'model_provider = "OpenAI"',
            f'model = "{model}"',
            f'review_model = "{model}"',
            f'model_reasoning_effort = "{DEFAULT_REASONING_EFFORT}"',
            "disable_response_storage = true",
            'network_access = "enabled"',
            "windows_wsl_setup_acknowledged = true",
            f"model_context_window = {context_window}",
            f"model_auto_compact_token_limit = {auto_compact_token_limit}",
            "",
            "[model_providers.OpenAI]",
            'name = "OpenAI"',
            f'base_url = "{endpoint}"',
            f'wire_api = "{wire_api}"',
            "supports_websockets = false",
            "requires_openai_auth = true",
            "",
            "[features]",
            "responses_websockets_v2 = false",
            "",
            "[windows]",
            f'sandbox = "{sandbox}"',
            "",
        ]
    )


def write_provider_files(
    *,
    provider_dir: Path,
    provider_name: str,
    endpoint: str,
    model: str,
    homepage: str,
    enabled: bool,
    api_key: str,
    wire_api: str,
    sandbox: str,
    write_auth: bool,
) -> None:
    provider_dir.mkdir(parents=True, exist_ok=True)

    config_path = provider_dir / "config.toml"
    config_path.write_text(
        build_config_toml(model=model, endpoint=endpoint, wire_api=wire_api, sandbox=sandbox),
        encoding="utf-8",
    )

    metadata_path = provider_dir / "provider.json"
    metadata_path.write_text(
        json.dumps(
            {
                "name": provider_name,
                "endpoint": endpoint,
                "model": model,
                "homepage": homepage,
                "enabled": enabled,
                "resource": "provider",
                "app": "codex",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    if write_auth:
        auth_path = provider_dir / "auth.json"
        auth_path.write_text(
            json.dumps({"OPENAI_API_KEY": api_key}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import a ccswitch:// provider link into ai-api/codex/<host>/"
    )
    parser.add_argument("uri", help="Full ccswitch://v1/import?... URI")
    parser.add_argument(
        "--wire-api",
        default=DEFAULT_WIRE_API,
        choices=["responses", "chat"],
        help="Codex wire_api value for this provider",
    )
    parser.add_argument(
        "--sandbox",
        default=DEFAULT_SANDBOX,
        choices=["elevated", "unelevated"],
        help="Windows sandbox mode stored in config.toml",
    )
    parser.add_argument(
        "--no-write-auth",
        action="store_true",
        help="Create config.toml/provider.json only, skip auth.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    provider = parse_ccswitch_uri(args.uri)
    provider_dir = ROOT / str(provider["provider_dir"])

    write_provider_files(
        provider_dir=provider_dir,
        provider_name=str(provider["name"]),
        endpoint=str(provider["endpoint"]),
        model=str(provider["model"]),
        homepage=str(provider["homepage"]),
        enabled=bool(provider["enabled"]),
        api_key=str(provider["api_key"]),
        wire_api=args.wire_api,
        sandbox=args.sandbox,
        write_auth=not args.no_write_auth,
    )

    print(
        json.dumps(
            {
                "provider_dir": str(provider_dir),
                "config_path": str(provider_dir / "config.toml"),
                "metadata_path": str(provider_dir / "provider.json"),
                "auth_written": not args.no_write_auth,
                "auth_path": str(provider_dir / "auth.json"),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
