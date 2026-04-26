from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import httpx

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parent
DEFAULT_PROMPT = "Reply with exactly LIVE_OK"
DEFAULT_REASONING_EFFORT = "xhigh"
DEFAULT_CANDIDATE_MODELS = ("gpt-5.4", "gpt-5.2")


@dataclass
class ProviderSource:
    name: str
    base_url: str
    api_key: str
    channel_base_url: str
    source_kinds: list[str] = field(default_factory=list)
    provider_dir: str | None = None
    configured_model: str | None = None
    review_model: str | None = None
    enabled: bool | None = None
    issues: list[str] = field(default_factory=list)


@dataclass
class ProbeAttempt:
    model: str
    request_base_url: str
    ok: bool
    status_code: int | None
    message: str
    output_text: str | None = None


@dataclass
class SyncResult:
    name: str
    source_kinds: list[str]
    provider_dir: str | None
    configured_model: str | None
    review_model: str | None
    enabled: bool | None
    base_url: str
    channel_base_url: str
    selected_model: str | None
    selected_request_base_url: str | None
    upstream_attempts: list[ProbeAttempt] = field(default_factory=list)
    existing_channel_ids: list[int] = field(default_factory=list)
    deleted_channel_ids: list[int] = field(default_factory=list)
    channel_id: int | None = None
    channel_test_ok: bool | None = None
    channel_test_message: str | None = None
    channel_test_time: float | None = None
    result: str = ""
    issues: list[str] = field(default_factory=list)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def _load_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected TOML object in {path}")
    return data


def _read_nonempty_lines(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]


def _load_key_file_entry(path: Path) -> tuple[str | None, str | None]:
    if not path.exists():
        return None, None
    lines = _read_nonempty_lines(path)
    if not lines:
        return None, None
    base_url = lines[0]
    api_key = lines[1] if len(lines) >= 2 else None
    return base_url, api_key


def _normalize_channel_base_url(base_url: str) -> str:
    clean = base_url.strip().rstrip("/")
    parsed = urlsplit(clean)
    if parsed.path == "/v1":
        normalized = urlunsplit((parsed.scheme, parsed.netloc, "", parsed.query, parsed.fragment))
        return normalized.rstrip("/")
    return clean


def _append_v1_base_url(base_url: str) -> str:
    clean = base_url.rstrip("/")
    parsed = urlsplit(clean)
    if parsed.path.endswith("/v1"):
        return clean
    path = parsed.path.rstrip("/")
    next_path = f"{path}/v1" if path else "/v1"
    return urlunsplit((parsed.scheme, parsed.netloc, next_path, parsed.query, parsed.fragment)).rstrip("/")


def _host_name_from_url(base_url: str) -> str:
    parsed = urlsplit(base_url.strip())
    return parsed.netloc or parsed.path or base_url.strip()


def _headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _extract_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for item in payload.get("output") or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content") or []:
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        parts.append(output_text.strip())
    return "\n".join(parts).strip()


def _iter_provider_dirs(root: Path) -> list[Path]:
    result: list[Path] = []
    for item in sorted(root.iterdir()):
        if not item.is_dir():
            continue
        if item.name.startswith("portable_") or item.name.startswith("newapi-") or item.name == "__pycache__":
            continue
        result.append(item)
    return result


def load_provider_sources(root: Path) -> tuple[list[ProviderSource], list[dict[str, Any]]]:
    sources: dict[str, ProviderSource] = {}
    skipped: list[dict[str, Any]] = []

    for provider_dir in _iter_provider_dirs(root):
        config_path = provider_dir / "config.toml"
        auth_path = provider_dir / "auth.json"
        provider_path = provider_dir / "provider.json"
        key_path = provider_dir / "key.txt"

        issues: list[str] = []
        config: dict[str, Any] = {}
        provider_meta: dict[str, Any] = {}
        auth: dict[str, Any] = {}

        if config_path.exists():
            try:
                config = _load_toml(config_path)
            except Exception as exc:  # pragma: no cover - operational script
                issues.append(f"config.toml unreadable: {exc}")
        if provider_path.exists():
            try:
                provider_meta = _load_json(provider_path)
            except Exception as exc:  # pragma: no cover - operational script
                issues.append(f"provider.json unreadable: {exc}")
        if auth_path.exists():
            try:
                auth = _load_json(auth_path)
            except Exception as exc:  # pragma: no cover - operational script
                issues.append(f"auth.json unreadable: {exc}")

        key_base_url, key_api_key = _load_key_file_entry(key_path)
        provider_cfg = (config.get("model_providers") or {}).get("OpenAI") or {}
        base_url = str(
            provider_cfg.get("base_url")
            or provider_meta.get("endpoint")
            or key_base_url
            or ""
        ).strip()
        api_key = str(
            key_api_key
            or auth.get("OPENAI_API_KEY")
            or auth.get("api_key")
            or auth.get("apiKey")
            or ""
        ).strip()
        if not base_url or not api_key:
            skipped.append(
                {
                    "name": provider_dir.name,
                    "reason": "missing base_url or api_key",
                    "has_config": config_path.exists(),
                    "has_auth": auth_path.exists(),
                    "has_provider": provider_path.exists(),
                    "has_key": key_path.exists(),
                }
            )
            continue

        if not config_path.exists() and key_path.exists():
            issues.append("config.toml missing, loaded from key.txt only")
        if not auth_path.exists() and key_path.exists():
            issues.append("auth.json missing, loaded from key.txt only")
        if provider_meta.get("enabled") is False:
            issues.append("provider.json enabled=false")

        source = ProviderSource(
            name=provider_dir.name,
            base_url=base_url.rstrip("/"),
            api_key=api_key,
            channel_base_url=_normalize_channel_base_url(base_url),
            source_kinds=["provider_dir"],
            provider_dir=provider_dir.name,
            configured_model=str(config.get("model") or provider_meta.get("model") or "").strip() or None,
            review_model=str(config.get("review_model") or provider_meta.get("review_model") or "").strip() or None,
            enabled=provider_meta.get("enabled") if provider_meta else None,
            issues=issues,
        )
        if key_path.exists():
            source.source_kinds.append("provider_key.txt")
        sources[source.name] = source

    root_key_path = root / "key.txt"
    if root_key_path.exists():
        lines = _read_nonempty_lines(root_key_path)
        if len(lines) % 2 != 0:
            skipped.append(
                {
                    "name": "root:key.txt",
                    "reason": "odd number of non-empty lines",
                    "line_count": len(lines),
                }
            )
        for index in range(0, len(lines) - 1, 2):
            base_url = lines[index].strip()
            api_key = lines[index + 1].strip()
            name = _host_name_from_url(base_url)
            if not base_url or not api_key:
                skipped.append(
                    {
                        "name": f"root:key.txt:{index // 2}",
                        "reason": "missing base_url or api_key",
                    }
                )
                continue
            existing = sources.get(name)
            if existing is not None:
                if "root_key.txt" not in existing.source_kinds:
                    existing.source_kinds.append("root_key.txt")
                if existing.base_url != base_url.rstrip("/"):
                    existing.issues.append(f"root key.txt base_url differs: {base_url}")
                if existing.api_key != api_key:
                    existing.issues.append("root key.txt api_key differs from directory source")
                continue
            sources[name] = ProviderSource(
                name=name,
                base_url=base_url.rstrip("/"),
                api_key=api_key,
                channel_base_url=_normalize_channel_base_url(base_url),
                source_kinds=["root_key.txt"],
                issues=["loaded from root key.txt only"],
            )

    return list(sorted(sources.values(), key=lambda item: item.name.lower())), skipped


class NewAPIClient:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.user_id: int | None = None
        self.client = httpx.Client(
            base_url=self.base_url,
            timeout=60.0,
            follow_redirects=True,
            trust_env=False,
        )

    def close(self) -> None:
        self.client.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        headers = dict(kwargs.pop("headers", {}) or {})
        if self.user_id is not None:
            headers.setdefault("New-Api-User", str(self.user_id))
        response = self.client.request(method, path, headers=headers, **kwargs)
        response.raise_for_status()
        return response

    @staticmethod
    def _json(response: httpx.Response) -> dict[str, Any]:
        if not response.content:
            return {}
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Expected JSON object, got {type(payload).__name__}")
        return payload

    def ensure_setup(self) -> dict[str, Any]:
        status = self._json(self._request("GET", "/api/setup"))
        data = status.get("data") or {}
        initialized = bool(data.get("status")) or bool(data.get("root_init"))
        if initialized:
            return status
        payload = {
            "username": self.username,
            "password": self.password,
            "confirmPassword": self.password,
            "SelfUseModeEnabled": False,
            "DemoSiteEnabled": False,
        }
        response = self._json(self._request("POST", "/api/setup", json=payload))
        if not response.get("success"):
            raise RuntimeError(f"setup failed: {response}")
        return response

    def login(self) -> dict[str, Any]:
        response = self._json(
            self._request(
                "POST",
                "/api/user/login",
                json={"username": self.username, "password": self.password},
            )
        )
        if not response.get("success"):
            raise RuntimeError(f"login failed: {response}")
        data = response.get("data") or {}
        user_id = data.get("id")
        if not isinstance(user_id, int):
            raise RuntimeError(f"login response missing user id: {response}")
        self.user_id = user_id
        return response

    def list_channels(self) -> list[dict[str, Any]]:
        response = self._json(self._request("GET", "/api/channel/?p=1&page_size=300"))
        data = response.get("data") or {}
        items = data.get("items") or []
        return [item for item in items if isinstance(item, dict)]

    def find_channels_by_name(self, name: str) -> list[dict[str, Any]]:
        return [item for item in self.list_channels() if item.get("name") == name]

    def delete_channel(self, channel_id: int) -> None:
        response = self._json(self._request("DELETE", f"/api/channel/{channel_id}"))
        if not response.get("success"):
            raise RuntimeError(f"delete channel failed: {response}")

    def create_channel(self, source: ProviderSource, model_name: str) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": source.name,
            "type": 1,
            "models": model_name,
            "group": "default",
            "key": source.api_key,
            "base_url": source.channel_base_url,
            "priority": 0,
            "weight": 100,
            "status": 1,
            "auto_ban": 1,
            "test_model": model_name,
        }
        response = self._json(
            self._request("POST", "/api/channel/", json={"mode": "single", "channel": payload})
        )
        if response and not response.get("success", False):
            raise RuntimeError(f"create channel failed: {response}")
        channels = self.find_channels_by_name(source.name)
        if not channels:
            raise RuntimeError(f"channel {source.name} not found after creation")
        latest = channels[-1]
        channel_id = latest.get("id")
        if not isinstance(channel_id, int):
            raise RuntimeError(f"channel id missing after creation: {latest}")
        return latest

    def test_channel(self, channel_id: int, model_name: str) -> dict[str, Any]:
        path = f"/api/channel/test/{channel_id}?model={quote(model_name)}"
        return self._json(self._request("GET", path))


def _probe_attempt(
    *,
    client: httpx.Client,
    source: ProviderSource,
    request_base_url: str,
    model_name: str,
    prompt: str,
    reasoning_effort: str,
) -> ProbeAttempt:
    payload = {
        "model": model_name,
        "input": prompt,
        "max_output_tokens": 32,
        "store": False,
        "reasoning": {"effort": reasoning_effort},
    }
    url = request_base_url.rstrip("/") + "/responses"
    try:
        response = client.post(url, headers=_headers(source.api_key), json=payload)
    except Exception as exc:  # pragma: no cover - operational script
        return ProbeAttempt(
            model=model_name,
            request_base_url=request_base_url.rstrip("/"),
            ok=False,
            status_code=None,
            message=str(exc),
        )

    if response.status_code != 200:
        return ProbeAttempt(
            model=model_name,
            request_base_url=request_base_url.rstrip("/"),
            ok=False,
            status_code=response.status_code,
            message=response.text[:500],
        )

    try:
        body = response.json()
    except json.JSONDecodeError:
        return ProbeAttempt(
            model=model_name,
            request_base_url=request_base_url.rstrip("/"),
            ok=False,
            status_code=response.status_code,
            message=f"non-JSON 200 response: {response.text[:500]}",
        )
    output_text = _extract_text(body)
    ok = output_text.strip() == "LIVE_OK"
    message = "LIVE_OK" if ok else f"unexpected output: {output_text[:200]!r}"
    return ProbeAttempt(
        model=model_name,
        request_base_url=request_base_url.rstrip("/"),
        ok=ok,
        status_code=response.status_code,
        message=message,
        output_text=output_text,
    )


def probe_source(
    source: ProviderSource,
    *,
    candidate_models: list[str],
    prompt: str,
    reasoning_effort: str,
    timeout: float,
) -> tuple[str | None, str | None, list[ProbeAttempt]]:
    request_base_urls = [source.base_url.rstrip("/")]
    v1_base_url = _append_v1_base_url(source.base_url)
    if v1_base_url not in request_base_urls:
        request_base_urls.append(v1_base_url)

    attempts: list[ProbeAttempt] = []
    with httpx.Client(timeout=timeout, follow_redirects=True, trust_env=False) as client:
        for request_base_url in request_base_urls:
            for model_name in candidate_models:
                attempt = _probe_attempt(
                    client=client,
                    source=source,
                    request_base_url=request_base_url,
                    model_name=model_name,
                    prompt=prompt,
                    reasoning_effort=reasoning_effort,
                )
                attempts.append(attempt)
                if attempt.ok:
                    return model_name, request_base_url, attempts
    return None, None, attempts


def sync_channels(
    client: NewAPIClient,
    sources: list[ProviderSource],
    *,
    candidate_models: list[str],
    prompt: str,
    reasoning_effort: str,
    timeout: float,
) -> list[SyncResult]:
    results: list[SyncResult] = []

    for source in sources:
        existing_channel_ids: list[int] = []
        deleted_channel_ids: list[int] = []
        selected_model: str | None = None
        selected_request_base_url: str | None = None
        attempts: list[ProbeAttempt] = []
        created_channel_id: int | None = None
        try:
            existing_channels = client.find_channels_by_name(source.name)
            existing_channel_ids = [
                channel_id
                for channel in existing_channels
                for channel_id in [channel.get("id")]
                if isinstance(channel_id, int)
            ]
            selected_model, selected_request_base_url, attempts = probe_source(
                source,
                candidate_models=candidate_models,
                prompt=prompt,
                reasoning_effort=reasoning_effort,
                timeout=timeout,
            )

            if selected_model is None:
                for channel_id in existing_channel_ids:
                    client.delete_channel(channel_id)
                    deleted_channel_ids.append(channel_id)
                results.append(
                    SyncResult(
                        name=source.name,
                        source_kinds=source.source_kinds,
                        provider_dir=source.provider_dir,
                        configured_model=source.configured_model,
                        review_model=source.review_model,
                        enabled=source.enabled,
                        base_url=source.base_url,
                        channel_base_url=source.channel_base_url,
                        selected_model=None,
                        selected_request_base_url=None,
                        upstream_attempts=attempts,
                        existing_channel_ids=existing_channel_ids,
                        deleted_channel_ids=deleted_channel_ids,
                        result="UPSTREAM_FAILED",
                        issues=source.issues,
                    )
                )
                continue

            for channel_id in existing_channel_ids:
                client.delete_channel(channel_id)
                deleted_channel_ids.append(channel_id)

            channel = client.create_channel(source, selected_model)
            created_channel_id = channel.get("id")
            if not isinstance(created_channel_id, int):
                raise RuntimeError(f"channel id missing for {source.name}: {channel}")

            test_response = client.test_channel(created_channel_id, selected_model)
            channel_test_ok = bool(test_response.get("success"))
            channel_test_message = str(test_response.get("message") or "") or None
            channel_test_time = (
                float(test_response.get("time"))
                if test_response.get("time") is not None
                else None
            )
            result_name = "IMPORTED_OK"
            channel_id = created_channel_id
            if not channel_test_ok:
                client.delete_channel(created_channel_id)
                deleted_channel_ids.append(created_channel_id)
                channel_id = None
                created_channel_id = None
                result_name = "CHANNEL_TEST_FAILED"

            results.append(
                SyncResult(
                    name=source.name,
                    source_kinds=source.source_kinds,
                    provider_dir=source.provider_dir,
                    configured_model=source.configured_model,
                    review_model=source.review_model,
                    enabled=source.enabled,
                    base_url=source.base_url,
                    channel_base_url=source.channel_base_url,
                    selected_model=selected_model,
                    selected_request_base_url=selected_request_base_url,
                    upstream_attempts=attempts,
                    existing_channel_ids=existing_channel_ids,
                    deleted_channel_ids=deleted_channel_ids,
                    channel_id=channel_id,
                    channel_test_ok=channel_test_ok,
                    channel_test_message=channel_test_message,
                    channel_test_time=channel_test_time,
                    result=result_name,
                    issues=source.issues,
                )
            )
        except Exception as exc:  # pragma: no cover - operational script
            if created_channel_id is not None:
                try:
                    client.delete_channel(created_channel_id)
                    deleted_channel_ids.append(created_channel_id)
                except Exception:
                    pass
            result_name = "SYNC_EXCEPTION"
            if selected_model is None:
                result_name = "UPSTREAM_EXCEPTION"
            results.append(
                SyncResult(
                    name=source.name,
                    source_kinds=source.source_kinds,
                    provider_dir=source.provider_dir,
                    configured_model=source.configured_model,
                    review_model=source.review_model,
                    enabled=source.enabled,
                    base_url=source.base_url,
                    channel_base_url=source.channel_base_url,
                    selected_model=selected_model,
                    selected_request_base_url=selected_request_base_url,
                    upstream_attempts=attempts,
                    existing_channel_ids=existing_channel_ids,
                    deleted_channel_ids=deleted_channel_ids,
                    channel_id=None,
                    result=result_name,
                    issues=source.issues + [str(exc)],
                )
            )

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe ai-api/codex relays with gpt-5.4 -> gpt-5.2 fallback and sync only passing channels into New API."
    )
    parser.add_argument("--base-url", required=True, help="New API console base URL, for example http://host:3000")
    parser.add_argument("--username", required=True, help="New API admin username")
    parser.add_argument("--password", required=True, help="New API admin password")
    parser.add_argument(
        "--providers-root",
        default=str(ROOT),
        help="Directory containing provider folders and key.txt sources",
    )
    parser.add_argument(
        "--candidate-models",
        default=",".join(DEFAULT_CANDIDATE_MODELS),
        help="Comma-separated model fallback order, default gpt-5.4,gpt-5.2",
    )
    parser.add_argument(
        "--reasoning-effort",
        default=DEFAULT_REASONING_EFFORT,
        help="Reasoning effort used for upstream /responses probes",
    )
    parser.add_argument(
        "--prompt",
        default=DEFAULT_PROMPT,
        help="Prompt used for upstream /responses probes",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=45.0,
        help="HTTP timeout in seconds for upstream probes",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Optional path to write a JSON summary",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    candidate_models = [item.strip() for item in args.candidate_models.split(",") if item.strip()]
    if not candidate_models:
        raise SystemExit("At least one candidate model is required")

    providers_root = Path(args.providers_root).resolve()
    sources, skipped = load_provider_sources(providers_root)
    client = NewAPIClient(args.base_url, args.username, args.password)
    try:
        setup_response = client.ensure_setup()
        login_response = client.login()
        results = sync_channels(
            client,
            sources,
            candidate_models=candidate_models,
            prompt=args.prompt,
            reasoning_effort=args.reasoning_effort,
            timeout=args.timeout,
        )
        active_channels = [
            {
                "id": channel.get("id"),
                "name": channel.get("name"),
                "models": channel.get("models"),
                "base_url": channel.get("base_url"),
                "status": channel.get("status"),
            }
            for channel in client.list_channels()
        ]
    finally:
        client.close()

    imported_ok = [item.name for item in results if item.result == "IMPORTED_OK"]
    upstream_failed = [item.name for item in results if item.result == "UPSTREAM_FAILED"]
    channel_test_failed = [item.name for item in results if item.result == "CHANNEL_TEST_FAILED"]

    summary = {
        "base_url": args.base_url.rstrip("/"),
        "providers_root": str(providers_root),
        "candidate_models": candidate_models,
        "reasoning_effort": args.reasoning_effort,
        "prompt": args.prompt,
        "setup": setup_response,
        "login": {
            "success": bool(login_response.get("success")),
            "user_id": login_response.get("data", {}).get("id"),
        },
        "sources_loaded": [asdict(source) for source in sources],
        "skipped": skipped,
        "results": [asdict(item) for item in results],
        "imported_ok": imported_ok,
        "upstream_failed": upstream_failed,
        "channel_test_failed": channel_test_failed,
        "active_channels_after_sync": active_channels,
    }
    if args.out:
        out_path = Path(args.out).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(
        f"\nNew API relay sync complete: "
        f"{len(imported_ok)} imported, "
        f"{len(upstream_failed)} upstream failed, "
        f"{len(channel_test_failed)} channel-test failed"
    )
    return 0 if not channel_test_failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
