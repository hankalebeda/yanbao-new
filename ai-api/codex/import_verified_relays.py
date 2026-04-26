from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

from sync_newapi_channels import NewAPIClient


ROOT = Path(__file__).resolve().parent
DEFAULT_MODELS = ("gpt-5.4", "gpt-5.2")
DEFAULT_PROMPT = "Reply with exactly LIVE_OK"


@dataclass
class RelaySource:
    name: str
    base_url: str
    channel_base_url: str
    api_key: str
    source_labels: list[str] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)


@dataclass
class ProbeAttempt:
    model: str
    attempt: int
    status_code: int | None = None
    output_text: str | None = None
    resolved_model: str | None = None
    response_id: str | None = None
    error: str | None = None


@dataclass
class ProbeResult:
    ok: bool
    selected_model: str | None
    selected_source: RelaySource | None
    attempts: list[ProbeAttempt] = field(default_factory=list)


@dataclass
class RelayOutcome:
    name: str
    selected_model: str | None
    selected_source_labels: list[str]
    selected_base_url: str | None
    direct_probe_ok: bool
    channel_id: int | None
    channel_test_ok: bool | None
    channel_test_message: str | None
    channel_test_time_seconds: float | None
    deleted_existing_channel_ids: list[int] = field(default_factory=list)
    attempts: list[dict[str, Any]] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)


@dataclass
class ChannelSpec:
    name: str
    channel_base_url: str
    api_key: str
    exposed_model: str
    model_mapping: dict[str, str] | None = None
    issues: list[str] = field(default_factory=list)


def _load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def _load_toml(path: Path) -> dict[str, Any]:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected TOML object in {path}")
    return data


def _normalize_openai_base_url(base_url: str) -> str:
    clean = base_url.strip().rstrip("/")
    parsed = urlsplit(clean)
    if parsed.path.endswith("/v1"):
        return clean
    base_path = parsed.path.rstrip("/")
    normalized_path = f"{base_path}/v1" if base_path else "/v1"
    return urlunsplit((parsed.scheme, parsed.netloc, normalized_path, parsed.query, parsed.fragment)).rstrip("/")


def _normalize_channel_base_url(base_url: str) -> str:
    clean = base_url.strip().rstrip("/")
    parsed = urlsplit(clean)
    if parsed.path == "/v1":
        return urlunsplit((parsed.scheme, parsed.netloc, "", parsed.query, parsed.fragment)).rstrip("/")
    return clean


def _read_key_file(path: Path) -> tuple[str | None, str | None]:
    lines = [line.strip() for line in path.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
    if not lines:
        return None, None
    if len(lines) >= 2 and lines[0].startswith(("http://", "https://")):
        return lines[0], lines[1]
    return None, lines[0]


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
        if item.name.startswith("portable_") or item.name == "__pycache__":
            continue
        if item.name.startswith("newapi-"):
            continue
        result.append(item)
    return result


def _append_source(
    buckets: dict[str, list[RelaySource]],
    *,
    name: str,
    base_url: str,
    api_key: str,
    source_label: str,
    issues: list[str] | None = None,
) -> None:
    normalized_base_url = _normalize_openai_base_url(base_url)
    channel_base_url = _normalize_channel_base_url(normalized_base_url)
    relay = RelaySource(
        name=name,
        base_url=normalized_base_url,
        channel_base_url=channel_base_url,
        api_key=api_key.strip(),
        source_labels=[source_label],
        issues=list(issues or []),
    )
    bucket = buckets.setdefault(name, [])
    for existing in bucket:
        if existing.base_url == relay.base_url and existing.api_key == relay.api_key:
            if source_label not in existing.source_labels:
                existing.source_labels.append(source_label)
            existing.issues.extend(issue for issue in relay.issues if issue not in existing.issues)
            return
    bucket.append(relay)


def collect_sources(root: Path, include_root_key: bool) -> tuple[dict[str, list[RelaySource]], list[dict[str, Any]]]:
    buckets: dict[str, list[RelaySource]] = {}
    skipped: list[dict[str, Any]] = []
    for provider_dir in _iter_provider_dirs(root):
        base_url = ""
        api_key = ""
        issues: list[str] = []
        if (provider_dir / "config.toml").exists():
            config = _load_toml(provider_dir / "config.toml")
            base_url = str(((config.get("model_providers") or {}).get("OpenAI") or {}).get("base_url") or "").strip()
        if (provider_dir / "provider.json").exists():
            provider_meta = _load_json(provider_dir / "provider.json")
            if not base_url:
                base_url = str(provider_meta.get("endpoint") or "").strip()
            if provider_meta.get("enabled") is False:
                issues.append("provider.json enabled=false")
        else:
            issues.append("provider.json missing")
        if (provider_dir / "auth.json").exists():
            auth = _load_json(provider_dir / "auth.json")
            api_key = str(auth.get("OPENAI_API_KEY") or auth.get("api_key") or auth.get("apiKey") or "").strip()
        key_label = None
        if (provider_dir / "key.txt").exists():
            key_base_url, key_value = _read_key_file(provider_dir / "key.txt")
            if key_base_url:
                if base_url and _normalize_openai_base_url(base_url) != _normalize_openai_base_url(key_base_url):
                    issues.append("config.toml base_url differs from key.txt endpoint")
                base_url = key_base_url
            if key_value:
                if api_key and api_key != key_value:
                    issues.append("auth.json api key differs from key.txt")
                api_key = key_value
            key_label = f"{provider_dir.name}:key.txt"
        source_labels: list[str] = []
        if (provider_dir / "auth.json").exists() or (provider_dir / "config.toml").exists():
            source_labels.append(f"{provider_dir.name}:auth+config")
        if key_label:
            source_labels.append(key_label)
        if not base_url or not api_key:
            skipped.append(
                {
                    "name": provider_dir.name,
                    "reason": "missing base_url or api_key",
                    "base_url_found": bool(base_url),
                    "api_key_found": bool(api_key),
                }
            )
            continue
        for source_label in source_labels or [provider_dir.name]:
            _append_source(
                buckets,
                name=provider_dir.name,
                base_url=base_url,
                api_key=api_key,
                source_label=source_label,
                issues=issues,
            )
    if include_root_key:
        key_path = root / "key.txt"
        if key_path.exists():
            lines = [line.strip() for line in key_path.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
            if len(lines) % 2 != 0:
                skipped.append({"name": "key.txt", "reason": "root key.txt has an odd number of non-empty lines"})
            for index in range(0, len(lines) - 1, 2):
                base_url = lines[index]
                api_key = lines[index + 1]
                parsed = urlsplit(base_url)
                name = parsed.netloc or parsed.path
                if not name:
                    skipped.append({"name": f"key.txt:{index}", "reason": "could not derive provider name from URL"})
                    continue
                _append_source(
                    buckets,
                    name=name,
                    base_url=base_url,
                    api_key=api_key,
                    source_label="root:key.txt",
                )
    return buckets, skipped


def probe_source(
    client: httpx.Client,
    source: RelaySource,
    *,
    prompt: str,
    models: tuple[str, ...],
    retries: int,
) -> ProbeResult:
    attempts: list[ProbeAttempt] = []
    for model in models:
        for attempt in range(1, retries + 1):
            attempt_row = ProbeAttempt(model=model, attempt=attempt)
            try:
                response = client.post(
                    source.base_url.rstrip("/") + "/responses",
                    headers={
                        "Authorization": f"Bearer {source.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": model,
                        "input": prompt,
                        "max_output_tokens": 16,
                        "store": False,
                        "reasoning": {"effort": "xhigh"},
                    },
                )
                attempt_row.status_code = response.status_code
                if response.status_code != 200:
                    attempt_row.error = response.text[:600]
                    attempts.append(attempt_row)
                    continue
                payload = response.json()
                attempt_row.output_text = _extract_text(payload)
                attempt_row.resolved_model = str(payload.get("model") or "").strip() or None
                attempt_row.response_id = str(payload.get("id") or "").strip() or None
                attempts.append(attempt_row)
                if attempt_row.output_text == "LIVE_OK":
                    return ProbeResult(
                        ok=True,
                        selected_model=model,
                        selected_source=source,
                        attempts=attempts,
                    )
                attempt_row.error = f"unexpected output: {attempt_row.output_text[:200]!r}"
            except Exception as exc:  # pragma: no cover - operational network script
                attempt_row.error = str(exc)
                attempts.append(attempt_row)
    return ProbeResult(ok=False, selected_model=None, selected_source=None, attempts=attempts)


def _delete_existing_channels(newapi_client: NewAPIClient, name: str) -> list[int]:
    deleted_ids: list[int] = []
    for channel in newapi_client.find_channels_by_name(name):
        channel_id = channel.get("id")
        if isinstance(channel_id, int):
            newapi_client.delete_channel(channel_id)
            deleted_ids.append(channel_id)
    return deleted_ids


def sync_relays(
    *,
    newapi_client: NewAPIClient,
    providers_root: Path,
    prompt: str,
    models: tuple[str, ...],
    retries: int,
    include_root_key: bool,
    cleanup_failed_existing: bool,
) -> dict[str, Any]:
    source_buckets, skipped = collect_sources(providers_root, include_root_key=include_root_key)
    outcomes: list[RelayOutcome] = []
    with httpx.Client(timeout=45.0, follow_redirects=True, trust_env=False) as probe_client:
        for name in sorted(source_buckets):
            sources = source_buckets[name]
            selected_probe: ProbeResult | None = None
            all_attempts: list[dict[str, Any]] = []
            merged_issues: list[str] = []
            for source in sources:
                probe_result = probe_source(
                    probe_client,
                    source,
                    prompt=prompt,
                    models=models,
                    retries=retries,
                )
                all_attempts.append(
                    {
                        "source_labels": source.source_labels,
                        "base_url": source.base_url,
                        "channel_base_url": source.channel_base_url,
                        "issues": source.issues,
                        "probe_attempts": [asdict(item) for item in probe_result.attempts],
                        "ok": probe_result.ok,
                        "selected_model": probe_result.selected_model,
                    }
                )
                for issue in source.issues:
                    if issue not in merged_issues:
                        merged_issues.append(issue)
                if probe_result.ok:
                    selected_probe = probe_result
                    break
            deleted_channel_ids: list[int] = []
            channel_id: int | None = None
            channel_test_ok: bool | None = None
            channel_test_message: str | None = None
            channel_test_time_seconds: float | None = None
            if selected_probe and selected_probe.selected_source and selected_probe.selected_model:
                deleted_channel_ids = _delete_existing_channels(newapi_client, name)
                channel = newapi_client.create_channel(
                    ChannelSpec(
                        name=name,
                        channel_base_url=selected_probe.selected_source.channel_base_url,
                        api_key=selected_probe.selected_source.api_key,
                        exposed_model=selected_probe.selected_model,
                    ),
                    model_name=selected_probe.selected_model,
                )
                raw_channel_id = channel.get("id")
                if isinstance(raw_channel_id, int):
                    channel_id = raw_channel_id
                    test_response = newapi_client.test_channel(channel_id, selected_probe.selected_model)
                    channel_test_ok = bool(test_response.get("success"))
                    channel_test_message = str(test_response.get("message") or "") or None
                    raw_time = test_response.get("time")
                    if raw_time is not None:
                        try:
                            channel_test_time_seconds = float(raw_time)
                        except (TypeError, ValueError):
                            channel_test_time_seconds = None
            elif cleanup_failed_existing:
                deleted_channel_ids = _delete_existing_channels(newapi_client, name)
            outcomes.append(
                RelayOutcome(
                    name=name,
                    selected_model=selected_probe.selected_model if selected_probe else None,
                    selected_source_labels=selected_probe.selected_source.source_labels if selected_probe and selected_probe.selected_source else [],
                    selected_base_url=selected_probe.selected_source.base_url if selected_probe and selected_probe.selected_source else None,
                    direct_probe_ok=bool(selected_probe and selected_probe.ok),
                    channel_id=channel_id,
                    channel_test_ok=channel_test_ok,
                    channel_test_message=channel_test_message,
                    channel_test_time_seconds=channel_test_time_seconds,
                    deleted_existing_channel_ids=deleted_channel_ids,
                    attempts=all_attempts,
                    issues=merged_issues,
                )
            )
    passed = [item.name for item in outcomes if item.direct_probe_ok and item.channel_test_ok]
    direct_only = [item.name for item in outcomes if item.direct_probe_ok and not item.channel_test_ok]
    failed = [item.name for item in outcomes if not item.direct_probe_ok]
    return {
        "providers_root": str(providers_root),
        "prompt": prompt,
        "candidate_models": list(models),
        "retries_per_model": retries,
        "include_root_key": include_root_key,
        "cleanup_failed_existing": cleanup_failed_existing,
        "skipped_sources": skipped,
        "outcomes": [asdict(item) for item in outcomes],
        "passed": passed,
        "direct_only": direct_only,
        "failed": failed,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe ai-api/codex relays with gpt-5.4 xhigh then gpt-5.2 xhigh, import the passing ones into New API, and optionally clean up stale channels."
    )
    parser.add_argument("--base-url", required=True, help="New API console base URL, for example http://192.168.232.141:3000")
    parser.add_argument("--username", required=True, help="New API admin username")
    parser.add_argument("--password", required=True, help="New API admin password")
    parser.add_argument("--providers-root", default=str(ROOT), help="Directory containing relay folders, defaults to ai-api/codex")
    parser.add_argument("--prompt", default=DEFAULT_PROMPT, help="Prompt used for direct /responses probes")
    parser.add_argument("--model", action="append", default=[], help="Probe model. Can be passed multiple times. Default order: gpt-5.4, gpt-5.2")
    parser.add_argument("--retries", type=int, default=1, help="Attempts per model before falling back to the next model")
    parser.add_argument("--no-root-key", action="store_true", help="Do not include ai-api/codex/key.txt as a relay source")
    parser.add_argument("--keep-failed-existing", action="store_true", help="Keep existing same-name channels when the direct probe fails")
    parser.add_argument("--out", default=None, help="Optional JSON summary path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    models = tuple(args.model) if args.model else DEFAULT_MODELS
    providers_root = Path(args.providers_root).resolve()
    newapi_client = NewAPIClient(args.base_url, args.username, args.password)
    try:
        setup_response = newapi_client.ensure_setup()
        login_response = newapi_client.login()
        summary = sync_relays(
            newapi_client=newapi_client,
            providers_root=providers_root,
            prompt=args.prompt,
            models=models,
            retries=max(args.retries, 1),
            include_root_key=not args.no_root_key,
            cleanup_failed_existing=not args.keep_failed_existing,
        )
    finally:
        newapi_client.close()
    summary["base_url"] = args.base_url.rstrip("/")
    summary["setup"] = setup_response
    summary["login"] = {
        "success": bool(login_response.get("success")),
        "user_id": login_response.get("data", {}).get("id"),
    }
    if args.out:
        out_path = Path(args.out).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if not summary["direct_only"] and not summary["failed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
