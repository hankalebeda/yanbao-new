from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import re
import socket
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import httpx

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = str(os.environ.get(name, "")).strip()
    try:
        return max(minimum, int(raw))
    except (TypeError, ValueError):
        return default


ROOT = Path(__file__).resolve().parent
DEFAULT_LIVE_TRUTH_PATH = ROOT / "newapi_live_truth.json"
DEFAULT_CHANNEL_REGISTRY_PATH = ROOT / "newapi_channel_registry.json"
DEFAULT_EXCLUDES: set[str] = set()
DEFAULT_MODEL = "gpt-5.4"
DEFAULT_CODEX_MODEL = "gpt-5.3-codex"
DEFAULT_FALLBACK_MODEL = "gpt-5.2"
DEFAULT_REVIEW_MODEL = DEFAULT_CODEX_MODEL
DEFAULT_DEGRADED_REVIEW_MODEL = DEFAULT_FALLBACK_MODEL
DEFAULT_CANDIDATE_MODELS = [DEFAULT_MODEL, DEFAULT_CODEX_MODEL, DEFAULT_FALLBACK_MODEL]
DEFAULT_REASONING_EFFORT = "xhigh"
DEFAULT_UPSTREAM_PROBE_TIMEOUT = 180.0
DEFAULT_UPSTREAM_PROBE_ATTEMPTS = 3
DEFAULT_REQUIRED_PROBE_SUCCESSES = 2
DEFAULT_SOURCE_PROBE_MAX_WORKERS = _env_int("NEW_API_SOURCE_PROBE_MAX_WORKERS", 4)
DEFAULT_CANDIDATE_PROBE_MAX_WORKERS = _env_int("NEW_API_CANDIDATE_PROBE_MAX_WORKERS", 4)
DEFAULT_LOG_PAGE_SIZE = 100
DEFAULT_TOKEN_NAME = "codex-relay-xhigh"
DEFAULT_SYNC_LOCK_PATH = ROOT / ".sync_newapi_channels.lock"
# Persistent local store for relay token keys — prevents key regeneration on every run.
# Governance writes keys here on first creation; subsequent runs read from here.
DEFAULT_TOKEN_KEY_STORE_PATH = ROOT / ".token_key_store.json"
# Hard failures → channel should be retired (not auto-recoverable)
HARD_BLOCKING_LOG_SIGNATURES = {
    "auth_unavailable",
    "bad_response_body",
    "model_not_found",
    "token expired",
    "token invalidated",
    "unsupported legacy protocol",
}
# Soft/transient failures → channel should be quarantined (auto-recoverable)
SOFT_BLOCKING_LOG_SIGNATURES = {
    "status_code=429",
    "status_code=500",
    "status_code=503",
    "service temporarily unavailable",
    "too many requests",
    "system_cpu_overloaded",
}
# Combined set for backward-compat callers that just need "any block reason"
BLOCKING_LOG_SIGNATURES = HARD_BLOCKING_LOG_SIGNATURES | SOFT_BLOCKING_LOG_SIGNATURES

# Lane / group constants for dual-track isolation
LANE_STABLE = "codex-stable"
LANE_READONLY = "codex-readonly"
# Readonly provider-identity shards (failure-domain isolation).
# These are used as provider-home suffixes and as dedicated lane/group identities.
READONLY_SHARDS = ["ro-a", "ro-b", "ro-c", "ro-d"]
LANE_RO_A = "codex-ro-a"
LANE_RO_B = "codex-ro-b"
LANE_RO_C = "codex-ro-c"
LANE_RO_D = "codex-ro-d"
GATEWAY_LANE_SUFFIXES = ["stable", *READONLY_SHARDS]
GATEWAY_LANE_CHANNEL_SEPARATOR = "__lane__"
DEFAULT_LANE = LANE_READONLY
LANE_GROUPS = {
    LANE_STABLE: "codex-stable",
    LANE_READONLY: "codex-readonly",
    LANE_RO_A: "codex-ro-a",
    LANE_RO_B: "codex-ro-b",
    LANE_RO_C: "codex-ro-c",
    LANE_RO_D: "codex-ro-d",
}
RUNTIME_GROUP_LABELS = {
    LANE_READONLY: "Codex只读",
    LANE_STABLE: "Codex稳定",
    LANE_RO_A: "Codex只读A",
    LANE_RO_B: "Codex只读B",
    LANE_RO_C: "Codex只读C",
    LANE_RO_D: "Codex只读D",
}
RUNTIME_GROUP_DESCRIPTIONS = {
    LANE_READONLY: "Codex readonly",
    LANE_STABLE: "Codex stable",
    LANE_RO_A: "Codex readonly A",
    LANE_RO_B: "Codex readonly B",
    LANE_RO_C: "Codex readonly C",
    LANE_RO_D: "Codex readonly D",
}
CHANNEL_PRIORITY_BY_MODEL = {
    DEFAULT_MODEL: 10,
    DEFAULT_CODEX_MODEL: 20,
    DEFAULT_FALLBACK_MODEL: 30,
}
# Known bad providers to exclude from channel pool
KNOWN_BAD_PROVIDERS: set[str] = {
    "1uan.kequan.me",
    "free.9e.nz",
    "api.925214.xyz",
    "api.aillm.cyou",
    "freeapi.dgbmc.top",
}

INVENTORY_CLASS_MANAGED = "managed"
INVENTORY_CLASS_MANUAL = "manual"
INVENTORY_CLASS_UNMANAGED = "unmanaged"
INVENTORY_CLASS_LANE_CLONE = "lane_clone"

LIVE_STATE_MANAGED_ACTIVE = "managed_active"
LIVE_STATE_QUARANTINE = "quarantine"
LIVE_STATE_DISABLED_ARCHIVE = "disabled_archive"
LIVE_STATE_LANE_FROZEN = "lane_frozen"

CHANNEL_TAG_MANAGED = "managed"
CHANNEL_TAG_MANUAL_ARCHIVE = "manual-archive"
CHANNEL_TAG_DRIFT_ARCHIVE = "drift-archive"
CHANNEL_TAG_LANE_FROZEN = "lane-frozen"


@dataclass
class ProviderSource:
    name: str
    base_url: str
    probe_base_urls: list[str]
    channel_base_url: str
    api_key: str
    upstream_model: str
    exposed_model: str
    model_mapping: dict[str, str] | None
    enabled: bool | None
    source_refs: list[str] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)


@dataclass
class ChannelResult:
    name: str
    channel_id: int | None
    create_ok: bool
    test_ok: bool
    message: str
    upstream_probe_ok: bool | None = None
    upstream_probe_model: str | None = None
    channel_test_ok: bool | None = None
    channel_test_message: str | None = None
    time_seconds: float | None = None
    base_url: str | None = None
    selected_base_url: str | None = None
    channel_base_url: str | None = None
    upstream_model: str | None = None
    exposed_model: str | None = None
    selected_model: str | None = None
    channel_models: list[str] = field(default_factory=list)
    channel_test_model: str | None = None
    channel_priority: int | None = None
    model_mapping: dict[str, str] | None = None
    source_refs: list[str] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)
    inventory_class: str = INVENTORY_CLASS_MANAGED
    upstream_probe_error: str | None = None
    error_detail: str | None = None


@dataclass
class ChannelLayout:
    channel_models: list[str]
    test_model: str
    priority: int
    model_mapping: dict[str, str] | None = None


def _split_models(value: str | None) -> list[str]:
    if not value:
        return []
    return _dedupe_models([item.strip() for item in str(value).split(",")])


def _parse_json_dict(raw: Any) -> dict[str, str] | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    result: dict[str, str] = {}
    for key, value in payload.items():
        key_text = str(key or "").strip()
        value_text = str(value or "").strip()
        if key_text and value_text:
            result[key_text] = value_text
    return result or None


def _serialize_model_mapping(model_mapping: dict[str, str] | None) -> str:
    return json.dumps(model_mapping, ensure_ascii=False) if model_mapping else ""


def _parse_json_object_option(raw: Any, *, option_key: str) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    text = str(raw).strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{option_key} is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{option_key} must be a JSON object, got {type(payload).__name__}")
    return dict(payload)


def _load_live_truth(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    data = _load_json(path)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def default_channel_registry() -> dict[str, Any]:
    return {
        "defaults": {
            "auto_disable": True,
            "auto_enable": False,
            "allow_delete": False,
        },
        "lane_policy": {
            "materialize_live_clones": False,
        },
        "manual_channels": [],
    }


def _load_channel_registry(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    data = _load_json(path)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def _text_value(raw: Any) -> str:
    return str(raw or "").strip()


def _truth_errors(payload: dict[str, Any]) -> list[str]:
    if not payload:
        return ["truth file missing or empty"]
    errors: list[str] = []
    if not _truth_string_list(payload, "managed_sources"):
        errors.append("truth.managed_sources must be a non-empty list")
    if not _truth_string_list(payload, "candidate_models"):
        errors.append("truth.candidate_models must be a non-empty list")
    token_name = _text_value(payload.get("token_name"))
    if not token_name:
        errors.append("truth.token_name is required")
    for key in ("disable_unmanaged_candidates", "include_root_key_sources"):
        value = payload.get(key)
        if value is not None and not isinstance(value, bool):
            errors.append(f"truth.{key} must be a boolean when present")
    return errors


def _registry_entry(
    entry: dict[str, Any],
    *,
    defaults: dict[str, Any],
    index: int,
    errors: list[str],
) -> dict[str, Any]:
    name = _text_value(entry.get("name"))
    base_url = _text_value(entry.get("base_url")).rstrip("/")
    if not name:
        errors.append(f"registry.manual_channels[{index}].name is required")
    if not base_url:
        errors.append(f"registry.manual_channels[{index}].base_url is required")
    result = {
        "name": name,
        "base_url": base_url,
        "preserve": bool(entry.get("preserve", True)),
        "auto_disable": bool(entry.get("auto_disable", defaults.get("auto_disable", True))),
        "auto_enable": bool(entry.get("auto_enable", defaults.get("auto_enable", False))),
        "allow_delete": bool(entry.get("allow_delete", defaults.get("allow_delete", False))),
        "source": _text_value(entry.get("source")) or "manual",
        "notes": _text_value(entry.get("notes")),
    }
    if result["allow_delete"]:
        errors.append(f"registry.manual_channels[{index}] allow_delete=true is forbidden in phase 1")
    if result["auto_enable"]:
        errors.append(f"registry.manual_channels[{index}] auto_enable=true is forbidden in phase 1")
    return result


def _registry_errors(payload: dict[str, Any]) -> list[str]:
    if not payload:
        return ["registry file missing or empty"]
    errors: list[str] = []
    defaults = payload.get("defaults")
    if defaults is None:
        errors.append("registry.defaults is required")
        defaults = {}
    if not isinstance(defaults, dict):
        errors.append("registry.defaults must be an object")
        defaults = {}
    lane_policy = payload.get("lane_policy")
    if lane_policy is None:
        errors.append("registry.lane_policy is required")
        lane_policy = {}
    if not isinstance(lane_policy, dict):
        errors.append("registry.lane_policy must be an object")
        lane_policy = {}
    materialize_live_clones = lane_policy.get("materialize_live_clones")
    if materialize_live_clones not in (True, False):
        errors.append("registry.lane_policy.materialize_live_clones must be a boolean")
    manual_channels = payload.get("manual_channels")
    if manual_channels is None:
        errors.append("registry.manual_channels is required")
        manual_channels = []
    if not isinstance(manual_channels, list):
        errors.append("registry.manual_channels must be a list")
        manual_channels = []

    seen_names: set[str] = set()
    seen_base_urls: set[str] = set()
    for index, raw_entry in enumerate(manual_channels):
        if not isinstance(raw_entry, dict):
            errors.append(f"registry.manual_channels[{index}] must be an object")
            continue
        entry = _registry_entry(raw_entry, defaults=defaults, index=index, errors=errors)
        if entry["name"]:
            if entry["name"] in seen_names:
                errors.append(f"registry.manual_channels[{index}] duplicate name: {entry['name']}")
            seen_names.add(entry["name"])
        if entry["base_url"]:
            if entry["base_url"] in seen_base_urls:
                errors.append(f"registry.manual_channels[{index}] duplicate base_url: {entry['base_url']}")
            seen_base_urls.add(entry["base_url"])
    return errors


def _manual_registry_entries(registry: dict[str, Any]) -> list[dict[str, Any]]:
    defaults = registry.get("defaults") if isinstance(registry.get("defaults"), dict) else {}
    result: list[dict[str, Any]] = []
    for index, raw_entry in enumerate(registry.get("manual_channels") or []):
        if not isinstance(raw_entry, dict):
            continue
        errors: list[str] = []
        entry = _registry_entry(raw_entry, defaults=defaults, index=index, errors=errors)
        if errors:
            continue
        result.append(entry)
    return result


def _manual_registry_alias_map(registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    alias_map: dict[str, dict[str, Any]] = {}
    for entry in _manual_registry_entries(registry):
        aliases = {
            _channel_identity(entry.get("name")),
            _channel_identity(entry.get("base_url")),
        }
        for alias in aliases:
            if alias:
                alias_map[alias] = entry
    return alias_map


def build_manual_channel_registry(
    channels: list[dict[str, Any]],
    *,
    sources: list[ProviderSource],
    candidate_models: list[str],
) -> dict[str, Any]:
    registry = default_channel_registry()
    source_aliases = {alias for source in sources for alias in _source_aliases(source)}
    seen_names: set[str] = set()
    for channel in channels:
        name = _text_value(channel.get("name"))
        if not name:
            continue
        base_name, lane_suffix = parse_gateway_lane_channel_name(name)
        if lane_suffix:
            continue
        aliases = _channel_aliases(channel)
        if aliases & source_aliases:
            continue
        if not _channel_targets_candidate_models(channel, candidate_models):
            continue
        if name in seen_names:
            continue
        seen_names.add(name)
        registry["manual_channels"].append(
            {
                "name": name,
                "base_url": _text_value(channel.get("base_url")).rstrip("/"),
                "preserve": True,
                "auto_disable": True,
                "auto_enable": False,
                "allow_delete": False,
                "source": "live_import",
                "notes": f"imported from current live channel inventory ({base_name or name})",
            }
        )
    return registry


def _truth_string_list(payload: dict[str, Any], key: str) -> list[str]:
    raw = payload.get(key)
    if not isinstance(raw, list):
        return []
    values: list[str] = []
    for item in raw:
        clean = str(item or "").strip()
        if clean and clean not in values:
            values.append(clean)
    return values


def _truth_bool(payload: dict[str, Any], key: str, default: bool) -> bool:
    raw = payload.get(key)
    if isinstance(raw, bool):
        return raw
    return default


def _channel_identity(value: str | None) -> str:
    clean = str(value or "").strip().rstrip("/")
    if not clean:
        return ""
    parsed = urlsplit(clean)
    return (parsed.netloc or parsed.path or clean).strip().lower()


def _channel_aliases(channel: dict[str, Any]) -> set[str]:
    base_name, lane_suffix = parse_gateway_lane_channel_name(channel.get("name"))
    aliases = {
        _channel_identity(channel.get("name")),
        _channel_identity(channel.get("base_url")),
    }
    if lane_suffix and base_name:
        aliases.add(_channel_identity(base_name))
    return {item for item in aliases if item}


def _source_aliases(source: ProviderSource) -> set[str]:
    aliases = {
        _channel_identity(source.name),
        _channel_identity(source.base_url),
        _channel_identity(source.channel_base_url),
    }
    return {item for item in aliases if item}


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


def _non_empty_lines(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]


def _infer_source_name_from_url(base_url: str) -> str:
    parsed = urlsplit(base_url.strip())
    return (parsed.netloc or parsed.path or "unnamed-provider").strip()


def _key_entries(path: Path, *, default_name: str | None = None) -> tuple[list[dict[str, str]], list[str]]:
    if not path.exists():
        return [], []
    lines = _non_empty_lines(path)
    entries: list[dict[str, str]] = []
    issues: list[str] = []
    if len(lines) % 2 != 0:
        issues.append(f"{path} has an odd number of non-empty lines; trailing line ignored")
    for index in range(0, len(lines) - 1, 2):
        base_url = lines[index]
        api_key = lines[index + 1]
        name = default_name or _infer_source_name_from_url(base_url)
        entries.append(
            {
                "name": name,
                "base_url": base_url,
                "api_key": api_key,
            }
        )
    return entries, issues


def _iter_provider_dirs(root: Path, excludes: set[str]) -> list[Path]:
    result: list[Path] = []
    for item in sorted(root.iterdir()):
        if not item.is_dir():
            continue
        if item.name.startswith("portable_") or item.name == "__pycache__":
            continue
        if item.name.startswith("newapi-"):
            continue
        if item.name in excludes:
            continue
        result.append(item)
    return result


def _normalize_model(model: str) -> tuple[str, dict[str, str] | None, list[str]]:
    clean = model.strip()
    issues: list[str] = []
    if clean == DEFAULT_MODEL:
        return DEFAULT_MODEL, None, issues
    if clean == "gpt-5.4-fast":
        issues.append("upstream model is gpt-5.4-fast, mapped to gpt-5.4")
        return DEFAULT_MODEL, {DEFAULT_MODEL: clean}, issues
    issues.append(f"upstream model {clean or '<missing>'} is not normalized to {DEFAULT_MODEL}")
    return clean or DEFAULT_MODEL, None, issues


def _dedupe_models(models: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for model_name in models:
        clean = model_name.strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        result.append(clean)
    return result


def build_channel_layout(
    source: ProviderSource,
    *,
    selected_model: str,
    candidate_models: list[str],
) -> ChannelLayout:
    ordered_candidates = _dedupe_models(candidate_models)
    if selected_model in ordered_candidates:
        selected_index = ordered_candidates.index(selected_model)
        # Only advertise the selected model and lower-priority explicit failover
        # models. This prevents gpt-5.4 requests from silently downshifting to a
        # lower model through channel-side model_mapping.
        channel_models = ordered_candidates[selected_index:]
    else:
        channel_models = [selected_model]

    model_mapping: dict[str, str] = {}
    if selected_model == source.exposed_model and source.model_mapping:
        model_mapping.update(source.model_mapping)

    test_model = selected_model
    return ChannelLayout(
        channel_models=channel_models,
        test_model=test_model,
        priority=CHANNEL_PRIORITY_BY_MODEL.get(selected_model, 0),
        model_mapping=model_mapping or None,
    )


def _normalize_channel_base_url(base_url: str) -> tuple[str, list[str]]:
    clean = base_url.strip().rstrip("/")
    issues: list[str] = []
    parsed = urlsplit(clean)
    if parsed.path == "/v1":
        normalized = urlunsplit((parsed.scheme, parsed.netloc, "", parsed.query, parsed.fragment))
        issues.append("channel base_url normalized from /v1 endpoint to root endpoint")
        return normalized.rstrip("/"), issues
    return clean, issues


def _build_probe_base_urls(base_url: str) -> list[str]:
    clean = base_url.strip().rstrip("/")
    if not clean:
        return []
    candidates = [clean]
    normalized = _gateway_openai_base_url(clean)
    if normalized not in candidates:
        candidates.append(normalized)
    return candidates


def _load_api_key(provider_dir: Path) -> tuple[str, list[str]]:
    key_path = provider_dir / "key.txt"
    if key_path.exists():
        entries, issues = _key_entries(key_path, default_name=provider_dir.name)
        if entries:
            return entries[0]["api_key"], issues

    auth_path = provider_dir / "auth.json"
    if auth_path.exists():
        auth = _load_json(auth_path)
        api_key = str(
            auth.get("OPENAI_API_KEY") or auth.get("api_key") or auth.get("apiKey") or ""
        ).strip()
        return api_key, []

    return "", []


def _gateway_provider_dir_name(base_url: str, *, explicit_name: str | None = None) -> str:
    if explicit_name:
        return explicit_name.strip()
    parsed = urlsplit(base_url)
    host = parsed.netloc or parsed.path or "newapi-gateway"
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", host).strip("-")
    return f"newapi-{normalized or 'gateway'}"


def _gateway_openai_base_url(base_url: str) -> str:
    clean = base_url.rstrip("/")
    parsed = urlsplit(clean)
    if parsed.path.endswith("/v1"):
        return clean
    base_path = parsed.path.rstrip("/")
    normalized_path = f"{base_path}/v1" if base_path else "/v1"
    return urlunsplit((parsed.scheme, parsed.netloc, normalized_path, parsed.query, parsed.fragment)).rstrip("/")


def lane_for_shard(shard: str) -> str:
    shard = str(shard or "").strip().lower()
    if shard == "ro-a":
        return LANE_RO_A
    if shard == "ro-b":
        return LANE_RO_B
    if shard == "ro-c":
        return LANE_RO_C
    if shard == "ro-d":
        return LANE_RO_D
    raise ValueError(f"unknown shard: {shard}")


def gateway_provider_home_name(base_provider_name: str, *, suffix: str | None = None) -> str:
    """Compute ai-api/codex provider-home dir name for the New API gateway.

    Backward compatibility: when suffix is empty/None, returns the base name.
    """

    base = str(base_provider_name or "").strip()
    if not base:
        raise ValueError("base_provider_name is required")
    clean_suffix = str(suffix or "").strip().lower()
    if not clean_suffix:
        return base
    return f"{base}-{clean_suffix}"


def gateway_token_name(base_token_name: str, *, suffix: str | None = None) -> str:
    """Compute token name to be created on New API for a given gateway home.

    Backward compatibility: when suffix is empty/None, returns base_token_name.
    """

    base = str(base_token_name or "").strip() or DEFAULT_TOKEN_NAME
    clean_suffix = str(suffix or "").strip().lower()
    if not clean_suffix:
        return base
    return f"{base}-{clean_suffix}"


def lane_for_gateway_suffix(suffix: str) -> str:
    """Map gateway home suffix to a New API lane/group identity."""

    clean = str(suffix or "").strip().lower()
    if clean == "stable":
        return LANE_STABLE
    if clean in READONLY_SHARDS:
        return lane_for_shard(clean)
    raise ValueError(f"unknown gateway suffix: {suffix}")


def selected_gateway_lane_suffixes(raw: str | None) -> list[str]:
    requested = {item.strip().lower() for item in str(raw or "").split(",") if item.strip()}
    shard_suffixes = [suffix for suffix in READONLY_SHARDS if suffix in requested] if requested else list(READONLY_SHARDS)
    return ["stable", *shard_suffixes]


def runtime_groups_for_gateway(*, include_shards: bool, gateway_provider_shards: str | None = None) -> list[str]:
    groups = [DEFAULT_LANE]
    if include_shards:
        for suffix in selected_gateway_lane_suffixes(gateway_provider_shards):
            lane = lane_for_gateway_suffix(suffix)
            if lane not in groups:
                groups.append(lane)
    return groups


def gateway_lane_channel_name(base_name: str, *, suffix: str) -> str:
    base = str(base_name or "").strip()
    clean_suffix = str(suffix or "").strip().lower()
    if not base:
        raise ValueError("base_name is required")
    if clean_suffix not in GATEWAY_LANE_SUFFIXES:
        raise ValueError(f"unknown gateway lane suffix: {suffix}")
    return f"{base}{GATEWAY_LANE_CHANNEL_SEPARATOR}{clean_suffix}"


def parse_gateway_lane_channel_name(name: str | None) -> tuple[str, str | None]:
    clean = str(name or "").strip()
    if not clean or GATEWAY_LANE_CHANNEL_SEPARATOR not in clean:
        return (clean, None)
    base_name, suffix = clean.rsplit(GATEWAY_LANE_CHANNEL_SEPARATOR, 1)
    clean_suffix = suffix.strip().lower()
    if not base_name or clean_suffix not in GATEWAY_LANE_SUFFIXES:
        return (clean, None)
    return (base_name, clean_suffix)


def write_gateway_provider_dir(
    *,
    providers_root: Path,
    base_url: str,
    token_key: str,
    provider_name: str,
    model: str = DEFAULT_MODEL,
    review_model: str = DEFAULT_REVIEW_MODEL,
    reasoning_effort: str = "xhigh",
) -> Path:
    provider_dir = providers_root / provider_name
    provider_dir.mkdir(parents=True, exist_ok=True)
    endpoint = base_url.rstrip("/")
    openai_base_url = _gateway_openai_base_url(endpoint)
    homepage = urlunsplit((urlsplit(endpoint).scheme, urlsplit(endpoint).netloc, "", "", "")).rstrip("/")
    provider_payload = {
        "name": provider_name,
        "endpoint": openai_base_url,
        "model": model,
        "review_model": review_model,
        "fallback_models": list(DEFAULT_CANDIDATE_MODELS),
        "homepage": homepage or endpoint,
        "enabled": True,
        "resource": "provider",
        "app": "codex",
    }
    config_text = "\n".join(
        [
            'model_provider = "OpenAI"',
            f'model = "{model}"',
            f'review_model = "{review_model}"',
            f"fallback_models = {json.dumps(list(DEFAULT_CANDIDATE_MODELS), ensure_ascii=False)}",
            f'model_reasoning_effort = "{reasoning_effort}"',
            "disable_response_storage = true",
            'network_access = "enabled"',
            "windows_wsl_setup_acknowledged = true",
            "model_context_window = 1000000",
            "model_auto_compact_token_limit = 900000",
            'personality = "pragmatic"',
            "",
            "[model_providers.OpenAI]",
            'name = "OpenAI"',
            f'base_url = "{openai_base_url}"',
            'wire_api = "responses"',
            "supports_websockets = false",
            "requires_openai_auth = true",
            "",
            "[features]",
            "responses_websockets_v2 = false",
            "multi_agent = true",
            "",
            "[windows]",
            'sandbox = "elevated"',
            "",
        ]
    )
    (provider_dir / "provider.json").write_text(json.dumps(provider_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (provider_dir / "auth.json").write_text(
        json.dumps({"OPENAI_API_KEY": token_key}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (provider_dir / "config.toml").write_text(config_text, encoding="utf-8")
    (provider_dir / "key.txt").write_text(f"{openai_base_url}\n{token_key}\n", encoding="utf-8")
    return provider_dir


def write_sharded_gateway_provider_dirs(
    *,
    providers_root: Path,
    base_url: str,
    base_provider_name: str,
    token_keys_by_suffix: dict[str, str],
    model: str = DEFAULT_MODEL,
    review_model: str = DEFAULT_REVIEW_MODEL,
    reasoning_effort: str = "xhigh",
) -> dict[str, Path]:
    """Write multiple provider homes for stable + readonly shards.

    This helper only writes local provider directories. It does not mutate New API.
    """

    written: dict[str, Path] = {}
    for suffix, token_key in token_keys_by_suffix.items():
        home_name = gateway_provider_home_name(base_provider_name, suffix=suffix)
        written[suffix] = write_gateway_provider_dir(
            providers_root=providers_root,
            base_url=base_url,
            token_key=token_key,
            provider_name=home_name,
            model=model,
            review_model=review_model,
            reasoning_effort=reasoning_effort,
        )
    return written


def _merge_source(
    sources_by_name: dict[str, ProviderSource],
    source: ProviderSource,
) -> None:
    existing = sources_by_name.get(source.name)
    if existing is None:
        sources_by_name[source.name] = source
        return

    for ref in source.source_refs:
        if ref not in existing.source_refs:
            existing.source_refs.append(ref)
    for issue in source.issues:
        if issue not in existing.issues:
            existing.issues.append(issue)
    if source.base_url and source.base_url != existing.base_url:
        existing.issues.append(
            f"alternate base_url from {', '.join(source.source_refs) or 'unknown source'}: {source.base_url}"
        )
    if source.api_key and source.api_key != existing.api_key:
        existing.issues.append(
            f"alternate api_key from {', '.join(source.source_refs) or 'unknown source'} differs from primary source"
        )
    if not existing.upstream_model and source.upstream_model:
        existing.upstream_model = source.upstream_model
    if not existing.exposed_model and source.exposed_model:
        existing.exposed_model = source.exposed_model
    if existing.enabled is None and source.enabled is not None:
        existing.enabled = source.enabled

def load_provider_sources(
    root: Path,
    excludes: set[str],
    *,
    include_root_key_sources: bool = False,
    managed_sources: set[str] | None = None,
) -> tuple[list[ProviderSource], list[dict[str, Any]]]:
    sources_by_name: dict[str, ProviderSource] = {}
    skipped: list[dict[str, Any]] = []
    discovered_names: set[str] = set()
    for provider_dir in _iter_provider_dirs(root, excludes):
        if managed_sources and provider_dir.name not in managed_sources:
            continue
        discovered_names.add(provider_dir.name)
        issues: list[str] = []
        config_path = provider_dir / "config.toml"
        provider_path = provider_dir / "provider.json"
        key_path = provider_dir / "key.txt"
        config = _load_toml(config_path) if config_path.exists() else {}
        provider_meta = _load_json(provider_path) if provider_path.exists() else {}
        provider_cfg = (config.get("model_providers") or {}).get("OpenAI") or {}
        key_entries, key_issues = _key_entries(key_path, default_name=provider_dir.name)
        issues.extend(key_issues)
        base_url = str(provider_cfg.get("base_url") or provider_meta.get("endpoint") or "").strip()
        if not base_url and key_entries:
            base_url = key_entries[0]["base_url"].strip()
        api_key, api_key_issues = _load_api_key(provider_dir)
        issues.extend(api_key_issues)
        upstream_model = str(provider_meta.get("model") or config.get("model") or "").strip()
        enabled = provider_meta.get("enabled") if provider_meta else None
        if not provider_path.exists():
            issues.append("provider.json missing")
        if not config_path.exists():
            issues.append("config.toml missing")
        if provider_dir.name in KNOWN_BAD_PROVIDERS:
            issues.append("legacy runs marked this provider as bad; rechecked in this pass")
        if enabled is False:
            issues.append("provider.json enabled=false, imported anyway for testing")
        if not base_url:
            skipped.append({"name": provider_dir.name, "reason": "missing base_url"})
            continue
        if not api_key:
            skipped.append({"name": provider_dir.name, "reason": "missing api_key"})
            continue
        exposed_model, model_mapping, model_issues = _normalize_model(upstream_model)
        issues.extend(model_issues)
        channel_base_url, base_url_issues = _normalize_channel_base_url(base_url)
        issues.extend(base_url_issues)
        _merge_source(
            sources_by_name,
            ProviderSource(
                name=provider_dir.name,
                base_url=base_url,
                probe_base_urls=_build_probe_base_urls(base_url),
                channel_base_url=channel_base_url,
                api_key=api_key,
                upstream_model=upstream_model,
                exposed_model=exposed_model,
                model_mapping=model_mapping,
                enabled=enabled,
                source_refs=[f"provider_dir:{provider_dir.name}"],
                issues=issues,
            ),
        )

    if include_root_key_sources:
        root_key_path = root / "key.txt"
        root_entries, root_issues = _key_entries(root_key_path)
        if root_issues:
            skipped.append({"name": "root:key.txt", "reason": "; ".join(root_issues)})
        for entry in root_entries:
            entry_name = entry["name"]
            if managed_sources and entry_name not in managed_sources:
                continue
            base_url = entry["base_url"].strip()
            channel_base_url, base_url_issues = _normalize_channel_base_url(base_url)
            _merge_source(
                sources_by_name,
                ProviderSource(
                    name=entry_name,
                    base_url=base_url,
                    probe_base_urls=_build_probe_base_urls(base_url),
                    channel_base_url=channel_base_url,
                    api_key=entry["api_key"].strip(),
                    upstream_model="",
                    exposed_model=DEFAULT_MODEL,
                    model_mapping=None,
                    enabled=True,
                    source_refs=[f"root_key:{root_key_path.name}"],
                    issues=base_url_issues,
                ),
            )

    if managed_sources:
        for missing in sorted(managed_sources - discovered_names):
            skipped.append({"name": missing, "reason": "managed source missing under providers_root"})

    return sorted(sources_by_name.values(), key=lambda item: item.name), skipped


class NewAPIClient:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.user_id: int | None = None
        self.client = httpx.Client(
            base_url=self.base_url,
            timeout=180.0,
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
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError(f"Expected JSON object, got {type(data).__name__}")
        return data

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

    def list_options(self) -> list[dict[str, Any]]:
        response = self._json(self._request("GET", "/api/option/"))
        data = response.get("data") or []
        return [item for item in data if isinstance(item, dict)]

    def option_values(self) -> dict[str, str]:
        values: dict[str, str] = {}
        for item in self.list_options():
            key = str(item.get("key") or "").strip()
            if not key:
                continue
            values[key] = str(item.get("value") or "")
        return values

    def update_option(self, key: str, value: str) -> dict[str, Any]:
        response = self._json(self._request("PUT", "/api/option/", json={"key": key, "value": value}))
        if response and not response.get("success", False):
            raise RuntimeError(f"update option {key} failed: {response}")
        return response

    def ensure_runtime_groups(
        self,
        groups: list[str],
        *,
        descriptions: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        desired_groups = [group for group in groups if str(group or "").strip()]
        options = self.option_values()
        group_ratio = _parse_json_object_option(options.get("GroupRatio"), option_key="GroupRatio")
        user_usable_groups = _parse_json_object_option(options.get("UserUsableGroups"), option_key="UserUsableGroups")
        description_map = descriptions or {}

        added_group_ratio: list[str] = []
        added_user_usable_groups: list[str] = []
        for group in desired_groups:
            if group not in group_ratio:
                group_ratio[group] = 1
                added_group_ratio.append(group)
            if group not in user_usable_groups:
                user_usable_groups[group] = description_map.get(group, group)
                added_user_usable_groups.append(group)

        if added_group_ratio:
            self.update_option("GroupRatio", json.dumps(group_ratio, ensure_ascii=False))
        if added_user_usable_groups:
            self.update_option("UserUsableGroups", json.dumps(user_usable_groups, ensure_ascii=False))

        return {
            "groups": desired_groups,
            "group_ratio_added": added_group_ratio,
            "user_usable_groups_added": added_user_usable_groups,
            "group_ratio_updated": bool(added_group_ratio),
            "user_usable_groups_updated": bool(added_user_usable_groups),
        }

    def _paged_items(
        self,
        *,
        path_template: str,
        page_size: int,
        max_items: int | None = None,
        start_page: int = 1,
    ) -> list[dict[str, Any]]:
        page = start_page
        collected: list[dict[str, Any]] = []
        effective_page_size = max(1, int(page_size))
        while True:
            response = self._json(self._request("GET", path_template.format(page=page, page_size=effective_page_size)))
            data = response.get("data") or {}
            items = [item for item in (data.get("items") or []) if isinstance(item, dict)]
            if not items:
                break
            collected.extend(items)
            if max_items is not None and len(collected) >= max_items:
                return collected[:max_items]
            if len(items) < effective_page_size:
                break
            page += 1
        return collected

    def _list_channels(self, *, page_size: int = 200) -> list[dict[str, Any]]:
        return self._paged_items(
            path_template="/api/channel/?p={page}&page_size={page_size}",
            page_size=page_size,
        )

    def find_channel_by_name(self, name: str) -> dict[str, Any] | None:
        channels = self.find_channels_by_name(name)
        return channels[0] if channels else None

    def find_channels_by_name(self, name: str) -> list[dict[str, Any]]:
        return [item for item in self._list_channels() if item.get("name") == name]

    def get_channel(self, channel_id: int) -> dict[str, Any]:
        response = self._json(self._request("GET", f"/api/channel/{channel_id}"))
        data = response.get("data")
        if not isinstance(data, dict):
            raise RuntimeError(f"channel {channel_id} missing from response: {response}")
        return data

    def delete_channel(self, channel_id: int) -> None:
        response = self._json(self._request("DELETE", f"/api/channel/{channel_id}"))
        if not response.get("success"):
            raise RuntimeError(f"delete channel failed: {response}")

    def update_channel(
        self,
        channel: dict[str, Any],
        *,
        channel_models: list[str] | None = None,
        test_model: str | None = None,
        priority: int | None = None,
        weight: int | None = None,
        status: int | None = None,
        model_mapping: dict[str, str] | None | object = ...,
        lane: str | None = None,
        name: str | None = None,
        tag: str | None = None,
        remark: str | None = None,
    ) -> dict[str, Any]:
        channel_id = channel.get("id")
        if not isinstance(channel_id, int):
            raise RuntimeError(f"channel id missing: {channel}")
        payload: dict[str, Any] = {
            "id": channel_id,
            "name": str(name if name is not None else (channel.get("name") or "")),
            "type": int(channel.get("type") or 1),
            "models": ",".join(_dedupe_models(channel_models)) if channel_models is not None else str(channel.get("models") or ""),
            "group": str(
                LANE_GROUPS.get(lane, lane)
                if lane is not None
                else (channel.get("group") or LANE_GROUPS.get(DEFAULT_LANE, DEFAULT_LANE))
            ),
            "key": str(channel.get("key") or ""),
            "base_url": str(channel.get("base_url") or ""),
            "priority": int(priority if priority is not None else (channel.get("priority") or 0)),
            "weight": int(weight if weight is not None else (channel.get("weight") or 0)),
            "status": int(status if status is not None else (channel.get("status") or 0)),
            "auto_ban": 0,
            "test_model": str(test_model if test_model is not None else (channel.get("test_model") or "")),
            "status_code_mapping": str(channel.get("status_code_mapping") or ""),
            "other": str(channel.get("other") or ""),
            "setting": str(channel.get("setting") or ""),
            "param_override": str(channel.get("param_override") or ""),
            "tag": str(tag if tag is not None else (channel.get("tag") or "")),
            "remark": str(remark if remark is not None else (channel.get("remark") or "")),
        }
        if model_mapping is ...:
            payload["model_mapping"] = str(channel.get("model_mapping") or "")
        else:
            payload["model_mapping"] = _serialize_model_mapping(model_mapping)
        response = self._json(self._request("PUT", "/api/channel/", json=payload))
        if response and not response.get("success", False):
            raise RuntimeError(f"update channel failed: {response}")
        data = response.get("data")
        if isinstance(data, dict):
            return data
        return self.get_channel(channel_id)

    def create_channel(
        self,
        source: ProviderSource,
        *,
        channel_models: list[str],
        test_model: str,
        priority: int,
        model_mapping: dict[str, str] | None = None,
        lane: str = DEFAULT_LANE,
        name: str | None = None,
        tag: str | None = None,
        remark: str | None = None,
    ) -> dict[str, Any]:
        models_value = ",".join(_dedupe_models(channel_models))
        group = LANE_GROUPS.get(lane, lane)
        channel_payload: dict[str, Any] = {
            "name": str(name or source.name),
            "type": 1,
            "models": models_value,
            "group": group,
            "key": source.api_key,
            "base_url": source.channel_base_url,
            "priority": priority,
            "weight": 100,
            "status": 1,
            "auto_ban": 0,
            "test_model": test_model,
            "tag": str(tag or ""),
            "remark": str(remark or ""),
        }
        if model_mapping:
            channel_payload["model_mapping"] = json.dumps(model_mapping, ensure_ascii=False)
        response = self._json(
            self._request(
                "POST",
                "/api/channel/",
                json={"mode": "single", "channel": channel_payload},
            )
        )
        if response and not response.get("success", False):
            raise RuntimeError(f"create channel failed: {response}")
        channel = self.find_channel_by_name(str(name or source.name))
        if channel is None:
            raise RuntimeError(f"channel {str(name or source.name)} was not found after creation")
        return channel

    def test_channel(self, channel_id: int, model_name: str) -> dict[str, Any]:
        path = f"/api/channel/test/{channel_id}?model={quote(model_name)}"
        return self._json(self._request("GET", path))

    def list_logs(self, *, page_size: int = DEFAULT_LOG_PAGE_SIZE, page: int = 1) -> list[dict[str, Any]]:
        max_items = max(0, int(page_size))
        if max_items == 0:
            return []
        return self._paged_items(
            path_template="/api/log/?p={page}&page_size={page_size}",
            page_size=min(max_items, DEFAULT_LOG_PAGE_SIZE),
            max_items=max_items,
            start_page=page,
        )

    def list_tokens(self) -> list[dict[str, Any]]:
        response = self._json(self._request("GET", "/api/token/?p=1&size=200"))
        data = response.get("data") or {}
        items = data.get("items") or []
        return [item for item in items if isinstance(item, dict)]

    def find_token_by_name(self, name: str) -> dict[str, Any] | None:
        for item in self.list_tokens():
            if isinstance(item, dict) and item.get("name") == name:
                return item
        return None

    def create_token(
        self,
        name: str,
        *,
        lane: str = DEFAULT_LANE,
        key_store_path: Path | None = None,
    ) -> dict[str, Any]:
        """Create or reuse a named token on New API.

        Key Stability:  
        The first time a token is created (or after a fresh provision), the
        plaintext key is fetched from the API and written to the local key
        store (`key_store_path`).  On subsequent runs the stored key is returned
        WITHOUT calling the potentially-regenerating `/api/token/{id}/key`
        endpoint, keeping relay keys stable across governance cycles.
        """
        store_path = key_store_path or DEFAULT_TOKEN_KEY_STORE_PATH
        stored_keys = _load_token_key_store(store_path)
        # If we already have a stable stored key for this token, skip API call
        if name in stored_keys:
            existing = self.find_token_by_name(name)
            if existing is None:
                # Token was deleted externally — create it fresh
                stored_keys.pop(name, None)
            else:
                existing["full_key"] = stored_keys[name]
                return existing

        existing = self.find_token_by_name(name)
        if existing is None:
            group = LANE_GROUPS.get(lane, lane)
            payload = {
                "name": name,
                "remain_quota": 500000000,
                "unlimited_quota": True,
                "expired_time": -1,
                "group": group,
                "cross_group_retry": False,
            }
            response = self._json(self._request("POST", "/api/token/", json=payload))
            if not response.get("success"):
                raise RuntimeError(f"create token failed: {response}")
            existing = self.find_token_by_name(name)
        if existing is None:
            raise RuntimeError(f"token {name} not found after creation")
        token_id = existing.get("id")
        if not isinstance(token_id, int):
            raise RuntimeError(f"token id missing: {existing}")
        key_response = self._json(self._request("POST", f"/api/token/{token_id}/key"))
        data = key_response.get("data") or {}
        key = data.get("key")
        if not isinstance(key, str) or not key:
            raise RuntimeError(f"token key missing: {key_response}")
        existing["full_key"] = key
        # Persist key so future runs don't call the key endpoint again
        stored_keys[name] = key
        _save_token_key_store(store_path, stored_keys)
        return existing


def _load_token_key_store(path: Path) -> dict[str, str]:
    """Load the local relay token key store (name → full_key mapping)."""
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items() if k and v}
    except Exception:
        pass
    return {}


def _save_token_key_store(path: Path, keys: dict[str, str]) -> None:
    """Persist the relay token key store atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(keys, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)



def _candidate_probe_targets(source: ProviderSource, candidate_models: list[str]) -> list[tuple[int, str, str]]:
    targets: list[tuple[int, str, str]] = []
    ordered_models = _dedupe_models(candidate_models or DEFAULT_CANDIDATE_MODELS)
    ordered_base_urls = list(source.probe_base_urls or [source.base_url])
    for index, (model_name, probe_base_url) in enumerate(
        (item for model_name in ordered_models for item in ((model_name, probe_base_url) for probe_base_url in ordered_base_urls))
    ):
        targets.append((index, model_name, probe_base_url))
    return targets


def _probe_candidate_target(
    source: ProviderSource,
    *,
    model_name: str,
    probe_base_url: str,
    reasoning_effort: str,
    timeout: float,
) -> tuple[bool, str]:
    success_count = 0
    local_error = "probe did not complete"
    url = probe_base_url.rstrip("/") + "/responses"
    payload = {
        "model": model_name,
        "input": "Reply with exactly LIVE_OK",
        "max_output_tokens": 16,
        "store": False,
        "reasoning": {"effort": reasoning_effort},
    }
    with httpx.Client(timeout=timeout, follow_redirects=True, trust_env=False) as client:
        for _ in range(DEFAULT_UPSTREAM_PROBE_ATTEMPTS):
            try:
                response = client.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {source.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
            except Exception as exc:  # pragma: no cover - operational network script
                local_error = str(exc)
                continue
            if response.status_code != 200:
                local_error = response.text[:500]
                continue
            try:
                body = response.json()
            except ValueError as exc:
                local_error = f"invalid json: {exc}"
                continue
            output_text = _extract_text(body)
            if output_text.strip() == "LIVE_OK":
                success_count += 1
                if success_count >= DEFAULT_REQUIRED_PROBE_SUCCESSES:
                    return True, ""
            else:
                local_error = f"unexpected output {output_text[:200]!r}"
    return (
        False,
        f"{model_name} @ {probe_base_url}: "
        f"{success_count}/{DEFAULT_UPSTREAM_PROBE_ATTEMPTS} successful probes; "
        f"last_error={local_error}",
    )


def probe_upstream_responses(
    source: ProviderSource,
    *,
    candidate_models: list[str],
    reasoning_effort: str,
    timeout: float = DEFAULT_UPSTREAM_PROBE_TIMEOUT,
    candidate_probe_workers: int | None = None,
) -> tuple[bool, str, str | None, str | None]:
    targets = _candidate_probe_targets(source, candidate_models)
    if not targets:
        return False, "no candidate models", None, None

    max_workers = min(
        max(1, int(candidate_probe_workers or DEFAULT_CANDIDATE_PROBE_MAX_WORKERS)),
        len(targets),
    )
    if max_workers == 1:
        last_error = "no candidate models"
        for _index, model_name, probe_base_url in targets:
            ok, detail = _probe_candidate_target(
                source,
                model_name=model_name,
                probe_base_url=probe_base_url,
                reasoning_effort=reasoning_effort,
                timeout=timeout,
            )
            if ok:
                return True, "", model_name, probe_base_url
            last_error = detail
        return False, last_error, None, None

    outcomes: dict[int, tuple[bool, str]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_target = {
            executor.submit(
                _probe_candidate_target,
                source,
                model_name=model_name,
                probe_base_url=probe_base_url,
                reasoning_effort=reasoning_effort,
                timeout=timeout,
            ): (index, model_name, probe_base_url)
            for index, model_name, probe_base_url in targets
        }
        for future in as_completed(future_to_target):
            index, model_name, probe_base_url = future_to_target[future]
            try:
                outcomes[index] = future.result()
            except Exception as exc:  # pragma: no cover - defensive; worker already handles network failures
                outcomes[index] = (False, f"{model_name} @ {probe_base_url}: worker_error={exc}")

    last_error = "no candidate models"
    for index, model_name, probe_base_url in targets:
        ok, detail = outcomes.get(index, (False, f"{model_name} @ {probe_base_url}: probe did not run"))
        if ok:
            return True, "", model_name, probe_base_url
        last_error = detail
    return False, last_error, None, None


def _probe_sources(
    sources: list[ProviderSource],
    *,
    candidate_models: list[str],
    reasoning_effort: str,
    timeout: float = DEFAULT_UPSTREAM_PROBE_TIMEOUT,
    source_probe_workers: int | None = None,
    candidate_probe_workers: int | None = None,
) -> dict[str, tuple[bool, str, str | None, str | None]]:
    if not sources:
        return {}

    def _run_probe(source: ProviderSource) -> tuple[bool, str, str | None, str | None]:
        probe_kwargs: dict[str, Any] = {
            "candidate_models": candidate_models,
            "reasoning_effort": reasoning_effort,
            "timeout": timeout,
        }
        if candidate_probe_workers is not None:
            probe_kwargs["candidate_probe_workers"] = candidate_probe_workers
        return probe_upstream_responses(source, **probe_kwargs)

    max_workers = min(
        max(1, int(source_probe_workers or DEFAULT_SOURCE_PROBE_MAX_WORKERS)),
        len(sources),
    )
    if max_workers == 1:
        return {source.name: _run_probe(source) for source in sources}

    outcomes: dict[str, tuple[bool, str, str | None, str | None]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_source = {executor.submit(_run_probe, source): source for source in sources}
        for future in as_completed(future_to_source):
            source = future_to_source[future]
            try:
                outcomes[source.name] = future.result()
            except Exception as exc:  # pragma: no cover - defensive; worker already handles network failures
                outcomes[source.name] = (False, str(exc), None, None)
    return outcomes


def _channels_by_name(channels: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for channel in channels:
        name = str(channel.get("name") or "").strip()
        if not name:
            continue
        grouped.setdefault(name, []).append(channel)
    return grouped


def _existing_channels_by_name(client: Any, sources: list[ProviderSource]) -> dict[str, list[dict[str, Any]]]:
    list_channels = getattr(client, "_list_channels", None)
    if callable(list_channels):
        try:
            return _channels_by_name(list_channels())
        except Exception:
            pass

    grouped: dict[str, list[dict[str, Any]]] = {}
    finder = getattr(client, "find_channels_by_name", None)
    if not callable(finder):
        return grouped
    for source in sources:
        try:
            grouped[source.name] = [item for item in (finder(source.name) or []) if isinstance(item, dict)]
        except Exception:
            grouped[source.name] = []
    return grouped


def _healthy_results_by_model(results: list[ChannelResult]) -> dict[str, list[ChannelResult]]:
    grouped: dict[str, list[ChannelResult]] = {model: [] for model in DEFAULT_CANDIDATE_MODELS}
    for item in results:
        if not item.channel_test_ok:
            continue
        for model_name in _dedupe_models(item.channel_models or [str(item.selected_model or "").strip()]):
            if model_name in grouped:
                grouped[model_name].append(item)
    return grouped


def _failed_results_by_model(results: list[ChannelResult]) -> dict[str, list[ChannelResult]]:
    grouped: dict[str, list[ChannelResult]] = {model: [] for model in DEFAULT_CANDIDATE_MODELS}
    for item in results:
        model_name = str(item.selected_model or item.upstream_probe_model or "").strip()
        if not model_name or model_name not in grouped:
            continue
        if item.channel_test_ok:
            continue
        grouped[model_name].append(item)
    return grouped


def _choose_gateway_review_model(results: list[ChannelResult]) -> str:
    healthy = _healthy_results_by_model(results)
    if healthy.get(DEFAULT_CODEX_MODEL):
        return DEFAULT_CODEX_MODEL
    if healthy.get(DEFAULT_FALLBACK_MODEL):
        return DEFAULT_FALLBACK_MODEL
    return DEFAULT_DEGRADED_REVIEW_MODEL


def _log_block_reason(item: dict[str, Any]) -> str | None:
    haystacks: list[str] = []
    content = str(item.get("content") or "").strip()
    if content:
        haystacks.append(content.lower())
    other = str(item.get("other") or "").strip()
    if other:
        haystacks.append(other.lower())
    for haystack in haystacks:
        for signature in BLOCKING_LOG_SIGNATURES:
            if signature in haystack:
                return signature
    return None


def _log_block_severity(signature: str) -> str:
    """Return 'hard' for truly fatal errors, 'soft' for transient/recoverable."""
    if signature in HARD_BLOCKING_LOG_SIGNATURES:
        return "hard"
    return "soft"


def _recent_log_blocks(log_items: list[dict[str, Any]]) -> dict[int, list[str]]:
    blocked: dict[int, list[str]] = {}
    for item in log_items:
        channel_id = item.get("channel")
        if not isinstance(channel_id, int):
            continue
        signature = _log_block_reason(item)
        if not signature:
            continue
        blocked.setdefault(channel_id, [])
        if signature not in blocked[channel_id]:
            blocked[channel_id].append(signature)
    return blocked


def _channel_targets_candidate_models(channel: dict[str, Any], candidate_models: list[str]) -> bool:
    models = _split_models(str(channel.get("models") or ""))
    if any(model_name in candidate_models for model_name in models):
        return True
    model_mapping = _parse_json_dict(channel.get("model_mapping"))
    if model_mapping and any(model_name in candidate_models for model_name in model_mapping):
        return True
    return False


def _classify_live_channel(
    channel: dict[str, Any],
    *,
    managed_aliases: set[str],
    manual_aliases: dict[str, dict[str, Any]],
    candidate_models: list[str],
    archive_unmanaged: bool,
    freeze_lane_clones: bool,
) -> tuple[str | None, dict[str, Any] | None]:
    aliases = _channel_aliases(channel)
    _base_name, lane_suffix = parse_gateway_lane_channel_name(channel.get("name"))
    if lane_suffix and freeze_lane_clones:
        return INVENTORY_CLASS_LANE_CLONE, None
    if aliases & managed_aliases:
        return INVENTORY_CLASS_MANAGED, None
    manual_entry = next((manual_aliases[alias] for alias in aliases if alias in manual_aliases), None)
    if manual_entry is not None:
        return INVENTORY_CLASS_MANUAL, manual_entry
    if archive_unmanaged and _channel_targets_candidate_models(channel, candidate_models):
        return INVENTORY_CLASS_UNMANAGED, None
    return None, None


def summarize_governance_metrics(channels: list[dict[str, Any]]) -> dict[str, int]:
    metrics = {
        "managed_active_count": 0,
        "managed_quarantine_count": 0,
        "manual_archive_count": 0,
        "unmanaged_drift_count": 0,
        "lane_frozen_count": 0,
        "invalid_state_combo_count": 0,
        "active_outside_truth_count": 0,
    }
    for channel in channels:
        status = int(channel.get("status") or 0)
        weight = int(channel.get("weight") or 0)
        tag = _text_value(channel.get("tag"))
        is_active = status == 1 and weight > 0
        if (status == 1 and weight != 100) or (status == 2 and weight != 0) or (status == 1 and weight == 0):
            metrics["invalid_state_combo_count"] += 1
        if tag == CHANNEL_TAG_MANAGED:
            if is_active:
                metrics["managed_active_count"] += 1
            elif status == 2 and weight == 0:
                metrics["managed_quarantine_count"] += 1
        elif tag == CHANNEL_TAG_MANUAL_ARCHIVE and status == 2 and weight == 0:
            metrics["manual_archive_count"] += 1
        elif tag == CHANNEL_TAG_DRIFT_ARCHIVE and status == 2 and weight == 0:
            metrics["unmanaged_drift_count"] += 1
        elif tag == CHANNEL_TAG_LANE_FROZEN and status == 2 and weight == 0:
            metrics["lane_frozen_count"] += 1
        if is_active and tag != CHANNEL_TAG_MANAGED:
            metrics["active_outside_truth_count"] += 1
    return metrics


def reconcile_channel_pool(
    client: NewAPIClient,
    *,
    sources: list[ProviderSource],
    results: list[ChannelResult],
    registry: dict[str, Any],
    candidate_models: list[str],
    log_page_size: int,
    archive_unmanaged: bool,
    freeze_lane_clones: bool,
) -> dict[str, Any]:
    managed_aliases = {alias for source in sources for alias in _source_aliases(source)}
    manual_aliases = _manual_registry_alias_map(registry)
    current_channels = client._list_channels()
    log_items = client.list_logs(page_size=log_page_size) if log_page_size > 0 else []
    blocked_by_logs = _recent_log_blocks(log_items)
    log_hit_counts: dict[int, int] = {}
    for item in log_items:
        channel_id = item.get("channel")
        if isinstance(channel_id, int):
            log_hit_counts[channel_id] = log_hit_counts.get(channel_id, 0) + 1

    healthy_by_alias: dict[str, ChannelResult] = {}
    for result in results:
        if not result.channel_test_ok or result.channel_id is None:
            continue
        aliases = {
            _channel_identity(result.name),
            _channel_identity(result.base_url),
            _channel_identity(result.channel_base_url),
        }
        for alias in aliases:
            if alias:
                healthy_by_alias[alias] = result

    activated_channels: list[dict[str, Any]] = []
    disabled_channels: list[dict[str, Any]] = []
    quarantined_channels: list[dict[str, Any]] = []
    archived_channels: list[dict[str, Any]] = []
    frozen_channels: list[dict[str, Any]] = []

    for channel in current_channels:
        channel_id = channel.get("id")
        if not isinstance(channel_id, int):
            continue
        _, lane_suffix = parse_gateway_lane_channel_name(channel.get("name"))
        lane = lane_for_gateway_suffix(lane_suffix) if lane_suffix else None
        classification, manual_entry = _classify_live_channel(
            channel,
            managed_aliases=managed_aliases,
            manual_aliases=manual_aliases,
            candidate_models=candidate_models,
            archive_unmanaged=archive_unmanaged,
            freeze_lane_clones=freeze_lane_clones,
        )
        if classification is None:
            continue
        aliases = _channel_aliases(channel)
        matching_result = next((healthy_by_alias[alias] for alias in aliases if alias in healthy_by_alias), None)

        if (
            classification == INVENTORY_CLASS_MANAGED
            and matching_result is not None
            and channel_id not in blocked_by_logs
            and (lane_suffix or channel_id == matching_result.channel_id)
        ):
            updated = client.update_channel(
                channel,
                channel_models=matching_result.channel_models,
                test_model=matching_result.channel_test_model,
                priority=matching_result.channel_priority,
                weight=100,
                status=1,
                model_mapping=matching_result.model_mapping,
                lane=lane,
                name=str(channel.get("name") or ""),
                tag=CHANNEL_TAG_MANAGED,
                remark="govern:managed_active",
            )
            activated_channels.append(
                {
                    "id": updated.get("id"),
                    "name": updated.get("name"),
                    "models": updated.get("models"),
                    "priority": updated.get("priority"),
                    "status": updated.get("status"),
                    "weight": updated.get("weight"),
                    "selected_model": matching_result.selected_model,
                    "lane_suffix": lane_suffix,
                }
            )
            continue

        if classification == INVENTORY_CLASS_MANAGED:
            log_sigs = blocked_by_logs.get(channel_id, [])
            remark = "govern:quarantine"
            if log_sigs:
                remark = f"{remark}:{','.join(log_sigs)}"
            updated = client.update_channel(
                channel,
                weight=0,
                status=2,
                lane=lane,
                name=str(channel.get("name") or ""),
                tag=CHANNEL_TAG_MANAGED,
                remark=remark,
            )
            quarantined_channels.append(
                {
                    "id": updated.get("id"),
                    "name": updated.get("name"),
                    "models": updated.get("models"),
                    "priority": updated.get("priority"),
                    "status": updated.get("status"),
                    "weight": updated.get("weight"),
                    "blocked_by_logs": log_sigs,
                    "lane_suffix": lane_suffix,
                }
            )
            continue

        if classification == INVENTORY_CLASS_MANUAL:
            updated = client.update_channel(
                channel,
                weight=0,
                status=2,
                lane=lane,
                name=str(channel.get("name") or ""),
                tag=CHANNEL_TAG_MANUAL_ARCHIVE,
                remark="govern:manual_archive",
            )
            archived_channels.append(
                {
                    "id": updated.get("id"),
                    "name": updated.get("name"),
                    "models": updated.get("models"),
                    "priority": updated.get("priority"),
                    "status": updated.get("status"),
                    "weight": updated.get("weight"),
                    "inventory_class": INVENTORY_CLASS_MANUAL,
                    "registry_name": manual_entry.get("name") if manual_entry else None,
                    "lane_suffix": lane_suffix,
                }
            )
            continue

        if classification == INVENTORY_CLASS_LANE_CLONE:
            updated = client.update_channel(
                channel,
                weight=0,
                status=2,
                lane=lane,
                name=str(channel.get("name") or ""),
                tag=CHANNEL_TAG_LANE_FROZEN,
                remark="govern:lane_frozen",
            )
            frozen_channels.append(
                {
                    "id": updated.get("id"),
                    "name": updated.get("name"),
                    "models": updated.get("models"),
                    "priority": updated.get("priority"),
                    "status": updated.get("status"),
                    "weight": updated.get("weight"),
                    "inventory_class": INVENTORY_CLASS_LANE_CLONE,
                    "lane_suffix": lane_suffix,
                }
            )
            continue

        updated = client.update_channel(
            channel,
            weight=0,
            status=2,
            lane=lane,
            name=str(channel.get("name") or ""),
            tag=CHANNEL_TAG_DRIFT_ARCHIVE,
            remark="govern:drift_archive",
        )
        archived_channels.append(
            {
                "id": updated.get("id"),
                "name": updated.get("name"),
                "models": updated.get("models"),
                "priority": updated.get("priority"),
                "status": updated.get("status"),
                "weight": updated.get("weight"),
                "inventory_class": INVENTORY_CLASS_UNMANAGED,
                "lane_suffix": lane_suffix,
            }
        )

    return {
        "recent_log_hit_counts": log_hit_counts,
        "recent_log_blocks": blocked_by_logs,
        "activated_channels": activated_channels,
        "disabled_channels": disabled_channels,
        "quarantined_channels": quarantined_channels,
        "archived_channels": archived_channels,
        "frozen_channels": frozen_channels,
        "post_reconcile_channels": client._list_channels(),
    }


def materialize_lane_channels(
    client: NewAPIClient,
    *,
    sources: list[ProviderSource],
    results: list[ChannelResult],
    suffixes: list[str],
) -> dict[str, Any]:
    result_by_name = {item.name: item for item in results}
    current_by_name: dict[str, list[dict[str, Any]]] = {}
    for channel in client._list_channels():
        name = str(channel.get("name") or "").strip()
        if not name:
            continue
        current_by_name.setdefault(name, []).append(channel)

    created_channels: list[dict[str, Any]] = []
    updated_channels: list[dict[str, Any]] = []
    disabled_channels: list[dict[str, Any]] = []

    def _record(
        channel: dict[str, Any],
        *,
        source_name: str,
        suffix: str,
        lane: str,
        action: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "id": channel.get("id"),
            "name": channel.get("name"),
            "group": channel.get("group"),
            "models": channel.get("models"),
            "priority": channel.get("priority"),
            "status": channel.get("status"),
            "weight": channel.get("weight"),
            "source_name": source_name,
            "suffix": suffix,
            "lane": lane,
            "action": action,
        }
        if reason:
            payload["reason"] = reason
        return payload

    for source in sources:
        result = result_by_name.get(source.name)
        active = bool(result and result.channel_test_ok)
        for suffix in suffixes:
            lane = lane_for_gateway_suffix(suffix)
            clone_name = gateway_lane_channel_name(source.name, suffix=suffix)
            existing = list(current_by_name.get(clone_name) or [])
            if active and result is not None:
                channel_models = _dedupe_models(list(result.channel_models or []))
                if not channel_models and result.selected_model:
                    channel_models = [str(result.selected_model)]
                test_model = str(result.channel_test_model or result.selected_model or DEFAULT_MODEL).strip() or DEFAULT_MODEL
                priority = int(result.channel_priority or CHANNEL_PRIORITY_BY_MODEL.get(str(result.selected_model or DEFAULT_MODEL), 0))
                if existing:
                    updated = client.update_channel(
                        existing[0],
                        channel_models=channel_models,
                        test_model=test_model,
                        priority=priority,
                        weight=100,
                        status=1,
                        model_mapping=result.model_mapping,
                        lane=lane,
                        name=clone_name,
                    )
                    updated_channels.append(_record(updated, source_name=source.name, suffix=suffix, lane=lane, action="updated"))
                else:
                    created = client.create_channel(
                        source,
                        channel_models=channel_models,
                        test_model=test_model,
                        priority=priority,
                        model_mapping=result.model_mapping,
                        lane=lane,
                        name=clone_name,
                    )
                    created_channels.append(_record(created, source_name=source.name, suffix=suffix, lane=lane, action="created"))
                for duplicate in existing[1:]:
                    disabled = client.update_channel(
                        duplicate,
                        weight=0,
                        status=2,
                        lane=lane,
                        name=clone_name,
                    )
                    disabled_channels.append(
                        _record(
                            disabled,
                            source_name=source.name,
                            suffix=suffix,
                            lane=lane,
                            action="disabled",
                            reason="duplicate_lane_clone",
                        )
                    )
            else:
                for stale in existing:
                    disabled = client.update_channel(
                        stale,
                        weight=0,
                        status=2,
                        lane=lane,
                        name=clone_name,
                    )
                    disabled_channels.append(
                        _record(
                            disabled,
                            source_name=source.name,
                            suffix=suffix,
                            lane=lane,
                            action="disabled",
                            reason="source_not_healthy",
                        )
                    )

    return {
        "suffixes": list(suffixes),
        "created": created_channels,
        "updated": updated_channels,
        "disabled": disabled_channels,
    }


def summarize_channels_by_model(channels: list[dict[str, Any]], candidate_models: list[str]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    summary: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for model_name in candidate_models:
        active: list[dict[str, Any]] = []
        inactive: list[dict[str, Any]] = []
        for channel in channels:
            if not _channel_targets_candidate_models(channel, [model_name]):
                continue
            item = {
                "id": channel.get("id"),
                "name": channel.get("name"),
                "models": channel.get("models"),
                "status": channel.get("status"),
                "weight": channel.get("weight"),
                "priority": channel.get("priority"),
                "response_time": channel.get("response_time"),
            }
            if int(channel.get("status") or 0) == 1 and int(channel.get("weight") or 0) > 0:
                active.append(item)
            else:
                inactive.append(item)
        summary[model_name] = {
            "active": active,
            "inactive": inactive,
        }
    return summary


def _determine_channel_test_model(channel: dict[str, Any], *, fallback: str | None = None) -> str:
    explicit = _text_value(channel.get("test_model"))
    if explicit:
        return explicit
    models = _split_models(_text_value(channel.get("models")))
    if models:
        return models[0]
    if fallback:
        return fallback
    return DEFAULT_MODEL


def inventory_sources(
    client: NewAPIClient,
    sources: list[ProviderSource],
    *,
    candidate_models: list[str],
    reasoning_effort: str,
    source_probe_workers: int | None = None,
    candidate_probe_workers: int | None = None,
) -> list[ChannelResult]:
    results: list[ChannelResult] = []
    gateway_netloc = urlsplit(client.base_url).netloc.lower()
    eligible_sources: list[ProviderSource] = []
    current_channels_by_name = _existing_channels_by_name(client, sources)
    for source in sources:
        source_netloc = urlsplit(source.channel_base_url).netloc.lower()
        if source_netloc and source_netloc == gateway_netloc:
            results.append(
                ChannelResult(
                    name=source.name,
                    channel_id=None,
                    create_ok=False,
                    test_ok=False,
                    message="inventory: skipped self-referencing gateway source",
                    upstream_probe_ok=None,
                    upstream_probe_error="self-referencing gateway excluded",
                    base_url=source.base_url,
                    channel_base_url=source.channel_base_url,
                    upstream_model=source.upstream_model,
                    exposed_model=source.exposed_model,
                    source_refs=source.source_refs,
                    issues=["self-referencing gateway excluded"],
                    inventory_class=INVENTORY_CLASS_MANAGED,
                )
            )
            continue

        eligible_sources.append(source)

    probe_results = _probe_sources(
        eligible_sources,
        candidate_models=candidate_models,
        reasoning_effort=reasoning_effort,
        source_probe_workers=source_probe_workers,
        candidate_probe_workers=candidate_probe_workers,
    )

    for source in eligible_sources:
        try:
            existing_channels = list(current_channels_by_name.get(source.name) or [])
            upstream_probe_ok, upstream_probe_message, upstream_probe_model, selected_base_url = probe_results.get(
                source.name,
                (False, "probe missing", None, None),
            )
            channel_layout: ChannelLayout | None = None
            if upstream_probe_ok:
                channel_layout = build_channel_layout(
                    source,
                    selected_model=upstream_probe_model or DEFAULT_MODEL,
                    candidate_models=candidate_models,
                )
            channel_id: int | None = None
            channel_test_ok: bool | None = None
            channel_test_message: str | None = None
            time_seconds: float | None = None
            if existing_channels:
                channel = existing_channels[0]
                raw_channel_id = channel.get("id")
                if isinstance(raw_channel_id, int):
                    channel_id = raw_channel_id
                    test_model = _determine_channel_test_model(
                        channel,
                        fallback=channel_layout.test_model if channel_layout is not None else upstream_probe_model,
                    )
                    test_response = client.test_channel(channel_id, test_model)
                    channel_test_ok = bool(test_response.get("success"))
                    channel_test_message = _text_value(test_response.get("message")) or None
                    if test_response.get("time") is not None:
                        time_seconds = float(test_response.get("time"))
            results.append(
                ChannelResult(
                    name=source.name,
                    channel_id=channel_id,
                    create_ok=False,
                    test_ok=bool(upstream_probe_ok),
                    message="inventory only",
                    upstream_probe_ok=upstream_probe_ok,
                    upstream_probe_model=upstream_probe_model,
                    upstream_probe_error=None if upstream_probe_ok else upstream_probe_message,
                    channel_test_ok=channel_test_ok,
                    channel_test_message=channel_test_message,
                    time_seconds=time_seconds,
                    base_url=source.base_url,
                    selected_base_url=selected_base_url,
                    channel_base_url=source.channel_base_url,
                    upstream_model=source.upstream_model,
                    exposed_model=source.exposed_model,
                    selected_model=upstream_probe_model or None,
                    channel_models=list(channel_layout.channel_models if channel_layout is not None else []),
                    channel_test_model=channel_layout.test_model if channel_layout is not None else None,
                    channel_priority=channel_layout.priority if channel_layout is not None else None,
                    model_mapping=channel_layout.model_mapping if channel_layout is not None else source.model_mapping,
                    source_refs=source.source_refs,
                    issues=source.issues,
                    inventory_class=INVENTORY_CLASS_MANAGED,
                )
            )
        except Exception as exc:  # pragma: no cover - operational script
            results.append(
                ChannelResult(
                    name=source.name,
                    channel_id=None,
                    create_ok=False,
                    test_ok=False,
                    message=f"inventory error: {exc}",
                    upstream_probe_ok=None,
                    upstream_probe_error=str(exc),
                    error_detail=str(exc),
                    base_url=source.base_url,
                    selected_base_url=None,
                    channel_base_url=source.channel_base_url,
                    upstream_model=source.upstream_model,
                    exposed_model=source.exposed_model,
                    selected_model=None,
                    model_mapping=source.model_mapping,
                    source_refs=source.source_refs,
                    issues=source.issues,
                    inventory_class=INVENTORY_CLASS_MANAGED,
                )
            )
    return results


def _detect_token_truth_drift(
    tokens: list[dict[str, Any]],
    canonical_token_name: str,
    *,
    known_relay_prefixes: list[str] | None = None,
) -> list[dict[str, Any]]:
    drift: list[dict[str, Any]] = []
    canonical = str(canonical_token_name or "").strip()
    allowed_suffixes = {"stable"} | set(READONLY_SHARDS)
    # Allow extra relay naming conventions (e.g. codex-relay-v4-*)
    extra_prefixes: list[str] = [str(p).strip() for p in (known_relay_prefixes or []) if str(p).strip()]
    for item in tokens:
        name = str(item.get("name") or "").strip()
        if not name.startswith("codex-") or name == canonical_token_name:
            continue
        # Allow sharded token family: <canonical>-stable / <canonical>-ro-a..ro-d
        if canonical and name.startswith(f"{canonical}-"):
            suffix = name[len(canonical) + 1 :].strip().lower()
            if suffix in allowed_suffixes:
                continue
        # Allow known pre-provisioned relay naming conventions (e.g. codex-relay-v4-*)
        if any(name.startswith(prefix) for prefix in extra_prefixes):
            continue
        if int(item.get("status") or 0) != 1:
            continue
        drift.append(
            {
                "id": item.get("id"),
                "name": name,
                "group": item.get("group"),
                "accessed_time": item.get("accessed_time"),
                "created_time": item.get("created_time"),
            }
        )
    return drift


@contextmanager
def _sync_run_lock(lock_path: Path):
    now = time.time()
    if lock_path.exists():
        age_seconds = max(0.0, now - lock_path.stat().st_mtime)
        if age_seconds < 4 * 3600:
            try:
                existing = json.loads(lock_path.read_text(encoding="utf-8"))
            except Exception:
                existing = {"path": str(lock_path)}
            raise RuntimeError(f"sync lock is already held: {existing}")
        lock_path.unlink(missing_ok=True)

    payload = {
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now)),
    }
    lock_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        yield payload
    finally:
        lock_path.unlink(missing_ok=True)


def sync_channels(
    client: NewAPIClient,
    sources: list[ProviderSource],
    *,
    replace_existing: bool,
    candidate_models: list[str],
    reasoning_effort: str,
    source_probe_workers: int | None = None,
    candidate_probe_workers: int | None = None,
) -> list[ChannelResult]:
    results: list[ChannelResult] = []
    gateway_netloc = urlsplit(client.base_url).netloc.lower()
    eligible_sources: list[ProviderSource] = []
    current_channels_by_name = _existing_channels_by_name(client, sources)
    for source in sources:
        # Skip self-referencing gateway channels
        source_netloc = urlsplit(source.channel_base_url).netloc.lower()
        if source_netloc and source_netloc == gateway_netloc:
            results.append(
                ChannelResult(
                    name=source.name,
                    channel_id=None,
                    create_ok=False,
                    test_ok=False,
                    message="skipped: channel base_url points to the gateway itself",
                    upstream_probe_ok=None,
                    upstream_probe_error="self-referencing gateway excluded",
                    base_url=source.base_url,
                    channel_base_url=source.channel_base_url,
                    upstream_model=source.upstream_model,
                    exposed_model=source.exposed_model,
                    source_refs=source.source_refs,
                    issues=["self-referencing gateway excluded"],
                    inventory_class=INVENTORY_CLASS_MANAGED,
                )
            )
            continue

        eligible_sources.append(source)

    probe_results = _probe_sources(
        eligible_sources,
        candidate_models=candidate_models,
        reasoning_effort=reasoning_effort,
        source_probe_workers=source_probe_workers,
        candidate_probe_workers=candidate_probe_workers,
    )

    for source in eligible_sources:
        try:
            existing_channels = list(current_channels_by_name.get(source.name) or [])
            if existing_channels and replace_existing:
                for existing in existing_channels:
                    existing_id = existing.get("id")
                    if isinstance(existing_id, int):
                        client.delete_channel(existing_id)
                existing_channels = []
            upstream_probe_ok, upstream_probe_message, upstream_probe_model, selected_base_url = probe_results.get(
                source.name,
                (False, "probe missing", None, None),
            )
            if not upstream_probe_ok:
                results.append(
                    ChannelResult(
                        name=source.name,
                        channel_id=None,
                        create_ok=False,
                        test_ok=False,
                        message=f"upstream /responses probe failed: {upstream_probe_message}",
                        upstream_probe_ok=False,
                        upstream_probe_model=upstream_probe_model,
                        upstream_probe_error=upstream_probe_message,
                        base_url=source.base_url,
                        selected_base_url=selected_base_url,
                        channel_base_url=source.channel_base_url,
                        upstream_model=source.upstream_model,
                        exposed_model=source.exposed_model,
                        selected_model=upstream_probe_model,
                        model_mapping=source.model_mapping,
                        source_refs=source.source_refs,
                        issues=source.issues,
                        inventory_class=INVENTORY_CLASS_MANAGED,
                    )
                )
                continue
            channel_model = upstream_probe_model or DEFAULT_MODEL
            channel_layout = build_channel_layout(
                source,
                selected_model=channel_model,
                candidate_models=candidate_models,
            )
            if existing_channels and not replace_existing:
                channel = existing_channels[0]
                channel_id = channel.get("id")
                if not isinstance(channel_id, int):
                    raise RuntimeError(f"channel id missing: {channel}")
                test_response = client.test_channel(channel_id, channel_layout.test_model)
                channel_test_ok = bool(test_response.get("success"))
                channel_test_message = str(test_response.get("message") or "")
                message = "channel already exists, skipped creation"
                if len(existing_channels) > 1:
                    message = f"{message}; found {len(existing_channels)} existing channels with same name"
                if not channel_test_ok and channel_test_message:
                    message = f"{message}; existing channel test failed: {channel_test_message}"
                results.append(
                    ChannelResult(
                        name=source.name,
                        channel_id=channel_id,
                        create_ok=True,
                        test_ok=upstream_probe_ok,
                        message=message,
                        upstream_probe_ok=upstream_probe_ok,
                        upstream_probe_model=upstream_probe_model,
                        channel_test_ok=channel_test_ok,
                        channel_test_message=channel_test_message or None,
                        time_seconds=float(test_response.get("time")) if test_response.get("time") is not None else None,
                        base_url=source.base_url,
                        selected_base_url=selected_base_url,
                        channel_base_url=source.channel_base_url,
                        upstream_model=source.upstream_model,
                        exposed_model=source.exposed_model,
                        selected_model=channel_model,
                        channel_models=channel_layout.channel_models,
                        channel_test_model=channel_layout.test_model,
                        channel_priority=channel_layout.priority,
                        model_mapping=channel_layout.model_mapping,
                        source_refs=source.source_refs,
                        issues=source.issues,
                        inventory_class=INVENTORY_CLASS_MANAGED,
                    )
                )
                continue
            channel = client.create_channel(
                source,
                channel_models=channel_layout.channel_models,
                test_model=channel_layout.test_model,
                priority=channel_layout.priority,
                model_mapping=channel_layout.model_mapping,
            )
            channel_id = channel.get("id")
            if not isinstance(channel_id, int):
                raise RuntimeError(f"channel id missing: {channel}")
            test_response = client.test_channel(channel_id, channel_layout.test_model)
            channel_test_ok = bool(test_response.get("success"))
            channel_test_message = str(test_response.get("message") or "")
            ok = upstream_probe_ok
            message = ""
            if not channel_test_ok and channel_test_message:
                message = f"channel test failed but upstream /responses probe passed: {channel_test_message}"
            results.append(
                ChannelResult(
                    name=source.name,
                    channel_id=channel_id,
                    create_ok=True,
                    test_ok=ok,
                    message=message,
                    upstream_probe_ok=upstream_probe_ok,
                    upstream_probe_model=upstream_probe_model,
                    channel_test_ok=channel_test_ok,
                    channel_test_message=channel_test_message or None,
                    time_seconds=float(test_response.get("time")) if test_response.get("time") is not None else None,
                    base_url=source.base_url,
                    selected_base_url=selected_base_url,
                    channel_base_url=source.channel_base_url,
                    upstream_model=source.upstream_model,
                    exposed_model=source.exposed_model,
                    selected_model=channel_model,
                    channel_models=channel_layout.channel_models,
                    channel_test_model=channel_layout.test_model,
                    channel_priority=channel_layout.priority,
                    model_mapping=channel_layout.model_mapping,
                    source_refs=source.source_refs,
                    issues=source.issues,
                    inventory_class=INVENTORY_CLASS_MANAGED,
                )
            )
        except Exception as exc:  # pragma: no cover - operational script
            results.append(
                ChannelResult(
                    name=source.name,
                    channel_id=None,
                    create_ok=False,
                    test_ok=False,
                    message=str(exc),
                    upstream_probe_ok=None,
                    upstream_probe_model=None,
                    upstream_probe_error=str(exc),
                    error_detail=str(exc),
                    base_url=source.base_url,
                    selected_base_url=None,
                    channel_base_url=source.channel_base_url,
                    upstream_model=source.upstream_model,
                    exposed_model=source.exposed_model,
                    selected_model=None,
                    model_mapping=source.model_mapping,
                    source_refs=source.source_refs,
                    issues=source.issues,
                    inventory_class=INVENTORY_CLASS_MANAGED,
                )
            )
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import ai-api/codex providers into a running New API instance and test them."
    )
    parser.add_argument("--base-url", required=True, help="New API console base URL, for example http://host:3000")
    parser.add_argument("--username", required=True, help="New API admin username")
    parser.add_argument("--password", required=True, help="New API admin password")
    parser.add_argument(
        "--providers-root",
        default=str(ROOT),
        help="Directory containing provider folders, defaults to ai-api/codex",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Provider directory name to exclude. Can be passed multiple times.",
    )
    parser.add_argument(
        "--token-name",
        default=DEFAULT_TOKEN_NAME,
        help="Name of the New API access token to create or reuse",
    )
    parser.add_argument(
        "--candidate-model",
        action="append",
        default=None,
        help="Model candidate to probe, in priority order. Can be passed multiple times.",
    )
    parser.add_argument(
        "--reasoning-effort",
        default=DEFAULT_REASONING_EFFORT,
        help="Reasoning effort used for upstream /responses probes.",
    )
    parser.add_argument(
        "--source-probe-workers",
        type=int,
        default=DEFAULT_SOURCE_PROBE_MAX_WORKERS,
        help="Max concurrent provider-source probes during inventory/sync.",
    )
    parser.add_argument(
        "--candidate-probe-workers",
        type=int,
        default=DEFAULT_CANDIDATE_PROBE_MAX_WORKERS,
        help="Max concurrent candidate-model/base-url probes per provider source.",
    )
    parser.add_argument(
        "--log-page-size",
        type=int,
        default=100,
        help="Recent /api/log sample size used for health gating and summary.",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Optional path to write a JSON summary",
    )
    parser.add_argument(
        "--disable-unmanaged-candidates",
        action="store_true",
        help="Disable GPT candidate-model channels not backed by current provider sources.",
    )
    parser.add_argument(
        "--include-root-key-sources",
        action="store_true",
        help="Include ai-api/codex/key.txt relay entries as live-managed sources. Default is disabled to avoid unmanaged pool drift.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Explicitly delete and recreate same-name channels before sync. Default is preserve existing channels.",
    )
    parser.add_argument(
        "--no-replace-existing",
        action="store_true",
        help="Deprecated compatibility flag. Existing channels are preserved by default.",
    )
    parser.add_argument(
        "--write-gateway-provider-dir",
        action="store_true",
        help="Write a local ai-api/codex provider directory that points Codex/OpenAI-compatible calls to New API /v1.",
    )
    parser.add_argument(
        "--write-sharded-gateway-provider-dirs",
        action="store_true",
        help=(
            "Write multiple local ai-api/codex provider directories for stable + ro-a..ro-d shards. "
            "Also creates per-shard New API tokens in separate groups so readonly workers can use "
            "provider-identity isolation. Default is disabled for backward compatibility."
        ),
    )
    parser.add_argument(
        "--gateway-provider-shards",
        default=",".join(READONLY_SHARDS),
        help="Comma-separated shard suffixes for --write-sharded-gateway-provider-dirs. Default: ro-a,ro-b,ro-c,ro-d",
    )
    parser.add_argument(
        "--gateway-provider-name",
        default=None,
        help="Optional local provider directory name for the New API gateway provider.",
    )
    parser.add_argument(
        "--truth-file",
        default=None,
        help="Optional JSON file that defines canonical token/source truth for live sync.",
    )
    parser.add_argument(
        "--registry-file",
        default=None,
        help="Optional JSON file that defines manual channel preservation and lane freeze policy.",
    )
    parser.add_argument(
        "--managed-source",
        action="append",
        default=[],
        help="Provider directory name to manage. Can be passed multiple times. Overrides truth-file managed_sources when supplied.",
    )
    parser.add_argument(
        "--allow-token-fork",
        action="store_true",
        help="Continue even when additional active codex-* tokens are present. Default is fail-closed.",
    )
    parser.add_argument(
        "--provision-gateway-only",
        action="store_true",
        help=(
            "Provision gateway token/provider-dir assets only. Skips channel sync and pool reconcile so "
            "live New API channel state is not mutated."
        ),
    )
    parser.add_argument(
        "--materialize-lane-channels",
        action="store_true",
        help=(
            "Create or update stable + ro-a..ro-d channel copies in lane-specific New API groups so "
            "sharded gateway tokens route to isolated provider identities."
        ),
    )
    parser.add_argument(
        "--archive-unmanaged",
        action="store_true",
        help="Archive unmanaged GPT candidate channels instead of leaving them untouched.",
    )
    parser.add_argument(
        "--freeze-lane-clones",
        action="store_true",
        help="Freeze live __lane__* channels into lane-frozen archive state.",
    )
    parser.add_argument(
        "--lock-file",
        default=str(DEFAULT_SYNC_LOCK_PATH),
        help="Local lock file used to prevent concurrent sync_newapi_channels runs from the same workspace.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    truth_path = Path(args.truth_file).resolve() if args.truth_file else (DEFAULT_LIVE_TRUTH_PATH if DEFAULT_LIVE_TRUTH_PATH.exists() else None)
    truth = _load_live_truth(truth_path)
    registry_path = Path(args.registry_file).resolve() if args.registry_file else (DEFAULT_CHANNEL_REGISTRY_PATH if DEFAULT_CHANNEL_REGISTRY_PATH.exists() else None)
    registry = _load_channel_registry(registry_path)
    truth_errors = _truth_errors(truth)
    registry_errors = _registry_errors(registry)
    excludes = set(DEFAULT_EXCLUDES)
    excludes.update(filter(None, args.exclude))
    candidate_models = args.candidate_model or _truth_string_list(truth, "candidate_models") or list(DEFAULT_CANDIDATE_MODELS)
    managed_sources = set(filter(None, args.managed_source)) or set(_truth_string_list(truth, "managed_sources"))
    disable_unmanaged_candidates = bool(args.disable_unmanaged_candidates or _truth_bool(truth, "disable_unmanaged_candidates", False))
    include_root_key_sources = bool(args.include_root_key_sources or _truth_bool(truth, "include_root_key_sources", False))
    # Known relay token name prefixes that should be exempt from drift detection
    known_relay_prefixes: list[str] = _truth_string_list(truth, "known_relay_token_prefixes")
    # allow_token_fork: truth file overrides CLI default (CLI still wins if explicitly set)
    truth_allow_token_fork = _truth_bool(truth, "allow_token_fork", False)
    archive_unmanaged = bool(args.archive_unmanaged or disable_unmanaged_candidates)
    lane_policy = registry.get("lane_policy") if isinstance(registry.get("lane_policy"), dict) else {}
    freeze_lane_clones = bool(args.freeze_lane_clones or not bool(lane_policy.get("materialize_live_clones", False)))
    token_name = str(truth.get("token_name") or args.token_name or DEFAULT_TOKEN_NAME).strip() or DEFAULT_TOKEN_NAME
    providers_root = Path(args.providers_root).resolve()
    source_probe_workers = max(
        1,
        int(getattr(args, "source_probe_workers", DEFAULT_SOURCE_PROBE_MAX_WORKERS) or DEFAULT_SOURCE_PROBE_MAX_WORKERS),
    )
    candidate_probe_workers = max(
        1,
        int(
            getattr(args, "candidate_probe_workers", DEFAULT_CANDIDATE_PROBE_MAX_WORKERS)
            or DEFAULT_CANDIDATE_PROBE_MAX_WORKERS
        ),
    )
    sources, skipped = load_provider_sources(
        providers_root,
        excludes,
        include_root_key_sources=include_root_key_sources,
        managed_sources=managed_sources or None,
    )
    lock_path = Path(args.lock_file).resolve()
    client = NewAPIClient(args.base_url, args.username, args.password)
    blocked_errors: list[str] = []
    token_truth_drift: list[dict[str, Any]] = []
    token: dict[str, Any] = {}
    results: list[ChannelResult] = []
    setup_response: dict[str, Any] = {}
    login_response: dict[str, Any] = {}
    lane_channel_summary: dict[str, Any] = {"suffixes": [], "created": [], "updated": [], "disabled": []}
    runtime_group_summary: dict[str, Any] = {
        "groups": [],
        "group_ratio_added": [],
        "user_usable_groups_added": [],
        "group_ratio_updated": False,
        "user_usable_groups_updated": False,
    }
    reconcile_summary: dict[str, Any] = {
        "recent_log_hit_counts": {},
        "recent_log_blocks": {},
        "activated_channels": [],
        "disabled_channels": [],
        "post_reconcile_channels": [],
    }
    snapshot_before: list[dict[str, Any]] = []
    snapshot_after: list[dict[str, Any]] = []
    try:
        with _sync_run_lock(lock_path):
            setup_response = client.ensure_setup()
            login_response = client.login()
            if not args.provision_gateway_only:
                blocked_errors.extend(truth_errors)
                blocked_errors.extend(registry_errors)
            tokens_before = client.list_tokens()
            token_truth_drift = _detect_token_truth_drift(
                tokens_before, token_name, known_relay_prefixes=known_relay_prefixes
            )
            effective_allow_token_fork = bool(args.allow_token_fork or truth_allow_token_fork)
            if token_truth_drift and not effective_allow_token_fork:
                blocked_errors.append(
                    f"live token truth is forked: canonical={token_name}, extras="
                    + ",".join(str(item.get('name')) for item in token_truth_drift)
                )
            snapshot_before = client._list_channels()
            if args.materialize_lane_channels and args.provision_gateway_only:
                blocked_errors.append("materialize_lane_channels requires live sync; omit --provision-gateway-only")
            if args.materialize_lane_channels and freeze_lane_clones:
                blocked_errors.append("lane clone materialization is frozen by registry policy; omit --materialize-lane-channels")
            if not blocked_errors:
                runtime_group_summary = client.ensure_runtime_groups(
                    runtime_groups_for_gateway(
                        include_shards=bool(args.write_sharded_gateway_provider_dirs or args.materialize_lane_channels),
                        gateway_provider_shards=args.gateway_provider_shards,
                    ),
                    descriptions=RUNTIME_GROUP_DESCRIPTIONS,
                )
                token = client.create_token(token_name, key_store_path=DEFAULT_TOKEN_KEY_STORE_PATH)
                if args.provision_gateway_only:
                    reconcile_summary = {
                        "recent_log_hit_counts": {},
                        "recent_log_blocks": {},
                        "activated_channels": [],
                        "disabled_channels": [],
                        "quarantined_channels": [],
                        "archived_channels": [],
                        "frozen_channels": [],
                        "post_reconcile_channels": snapshot_before,
                    }
                    snapshot_after = snapshot_before
                else:
                    results = sync_channels(
                        client,
                        sources,
                        replace_existing=bool(args.replace_existing),
                        candidate_models=candidate_models,
                        reasoning_effort=args.reasoning_effort,
                        source_probe_workers=source_probe_workers,
                        candidate_probe_workers=candidate_probe_workers,
                    )
                    reconcile_summary = reconcile_channel_pool(
                        client,
                        sources=sources,
                        results=results,
                        registry=registry,
                        candidate_models=candidate_models,
                        log_page_size=max(0, int(args.log_page_size)),
                        archive_unmanaged=archive_unmanaged,
                        freeze_lane_clones=freeze_lane_clones,
                    )
                    if args.materialize_lane_channels:
                        lane_channel_summary = materialize_lane_channels(
                            client,
                            sources=sources,
                            results=results,
                            suffixes=selected_gateway_lane_suffixes(args.gateway_provider_shards),
                        )
                        snapshot_after = client._list_channels()
                    else:
                        snapshot_after = reconcile_summary["post_reconcile_channels"]
            else:
                reconcile_summary = {
                    "recent_log_hit_counts": {},
                    "recent_log_blocks": {},
                    "activated_channels": [],
                    "disabled_channels": [],
                    "post_reconcile_channels": snapshot_before,
                }
                snapshot_after = snapshot_before

        healthy_by_model = _healthy_results_by_model(results)
        failed_by_model = _failed_results_by_model(results)
        active_by_model = summarize_channels_by_model(snapshot_after, candidate_models)
        governance_metrics = summarize_governance_metrics(snapshot_after)
        activated_channel_ids = {
            item.get("id")
            for item in reconcile_summary["activated_channels"]
            if isinstance(item.get("id"), int)
        }
        gateway_review_model = _choose_gateway_review_model(
            [item for item in results if item.channel_id in activated_channel_ids]
        )
        if args.provision_gateway_only and not results:
            gateway_review_model = DEFAULT_REVIEW_MODEL
        summary = {
            "base_url": args.base_url.rstrip("/"),
            "providers_root": str(providers_root),
            "truth_file": str(truth_path) if truth_path is not None else None,
            "registry_file": str(registry_path) if registry_path is not None else None,
            "excluded": sorted(excludes),
            "managed_sources": sorted(managed_sources),
            "candidate_models": candidate_models,
            "reasoning_effort": args.reasoning_effort,
            "source_probe_workers": source_probe_workers,
            "candidate_probe_workers": candidate_probe_workers,
            "include_root_key_sources": include_root_key_sources,
            "disable_unmanaged_candidates": disable_unmanaged_candidates,
            "archive_unmanaged": archive_unmanaged,
            "replace_existing": bool(args.replace_existing),
            "provision_gateway_only": bool(args.provision_gateway_only),
            "materialize_lane_channels": bool(args.materialize_lane_channels),
            "freeze_lane_clones": freeze_lane_clones,
            "blocked_errors": blocked_errors,
            "truth_validation_errors": truth_errors,
            "registry_validation_errors": registry_errors,
            "setup": setup_response,
            "login": {"success": bool(login_response.get("success")), "user_id": login_response.get("data", {}).get("id")},
            "token_truth_canonical_name": token_name,
            "token_truth_drift": token_truth_drift,
            "token": {
                "id": token.get("id"),
                "name": token.get("name"),
                "group": token.get("group"),
                "full_key": token.get("full_key"),
            },
            "runtime_group_sync": runtime_group_summary,
            "sources_loaded": [asdict(source) for source in sources],
            "manual_registry_count": len(_manual_registry_entries(registry)),
            "skipped": skipped,
            "results": [asdict(item) for item in results],
            "passed": [item.name for item in results if item.channel_test_ok],
            "upstream_only_passed": [item.name for item in results if item.upstream_probe_ok and not item.channel_test_ok],
            "failed": [item.name for item in results if item.test_ok is not None and not item.channel_test_ok],
            "healthy_by_model": {
                model_name: [item.name for item in items]
                for model_name, items in healthy_by_model.items()
            },
            "failed_by_model": {
                model_name: [item.name for item in items]
                for model_name, items in failed_by_model.items()
            },
            "active_by_model": active_by_model,
            "active_channels": [
                {
                    "name": item.name,
                    "channel_id": item.channel_id,
                    "selected_model": item.selected_model,
                    "channel_models": item.channel_models,
                    "channel_priority": item.channel_priority,
                }
                for item in results
                if item.channel_test_ok
            ],
            "disabled_channels": [
                {
                    "name": item.name,
                    "selected_model": item.selected_model,
                    "message": item.message,
                }
                for item in results
                if not item.channel_test_ok
            ],
            "recent_log_hit_counts": reconcile_summary["recent_log_hit_counts"],
            "recent_log_blocks": reconcile_summary["recent_log_blocks"],
            "reconciled_active_channels": reconcile_summary["activated_channels"],
            "reconciled_disabled_channels": reconcile_summary["disabled_channels"],
            "reconciled_quarantined_channels": reconcile_summary["quarantined_channels"],
            "reconciled_archived_channels": reconcile_summary["archived_channels"],
            "reconciled_frozen_channels": reconcile_summary["frozen_channels"],
            "lane_channel_materialization": lane_channel_summary,
            "gateway_review_model_selected": gateway_review_model,
            "governance_metrics": governance_metrics,
        }
        base_provider_name = _gateway_provider_dir_name(
            args.base_url,
            explicit_name=args.gateway_provider_name,
        )
        if args.write_gateway_provider_dir and token.get("full_key"):
            provider_dir = write_gateway_provider_dir(
                providers_root=providers_root,
                base_url=args.base_url,
                token_key=str(token.get("full_key") or "").strip(),
                provider_name=base_provider_name,
                review_model=gateway_review_model,
            )
            summary["gateway_provider"] = {
                "provider_dir": str(provider_dir),
                "openai_base_url": _gateway_openai_base_url(args.base_url),
                "model": DEFAULT_MODEL,
                "review_model": gateway_review_model,
                "fallback_models": list(DEFAULT_CANDIDATE_MODELS),
            }
        if args.write_sharded_gateway_provider_dirs and token.get("full_key"):
            suffixes = selected_gateway_lane_suffixes(args.gateway_provider_shards)
            token_keys_by_suffix: dict[str, str] = {}
            sharded_tokens: list[dict[str, Any]] = []
            for suffix in suffixes:
                lane = lane_for_gateway_suffix(suffix)
                sharded_token_name = gateway_token_name(token_name, suffix=suffix)
                t = client.create_token(sharded_token_name, lane=lane)
                sharded_tokens.append({"id": t.get("id"), "name": t.get("name"), "group": t.get("group")})
                token_keys_by_suffix[suffix] = str(t.get("full_key") or "").strip()
            sharded_dirs = write_sharded_gateway_provider_dirs(
                providers_root=providers_root,
                base_url=args.base_url,
                base_provider_name=base_provider_name,
                token_keys_by_suffix=token_keys_by_suffix,
                review_model=gateway_review_model,
                reasoning_effort=str(args.reasoning_effort or DEFAULT_REASONING_EFFORT),
            )
            summary["gateway_provider_shards"] = {
                "base_provider_name": base_provider_name,
                "suffixes": suffixes,
                "provider_dirs": {k: str(v) for k, v in sharded_dirs.items()},
                "tokens": sharded_tokens,
            }
        if args.out:
            out_path = Path(args.out).resolve()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            before_path = out_path.with_name(f"{out_path.stem}_channels_before{out_path.suffix}")
            after_path = out_path.with_name(f"{out_path.stem}_channels_after{out_path.suffix}")
            before_path.write_text(json.dumps(snapshot_before, ensure_ascii=False, indent=2), encoding="utf-8")
            after_path.write_text(json.dumps(snapshot_after, ensure_ascii=False, indent=2), encoding="utf-8")
            summary["snapshot_before_path"] = str(before_path)
            summary["snapshot_after_path"] = str(after_path)
            out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        passed_count = len(summary["passed"])
        failed_count = len(summary["failed"])
        print(f"\nChannel sync complete: {passed_count} passed, {failed_count} failed")
        if blocked_errors:
            return 2
        return 0 if not summary["failed"] else 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
