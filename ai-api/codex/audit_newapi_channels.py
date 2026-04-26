from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from sync_newapi_channels import (
    DEFAULT_CANDIDATE_MODELS,
    DEFAULT_REASONING_EFFORT,
    NewAPIClient,
    ProviderSource,
    _build_probe_base_urls,
    _channel_identity,
    _extract_text,
    _parse_json_dict,
    _split_models,
)

try:
    import sqlite3
except Exception:  # pragma: no cover
    sqlite3 = None  # type: ignore[assignment]

import httpx

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent.parent
OUTPUT_ROOT = REPO_ROOT / "output"
DEFAULT_PAGE_SIZE = 200
DEFAULT_MAX_CONCURRENCY = 2
DEFAULT_PROBE_TIMEOUT = 8.0
DEFAULT_CHANNEL_TEST_TIMEOUT = 8.0
DEFAULT_PROBE_ATTEMPTS = 3
DEFAULT_REQUIRED_PROBE_SUCCESSES = 2
DEFAULT_PROXY_URL = "http://127.0.0.1:10808"
MAX_PROBE_GPT_CANDIDATES = 6

GPT_MODEL_PRIORITY = [
    "gpt-5.4",
    "gpt-5.3-codex",
    "gpt-5.2",
    "gpt-5.2-codex",
    "gpt-5.1-codex-max",
    "gpt-5.1-codex",
    "gpt-5.1-codex-mini",
    "gpt-5-codex",
    "gpt-5-codex-mini",
    "gpt-5.1",
    "gpt-5",
]


@dataclass
class ChannelAuditResult:
    channel_id: int
    name: str
    base_url: str
    current_status: int
    current_weight: int
    current_priority: int
    models: str
    test_model_used: str | None
    channel_test_ok: bool
    channel_test_latency_s: float | None
    channel_test_message: str
    upstream_probe_ok: bool
    upstream_probe_model: str | None
    upstream_probe_latency_s: float | None
    upstream_probe_message: str
    classification: str
    recommended_action: str
    sibling_group: str
    sibling_count: int
    risk_types: list[str]
    current_group: str
    current_tag: str | None
    current_remark: str | None
    models_probe_ok: bool = False
    models_probe_message: str = ""
    advertised_gpt_models: list[str] | None = None
    best_available_gpt_model: str | None = None
    codex_usable: bool = False
    model_list_ok: bool = False
    model_list_message: str = ""
    provider_models: list[str] | None = None
    gpt_related_models: list[str] | None = None
    upstream_probe_via: str | None = None
    newapi_ready: bool = False
    supplier_usable: bool = False


def _timestamp_slug() -> str:
    return time.strftime("%Y%m%d_%H%M%S")


def _is_active(channel: dict[str, Any]) -> bool:
    return int(channel.get("status") or 0) == 1 and int(channel.get("weight") or 0) > 0


def _normalized_sibling_group(channel: dict[str, Any]) -> str:
    for candidate in (channel.get("base_url"), channel.get("name")):
        identity = _channel_identity(candidate)
        if identity:
            return identity
    return f"channel-{channel.get('id') or 'unknown'}"


def resolve_test_model(channel: dict[str, Any]) -> str | None:
    explicit = str(channel.get("test_model") or "").strip()
    if explicit:
        return explicit
    models = _split_models(channel.get("models"))
    model_mapping = _parse_json_dict(channel.get("model_mapping")) or {}
    exposed = set(models) | set(model_mapping.keys())
    for candidate in DEFAULT_CANDIDATE_MODELS:
        if candidate in exposed:
            return candidate
    return models[0] if models else None


def determine_test_model(channel: dict[str, Any]) -> str | None:
    return resolve_test_model(channel)


def filter_gpt_models(models: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for model in models:
        clean = str(model or "").strip()
        if not clean:
            continue
        lowered = clean.lower()
        if "gpt" not in lowered:
            continue
        if clean in seen:
            continue
        seen.add(clean)
        result.append(clean)
    return result


def prioritize_gpt_models(models: list[str]) -> list[str]:
    preferred = ["gpt-5.4", "gpt-5.3-codex", "gpt-5.2"]
    filtered = filter_gpt_models(models)
    ordered: list[str] = []
    seen: set[str] = set()
    for candidate in preferred:
        for model in filtered:
            if model == candidate and model not in seen:
                seen.add(model)
                ordered.append(model)
    for model in filtered:
        if model not in seen:
            seen.add(model)
            ordered.append(model)
    return ordered


def _is_gpt_related_model(model_name: str) -> bool:
    clean = str(model_name or "").strip().lower()
    return clean.startswith("gpt") or clean.startswith("chatgpt")


def _dedupe_text(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        clean = str(item or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        result.append(clean)
    return result


def _priority_index(model_name: str) -> tuple[int, str]:
    clean = str(model_name or "").strip()
    try:
        return (GPT_MODEL_PRIORITY.index(clean), clean)
    except ValueError:
        return (len(GPT_MODEL_PRIORITY), clean)


def prioritize_gpt_models(models: list[str], *, preferred: str | None = None) -> list[str]:
    gpt_models = [item for item in _dedupe_text(models) if _is_gpt_related_model(item)]
    ordered = sorted(gpt_models, key=_priority_index)
    if preferred and preferred in ordered:
        ordered.remove(preferred)
        ordered.insert(0, preferred)
    return ordered


def _build_models_probe_urls(base_url: str) -> list[str]:
    clean = str(base_url or "").strip().rstrip("/")
    if not clean:
        return []
    urls = [clean]
    parsed = _channel_identity(clean)
    if clean.endswith("/v1"):
        root = clean[:-3].rstrip("/")
        if root and root not in urls:
            urls.append(root)
    else:
        v1 = clean + "/v1"
        if v1 not in urls:
            urls.append(v1)
    final_urls: list[str] = []
    for prefix in urls:
        models_url = prefix.rstrip("/") + "/models"
        if models_url not in final_urls:
            final_urls.append(models_url)
    return final_urls


def _extract_model_ids(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    items = data if isinstance(data, list) else payload.get("models")
    if not isinstance(items, list):
        return []
    result: list[str] = []
    for item in items:
        if isinstance(item, dict):
            model_id = item.get("id") or item.get("name")
            if isinstance(model_id, str) and model_id.strip():
                result.append(model_id.strip())
        elif isinstance(item, str) and item.strip():
            result.append(item.strip())
    return _dedupe_text(result)


def classify_channel(
    *,
    is_active: bool,
    channel_test_ok: bool,
    upstream_probe_ok: bool,
    risk_types: list[str],
    sibling_conflict: bool,
    test_model_used: str | None,
    model_list_ok: bool = False,
    gpt_related_models: list[str] | None = None,
) -> tuple[str, str]:
    newapi_ready = channel_test_ok and upstream_probe_ok
    supplier_usable = model_list_ok and bool(gpt_related_models or []) and upstream_probe_ok
    if sibling_conflict or (not test_model_used and not supplier_usable):
        return "manual_review", "manual_review"
    if newapi_ready:
        if is_active:
            return "healthy_active", "keep_active"
        return "healthy_disabled", "candidate_reactivate"
    if supplier_usable:
        if is_active:
            return "usable_active", "keep_active"
        return "usable_disabled", "candidate_reactivate"
    if is_active:
        return "active_drift", "disable_keep"
    unrecoverable_markers = {
        "auth/token expired",
        "model_not_found",
        "Cloudflare / block",
        "bad response body / 非 LIVE_OK",
    }
    if (not channel_test_ok) and (not upstream_probe_ok) and any(item in unrecoverable_markers for item in risk_types):
        return "broken", "retire_candidate_but_do_not_delete"
    return "warm_disabled", "disable_keep"


def detect_risk_types(*messages: str) -> list[str]:
    combined = " ".join(str(item or "") for item in messages).lower()
    risks: list[str] = []
    checks = [
        ("503 / service temporarily unavailable", ["status code 503", "service temporarily unavailable"]),
        ("429 / quota / rate limit", ["status code 429", "too many requests", "quota exceeded", "rate limit"]),
        ("auth/token expired", ["token expired", "unauthorized", "auth_unavailable", "invalid api key", "incorrect api key", "authentication"]),
        ("model_not_found", ["model_not_found", "no available channel for model"]),
        ("Cloudflare / block", ["cloudflare", "cf block", "attention required"]),
        ("timeout / connection reset", ["timed out", "timeout", "connection reset", "read timed out", "winerror 10060"]),
        ("bad response body / 非 LIVE_OK", ["bad response body", "unexpected output", "invalid json", "do request failed"]),
    ]
    for label, patterns in checks:
        if any(pattern in combined for pattern in patterns):
            risks.append(label)
    return risks


def _httpx_client_kwargs(*, timeout: float, proxy_url: str | None = None, trust_env: bool = False) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "timeout": timeout,
        "follow_redirects": True,
        "trust_env": trust_env,
    }
    if proxy_url:
        kwargs["proxy"] = proxy_url
    return kwargs


def fetch_provider_models(
    *,
    base_url: str,
    api_key: str,
    timeout: float,
    proxy_url: str | None = None,
) -> tuple[bool, list[str], str, str | None]:
    attempts: list[tuple[str, str | None]] = [("direct", None)]
    if proxy_url:
        attempts.append(("proxy", proxy_url))
    last_error = "models probe did not complete"
    for via, current_proxy in attempts:
        try:
            with httpx.Client(**_httpx_client_kwargs(timeout=timeout, proxy_url=current_proxy)) as client:
                for url in _build_models_probe_urls(base_url):
                    try:
                        response = client.get(
                            url,
                            headers={"Authorization": f"Bearer {api_key}"},
                        )
                    except Exception as exc:
                        last_error = str(exc)
                        continue
                    if response.status_code != 200:
                        last_error = response.text[:500]
                        continue
                    try:
                        body = response.json()
                    except ValueError as exc:
                        last_error = f"invalid json: {exc}"
                        continue
                    model_ids = _extract_model_ids(body)
                    if model_ids:
                        return True, model_ids, "", via
                    last_error = "empty model list"
        except Exception as exc:
            last_error = str(exc)
    return False, [], last_error, None


def summarize_results(results: list[ChannelAuditResult]) -> dict[str, Any]:
    active_results = [item for item in results if item.current_status == 1 and item.current_weight > 0]
    disabled_results = [item for item in results if not (item.current_status == 1 and item.current_weight > 0)]
    status_summary = {
        "active_count": len(active_results),
        "disabled_count": len(disabled_results),
        "active_fail_count": sum(1 for item in active_results if not (item.channel_test_ok and item.upstream_probe_ok)),
        "disabled_recoverable_count": sum(1 for item in disabled_results if item.classification in {"healthy_disabled", "usable_disabled"}),
        "active_usable_count": sum(1 for item in active_results if item.classification in {"healthy_active", "usable_active"}),
    }
    model_summary = {
        "direct_gpt_5_4_count": sum(1 for item in results if item.upstream_probe_ok and item.upstream_probe_model == "gpt-5.4"),
        "gpt_5_3_codex_only_count": sum(1 for item in results if item.upstream_probe_ok and item.upstream_probe_model == "gpt-5.3-codex"),
        "gpt_5_2_only_count": sum(1 for item in results if item.upstream_probe_ok and item.upstream_probe_model == "gpt-5.2"),
        "all_failed_count": sum(1 for item in results if not item.upstream_probe_ok),
        "supplier_model_list_ok_count": sum(1 for item in results if item.model_list_ok),
        "supplier_usable_count": sum(1 for item in results if item.supplier_usable),
    }
    risk_summary: dict[str, int] = {}
    for item in results:
        for risk in item.risk_types:
            risk_summary[risk] = risk_summary.get(risk, 0) + 1
    next_steps = {
        "active_unhealthy": [item.name for item in results if item.classification == "active_drift"],
        "disabled_recoverable": [item.name for item in results if item.classification in {"healthy_disabled", "usable_disabled"}],
        "inventory_only": [item.name for item in results if item.classification in {"warm_disabled", "broken"}],
        "manual_review": [item.name for item in results if item.classification == "manual_review"],
        "supplier_usable": [item.name for item in results if item.supplier_usable],
    }
    classification_counts: dict[str, int] = {}
    for item in results:
        classification_counts[item.classification] = classification_counts.get(item.classification, 0) + 1
    return {
        "status_summary": status_summary,
        "model_summary": model_summary,
        "risk_summary": risk_summary,
        "classification_counts": classification_counts,
        "next_steps": next_steps,
    }


def render_markdown_report(*, base_url: str, generated_at: str, snapshot_path: Path, results: list[ChannelAuditResult], summary: dict[str, Any]) -> str:
    lines = [
        "# New API ?? Channel ????",
        "",
        f"- Base URL: `{base_url}`",
        f"- Generated at: `{generated_at}`",
        f"- Snapshot: `{snapshot_path}`",
        f"- Total channels: **{len(results)}**",
        "",
        "## ????",
        "",
        "| ?? | ?? |",
        "| --- | ---: |",
        f"| active_count | {summary['status_summary']['active_count']} |",
        f"| disabled_count | {summary['status_summary']['disabled_count']} |",
        f"| active_fail_count | {summary['status_summary']['active_fail_count']} |",
        f"| disabled_recoverable_count | {summary['status_summary']['disabled_recoverable_count']} |",
        f"| active_usable_count | {summary['status_summary']['active_usable_count']} |",
        "",
        "## ??????",
        "",
        "| ?? | ?? |",
        "| --- | ---: |",
        f"| direct_gpt_5_4_count | {summary['model_summary']['direct_gpt_5_4_count']} |",
        f"| gpt_5_3_codex_only_count | {summary['model_summary']['gpt_5_3_codex_only_count']} |",
        f"| gpt_5_2_only_count | {summary['model_summary']['gpt_5_2_only_count']} |",
        f"| all_failed_count | {summary['model_summary']['all_failed_count']} |",
        f"| supplier_model_list_ok_count | {summary['model_summary']['supplier_model_list_ok_count']} |",
        f"| supplier_usable_count | {summary['model_summary']['supplier_usable_count']} |",
        "",
        "## ????",
        "",
        "| ???? | ?? |",
        "| --- | ---: |",
    ]
    for risk, count in sorted(summary["risk_summary"].items()):
        lines.append(f"| {risk} | {count} |")
    if not summary["risk_summary"]:
        lines.append("| none | 0 |")
    lines.extend([
        "",
        "## ???????",
        "",
        f"- active_unhealthy: {', '.join(summary['next_steps']['active_unhealthy']) or 'none'}",
        f"- disabled_recoverable: {', '.join(summary['next_steps']['disabled_recoverable']) or 'none'}",
        f"- inventory_only: {', '.join(summary['next_steps']['inventory_only']) or 'none'}",
        f"- manual_review: {', '.join(summary['next_steps']['manual_review']) or 'none'}",
        f"- supplier_usable: {', '.join(summary['next_steps']['supplier_usable']) or 'none'}",
        "",
        "## ?????",
        "",
        "| id | name | active | test_model | model_list | gpt_models | channel_test | upstream_probe | via | classification | action | risks |",
        "| ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ])
    for item in sorted(results, key=lambda row: row.channel_id):
        active = "yes" if item.current_status == 1 and item.current_weight > 0 else "no"
        channel_test = "ok" if item.channel_test_ok else "fail"
        upstream = f"ok:{item.upstream_probe_model}" if item.upstream_probe_ok else "fail"
        model_list = "ok" if item.model_list_ok else "fail"
        gpt_models = ", ".join(item.gpt_related_models or []) or "none"
        risks = ", ".join(item.risk_types) or "none"
        name = item.name.replace("|", "\\|")
        lines.append(
            f"| {item.channel_id} | {name} | {active} | {item.test_model_used or ''} | {model_list} | {gpt_models} | {channel_test} | {upstream} | {item.upstream_probe_via or ''} | {item.classification} | {item.recommended_action} | {risks} |"
        )
    return "\n".join(lines) + "\n"


def load_channel_key_map(sqlite_db: Path | None) -> dict[int, str]:
    if sqlite_db is None:
        return {}
    if sqlite3 is None:
        raise RuntimeError("sqlite3 is unavailable in this Python environment")
    if not sqlite_db.exists():
        raise FileNotFoundError(sqlite_db)
    conn = sqlite3.connect(str(sqlite_db))
    try:
        rows = conn.execute("select id, key from channels").fetchall()
    finally:
        conn.close()
    result: dict[int, str] = {}
    for channel_id, key in rows:
        if isinstance(channel_id, int) and isinstance(key, str) and key.strip():
            result[channel_id] = key.strip()
    return result


def list_all_channels(client: NewAPIClient, *, include_disabled: bool, page_size: int = DEFAULT_PAGE_SIZE) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    page = 1
    while True:
        response = client._json(client._request("GET", f"/api/channel/?p={page}&page_size={page_size}"))
        data = response.get("data") or {}
        batch = [item for item in (data.get("items") or []) if isinstance(item, dict)]
        if not batch:
            break
        items.extend(batch)
        total = data.get("total")
        if total is not None and len(items) >= int(total):
            break
        if len(batch) < page_size:
            break
        page += 1
    if include_disabled:
        return items
    return [item for item in items if _is_active(item)]


def snapshot_channel(channel: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": channel.get("id"),
        "name": channel.get("name"),
        "base_url": channel.get("base_url"),
        "models": channel.get("models"),
        "status": channel.get("status"),
        "weight": channel.get("weight"),
        "priority": channel.get("priority"),
        "test_model": channel.get("test_model"),
        "model_mapping": channel.get("model_mapping"),
        "group": channel.get("group"),
        "tag": channel.get("tag"),
        "remark": channel.get("remark"),
    }


def login_with_retry(client: NewAPIClient, *, attempts: int = 5, sleep_seconds: float = 2.0) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            return client.login()
        except Exception as exc:  # pragma: no cover - network behavior
            last_exc = exc
            if attempt == attempts - 1:
                raise
            time.sleep(sleep_seconds * (attempt + 1))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("login failed unexpectedly")


def build_probe_source(channel: dict[str, Any], api_key: str) -> ProviderSource:
    base_url = str(channel.get("base_url") or "").strip()
    model_mapping = _parse_json_dict(channel.get("model_mapping")) or None
    return ProviderSource(
        name=str(channel.get("name") or ""),
        base_url=base_url,
        probe_base_urls=_build_probe_base_urls(base_url),
        channel_base_url=base_url,
        api_key=api_key,
        upstream_model="",
        exposed_model=DEFAULT_CANDIDATE_MODELS[0],
        model_mapping=model_mapping,
        enabled=bool(int(channel.get("status") or 0) == 1),
    )


def probe_upstream_responses_audit(
    source: ProviderSource,
    *,
    candidate_models: list[str],
    reasoning_effort: str,
    timeout: float,
    max_attempts: int,
    required_successes: int,
    proxy_url: str | None = None,
) -> tuple[bool, str, str | None, str | None]:
    last_error = "no candidate models"
    transports: list[tuple[str, str | None]] = [("direct", None)]
    if proxy_url:
        transports.append(("proxy", proxy_url))
    for via, current_proxy in transports:
        with httpx.Client(**_httpx_client_kwargs(timeout=timeout, proxy_url=current_proxy)) as client:
            for model_name in candidate_models:
                for probe_base_url in source.probe_base_urls or [source.base_url]:
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
                    for attempt in range(max_attempts):
                        remaining = max_attempts - attempt
                        if success_count + remaining < required_successes:
                            break
                        try:
                            response = client.post(
                                url,
                                headers={
                                    "Authorization": f"Bearer {source.api_key}",
                                    "Content-Type": "application/json",
                                },
                                json=payload,
                            )
                        except Exception as exc:  # pragma: no cover
                            local_error = str(exc)
                            continue
                        if response.status_code != 200:
                            local_error = response.text[:500]
                            lowered = local_error.lower()
                            if "model_not_found" in lowered or "token expired" in lowered:
                                break
                            continue
                        try:
                            body = response.json()
                        except ValueError as exc:
                            local_error = f"invalid json: {exc}"
                            continue
                        output_text = _extract_text(body)
                        if output_text.strip() == "LIVE_OK":
                            success_count += 1
                            if success_count >= required_successes:
                                return True, "", model_name, via
                        else:
                            local_error = f"unexpected output {output_text[:200]!r}"
                    last_error = (
                        f"{model_name} @ {probe_base_url} via {via}: "
                        f"{success_count}/{max_attempts} successful probes; "
                        f"last_error={local_error}"
                    )
    return False, last_error, None, None


def run_channel_test(channel: dict[str, Any], *, client: NewAPIClient, channel_test_timeout: float) -> dict[str, Any]:
    channel_id = int(channel.get("id") or 0)
    test_model = resolve_test_model(channel)
    channel_test_ok = False
    channel_test_latency_s: float | None = None
    channel_test_message = ""
    if test_model:
        try:
            started = time.monotonic()
            test_response = client._json(client._request("GET", f"/api/channel/test/{channel_id}?model={test_model}", timeout=channel_test_timeout))
            channel_test_latency_s = round(time.monotonic() - started, 2)
            channel_test_ok = bool(test_response.get("success"))
            if test_response.get("time") is not None:
                channel_test_latency_s = float(test_response.get("time"))
            channel_test_message = str(test_response.get("message") or "")
        except Exception as exc:  # pragma: no cover
            channel_test_message = str(exc)
    else:
        channel_test_message = "unable to resolve canonical test_model from current channel configuration"
    return {
        "channel_id": channel_id,
        "test_model_used": test_model,
        "channel_test_ok": channel_test_ok,
        "channel_test_latency_s": channel_test_latency_s,
        "channel_test_message": channel_test_message,
    }


def run_upstream_probe(
    channel: dict[str, Any],
    *,
    key_map: dict[int, str],
    probe_timeout: float,
    probe_attempts: int,
    required_successes: int,
    proxy_url: str | None = None,
) -> dict[str, Any]:
    channel_id = int(channel.get("id") or 0)
    upstream_probe_ok = False
    upstream_probe_model: str | None = None
    upstream_probe_latency_s: float | None = None
    upstream_probe_message = ""
    model_list_ok = False
    model_list_message = ""
    provider_models: list[str] = []
    gpt_related_models: list[str] = []
    upstream_probe_via: str | None = None
    api_key = key_map.get(channel_id, "").strip()
    if api_key:
        source = build_probe_source(channel, api_key)
        started = time.monotonic()
        try:
            model_list_ok, provider_models, model_list_message, _models_via = fetch_provider_models(
                base_url=source.base_url,
                api_key=source.api_key,
                timeout=probe_timeout,
                proxy_url=proxy_url,
            )
            preferred_test_model = resolve_test_model(channel)
            gpt_related_models = prioritize_gpt_models(provider_models, preferred=preferred_test_model) if model_list_ok else []
            candidate_models = (gpt_related_models[:MAX_PROBE_GPT_CANDIDATES] if gpt_related_models else list(DEFAULT_CANDIDATE_MODELS))
            upstream_probe_ok, upstream_probe_message, upstream_probe_model, upstream_probe_via = probe_upstream_responses_audit(
                source,
                candidate_models=candidate_models,
                reasoning_effort=DEFAULT_REASONING_EFFORT,
                timeout=probe_timeout,
                max_attempts=probe_attempts,
                required_successes=required_successes,
                proxy_url=proxy_url,
            )
            upstream_probe_latency_s = round(time.monotonic() - started, 2)
        except Exception as exc:  # pragma: no cover
            upstream_probe_message = str(exc)
            upstream_probe_latency_s = round(time.monotonic() - started, 2)
    else:
        upstream_probe_message = "api_key unavailable from current API response; supply --sqlite-db to enable direct upstream probe"
    return {
        "upstream_probe_ok": upstream_probe_ok,
        "upstream_probe_model": upstream_probe_model,
        "upstream_probe_latency_s": upstream_probe_latency_s,
        "upstream_probe_message": upstream_probe_message,
        "model_list_ok": model_list_ok,
        "model_list_message": model_list_message,
        "provider_models": provider_models,
        "gpt_related_models": gpt_related_models,
        "upstream_probe_via": upstream_probe_via,
    }


def audit_channels(
    channels: list[dict[str, Any]],
    *,
    client: NewAPIClient,
    key_map: dict[int, str],
    max_concurrency: int,
    probe_timeout: float,
    probe_attempts: int,
    required_successes: int,
    channel_test_timeout: float,
    proxy_url: str | None = None,
) -> list[ChannelAuditResult]:
    channel_test_results: dict[int, dict[str, Any]] = {}
    for channel in channels:
        cid = int(channel.get("id") or 0)
        result = run_channel_test(channel, client=client, channel_test_timeout=channel_test_timeout)
        result.update({
            "upstream_probe_ok": False,
            "upstream_probe_model": None,
            "upstream_probe_latency_s": None,
            "upstream_probe_message": "api_key unavailable from current API response; supply --sqlite-db to enable direct upstream probe",
            "model_list_ok": False,
            "model_list_message": "",
            "provider_models": [],
            "gpt_related_models": [],
            "upstream_probe_via": None,
        })
        channel_test_results[cid] = result

    upstream_candidates = [channel for channel in channels if key_map.get(int(channel.get("id") or 0), "").strip()]
    max_workers = max(1, int(max_concurrency or 1))
    probe_results: dict[int, dict[str, Any]] = {}
    if max_workers == 1:
        for channel in upstream_candidates:
            cid = int(channel.get("id") or 0)
            probe_results[cid] = run_upstream_probe(
                channel,
                key_map=key_map,
                probe_timeout=probe_timeout,
                probe_attempts=probe_attempts,
                required_successes=required_successes,
                proxy_url=proxy_url,
            )
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    run_upstream_probe,
                    channel,
                    key_map=key_map,
                    probe_timeout=probe_timeout,
                    probe_attempts=probe_attempts,
                    required_successes=required_successes,
                    proxy_url=proxy_url,
                ): int(channel.get("id") or 0)
                for channel in upstream_candidates
            }
            for future in as_completed(futures):
                cid = futures[future]
                probe_results[cid] = future.result()
    for cid, probe_only in probe_results.items():
        channel_test_results[cid].update(probe_only)

    sibling_groups: dict[str, list[int]] = {}
    for channel in channels:
        group = _normalized_sibling_group(channel)
        sibling_groups.setdefault(group, []).append(int(channel.get("id") or 0))

    healthy_active_by_group: dict[str, int] = {}
    for channel in channels:
        cid = int(channel.get("id") or 0)
        group = _normalized_sibling_group(channel)
        test = channel_test_results[cid]
        if _is_active(channel) and test["channel_test_ok"] and test["upstream_probe_ok"]:
            healthy_active_by_group[group] = healthy_active_by_group.get(group, 0) + 1

    results: list[ChannelAuditResult] = []
    for channel in channels:
        cid = int(channel.get("id") or 0)
        group = _normalized_sibling_group(channel)
        test = channel_test_results[cid]
        risk_types = detect_risk_types(test["channel_test_message"], test["upstream_probe_message"])
        sibling_conflict = healthy_active_by_group.get(group, 0) > 1
        classification, action = classify_channel(
            is_active=_is_active(channel),
            channel_test_ok=bool(test["channel_test_ok"]),
            upstream_probe_ok=bool(test["upstream_probe_ok"]),
            risk_types=risk_types,
            sibling_conflict=sibling_conflict,
            test_model_used=test["test_model_used"],
            model_list_ok=bool(test.get("model_list_ok")),
            gpt_related_models=list(test.get("gpt_related_models") or []),
        )
        newapi_ready = bool(test["channel_test_ok"]) and bool(test["upstream_probe_ok"])
        supplier_usable = bool(test.get("model_list_ok")) and bool(test.get("gpt_related_models") or []) and bool(test["upstream_probe_ok"])
        results.append(
            ChannelAuditResult(
                channel_id=cid,
                name=str(channel.get("name") or ""),
                base_url=str(channel.get("base_url") or ""),
                current_status=int(channel.get("status") or 0),
                current_weight=int(channel.get("weight") or 0),
                current_priority=int(channel.get("priority") or 0),
                models=str(channel.get("models") or ""),
                test_model_used=test["test_model_used"],
                channel_test_ok=bool(test["channel_test_ok"]),
                channel_test_latency_s=test["channel_test_latency_s"],
                channel_test_message=str(test["channel_test_message"] or ""),
                upstream_probe_ok=bool(test["upstream_probe_ok"]),
                upstream_probe_model=test["upstream_probe_model"],
                upstream_probe_latency_s=test["upstream_probe_latency_s"],
                upstream_probe_message=str(test["upstream_probe_message"] or ""),
                classification=classification,
                recommended_action=action,
                sibling_group=group,
                sibling_count=len(sibling_groups[group]),
                risk_types=risk_types,
                current_group=str(channel.get("group") or ""),
                current_tag=(str(channel.get("tag")) if channel.get("tag") is not None else None),
                current_remark=(str(channel.get("remark")) if channel.get("remark") is not None else None),
                model_list_ok=bool(test.get("model_list_ok")),
                model_list_message=str(test.get("model_list_message") or ""),
                provider_models=list(test.get("provider_models") or []),
                gpt_related_models=list(test.get("gpt_related_models") or []),
                upstream_probe_via=test.get("upstream_probe_via"),
                newapi_ready=newapi_ready,
                supplier_usable=supplier_usable,
                models_probe_ok=bool(test.get("model_list_ok")),
                models_probe_message=str(test.get("model_list_message") or ""),
                advertised_gpt_models=list(test.get("gpt_related_models") or []),
                best_available_gpt_model=(list(test.get("gpt_related_models") or [None])[0] if list(test.get("gpt_related_models") or []) else None),
                codex_usable=supplier_usable,
            )
        )
    return sorted(results, key=lambda item: item.channel_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only audit of current New API channel inventory")
    parser.add_argument("--base-url", required=True, help="New API console base URL, for example http://host:3000")
    parser.add_argument("--username", required=True, help="New API admin username")
    parser.add_argument("--password", required=True, help="New API admin password")
    parser.add_argument("--out", default=None, help="JSON output path for the audit report")
    parser.add_argument("--include-disabled", action="store_true", default=True, help="Include disabled channels (default: true)")
    parser.add_argument("--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY, help="Low concurrency for upstream probe work")
    parser.add_argument("--sqlite-db", default=None, help="Optional SQLite DB snapshot path used to recover channel API keys for direct upstream probe")
    parser.add_argument("--probe-timeout", type=float, default=DEFAULT_PROBE_TIMEOUT, help="Per-upstream probe timeout in seconds")
    parser.add_argument("--probe-attempts", type=int, default=DEFAULT_PROBE_ATTEMPTS, help="Max upstream probe attempts per model")
    parser.add_argument("--required-successes", type=int, default=DEFAULT_REQUIRED_PROBE_SUCCESSES, help="Required successful upstream probes per model")
    parser.add_argument("--channel-test-timeout", type=float, default=DEFAULT_CHANNEL_TEST_TIMEOUT, help="HTTP timeout for login/list/channel-test requests")
    parser.add_argument("--proxy-url", default=None, help="Optional HTTP proxy for upstream model-list and /responses probes, e.g. http://127.0.0.1:10808")
    return parser.parse_args()


def resolve_output_paths(out: str | None) -> tuple[Path, Path, Path]:
    if out:
        json_path = Path(out)
    else:
        json_path = OUTPUT_ROOT / f"newapi_channel_audit_{_timestamp_slug()}.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path = json_path.with_name(f"{json_path.stem}.snapshot.json")
    markdown_path = json_path.with_suffix(".md")
    return json_path, snapshot_path, markdown_path


def main() -> int:
    args = parse_args()
    json_path, snapshot_path, markdown_path = resolve_output_paths(args.out)
    client = NewAPIClient(args.base_url, args.username, args.password)
    client.client.timeout = float(args.channel_test_timeout)
    try:
        client.ensure_setup()
        login_with_retry(client)
        channels = list_all_channels(client, include_disabled=bool(args.include_disabled))
        snapshot = [snapshot_channel(channel) for channel in channels]
        snapshot_path.write_text(
            json.dumps(
                {
                    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "base_url": args.base_url.rstrip("/"),
                    "channel_count": len(snapshot),
                    "channels": snapshot,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        key_map = load_channel_key_map(Path(args.sqlite_db)) if args.sqlite_db else {}
        results = audit_channels(
            channels,
            client=client,
            key_map=key_map,
            max_concurrency=max(1, int(args.max_concurrency or 1)),
            probe_timeout=float(args.probe_timeout),
            probe_attempts=max(1, int(args.probe_attempts or 1)),
            required_successes=max(1, int(args.required_successes or 1)),
            channel_test_timeout=float(args.channel_test_timeout),
            proxy_url=str(args.proxy_url or "").strip() or None,
        )
    finally:
        client.close()

    generated_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    summary = summarize_results(results)
    payload = {
        "generated_at": generated_at,
        "base_url": args.base_url.rstrip("/"),
        "sqlite_db": str(args.sqlite_db) if args.sqlite_db else None,
        "snapshot_path": str(snapshot_path),
        "markdown_path": str(markdown_path),
        "channel_count": len(results),
        "summary": summary,
        "results": [asdict(item) for item in results],
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_markdown_report(
            base_url=args.base_url.rstrip("/"),
            generated_at=generated_at,
            snapshot_path=snapshot_path,
            results=results,
            summary=summary,
        ),
        encoding="utf-8",
    )
    print(json.dumps({
        "json_report": str(json_path),
        "snapshot": str(snapshot_path),
        "markdown_report": str(markdown_path),
        "channel_count": len(results),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
