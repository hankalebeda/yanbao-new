from __future__ import annotations

import argparse
import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

TARGET_MODEL_FAMILIES = [
    'gpt-5.4',
    'gpt-5.3-codex',
    'gpt-5.2-codex',
    'gpt-5.2',
]
DEFAULT_PROXY_URL = 'http://127.0.0.1:10808'
DEFAULT_TIMEOUT = 30.0
DEFAULT_ATTEMPTS = 4
DEFAULT_REQUIRED_SUCCESSES = 2
DEFAULT_MAX_CONCURRENCY = 6
LIVE_OK_PROMPT = 'Reply with exactly LIVE_OK'


@dataclass
class ModelProbeResult:
    family: str
    aliases: list[str]
    attempted_aliases: list[str]
    working_alias: str | None
    via: str | None
    success_count: int
    attempts: int
    stable: bool
    usable: bool
    last_error: str


@dataclass
class SupplierAuditResult:
    channel_id: int
    name: str
    base_url: str
    current_status: int
    current_weight: int
    current_priority: int
    model_list_ok: bool
    model_list_via: str | None
    model_list_message: str
    provider_models: list[str]
    family_results: dict[str, dict[str, Any]]
    usable_families: list[str]
    stable_families: list[str]


def _channel_identity(value: str | None) -> str:
    clean = str(value or '').strip().rstrip('/')
    if not clean:
        return ''
    parsed = urlsplit(clean)
    return (parsed.netloc or parsed.path or clean).strip().lower()


def _extract_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for item in payload.get('output') or []:
        if not isinstance(item, dict):
            continue
        for content in item.get('content') or []:
            if not isinstance(content, dict):
                continue
            text = content.get('text')
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    output_text = payload.get('output_text')
    if isinstance(output_text, str) and output_text.strip():
        parts.append(output_text.strip())
    return '\n'.join(parts).strip()


def _build_api_base_candidates(base_url: str) -> list[str]:
    clean = str(base_url or '').strip().rstrip('/')
    if not clean:
        return []
    parsed = urlsplit(clean)
    urls = [clean]
    if parsed.path.endswith('/v1'):
        root = urlunsplit((parsed.scheme, parsed.netloc, parsed.path[:-3].rstrip('/'), parsed.query, parsed.fragment)).rstrip('/')
        if root and root not in urls:
            urls.append(root)
    else:
        v1 = urlunsplit((parsed.scheme, parsed.netloc, (parsed.path.rstrip('/') + '/v1') if parsed.path else '/v1', parsed.query, parsed.fragment)).rstrip('/')
        if v1 not in urls:
            urls.append(v1)
    return urls


def _httpx_client(timeout: float, proxy_url: str | None = None) -> httpx.Client:
    kwargs: dict[str, Any] = {
        'timeout': timeout,
        'follow_redirects': True,
        'trust_env': False,
    }
    if proxy_url:
        kwargs['proxies'] = proxy_url
    return httpx.Client(**kwargs)


def _normalize_model_name(model_name: str) -> str:
    return str(model_name or '').strip().lower().replace('_', '-')


def model_family(model_name: str) -> str | None:
    clean = _normalize_model_name(model_name)
    if 'gpt-5.3-codex' in clean:
        return 'gpt-5.3-codex'
    if 'gpt-5.2-codex' in clean:
        return 'gpt-5.2-codex'
    if 'gpt-5.4' in clean:
        return 'gpt-5.4'
    if 'gpt-5.2' in clean:
        return 'gpt-5.2'
    return None


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        clean = str(item or '').strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        result.append(clean)
    return result


def _extract_model_ids(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return []
    data = payload.get('data')
    items = data if isinstance(data, list) else payload.get('models')
    if not isinstance(items, list):
        return []
    result: list[str] = []
    for item in items:
        if isinstance(item, dict):
            model_id = item.get('id') or item.get('name')
            if isinstance(model_id, str) and model_id.strip():
                result.append(model_id.strip())
        elif isinstance(item, str) and item.strip():
            result.append(item.strip())
    return _dedupe(result)


def load_snapshot(snapshot_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(snapshot_path.read_text(encoding='utf-8'))
    if isinstance(payload, dict) and isinstance(payload.get('channels'), list):
        return [item for item in payload['channels'] if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    raise ValueError(f'Unsupported snapshot shape in {snapshot_path}')


def load_keys(sqlite_db: Path) -> dict[int, str]:
    conn = sqlite3.connect(str(sqlite_db))
    try:
        rows = conn.execute('select id, key from channels').fetchall()
    finally:
        conn.close()
    result: dict[int, str] = {}
    for channel_id, key in rows:
        if isinstance(channel_id, int) and isinstance(key, str) and key.strip():
            result[channel_id] = key.strip()
    return result


def fetch_provider_models(base_url: str, api_key: str, timeout: float, proxy_url: str | None) -> tuple[bool, list[str], str, str | None]:
    attempts: list[tuple[str, str | None]] = [('direct', None)]
    if proxy_url:
        attempts.append(('proxy', proxy_url))
    last_error = 'models fetch did not complete'
    for via, current_proxy in attempts:
        with _httpx_client(timeout=timeout, proxy_url=current_proxy) as client:
            for api_base in _build_api_base_candidates(base_url):
                url = api_base.rstrip('/') + '/models'
                try:
                    response = client.get(url, headers={'Authorization': f'Bearer {api_key}'})
                except Exception as exc:
                    last_error = str(exc)
                    continue
                if response.status_code != 200:
                    last_error = response.text[:500]
                    continue
                try:
                    body = response.json()
                except ValueError as exc:
                    last_error = f'invalid json: {exc}'
                    continue
                models = _extract_model_ids(body)
                if models:
                    return True, models, '', via
                last_error = 'empty model list'
    return False, [], last_error, None


def family_aliases(provider_models: list[str], family: str, fallback_models: str = '') -> list[str]:
    from_provider = [model for model in provider_models if model_family(model) == family]
    if not from_provider and fallback_models:
        from_provider = [item.strip() for item in fallback_models.split(',') if model_family(item) == family]
    ordered = sorted(_dedupe(from_provider), key=lambda item: (0 if _normalize_model_name(item) == family else 1, len(item), item))
    if family not in ordered and (not provider_models or any(model_family(item) == family for item in ordered)):
        ordered.insert(0, family)
    return _dedupe(ordered)[:3]


def probe_model_alias(base_url: str, api_key: str, model_name: str, timeout: float, attempts: int, required_successes: int, proxy_url: str | None) -> tuple[int, int, str | None, str]:
    transports: list[tuple[str, str | None]] = [('direct', None)]
    if proxy_url:
        transports.append(('proxy', proxy_url))
    last_error = 'probe did not complete'
    for via, current_proxy in transports:
        with _httpx_client(timeout=timeout, proxy_url=current_proxy) as client:
            success_count = 0
            for attempt in range(attempts):
                remaining = attempts - attempt
                if success_count + remaining < required_successes:
                    break
                try:
                    response = client.post(
                        _build_api_base_candidates(base_url)[-1].rstrip('/') + '/responses',
                        headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
                        json={
                            'model': model_name,
                            'input': LIVE_OK_PROMPT,
                            'max_output_tokens': 16,
                            'store': False,
                            'reasoning': {'effort': 'xhigh'},
                        },
                    )
                except Exception as exc:
                    last_error = str(exc)
                    continue
                if response.status_code != 200:
                    last_error = response.text[:500]
                    lowered = last_error.lower()
                    if "model_not_found" in lowered or "token expired" in lowered or "unauthorized" in lowered:
                        break
                    continue
                try:
                    body = response.json()
                except ValueError as exc:
                    last_error = f'invalid json: {exc}'
                    continue
                if _extract_text(body).strip() == 'LIVE_OK':
                    success_count += 1
                    if success_count >= required_successes:
                        return success_count, attempt + 1, via, ''
                else:
                    last_error = 'unexpected output != LIVE_OK'
            if success_count > 0:
                return success_count, attempts, via, last_error
    return 0, attempts, None, last_error


def evaluate_family(base_url: str, api_key: str, family: str, aliases: list[str], timeout: float, attempts: int, required_successes: int, proxy_url: str | None) -> ModelProbeResult:
    attempted_aliases: list[str] = []
    best_alias: str | None = None
    best_via: str | None = None
    best_success = 0
    best_attempts = attempts
    best_error = 'family unavailable'
    stable = False
    usable = False
    for alias in aliases:
        attempted_aliases.append(alias)
        success_count, used_attempts, via, last_error = probe_model_alias(base_url, api_key, alias, timeout, attempts, required_successes, proxy_url)
        if success_count > best_success:
            best_success = success_count
            best_alias = alias
            best_via = via
            best_attempts = used_attempts
            best_error = last_error
        if success_count >= required_successes:
            stable = True
            usable = True
            best_alias = alias
            best_via = via
            best_attempts = used_attempts
            best_error = ''
            break
        if success_count > 0:
            usable = True
    return ModelProbeResult(
        family=family,
        aliases=aliases,
        attempted_aliases=attempted_aliases,
        working_alias=best_alias,
        via=best_via,
        success_count=best_success,
        attempts=best_attempts,
        stable=stable,
        usable=usable,
        last_error=best_error,
    )


def audit_supplier(channel: dict[str, Any], key_map: dict[int, str], timeout: float, attempts: int, required_successes: int, proxy_url: str | None) -> SupplierAuditResult:
    channel_id = int(channel.get('id') or 0)
    api_key = key_map.get(channel_id, '').strip()
    if not api_key:
        return SupplierAuditResult(
            channel_id=channel_id,
            name=str(channel.get('name') or ''),
            base_url=str(channel.get('base_url') or ''),
            current_status=int(channel.get('status') or 0),
            current_weight=int(channel.get('weight') or 0),
            current_priority=int(channel.get('priority') or 0),
            model_list_ok=False,
            model_list_via=None,
            model_list_message='missing api key in sqlite snapshot',
            provider_models=[],
            family_results={},
            usable_families=[],
            stable_families=[],
        )
    model_list_ok, provider_models, model_list_message, model_list_via = fetch_provider_models(str(channel.get('base_url') or ''), api_key, timeout, proxy_url)
    family_results: dict[str, dict[str, Any]] = {}
    usable_families: list[str] = []
    stable_families: list[str] = []
    for family in TARGET_MODEL_FAMILIES:
        aliases = family_aliases(provider_models, family, str(channel.get('models') or ''))
        if not aliases:
            family_results[family] = asdict(ModelProbeResult(
                family=family,
                aliases=[],
                attempted_aliases=[],
                working_alias=None,
                via=None,
                success_count=0,
                attempts=0,
                stable=False,
                usable=False,
                last_error='family not advertised by supplier',
            ))
            continue
        result = evaluate_family(str(channel.get('base_url') or ''), api_key, family, aliases, timeout, attempts, required_successes, proxy_url)
        family_results[family] = asdict(result)
        if result.usable:
            usable_families.append(family)
        if result.stable:
            stable_families.append(family)
    return SupplierAuditResult(
        channel_id=channel_id,
        name=str(channel.get('name') or ''),
        base_url=str(channel.get('base_url') or ''),
        current_status=int(channel.get('status') or 0),
        current_weight=int(channel.get('weight') or 0),
        current_priority=int(channel.get('priority') or 0),
        model_list_ok=model_list_ok,
        model_list_via=model_list_via,
        model_list_message=model_list_message,
        provider_models=provider_models,
        family_results=family_results,
        usable_families=usable_families,
        stable_families=stable_families,
    )


def summarize(results: list[SupplierAuditResult]) -> dict[str, Any]:
    family_summary: dict[str, dict[str, int]] = {family: {'stable': 0, 'usable': 0, 'advertised': 0} for family in TARGET_MODEL_FAMILIES}
    for item in results:
        for family in TARGET_MODEL_FAMILIES:
            info = item.family_results.get(family) or {}
            if info.get('aliases'):
                family_summary[family]['advertised'] += 1
            if info.get('usable'):
                family_summary[family]['usable'] += 1
            if info.get('stable'):
                family_summary[family]['stable'] += 1
    return {
        'supplier_count': len(results),
        'model_list_ok_count': sum(1 for item in results if item.model_list_ok),
        'usable_supplier_count': sum(1 for item in results if item.usable_families),
        'stable_supplier_count': sum(1 for item in results if item.stable_families),
        'family_summary': family_summary,
        'usable_suppliers': [item.name for item in results if item.usable_families],
        'stable_suppliers': [item.name for item in results if item.stable_families],
    }


def render_markdown(results: list[SupplierAuditResult], summary: dict[str, Any], snapshot_path: Path, sqlite_db: Path) -> str:
    lines = [
        '# New API Supplier GPT Audit',
        '',
        f'- Snapshot: `{snapshot_path}`',
        f'- SQLite DB: `{sqlite_db}`',
        f'- Suppliers tested: **{summary["supplier_count"]}**',
        f'- Model list ok: **{summary["model_list_ok_count"]}**',
        f'- Any usable GPT family: **{summary["usable_supplier_count"]}**',
        f'- Stable suppliers: **{summary["stable_supplier_count"]}**',
        '',
        '## Family Summary',
        '',
        '| Family | Advertised | Usable | Stable |',
        '| --- | ---: | ---: | ---: |',
    ]
    for family in TARGET_MODEL_FAMILIES:
        info = summary['family_summary'][family]
        lines.append(f'| {family} | {info["advertised"]} | {info["usable"]} | {info["stable"]} |')
    lines.extend([
        '',
        '## Supplier Details',
        '',
        '| id | name | active | model_list | via | usable_families | stable_families |',
        '| ---: | --- | --- | --- | --- | --- | --- |',
    ])
    for item in results:
        active = 'yes' if item.current_status == 1 and item.current_weight > 0 else 'no'
        lines.append(f'| {item.channel_id} | {item.name.replace("|","\\|")} | {active} | {"ok" if item.model_list_ok else "fail"} | {item.model_list_via or ""} | {", ".join(item.usable_families) or "none"} | {", ".join(item.stable_families) or "none"} |')
    return '\n'.join(lines) + '\n'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Audit GPT-family models for current New API suppliers')
    parser.add_argument('--snapshot-json', required=True)
    parser.add_argument('--sqlite-db', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--max-concurrency', type=int, default=DEFAULT_MAX_CONCURRENCY)
    parser.add_argument('--timeout', type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument('--attempts', type=int, default=DEFAULT_ATTEMPTS)
    parser.add_argument('--required-successes', type=int, default=DEFAULT_REQUIRED_SUCCESSES)
    parser.add_argument('--proxy-url', default=DEFAULT_PROXY_URL)
    parser.add_argument('--disable-proxy-fallback', action='store_true')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshot_path = Path(args.snapshot_json)
    sqlite_db = Path(args.sqlite_db)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    md_path = out_path.with_suffix('.md')
    channels = load_snapshot(snapshot_path)
    key_map = load_keys(sqlite_db)
    proxy_url = None if args.disable_proxy_fallback else str(args.proxy_url or '').strip() or None
    results: list[SupplierAuditResult] = []
    max_workers = max(1, int(args.max_concurrency or 1))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                audit_supplier,
                channel,
                key_map,
                float(args.timeout),
                max(1, int(args.attempts or 1)),
                max(1, int(args.required_successes or 1)),
                proxy_url,
            ): int(channel.get('id') or 0)
            for channel in channels
        }
        for future in as_completed(futures):
            results.append(future.result())
    results.sort(key=lambda item: item.channel_id)
    summary = summarize(results)
    payload = {
        'generated_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'snapshot_json': str(snapshot_path),
        'sqlite_db': str(sqlite_db),
        'timeout': float(args.timeout),
        'attempts': int(args.attempts),
        'required_successes': int(args.required_successes),
        'proxy_url': proxy_url,
        'summary': summary,
        'results': [asdict(item) for item in results],
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    md_path.write_text(render_markdown(results, summary, snapshot_path, sqlite_db), encoding='utf-8')
    print(json.dumps({'json_report': str(out_path), 'markdown_report': str(md_path), 'supplier_count': len(results)}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
