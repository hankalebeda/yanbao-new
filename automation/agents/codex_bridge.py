"""Codex CLI async bridge for the Escort Team.

Provides an ``asyncio``-native wrapper around the Codex CLI executable,
enabling FixAgent and AnalysisAgent to use local Codex CLI as a fallback
when WebAI REST endpoints are unavailable.

Design mirrors ``scripts/prompt6_hourly_codex.py`` provider mechanics
(portable HOME, config.toml + auth.json copy) but exposes a simple
``codex_exec()`` coroutine instead of driving the full hourly loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import tempfile
import time
import uuid as _uuid
from pathlib import Path
from typing import Any, Optional

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.11+ ships tomllib
    import tomli as tomllib  # type: ignore[no-redef]

logger = logging.getLogger(__name__)

DEFAULT_CANONICAL_PROVIDER = "bus.042999.xyz"
# 2026-04-01 provider priority — live-verified, disabled providers kept but deprioritized
# Active (gpt-5.4 + gpt-5.3-codex verified):
#   bus.042999.xyz, marybrown.dpdns.org
# Active (gpt-5.4 only):
#   code.claudex.us.ci, elysiver.h-e.top
# Disabled (token expired / 503 / model_not_found — NOT deleted):
#   newapi-192.168.232.141-3000, sub.jlypx.de, snew.145678.xyz,
#   ai.qaq.al, infiniteai.cc, 119.8.113.226, freeapi.dgbmc.top,
#   api.925214.xyz, wududu.edu.kg, codex.sakurapy.de,
#   api.einzieg.site, newapi.linuxdo.edu.rs, sapi.777114.xyz
DEFAULT_PROVIDER_PRIORITY = [
    "bus.042999.xyz",
    "marybrown.dpdns.org",
    "supercodex.space",
    "ai.acmi.run",
    "code.claudex.us.ci",
    "elysiver.h-e.top",
]
_PROVIDER_PROBE_TTL_SECONDS = 300
_provider_probe_cache: dict[tuple[str, str], tuple[float, bool]] = {}
_provider_success_cache: dict[str, str] = {}

# ---------------------------------------------------------------------------
# Executable resolution
# ---------------------------------------------------------------------------

def resolve_codex_executable() -> Optional[str]:
    """Locate ``codex`` on PATH.  Returns *None* if not found.

    v5: Supports ``CODEX_CLI_PATH`` env override and searches additional
    well-known locations on Windows (npm global, node_modules/.bin).
    """
    # 1. Explicit env override (highest priority)
    env_path = os.environ.get("CODEX_CLI_PATH", "").strip()
    if env_path and os.path.isfile(env_path):
        return env_path

    # 2. Standard PATH search
    for name in ("codex.exe", "codex.cmd", "codex"):
        found = shutil.which(name)
        if found:
            return found

    # 3. Well-known Windows locations
    if os.name == "nt":
        appdata = os.environ.get("APPDATA", "")
        if appdata:
            npm_global = os.path.join(appdata, "npm", "codex.cmd")
            if os.path.isfile(npm_global):
                return npm_global
        # Check workspace-local node_modules
        for base in (".", os.environ.get("LOOP_CONTROLLER_REPO_ROOT", "")):
            if base:
                local = os.path.join(base, "node_modules", ".bin", "codex.cmd")
                if os.path.isfile(local):
                    return local
    return None


_CACHE_TTL_SECONDS = 300  # re-check every 5 minutes
_codex_available_cache: Optional[bool] = None
_codex_available_ts: float = 0.0
_codex_available_lock: asyncio.Lock | None = None


def _get_lock() -> asyncio.Lock:
    """Lazy-init an asyncio.Lock (must be created inside a running loop)."""
    global _codex_available_lock
    if _codex_available_lock is None:
        _codex_available_lock = asyncio.Lock()
    return _codex_available_lock


async def codex_available() -> bool:
    """Return *True* if the Codex CLI is reachable and responds to ``--version``.

    Result is cached for ``_CACHE_TTL_SECONDS`` and protected by an
    ``asyncio.Lock`` so concurrent callers share a single probe.
    """
    global _codex_available_cache, _codex_available_ts

    now = time.monotonic()
    if _codex_available_cache is not None and (now - _codex_available_ts) < _CACHE_TTL_SECONDS:
        return _codex_available_cache

    async with _get_lock():
        # Double-check after acquiring lock
        now = time.monotonic()
        if _codex_available_cache is not None and (now - _codex_available_ts) < _CACHE_TTL_SECONDS:
            return _codex_available_cache

        exe = resolve_codex_executable()
        if not exe:
            _codex_available_cache = False
            _codex_available_ts = now
            return False
        try:
            proc = await asyncio.create_subprocess_exec(
                exe, "--version",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(proc.wait(), timeout=5)
            _codex_available_cache = proc.returncode == 0
        except Exception:
            _codex_available_cache = False
        _codex_available_ts = now
        return _codex_available_cache


def reset_cache() -> None:
    """Reset the availability cache (for testing)."""
    global _codex_available_cache, _codex_available_ts, _codex_available_lock
    _codex_available_cache = None
    _codex_available_ts = 0.0
    _codex_available_lock = None


# ---------------------------------------------------------------------------
# Provider helpers (mirrors prompt6_hourly_codex)
# ---------------------------------------------------------------------------

def _safe_provider_name(provider: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in provider)


def _provider_root(repo_root: Path) -> Path:
    return repo_root / "ai-api" / "codex"


def _state_path(repo_root: Path) -> Path:
    return repo_root / "runtime" / "agents" / "codex_bridge_state.json"


def _load_persisted_success_provider(repo_root: Path) -> str:
    path = _state_path(repo_root)
    if not path.exists():
        return ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("last_success_provider") or "").strip()


def _record_success_provider(repo_root: Path, provider: str) -> None:
    clean = str(provider or "").strip()
    if not clean:
        return
    root_key = str(repo_root.resolve())
    _provider_success_cache[root_key] = clean

    path = _state_path(repo_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "last_success_provider": clean,
                "updated_at": int(time.time()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _normalize_provider_ref(value: str) -> str:
    return str(value or "").strip().lower()


def _normalize_openai_base_url(value: str) -> str:
    clean = str(value or "").strip().rstrip("/")
    if not clean:
        return ""
    return clean if clean.endswith("/v1") else f"{clean}/v1"


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        clean = str(item or "").strip()
        if not clean:
            continue
        token = _normalize_provider_ref(clean)
        if token in seen:
            continue
        seen.add(token)
        ordered.append(clean)
    return ordered


def _load_toml(path: Path) -> dict[str, Any]:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def _read_provider_key_lines(provider_dir: Path) -> list[str]:
    key_path = provider_dir / "key.txt"
    if not key_path.exists():
        return []
    raw = key_path.read_text(encoding="utf-8-sig", errors="ignore")
    return [line.strip() for line in raw.splitlines() if line.strip()]


def _provider_metadata(provider_dir: Path) -> dict[str, str]:
    config_path = provider_dir / "config.toml"
    if not config_path.exists():
        return {}

    config = _load_toml(config_path)
    provider_cfg = config.get("model_providers", {}).get("OpenAI", {})
    key_lines = _read_provider_key_lines(provider_dir)
    auth: dict[str, Any] = {}
    auth_path = provider_dir / "auth.json"
    if auth_path.exists():
        try:
            auth_payload = json.loads(auth_path.read_text(encoding="utf-8"))
            if isinstance(auth_payload, dict):
                auth = auth_payload
        except Exception:
            auth = {}

    base_url = _normalize_openai_base_url(provider_cfg.get("base_url") or "")
    if not base_url and len(key_lines) >= 2 and key_lines[0].startswith(("http://", "https://")):
        base_url = _normalize_openai_base_url(key_lines[0])

    api_key = ""
    if len(key_lines) >= 2 and key_lines[0].startswith(("http://", "https://")):
        api_key = key_lines[1]
    elif key_lines:
        api_key = key_lines[0]
    if not api_key:
        api_key = str(auth.get("OPENAI_API_KEY") or auth.get("api_key") or auth.get("apiKey") or "").strip()

    return {
        "base_url": base_url,
        "api_key": api_key,
        "model": str(config.get("model") or "").strip(),
        "review_model": str(config.get("review_model") or "").strip(),
        "reasoning_effort": str(config.get("model_reasoning_effort") or "").strip() or "xhigh",
    }


def _render_gateway_config(*, model: str, review_model: str, reasoning_effort: str, base_url: str) -> str:
    review_line = f'review_model = "{review_model}"' if review_model else 'review_model = "gpt-5.2"'
    return "\n".join(
        [
            f'model = "{model or "gpt-5.4"}"',
            review_line,
            f'model_reasoning_effort = "{reasoning_effort or "xhigh"}"',
            "",
            "[model_providers.OpenAI]",
            'name = "OpenAI"',
            f'base_url = "{_normalize_openai_base_url(base_url)}"',
            'wire_api = "responses"',
            'supports_websockets = false',
            'requires_openai_auth = true',
            "",
            "[features]",
            'responses_websockets_v2 = false',
            'multi_agent = true',
            "",
            "[windows]",
            'sandbox = "elevated"',
        ]
    )


def _gateway_env_override(provider: str, provider_dir: Path) -> dict[str, str] | None:
    canonical_env = str(os.environ.get("CODEX_CANONICAL_PROVIDER") or "").strip()
    canonical = canonical_env or DEFAULT_CANONICAL_PROVIDER
    if _normalize_provider_ref(provider) != _normalize_provider_ref(canonical):
        return None

    base_url = _normalize_openai_base_url(
        os.environ.get("NEW_API_BASE_URL")
        or os.environ.get("PROMOTE_PREP_NEW_API_BASE_URL")
        or ""
    )
    api_key = str(
        os.environ.get("NEW_API_TOKEN")
        or os.environ.get("PROMOTE_PREP_NEW_API_TOKEN")
        or ""
    ).strip()
    if not (base_url and api_key):
        return None

    metadata = _provider_metadata(provider_dir)
    return {
        "base_url": base_url,
        "api_key": api_key,
        "model": metadata.get("model") or "gpt-5.4",
        "review_model": metadata.get("review_model") or "gpt-5.2",
        "reasoning_effort": metadata.get("reasoning_effort") or "xhigh",
    }


def _valid_provider_names(repo_root: Path) -> list[str]:
    root = _provider_root(repo_root)
    if not root.is_dir():
        return []

    providers: list[str] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        has_config = (child / "config.toml").exists()
        has_auth = (child / "auth.json").exists() or (child / "key.txt").exists()
        if has_config and has_auth:
            providers.append(child.name)
    return providers


def detect_provider_candidates(repo_root: Path) -> list[str]:
    override = os.environ.get("CODEX_PROVIDER", "").strip()
    if override:
        return [override]

    valid = _valid_provider_names(repo_root)
    if not valid:
        return []

    root_key = str(repo_root.resolve())
    candidates: list[str] = []

    cached_success = _provider_success_cache.get(root_key, "").strip()
    if cached_success:
        candidates.append(cached_success)

    persisted_success = _load_persisted_success_provider(repo_root)
    if persisted_success:
        candidates.append(persisted_success)

    canonical_env = str(os.environ.get("CODEX_CANONICAL_PROVIDER") or "").strip()
    gateway_configured = bool(
        (os.environ.get("NEW_API_BASE_URL") or os.environ.get("PROMOTE_PREP_NEW_API_BASE_URL"))
        and (os.environ.get("NEW_API_TOKEN") or os.environ.get("PROMOTE_PREP_NEW_API_TOKEN"))
    )
    if canonical_env:
        candidates.append(canonical_env)
    elif gateway_configured:
        candidates.append(DEFAULT_CANONICAL_PROVIDER)

    readonly_allowlist = str(os.environ.get("CODEX_READONLY_PROVIDER_ALLOWLIST") or "").strip()
    if readonly_allowlist:
        candidates.extend(item.strip() for item in readonly_allowlist.split(",") if item.strip())

    provider_allowlist = str(os.environ.get("CODEX_PROVIDER_ALLOWLIST") or "").strip()
    if provider_allowlist:
        candidates.extend(item.strip() for item in provider_allowlist.split(",") if item.strip())

    candidates.extend(DEFAULT_PROVIDER_PRIORITY)
    candidates.extend(sorted(valid))
    valid_refs = {_normalize_provider_ref(item) for item in valid}
    return [item for item in _dedupe_keep_order(candidates) if _normalize_provider_ref(item) in valid_refs]


def _extract_response_text(payload: dict[str, Any]) -> str:
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


def _probe_provider_live(repo_root: Path, provider: str, *, timeout_s: float = 8.0) -> bool:
    cache_key = (str(repo_root.resolve()), provider)
    now = time.monotonic()
    cached = _provider_probe_cache.get(cache_key)
    if cached and (now - cached[0]) < _PROVIDER_PROBE_TTL_SECONDS:
        return cached[1]

    try:
        import httpx
    except Exception:
        _provider_probe_cache[cache_key] = (now, False)
        return False

    provider_dir = _provider_root(repo_root) / provider
    try:
        metadata = _provider_metadata(provider_dir)
        base_url = metadata.get("base_url") or ""
        api_key = metadata.get("api_key") or ""
        model = metadata.get("model") or ""
        if not (base_url and api_key and model):
            ok = False
        else:
            response = httpx.post(
                f"{base_url.rstrip('/')}/responses",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "input": "Reply with exactly: LIVE_OK",
                    "max_output_tokens": 32,
                    "store": False,
                },
                timeout=timeout_s,
            )
            ok = response.status_code == 200 and _extract_response_text(response.json()).strip() == "LIVE_OK"
    except Exception:
        ok = False

    _provider_probe_cache[cache_key] = (now, ok)
    if ok:
        _provider_success_cache[str(repo_root.resolve())] = provider
    return ok


def detect_provider(repo_root: Path) -> str:
    """Return the preferred live provider when possible, else the best local candidate."""
    candidates = detect_provider_candidates(repo_root)
    if not candidates:
        return ""
    persisted_success = _load_persisted_success_provider(repo_root)
    if persisted_success and any(
        _normalize_provider_ref(candidate) == _normalize_provider_ref(persisted_success)
        for candidate in candidates
    ):
        return persisted_success
    for candidate in candidates:
        if _probe_provider_live(repo_root, candidate):
            return candidate
    return candidates[0]


def _prepare_provider_home(repo_root: Path, provider: str) -> Path:
    """Copy provider credentials into an isolated portable HOME directory.

    Uses a fresh temp home per invocation so Windows Codex skill installs do not
    trip over stale locked directories from earlier runs.
    """
    source_dir = _provider_root(repo_root) / provider
    config_path = source_dir / "config.toml"
    auth_path = source_dir / "auth.json"
    key_lines = _read_provider_key_lines(source_dir)
    if not config_path.exists() or (not auth_path.exists() and not key_lines):
        raise FileNotFoundError(f"Provider {provider} missing config.toml and credentials")

    temp_root = repo_root / "runtime" / "agents" / "codex_home"
    temp_root.mkdir(parents=True, exist_ok=True)
    portable_home = Path(
        tempfile.mkdtemp(
            prefix=f"portable_{_safe_provider_name(provider)}_",
            dir=str(temp_root),
        )
    )
    portable_codex = portable_home / ".codex"
    portable_codex.mkdir(parents=True, exist_ok=True)

    override = _gateway_env_override(provider, source_dir)
    if override:
        (portable_codex / "config.toml").write_text(
            _render_gateway_config(
                model=override["model"],
                review_model=override["review_model"],
                reasoning_effort=override["reasoning_effort"],
                base_url=override["base_url"],
            ),
            encoding="utf-8",
        )
        (portable_codex / "auth.json").write_text(
            json.dumps({"OPENAI_API_KEY": override["api_key"]}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return portable_home

    shutil.copy2(config_path, portable_codex / "config.toml")
    if auth_path.exists():
        shutil.copy2(auth_path, portable_codex / "auth.json")
    else:
        api_key = ""
        if len(key_lines) >= 2 and key_lines[0].startswith(("http://", "https://")):
            api_key = key_lines[1]
        elif key_lines:
            api_key = key_lines[0]
        if not api_key:
            raise FileNotFoundError(f"Provider {provider} missing auth.json and key.txt API key")
        (portable_codex / "auth.json").write_text(
            json.dumps({"OPENAI_API_KEY": api_key}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return portable_home


def _provider_env(portable_home: Path) -> dict[str, str]:
    """Build env dict with HOME/USERPROFILE pointing at *portable_home*."""
    env = os.environ.copy()
    env["HOME"] = str(portable_home)
    env["USERPROFILE"] = str(portable_home)
    env["CODEX_HOME"] = str(portable_home / ".codex")
    env["PYTHONIOENCODING"] = "utf-8"

    auth_path = portable_home / ".codex" / "auth.json"
    if auth_path.exists():
        try:
            auth_payload = json.loads(auth_path.read_text(encoding="utf-8"))
            api_key = str(auth_payload.get("OPENAI_API_KEY") or "").strip()
            if api_key:
                env["OPENAI_API_KEY"] = api_key
        except Exception:
            pass

    config_path = portable_home / ".codex" / "config.toml"
    if config_path.exists():
        try:
            config = _load_toml(config_path)
            model = str(config.get("model") or "").strip()
            if model:
                env["OPENAI_MODEL"] = model
            provider_cfg = config.get("model_providers", {}).get("OpenAI", {})
            base_url = _normalize_openai_base_url(provider_cfg.get("base_url") or "")
            if base_url:
                env["OPENAI_BASE_URL"] = base_url
        except Exception:
            pass

    for proxy_key in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        env.pop(proxy_key, None)
    env["NO_PROXY"] = "*"
    return env


def _extract_agent_message(stdout_text: str) -> str:
    messages: list[str] = []
    for raw_line in (stdout_text or "").splitlines():
        line = raw_line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        item = payload.get("item")
        if isinstance(item, dict) and item.get("type") == "agent_message":
            text = str(item.get("text") or "").strip()
            if text:
                messages.append(text)
    return "\n".join(messages).strip()


def _failure_preview(stderr_text: str, stdout_text: str) -> str:
    candidate = (stderr_text or "").strip() or (stdout_text or "").strip()
    return candidate[:500]


async def _run_codex_attempt(
    prompt: str,
    repo_root: Path,
    exe: str,
    *,
    timeout_s: int,
    provider: str = "",
) -> tuple[str, str]:
    env: dict[str, str] | None = None
    portable_home: Path | None = None
    if provider:
        try:
            portable_home = _prepare_provider_home(repo_root, provider)
            env = _provider_env(portable_home)
        except FileNotFoundError as exc:
            logger.warning("[codex_bridge] Provider setup failed for %s: %s", provider, exc)
            return "", str(exc)

    tmp_dir = repo_root / "runtime" / "agents" / "codex_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    last_msg_path = tmp_dir / f"last_msg_{os.getpid()}_{_uuid.uuid4().hex[:12]}.txt"

    cmd = [
        exe, "exec",
        "--skip-git-repo-check",
        "--dangerously-bypass-approvals-and-sandbox",
        "--ephemeral",
        "--color", "never",
        "--cd", str(repo_root),
        "--json",
        "--output-last-message", str(last_msg_path),
        "-",
    ]

    logger.info(
        "[codex_bridge] Invoking Codex CLI (provider=%s, timeout=%ds)",
        provider or "(default)", timeout_s,
    )

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(repo_root),
            env=env,
        )

        communicate_task = asyncio.create_task(
            proc.communicate(input=prompt.encode("utf-8"))
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                asyncio.shield(communicate_task),
                timeout=timeout_s,
            )
        except asyncio.TimeoutError:
            logger.warning("[codex_bridge] Codex CLI timed out after %ds for provider=%s", timeout_s, provider or "(default)")
            pid = getattr(proc, "pid", None)
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            if os.name == "nt" and isinstance(pid, int) and pid > 0:
                try:
                    killer = await asyncio.create_subprocess_exec(
                        "taskkill",
                        "/PID",
                        str(pid),
                        "/T",
                        "/F",
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    await asyncio.wait_for(killer.wait(), timeout=5)
                except Exception:
                    pass
            try:
                await asyncio.wait_for(communicate_task, timeout=5)
            except Exception:
                communicate_task.cancel()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except Exception:
                    pass
            return "", f"timeout_after_{timeout_s}s"

        stdout_text = stdout.decode("utf-8", errors="replace")
        stderr_text = stderr.decode("utf-8", errors="replace")
        result = ""

        if last_msg_path.exists():
            result = last_msg_path.read_text(encoding="utf-8", errors="replace").strip()
        if not result:
            result = _extract_agent_message(stdout_text)
        if not result and stdout_text.strip() and not stdout_text.lstrip().startswith('{"type":'):
            result = stdout_text.strip()

        if proc.returncode == 0 and result:
            logger.info("[codex_bridge] Got %d chars from provider=%s", len(result), provider or "(default)")
            return result, ""

        if proc.returncode != 0 and result:
            logger.warning(
                "[codex_bridge] Codex CLI exited %d for provider=%s but produced output; accepting result",
                proc.returncode,
                provider or "(default)",
            )
            return result, _failure_preview(stderr_text, stdout_text)

        return "", _failure_preview(stderr_text, stdout_text)
    except Exception as exc:
        logger.exception("[codex_bridge] Codex CLI execution failed for provider=%s: %s", provider or "(default)", exc)
        return "", str(exc)
    finally:
        try:
            if last_msg_path.exists():
                last_msg_path.unlink()
        except OSError:
            pass
        if portable_home is not None:
            shutil.rmtree(portable_home, ignore_errors=True)


# ---------------------------------------------------------------------------
# Core execution
# ---------------------------------------------------------------------------

async def codex_exec(
    prompt: str,
    repo_root: Path,
    *,
    timeout_s: int = 300,
    provider: str = "",
) -> str:
    """Run Codex CLI with *prompt* and return the last-message text.

    Parameters
    ----------
    prompt : str
        The instruction to send via stdin.
    repo_root : Path
        Project root (for provider discovery and ``--skip-git-repo-check``).
    timeout_s : int
        Hard timeout in seconds (default 300).
    provider : str
        Explicit provider name.  If empty, ``detect_provider()`` is used.

    Returns
    -------
    str
        The Codex last-message output, or empty string on failure.
    """
    deadline = time.monotonic() + max(1, timeout_s)

    # --- Phase A: Try direct REST API first (fast, ~5-15s per attempt) ---
    logger.info("[codex_bridge] Trying REST API first (fast path)")
    rest_budget = min(90, max(30, timeout_s // 3))
    result = await _rest_api_fallback(prompt, repo_root, timeout_s=rest_budget)
    if result:
        return result

    # --- Phase B: Fall back to Codex CLI (slower but more capable) ---
    exe = resolve_codex_executable()
    if not exe:
        logger.warning("[codex_bridge] Codex CLI not found on PATH, REST failed too")
        return ""

    primary_provider = provider
    if provider:
        providers = [provider]
    else:
        preferred = detect_provider(repo_root)
        primary_provider = preferred
        providers = detect_provider_candidates(repo_root)
        if preferred:
            providers = _dedupe_keep_order([preferred, *providers])

    if not providers:
        providers = [""]

    failures: list[str] = []

    for index, candidate in enumerate(providers[:3]):  # cap at 3 CLI attempts
        remaining = int(deadline - time.monotonic())
        if remaining <= 15:
            break

        if provider or len(providers) - index <= 1:
            attempt_timeout = remaining
        elif index == 0 and primary_provider and _normalize_provider_ref(candidate) == _normalize_provider_ref(primary_provider):
            preferred_floor = 60 if remaining > 90 else 30
            attempt_timeout = min(remaining, max(preferred_floor, remaining // 2))
        else:
            attempt_timeout = min(remaining, max(15, remaining // max(1, len(providers) - index)))

        result, failure = await _run_codex_attempt(
            prompt,
            repo_root,
            exe,
            timeout_s=attempt_timeout,
            provider=candidate,
        )
        if result:
            if candidate:
                _record_success_provider(repo_root, candidate)
            return result

        label = candidate or "(default)"
        failures.append(f"{label}: {failure or 'no output'}")

    if failures:
        logger.warning("[codex_bridge] All Codex CLI providers failed: %s", " | ".join(failures[:6]))

    return ""


async def _rest_api_fallback(
    prompt: str,
    repo_root: Path,
    *,
    timeout_s: int = 120,
) -> str:
    """Direct HTTP call to provider /v1/responses as fast path."""
    try:
        import httpx
    except ImportError:
        logger.warning("[codex_bridge] httpx not installed, REST fallback unavailable")
        return ""

    candidates = detect_provider_candidates(repo_root)
    if not candidates:
        candidates = list(DEFAULT_PROVIDER_PRIORITY)

    for candidate in candidates[:4]:
        provider_dir = _provider_root(repo_root) / candidate
        if not provider_dir.is_dir():
            continue

        metadata = _provider_metadata(provider_dir)
        base_url = metadata.get("base_url", "")
        api_key = metadata.get("api_key", "")
        model = metadata.get("model", "gpt-5.4")

        if not (base_url and api_key):
            continue

        raw_base = base_url.rstrip("/")
        if raw_base.endswith("/v1"):
            raw_base = raw_base[:-3]

        for _attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_s, connect=10)) as client:
                    body: dict[str, Any] = {
                        "model": model,
                        "input": prompt[:32000],
                        "max_output_tokens": 8192,
                        "store": False,
                    }
                    resp = await client.post(
                        f"{raw_base}/v1/responses",
                        headers={
                            "Authorization": f"Bearer {api_key}",
                            "Content-Type": "application/json",
                        },
                        json=body,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        text = _extract_response_text(data)
                        if not text:
                            # Try alternate extraction: output_text at top-level or text.output
                            text = str(data.get("output_text") or "").strip()
                        if not text:
                            # Deep extraction: check text dict for output field
                            text_field = data.get("text")
                            if isinstance(text_field, dict):
                                text = str(text_field.get("output") or "").strip()
                        if text:
                            logger.info("[codex_bridge] REST API succeeded via %s (%d chars)", candidate, len(text))
                            _record_success_provider(repo_root, candidate)
                            return text
                        else:
                            logger.warning("[codex_bridge] REST %s: 200 but empty response body", candidate)
                            break  # empty body won't improve on retry
                    elif resp.status_code == 429:
                        logger.warning("[codex_bridge] REST %s: rate limited (429)", candidate)
                        break
                    elif resp.status_code == 503:
                        logger.warning("[codex_bridge] REST %s: HTTP 503 (attempt %d) %s", candidate, _attempt + 1, resp.text[:100])
                        import asyncio
                        await asyncio.sleep(15)
                        continue  # retry same candidate
                    else:
                        logger.warning("[codex_bridge] REST %s: HTTP %d %s", candidate, resp.status_code, resp.text[:100])
                        break
            except httpx.TimeoutException:
                logger.warning("[codex_bridge] REST %s: timeout after %ds", candidate, timeout_s)
                break
            except Exception as exc:
                logger.warning("[codex_bridge] REST %s failed: %s: %s", candidate, type(exc).__name__, exc)
                break

    return ""
