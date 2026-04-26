from __future__ import annotations

import argparse
import fnmatch
import json
import logging
import os
import re
import shutil
import statistics
import subprocess
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.11+ ships tomllib
    import tomli as tomllib  # type: ignore[no-redef]


logger = logging.getLogger(__name__)


def _kill_process_tree(pid: int) -> None:
    """Kill a process and all its children (Windows: taskkill /T, else SIGKILL)."""
    import sys
    try:
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True, check=False, timeout=15,
            )
        else:
            os.kill(pid, 9)
    except Exception:
        pass


DEFAULT_PROVIDER_PRIORITY = [
    "newapi-192.168.232.141-3000-stable",
    "newapi-192.168.232.141-3000-ro-a",
    "newapi-192.168.232.141-3000-ro-b",
    "newapi-192.168.232.141-3000-ro-c",
    "newapi-192.168.232.141-3000-ro-d",
    "bus.042999.xyz",
    "marybrown.dpdns.org",
    "code.claudex.us.ci",
    "elysiver.h-e.top",
]
DEFAULT_PROVIDER_SELECTION_COUNT = 5
DEFAULT_PROVIDER_ALLOWLIST = DEFAULT_PROVIDER_PRIORITY[:DEFAULT_PROVIDER_SELECTION_COUNT]
DEFAULT_PROVIDER_DENYLIST: list[str] = []
DEFAULT_MAX_EXTERNAL_DEPTH = 2
DEFAULT_MAX_WORKERS = 12
DEFAULT_INNER_AGENT_MAX_DEPTH = 1
DEFAULT_INNER_AGENT_MAX_THREADS = 4
DEFAULT_ALLOW_NATIVE_SUBAGENTS_AT_EXTERNAL_LIMIT = True
DEFAULT_TIMEOUT_SECONDS = 50 * 60
DEFAULT_HEDGE_DELAY_SECONDS = 30
DEFAULT_BENCH_ITERATIONS = 5
DEFAULT_BENCH_SUITE = "analysis_review"
DEFAULT_SUCCESS_RATE_THRESHOLD = 0.90
DEFAULT_MEDIAN_IMPROVEMENT_THRESHOLD = 0.20
DEFAULT_P95_REGRESSION_LIMIT = 0.10
DEFAULT_COOLDOWN_SECONDS = 120
# After this many seconds without any update, a provider's failure stats are
# considered stale and will be auto-recovered (counters halved) on next use.
DEFAULT_STALE_RECOVERY_SECONDS = 600
_STATE_LOCK = threading.Lock()
SINGLE_WRITER_PATHS = [
    "github/automation/live_fix_loop/issue_register.md",
    "github/automation/live_fix_loop/review_log.md",
]
ISSUE_REGISTER_PATH = SINGLE_WRITER_PATHS[0]
REVIEW_LOG_PATH = SINGLE_WRITER_PATHS[1]
_SHALLOW_COPY_IGNORE_PATTERNS = (
    ".git",
    "__pycache__",
    ".pytest_cache",
    ".pytest-tmp",
    ".pytest-*",
    ".pytest_tmp",
    ".pytest_*",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "node_modules",
    "runtime",
    "output",
    "logs",
    "data",
    ".playwright-cli",
    ".vscode",
    ".vscode*userdata",
    ".vscode*last-launch.txt",
    "_archive",
    "_temp",
    "tmp",
    "tmp-*",
    "tmp_*",
    "tmp_pytest*",
    "basetemp*",
    "*.sqlite",
    "*.sqlite*",
    "*.db",
    "*.db*",
    "*.tgz",
    "*.tar.gz",
    "*.etl",
    ".sandbox-bin",
)
_SHALLOW_COPY_SKIP_PREFIXES = (
    "docs/old",
    "ai-api/codex",
    "LiteLLM",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def runtime_root(root: Path | None = None) -> Path:
    return (root or repo_root()) / "runtime" / "codex_mesh"


def runs_root(root: Path | None = None) -> Path:
    return runtime_root(root) / "runs"


def worktrees_root(root: Path | None = None) -> Path:
    return runtime_root(root) / "worktrees"


def benchmarks_root(root: Path | None = None) -> Path:
    return runtime_root(root) / "benchmarks"


def state_path(root: Path | None = None) -> Path:
    return runtime_root(root) / "state.json"


def latest_summary_path(root: Path | None = None) -> Path:
    return runtime_root(root) / "latest_summary.json"


def codex_home_root(root: Path | None = None) -> Path:
    override = os.environ.get("CODEX_HOME_ROOT")
    if override:
        return Path(override)
    return (root or repo_root()) / "ai-api" / "codex"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat()


def run_id_now() -> str:
    return now_utc().astimezone().strftime("%Y%m%dT%H%M%S")


def _normalize_ref(value: str | None) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(_read_text(path))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return tomllib.loads(_read_text(path))
    except Exception:
        return {}


def _normalize_openai_base_url(value: str | None) -> str:
    clean = str(value or "").strip().rstrip("/")
    if not clean:
        return ""
    parsed = urlsplit(clean)
    path = parsed.path.rstrip("/")
    if not path.endswith("/v1"):
        path = f"{path}/v1" if path else "/v1"
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", "")).rstrip("/")


def _context_limits_for_model(model: str) -> tuple[int, int]:
    normalized = str(model or "").strip().lower()
    if normalized.startswith("gpt-5.4"):
        return 1000000, 900000
    return 400000, 320000


def _render_gateway_config(
    *,
    model: str,
    review_model: str,
    reasoning_effort: str,
    base_url: str,
) -> str:
    context_window, auto_compact_limit = _context_limits_for_model(model)
    return "\n".join(
        [
            'model_provider = "OpenAI"',
            f'model = "{model}"',
            f'review_model = "{review_model}"',
            f'model_reasoning_effort = "{reasoning_effort}"',
            "disable_response_storage = true",
            'network_access = "enabled"',
            "windows_wsl_setup_acknowledged = true",
            f"model_context_window = {context_window}",
            f"model_auto_compact_token_limit = {auto_compact_limit}",
            'personality = "pragmatic"',
            "",
            "[model_providers.OpenAI]",
            'name = "OpenAI"',
            f'base_url = "{base_url}"',
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


def _canonical_gateway_env_override(provider: ProviderSpec, source_dir: Path) -> dict[str, str] | None:
    canonical_provider = str(os.environ.get("CODEX_CANONICAL_PROVIDER") or "").strip()
    if not canonical_provider:
        return None
    if _normalize_ref(provider.provider_name) != _normalize_ref(canonical_provider):
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

    config = _load_toml(source_dir / "config.toml")
    model = str(config.get("model") or "").strip() or "gpt-5.4"
    review_model = str(config.get("review_model") or "").strip() or "gpt-5.2"
    reasoning_effort = str(config.get("model_reasoning_effort") or "").strip() or "xhigh"
    return {
        "base_url": base_url,
        "api_key": api_key,
        "model": model,
        "review_model": review_model,
        "reasoning_effort": reasoning_effort,
    }


def _copy_rel_posix(path: Path, source_root: Path) -> str:
    try:
        return path.relative_to(source_root).as_posix()
    except ValueError:
        return path.as_posix()


def _should_skip_copy_entry(path: Path, source_root: Path) -> bool:
    rel = _copy_rel_posix(path, source_root)
    if any(rel == prefix or rel.startswith(f"{prefix}/") for prefix in _SHALLOW_COPY_SKIP_PREFIXES):
        return True
    return any(fnmatch.fnmatch(path.name, pattern) for pattern in _SHALLOW_COPY_IGNORE_PATTERNS)


def _shallow_copytree(src: Path, dst: Path, *, source_root: Path | None = None) -> None:
    # Dirty worktrees often contain locked local caches. Skip transient paths and
    # tolerate per-file copy failures so mesh can still get an isolated workspace.
    source_root = source_root or src
    try:
        entries = list(src.iterdir())
    except OSError:
        return

    dst.mkdir(parents=True, exist_ok=True)
    for entry in entries:
        if _should_skip_copy_entry(entry, source_root):
            continue
        target = dst / entry.name
        try:
            is_dir = entry.is_dir()
        except OSError:
            continue
        if is_dir:
            _shallow_copytree(entry, target, source_root=source_root)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(entry, target)
        except OSError:
            continue


def resolve_codex_executable() -> str:
    # Allow overriding the codex binary via env var (e.g., for fake_codex.py wrapper)
    override = os.environ.get("CODEX_CLI_OVERRIDE", "").strip()
    if override and Path(override).exists():
        return override
    candidates = [
        shutil.which("codex.exe"),
        shutil.which("codex.cmd"),
        shutil.which("codex"),
    ]
    for candidate in candidates:
        if candidate:
            return candidate
    raise FileNotFoundError("Unable to locate Codex CLI on PATH.")


def _provider_tokens(provider_name: str, launcher_name: str | None = None) -> set[str]:
    tokens = {
        provider_name.strip().lower(),
        _normalize_ref(provider_name),
    }
    if launcher_name:
        tokens.add(launcher_name.strip().lower())
        tokens.add(_normalize_ref(launcher_name))
    return {token for token in tokens if token}


def _provider_sort_rank(provider_name: str) -> tuple[int, str]:
    try:
        return DEFAULT_PROVIDER_PRIORITY.index(provider_name), provider_name
    except ValueError:
        return len(DEFAULT_PROVIDER_PRIORITY), provider_name


def _extract_provider_from_ps1(ps1_path: Path) -> str | None:
    if not ps1_path.exists():
        return None
    text = _read_text(ps1_path)
    match = re.search(r'ai-api[\\/]+codex[\\/]+([^"\r\n]+)', text, flags=re.IGNORECASE)
    if not match:
        return None
    value = match.group(1).strip()
    return value.replace("\\", "/").split("/")[0].strip() or None


def _is_git_root_dirty(root: Path) -> bool:
    completed = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return completed.returncode == 0 and bool(completed.stdout.strip())


def _task_requires_isolated_workspace(task_kind: str, write_scope: list[str]) -> bool:
    return task_kind in {"write", "mixed"} and bool(write_scope)


def _path_like(value: str) -> str:
    return str(Path(value)).replace("\\", "/").rstrip("/")


def write_scopes_overlap(left: list[str], right: list[str]) -> bool:
    if not left or not right:
        return False
    for a in [_path_like(item) for item in left]:
        for b in [_path_like(item) for item in right]:
            if a == b:
                return True
            if a and b and (a.startswith(b + "/") or b.startswith(a + "/")):
                return True
    return False


@dataclass(slots=True)
class ProviderSpec:
    provider_name: str
    launcher_name: str
    launcher_cmd: str
    launcher_ps1: str
    codex_home: str
    model: str
    review_model: str | None
    reasoning_effort: str | None
    multi_agent_enabled: bool
    available: bool
    problems: list[str] = field(default_factory=list)

    @property
    def tokens(self) -> set[str]:
        return _provider_tokens(self.provider_name, self.launcher_name)


@dataclass(slots=True)
class MeshTaskManifest:
    task_id: str
    goal: str
    prompt: str
    task_kind: str = "analysis"
    read_scope: list[str] = field(default_factory=list)
    write_scope: list[str] = field(default_factory=list)
    max_external_depth: int = DEFAULT_MAX_EXTERNAL_DEPTH
    allow_native_subagents: bool = True
    allow_native_subagents_at_external_limit: bool = DEFAULT_ALLOW_NATIVE_SUBAGENTS_AT_EXTERNAL_LIMIT
    inner_agent_max_depth: int = DEFAULT_INNER_AGENT_MAX_DEPTH
    inner_agent_max_threads: int | None = DEFAULT_INNER_AGENT_MAX_THREADS
    provider_allowlist: list[str] = field(default_factory=list)
    provider_denylist: list[str] = field(default_factory=list)
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    benchmark_label: str | None = None
    output_mode: str = "text"
    working_root: str | None = None
    parent_task_id: str | None = None
    lineage_id: str | None = None
    depth: int = 1
    hedge_delay_seconds: int = DEFAULT_HEDGE_DELAY_SECONDS


@dataclass(slots=True)
class MeshRunManifest:
    tasks: list[MeshTaskManifest]
    execution_mode: str = "mesh"
    max_workers: int = DEFAULT_MAX_WORKERS
    benchmark_label: str | None = None
    dangerously_bypass: bool = True
    sandbox: str = "danger-full-access"
    ephemeral: bool = True
    provider_allowlist: list[str] = field(default_factory=list)
    provider_denylist: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ProviderAttemptResult:
    provider: str
    command: list[str]
    returncode: int | None
    stdout_path: str
    stderr_path: str
    last_message_path: str
    duration_seconds: float
    ok: bool
    status: str
    error: str | None = None
    started_at: str | None = None
    finished_at: str | None = None


@dataclass(slots=True)
class MeshTaskResult:
    task_id: str
    goal: str
    task_kind: str
    success: bool
    selected_provider: str | None
    output_mode: str
    provider_order: list[str]
    attempts: list[ProviderAttemptResult]
    execution_root: str
    workspace_kind: str
    depth: int
    parent_task_id: str | None
    lineage_id: str
    started_at: str
    finished_at: str
    benchmark_label: str | None = None


@dataclass(slots=True)
class MeshRunSummary:
    run_id: str
    execution_mode: str
    success: bool
    task_count: int
    max_workers: int
    benchmark_label: str | None
    manifest_path: str
    output_dir: str
    tasks: list[MeshTaskResult]
    provider_health: list[dict[str, Any]]
    started_at: str
    finished_at: str


@dataclass(slots=True)
class ExternalExecutionContext:
    depth: int
    max_external_depth: int
    parent_task_id: str | None = None
    lineage_id: str | None = None


class ExternalDepthLimitError(RuntimeError):
    pass


def default_state() -> dict[str, Any]:
    return {
        "rotation_cursor": 0,
        "providers": {},
        "latest_runs": [],
    }


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def resolve_external_execution_context(
    requested_max_depth: int,
    *,
    env: dict[str, str] | None = None,
) -> ExternalExecutionContext:
    current_env = env or os.environ
    effective_max_depth = max(1, int(requested_max_depth))
    parent_max_depth = _safe_int(current_env.get("CODEX_MESH_MAX_DEPTH"))
    if parent_max_depth is not None and parent_max_depth > 0:
        effective_max_depth = min(effective_max_depth, parent_max_depth)

    parent_depth = _safe_int(current_env.get("CODEX_MESH_DEPTH"))
    next_depth = 1 if parent_depth is None else parent_depth + 1
    if next_depth > effective_max_depth:
        raise ExternalDepthLimitError(
            f"external codex depth limit reached ({next_depth}>{effective_max_depth})"
        )

    parent_task_id = None
    lineage_id = None
    if parent_depth is not None:
        parent_task_id = str(current_env.get("CODEX_MESH_TASK_ID") or "").strip() or None
        lineage_id = str(current_env.get("CODEX_MESH_LINEAGE_ID") or parent_task_id or "").strip() or None

    return ExternalExecutionContext(
        depth=next_depth,
        max_external_depth=effective_max_depth,
        parent_task_id=parent_task_id,
        lineage_id=lineage_id,
    )


def apply_external_execution_context_env(
    env: dict[str, str],
    *,
    run_id: str,
    task_id: str,
    context: ExternalExecutionContext,
    lineage_id: str | None = None,
    parent_task_id: str | None = None,
) -> dict[str, str]:
    env["CODEX_MESH_RUN_ID"] = run_id
    env["CODEX_MESH_TASK_ID"] = task_id
    env["CODEX_MESH_DEPTH"] = str(context.depth)
    env["CODEX_MESH_MAX_DEPTH"] = str(context.max_external_depth)
    effective_parent = parent_task_id or context.parent_task_id
    effective_lineage = lineage_id or context.lineage_id or task_id
    env["CODEX_MESH_LINEAGE_ID"] = effective_lineage
    if effective_parent:
        env["CODEX_MESH_PARENT_TASK_ID"] = effective_parent
    else:
        env.pop("CODEX_MESH_PARENT_TASK_ID", None)
    return env


def inner_codex_config_overrides(
    enable_multi_agent: bool,
    *,
    agent_max_depth: int = DEFAULT_INNER_AGENT_MAX_DEPTH,
    agent_max_threads: int | None = DEFAULT_INNER_AGENT_MAX_THREADS,
) -> list[str]:
    # Newer Codex CLI builds parse `agents` as a structured role map, so the
    # legacy scalar overrides (`agents.max_depth=1`) fail before the session
    # starts. Keep subagent control at the feature-toggle/orchestrator layer
    # and avoid emitting incompatible config overrides.
    _ = enable_multi_agent
    _ = max(1, int(agent_max_depth))
    if agent_max_threads is not None:
        _ = max(1, int(agent_max_threads))
    return []


def inner_codex_feature_toggle_args(enable_multi_agent: bool) -> list[str]:
    return ["--enable", "multi_agent"] if enable_multi_agent else ["--disable", "multi_agent"]


def resolve_inner_codex_options(
    *,
    allow_native_subagents: bool,
    allow_native_subagents_at_external_limit: bool = DEFAULT_ALLOW_NATIVE_SUBAGENTS_AT_EXTERNAL_LIMIT,
    depth: int,
    max_external_depth: int,
    agent_max_depth: int = DEFAULT_INNER_AGENT_MAX_DEPTH,
    agent_max_threads: int | None = DEFAULT_INNER_AGENT_MAX_THREADS,
) -> dict[str, Any]:
    keep_native_subagents = bool(allow_native_subagents) and (
        bool(allow_native_subagents_at_external_limit)
        or int(depth) < max(1, int(max_external_depth))
    )
    return {
        "enable_multi_agent": keep_native_subagents,
        "agent_max_depth": max(1, int(agent_max_depth)),
        "agent_max_threads": None if agent_max_threads is None else max(1, int(agent_max_threads)),
    }


def load_state(root: Path | None = None) -> dict[str, Any]:
    state = _load_json(state_path(root))
    if not state:
        return default_state()
    payload = default_state()
    payload.update(state)
    payload["providers"] = state.get("providers", {}) if isinstance(state.get("providers"), dict) else {}
    payload["latest_runs"] = state.get("latest_runs", []) if isinstance(state.get("latest_runs"), list) else []
    return payload


def save_state(payload: dict[str, Any], root: Path | None = None) -> None:
    _write_json(state_path(root), payload)


def provider_health(spec: ProviderSpec, state: dict[str, Any], *, now_ts: float | None = None) -> dict[str, Any]:
    now_ts = time.time() if now_ts is None else now_ts
    entry = state.get("providers", {}).get(spec.provider_name, {})
    attempts = int(entry.get("attempts", 0))
    successes = int(entry.get("successes", 0))
    failures = int(entry.get("failures", 0))
    durations = [float(item) for item in entry.get("durations", []) if isinstance(item, (int, float))]
    cooldown_until = float(entry.get("cooldown_until", 0) or 0)
    median_duration = statistics.median(durations) if durations else None
    success_rate = (successes / attempts) if attempts else None
    return {
        "provider": spec.provider_name,
        "launcher": spec.launcher_name,
        "attempts": attempts,
        "successes": successes,
        "failures": failures,
        "success_rate": success_rate,
        "median_duration_seconds": median_duration,
        "cooldown_active": cooldown_until > now_ts,
        "cooldown_until": cooldown_until or None,
        "multi_agent_enabled": spec.multi_agent_enabled,
        "available": spec.available,
        "problems": list(spec.problems),
    }


def discover_providers(
    root: Path | None = None,
    *,
    allowlist: list[str] | None = None,
    denylist: list[str] | None = None,
) -> list[ProviderSpec]:
    root = root or repo_root()
    codex_root = codex_home_root(root)
    allow_tokens = {_normalize_ref(item) for item in (allowlist or []) if item}
    deny_tokens = {_normalize_ref(item) for item in (denylist or DEFAULT_PROVIDER_DENYLIST) if item}
    specs: list[ProviderSpec] = []
    seen: set[str] = set()

    for cmd_path in sorted(root.glob("open-with-*.cmd")):
        launcher_name = cmd_path.stem[len("open-with-") :]
        ps1_path = cmd_path.with_suffix(".ps1")
        provider_name = _extract_provider_from_ps1(ps1_path) or launcher_name
        tokens = _provider_tokens(provider_name, launcher_name)
        if allow_tokens and not tokens.intersection(allow_tokens):
            continue
        if deny_tokens and tokens.intersection(deny_tokens):
            continue
        if provider_name in seen:
            continue
        seen.add(provider_name)

        provider_home = codex_root / provider_name
        config_path = provider_home / "config.toml"
        auth_path = provider_home / "auth.json"
        config = _load_toml(config_path)
        features = config.get("features", {}) if isinstance(config.get("features"), dict) else {}
        problems: list[str] = []
        if not config_path.exists():
            problems.append("missing_config")
        if not auth_path.exists():
            problems.append("missing_auth")
        if features.get("multi_agent") is not True:
            problems.append("multi_agent_disabled")
        model = str(config.get("model") or "").strip()
        review_model = str(config.get("review_model") or "").strip() or None
        reasoning_effort = str(config.get("model_reasoning_effort") or "").strip() or None
        specs.append(
            ProviderSpec(
                provider_name=provider_name,
                launcher_name=launcher_name,
                launcher_cmd=str(cmd_path),
                launcher_ps1=str(ps1_path),
                codex_home=str(provider_home),
                model=model,
                review_model=review_model,
                reasoning_effort=reasoning_effort,
                multi_agent_enabled=features.get("multi_agent") is True,
                available=not problems,
                problems=problems,
            )
        )

    if allow_tokens:
        found_tokens = set().union(*(spec.tokens for spec in specs)) if specs else set()
        unresolved_tokens = allow_tokens.difference(found_tokens)
    else:
        unresolved_tokens = set()
    if unresolved_tokens:
        for provider_home in sorted(codex_root.iterdir() if codex_root.exists() else [], key=lambda item: item.name):
            if not provider_home.is_dir() or provider_home.name.startswith("portable_"):
                continue
            provider_name = provider_home.name
            tokens = _provider_tokens(provider_name)
            if not unresolved_tokens.intersection(tokens):
                continue
            if deny_tokens and tokens.intersection(deny_tokens):
                continue
            if provider_name in seen:
                continue
            config_path = provider_home / "config.toml"
            auth_path = provider_home / "auth.json"
            config = _load_toml(config_path)
            features = config.get("features", {}) if isinstance(config.get("features"), dict) else {}
            problems: list[str] = []
            if not config_path.exists():
                problems.append("missing_config")
            if not auth_path.exists():
                problems.append("missing_auth")
            if features.get("multi_agent") is not True:
                problems.append("multi_agent_disabled")
            specs.append(
                ProviderSpec(
                    provider_name=provider_name,
                    launcher_name=provider_name,
                    launcher_cmd="",
                    launcher_ps1="",
                    codex_home=str(provider_home),
                    model=str(config.get("model") or "").strip(),
                    review_model=str(config.get("review_model") or "").strip() or None,
                    reasoning_effort=str(config.get("model_reasoning_effort") or "").strip() or None,
                    multi_agent_enabled=features.get("multi_agent") is True,
                    available=not problems,
                    problems=problems,
                )
            )
            seen.add(provider_name)

    specs.sort(key=lambda item: _provider_sort_rank(item.provider_name))
    return [spec for spec in specs if spec.available]


def order_providers(specs: list[ProviderSpec], state: dict[str, Any]) -> list[ProviderSpec]:
    if not specs:
        return []
    base = list(specs)
    cursor = int(state.get("rotation_cursor", 0)) % len(base)
    rotated = base[cursor:] + base[:cursor]
    now_ts = time.time()
    scored: list[tuple[tuple[int, float, float, int], ProviderSpec]] = []
    for index, spec in enumerate(rotated):
        health = provider_health(spec, state, now_ts=now_ts)
        success_rate = health["success_rate"] if health["success_rate"] is not None else 0.5
        median = health["median_duration_seconds"] if health["median_duration_seconds"] is not None else float(DEFAULT_TIMEOUT_SECONDS)
        cooldown_bucket = 1 if health["cooldown_active"] else 0
        score = (
            cooldown_bucket,
            -float(success_rate),
            float(median),
            index,
        )
        scored.append((score, spec))
    return [item for _, item in sorted(scored, key=lambda pair: pair[0])]


def resolve_provider_allowlist(
    root: Path | None = None,
    *,
    allowlist: list[str] | None = None,
    denylist: list[str] | None = None,
    state: dict[str, Any] | None = None,
    selection_count: int = DEFAULT_PROVIDER_SELECTION_COUNT,
) -> list[str]:
    explicit = [item for item in (allowlist or []) if item]
    if explicit:
        return explicit

    # Check for lane-specific readonly allowlist first (new dual-track support)
    readonly_allowlist_raw = str(os.environ.get("CODEX_READONLY_PROVIDER_ALLOWLIST") or "").strip()
    readonly_lane = str(os.environ.get("CODEX_READONLY_LANE") or "").strip()
    if readonly_allowlist_raw and readonly_lane:
        # Readonly lane has explicit provider list — use it instead of canonical
        return [p.strip() for p in readonly_allowlist_raw.split(",") if p.strip()]

    # Gateway-only mode: only allowed for stable lane, no longer forces readonly workers
    gateway_only = str(os.environ.get("CODEX_AUDIT_GATEWAY_ONLY") or "").strip().lower()
    canonical_provider = str(os.environ.get("CODEX_CANONICAL_PROVIDER") or "").strip()
    stable_lane = str(os.environ.get("CODEX_STABLE_LANE") or "").strip()
    if gateway_only in {"1", "true", "yes", "on"} and canonical_provider:
        if stable_lane:
            # Gateway-only is restricted to stable lane; readonly workers should use
            # their own allowlist (set above) or fall through to multi-provider discovery
            pass
        else:
            # Legacy behavior: no lane config, fall back to canonical
            return [canonical_provider]

    effective_denylist = [item for item in (denylist or DEFAULT_PROVIDER_DENYLIST) if item]
    limit = max(1, selection_count)
    specs = discover_providers(
        root,
        allowlist=list(DEFAULT_PROVIDER_PRIORITY),
        denylist=effective_denylist,
    )
    if specs:
        ordered = order_providers(specs, state or default_state())
        return [item.provider_name for item in ordered[:limit]]

    deny_tokens = {_normalize_ref(item) for item in effective_denylist}
    fallback = [
        item for item in DEFAULT_PROVIDER_ALLOWLIST
        if _normalize_ref(item) not in deny_tokens
    ]
    return fallback[:limit]


def _update_provider_state(
    state: dict[str, Any],
    provider_name: str,
    *,
    ok: bool,
    duration_seconds: float,
    cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
) -> None:
    with _STATE_LOCK:
        providers = state.setdefault("providers", {})
        entry = providers.setdefault(
            provider_name,
            {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "durations": [],
                "cooldown_until": 0,
                "last_status": None,
                "updated_at": None,
            },
        )
        # Auto-recovery: if cooldown expired and entry is stale, halve failure
        # stats so the provider gets a fresh chance instead of being permanently
        # penalised.
        now = time.time()
        cooldown_until = float(entry.get("cooldown_until", 0) or 0)
        updated_at_str = entry.get("updated_at") or ""
        stale = False
        if cooldown_until and cooldown_until < now:
            # Cooldown expired — check staleness
            if updated_at_str:
                try:
                    last_update = datetime.fromisoformat(updated_at_str).timestamp()
                    stale = (now - last_update) > DEFAULT_STALE_RECOVERY_SECONDS
                except (ValueError, TypeError, OSError):
                    stale = True
            else:
                stale = True
        if stale and int(entry.get("failures", 0)) > 0:
            old_failures = int(entry.get("failures", 0))
            entry["failures"] = max(0, old_failures // 2)
            entry["attempts"] = max(int(entry.get("successes", 0)), int(entry.get("attempts", 0)) // 2)
            entry["cooldown_until"] = 0

        entry["attempts"] = int(entry.get("attempts", 0)) + 1
        if ok:
            entry["successes"] = int(entry.get("successes", 0)) + 1
            entry["cooldown_until"] = 0
            entry["last_status"] = "ok"
        else:
            entry["failures"] = int(entry.get("failures", 0)) + 1
            entry["cooldown_until"] = time.time() + cooldown_seconds
            entry["last_status"] = "failed"
        durations = [float(item) for item in entry.get("durations", []) if isinstance(item, (int, float))]
        durations.append(float(duration_seconds))
        entry["durations"] = durations[-20:]
        entry["updated_at"] = now_iso()


def _advance_rotation_cursor(state: dict[str, Any], provider_count: int) -> None:
    if provider_count <= 0:
        return
    with _STATE_LOCK:
        state["rotation_cursor"] = (int(state.get("rotation_cursor", 0)) + 1) % provider_count


# ---------------------------------------------------------------------------
# Codex CLI auth & base_url helpers
# ---------------------------------------------------------------------------
_CODEX_AUTH_LOCK = threading.Lock()
_CODEX_AUTH_CURRENT_KEY: str | None = None


def _ensure_codex_auth(api_key: str) -> None:
    """Ensure the global codex credential store has the correct API key.

    Codex CLI v0.104+ reads auth from a secure credential store (e.g.
    Windows Credential Manager) populated by ``codex login --with-api-key``.
    Simply writing ``~/.codex/auth.json`` is NOT sufficient — the CLI
    ignores it for the Responses API auth flow.

    This function calls ``codex login --with-api-key`` via subprocess,
    feeding the key on stdin, which is the only reliable way to set auth.
    A threading lock ensures concurrent tasks don't race on the global
    credential store.
    """
    global _CODEX_AUTH_CURRENT_KEY
    with _CODEX_AUTH_LOCK:
        if _CODEX_AUTH_CURRENT_KEY == api_key:
            return  # already set
        try:
            codex_bin = resolve_codex_executable()
            proc = subprocess.run(
                [codex_bin, "login", "--with-api-key"],
                input=api_key,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if proc.returncode == 0:
                _CODEX_AUTH_CURRENT_KEY = api_key
        except Exception:
            pass  # best-effort; the existing global auth may still work


def _extract_provider_base_url(portable_home: Path) -> str | None:
    """Extract base_url from the provider's portable config.toml."""
    cfg_file = portable_home / ".codex" / "config.toml"
    if not cfg_file.exists():
        return None
    try:
        cfg = _load_toml(cfg_file)
        for prov in cfg.get("model_providers", {}).values():
            if isinstance(prov, dict):
                base_url = str(prov.get("base_url") or "").strip()
                if base_url:
                    return base_url
    except Exception:
        pass
    return None


def _extract_provider_api_key(portable_home: Path) -> str | None:
    """Extract API key from the provider's portable auth.json."""
    auth_file = portable_home / ".codex" / "auth.json"
    if not auth_file.exists():
        return None
    try:
        auth = json.loads(auth_file.read_text(encoding="utf-8"))
        key = str(auth.get("OPENAI_API_KEY") or "").strip()
        return key if key else None
    except Exception:
        return None


def _build_provider_home(root: Path, provider: ProviderSpec) -> Path:
    source_dir = Path(provider.codex_home)
    override = _canonical_gateway_env_override(provider, source_dir)

    portable_home = codex_home_root(root) / f"portable_{provider.provider_name.replace('/', '_')}"
    portable_codex = portable_home / ".codex"
    portable_codex.mkdir(parents=True, exist_ok=True)

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

    config_path = source_dir / "config.toml"
    auth_path = source_dir / "auth.json"
    if not config_path.exists():
        raise FileNotFoundError(f"Missing provider config.toml: {config_path}")
    if not auth_path.exists():
        raise FileNotFoundError(f"Missing provider auth.json: {auth_path}")

    shutil.copy2(config_path, portable_codex / "config.toml")
    shutil.copy2(auth_path, portable_codex / "auth.json")
    return portable_home


def _provider_env(
    portable_home: Path,
    *,
    run_id: str,
    task: MeshTaskManifest,
    provider_allowlist: list[str],
    provider_denylist: list[str],
) -> dict[str, str]:
    env = os.environ.copy()
    env["HOME"] = str(portable_home)
    env["USERPROFILE"] = str(portable_home)
    env["PYTHONIOENCODING"] = "utf-8"
    # Inject API key and base URL directly as env vars so codex CLI uses the
    # provider's API key instead of any cached chatgpt JWT tokens.
    _codex_dir = portable_home / ".codex"
    _auth_file = _codex_dir / "auth.json"
    _cfg_file = _codex_dir / "config.toml"
    if _auth_file.exists():
        try:
            _auth = json.loads(_auth_file.read_text(encoding="utf-8"))
            _api_key = str(_auth.get("OPENAI_API_KEY") or "").strip()
            if _api_key:
                env["OPENAI_API_KEY"] = _api_key
        except Exception:
            pass
    if _cfg_file.exists():
        try:
            _cfg = _load_toml(_cfg_file)
            _model_name = str(_cfg.get("model") or "").strip()
            if _model_name:
                env["OPENAI_MODEL"] = _model_name
            _providers_cfg = _cfg.get("model_providers", {})
            for _prov in _providers_cfg.values():
                if isinstance(_prov, dict):
                    _base_url = str(_prov.get("base_url") or "").strip()
                    if _base_url:
                        env["OPENAI_BASE_URL"] = _base_url
                        break
        except Exception:
            pass
    # Ensure provider calls are direct and not hijacked by system/global proxies.
    for _proxy_key in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        env.pop(_proxy_key, None)
    env["NO_PROXY"] = "*"
    env["no_proxy"] = "*"
    apply_external_execution_context_env(
        env,
        run_id=run_id,
        task_id=task.task_id,
        context=ExternalExecutionContext(
            depth=task.depth,
            max_external_depth=task.max_external_depth,
            parent_task_id=task.parent_task_id,
            lineage_id=task.lineage_id or task.task_id,
        ),
        lineage_id=task.lineage_id or task.task_id,
        parent_task_id=task.parent_task_id,
    )
    env["CODEX_MESH_PROVIDER_ALLOWLIST"] = ",".join(provider_allowlist)
    env["CODEX_MESH_PROVIDER_DENYLIST"] = ",".join(provider_denylist)
    env["CODEX_MESH_WRITE_SCOPE"] = json.dumps(task.write_scope, ensure_ascii=False)
    env["CODEX_MESH_SINGLE_WRITER_PATHS"] = json.dumps(SINGLE_WRITER_PATHS, ensure_ascii=False)
    return env


def build_codex_command(
    *,
    codex_binary: str,
    execution_root: Path,
    last_message_path: Path,
    enable_multi_agent: bool,
    agent_max_depth: int = DEFAULT_INNER_AGENT_MAX_DEPTH,
    agent_max_threads: int | None = DEFAULT_INNER_AGENT_MAX_THREADS,
    dangerously_bypass: bool,
    sandbox: str,
    ephemeral: bool,
    provider_base_url: str | None = None,
) -> list[str]:
    command = [
        codex_binary,
        "exec",
        "--skip-git-repo-check",
        "--color",
        "never",
        "--cd",
        str(execution_root),
        "--output-last-message",
        str(last_message_path),
        "--json",
    ]
    command.extend(inner_codex_feature_toggle_args(enable_multi_agent))
    for override in inner_codex_config_overrides(
        enable_multi_agent,
        agent_max_depth=agent_max_depth,
        agent_max_threads=agent_max_threads,
    ):
        command.extend(["--config", override])
    # Codex CLI v0.104+ ignores OPENAI_BASE_URL env var for URL resolution;
    # the only reliable way to direct requests to a non-default endpoint is
    # via the `-c` config override flag.
    if provider_base_url:
        command.extend(["--config", f'model_providers.OpenAI.base_url="{provider_base_url}"'])
    if ephemeral:
        command.append("--ephemeral")
    if dangerously_bypass:
        command.append("--dangerously-bypass-approvals-and-sandbox")
    else:
        command.extend(["-s", sandbox])
    command.append("-")
    return command


def build_mesh_prompt(
    *,
    task: MeshTaskManifest,
    run_id: str,
    provider_order: list[str],
    execution_root: Path,
) -> str:
    """Build prompt for a mesh task.

    If the prompt already contains an orchestrator overlay (from run.py),
    don't add another one — avoid double overlay.
    """
    coordination_lines = [
        "--- Mesh Write Coordination ---",
        f"read_scope: {json.dumps(task.read_scope, ensure_ascii=False)}",
        f"write_scope: {json.dumps(task.write_scope, ensure_ascii=False)}",
        f"single_writer_paths: {json.dumps(SINGLE_WRITER_PATHS, ensure_ascii=False)}",
        f"issue_register_path: {ISSUE_REGISTER_PATH}",
        f"review_log_path: {REVIEW_LOG_PATH}",
        "---",
        "",
    ]
    if "--- Orchestrator Context ---" in task.prompt or "--- Codex Mesh Context ---" in task.prompt:
        prompt = task.prompt.strip()
        if "single_writer_paths:" in prompt or "--- Mesh Write Coordination ---" in prompt:
            return prompt + "\n"
        return prompt + "\n\n" + "\n".join(coordination_lines)
    overlay = [
        "--- Codex Mesh Context ---",
        f"run_id: {run_id}",
        f"task_id: {task.task_id}",
        f"depth: {task.depth}/{task.max_external_depth}",
        f"execution_root: {execution_root}",
        f"providers: {', '.join(provider_order)}",
        *coordination_lines,
    ]
    return "\n".join(overlay) + task.prompt.strip() + "\n"


def _write_workspace_agents_override(
    execution_root: Path,
    *,
    run_id: str,
    task: MeshTaskManifest,
    provider_order: list[str],
) -> None:
    """Write AGENTS.override.md so Codex natively picks up mesh context.

    Pure structural info — no tactical instructions.
    """
    content = (
        "# Mesh Orchestration Context\n\n"
        f"- run_id: {run_id}\n"
        f"- task_id: {task.task_id}\n"
        f"- external_depth: {task.depth}/{task.max_external_depth}\n"
        f"- providers: {', '.join(provider_order)}\n"
        f"- read_scope: {json.dumps(task.read_scope, ensure_ascii=False)}\n"
        f"- write_scope: {json.dumps(task.write_scope, ensure_ascii=False)}\n"
        f"- single_writer_paths: {json.dumps(SINGLE_WRITER_PATHS, ensure_ascii=False)}\n"
        f"- issue_register_path: {ISSUE_REGISTER_PATH}\n"
        f"- review_log_path: {REVIEW_LOG_PATH}\n"
    )
    override_path = execution_root / "AGENTS.override.md"
    override_path.write_text(content, encoding="utf-8")


def _ensure_execution_root(root: Path, run_id: str, task: MeshTaskManifest) -> tuple[Path, str]:
    source_root = Path(task.working_root or root).resolve()
    if not _task_requires_isolated_workspace(task.task_kind, task.write_scope):
        return source_root, "shared"

    target = worktrees_root(root) / run_id / task.task_id
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists():
        return target, "existing"

    if not _is_git_root_dirty(source_root):
        completed = subprocess.run(
            ["git", "-C", str(source_root), "worktree", "add", "--detach", str(target), "HEAD"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if completed.returncode == 0:
            return target, "git_worktree"

    try:
        _shallow_copytree(source_root, target)
    except Exception as exc:
        shutil.rmtree(target, ignore_errors=True)
        raise RuntimeError(f"workspace_copy_failed:{source_root}->{target}:{exc}") from exc
    return target, "copy"


def _cleanup_execution_root(root: Path, execution_root: Path, workspace_kind: str) -> None:
    if workspace_kind == "git_worktree":
        completed = subprocess.run(
            ["git", "-C", str(root), "worktree", "remove", "--force", str(execution_root)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if completed.returncode == 0:
            return
    if workspace_kind in {"git_worktree", "copy"}:
        shutil.rmtree(execution_root, ignore_errors=True)


class RunningAttempt:
    def __init__(
        self,
        *,
        provider: ProviderSpec,
        command: list[str],
        prompt: str,
        env: dict[str, str],
        execution_root: Path,
        stdout_path: Path,
        stderr_path: Path,
        last_message_path: Path,
        timeout_seconds: int,
    ) -> None:
        self.provider = provider
        self.command = command
        self.prompt = prompt
        self.env = env
        self.execution_root = execution_root
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path
        self.last_message_path = last_message_path
        self.timeout_seconds = timeout_seconds
        self.process: subprocess.Popen[str] | None = None
        self.done = threading.Event()
        self.started_at = now_iso()
        self.finished_at: str | None = None
        self.returncode: int | None = None
        self.duration_seconds = 0.0
        self.error: str | None = None
        self._cancel_reason: str | None = None
        self._thread = threading.Thread(target=self._communicate, daemon=True)

    def start(self) -> None:
        self.stdout_path.parent.mkdir(parents=True, exist_ok=True)
        self.last_message_path.parent.mkdir(parents=True, exist_ok=True)
        self.process = subprocess.Popen(
            self.command,
            cwd=str(self.execution_root),
            env=self.env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        self._thread.start()

    def _communicate(self) -> None:
        assert self.process is not None
        started = time.perf_counter()
        stdout = ""
        stderr = ""
        try:
            stdout, stderr = self.process.communicate(self.prompt, timeout=self.timeout_seconds)
            self.returncode = self.process.returncode
        except subprocess.TimeoutExpired:
            self.error = f"timeout_after_{self.timeout_seconds}s"
            _kill_process_tree(self.process.pid)
            try:
                stdout, stderr = self.process.communicate(timeout=10)
            except Exception:
                stdout = stdout or ""
                stderr = stderr or ""
            self.returncode = None
        except Exception as exc:  # pragma: no cover - defensive
            self.error = f"attempt_failed:{exc}"
            if self.process.poll() is None:
                _kill_process_tree(self.process.pid)
            try:
                stdout, stderr = self.process.communicate(timeout=10)
            except Exception:
                stdout = stdout or ""
                stderr = stderr or ""
            self.returncode = None
        finally:
            self.duration_seconds = round(time.perf_counter() - started, 3)
            self.stdout_path.write_text(stdout or "", encoding="utf-8")
            self.stderr_path.write_text(stderr or "", encoding="utf-8")
            if not self.last_message_path.exists():
                self.last_message_path.write_text("", encoding="utf-8")
            self.finished_at = now_iso()
            self.done.set()

    def terminate(self, reason: str) -> None:
        self._cancel_reason = reason
        if self.process is None:
            return
        if self.process.poll() is not None:
            return
        try:
            self.process.terminate()
        except OSError:
            return

    def join(self, timeout: float | None = None) -> None:
        self.done.wait(timeout=timeout)

    def result(
        self,
        *,
        status_override: str | None = None,
        join_timeout: float | None = None,
    ) -> ProviderAttemptResult:
        self.join(timeout=join_timeout)
        if self.finished_at is None:
            self.finished_at = now_iso()
        last_message = _read_text(self.last_message_path) if self.last_message_path.exists() else ""
        ok = self.returncode == 0 and bool(last_message.strip())
        status = status_override or ("success" if ok else "failed")
        if self._cancel_reason:
            status = self._cancel_reason
            ok = False
        return ProviderAttemptResult(
            provider=self.provider.provider_name,
            command=self.command,
            returncode=self.returncode,
            stdout_path=str(self.stdout_path),
            stderr_path=str(self.stderr_path),
            last_message_path=str(self.last_message_path),
            duration_seconds=self.duration_seconds,
            ok=ok,
            status=status,
            error=self.error,
            started_at=self.started_at,
            finished_at=self.finished_at,
        )


def _run_attempt_blocking(
    *,
    provider: ProviderSpec,
    command: list[str],
    prompt: str,
    env: dict[str, str],
    execution_root: Path,
    stdout_path: Path,
    stderr_path: Path,
    last_message_path: Path,
    timeout_seconds: int,
) -> ProviderAttemptResult:
    runner = RunningAttempt(
        provider=provider,
        command=command,
        prompt=prompt,
        env=env,
        execution_root=execution_root,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        last_message_path=last_message_path,
        timeout_seconds=timeout_seconds,
    )
    runner.start()
    runner.join(timeout=timeout_seconds + 5)
    return runner.result()


def _execute_with_hedge(
    *,
    root: Path,
    providers: list[ProviderSpec],
    prompt: str,
    task_dir: Path,
    execution_root: Path,
    run_id: str,
    task: MeshTaskManifest,
    dangerously_bypass: bool,
    sandbox: str,
    ephemeral: bool,
) -> list[ProviderAttemptResult]:
    pending = list(providers)
    active: list[RunningAttempt] = []
    results: list[ProviderAttemptResult] = []
    hedge_deadline: float | None = None
    codex_binary = resolve_codex_executable()
    inner_codex = resolve_inner_codex_options(
        allow_native_subagents=task.allow_native_subagents,
        allow_native_subagents_at_external_limit=task.allow_native_subagents_at_external_limit,
        depth=task.depth,
        max_external_depth=task.max_external_depth,
        agent_max_depth=task.inner_agent_max_depth,
        agent_max_threads=task.inner_agent_max_threads,
    )

    def launch(provider: ProviderSpec) -> RunningAttempt:
        provider_dir = task_dir / provider.provider_name.replace("/", "_")
        portable_home = _build_provider_home(root, provider)
        # Ensure the global codex auth has the provider's API key
        _prov_api_key = _extract_provider_api_key(portable_home)
        if _prov_api_key:
            _ensure_codex_auth(_prov_api_key)
        _prov_base_url = _extract_provider_base_url(portable_home)
        env = _provider_env(
            portable_home,
            run_id=run_id,
            task=task,
            provider_allowlist=task.provider_allowlist or DEFAULT_PROVIDER_ALLOWLIST,
            provider_denylist=task.provider_denylist or DEFAULT_PROVIDER_DENYLIST,
        )
        command = build_codex_command(
            codex_binary=codex_binary,
            execution_root=execution_root,
            last_message_path=provider_dir / "last_message.txt",
            enable_multi_agent=bool(inner_codex["enable_multi_agent"]),
            agent_max_depth=int(inner_codex["agent_max_depth"]),
            agent_max_threads=inner_codex["agent_max_threads"],
            dangerously_bypass=dangerously_bypass,
            sandbox=sandbox,
            ephemeral=ephemeral,
            provider_base_url=_prov_base_url,
        )
        attempt = RunningAttempt(
            provider=provider,
            command=command,
            prompt=prompt,
            env=env,
            execution_root=execution_root,
            stdout_path=provider_dir / "stdout.jsonl",
            stderr_path=provider_dir / "stderr.log",
            last_message_path=provider_dir / "last_message.txt",
            timeout_seconds=task.timeout_seconds,
        )
        attempt.start()
        return attempt

    if pending:
        active.append(launch(pending.pop(0)))
        hedge_deadline = time.monotonic() + max(1, task.hedge_delay_seconds)

    while active:
        completed_any = False
        for attempt in list(active):
            if attempt.done.is_set():
                completed_any = True
                result = attempt.result()
                results.append(result)
                active.remove(attempt)
                if result.ok:
                    for other in active:
                        other.terminate("late_cancelled")
                    for other in list(active):
                        other.join(timeout=15)
                        results.append(other.result(status_override="late_cancelled", join_timeout=0))
                        active.remove(other)
                    return results

        if completed_any and not active and pending:
            active.append(launch(pending.pop(0)))
            hedge_deadline = time.monotonic() + max(1, task.hedge_delay_seconds)
            continue

        now = time.monotonic()
        if pending and hedge_deadline is not None and now >= hedge_deadline and len(active) == 1:
            active.append(launch(pending.pop(0)))
            hedge_deadline = None

        time.sleep(0.2)

    return results


def _execute_task(
    *,
    root: Path,
    run_id: str,
    task: MeshTaskManifest,
    state: dict[str, Any],
    execution_mode: str,
    dangerously_bypass: bool,
    sandbox: str,
    ephemeral: bool,
    provider_allowlist: list[str],
    provider_denylist: list[str],
) -> MeshTaskResult:
    task_started_at = now_iso()
    lineage_id = task.lineage_id or task.task_id
    task_dir = runs_root(root) / run_id / "tasks" / task.task_id
    task_dir.mkdir(parents=True, exist_ok=True)
    execution_root: Path | None = None
    workspace_kind = "shared"
    try:
        eligible_specs = discover_providers(
            root=root,
            allowlist=task.provider_allowlist or provider_allowlist,
            denylist=task.provider_denylist or provider_denylist,
        )
        ordered_specs = order_providers(eligible_specs, state)
        provider_names = [item.provider_name for item in ordered_specs]
        execution_root, workspace_kind = _ensure_execution_root(root, run_id, task)

        # Write AGENTS.override.md in isolated workspaces so Codex natively
        # picks up mesh context without needing prompt overlay duplication.
        if workspace_kind in ("git_worktree", "copy"):
            _write_workspace_agents_override(execution_root, run_id=run_id, task=task, provider_order=provider_names)

        prompt = build_mesh_prompt(
            task=task,
            run_id=run_id,
            provider_order=provider_names,
            execution_root=execution_root,
        )
        _write_json(task_dir / "manifest.json", asdict(task))
        (task_dir / "prompt.txt").write_text(prompt, encoding="utf-8")

        attempts: list[ProviderAttemptResult]
        if execution_mode == "mesh" and len(ordered_specs) > 1:
            attempts = _execute_with_hedge(
                root=root,
                providers=ordered_specs,
                prompt=prompt,
                task_dir=task_dir,
                execution_root=execution_root,
                run_id=run_id,
                task=task,
                dangerously_bypass=dangerously_bypass,
                sandbox=sandbox,
                ephemeral=ephemeral,
            )
        else:
            attempts = []
            codex_binary = resolve_codex_executable()
            inner_codex = resolve_inner_codex_options(
                allow_native_subagents=task.allow_native_subagents,
                allow_native_subagents_at_external_limit=task.allow_native_subagents_at_external_limit,
                depth=task.depth,
                max_external_depth=task.max_external_depth,
                agent_max_depth=task.inner_agent_max_depth,
                agent_max_threads=task.inner_agent_max_threads,
            )
            for provider in ordered_specs:
                provider_dir = task_dir / provider.provider_name.replace("/", "_")
                portable_home = _build_provider_home(root, provider)
                # Ensure the global codex auth has the provider's API key
                _prov_api_key = _extract_provider_api_key(portable_home)
                if _prov_api_key:
                    _ensure_codex_auth(_prov_api_key)
                _prov_base_url = _extract_provider_base_url(portable_home)
                env = _provider_env(
                    portable_home,
                    run_id=run_id,
                    task=task,
                    provider_allowlist=task.provider_allowlist or provider_allowlist,
                    provider_denylist=task.provider_denylist or provider_denylist,
                )
                command = build_codex_command(
                    codex_binary=codex_binary,
                    execution_root=execution_root,
                    last_message_path=provider_dir / "last_message.txt",
                    enable_multi_agent=bool(inner_codex["enable_multi_agent"]),
                    agent_max_depth=int(inner_codex["agent_max_depth"]),
                    agent_max_threads=inner_codex["agent_max_threads"],
                    dangerously_bypass=dangerously_bypass,
                    sandbox=sandbox,
                    ephemeral=ephemeral,
                    provider_base_url=_prov_base_url,
                )
                attempt = _run_attempt_blocking(
                    provider=provider,
                    command=command,
                    prompt=prompt,
                    env=env,
                    execution_root=execution_root,
                    stdout_path=provider_dir / "stdout.jsonl",
                    stderr_path=provider_dir / "stderr.log",
                    last_message_path=provider_dir / "last_message.txt",
                    timeout_seconds=task.timeout_seconds,
                )
                attempts.append(attempt)
                if attempt.ok:
                    break

        for attempt in attempts:
            _update_provider_state(state, attempt.provider, ok=attempt.ok, duration_seconds=attempt.duration_seconds)
        _advance_rotation_cursor(state, len(ordered_specs))

        selected_provider = next((item.provider for item in attempts if item.ok), None)
        result = MeshTaskResult(
            task_id=task.task_id,
            goal=task.goal,
            task_kind=task.task_kind,
            success=selected_provider is not None,
            selected_provider=selected_provider,
            output_mode=task.output_mode,
            provider_order=provider_names,
            attempts=attempts,
            execution_root=str(execution_root),
            workspace_kind=workspace_kind,
            depth=task.depth,
            parent_task_id=task.parent_task_id,
            lineage_id=lineage_id,
            started_at=task_started_at,
            finished_at=now_iso(),
            benchmark_label=task.benchmark_label,
        )
        _write_json(task_dir / "summary.json", asdict(result))
        return result
    finally:
        if ephemeral and execution_root is not None and workspace_kind in {"git_worktree", "copy"}:
            _cleanup_execution_root(root, execution_root, workspace_kind)


def _single_writer_conflict(left_scope: list[str], right_scope: list[str]) -> bool:
    """Check if two write scopes would both touch a SINGLE_WRITER_PATH."""
    for swp in SINGLE_WRITER_PATHS:
        left_touches = any(
            swp == _path_like(s) or swp.startswith(_path_like(s) + "/") or _path_like(s).startswith(swp + "/")
            for s in left_scope
        )
        right_touches = any(
            swp == _path_like(s) or swp.startswith(_path_like(s) + "/") or _path_like(s).startswith(swp + "/")
            for s in right_scope
        )
        if left_touches and right_touches:
            return True
    return False


def _task_conflicts_with_running(task: MeshTaskManifest, running: list[MeshTaskManifest]) -> bool:
    if not _task_requires_isolated_workspace(task.task_kind, task.write_scope):
        return False
    for other in running:
        if not _task_requires_isolated_workspace(other.task_kind, other.write_scope):
            continue
        if write_scopes_overlap(task.write_scope, other.write_scope):
            return True
        # enforce single-writer path exclusivity
        if _single_writer_conflict(task.write_scope, other.write_scope):
            return True
    return False


def execute_manifest(root: Path, manifest: MeshRunManifest) -> MeshRunSummary:
    run_id = run_id_now()
    output_dir = runs_root(root) / run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "manifest.json"
    _write_json(
        manifest_path,
        {
            "tasks": [asdict(task) for task in manifest.tasks],
            "execution_mode": manifest.execution_mode,
            "max_workers": manifest.max_workers,
            "benchmark_label": manifest.benchmark_label,
            "dangerously_bypass": manifest.dangerously_bypass,
            "sandbox": manifest.sandbox,
            "ephemeral": manifest.ephemeral,
            "provider_allowlist": manifest.provider_allowlist,
            "provider_denylist": manifest.provider_denylist,
        },
    )
    state = load_state(root)
    effective_provider_denylist = list(manifest.provider_denylist or DEFAULT_PROVIDER_DENYLIST)
    effective_provider_allowlist = resolve_provider_allowlist(
        root,
        allowlist=manifest.provider_allowlist,
        denylist=effective_provider_denylist,
        state=state,
    )
    started_at = now_iso()
    completed_results: list[MeshTaskResult] = []
    pending = list(manifest.tasks)
    running: dict[Any, MeshTaskManifest] = {}
    executors: list[ThreadPoolExecutor] = []

    def _new_executor() -> ThreadPoolExecutor:
        executor = ThreadPoolExecutor(max_workers=max(1, manifest.max_workers))
        executors.append(executor)
        return executor

    def _submit_task(executor: ThreadPoolExecutor, task: MeshTaskManifest) -> tuple[ThreadPoolExecutor, Any]:
        submit_kwargs = {
            "root": root,
            "run_id": run_id,
            "task": task,
            "state": state,
            "execution_mode": manifest.execution_mode,
            "dangerously_bypass": manifest.dangerously_bypass,
            "sandbox": manifest.sandbox,
            "ephemeral": manifest.ephemeral,
            "provider_allowlist": effective_provider_allowlist,
            "provider_denylist": effective_provider_denylist,
        }
        try:
            return executor, executor.submit(_execute_task, **submit_kwargs)
        except RuntimeError as exc:
            if "cannot schedule new futures after shutdown" not in str(exc).lower():
                raise
            logger.warning("mesh executor was already shutting down; recreating thread pool for remaining tasks")
            executor = _new_executor()
            return executor, executor.submit(_execute_task, **submit_kwargs)

    executor = _new_executor()
    try:
        while pending or running:
            running_tasks = list(running.values())
            launched = False
            for task in list(pending):
                if len(running) >= max(1, manifest.max_workers):
                    break
                if _task_conflicts_with_running(task, running_tasks):
                    continue
                executor, future = _submit_task(executor, task)
                running[future] = task
                pending.remove(task)
                running_tasks.append(task)
                launched = True
            if not running:
                if pending and not launched:
                    task = pending.pop(0)
                    executor, future = _submit_task(executor, task)
                    running[future] = task
                continue
            done, _ = wait(running.keys(), return_when=FIRST_COMPLETED)
            for future in done:
                task_for_future = running.pop(future, None)
                try:
                    completed_results.append(future.result())
                except Exception as _task_exc:
                    _failed = MeshTaskResult(
                        task_id=task_for_future.task_id if task_for_future else "unknown",
                        goal=task_for_future.goal if task_for_future else "",
                        task_kind=task_for_future.task_kind if task_for_future else "analysis",
                        success=False,
                        selected_provider=None,
                        output_mode=task_for_future.output_mode if task_for_future else "json",
                        provider_order=[],
                        attempts=[],
                        execution_root=str(root),
                        workspace_kind="shared",
                        depth=task_for_future.depth if task_for_future else 0,
                        parent_task_id=task_for_future.parent_task_id if task_for_future else None,
                        lineage_id=task_for_future.lineage_id or task_for_future.task_id if task_for_future else "unknown",
                        started_at=started_at,
                        finished_at=now_iso(),
                    )
                    completed_results.append(_failed)
    finally:
        for pool in executors:
            try:
                pool.shutdown(wait=True)
            except Exception:
                pass

    provider_specs = discover_providers(
        root,
        allowlist=effective_provider_allowlist,
        denylist=effective_provider_denylist,
    )
    provider_status = [provider_health(spec, state) for spec in provider_specs]
    state["latest_runs"] = ([run_id] + [item for item in state.get("latest_runs", []) if item != run_id])[:10]
    save_state(state, root)

    summary = MeshRunSummary(
        run_id=run_id,
        execution_mode=manifest.execution_mode,
        success=all(item.success for item in completed_results),
        task_count=len(completed_results),
        max_workers=max(1, manifest.max_workers),
        benchmark_label=manifest.benchmark_label,
        manifest_path=str(manifest_path),
        output_dir=str(output_dir),
        tasks=completed_results,
        provider_health=provider_status,
        started_at=started_at,
        finished_at=now_iso(),
    )
    _write_json(output_dir / "summary.json", asdict(summary))
    _write_json(latest_summary_path(root), asdict(summary))
    return summary


def build_single_task_manifest_from_args(args: argparse.Namespace) -> MeshRunManifest:
    if args.prompt_text and args.prompt_file:
        raise ValueError("--prompt-text and --prompt-file are mutually exclusive")
    prompt = args.prompt_text or (args.prompt_file.read_text(encoding="utf-8") if args.prompt_file else "")
    if not prompt.strip():
        raise ValueError("A prompt is required. Use --prompt-text, --prompt-file, or --manifest.")
    provider_denylist = list(args.disable_provider or DEFAULT_PROVIDER_DENYLIST)
    provider_allowlist = resolve_provider_allowlist(
        repo_root(),
        allowlist=list(args.provider or []),
        denylist=provider_denylist,
    )
    inherited_context = resolve_external_execution_context(args.max_external_depth)
    task = MeshTaskManifest(
        task_id=args.task_id,
        goal=args.goal,
        prompt=prompt,
        task_kind=args.task_kind,
        read_scope=list(args.read_scope or []),
        write_scope=list(args.write_scope or []),
        max_external_depth=inherited_context.max_external_depth,
        allow_native_subagents=not args.disable_native_subagents,
        allow_native_subagents_at_external_limit=args.allow_native_subagents_at_external_limit,
        inner_agent_max_depth=args.inner_agent_max_depth,
        inner_agent_max_threads=args.inner_agent_max_threads,
        provider_allowlist=provider_allowlist,
        provider_denylist=provider_denylist,
        timeout_seconds=args.timeout_seconds,
        benchmark_label=args.mesh_benchmark_label,
        output_mode=args.output_mode,
        working_root=str(args.working_root.resolve()) if args.working_root else None,
        parent_task_id=args.parent_task_id if args.parent_task_id is not None else inherited_context.parent_task_id,
        lineage_id=args.lineage_id if args.lineage_id is not None else inherited_context.lineage_id,
        depth=args.depth if args.depth is not None else inherited_context.depth,
        hedge_delay_seconds=args.hedge_delay_seconds,
    )
    return MeshRunManifest(
        tasks=[task],
        execution_mode=args.execution_mode,
        max_workers=args.mesh_max_workers,
        benchmark_label=args.mesh_benchmark_label,
        dangerously_bypass=not args.no_dangerously_bypass,
        sandbox=args.sandbox,
        ephemeral=not args.no_ephemeral,
        provider_allowlist=provider_allowlist,
        provider_denylist=provider_denylist,
    )


def load_manifest(path: Path) -> MeshRunManifest:
    payload = _load_json(path)
    if not payload:
        raise ValueError(f"Invalid manifest: {path}")
    tasks = [MeshTaskManifest(**item) for item in payload.get("tasks", [])]
    return MeshRunManifest(
        tasks=tasks,
        execution_mode=str(payload.get("execution_mode") or "mesh"),
        max_workers=int(payload.get("max_workers") or DEFAULT_MAX_WORKERS),
        benchmark_label=payload.get("benchmark_label"),
        dangerously_bypass=bool(payload.get("dangerously_bypass", True)),
        sandbox=str(payload.get("sandbox") or "danger-full-access"),
        ephemeral=bool(payload.get("ephemeral", True)),
        provider_allowlist=list(payload.get("provider_allowlist") or []),
        provider_denylist=list(payload.get("provider_denylist") or DEFAULT_PROVIDER_DENYLIST),
    )


def _metrics_from_runs(runs: list[MeshRunSummary]) -> dict[str, Any]:
    durations = [
        sum(attempt.duration_seconds for task in run.tasks for attempt in task.attempts)
        for run in runs
    ]
    successes = [1 if run.success else 0 for run in runs]
    median_duration = statistics.median(durations) if durations else None
    p95_duration = None
    if durations:
        ordered = sorted(durations)
        p95_index = max(0, round((len(ordered) - 1) * 0.95))
        p95_duration = ordered[p95_index]
    return {
        "iterations": len(runs),
        "success_rate": (sum(successes) / len(successes)) if successes else None,
        "median_wall_clock_seconds": median_duration,
        "p95_wall_clock_seconds": p95_duration,
        "durations": durations,
    }


def benchmark_manifest(
    root: Path,
    manifest: MeshRunManifest,
    *,
    iterations: int = DEFAULT_BENCH_ITERATIONS,
    suite: str = DEFAULT_BENCH_SUITE,
) -> dict[str, Any]:
    if any(task.task_kind not in {"analysis", "review"} for task in manifest.tasks):
        raise ValueError("Benchmark v1 only supports analysis/review tasks.")
    serial_runs: list[MeshRunSummary] = []
    mesh_runs: list[MeshRunSummary] = []
    for _ in range(iterations):
        serial_manifest = MeshRunManifest(
            tasks=manifest.tasks,
            execution_mode="serial",
            max_workers=1,
            benchmark_label=manifest.benchmark_label or suite,
            dangerously_bypass=manifest.dangerously_bypass,
            sandbox=manifest.sandbox,
            ephemeral=manifest.ephemeral,
            provider_allowlist=manifest.provider_allowlist,
            provider_denylist=manifest.provider_denylist,
        )
        mesh_manifest = MeshRunManifest(
            tasks=manifest.tasks,
            execution_mode="mesh",
            max_workers=manifest.max_workers,
            benchmark_label=manifest.benchmark_label or suite,
            dangerously_bypass=manifest.dangerously_bypass,
            sandbox=manifest.sandbox,
            ephemeral=manifest.ephemeral,
            provider_allowlist=manifest.provider_allowlist,
            provider_denylist=manifest.provider_denylist,
        )
        serial_runs.append(execute_manifest(root, serial_manifest))
        mesh_runs.append(execute_manifest(root, mesh_manifest))

    serial_metrics = _metrics_from_runs(serial_runs)
    mesh_metrics = _metrics_from_runs(mesh_runs)
    median_improvement = None
    p95_regression = None
    if serial_metrics["median_wall_clock_seconds"] and mesh_metrics["median_wall_clock_seconds"]:
        median_improvement = 1 - (
            mesh_metrics["median_wall_clock_seconds"] / serial_metrics["median_wall_clock_seconds"]
        )
    if serial_metrics["p95_wall_clock_seconds"] and mesh_metrics["p95_wall_clock_seconds"]:
        p95_regression = (
            (mesh_metrics["p95_wall_clock_seconds"] - serial_metrics["p95_wall_clock_seconds"])
            / serial_metrics["p95_wall_clock_seconds"]
        )
    accepted = (
        (mesh_metrics["success_rate"] or 0) >= DEFAULT_SUCCESS_RATE_THRESHOLD
        and (median_improvement or -1) >= DEFAULT_MEDIAN_IMPROVEMENT_THRESHOLD
        and (p95_regression or 0) <= DEFAULT_P95_REGRESSION_LIMIT
    )
    payload = {
        "suite": suite,
        "timestamp": now_iso(),
        "acceptance": {
            "success_rate_threshold": DEFAULT_SUCCESS_RATE_THRESHOLD,
            "median_improvement_threshold": DEFAULT_MEDIAN_IMPROVEMENT_THRESHOLD,
            "p95_regression_limit": DEFAULT_P95_REGRESSION_LIMIT,
            "accepted": accepted,
        },
        "serial": serial_metrics,
        "mesh": mesh_metrics,
        "median_improvement": median_improvement,
        "p95_regression": p95_regression,
    }
    bench_dir = benchmarks_root(root) / suite
    bench_dir.mkdir(parents=True, exist_ok=True)
    _write_json(bench_dir / f"{run_id_now()}.json", payload)
    return payload


def status_payload(root: Path) -> dict[str, Any]:
    state = load_state(root)
    specs = discover_providers(root)
    return {
        "state_path": str(state_path(root)),
        "latest_summary_path": str(latest_summary_path(root)),
        "latest_summary": _load_json(latest_summary_path(root)),
        "providers": [provider_health(spec, state) for spec in specs],
        "latest_runs": list(state.get("latest_runs", [])),
    }


def replay_run(root: Path, run_id: str) -> MeshRunSummary:
    manifest = load_manifest(runs_root(root) / run_id / "manifest.json")
    return execute_manifest(root, manifest)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Codex Mesh router for external relay-backed Codex workers.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="execute a mesh task or manifest once")
    run_parser.add_argument("--manifest", type=Path)
    run_parser.add_argument("--goal", default="ad-hoc mesh task")
    run_parser.add_argument("--task-id", default="task-001")
    run_parser.add_argument("--task-kind", choices=["analysis", "review", "write", "mixed"], default="analysis")
    run_parser.add_argument("--prompt-text")
    run_parser.add_argument("--prompt-file", type=Path)
    run_parser.add_argument("--read-scope", nargs="*")
    run_parser.add_argument("--write-scope", nargs="*")
    run_parser.add_argument("--provider", action="append")
    run_parser.add_argument("--disable-provider", action="append")
    run_parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    run_parser.add_argument("--max-external-depth", type=int, default=DEFAULT_MAX_EXTERNAL_DEPTH)
    run_parser.add_argument("--mesh-max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    run_parser.add_argument("--mesh-benchmark-label", default=None)
    run_parser.add_argument("--output-mode", choices=["text", "json", "patch"], default="text")
    run_parser.add_argument("--execution-mode", choices=["serial", "mesh"], default="mesh")
    run_parser.add_argument("--working-root", type=Path, default=repo_root())
    run_parser.add_argument("--parent-task-id", default=None)
    run_parser.add_argument("--lineage-id", default=None)
    run_parser.add_argument("--depth", type=int, default=None)
    run_parser.add_argument("--hedge-delay-seconds", type=int, default=DEFAULT_HEDGE_DELAY_SECONDS)
    run_parser.add_argument("--disable-native-subagents", action="store_true")
    run_parser.add_argument(
        "--disallow-native-subagents-at-external-limit",
        dest="allow_native_subagents_at_external_limit",
        action="store_false",
        default=DEFAULT_ALLOW_NATIVE_SUBAGENTS_AT_EXTERNAL_LIMIT,
    )
    run_parser.add_argument("--inner-agent-max-depth", type=int, default=DEFAULT_INNER_AGENT_MAX_DEPTH)
    run_parser.add_argument("--inner-agent-max-threads", type=int, default=DEFAULT_INNER_AGENT_MAX_THREADS)
    run_parser.add_argument("--sandbox", default="danger-full-access")
    run_parser.add_argument("--no-dangerously-bypass", action="store_true")
    run_parser.add_argument("--no-ephemeral", action="store_true")
    run_parser.add_argument("--json", action="store_true")

    bench_parser = subparsers.add_parser("bench", help="compare serial failover and mesh runs")
    bench_parser.add_argument("--manifest", type=Path)
    bench_parser.add_argument("--goal", default="analysis benchmark")
    bench_parser.add_argument("--task-id", default="bench-task")
    bench_parser.add_argument("--prompt-text")
    bench_parser.add_argument("--prompt-file", type=Path)
    bench_parser.add_argument("--read-scope", nargs="*")
    bench_parser.add_argument("--provider", action="append")
    bench_parser.add_argument("--disable-provider", action="append")
    bench_parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    bench_parser.add_argument("--iterations", type=int, default=DEFAULT_BENCH_ITERATIONS)
    bench_parser.add_argument("--suite", default=DEFAULT_BENCH_SUITE)
    bench_parser.add_argument("--mesh-max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    bench_parser.add_argument("--mesh-benchmark-label", default=None)
    bench_parser.add_argument("--working-root", type=Path, default=repo_root())
    bench_parser.add_argument("--json", action="store_true")

    status_parser = subparsers.add_parser("status", help="show provider health and recent mesh runs")
    status_parser.add_argument("--json", action="store_true")

    replay_parser = subparsers.add_parser("replay", help="re-run a previous manifest by run id")
    replay_parser.add_argument("--run-id", required=True)
    replay_parser.add_argument("--json", action="store_true")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = repo_root()

    if args.command == "run":
        manifest = load_manifest(args.manifest) if args.manifest else build_single_task_manifest_from_args(args)
        summary = execute_manifest(root, manifest)
        payload = asdict(summary)
        print(json.dumps(payload, ensure_ascii=False, indent=2) if args.json else json.dumps(payload, ensure_ascii=False))
        return 0 if summary.success else 1

    if args.command == "bench":
        if args.manifest:
            manifest = load_manifest(args.manifest)
        else:
            bench_args = argparse.Namespace(
                goal=args.goal,
                task_id=args.task_id,
                task_kind="analysis",
                prompt_text=args.prompt_text,
                prompt_file=args.prompt_file,
                read_scope=args.read_scope,
                write_scope=[],
                provider=args.provider,
                disable_provider=args.disable_provider,
                timeout_seconds=args.timeout_seconds,
                max_external_depth=DEFAULT_MAX_EXTERNAL_DEPTH,
                mesh_max_workers=args.mesh_max_workers,
                mesh_benchmark_label=args.mesh_benchmark_label,
                output_mode="text",
                execution_mode="mesh",
                working_root=args.working_root,
                parent_task_id=None,
                lineage_id=None,
                depth=None,
                hedge_delay_seconds=DEFAULT_HEDGE_DELAY_SECONDS,
                disable_native_subagents=False,
                allow_native_subagents_at_external_limit=DEFAULT_ALLOW_NATIVE_SUBAGENTS_AT_EXTERNAL_LIMIT,
                inner_agent_max_depth=DEFAULT_INNER_AGENT_MAX_DEPTH,
                inner_agent_max_threads=DEFAULT_INNER_AGENT_MAX_THREADS,
                sandbox="danger-full-access",
                no_dangerously_bypass=False,
                no_ephemeral=False,
            )
            manifest = build_single_task_manifest_from_args(bench_args)
        payload = benchmark_manifest(root, manifest, iterations=args.iterations, suite=args.suite)
        print(json.dumps(payload, ensure_ascii=False, indent=2) if args.json else json.dumps(payload, ensure_ascii=False))
        return 0 if payload["acceptance"]["accepted"] else 1

    if args.command == "status":
        payload = status_payload(root)
        print(json.dumps(payload, ensure_ascii=False, indent=2) if args.json else json.dumps(payload, ensure_ascii=False))
        return 0

    if args.command == "replay":
        summary = replay_run(root, args.run_id)
        payload = asdict(summary)
        print(json.dumps(payload, ensure_ascii=False, indent=2) if args.json else json.dumps(payload, ensure_ascii=False))
        return 0 if summary.success else 1

    raise AssertionError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

