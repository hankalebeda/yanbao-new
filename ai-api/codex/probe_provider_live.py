from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.11+ ships tomllib
    import tomli as tomllib  # type: ignore[no-redef]


DEFAULT_PROMPT = "Reply with exactly: LIVE_OK"
DEFAULT_TIMEOUT = 40.0
ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent.parent


@dataclass(frozen=True)
class ProviderSpec:
    provider_dir: Path
    provider_name: str
    base_url: str
    api_key: str
    model: str
    review_model: str | None
    enabled: bool

    @property
    def models_url(self) -> str:
        return self.base_url.rstrip("/") + "/models"

    @property
    def responses_url(self) -> str:
        return self.base_url.rstrip("/") + "/responses"

    @property
    def masked_key(self) -> str:
        if len(self.api_key) <= 10:
            return "***"
        return f"{self.api_key[:6]}...{self.api_key[-4:]}"


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


def _load_api_key(provider_dir: Path) -> str:
    key_path = provider_dir / "key.txt"
    if key_path.exists():
        lines = [line.strip() for line in key_path.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
        if lines:
            if len(lines) >= 2 and lines[0].startswith(("http://", "https://")):
                return lines[1]
            return lines[0]

    auth = _load_json(provider_dir / "auth.json")
    return str(
        auth.get("OPENAI_API_KEY") or auth.get("api_key") or auth.get("apiKey") or ""
    ).strip()


def _resolve_provider_dir(raw: str) -> Path:
    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate
    direct = (ROOT / candidate).resolve()
    if direct.exists():
        return direct
    return candidate.resolve()


def _extract_text(payload: dict[str, Any]) -> str:
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


def _extract_models(payload: dict[str, Any]) -> list[str]:
    result: list[str] = []
    for item in payload.get("data") or []:
        model_id = item.get("id")
        if isinstance(model_id, str) and model_id:
            result.append(model_id)
    return result


def load_provider_spec(provider_dir: Path, requested_model: str | None = None) -> ProviderSpec:
    metadata = _load_json(provider_dir / "provider.json")
    config = _load_toml(provider_dir / "config.toml")
    provider_cfg = config.get("model_providers", {}).get("OpenAI", {})

    base_url = str(provider_cfg.get("base_url") or metadata.get("endpoint") or "").strip().rstrip("/")
    api_key = _load_api_key(provider_dir)
    model = str(requested_model or config.get("model") or metadata.get("model") or "").strip()
    review_model = str(config.get("review_model") or "").strip() or None
    enabled = metadata.get("enabled") is not False
    provider_name = str(metadata.get("name") or provider_dir.name).strip() or provider_dir.name

    if not base_url:
        raise ValueError(f"Missing endpoint/base_url in {provider_dir}")
    if not api_key:
        raise ValueError(f"Missing OPENAI_API_KEY in {provider_dir}")
    if not model:
        raise ValueError(f"Missing model in {provider_dir}")

    return ProviderSpec(
        provider_dir=provider_dir,
        provider_name=provider_name,
        base_url=base_url,
        api_key=api_key,
        model=model,
        review_model=review_model,
        enabled=enabled,
    )


def _headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def probe_models(spec: ProviderSpec, timeout: float) -> dict[str, Any]:
    response = httpx.get(spec.models_url, headers=_headers(spec.api_key), timeout=timeout)
    summary: dict[str, Any] = {
        "step": "models_probe",
        "provider_name": spec.provider_name,
        "models_url": spec.models_url,
        "status_code": response.status_code,
    }
    if response.status_code != 200:
        summary["ok"] = False
        summary["error"] = response.text[:500]
        return summary

    payload = response.json()
    models = _extract_models(payload)
    summary["ok"] = True
    summary["model_count"] = len(models)
    summary["contains_requested_model"] = spec.model in models
    summary["sample_models"] = models[:10]
    return summary


def probe_responses(spec: ProviderSpec, prompt: str, timeout: float) -> dict[str, Any]:
    payload = {
        "model": spec.model,
        "input": prompt,
        "max_output_tokens": 32,
        "store": False,
    }
    response = httpx.post(
        spec.responses_url,
        headers=_headers(spec.api_key),
        json=payload,
        timeout=timeout,
    )
    summary: dict[str, Any] = {
        "step": "responses_probe",
        "provider_name": spec.provider_name,
        "responses_url": spec.responses_url,
        "status_code": response.status_code,
    }
    if response.status_code != 200:
        summary["ok"] = False
        summary["error"] = response.text[:1000]
        return summary

    body = response.json()
    output_text = _extract_text(body)
    summary["ok"] = True
    summary["resolved_model"] = body.get("model")
    summary["response_id"] = body.get("id")
    summary["status"] = body.get("status")
    summary["output_text"] = output_text
    summary["matches_expected"] = output_text.strip() == "LIVE_OK"
    return summary


def probe_discovery(spec: ProviderSpec) -> dict[str, Any]:
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    from app.services.codex_client import discover_codex_provider_specs

    discovered = discover_codex_provider_specs(ROOT)
    names = [item.provider_name for item in discovered]
    summary: dict[str, Any] = {
        "step": "discovery_probe",
        "provider_name": spec.provider_name,
        "discovered_names": names,
        "ok": spec.provider_name in names,
    }
    return summary


def probe_codex_exec(spec: ProviderSpec, prompt: str, timeout: float) -> dict[str, Any]:
    if spec.provider_dir.parent.resolve() != ROOT.resolve():
        return {
            "step": "codex_exec_probe",
            "provider_name": spec.provider_name,
            "ok": False,
            "error": "provider dir is not under ai-api/codex, cannot use run_codex_provider.ps1",
        }

    command = [
        "powershell",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(ROOT / "run_codex_provider.ps1"),
        "-ProviderDir",
        spec.provider_dir.name,
        "exec",
        "--skip-git-repo-check",
        prompt,
    ]
    completed = subprocess.run(
        command,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=timeout,
        encoding="utf-8",
        errors="replace",
    )
    stdout = completed.stdout.strip()
    stderr = completed.stderr.strip()
    ok = completed.returncode == 0 and "LIVE_OK" in stdout
    return {
        "step": "codex_exec_probe",
        "provider_name": spec.provider_name,
        "ok": ok,
        "returncode": completed.returncode,
        "stdout_tail": stdout[-1000:],
        "stderr_tail": stderr[-1000:],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe one ai-api/codex/<provider>/ directory for live usability"
    )
    parser.add_argument("provider", help="Provider directory name or absolute path")
    parser.add_argument("--model", default=None, help="Override model from config.toml")
    parser.add_argument("--prompt", default=DEFAULT_PROMPT, help="Prompt used for live checks")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="HTTP timeout in seconds")
    parser.add_argument(
        "--skip-codex-exec",
        action="store_true",
        help="Skip the final codex exec smoke test",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    provider_dir = _resolve_provider_dir(args.provider)
    spec = load_provider_spec(provider_dir, requested_model=args.model)

    print(
        json.dumps(
            {
                "step": "probe_start",
                "provider_dir": str(provider_dir),
                "provider_name": spec.provider_name,
                "base_url": spec.base_url,
                "model": spec.model,
                "review_model": spec.review_model,
                "enabled": spec.enabled,
                "api_key": spec.masked_key,
            },
            ensure_ascii=False,
        )
    )

    if not spec.enabled:
        print(
            json.dumps(
                {
                    "step": "config_gate",
                    "provider_name": spec.provider_name,
                    "ok": False,
                    "error": "provider.json enabled=false, app discovery will skip this provider",
                },
                ensure_ascii=False,
            )
        )
        return 1

    checks: list[dict[str, Any]] = []
    checks.append(probe_discovery(spec))
    checks.append(probe_models(spec, args.timeout))
    checks.append(probe_responses(spec, args.prompt, args.timeout))
    if not args.skip_codex_exec:
        checks.append(probe_codex_exec(spec, args.prompt, max(args.timeout, 120.0)))

    overall_ok = all(bool(check.get("ok")) for check in checks)
    for check in checks:
        print(json.dumps(check, ensure_ascii=False))

    print(
        json.dumps(
            {
                "step": "probe_done",
                "provider_name": spec.provider_name,
                "result": "PROVIDER_USABLE" if overall_ok else "PROVIDER_NOT_USABLE",
            },
            ensure_ascii=False,
        )
    )
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
