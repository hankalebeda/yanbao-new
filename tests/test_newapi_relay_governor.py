from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "newapi_relay_governor.py"
    spec = importlib.util.spec_from_file_location("newapi_relay_governor_under_test", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


governor = _load_module()


def test_candidate_models_follow_truth_file(tmp_path: Path):
    truth_path = tmp_path / "truth.json"
    truth_path.write_text(
        json.dumps(
            {
                "candidate_models": ["gpt-5.4", "gpt-5.3-codex"],
                "managed_sources": ["bus.042999.xyz", "marybrown.dpdns.org"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    assert governor._candidate_models([], truth_path) == ["gpt-5.4", "gpt-5.3-codex"]


def test_sanitize_output_removes_full_key():
    payload = {
        "token": {"id": 7, "name": "codex-relay-xhigh", "group": "codex-readonly", "full_key": "sk-secret"}
    }

    sanitized = governor._sanitize_output_payload(payload)

    assert sanitized["token"] == {"id": 7, "name": "codex-relay-xhigh", "group": "codex-readonly"}
    assert payload["token"]["full_key"] == "sk-secret"


def test_build_step_plans_contains_provision_and_govern(tmp_path: Path):
    repo_root = tmp_path
    truth_file = repo_root / "ai-api" / "codex" / "newapi_live_truth.json"
    registry_file = repo_root / "ai-api" / "codex" / "newapi_channel_registry.json"
    truth_file.parent.mkdir(parents=True, exist_ok=True)
    truth_file.write_text("{}", encoding="utf-8")
    registry_file.write_text("{}", encoding="utf-8")

    plans = governor.build_step_plans(
        repo_root=repo_root,
        base_url="http://192.168.232.141:3000",
        username="naadmin",
        password="secret",
        providers_root=repo_root / "ai-api" / "codex",
        truth_file=truth_file,
        registry_file=registry_file,
        token_name="codex-relay-xhigh",
        gateway_provider_name="newapi-192.168.232.141-3000",
        gateway_provider_shards="ro-a,ro-b",
        reasoning_effort="xhigh",
        candidate_models=["gpt-5.4", "gpt-5.3-codex"],
        provision_output=repo_root / "output" / "provision.json",
        govern_output=repo_root / "output" / "govern.json",
        allow_token_fork=False,
        skip_provision=False,
        skip_govern=False,
        lease_ttl_seconds=1800,
        source_probe_workers=4,
        candidate_probe_workers=4,
        provision_retries=1,
        govern_retries=2,
        retry_delay_seconds=5,
    )

    assert [plan.name for plan in plans] == ["provision", "govern"]
    assert "--provision-gateway-only" in plans[0].command
    assert "--write-sharded-gateway-provider-dirs" in plans[0].command
    assert "--mode" in plans[1].command
    assert "govern" in plans[1].command
    assert plans[0].command.count("--candidate-model") == 2
    assert plans[1].command.count("--candidate-model") == 2
    assert "--lease-ttl-seconds" in plans[1].command
    assert "--source-probe-workers" in plans[1].command
    assert "--candidate-probe-workers" in plans[1].command
    assert plans[0].retries == 1
    assert plans[1].retries == 2


def test_run_step_retries_until_success(monkeypatch, tmp_path: Path):
    calls: list[int] = []
    sleeps: list[int] = []

    class Completed:
        def __init__(self, returncode: int, stdout: str, stderr: str):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def _fake_run(*args, **kwargs):
        calls.append(len(calls) + 1)
        if len(calls) == 1:
            return Completed(1, "first failure", "boom")
        return Completed(0, "second success", "")

    monkeypatch.setattr(governor.subprocess, "run", _fake_run)
    monkeypatch.setattr(governor.time, "sleep", lambda seconds: sleeps.append(seconds))

    result = governor.run_step(
        governor.StepPlan(
            name="govern",
            command=["python", "govern.py"],
            output_path=tmp_path / "govern.json",
            retries=1,
            retry_delay_seconds=3,
        ),
        cwd=tmp_path,
        password="secret",
    )

    assert result["returncode"] == 0
    assert result["attempt_count"] == 2
    assert result["retried"] is True
    assert [attempt["returncode"] for attempt in result["attempts"]] == [1, 0]
    assert sleeps == [3]


def test_main_writes_sanitized_summary(monkeypatch, tmp_path: Path):
    repo_root = tmp_path
    codex_root = repo_root / "ai-api" / "codex"
    output_root = repo_root / "output"
    codex_root.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True, exist_ok=True)
    (codex_root / "newapi_live_truth.json").write_text(
        json.dumps(
            {"candidate_models": ["gpt-5.4", "gpt-5.3-codex"], "managed_sources": ["bus.042999.xyz"]},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (codex_root / "newapi_channel_registry.json").write_text(
        json.dumps({"defaults": {"auto_disable": True, "auto_enable": False, "allow_delete": False}}),
        encoding="utf-8",
    )

    def _fake_run_step(step, *, cwd, password):
        if step.name == "provision":
            step.output_path.write_text(
                json.dumps(
                    {
                        "token": {
                            "id": 7,
                            "name": "codex-relay-xhigh",
                            "group": "codex-readonly",
                            "full_key": "sk-secret",
                        },
                        "gateway_provider": {"provider_dir": "D:/yanbao/ai-api/codex/newapi-192.168.232.141-3000"},
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        else:
            step.output_path.write_text(
                json.dumps(
                    {
                        "mode": "govern",
                        "inventory": {"probed": 2, "healthy": 2, "summary": {"by_state": {"active": 2}}},
                        "reconcile": {"activated_channels": [{"id": 1}], "quarantined_channels": []},
                        "summary": {"by_state": {"active": 2, "quarantine": 0, "retired": 0}},
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        return {"name": step.name, "returncode": 0, "command": ["python", step.name], "stdout_tail": "", "stderr_tail": ""}

    monkeypatch.setattr(governor, "run_step", _fake_run_step)

    rc = governor.main(
        [
            "--repo-root",
            str(repo_root),
            "--password",
            "secret",
            "--summary-out",
            str(output_root / "summary.json"),
            "--provision-out",
            str(output_root / "provision.json"),
            "--govern-out",
            str(output_root / "govern.json"),
        ]
    )

    assert rc == 0
    summary = json.loads((output_root / "summary.json").read_text(encoding="utf-8"))
    provision = json.loads((output_root / "provision.json").read_text(encoding="utf-8"))
    assert summary["success"] is True
    assert summary["steps"][0]["output_summary"]["token"] == {
        "id": 7,
        "name": "codex-relay-xhigh",
        "group": "codex-readonly",
    }
    assert "full_key" not in provision["token"]