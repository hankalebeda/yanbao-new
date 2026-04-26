from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import types


def _load_sync_module():
    module_path = Path(__file__).resolve().parents[1] / "ai-api" / "codex" / "sync_newapi_channels.py"
    spec = importlib.util.spec_from_file_location("sync_newapi_channels_for_governor_tests", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_governance_module():
    module_path = Path(__file__).resolve().parents[1] / "ai-api" / "codex" / "newapi_governance.py"
    spec = importlib.util.spec_from_file_location("governor_testpkg.newapi_governance", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_governor_module(sync_mod, gov_mod):
    compat = types.ModuleType("ai_api_codex_compat")
    compat.sync_newapi_channels = sync_mod
    sys.modules["ai_api_codex_compat"] = compat

    package_name = "governor_testpkg"
    package = types.ModuleType(package_name)
    package.__path__ = []  # type: ignore[attr-defined]
    sys.modules[package_name] = package
    sys.modules[f"{package_name}.newapi_governance"] = gov_mod

    module_path = Path(__file__).resolve().parents[1] / "ai-api" / "codex" / "channel_governor.py"
    spec = importlib.util.spec_from_file_location(f"{package_name}.channel_governor", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    module.__package__ = package_name
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sync_mod = _load_sync_module()
gov_mod = _load_governance_module()
channel_governor = _load_governor_module(sync_mod, gov_mod)


def test_run_inventory_is_read_only(monkeypatch, tmp_path):
    monkeypatch.setattr(gov_mod, "GOVERNANCE_DIR", tmp_path / ".governance")
    monkeypatch.setattr(gov_mod, "GOVERNANCE_STATE_PATH", tmp_path / ".governance" / "channel_state.json")
    monkeypatch.setattr(gov_mod, "LEASE_PATH", tmp_path / ".governance" / "governor_lease.json")

    result_row = sync_mod.ChannelResult(
        name="alpha",
        channel_id=11,
        create_ok=False,
        test_ok=True,
        message="inventory only",
        upstream_probe_ok=True,
        upstream_probe_model="gpt-5.4",
        channel_test_ok=True,
        channel_test_message=None,
    )

    monkeypatch.setattr(channel_governor.sync_mod, "inventory_sources", lambda *args, **kwargs: [result_row])
    monkeypatch.setattr(channel_governor.sync_mod, "sync_channels", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("sync_channels must not be called in inventory")))

    payload = channel_governor.run_inventory(
        client=object(),  # type: ignore[arg-type]
        sources=[],
        candidate_models=list(sync_mod.DEFAULT_CANDIDATE_MODELS),
        reasoning_effort="xhigh",
    )

    assert payload["mode"] == "inventory"
    assert payload["healthy"] == 1
    assert payload["summary"]["by_state"]["active"] == 1
    state = gov_mod.load_governance_state()
    assert state["channels"]["alpha"]["inventory_class"] == "managed"
    assert state["channels"]["alpha"]["allow_auto_create"] is True


def test_main_govern_fail_closed_on_missing_registry(monkeypatch, tmp_path):
    summary_path = tmp_path / "result.json"

    class FakeClient:
        def ensure_setup(self):
            return {"success": True}

        def login(self):
            return {"success": True, "data": {"id": 1}}

        def close(self):
            return None

    monkeypatch.setattr(
        channel_governor,
        "parse_args",
        lambda: types.SimpleNamespace(
            mode="govern",
            base_url="http://localhost:3000",
            username="admin",
            password="secret",
            candidate_model=[],
            reasoning_effort="xhigh",
            providers_root=str(tmp_path),
            truth_file=str(tmp_path / "truth.json"),
            registry_file=str(tmp_path / "missing_registry.json"),
            out=str(summary_path),
            log_page_size=10,
            disable_unmanaged_candidates=False,
            archive_unmanaged=False,
            freeze_lane_clones=False,
            include_root_key_sources=False,
            managed_source=[],
        ),
    )
    monkeypatch.setattr(channel_governor, "_load_inputs", lambda args: {
        "truth_path": Path(args.truth_file),
        "registry_path": Path(args.registry_file),
        "truth": {"managed_sources": ["alpha"], "candidate_models": ["gpt-5.4"], "token_name": "tok"},
        "registry": {},
        "truth_errors": [],
        "registry_errors": ["registry file missing or empty"],
        "managed_sources": {"alpha"},
        "sources": [],
        "skipped": [],
        "archive_unmanaged": False,
        "freeze_lane_clones": True,
    })
    monkeypatch.setattr(channel_governor.sync_mod, "NewAPIClient", lambda *args, **kwargs: FakeClient())
    monkeypatch.setattr(channel_governor, "run_govern", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("run_govern must not be called when registry is invalid")))

    rc = channel_governor.main()

    assert rc == 2
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["blocked_errors"] == ["registry file missing or empty"]
    assert payload["mode"] == "govern"


def test_generate_registry_writes_manual_channels(monkeypatch, tmp_path):
    registry_path = tmp_path / "newapi_channel_registry.json"

    class FakeClient:
        def ensure_setup(self):
            return {"success": True}

        def login(self):
            return {"success": True, "data": {"id": 1}}

        def _list_channels(self):
            return [
                {"name": "alpha", "base_url": "https://alpha.example", "models": "gpt-5.4"},
                {"name": "manual.example", "base_url": "https://manual.example", "models": "gpt-5.4"},
                {"name": "alpha__lane__ro-a", "base_url": "https://alpha.example", "models": "gpt-5.4"},
            ]

        def close(self):
            return None

    monkeypatch.setattr(
        channel_governor,
        "parse_args",
        lambda: types.SimpleNamespace(
            mode="generate-registry",
            base_url="http://localhost:3000",
            username="admin",
            password="secret",
            candidate_model=[],
            reasoning_effort="xhigh",
            providers_root=str(tmp_path),
            truth_file=str(tmp_path / "truth.json"),
            registry_file=str(registry_path),
            out="",
            log_page_size=10,
            disable_unmanaged_candidates=False,
            archive_unmanaged=False,
            freeze_lane_clones=False,
            include_root_key_sources=False,
            managed_source=[],
        ),
    )
    monkeypatch.setattr(channel_governor, "_load_inputs", lambda args: {
        "truth_path": Path(args.truth_file),
        "registry_path": registry_path,
        "truth": {"managed_sources": ["alpha"], "candidate_models": ["gpt-5.4"], "token_name": "tok"},
        "registry": sync_mod.default_channel_registry(),
        "truth_errors": [],
        "registry_errors": [],
        "managed_sources": {"alpha"},
        "sources": [
            sync_mod.ProviderSource(
                name="alpha",
                base_url="https://alpha.example/v1",
                probe_base_urls=["https://alpha.example/v1"],
                channel_base_url="https://alpha.example",
                api_key="sk-alpha",
                upstream_model="gpt-5.4",
                exposed_model="gpt-5.4",
                model_mapping=None,
                enabled=True,
            )
        ],
        "skipped": [],
        "archive_unmanaged": False,
        "freeze_lane_clones": True,
    })
    monkeypatch.setattr(channel_governor.sync_mod, "NewAPIClient", lambda *args, **kwargs: FakeClient())

    rc = channel_governor.main()

    assert rc == 0
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    assert registry["manual_channels"] == [
        {
            "name": "manual.example",
            "base_url": "https://manual.example",
            "preserve": True,
            "auto_disable": True,
            "auto_enable": False,
            "allow_delete": False,
            "source": "live_import",
            "notes": "imported from current live channel inventory (manual.example)",
        }
    ]
