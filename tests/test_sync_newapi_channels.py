from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
from pathlib import Path
import sys


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "ai-api" / "codex" / "sync_newapi_channels.py"
    spec = importlib.util.spec_from_file_location("sync_newapi_channels_under_test", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sync_newapi_channels = _load_module()


def _provider_source(*, model_mapping=None):
    return sync_newapi_channels.ProviderSource(
        name="alpha",
        base_url="https://alpha.example/v1",
        probe_base_urls=["https://alpha.example/v1"],
        channel_base_url="https://alpha.example",
        api_key="sk-alpha",
        upstream_model="gpt-5.4",
        exposed_model="gpt-5.4",
        model_mapping=model_mapping,
        enabled=True,
    )


def test_write_gateway_provider_dir_creates_openai_compatible_provider(tmp_path: Path):
    provider_dir = sync_newapi_channels.write_gateway_provider_dir(
        providers_root=tmp_path,
        base_url="http://192.168.232.141:3000",
        token_key="tok-test",
        provider_name="newapi-192.168.232.141-3000",
    )

    assert provider_dir == tmp_path / "newapi-192.168.232.141-3000"
    provider_payload = json.loads((provider_dir / "provider.json").read_text(encoding="utf-8"))
    auth_payload = json.loads((provider_dir / "auth.json").read_text(encoding="utf-8"))
    config_text = (provider_dir / "config.toml").read_text(encoding="utf-8")
    key_lines = (provider_dir / "key.txt").read_text(encoding="utf-8").splitlines()

    assert provider_payload["endpoint"] == "http://192.168.232.141:3000/v1"
    assert provider_payload["model"] == "gpt-5.4"
    assert provider_payload["review_model"] == "gpt-5.3-codex"
    assert provider_payload["fallback_models"] == ["gpt-5.4", "gpt-5.3-codex", "gpt-5.2"]
    assert auth_payload["OPENAI_API_KEY"] == "tok-test"
    assert 'base_url = "http://192.168.232.141:3000/v1"' in config_text
    assert 'review_model = "gpt-5.3-codex"' in config_text
    assert 'fallback_models = ["gpt-5.4", "gpt-5.3-codex", "gpt-5.2"]' in config_text
    assert "multi_agent = true" in config_text
    assert key_lines == ["http://192.168.232.141:3000/v1", "tok-test"]


def test_gateway_naming_helpers_preserve_backward_compatibility():
    assert (
        sync_newapi_channels.gateway_provider_home_name("newapi-192.168.232.141-3000")
        == "newapi-192.168.232.141-3000"
    )
    assert sync_newapi_channels.gateway_token_name("codex-relay-xhigh") == "codex-relay-xhigh"


def test_gateway_naming_helpers_append_suffixes():
    assert (
        sync_newapi_channels.gateway_provider_home_name("newapi-192.168.232.141-3000", suffix="stable")
        == "newapi-192.168.232.141-3000-stable"
    )
    assert (
        sync_newapi_channels.gateway_provider_home_name("newapi-192.168.232.141-3000", suffix="ro-a")
        == "newapi-192.168.232.141-3000-ro-a"
    )
    assert sync_newapi_channels.gateway_token_name("codex-relay-xhigh", suffix="stable") == "codex-relay-xhigh-stable"
    assert sync_newapi_channels.gateway_token_name("codex-relay-xhigh", suffix="ro-b") == "codex-relay-xhigh-ro-b"


def test_write_sharded_gateway_provider_dirs_writes_distinct_homes(tmp_path: Path):
    written = sync_newapi_channels.write_sharded_gateway_provider_dirs(
        providers_root=tmp_path,
        base_url="http://192.168.232.141:3000",
        base_provider_name="newapi-192.168.232.141-3000",
        token_keys_by_suffix={
            "stable": "tok-stable",
            "ro-a": "tok-a",
        },
        review_model="gpt-5.3-codex",
    )
    assert set(written.keys()) == {"stable", "ro-a"}
    assert written["stable"] == tmp_path / "newapi-192.168.232.141-3000-stable"
    assert written["ro-a"] == tmp_path / "newapi-192.168.232.141-3000-ro-a"

    stable_key = (written["stable"] / "key.txt").read_text(encoding="utf-8").splitlines()
    shard_key = (written["ro-a"] / "key.txt").read_text(encoding="utf-8").splitlines()
    assert stable_key == ["http://192.168.232.141:3000/v1", "tok-stable"]
    assert shard_key == ["http://192.168.232.141:3000/v1", "tok-a"]


def test_create_token_uses_lane_specific_group_payload():
    class FakeResponse:
        def __init__(self, payload):
            self.content = b"1"
            self._payload = payload

        def json(self):
            return self._payload

    client = sync_newapi_channels.NewAPIClient.__new__(sync_newapi_channels.NewAPIClient)
    tokens: list[dict[str, object]] = []
    requests: list[tuple[str, str, dict[str, object]]] = []

    def _list_tokens():
        return list(tokens)

    def _request(method: str, path: str, **kwargs):
        requests.append((method, path, kwargs))
        if method == "POST" and path == "/api/token/":
            payload = dict(kwargs["json"])
            tokens.append({"id": 41, "name": payload["name"], "group": payload["group"]})
            return FakeResponse({"success": True})
        if method == "POST" and path == "/api/token/41/key":
            return FakeResponse({"data": {"key": "sk-ro-a"}})
        raise AssertionError(f"unexpected request: {method} {path}")

    client.list_tokens = _list_tokens  # type: ignore[method-assign]
    client._request = _request  # type: ignore[method-assign]

    token = client.create_token("codex-relay-xhigh-ro-a", lane=sync_newapi_channels.LANE_RO_A)

    assert requests[0][2]["json"]["group"] == "codex-ro-a"
    assert token["group"] == "codex-ro-a"
    assert token["full_key"] == "sk-ro-a"


def test_ensure_runtime_groups_updates_user_usable_groups_and_group_ratio():
    client = sync_newapi_channels.NewAPIClient.__new__(sync_newapi_channels.NewAPIClient)
    updates: list[tuple[str, dict[str, object]]] = []

    client.list_options = lambda: [  # type: ignore[method-assign]
        {"key": "GroupRatio", "value": '{"default": 1, "vip": 1}'},
        {"key": "UserUsableGroups", "value": '{"default": "Default", "vip": "VIP"}'},
    ]

    def _update_option(key: str, value: str):
        updates.append((key, json.loads(value)))
        return {"success": True}

    client.update_option = _update_option  # type: ignore[method-assign]

    summary = client.ensure_runtime_groups(  # type: ignore[attr-defined]
        [sync_newapi_channels.LANE_READONLY, sync_newapi_channels.LANE_STABLE],
        descriptions=sync_newapi_channels.RUNTIME_GROUP_DESCRIPTIONS,
    )

    assert summary["group_ratio_added"] == ["codex-readonly", "codex-stable"]
    assert summary["user_usable_groups_added"] == ["codex-readonly", "codex-stable"]
    assert updates == [
        (
            "GroupRatio",
            {
                "default": 1,
                "vip": 1,
                "codex-readonly": 1,
                "codex-stable": 1,
            },
        ),
        (
            "UserUsableGroups",
            {
                "default": "Default",
                "vip": "VIP",
                "codex-readonly": "Codex readonly",
                "codex-stable": "Codex stable",
            },
        ),
    ]


def test_build_channel_layout_exposes_only_explicit_failover_models_for_review_channel():
    layout = sync_newapi_channels.build_channel_layout(
        _provider_source(),
        selected_model="gpt-5.3-codex",
        candidate_models=list(sync_newapi_channels.DEFAULT_CANDIDATE_MODELS),
    )

    assert layout.channel_models == ["gpt-5.3-codex", "gpt-5.2"]
    assert layout.test_model == "gpt-5.3-codex"
    assert layout.priority == 20
    assert layout.model_mapping is None


def test_build_channel_layout_exposes_only_final_backup_model_for_fallback_channel():
    layout = sync_newapi_channels.build_channel_layout(
        _provider_source(),
        selected_model="gpt-5.2",
        candidate_models=list(sync_newapi_channels.DEFAULT_CANDIDATE_MODELS),
    )

    assert layout.channel_models == ["gpt-5.2"]
    assert layout.test_model == "gpt-5.2"
    assert layout.priority == 30
    assert layout.model_mapping is None


def test_build_channel_layout_preserves_existing_model_mapping_for_primary_model():
    layout = sync_newapi_channels.build_channel_layout(
        _provider_source(model_mapping={"gpt-5.4": "gpt-5.4-fast"}),
        selected_model="gpt-5.4",
        candidate_models=list(sync_newapi_channels.DEFAULT_CANDIDATE_MODELS),
    )

    assert layout.channel_models == ["gpt-5.4", "gpt-5.3-codex", "gpt-5.2"]
    assert layout.test_model == "gpt-5.4"
    assert layout.priority == 10
    assert layout.model_mapping == {"gpt-5.4": "gpt-5.4-fast"}


def test_choose_gateway_review_model_prefers_healthy_codex_tier():
    results = [
        sync_newapi_channels.ChannelResult(
            name="alpha",
            channel_id=1,
            create_ok=True,
            test_ok=True,
            message="",
            channel_test_ok=True,
            selected_model="gpt-5.4",
            channel_models=["gpt-5.4", "gpt-5.3-codex", "gpt-5.2"],
        )
    ]

    assert sync_newapi_channels._choose_gateway_review_model(results) == "gpt-5.3-codex"


def test_choose_gateway_review_model_falls_back_to_gpt52_when_codex_tier_is_unhealthy():
    results = [
        sync_newapi_channels.ChannelResult(
            name="alpha",
            channel_id=1,
            create_ok=True,
            test_ok=True,
            message="",
            channel_test_ok=True,
            selected_model="gpt-5.2",
            channel_models=["gpt-5.2"],
        )
    ]

    assert sync_newapi_channels._choose_gateway_review_model(results) == "gpt-5.2"


def test_choose_gateway_review_model_defaults_to_gpt52_when_no_active_backup_is_healthy():
    assert sync_newapi_channels._choose_gateway_review_model([]) == "gpt-5.2"


def test_parse_args_preserves_existing_channels_by_default(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        ["sync_newapi_channels.py", "--base-url", "http://127.0.0.1:3000", "--username", "u", "--password", "p"],
    )
    args = sync_newapi_channels.parse_args()
    assert args.replace_existing is False
    assert args.include_root_key_sources is False


def test_parse_args_allows_explicit_replace_existing(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sync_newapi_channels.py",
            "--base-url",
            "http://127.0.0.1:3000",
            "--username",
            "u",
            "--password",
            "p",
            "--replace-existing",
        ],
    )
    args = sync_newapi_channels.parse_args()
    assert args.replace_existing is True


def test_load_provider_sources_ignores_root_key_entries_by_default(tmp_path: Path):
    provider_dir = tmp_path / "alpha"
    provider_dir.mkdir()
    (provider_dir / "provider.json").write_text(
        json.dumps({"name": "alpha", "endpoint": "https://alpha.example/v1", "model": "gpt-5.4", "enabled": True}),
        encoding="utf-8",
    )
    (provider_dir / "config.toml").write_text(
        '\n'.join(
            [
                'model = "gpt-5.4"',
                '',
                '[model_providers.OpenAI]',
                'base_url = "https://alpha.example/v1"',
                '',
            ]
        ),
        encoding="utf-8",
    )
    (provider_dir / "auth.json").write_text(json.dumps({"OPENAI_API_KEY": "sk-alpha"}), encoding="utf-8")
    (tmp_path / "key.txt").write_text("https://legacy.example/v1\nsk-legacy\n", encoding="utf-8")

    sources, skipped = sync_newapi_channels.load_provider_sources(tmp_path, set())

    assert [source.name for source in sources] == ["alpha"]
    assert skipped == []


def test_detect_token_truth_drift_flags_noncanonical_codex_tokens():
    drift = sync_newapi_channels._detect_token_truth_drift(
        [
            {"id": 2, "name": "codex-relay-xhigh", "status": 1},
            {"id": 3, "name": "codex-relay-xhigh-ro-a", "status": 1},
            {"id": 1, "name": "codex-gpt54-xhigh", "status": 1, "group": "default"},
            {"id": 9, "name": "unrelated", "status": 1},
        ],
        "codex-relay-xhigh",
    )

    assert drift == [
        {
            "id": 1,
            "name": "codex-gpt54-xhigh",
            "group": "default",
            "accessed_time": None,
            "created_time": None,
        }
    ]


def test_sync_channels_skips_duplicate_create_when_channel_exists(monkeypatch):
    source = _provider_source()

    class FakeClient:
        base_url = "http://localhost:3000"

        def __init__(self):
            self.created = []
            self.deleted = []
            self.tested = []

        def find_channels_by_name(self, name: str):
            return [{"id": 11, "name": name}, {"id": 12, "name": name}]

        def delete_channel(self, channel_id: int):
            self.deleted.append(channel_id)

        def test_channel(self, channel_id: int, model_name: str):
            self.tested.append((channel_id, model_name))
            return {"success": True, "message": "", "time": 0.25}

        def create_channel(self, **kwargs):
            self.created.append(kwargs)
            return {"id": 99}

    monkeypatch.setattr(
        sync_newapi_channels,
        "probe_upstream_responses",
        lambda source, *, candidate_models, reasoning_effort, timeout=60.0: (
            True,
            "",
            "gpt-5.3-codex",
            "https://alpha.example/v1",
        ),
    )

    client = FakeClient()
    results = sync_newapi_channels.sync_channels(
        client,
        [source],
        replace_existing=False,
        candidate_models=list(sync_newapi_channels.DEFAULT_CANDIDATE_MODELS),
        reasoning_effort="xhigh",
    )

    assert client.created == []
    assert client.deleted == []
    assert client.tested == [(11, "gpt-5.3-codex")]
    assert len(results) == 1
    assert results[0].channel_id == 11
    assert results[0].channel_models == ["gpt-5.3-codex", "gpt-5.2"]
    assert results[0].channel_test_model == "gpt-5.3-codex"
    assert results[0].channel_priority == 20
    assert results[0].model_mapping is None
    assert "skipped creation" in results[0].message
    assert "found 2 existing channels" in results[0].message


def test_sync_channels_replaces_all_duplicate_channels_before_create(monkeypatch):
    source = _provider_source()

    class FakeClient:
        base_url = "http://localhost:3000"

        def __init__(self):
            self.created = []
            self.deleted = []
            self.tested = []

        def find_channels_by_name(self, name: str):
            return [{"id": 11, "name": name}, {"id": 12, "name": name}]

        def delete_channel(self, channel_id: int):
            self.deleted.append(channel_id)

        def test_channel(self, channel_id: int, model_name: str):
            self.tested.append((channel_id, model_name))
            return {"success": True, "message": "", "time": 0.25}

        def create_channel(self, source, *, channel_models, test_model, priority, model_mapping):
            self.created.append(
                {
                    "name": source.name,
                    "channel_models": channel_models,
                    "test_model": test_model,
                    "priority": priority,
                    "model_mapping": model_mapping,
                }
            )
            return {"id": 99}

    monkeypatch.setattr(
        sync_newapi_channels,
        "probe_upstream_responses",
        lambda source, *, candidate_models, reasoning_effort, timeout=60.0: (
            True,
            "",
            "gpt-5.2",
            "https://alpha.example/v1",
        ),
    )

    client = FakeClient()
    results = sync_newapi_channels.sync_channels(
        client,
        [source],
        replace_existing=True,
        candidate_models=list(sync_newapi_channels.DEFAULT_CANDIDATE_MODELS),
        reasoning_effort="xhigh",
    )

    assert client.deleted == [11, 12]
    assert client.created == [
        {
            "name": "alpha",
            "channel_models": ["gpt-5.2"],
            "test_model": "gpt-5.2",
            "priority": 30,
            "model_mapping": None,
        }
    ]
    assert client.tested == [(99, "gpt-5.2")]
    assert len(results) == 1
    assert results[0].channel_id == 99
    assert results[0].channel_models == ["gpt-5.2"]
    assert results[0].channel_test_model == "gpt-5.2"
    assert results[0].channel_priority == 30
    assert results[0].model_mapping is None


def test_inventory_sources_reuses_single_channel_snapshot(monkeypatch):
    source = _provider_source()

    class FakeClient:
        base_url = "http://localhost:3000"

        def __init__(self):
            self.list_calls = 0
            self.tested = []

        def _list_channels(self):
            self.list_calls += 1
            return [{"id": 11, "name": source.name}]

        def test_channel(self, channel_id: int, model_name: str):
            self.tested.append((channel_id, model_name))
            return {"success": True, "message": "", "time": 0.2}

    monkeypatch.setattr(
        sync_newapi_channels,
        "probe_upstream_responses",
        lambda source, *, candidate_models, reasoning_effort, timeout=60.0: (
            True,
            "",
            "gpt-5.4",
            "https://alpha.example/v1",
        ),
    )

    client = FakeClient()
    results = sync_newapi_channels.inventory_sources(
        client,
        [source],
        candidate_models=list(sync_newapi_channels.DEFAULT_CANDIDATE_MODELS),
        reasoning_effort="xhigh",
    )

    assert client.list_calls == 1
    assert client.tested == [(11, "gpt-5.4")]
    assert len(results) == 1
    assert results[0].channel_id == 11


def test_main_writes_sharded_gateway_summary_without_base_provider_dir(monkeypatch, tmp_path: Path):
    class FakeClient:
        def __init__(self):
            self.closed = False
            self.tokens: dict[str, dict[str, object]] = {}
            self.runtime_group_calls: list[list[str]] = []

        def ensure_setup(self):
            return {"success": True}

        def login(self):
            return {"success": True, "data": {"id": 7}}

        def list_tokens(self):
            return list(self.tokens.values())

        def ensure_runtime_groups(self, groups: list[str], *, descriptions=None):
            self.runtime_group_calls.append(list(groups))
            return {
                "groups": list(groups),
                "group_ratio_added": list(groups),
                "user_usable_groups_added": list(groups),
                "group_ratio_updated": True,
                "user_usable_groups_updated": True,
            }

        def _list_channels(self):
            return []

        def create_token(self, name: str, *, lane: str = sync_newapi_channels.DEFAULT_LANE, key_store_path=None):
            assert not self.closed, "client closed before sharded provisioning finished"
            group = sync_newapi_channels.LANE_GROUPS.get(lane, lane)
            token = self.tokens.setdefault(
                name,
                {
                    "id": len(self.tokens) + 1,
                    "name": name,
                    "group": group,
                },
            )
            token["full_key"] = f"sk-{name}"
            return dict(token)

        def close(self):
            self.closed = True

    fake_client = FakeClient()
    providers_root = tmp_path / "providers"
    summary_path = tmp_path / "summary.json"

    monkeypatch.setattr(
        sync_newapi_channels,
        "parse_args",
        lambda: argparse.Namespace(
            base_url="http://192.168.232.141:3000",
            username="admin",
            password="secret",
            providers_root=str(providers_root),
            exclude=[],
            token_name="codex-relay-xhigh",
            candidate_model=None,
            reasoning_effort="xhigh",
            log_page_size=100,
            out=str(summary_path),
            disable_unmanaged_candidates=False,
            include_root_key_sources=False,
            replace_existing=False,
            no_replace_existing=False,
            write_gateway_provider_dir=False,
            write_sharded_gateway_provider_dirs=True,
            provision_gateway_only=True,
            gateway_provider_shards="ro-a,ro-b,ro-c,ro-d",
            gateway_provider_name="newapi-192.168.232.141-3000",
            truth_file=None,
            registry_file=None,
            managed_source=[],
            allow_token_fork=False,
            materialize_lane_channels=False,
            archive_unmanaged=False,
            freeze_lane_clones=False,
            lock_file=str(tmp_path / ".sync.lock"),
        ),
    )
    monkeypatch.setattr(sync_newapi_channels, "_sync_run_lock", lambda path: contextlib.nullcontext())
    monkeypatch.setattr(sync_newapi_channels, "_load_live_truth", lambda path: {})
    monkeypatch.setattr(sync_newapi_channels, "_load_channel_registry", lambda path: {})
    monkeypatch.setattr(sync_newapi_channels, "load_provider_sources", lambda *args, **kwargs: ([], []))
    monkeypatch.setattr(sync_newapi_channels, "NewAPIClient", lambda *args, **kwargs: fake_client)
    monkeypatch.setattr(sync_newapi_channels, "sync_channels", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("sync should be skipped")))
    monkeypatch.setattr(
        sync_newapi_channels,
        "reconcile_channel_pool",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("reconcile should be skipped")),
    )

    rc = sync_newapi_channels.main()

    assert rc == 0
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert "gateway_provider" not in summary
    assert summary["provision_gateway_only"] is True
    assert summary["runtime_group_sync"]["groups"] == [
        "codex-readonly",
        "codex-stable",
        "codex-ro-a",
        "codex-ro-b",
        "codex-ro-c",
        "codex-ro-d",
    ]
    assert summary["gateway_provider_shards"]["suffixes"] == ["stable", "ro-a", "ro-b", "ro-c", "ro-d"]
    assert summary["gateway_provider_shards"]["tokens"] == [
        {"id": 2, "name": "codex-relay-xhigh-stable", "group": "codex-stable"},
        {"id": 3, "name": "codex-relay-xhigh-ro-a", "group": "codex-ro-a"},
        {"id": 4, "name": "codex-relay-xhigh-ro-b", "group": "codex-ro-b"},
        {"id": 5, "name": "codex-relay-xhigh-ro-c", "group": "codex-ro-c"},
        {"id": 6, "name": "codex-relay-xhigh-ro-d", "group": "codex-ro-d"},
    ]
    assert (providers_root / "newapi-192.168.232.141-3000-stable").exists()
    assert (providers_root / "newapi-192.168.232.141-3000-ro-d").exists()
    assert fake_client.runtime_group_calls == [[
        "codex-readonly",
        "codex-stable",
        "codex-ro-a",
        "codex-ro-b",
        "codex-ro-c",
        "codex-ro-d",
    ]]
    assert fake_client.closed is True


def test_registry_errors_accept_phase1_default_registry():
    registry = sync_newapi_channels.default_channel_registry()
    assert sync_newapi_channels._registry_errors(registry) == []


def test_build_manual_channel_registry_excludes_managed_and_lane_channels():
    source = _provider_source()
    channels = [
        {"name": "alpha", "base_url": "https://alpha.example", "models": "gpt-5.4"},
        {"name": "alpha__lane__ro-a", "base_url": "https://alpha.example", "models": "gpt-5.4"},
        {"name": "manual.example", "base_url": "https://manual.example", "models": "gpt-5.4"},
    ]

    registry = sync_newapi_channels.build_manual_channel_registry(
        channels,
        sources=[source],
        candidate_models=list(sync_newapi_channels.DEFAULT_CANDIDATE_MODELS),
    )

    assert registry["lane_policy"]["materialize_live_clones"] is False
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


def test_list_channels_paginates_until_all_pages_are_collected():
    client = sync_newapi_channels.NewAPIClient.__new__(sync_newapi_channels.NewAPIClient)

    class FakeResponse:
        def __init__(self, payload):
            self.content = b"1"
            self._payload = payload

        def json(self):
            return self._payload

    seen_paths: list[str] = []

    def _request(method: str, path: str, **kwargs):
        seen_paths.append(path)
        if "p=1" in path:
            return FakeResponse({"data": {"items": [{"id": 1}, {"id": 2}]}})
        if "p=2" in path:
            return FakeResponse({"data": {"items": [{"id": 3}]}})
        return FakeResponse({"data": {"items": []}})

    client._request = _request  # type: ignore[method-assign]
    client._json = sync_newapi_channels.NewAPIClient._json  # type: ignore[method-assign]

    items = client._list_channels(page_size=2)  # type: ignore[attr-defined]

    assert [item["id"] for item in items] == [1, 2, 3]
    assert seen_paths == [
        "/api/channel/?p=1&page_size=2",
        "/api/channel/?p=2&page_size=2",
    ]


def test_reconcile_channel_pool_archives_manual_unmanaged_and_freezes_lane():
    source = _provider_source()
    registry = {
        "defaults": {"auto_disable": True, "auto_enable": False, "allow_delete": False},
        "lane_policy": {"materialize_live_clones": False},
        "manual_channels": [
            {
                "name": "manual.example",
                "base_url": "https://manual.example",
                "preserve": True,
                "auto_disable": True,
                "auto_enable": False,
                "allow_delete": False,
                "source": "manual",
                "notes": "",
            }
        ],
    }

    class FakeClient:
        def __init__(self):
            self.channels = [
                {"id": 1, "name": "alpha", "base_url": "https://alpha.example", "models": "gpt-5.4", "status": 1, "weight": 1, "priority": 10, "tag": "", "remark": ""},
                {"id": 2, "name": "manual.example", "base_url": "https://manual.example", "models": "gpt-5.4", "status": 1, "weight": 100, "priority": 10, "tag": "", "remark": ""},
                {"id": 3, "name": "alpha__lane__ro-a", "base_url": "https://alpha.example", "models": "gpt-5.4", "status": 1, "weight": 100, "priority": 10, "tag": "", "remark": ""},
                {"id": 4, "name": "drift.example", "base_url": "https://drift.example", "models": "gpt-5.4", "status": 2, "weight": 100, "priority": 10, "tag": "", "remark": ""},
            ]

        def _list_channels(self, page_size: int = 200):
            return [dict(channel) for channel in self.channels]

        def list_logs(self, *, page_size: int = 100, page: int = 1):
            return []

        def update_channel(self, channel, **kwargs):
            updated = dict(channel)
            updated.update({
                "weight": kwargs.get("weight", updated.get("weight")),
                "status": kwargs.get("status", updated.get("status")),
                "tag": kwargs.get("tag", updated.get("tag")),
                "remark": kwargs.get("remark", updated.get("remark")),
                "name": kwargs.get("name", updated.get("name")),
                "group": kwargs.get("lane", updated.get("group")),
                "models": ",".join(kwargs["channel_models"]) if kwargs.get("channel_models") else updated.get("models"),
            })
            self.channels = [updated if item["id"] == updated["id"] else item for item in self.channels]
            return updated

    client = FakeClient()
    results = [
        sync_newapi_channels.ChannelResult(
            name="alpha",
            channel_id=1,
            create_ok=True,
            test_ok=True,
            message="",
            upstream_probe_ok=True,
            channel_test_ok=True,
            selected_model="gpt-5.4",
            channel_models=["gpt-5.4", "gpt-5.3-codex", "gpt-5.2"],
            channel_test_model="gpt-5.4",
            channel_priority=10,
        )
    ]

    summary = sync_newapi_channels.reconcile_channel_pool(
        client,
        sources=[source],
        results=results,
        registry=registry,
        candidate_models=list(sync_newapi_channels.DEFAULT_CANDIDATE_MODELS),
        log_page_size=0,
        archive_unmanaged=True,
        freeze_lane_clones=True,
    )

    assert summary["activated_channels"][0]["id"] == 1
    assert summary["archived_channels"][0]["name"] == "manual.example"
    assert summary["archived_channels"][1]["name"] == "drift.example"
    assert summary["frozen_channels"][0]["name"] == "alpha__lane__ro-a"
    metrics = sync_newapi_channels.summarize_governance_metrics(summary["post_reconcile_channels"])
    assert metrics == {
        "managed_active_count": 1,
        "managed_quarantine_count": 0,
        "manual_archive_count": 1,
        "unmanaged_drift_count": 1,
        "lane_frozen_count": 1,
        "invalid_state_combo_count": 0,
        "active_outside_truth_count": 0,
    }
