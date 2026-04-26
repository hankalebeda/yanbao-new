"""Tests for New API channel governance, three-state machine, dual-track lane
isolation, governor lease/fencing, and deployment-layer changes.

Covers:
- Phase 1: auto_ban=0, lane-aware group/token, soft/hard error classification
- Phase 2: governance state machine (active/quarantine/retired), lease/fencing
- Phase 3: mesh.py readonly lane support, runner.py shard routing
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Module loaders (ai-api/codex is not a normal package)
# ---------------------------------------------------------------------------

def _load_sync_module():
    module_path = Path(__file__).resolve().parents[1] / "ai-api" / "codex" / "sync_newapi_channels.py"
    spec = importlib.util.spec_from_file_location("sync_newapi_channels_test", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_governance_module():
    module_path = Path(__file__).resolve().parents[1] / "ai-api" / "codex" / "newapi_governance.py"
    spec = importlib.util.spec_from_file_location("newapi_governance_test", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sync_mod = _load_sync_module()
gov_mod = _load_governance_module()


# ---------------------------------------------------------------------------
# Phase 1: Error classification and auto_ban
# ---------------------------------------------------------------------------

class TestErrorClassification:
    """Phase 1.4: BLOCKING_LOG_SIGNATURES split into hard/soft."""

    def test_hard_signatures_are_subset_of_all(self):
        assert sync_mod.HARD_BLOCKING_LOG_SIGNATURES < sync_mod.BLOCKING_LOG_SIGNATURES

    def test_soft_signatures_are_subset_of_all(self):
        assert sync_mod.SOFT_BLOCKING_LOG_SIGNATURES < sync_mod.BLOCKING_LOG_SIGNATURES

    def test_hard_and_soft_are_disjoint(self):
        overlap = sync_mod.HARD_BLOCKING_LOG_SIGNATURES & sync_mod.SOFT_BLOCKING_LOG_SIGNATURES
        assert not overlap, f"hard/soft overlap: {overlap}"

    def test_hard_union_soft_equals_all(self):
        assert (
            sync_mod.HARD_BLOCKING_LOG_SIGNATURES | sync_mod.SOFT_BLOCKING_LOG_SIGNATURES
            == sync_mod.BLOCKING_LOG_SIGNATURES
        )

    def test_429_is_soft_not_hard(self):
        assert "status_code=429" in sync_mod.SOFT_BLOCKING_LOG_SIGNATURES
        assert "status_code=429" not in sync_mod.HARD_BLOCKING_LOG_SIGNATURES

    def test_503_is_soft_not_hard(self):
        assert "status_code=503" in sync_mod.SOFT_BLOCKING_LOG_SIGNATURES
        assert "status_code=503" not in sync_mod.HARD_BLOCKING_LOG_SIGNATURES

    def test_auth_unavailable_is_hard(self):
        assert "auth_unavailable" in sync_mod.HARD_BLOCKING_LOG_SIGNATURES

    def test_log_block_severity_hard(self):
        assert sync_mod._log_block_severity("auth_unavailable") == "hard"
        assert sync_mod._log_block_severity("bad_response_body") == "hard"

    def test_log_block_severity_soft(self):
        assert sync_mod._log_block_severity("status_code=429") == "soft"
        assert sync_mod._log_block_severity("status_code=503") == "soft"
        assert sync_mod._log_block_severity("too many requests") == "soft"


class TestLaneConstants:
    """Phase 1.1: Lane/group constants are properly defined."""

    def test_lane_groups_defined(self):
        assert sync_mod.LANE_STABLE == "codex-stable"
        assert sync_mod.LANE_READONLY == "codex-readonly"
        assert sync_mod.LANE_RO_A == "codex-ro-a"
        assert sync_mod.LANE_RO_B == "codex-ro-b"
        assert sync_mod.LANE_RO_C == "codex-ro-c"
        assert sync_mod.LANE_RO_D == "codex-ro-d"
        assert sync_mod.READONLY_SHARDS == ["ro-a", "ro-b", "ro-c", "ro-d"]
        assert sync_mod.DEFAULT_LANE == sync_mod.LANE_READONLY

    def test_lane_groups_mapping(self):
        assert sync_mod.LANE_GROUPS[sync_mod.LANE_STABLE] == "codex-stable"
        assert sync_mod.LANE_GROUPS[sync_mod.LANE_READONLY] == "codex-readonly"
        assert sync_mod.LANE_GROUPS[sync_mod.LANE_RO_A] == "codex-ro-a"
        assert sync_mod.LANE_GROUPS[sync_mod.LANE_RO_B] == "codex-ro-b"
        assert sync_mod.LANE_GROUPS[sync_mod.LANE_RO_C] == "codex-ro-c"
        assert sync_mod.LANE_GROUPS[sync_mod.LANE_RO_D] == "codex-ro-d"

    def test_governance_lane_constants_track_sharded_groups(self):
        assert gov_mod.LANE_STABLE == "codex-stable"
        assert gov_mod.LANE_READONLY == "codex-readonly"
        assert gov_mod.VALID_LANES >= {
            "codex-stable",
            "codex-readonly",
            "codex-ro-a",
            "codex-ro-b",
            "codex-ro-c",
            "codex-ro-d",
        }
        assert gov_mod.lane_for_shard("ro-a") == "codex-ro-a"
        assert gov_mod.lane_for_shard("ro-d") == "codex-ro-d"


# ---------------------------------------------------------------------------
# Phase 2: Governance state machine
# ---------------------------------------------------------------------------

class TestGovernanceStateMachine:
    """Phase 2: Three-state machine (active/quarantine/retired)."""

    @pytest.fixture(autouse=True)
    def _setup_tmp_governance(self, tmp_path, monkeypatch):
        monkeypatch.setattr(gov_mod, "GOVERNANCE_DIR", tmp_path / ".governance")
        monkeypatch.setattr(gov_mod, "GOVERNANCE_STATE_PATH", tmp_path / ".governance" / "channel_state.json")
        monkeypatch.setattr(gov_mod, "LEASE_PATH", tmp_path / ".governance" / "governor_lease.json")

    def test_initial_state_is_empty(self):
        state = gov_mod.load_governance_state()
        assert state["channels"] == {}
        assert state["next_fencing_token"] == 1

    def test_default_channel_entry_assigns_deterministic_shard_lane(self):
        entry = gov_mod.default_channel_entry("wududu.edu.kg")
        assert entry.shard in {"ro-a", "ro-b", "ro-c", "ro-d"}
        assert entry.lane == gov_mod.lane_for_shard(entry.shard)

    def test_probe_ok_transitions_to_active(self):
        state = gov_mod.load_governance_state()
        entry = gov_mod.transition_channel(state, "test.example.com", event="probe_ok", channel_id=42)
        assert entry.desired_state == "active"
        assert entry.consecutive_failures == 0
        assert entry.channel_id == 42

    def test_soft_error_transitions_to_quarantine(self):
        state = gov_mod.load_governance_state()
        entry = gov_mod.transition_channel(
            state, "test.example.com",
            event="probe_fail",
            error_text="status_429",
        )
        assert entry.desired_state == "quarantine"
        assert entry.consecutive_failures == 1
        assert entry.cooldown_until > time.time()

    def test_hard_error_transitions_to_retired(self):
        state = gov_mod.load_governance_state()
        entry = gov_mod.transition_channel(
            state, "test.example.com",
            event="probe_fail",
            error_text="model_not_found",
        )
        assert entry.desired_state == "retired"
        assert entry.consecutive_failures == 1

    def test_consecutive_failures_increase_cooldown(self):
        state = gov_mod.load_governance_state()
        # First failure
        entry1 = gov_mod.transition_channel(
            state, "test.example.com",
            event="probe_fail", error_text="timeout",
        )
        cd1 = entry1.cooldown_until
        # Second failure
        entry2 = gov_mod.transition_channel(
            state, "test.example.com",
            event="probe_fail", error_text="timeout",
        )
        cd2 = entry2.cooldown_until
        assert cd2 > cd1
        assert entry2.consecutive_failures == 2

    def test_probe_ok_resets_quarantine(self):
        state = gov_mod.load_governance_state()
        gov_mod.transition_channel(state, "ch1", event="probe_fail", error_text="timeout")
        entry = gov_mod.transition_channel(state, "ch1", event="probe_ok")
        assert entry.desired_state == "active"
        assert entry.consecutive_failures == 0
        assert entry.cooldown_until == 0.0

    def test_manual_retire_sets_hold(self):
        state = gov_mod.load_governance_state()
        entry = gov_mod.transition_channel(state, "ch1", event="manual_retire")
        assert entry.desired_state == "retired"
        assert entry.manual_hold is True

    def test_manual_activate_clears_hold(self):
        state = gov_mod.load_governance_state()
        gov_mod.transition_channel(state, "ch1", event="manual_retire")
        entry = gov_mod.transition_channel(state, "ch1", event="manual_activate")
        assert entry.desired_state == "active"
        assert entry.manual_hold is False

    def test_channels_due_for_reprobe(self):
        state = gov_mod.load_governance_state()
        # Create a quarantined channel with expired cooldown
        gov_mod.transition_channel(state, "ch1", event="probe_fail", error_text="timeout")
        # Force cooldown to past
        state["channels"]["ch1"]["cooldown_until"] = time.time() - 60
        due = gov_mod.channels_due_for_reprobe(state)
        assert len(due) == 1
        assert due[0].channel_identity == "ch1"

    def test_channels_due_for_reprobe_can_filter_lane(self):
        state = gov_mod.load_governance_state()
        gov_mod.transition_channel(state, "ch1", event="probe_fail", error_text="timeout")
        gov_mod.transition_channel(state, "ch2", event="probe_fail", error_text="timeout")
        state["channels"]["ch1"]["cooldown_until"] = time.time() - 60
        state["channels"]["ch1"]["lane"] = gov_mod.lane_for_shard("ro-a")
        state["channels"]["ch1"]["shard"] = "ro-a"
        state["channels"]["ch2"]["cooldown_until"] = time.time() - 60
        state["channels"]["ch2"]["lane"] = gov_mod.lane_for_shard("ro-b")
        state["channels"]["ch2"]["shard"] = "ro-b"

        due = gov_mod.channels_due_for_reprobe(state, lane=gov_mod.lane_for_shard("ro-a"))

        assert [entry.channel_identity for entry in due] == ["ch1"]

    def test_channels_not_due_if_cooldown_active(self):
        state = gov_mod.load_governance_state()
        gov_mod.transition_channel(state, "ch1", event="probe_fail", error_text="timeout")
        # Cooldown is in the future (set by transition_channel)
        due = gov_mod.channels_due_for_reprobe(state)
        assert len(due) == 0

    def test_retired_channels_never_due_for_reprobe(self):
        state = gov_mod.load_governance_state()
        gov_mod.transition_channel(state, "ch1", event="probe_fail", error_text="model_not_found")
        due = gov_mod.channels_due_for_reprobe(state)
        assert len(due) == 0

    def test_state_persistence(self):
        state = gov_mod.load_governance_state()
        gov_mod.transition_channel(state, "ch1", event="probe_ok", channel_id=10)
        gov_mod.save_governance_state(state)
        reloaded = gov_mod.load_governance_state()
        assert "ch1" in reloaded["channels"]
        assert reloaded["channels"]["ch1"]["channel_id"] == 10


class TestGovernorLease:
    """Phase 2: Governor lease/fencing for exclusive mutation."""

    @pytest.fixture(autouse=True)
    def _setup_tmp_governance(self, tmp_path, monkeypatch):
        monkeypatch.setattr(gov_mod, "GOVERNANCE_DIR", tmp_path / ".governance")
        monkeypatch.setattr(gov_mod, "GOVERNANCE_STATE_PATH", tmp_path / ".governance" / "channel_state.json")
        monkeypatch.setattr(gov_mod, "LEASE_PATH", tmp_path / ".governance" / "governor_lease.json")

    def test_acquire_new_lease(self):
        lease = gov_mod.acquire_lease("holder-1")
        assert lease.holder_id == "holder-1"
        assert lease.fencing_token == 1
        assert not lease.is_expired()

    def test_same_holder_extends_lease(self):
        lease1 = gov_mod.acquire_lease("holder-1", ttl_seconds=60)
        lease2 = gov_mod.acquire_lease("holder-1", ttl_seconds=120)
        assert lease2.fencing_token == lease1.fencing_token
        assert lease2.expires_at > lease1.expires_at

    def test_renew_lease_extends_active_session(self):
        lease1 = gov_mod.acquire_lease("holder-1", ttl_seconds=60)
        previous_expiry = lease1.expires_at

        lease2 = gov_mod.renew_lease("holder-1", ttl_seconds=120, fencing_token=lease1.fencing_token)

        assert lease2.fencing_token == lease1.fencing_token
        assert lease2.expires_at > previous_expiry

    def test_renew_lease_rejects_wrong_fencing_token(self):
        lease = gov_mod.acquire_lease("holder-1", ttl_seconds=60)

        with pytest.raises(RuntimeError, match="Fencing token mismatch"):
            gov_mod.renew_lease("holder-1", ttl_seconds=120, fencing_token=lease.fencing_token + 1)

    def test_different_holder_rejected_while_active(self):
        gov_mod.acquire_lease("holder-1", ttl_seconds=600)
        with pytest.raises(RuntimeError, match="Governor lease held by holder-1"):
            gov_mod.acquire_lease("holder-2")

    def test_expired_lease_can_be_taken(self):
        lease1 = gov_mod.acquire_lease("holder-1", ttl_seconds=1)
        # Force expiry
        lease1_data = gov_mod._load_lease()
        lease1_data.expires_at = time.time() - 10
        gov_mod._save_lease(lease1_data)
        lease2 = gov_mod.acquire_lease("holder-2")
        assert lease2.holder_id == "holder-2"
        assert lease2.fencing_token == 2

    def test_release_lease(self):
        gov_mod.acquire_lease("holder-1")
        gov_mod.release_lease("holder-1")
        # Now another holder can acquire
        lease2 = gov_mod.acquire_lease("holder-2")
        assert lease2.holder_id == "holder-2"

    def test_fencing_token_increments(self):
        lease1 = gov_mod.acquire_lease("holder-1")
        gov_mod.release_lease("holder-1")
        lease2 = gov_mod.acquire_lease("holder-2")
        assert lease2.fencing_token == lease1.fencing_token + 1

    def test_validate_fencing_passes(self):
        lease = gov_mod.acquire_lease("holder-1")
        gov_mod.validate_fencing(lease.fencing_token)  # should not raise

    def test_validate_fencing_fails_on_mismatch(self):
        gov_mod.acquire_lease("holder-1")
        with pytest.raises(RuntimeError, match="Fencing token mismatch"):
            gov_mod.validate_fencing(9999)

    def test_governor_session_context_manager(self):
        with gov_mod.governor_session("holder-1") as lease:
            assert lease.holder_id == "holder-1"
            gov_mod.validate_fencing(lease.fencing_token)
        # After exit, lease should be released
        lease2 = gov_mod.acquire_lease("holder-2")
        assert lease2.holder_id == "holder-2"

    def test_only_one_governor_at_a_time(self):
        with gov_mod.governor_session("holder-1"):
            with pytest.raises(RuntimeError, match="Governor lease held"):
                gov_mod.acquire_lease("holder-2")


class TestErrorClassify:
    """Phase 2.3: Error classification for quarantine vs retired."""

    def test_timeout_quarantine(self):
        state, cls = gov_mod.classify_error("timeout")
        assert state == "quarantine"

    def test_429_quarantine(self):
        state, cls = gov_mod.classify_error("status_429")
        assert state == "quarantine"

    def test_503_quarantine(self):
        state, cls = gov_mod.classify_error("status_503")
        assert state == "quarantine"

    def test_model_not_found_retired(self):
        state, cls = gov_mod.classify_error("model_not_found")
        assert state == "retired"

    def test_token_expired_retired(self):
        state, cls = gov_mod.classify_error("token_expired")
        assert state == "retired"

    def test_unknown_defaults_to_quarantine(self):
        state, cls = gov_mod.classify_error("some_random_error")
        assert state == "quarantine"
        assert cls == "unknown_transient"

    def test_xhigh_not_supported_quarantine(self):
        state, cls = gov_mod.classify_error("xhigh not supported")
        assert state == "quarantine"

    def test_hard_cloudflare_block_retired(self):
        state, cls = gov_mod.classify_error("hard_cloudflare_block repeated")
        assert state == "retired"


# ---------------------------------------------------------------------------
# Phase 3: Mesh and Runner lane/shard support
# ---------------------------------------------------------------------------

class TestMeshReadonlyLane:
    """Phase 3.1: mesh.py resolve_provider_allowlist with lanes."""

    def test_readonly_allowlist_env_overrides_gateway_only(self, monkeypatch):
        from codex import mesh as codex_mesh

        monkeypatch.setenv("CODEX_AUDIT_GATEWAY_ONLY", "true")
        monkeypatch.setenv("CODEX_CANONICAL_PROVIDER", "newapi-192.168.232.141-3000")
        monkeypatch.setenv("CODEX_READONLY_PROVIDER_ALLOWLIST", "ro-a-provider,ro-b-provider")
        monkeypatch.setenv("CODEX_READONLY_LANE", "codex-readonly")

        result = codex_mesh.resolve_provider_allowlist()
        assert result == ["ro-a-provider", "ro-b-provider"]

    def test_gateway_only_without_lanes_falls_back_to_canonical(self, monkeypatch):
        from codex import mesh as codex_mesh

        monkeypatch.setenv("CODEX_AUDIT_GATEWAY_ONLY", "true")
        monkeypatch.setenv("CODEX_CANONICAL_PROVIDER", "newapi-192.168.232.141-3000")
        monkeypatch.delenv("CODEX_READONLY_PROVIDER_ALLOWLIST", raising=False)
        monkeypatch.delenv("CODEX_READONLY_LANE", raising=False)
        monkeypatch.delenv("CODEX_STABLE_LANE", raising=False)

        result = codex_mesh.resolve_provider_allowlist()
        assert result == ["newapi-192.168.232.141-3000"]

    def test_gateway_only_false_allows_multi_provider_discovery(self, monkeypatch):
        from codex import mesh as codex_mesh

        monkeypatch.setenv("CODEX_AUDIT_GATEWAY_ONLY", "false")
        monkeypatch.setenv("CODEX_CANONICAL_PROVIDER", "newapi-192.168.232.141-3000")
        monkeypatch.delenv("CODEX_READONLY_PROVIDER_ALLOWLIST", raising=False)
        monkeypatch.delenv("CODEX_READONLY_LANE", raising=False)

        # With gateway_only=false and no explicit allowlist, should use multi-provider discovery
        result = codex_mesh.resolve_provider_allowlist()
        # Should not be forced to single canonical
        # (the actual result depends on DEFAULT_PROVIDER_PRIORITY/discovery, but it should not be forced to single)
        assert isinstance(result, list)


class TestRunnerShardRouting:
    """Phase 3.2: runner.py MeshRunnerConfig with lane/shard support."""

    def test_config_from_env_reads_lane_vars(self, monkeypatch, tmp_path):
        from automation.mesh_runner import runner

        monkeypatch.setenv("ISSUE_MESH_REPO_ROOT", str(tmp_path))
        monkeypatch.setenv("MESH_RUNNER_AUTH_TOKEN", "test-token")
        monkeypatch.setenv("CODEX_CANONICAL_PROVIDER", "newapi-192.168.232.141-3000")
        monkeypatch.setenv("CODEX_READONLY_LANE", "codex-readonly")
        monkeypatch.setenv("CODEX_STABLE_LANE", "codex-stable")
        monkeypatch.setenv("CODEX_READONLY_PROVIDER_ALLOWLIST", "ro-a,ro-b,ro-c")
        monkeypatch.setenv("ISSUE_MESH_READONLY_MAX_WORKERS", "12")

        config = runner.MeshRunnerConfig.from_env()
        assert config.readonly_lane == "codex-readonly"
        assert config.stable_lane == "codex-stable"
        assert config.readonly_provider_allowlist == ["ro-a", "ro-b", "ro-c"]

    def test_config_from_env_empty_lane_defaults(self, monkeypatch, tmp_path):
        from automation.mesh_runner import runner

        monkeypatch.setenv("ISSUE_MESH_REPO_ROOT", str(tmp_path))
        monkeypatch.setenv("MESH_RUNNER_AUTH_TOKEN", "test-token")
        monkeypatch.setenv("CODEX_CANONICAL_PROVIDER", "newapi-192.168.232.141-3000")
        monkeypatch.delenv("CODEX_READONLY_LANE", raising=False)
        monkeypatch.delenv("CODEX_STABLE_LANE", raising=False)
        monkeypatch.delenv("CODEX_READONLY_PROVIDER_ALLOWLIST", raising=False)

        config = runner.MeshRunnerConfig.from_env()
        assert config.readonly_lane == ""
        assert config.stable_lane == ""
        assert config.readonly_provider_allowlist == []


class TestLaneSummary:
    """Phase 2: Governance summary and lane/shard queries."""

    @pytest.fixture(autouse=True)
    def _setup_tmp_governance(self, tmp_path, monkeypatch):
        monkeypatch.setattr(gov_mod, "GOVERNANCE_DIR", tmp_path / ".governance")
        monkeypatch.setattr(gov_mod, "GOVERNANCE_STATE_PATH", tmp_path / ".governance" / "channel_state.json")
        monkeypatch.setattr(gov_mod, "LEASE_PATH", tmp_path / ".governance" / "governor_lease.json")

    def test_summary_counts_by_state(self):
        state = gov_mod.load_governance_state()
        gov_mod.transition_channel(state, "ch1", event="probe_ok")
        gov_mod.transition_channel(state, "ch2", event="probe_fail", error_text="timeout")
        gov_mod.transition_channel(state, "ch3", event="probe_fail", error_text="model_not_found")
        s = gov_mod.summary(state)
        assert s["by_state"]["active"] == 1
        assert s["by_state"]["quarantine"] == 1
        assert s["by_state"]["retired"] == 1
        assert s["total_channels"] == 3

    def test_get_shard_channels(self):
        state = gov_mod.load_governance_state()
        entry = gov_mod.transition_channel(state, "ch1", event="probe_ok")
        entry.shard = "ro-a"
        gov_mod.set_channel_entry(state, entry)
        result = gov_mod.get_shard_channels(state, "ro-a")
        assert len(result) == 1
        assert result[0].channel_identity == "ch1"

    def test_get_lane_channels(self):
        state = gov_mod.load_governance_state()
        entry = gov_mod.transition_channel(state, "ch1", event="probe_ok")
        entry.lane = "codex-stable"
        gov_mod.set_channel_entry(state, entry)
        result = gov_mod.get_lane_channels(state, "codex-stable")
        assert len(result) == 1

    def test_default_lane_assignments_use_dedicated_ro_groups(self):
        assignments = {(item.shard, item.lane, item.group, item.token_name) for item in gov_mod.DEFAULT_LANE_ASSIGNMENTS}
        assert ("stable", "codex-stable", "codex-stable", "codex-stable") in assignments
        assert ("ro-a", "codex-ro-a", "codex-ro-a", "codex-ro-a") in assignments
        assert ("ro-b", "codex-ro-b", "codex-ro-b", "codex-ro-b") in assignments
        assert ("ro-c", "codex-ro-c", "codex-ro-c", "codex-ro-c") in assignments
        assert ("ro-d", "codex-ro-d", "codex-ro-d", "codex-ro-d") in assignments
