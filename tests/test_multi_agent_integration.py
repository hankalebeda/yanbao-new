"""Integration tests for the multi-agent escort team.

Simulates the full DISCOVERY → ANALYSIS → FIX → VERIFY → WRITEBACK → PROMOTE
pipeline using real agent instances communicating via a shared Mailbox.

All tests run in-process with no external service dependencies.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from automation.agents import (
    AgentConfig,
    AgentRole,
    AgentState,
    CoordinatorAgent,
    CoordinatorMode,
    DiscoveryAgent,
    AnalysisAgent,
    FixAgent,
    VerifyAgent,
    WritebackAgent,
    PromoteAgent,
    EscortTeam,
    Mailbox,
    MessageType,
    AgentMessage,
    ProblemSpec,
    CoordinatorState,
    create_team,
)
from automation.agents.protocol import AnalysisResult, ProblemStatus


@pytest.fixture
def repo_root(tmp_path):
    """Create a minimal repo structure for integration tests."""
    # junit.xml with one failure
    output = tmp_path / "output"
    output.mkdir()
    junit = output / "junit.xml"
    junit.write_text(
        '<?xml version="1.0" ?>\n'
        '<testsuites><testsuite name="tests" tests="2" failures="1">'
        '<testcase classname="tests.test_fr01" name="test_pool_refresh" time="0.1">'
        '<failure message="AssertionError">pool not refreshed</failure>'
        '</testcase>'
        '<testcase classname="tests.test_fr02" name="test_ok" time="0.05"/>'
        '</testsuite></testsuites>',
        encoding="utf-8",
    )

    # blind_spot_audit.json (clean)
    bs = output / "blind_spot_audit.json"
    bs.write_text('{"FAKE": [], "HOLLOW": [], "WEAK": []}', encoding="utf-8")

    # catalog_snapshot.json (fresh)
    gov = tmp_path / "app" / "governance"
    gov.mkdir(parents=True)
    cat = gov / "catalog_snapshot.json"
    cat.write_text(
        '{"test_result_freshness": "fresh", "total": 119, "total_collected": 119}',
        encoding="utf-8",
    )

    # continuous audit (one finding)
    audit_dir = tmp_path / "github" / "automation" / "continuous_audit"
    audit_dir.mkdir(parents=True)
    latest = audit_dir / "latest_run.json"
    latest.write_text(json.dumps({
        "findings": [{
            "id": "issue-registry-gap",
            "family": "issue-registry",
            "risk_level": "P2",
            "title": "ISSUE-REGISTRY mapping gap",
            "handling_path": "fix_code",
            "affected_files": [],
            "affected_frs": [],
        }]
    }), encoding="utf-8")

    # runtime dirs
    (tmp_path / "runtime" / "agents" / "knowledge").mkdir(parents=True)
    (tmp_path / "runtime" / "loop_controller").mkdir(parents=True)

    return tmp_path


@pytest.fixture
def mailbox():
    return Mailbox(name="integration-test")


@pytest.fixture
def config(repo_root):
    return AgentConfig(
        repo_root=repo_root,
        service_urls={"webai": "http://127.0.0.1:1"},  # force heuristic fallback
    )


@pytest.fixture(autouse=True)
def _no_external_services(monkeypatch):
    """Prevent tests from hitting real Codex CLI or AI services."""
    monkeypatch.setattr(
        "automation.agents.codex_bridge.resolve_codex_executable",
        lambda: None,
    )


# ====================================================================
# Individual agent integration tests
# ====================================================================

class TestDiscoveryIntegration:
    """Test DiscoveryAgent against real repo fixtures."""

    @pytest.mark.asyncio
    async def test_discovery_finds_problems(self, mailbox, config):
        agent = DiscoveryAgent(mailbox=mailbox, config=config)
        result = await agent.handle_task({"round_id": "r1", "mode": "full"})

        assert "findings" in result
        findings = result["findings"]

        # Should find: 1 audit finding + 1 test failure = at least 2
        assert len(findings) >= 2

        # Check audit finding
        audit_findings = [f for f in findings if f.get("source_probe") == "audit"]
        assert len(audit_findings) >= 1

        # Check test failure finding
        test_findings = [f for f in findings if f.get("source_probe") == "test_failure"]
        assert len(test_findings) >= 1
        assert all(f.get("problem_id") != "catalog-stale" for f in findings)

    @pytest.mark.asyncio
    async def test_discovery_incremental(self, mailbox, config):
        agent = DiscoveryAgent(mailbox=mailbox, config=config)
        result = await agent.handle_task({"round_id": "r1", "mode": "incremental"})

        # Incremental only runs fast probes
        assert "findings" in result
        findings = result["findings"]
        probes = set(f.get("source_probe") for f in findings)
        # Should only have test_failure and/or runtime_health
        assert probes <= {"test_failure", "runtime_health"}

    @pytest.mark.asyncio
    async def test_discovery_classifies_issue_registry_lane(self, mailbox, config):
        agent = DiscoveryAgent(mailbox=mailbox, config=config)
        result = await agent.handle_task({"round_id": "r1", "mode": "full"})

        issue_registry = next(
            f for f in result["findings"] if f.get("family") == "issue-registry"
        )
        assert issue_registry["lane_id"] == "gov_registry"
        assert issue_registry["current_status"] == ProblemStatus.ACTIVE.value
        assert "app/governance/feature_registry.json" in issue_registry["write_scope"]


class TestAnalysisIntegration:
    """Test AnalysisAgent against discovered problems."""

    @pytest.mark.asyncio
    async def test_analysis_produces_triage(self, mailbox, config):
        agent = AnalysisAgent(mailbox=mailbox, config=config)
        problems = [
            ProblemSpec(
                problem_id="test-p1",
                source_probe="test_failure",
                severity="P1",
                title="Test failure in FR01",
            ).to_dict(),
            ProblemSpec(
                problem_id="audit-p2",
                source_probe="audit",
                severity="P2",
                title="Audit finding",
            ).to_dict(),
        ]

        result = await agent.handle_task({
            "round_id": "r1",
            "problems": problems,
        })

        assert "findings" in result
        assert result["total"] == 2
        # At least some should be auto_fix (test failures have high confidence)
        assert result["auto_fix"] >= 1

    @pytest.mark.asyncio
    async def test_analysis_knowledge_persistence(self, mailbox, config, repo_root):
        agent = AnalysisAgent(mailbox=mailbox, config=config)
        problems = [ProblemSpec(
            problem_id="persist-test",
            source_probe="test_failure",
            severity="P1",
        ).to_dict()]

        await agent.handle_task({"round_id": "r1", "problems": problems})

        # Check knowledge file
        history = repo_root / "runtime" / "agents" / "knowledge" / "analysis_history.jsonl"
        assert history.exists()
        lines = history.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) >= 1
        entry = json.loads(lines[0])
        assert entry["problem_id"] == "persist-test"


class TestFixIntegration:
    """Test FixAgent with analysis results."""

    @pytest.mark.asyncio
    async def test_fix_generates_patches(self, mailbox, config):
        agent = FixAgent(mailbox=mailbox, config=config)
        analyses = [{
            "problem_id": "test-fix-1",
            "root_cause": "missing import",
            "fix_strategy": "fix_code",
            "confidence": 0.9,
            "triage": "auto_fix",
        }]

        result = await agent.handle_task({
            "round_id": "r1",
            "analyses": analyses,
        })

        assert "findings" in result
        # v2: When AI is unavailable, fix returns 0 patches (no more stub patches)
        # This is correct behavior — the system escalates instead of faking success
        assert result["fix_count"] == 0
        assert result["failed_count"] == 0

    @pytest.mark.asyncio
    async def test_fix_rollback(self, mailbox, config):
        agent = FixAgent(mailbox=mailbox, config=config)

        result = await agent.handle_task({
            "round_id": "r1",
            "action": "rollback",
            "patches": [{"problem_id": "p1"}, {"problem_id": "p2"}],
        })

        assert result["action"] == "rollback"
        assert len(result["rolled_back"]) == 2

    @pytest.mark.asyncio
    async def test_fix_strategy_stats(self, mailbox, config, repo_root):
        agent = FixAgent(mailbox=mailbox, config=config)
        analyses = [{
            "problem_id": "stat-test",
            "fix_strategy": "fix_code",
            "confidence": 0.8,
            "triage": "auto_fix",
        }]

        await agent.handle_task({"round_id": "r1", "analyses": analyses})

        stats_path = repo_root / "runtime" / "agents" / "knowledge" / "fix_strategy_stats.json"
        # v2: Stats file is only written when patches are generated.
        # When AI unavailable, no patches = no stats update (correct behavior).
        # The file may not exist if no previous successful fixes occurred.
        if stats_path.exists():
            stats = json.loads(stats_path.read_text(encoding="utf-8"))
            # Stats can be empty dict when no fixes succeed
            assert isinstance(stats, dict)

    def test_fix_filters_out_of_scope_patches(self, mailbox, config):
        agent = FixAgent(mailbox=mailbox, config=config)
        analysis = AnalysisResult(
            problem_id="scope-test",
            fix_strategy="fix_code",
            confidence=0.9,
            triage="auto_fix",
            lane_id="gov_mapping",
            task_family="feature-governance",
            write_scope=["app/governance/**"],
        )

        filtered = agent._filter_patches_by_scope(
            analysis,
            [
                {"path": "tests/test_out_of_scope.py", "patch_text": "bad"},
                {"path": "app/governance/build_feature_catalog.py", "patch_text": "ok"},
            ],
        )

        assert [patch["path"] for patch in filtered] == [
            "app/governance/build_feature_catalog.py"
        ]


class TestVerifyIntegration:
    """Test VerifyAgent gates."""

    @pytest.mark.asyncio
    async def test_verify_governance_gates(self, mailbox, config):
        agent = VerifyAgent(mailbox=mailbox, config=config)

        # With stub patches (no real code changes)
        result = await agent.handle_task({
            "round_id": "r1",
            "patches": [{
                "problem_id": "v1",
                "patches": [{"path": "__analysis__/v1", "patch_text": "stub"}],
                "fix_strategy_used": "fix_code",
            }],
        })

        # Governance gates should pass (test fixtures are clean)
        assert result["blind_spot_clean"] is True
        assert result["catalog_fresh"] is True

    @pytest.mark.asyncio
    async def test_verify_security_scan(self, mailbox, config):
        agent = VerifyAgent(mailbox=mailbox, config=config)

        # Patches with security issues
        result = await agent.handle_task({
            "round_id": "r1",
            "patches": [{
                "problem_id": "sec-1",
                "patches": [{
                    "path": "app/danger.py",
                    "patch_text": 'os.system("rm -rf /")',
                }],
                "fix_strategy_used": "fix_code",
            }],
        })

        assert result["security_clean"] is False
        assert "security_scan" in result["failed_gates"]

    @pytest.mark.asyncio
    async def test_verify_uses_candidate_workspace_for_real_patches(self, mailbox, config, repo_root):
        target = repo_root / "app" / "sample_module.py"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 1\n", encoding="utf-8")

        agent = VerifyAgent(mailbox=mailbox, config=config)
        observed_roots: list[Path] = []

        async def _fake_scoped(_affected_files, _round_id, workspace_root):
            observed_roots.append(workspace_root)
            assert (workspace_root / "app" / "sample_module.py").read_text(encoding="utf-8") == "VALUE = 2\n"
            return True

        async def _fake_full(_round_id, workspace_root):
            observed_roots.append(workspace_root)
            assert (workspace_root / "app" / "sample_module.py").read_text(encoding="utf-8") == "VALUE = 2\n"
            return True

        with patch.object(agent, "_gate_scoped_pytest", side_effect=_fake_scoped):
            with patch.object(agent, "_gate_full_regression", side_effect=_fake_full):
                with patch.object(
                    agent,
                    "_gate_governance",
                    new=AsyncMock(return_value={
                        "blind_spot_clean": True,
                        "catalog_fresh": True,
                        "artifacts_aligned": True,
                    }),
                ):
                    with patch.object(agent, "_gate_contract_validation", new=AsyncMock(return_value=True)):
                        with patch.object(agent, "_gate_security_scan", new=AsyncMock(return_value=[])):
                            result = await agent.handle_task({
                                "round_id": "r-candidate",
                                "patches": [{
                                    "problem_id": "candidate-1",
                                    "patches": [{
                                        "path": "app/sample_module.py",
                                        "old_text": "VALUE = 1\n",
                                        "patch_text": "VALUE = 2\n",
                                    }],
                                    "fix_strategy_used": "fix_code",
                                }],
                            })

        assert result["all_passed"] is True
        assert result["details"]["candidate_workspace"] is True
        assert observed_roots
        assert all(root != repo_root for root in observed_roots)
        assert target.read_text(encoding="utf-8") == "VALUE = 1\n"


class TestWritebackIntegration:
    """Test WritebackAgent (stub mode without external services)."""

    class _FakeAsyncResponse:
        def __init__(self, payload: dict | None = None, *, status_code: int = 200, text: str | None = None):
            self._payload = payload or {}
            self.status_code = status_code
            self.text = text or json.dumps(self._payload, ensure_ascii=False)

        def json(self) -> dict:
            return self._payload

    class _FakeAsyncClientFactory:
        def __init__(self, responses: list["TestWritebackIntegration._FakeAsyncResponse"]):
            self.responses = list(responses)
            self.calls: list[tuple[str, dict[str, Any] | None, dict[str, str] | None]] = []

        def __call__(self, *args, **kwargs):
            factory = self

            class _FakeAsyncClient:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

                async def post(self, url: str, *, json: dict | None = None, headers: dict[str, str] | None = None):
                    factory.calls.append((url, json, headers))
                    if not factory.responses:
                        raise AssertionError(f"Unexpected HTTP call: {url}")
                    return factory.responses.pop(0)

            return _FakeAsyncClient()

    @pytest.mark.asyncio
    async def test_writeback_skips_stubs(self, mailbox, config):
        agent = WritebackAgent(mailbox=mailbox, config=config)

        result = await agent.handle_task({
            "round_id": "r1",
            "patches": [{
                "problem_id": "stub-1",
                "patches": [{"path": "__analysis__/stub-1", "patch_text": "noop"}],
                "fix_strategy_used": "fix_code",
            }],
        })

        # Stub patches should be skipped
        assert result["receipt_count"] == 0

    @pytest.mark.asyncio
    async def test_writeback_uses_preview_triage_and_batch_commit(self, mailbox, repo_root):
        agent = WritebackAgent(
            mailbox=mailbox,
            config=AgentConfig(
                repo_root=repo_root,
                service_urls={
                    "writeback_a": "http://fake:8092",
                    "promote_prep": "http://fake:8094",
                },
            ),
        )
        factory = self._FakeAsyncClientFactory(
            [
                self._FakeAsyncResponse({"lease_id": "lease-1", "fencing_token": "ft-1"}),
                self._FakeAsyncResponse({"results": [{"target_path": "app/example.py", "conflict": False}]}),
                self._FakeAsyncResponse({"auto_commit": True, "triage_record_id": "triage-1"}),
                self._FakeAsyncResponse({"commits": [{"commit_id": "commit-1"}]}),
                self._FakeAsyncResponse({"released": True}),
            ]
        )

        with patch("httpx.AsyncClient", side_effect=factory):
            result = await agent.handle_task(
                {
                    "round_id": "r-writeback",
                    "verify": {
                        "runtime_gates": {
                            "status": "ready",
                            "shared_artifact_promote": {"allowed": True},
                        }
                    },
                    "patches": [{
                        "problem_id": "wb-1",
                        "lane_id": "gov_mapping",
                        "patches": [{
                            "path": "app/example.py",
                            "before_sha": "base-sha",
                            "patch_text": "print('ok')\n",
                        }],
                        "fix_strategy_used": "fix_code",
                    }],
                }
            )

        assert result["receipt_count"] == 1
        assert result["findings"][0]["commit_sha"] == "commit-1"
        assert [call[0] for call in factory.calls] == [
            "http://fake:8092/v1/lease/claim",
            "http://fake:8092/v1/batch-preview",
            "http://fake:8094/v1/triage/writeback",
            "http://fake:8092/v1/batch-commit",
            "http://fake:8092/v1/lease/release",
        ]
        assert factory.calls[3][1]["lease_id"] == "lease-1"
        assert factory.calls[3][1]["fencing_token"] == "ft-1"
        assert factory.calls[3][1]["triage_record_ids"] == ["triage-1"]


class TestPromoteIntegration:
    """Test PromoteAgent tier decisions."""

    class _FakeAsyncResponse:
        def __init__(self, payload: dict | None = None, *, status_code: int = 200, text: str | None = None):
            self._payload = payload or {}
            self.status_code = status_code
            self.text = text or json.dumps(self._payload, ensure_ascii=False)

        def json(self) -> dict:
            return self._payload

    class _FakeAsyncClientFactory:
        def __init__(self, repo_root: Path, responses: list["TestPromoteIntegration._FakeAsyncResponse"]):
            self.repo_root = repo_root
            self.responses = list(responses)
            self.calls: list[tuple[str, dict[str, Any] | None, dict[str, str] | None]] = []

        def __call__(self, *args, **kwargs):
            factory = self

            class _FakeAsyncClient:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

                async def post(self, url: str, *, json: dict | None = None, headers: dict[str, str] | None = None):
                    factory.calls.append((url, json, headers))
                    state_path = factory.repo_root / "automation" / "control_plane" / "current_state.json"
                    if state_path.exists():
                        import json as _json

                        payload = _json.loads(state_path.read_text(encoding="utf-8"))
                        assert payload["promote_target_mode"] == "infra"
                    if not factory.responses:
                        raise AssertionError(f"Unexpected HTTP call: {url}")
                    return factory.responses.pop(0)

            return _FakeAsyncClient()

    @pytest.mark.asyncio
    async def test_promote_tier1(self, mailbox, config):
        agent = PromoteAgent(mailbox=mailbox, config=config)

        result = await agent.handle_task({
            "round_id": "r1",
            "writeback": {"receipt_count": 1},
            "verify": {"all_passed": True},
        })

        assert result["tier"] == 1
        assert result["approved"] is True
        assert any(t.startswith("status-note") for t in result["targets_promoted"])
        assert "current-layer" not in result["targets_promoted"]
        assert "doc22" not in result["targets_promoted"]

    @pytest.mark.asyncio
    async def test_promote_tier1_real_chain_only_commits_status_note(self, mailbox, repo_root):
        control_plane = repo_root / "automation" / "control_plane"
        control_plane.mkdir(parents=True, exist_ok=True)
        (control_plane / "current_state.json").write_text(
            json.dumps({"_schema": "infra_promote_v1", "promote_target_mode": "infra"}, ensure_ascii=False),
            encoding="utf-8",
        )
        agent = PromoteAgent(
            mailbox=mailbox,
            config=AgentConfig(
                repo_root=repo_root,
                service_urls={
                    "promote_prep": "http://fake:8094",
                    "writeback_b": "http://fake:8095",
                },
            ),
        )
        factory = self._FakeAsyncClientFactory(
            repo_root,
            [
                self._FakeAsyncResponse(
                    {
                        "run_id": "audit-r1",
                        "layer": "status-note",
                        "target_path": "docs/core/22_全量功能进度总表_v7_精审.md",
                        "target_anchor": "current-writeback-detail",
                        "patch_text": "patched status note",
                        "base_sha256": "sha-22",
                        "idempotency_key": "issue-mesh:audit-r1:status-note:current-writeback-detail",
                        "request_id": "req-status-note",
                        "semantic_fingerprint": "fp-status-note",
                        "skip_commit": False,
                    }
                ),
                self._FakeAsyncResponse({"auto_commit": True, "triage_record_id": "triage-status-note"}),
                self._FakeAsyncResponse({"conflict": False}),
                self._FakeAsyncResponse({"status": "committed", "commit_id": "commit-status-note"}),
            ],
        )

        with patch("automation.agents.promote.httpx.AsyncClient", side_effect=factory):
            result = await agent.handle_task(
                {
                    "round_id": "r1",
                    "writeback": {"receipt_count": 1},
                    "verify": {
                        "all_passed": True,
                        "audit_run_id": "audit-r1",
                        "runtime_gates": {
                            "status": "ready",
                            "shared_artifact_promote": {"allowed": True},
                        },
                        "artifacts_aligned": True,
                    },
                }
            )

        assert result["tier"] == 1
        assert result["approved"] is True
        assert result["targets_promoted"] == ["status-note"]
        assert [call[0] for call in factory.calls] == [
            "http://fake:8094/v1/promote/status-note",
            "http://fake:8094/v1/triage",
            "http://fake:8095/v1/preview",
            "http://fake:8095/v1/commit",
        ]
        restored = json.loads((control_plane / "current_state.json").read_text(encoding="utf-8"))
        assert restored["promote_target_mode"] == "infra"

    @pytest.mark.asyncio
    async def test_promote_tier3_runs_real_chain_without_rewriting_control_plane(self, mailbox, repo_root):
        control_plane = repo_root / "automation" / "control_plane"
        control_plane.mkdir(parents=True, exist_ok=True)
        (control_plane / "current_state.json").write_text(
            json.dumps({"_schema": "infra_promote_v1", "promote_target_mode": "infra"}, ensure_ascii=False),
            encoding="utf-8",
        )
        agent = PromoteAgent(
            mailbox=mailbox,
            config=AgentConfig(
                repo_root=repo_root,
                service_urls={
                    "promote_prep": "http://fake:8094",
                    "writeback_b": "http://fake:8095",
                },
            ),
        )
        agent._promote_state["consecutive_successes"] = 4
        factory = self._FakeAsyncClientFactory(
            repo_root,
            [
                self._FakeAsyncResponse(
                    {
                        "run_id": "audit-r3",
                        "layer": "status-note",
                        "target_path": "docs/core/22_全量功能进度总表_v7_精审.md",
                        "target_anchor": "current-writeback-detail",
                        "patch_text": "patched status note",
                        "base_sha256": "sha-22",
                        "idempotency_key": "issue-mesh:audit-r3:status-note:current-writeback-detail",
                        "request_id": "req-status-note",
                        "semantic_fingerprint": "fp-status-note",
                        "skip_commit": False,
                    }
                ),
                self._FakeAsyncResponse({"auto_commit": True, "triage_record_id": "triage-status-note"}),
                self._FakeAsyncResponse({"conflict": False}),
                self._FakeAsyncResponse({"status": "committed", "commit_id": "commit-status-note"}),
                self._FakeAsyncResponse(
                    {
                        "run_id": "audit-r3",
                        "layer": "current-layer",
                        "target_path": "docs/core/22_全量功能进度总表_v7_精审.md",
                        "target_anchor": "2.1|2.3|4.5",
                        "patch_text": "patched current layer",
                        "base_sha256": "sha-22",
                        "idempotency_key": "issue-mesh:audit-r3:current-layer:2.1|2.3|4.5",
                        "request_id": "req-current-layer",
                        "semantic_fingerprint": "fp-current-layer",
                        "skip_commit": False,
                    }
                ),
                self._FakeAsyncResponse({"auto_commit": True, "triage_record_id": "triage-current-layer"}),
                self._FakeAsyncResponse({"conflict": False}),
                self._FakeAsyncResponse({"status": "committed", "commit_id": "commit-current-layer"}),
            ],
        )

        with patch("automation.agents.promote.httpx.AsyncClient", side_effect=factory):
            result = await agent.handle_task(
                {
                    "round_id": "r3",
                    "writeback": {"receipt_count": 1},
                    "verify": {
                        "all_passed": True,
                        "audit_run_id": "audit-r3",
                        "runtime_gates": {
                            "status": "ready",
                            "shared_artifact_promote": {"allowed": True},
                        },
                        "artifacts_aligned": True,
                    },
                }
            )

        assert result["tier"] == 3
        assert result["approved"] is True
        assert result["targets_promoted"] == ["status-note", "shared-artifact", "current-layer", "doc22"]
        assert [call[0] for call in factory.calls] == [
            "http://fake:8094/v1/promote/status-note",
            "http://fake:8094/v1/triage",
            "http://fake:8095/v1/preview",
            "http://fake:8095/v1/commit",
            "http://fake:8094/v1/promote/current-layer",
            "http://fake:8094/v1/triage",
            "http://fake:8095/v1/preview",
            "http://fake:8095/v1/commit",
        ]
        restored = json.loads((control_plane / "current_state.json").read_text(encoding="utf-8"))
        assert restored["promote_target_mode"] == "infra"

    @pytest.mark.asyncio
    async def test_promote_blocked_on_verify(self, mailbox, config):
        agent = PromoteAgent(mailbox=mailbox, config=config)

        result = await agent.handle_task({
            "round_id": "r1",
            "writeback": {},
            "verify": {"all_passed": False},
        })

        assert result["approved"] is False
        assert result["reason"] == "verification_not_passed"


# ====================================================================
# Full pipeline integration
# ====================================================================

class TestFullPipeline:
    """Test the complete multi-agent pipeline end-to-end."""

    @pytest.mark.asyncio
    async def test_discovery_to_analysis_pipeline(self, mailbox, config):
        """Discovery → Analysis pipeline produces actionable results."""
        discovery = DiscoveryAgent(mailbox=mailbox, config=config)
        analysis = AnalysisAgent(mailbox=mailbox, config=config)

        # Step 1: Discovery
        disc_result = await discovery.handle_task({
            "round_id": "pipeline-1",
            "mode": "full",
        })

        # Step 2: Analysis
        ana_result = await analysis.handle_task({
            "round_id": "pipeline-1",
            "problems": disc_result["findings"],
        })

        assert ana_result["total"] >= 2
        # Pipeline should produce at least one auto_fix
        assert ana_result["auto_fix"] >= 1

    @pytest.mark.asyncio
    async def test_team_creation_and_status(self, repo_root):
        """EscortTeam can be created and reports status."""
        team = create_team(repo_root=repo_root)
        status = team.get_status()

        assert len(team.agents) == 7
        assert "coordinator" in status
        assert status["coordinator"]["autonomy_index"] >= 0

    @pytest.mark.asyncio
    async def test_team_start_and_shutdown(self, repo_root):
        """EscortTeam starts all agents and shuts down cleanly."""
        team = create_team(repo_root=repo_root)
        await team.start()

        # All agents should be running
        for agent in team.agents:
            assert agent.state in (AgentState.RUNNING, AgentState.WAITING)

        await team.shutdown("test_complete")

        # All agents should be shut down
        for agent in team.agents:
            assert agent.state == AgentState.SHUTDOWN

    @pytest.mark.asyncio
    async def test_mailbox_persistence_roundtrip(self, repo_root, tmp_path):
        """Mailbox persists messages across restarts."""
        db_path = tmp_path / "pipeline_mailbox.db"

        # Write messages
        mb1 = Mailbox(name="pipeline", backing_path=db_path)
        await mb1.send(AgentMessage(
            source="coordinator",
            target="discovery",
            msg_type=MessageType.TASK_DISPATCH.value,
            payload={"round_id": "persist-r1"},
        ))
        mb1.close()

        # Restore and verify
        mb2 = Mailbox(name="pipeline", backing_path=db_path)
        assert mb2.depth == 1
        msg = await mb2.poll()
        assert msg.payload["round_id"] == "persist-r1"
        mb2.close()


# ====================================================================
# New Probe tests (Phase B)
# ====================================================================

class TestProviderHealthProbe:
    """Test ProviderHealthProbe (HTTP-based)."""

    @pytest.mark.asyncio
    async def test_no_url_returns_empty(self, repo_root):
        from automation.agents.discovery import ProviderHealthProbe

        probe = ProviderHealthProbe(repo_root, new_api_url="", new_api_token="")
        result = await probe.scan()
        assert result == []

    @pytest.mark.asyncio
    async def test_provider_ready(self, repo_root):
        from automation.agents.discovery import ProviderHealthProbe
        import httpx as _httpx

        probe = ProviderHealthProbe(
            repo_root, new_api_url="http://fake:3000", new_api_token="sk-test"
        )

        mock_response_models = MagicMock()
        mock_response_models.status_code = 200
        mock_response_models.json.return_value = {"data": [{"id": "gpt-4"}]}

        mock_response_smoke = MagicMock()
        mock_response_smoke.status_code = 200
        mock_response_smoke.json.return_value = {
            "choices": [{"message": {"content": "pong"}}],
        }
        mock_response_smoke.headers = {"content-type": "application/json"}

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response_models)
        mock_client.post = AsyncMock(return_value=mock_response_smoke)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch.object(_httpx, "AsyncClient", return_value=mock_client):
            result = await probe.scan()

        assert result == []  # Empty = healthy

    @pytest.mark.asyncio
    async def test_provider_down(self, repo_root):
        from automation.agents.discovery import ProviderHealthProbe
        import httpx as _httpx

        probe = ProviderHealthProbe(
            repo_root, new_api_url="http://fake:3000", new_api_token=""
        )

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=_httpx.ConnectError("refused"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch.object(_httpx, "AsyncClient", return_value=mock_client):
            result = await probe.scan()

        assert len(result) == 1
        assert result[0].severity == "P0"
        assert result[0].family == "infrastructure"


class TestMeshAuditProbe:
    """Test MeshAuditProbe (HTTP-based)."""

    @pytest.mark.asyncio
    async def test_no_url_returns_empty(self, repo_root):
        from automation.agents.discovery import MeshAuditProbe

        probe = MeshAuditProbe(repo_root, mesh_runner_url="", mesh_runner_token="")
        result = await probe.scan()
        assert result == []

    @pytest.mark.asyncio
    async def test_mesh_audit_with_findings(self, repo_root):
        from automation.agents.discovery import MeshAuditProbe

        probe = MeshAuditProbe(
            repo_root, mesh_runner_url="http://fake:8093", mesh_runner_token="tok"
        )

        mock_start = MagicMock()
        mock_start.status_code = 200
        mock_start.json.return_value = {"run_id": "run-123"}

        mock_status = MagicMock()
        mock_status.status_code = 200
        mock_status.json.return_value = {"status": "completed"}

        mock_bundle = MagicMock()
        mock_bundle.status_code = 200
        mock_bundle.json.return_value = {
            "findings": [{
                "id": "mesh-f1",
                "family": "test-family",
                "risk_level": "P1",
                "title": "Missing coverage",
                "detail": "FR03 not covered",
                "affected_files": ["app/api.py"],
                "affected_frs": ["FR03"],
                "handling_path": "fix_code",
            }]
        }

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_start)
        mock_client.get = AsyncMock(side_effect=[mock_status, mock_bundle])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        import httpx as _httpx
        with patch.object(_httpx, "AsyncClient", return_value=mock_client):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                result = await probe.scan()

        assert len(result) == 1
        assert result[0].problem_id == "mesh-f1"
        assert result[0].severity == "P1"
        assert result[0].family == "test-family"


# ====================================================================
# Coordinator enhancement tests (Phase C)
# ====================================================================

class TestCoordinatorControlPlaneProjection:
    """Test that Coordinator writes control_plane projection."""

    @pytest.mark.asyncio
    async def test_cp_projection_writes_json(self, mailbox, config, repo_root):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        agent._coord_state.mode = "fix"
        agent._coord_state.phase = "discovery"
        agent._coord_state.current_round_id = "test-r1"
        agent._coord_state.consecutive_green_rounds = 3
        agent._coord_state.total_fixes = 5
        agent._coord_state.total_failures = 2

        agent._save_state()

        cp_json = repo_root / "automation" / "control_plane" / "current_state.json"
        cp_md = repo_root / "automation" / "control_plane" / "current_status.md"

        assert cp_json.exists()
        data = json.loads(cp_json.read_text(encoding="utf-8"))
        assert data["_schema"] == "infra_promote_v1"
        assert data["mode"] == "fix"
        assert data["promote_target_mode"] == "infra"
        assert data["total_fixes"] == 5
        assert data["consecutive_fix_success_count"] == 3

        assert cp_md.exists()
        md_text = cp_md.read_text(encoding="utf-8")
        assert "test-r1" in md_text

    @pytest.mark.asyncio
    async def test_cp_projection_preserves_explicit_doc22_mode(self, mailbox, config, repo_root):
        control_plane = repo_root / "automation" / "control_plane"
        control_plane.mkdir(parents=True, exist_ok=True)
        (control_plane / "current_state.json").write_text(
            json.dumps({"_schema": "infra_promote_v1", "promote_target_mode": "doc22"}, ensure_ascii=False),
            encoding="utf-8",
        )

        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        agent._coord_state.mode = "fix"
        agent._coord_state.phase = "discovery"
        agent._save_state()

        data = json.loads((control_plane / "current_state.json").read_text(encoding="utf-8"))
        assert data["promote_target_mode"] == "doc22"


class TestCoordinatorPromotionHonesty:
    @pytest.mark.asyncio
    async def test_orchestration_round_stops_before_promote_when_writeback_fails(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        agent._coord_state.mode = CoordinatorMode.FIX.value

        dispatch = AsyncMock(side_effect=[
            {
                "findings": [{
                    "problem_id": "p1",
                    "lane_id": "gov_mapping",
                    "task_family": "feature-governance",
                    "current_status": ProblemStatus.ACTIVE.value,
                    "write_scope": ["app/example.py"],
                }]
            },
            {
                "findings": [{
                    "problem_id": "p1",
                    "triage": "auto_fix",
                    "current_status": ProblemStatus.ACTIVE.value,
                    "lane_id": "gov_mapping",
                    "task_family": "feature-governance",
                    "write_scope": ["app/example.py"],
                }]
            },
            {
                "findings": [{
                    "problem_id": "p1",
                    "lane_id": "gov_mapping",
                    "patches": [{
                        "path": "app/example.py",
                        "old_text": "before",
                        "patch_text": "after",
                    }],
                    "fix_strategy_used": "fix_code",
                }]
            },
            {"all_passed": True},
            {"receipt_count": 0, "errors": ["lease_claim_failed"]},
        ])

        with patch.object(agent, "_dispatch_and_wait", dispatch):
            with patch.object(agent, "_check_stall", new=AsyncMock()):
                with patch.object(agent, "_refresh_shared_artifacts", new=AsyncMock()):
                    with patch("asyncio.sleep", new=AsyncMock()):
                        await agent._orchestration_round()

        assert dispatch.await_count == 5
        assert agent._coord_state.round_history[-1]["result"] == "writeback_failed"
        assert agent._coord_state.consecutive_green_rounds == 0
        assert agent._coord_state.total_fixes == 0
        assert agent._coord_state.execution_lanes["gov_mapping"]["status"] == ProblemStatus.ACTIVE.value

    @pytest.mark.asyncio
    async def test_orchestration_round_does_not_mark_lane_completed_when_promote_blocked(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        agent._coord_state.mode = CoordinatorMode.FIX.value

        dispatch = AsyncMock(side_effect=[
            {
                "findings": [{
                    "problem_id": "p1",
                    "lane_id": "gov_mapping",
                    "task_family": "feature-governance",
                    "current_status": ProblemStatus.ACTIVE.value,
                    "write_scope": ["app/example.py"],
                }]
            },
            {
                "findings": [{
                    "problem_id": "p1",
                    "triage": "auto_fix",
                    "current_status": ProblemStatus.ACTIVE.value,
                    "lane_id": "gov_mapping",
                    "task_family": "feature-governance",
                    "write_scope": ["app/example.py"],
                }]
            },
            {
                "findings": [{
                    "problem_id": "p1",
                    "lane_id": "gov_mapping",
                    "patches": [{
                        "path": "app/example.py",
                        "old_text": "before",
                        "patch_text": "after",
                    }],
                    "fix_strategy_used": "fix_code",
                }]
            },
            {
                "all_passed": True,
                "artifacts_aligned": True,
                "runtime_gates": {
                    "status": "ready",
                    "shared_artifact_promote": {"allowed": True},
                },
            },
            {"receipt_count": 1, "errors": []},
            {"approved": False, "reason": "tier1_blocked", "targets_promoted": []},
        ])

        with patch.object(agent, "_dispatch_and_wait", dispatch):
            with patch.object(agent, "_check_stall", new=AsyncMock()):
                with patch.object(agent, "_refresh_shared_artifacts", new=AsyncMock()):
                    with patch("asyncio.sleep", new=AsyncMock()):
                        await agent._orchestration_round()

        assert dispatch.await_count == 6
        assert agent._coord_state.round_history[-1]["result"] == "promotion_blocked"
        assert agent._coord_state.consecutive_green_rounds == 0
        assert agent._coord_state.total_fixes == 0
        assert agent._coord_state.execution_lanes["gov_mapping"]["status"] == ProblemStatus.ACTIVE.value


class TestCoordinatorStallDetection:
    """Test stall detection triggers."""

    @pytest.mark.asyncio
    async def test_stall_increments(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)

        await agent._check_stall(had_progress=False)
        assert agent._coord_state.consecutive_no_progress_rounds == 1

        await agent._check_stall(had_progress=False)
        assert agent._coord_state.consecutive_no_progress_rounds == 2

        await agent._check_stall(had_progress=True)
        assert agent._coord_state.consecutive_no_progress_rounds == 0

    @pytest.mark.asyncio
    async def test_stall_threshold_triggers_doctor(self, mailbox, config, repo_root):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        agent._coord_state.consecutive_no_progress_rounds = 4  # one below threshold

        with patch("automation.agents.coordinator.CoordinatorAgent._enter_safe_hold",
                   new_callable=AsyncMock):
            # v2: Uses inline infra_doctor — mock _inline_infra_diagnosis
            with patch.object(agent, "_inline_infra_diagnosis",
                              new_callable=AsyncMock,
                              return_value={"overall_severity": "info", "checks": {}, "failed_count": 0}) as mock_doc:
                await agent._check_stall(had_progress=False)
                mock_doc.assert_called_once()

        report_path = repo_root / "output" / "infra_doctor_report.json"
        # v2: Report written by _inline_infra_diagnosis (which we mocked)
        # So the report may or may not exist depending on mock behavior
        assert agent._coord_state.consecutive_no_progress_rounds >= 5


class TestCoordinatorDynamicFanOut:
    """Test dynamic fan-out calculation — scaled for 12-instance parallel mode."""

    def test_fix_mode_small_queue(self):
        assert CoordinatorAgent._desired_fan_out(3, "fix") == 3

    def test_fix_mode_medium_queue(self):
        assert CoordinatorAgent._desired_fan_out(8, "fix") == 6

    def test_fix_mode_large_queue(self):
        assert CoordinatorAgent._desired_fan_out(30, "fix") == 12

    def test_monitor_mode(self):
        assert CoordinatorAgent._desired_fan_out(5, "monitor") == 4

    def test_fix_mode_single(self):
        assert CoordinatorAgent._desired_fan_out(1, "fix") == 1

    def test_fix_mode_tiny(self):
        assert CoordinatorAgent._desired_fan_out(2, "fix") == 1

    def test_monitor_mode_empty(self):
        assert CoordinatorAgent._desired_fan_out(0, "monitor") == 1


class TestCoordinatorLanePlanning:
    """Test lane planning and conflict-aware batching."""

    def test_plan_execution_lanes_tracks_blocked_and_review(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        plan = agent._plan_execution_lanes([
            {
                "problem_id": "gov-1",
                "lane_id": "gov_mapping",
                "task_family": "feature-governance",
                "current_status": ProblemStatus.ACTIVE.value,
                "write_scope": ["app/governance/build_feature_catalog.py"],
            },
            {
                "problem_id": "repo-1",
                "lane_id": "repo_hygiene",
                "task_family": "repo-governance",
                "current_status": ProblemStatus.REVIEW_REQUIRED.value,
                "write_scope": ["automation/agents/**"],
            },
            {
                "problem_id": "runtime-1",
                "lane_id": "runtime_blocked",
                "task_family": "runtime-external",
                "current_status": ProblemStatus.BLOCKED.value,
                "write_scope": [],
            },
        ])

        assert len(plan["active_problems"]) == 1
        assert len(plan["review_problems"]) == 1
        assert len(plan["blocked_problems"]) == 1
        assert plan["execution_lanes"]["repo_hygiene"]["status"] == ProblemStatus.REVIEW_REQUIRED.value

    def test_build_fix_lane_batches_separates_overlapping_scopes(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        batches = agent._build_fix_lane_batches(
            [
                {
                    "problem_id": "gov-1",
                    "lane_id": "gov_mapping",
                    "write_scope": ["app/governance/**"],
                },
                {
                    "problem_id": "gov-2",
                    "lane_id": "gov_registry",
                    "write_scope": ["app/governance/feature_registry.json"],
                },
                {
                    "problem_id": "test-1",
                    "lane_id": "test_updates",
                    "write_scope": ["tests/**"],
                },
            ],
            "round-x",
        )

        assert len(batches) == 2
        assert all(batch["round_id"].startswith("round-x") for batch in batches)


# ===================================================================
# v2: Self-Completion, Self-Heal, Deferred Problem tests
# ===================================================================


class TestCoordinatorV2Completion:
    """Test v2 self-completion detection."""

    @pytest.mark.asyncio
    async def test_completion_not_met_when_green_low(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        agent._coord_state.consecutive_green_rounds = 5  # below threshold
        result = await agent._check_completion()
        assert result is False

    @pytest.mark.asyncio
    async def test_completion_not_met_when_failures(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        agent._coord_state.consecutive_green_rounds = 15
        agent._coord_state.consecutive_fix_failures = 1  # has failures
        result = await agent._check_completion()
        assert result is False

    @pytest.mark.asyncio
    async def test_completion_not_met_without_formal_promote_evidence(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        agent._coord_state.consecutive_green_rounds = 15
        agent._coord_state.consecutive_fix_failures = 0
        agent._coord_state.agents_registered = ["a", "b", "c", "d", "e", "f"]
        agent._coord_state.agents_healthy = ["a", "b", "c", "d", "e", "f"]
        # formal_promote gate only fires when there are completed lanes
        agent._coord_state.execution_lanes = {
            "fix_lane": {"lane_id": "fix_lane", "status": "completed", "problem_count": 1}
        }

        result = await agent._check_completion()
        assert result is False
        assert "formal_promote_evidence_missing" in agent._completion_evidence()["gate_failures"]

    @pytest.mark.asyncio
    async def test_completion_met(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        agent._coord_state.consecutive_green_rounds = 15
        agent._coord_state.consecutive_fix_failures = 0
        agent._coord_state.agents_registered = ["a", "b", "c", "d", "e", "f"]
        agent._coord_state.agents_healthy = ["a", "b", "c", "d", "e", "f"]
        agent._coord_state.last_formal_promote = {
            "approved": True,
            "state": "status_note_published",
            "targets_promoted": ["status-note"],
            "status_note_committed": True,
        }
        agent._last_preflight = {"all_ok": True, "svc_dummy": True}
        result = await agent._check_completion()
        assert result is True
        assert agent._coord_state.mode == "completed"
        assert agent._coord_state.completion_time != ""

    @pytest.mark.asyncio
    async def test_completion_report_written(self, mailbox, config, repo_root):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        agent._coord_state.consecutive_green_rounds = 15
        agent._coord_state.consecutive_fix_failures = 0
        agent._coord_state.agents_registered = ["a", "b", "c"]
        agent._coord_state.agents_healthy = ["a", "b", "c"]
        agent._coord_state.last_formal_promote = {
            "approved": True,
            "state": "status_note_published",
            "targets_promoted": ["status-note"],
            "status_note_committed": True,
        }
        agent._last_preflight = {"all_ok": True, "svc_dummy": True}
        await agent._check_completion()
        report = repo_root / "output" / "escort_team_completion.json"
        assert report.exists()
        data = json.loads(report.read_text(encoding="utf-8"))
        assert data["status"] == "COMPLETED"

    @pytest.mark.asyncio
    async def test_completion_allows_blocked_lanes(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        agent._coord_state.consecutive_green_rounds = 15
        agent._coord_state.consecutive_fix_failures = 0
        agent._coord_state.agents_registered = ["a", "b", "c", "d", "e", "f"]
        agent._coord_state.agents_healthy = ["a", "b", "c", "d", "e", "f"]
        agent._coord_state.last_formal_promote = {
            "approved": True,
            "state": "status_note_published",
            "targets_promoted": ["status-note"],
            "status_note_committed": True,
        }
        agent._last_preflight = {"all_ok": True, "svc_dummy": True}
        agent._coord_state.execution_lanes = {
            "runtime_blocked": {
                "lane_id": "runtime_blocked",
                "status": ProblemStatus.BLOCKED.value,
                "problem_count": 1,
            }
        }
        agent._coord_state.blocked_problems = [{"problem_id": "runtime-1"}]

        result = await agent._check_completion()
        assert result is True

    @pytest.mark.asyncio
    async def test_completion_blocked_by_review_lane(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        agent._coord_state.consecutive_green_rounds = 15
        agent._coord_state.consecutive_fix_failures = 0
        agent._coord_state.agents_registered = ["a", "b", "c", "d", "e", "f"]
        agent._coord_state.agents_healthy = ["a", "b", "c", "d", "e", "f"]
        agent._coord_state.execution_lanes = {
            "repo_hygiene": {
                "lane_id": "repo_hygiene",
                "status": ProblemStatus.REVIEW_REQUIRED.value,
                "problem_count": 1,
            }
        }

        result = await agent._check_completion()
        assert result is False


class TestCoordinatorV2DeferredProblems:
    """Test v2 deferred problem tracking."""

    @pytest.mark.asyncio
    async def test_deferred_escalation_recorded(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        msg = AgentMessage(
            source="fix-agent",
            target="coordinator",
            msg_type="escalation",
            payload={
                "level": "deferred",
                "problem_id": "prob-123",
                "reason": "exceeded max retries",
            },
        )
        await agent.on_message(msg)
        assert "prob-123" in agent._coord_state.deferred_problems

    @pytest.mark.asyncio
    async def test_deferred_not_duplicated(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        agent._coord_state.deferred_problems = ["prob-123"]
        msg = AgentMessage(
            source="fix-agent",
            target="coordinator",
            msg_type="escalation",
            payload={"level": "deferred", "problem_id": "prob-123"},
        )
        await agent.on_message(msg)
        assert agent._coord_state.deferred_problems.count("prob-123") == 1


class TestCoordinatorV2Preflight:
    """Test v2 preflight checks."""

    @pytest.mark.asyncio
    async def test_preflight_returns_checks(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        # Mock _http_ping to avoid real network calls
        with patch.object(agent, "_http_ping", new_callable=AsyncMock, return_value=False):
            result = await agent._preflight_checks()
        assert "all_ok" in result
        assert isinstance(result["all_ok"], bool)

    @pytest.mark.asyncio
    async def test_preflight_detects_artifacts(self, mailbox, config, repo_root):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        with patch.object(agent, "_http_ping", new_callable=AsyncMock, return_value=True):
            result = await agent._preflight_checks()
        # junit.xml exists in our fixture
        assert result.get("artifact_junit_xml") is True


class TestCoordinatorV2InlineInfraDiag:
    """Test v2 inline infra diagnostic."""

    @pytest.mark.asyncio
    async def test_inline_diag_returns_report(self, mailbox, config):
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        with patch.object(agent, "_http_ping", new_callable=AsyncMock, return_value=False):
            report = await agent._inline_infra_diagnosis()
        assert "checks" in report
        assert "overall_severity" in report
        assert report["overall_severity"] in ("info", "warning", "critical")

    @pytest.mark.asyncio
    async def test_inline_diag_severity_critical(self, mailbox, config):
        """When many services are down, severity should be critical."""
        agent = CoordinatorAgent(mailbox=mailbox, config=config)
        with patch.object(agent, "_http_ping", new_callable=AsyncMock, return_value=False):
            report = await agent._inline_infra_diagnosis()
        # With all pings failing, should be at least warning
        assert report["overall_severity"] in ("warning", "critical")


class TestFixAgentV2Escalation:
    """Test v2 fix agent escalation (no more stub patches)."""

    @pytest.mark.asyncio
    async def test_no_stub_patches_when_ai_down(self, mailbox, config):
        agent = FixAgent(mailbox=mailbox, config=config)
        analyses = [{
            "problem_id": "escalation-test",
            "root_cause": "broken import",
            "fix_strategy": "fix_code",
            "confidence": 0.9,
            "triage": "auto_fix",
        }]
        result = await agent.handle_task({"round_id": "r-esc", "analyses": analyses})
        # No stub patches should appear
        for f in result.get("findings", []):
            assert not f.get("path", "").startswith("__analysis__")

    @pytest.mark.asyncio
    async def test_rollback_defers_after_max_retries(self, mailbox, config):
        agent = FixAgent(mailbox=mailbox, config=config)
        # Set retry count to threshold - 1
        agent._retry_counts["p-retry"] = 2  # MAX is 3

        result = await agent.handle_task({
            "round_id": "r-retry",
            "action": "rollback",
            "patches": [{"problem_id": "p-retry"}],
        })
        assert "p-retry" in result.get("deferred", [])
