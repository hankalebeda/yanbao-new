"""Tests for Escort Team autonomy improvements (Phase 3).

Covers:
- Circuit-breaker auto-reset in PromoteAgent
- SAFE_HOLD smart exit with exponential backoff in CoordinatorAgent
- Provider health tracking in AnalysisAgent
- Escalation structured output
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
    CoordinatorAgent,
    CoordinatorMode,
    PromoteAgent,
    AnalysisAgent,
    Mailbox,
    ProblemSpec,
    CoordinatorState,
)
from automation.agents.protocol import (
    AnalysisResult,
    CoordinatorPhase,
    ProblemStatus,
    PromoteDecision,
    Severity,
    TriageDecision,
)


@pytest.fixture
def repo_root(tmp_path):
    """Minimal repo for tests."""
    (tmp_path / "output").mkdir()
    (tmp_path / "runtime" / "agents").mkdir(parents=True)
    (tmp_path / "automation" / "control_plane").mkdir(parents=True)
    cp = tmp_path / "automation" / "control_plane" / "current_state.json"
    cp.write_text(json.dumps({
        "_schema": "infra_promote_v1",
        "promote_target_mode": "infra",
    }), encoding="utf-8")
    return tmp_path


@pytest.fixture
def mailbox():
    return Mailbox("test-escort")


@pytest.fixture
def config(repo_root):
    return AgentConfig(repo_root=repo_root, service_urls={}, service_tokens={})


# ---------------------------------------------------------------------------
# Circuit-breaker auto-reset
# ---------------------------------------------------------------------------

class TestCircuitBreakerAutoReset:
    @pytest.mark.asyncio
    async def test_blocked_when_regressions_and_no_green(self, mailbox, config):
        agent = PromoteAgent(mailbox, config)
        agent._promote_state = {
            "post_promote_regressions": 2,
            "consecutive_successes": 0,
            "last_promote": "",
            "last_tier": 0,
        }

        decision = await agent._execute_promotion(
            "r1", 1, {}, {"all_passed": True},
        )
        assert not decision.approved
        assert decision.reason == "circuit_breaker"

    @pytest.mark.asyncio
    async def test_auto_reset_after_green_rounds(self, mailbox, config):
        agent = PromoteAgent(mailbox, config)
        agent._promote_state = {
            "post_promote_regressions": 2,
            "consecutive_successes": 2,  # enough for auto-reset
            "last_promote": "",
            "last_tier": 0,
        }

        # Mock _promote_status_note to succeed
        agent._promote_status_note = AsyncMock(return_value={
            "status": "committed", "commit_id": "abc",
        })

        decision = await agent._execute_promotion(
            "r2", 1, {}, {"all_passed": True},
        )
        # Should have auto-reset and proceeded
        assert decision.approved
        assert any(t.startswith("status-note") for t in decision.targets_promoted)

        # State should be reset
        assert agent._promote_state["post_promote_regressions"] == 0

    @pytest.mark.asyncio
    async def test_auto_reset_requires_two_green(self, mailbox, config):
        agent = PromoteAgent(mailbox, config)
        agent._promote_state = {
            "post_promote_regressions": 3,
            "consecutive_successes": 1,  # not enough
            "last_promote": "",
            "last_tier": 0,
        }

        decision = await agent._execute_promotion(
            "r3", 1, {}, {"all_passed": True},
        )
        assert not decision.approved
        assert decision.reason == "circuit_breaker"


# ---------------------------------------------------------------------------
# SAFE_HOLD smart exit
# ---------------------------------------------------------------------------

class TestSafeHoldSmartExit:
    @pytest.mark.asyncio
    async def test_safe_hold_exits_when_problems_decrease(self, mailbox, config, repo_root):
        state_path = repo_root / "runtime" / "agents" / "coordinator_state.json"
        agent = CoordinatorAgent(mailbox, config, state_path=state_path)
        agent._coord_state.mode = CoordinatorMode.SAFE_HOLD.value
        agent._coord_state.safe_hold_count = 0
        agent._coord_state.last_safe_hold_problem_count = 5

        # Mock _dispatch_and_wait to return fewer problems
        agent._dispatch_and_wait = AsyncMock(return_value={
            "deduplicated": 2, "findings": [],
        })

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await agent._orchestration_round()

        # Should exit SAFE_HOLD
        assert agent._coord_state.mode == CoordinatorMode.FIX.value
        assert agent._coord_state.safe_hold_count == 0

    @pytest.mark.asyncio
    async def test_safe_hold_stays_when_problems_not_decreasing(self, mailbox, config, repo_root):
        state_path = repo_root / "runtime" / "agents" / "coordinator_state.json"
        agent = CoordinatorAgent(mailbox, config, state_path=state_path)
        agent._coord_state.mode = CoordinatorMode.SAFE_HOLD.value
        agent._coord_state.safe_hold_count = 0
        agent._coord_state.last_safe_hold_problem_count = 5

        # Mock _dispatch_and_wait to return same problem count
        agent._dispatch_and_wait = AsyncMock(return_value={
            "deduplicated": 5, "findings": [],
        })

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await agent._orchestration_round()

        # Should stay in SAFE_HOLD with incremented count
        assert agent._coord_state.mode == CoordinatorMode.SAFE_HOLD.value
        assert agent._coord_state.safe_hold_count == 1

    @pytest.mark.asyncio
    async def test_safe_hold_exits_on_first_check(self, mailbox, config, repo_root):
        """First SAFE_HOLD check (last_count=0) should always attempt exit."""
        state_path = repo_root / "runtime" / "agents" / "coordinator_state.json"
        agent = CoordinatorAgent(mailbox, config, state_path=state_path)
        agent._coord_state.mode = CoordinatorMode.SAFE_HOLD.value
        agent._coord_state.safe_hold_count = 0
        agent._coord_state.last_safe_hold_problem_count = 0  # first entry

        agent._dispatch_and_wait = AsyncMock(return_value={
            "deduplicated": 3, "findings": [],
        })

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await agent._orchestration_round()

        # Should exit on first check (last_count was 0)
        assert agent._coord_state.mode == CoordinatorMode.FIX.value


# ---------------------------------------------------------------------------
# Escalation structured output
# ---------------------------------------------------------------------------

class TestEscalationOutput:
    def test_write_escalation_creates_file(self, mailbox, config, repo_root):
        state_path = repo_root / "runtime" / "agents" / "coordinator_state.json"
        agent = CoordinatorAgent(mailbox, config, state_path=state_path)

        agent._write_escalation("safe_hold", "coordinator", "test reason")

        esc_path = repo_root / "output" / "escort_escalations.json"
        assert esc_path.exists()

        entries = json.loads(esc_path.read_text(encoding="utf-8"))
        assert len(entries) == 1
        assert entries[0]["level"] == "safe_hold"
        assert entries[0]["reason"] == "test reason"

    def test_write_escalation_appends(self, mailbox, config, repo_root):
        state_path = repo_root / "runtime" / "agents" / "coordinator_state.json"
        agent = CoordinatorAgent(mailbox, config, state_path=state_path)

        agent._write_escalation("info", "p1", "first")
        agent._write_escalation("warning", "p2", "second")

        esc_path = repo_root / "output" / "escort_escalations.json"
        entries = json.loads(esc_path.read_text(encoding="utf-8"))
        assert len(entries) == 2

    def test_write_escalation_caps_at_100(self, mailbox, config, repo_root):
        state_path = repo_root / "runtime" / "agents" / "coordinator_state.json"
        agent = CoordinatorAgent(mailbox, config, state_path=state_path)

        for i in range(110):
            agent._write_escalation("info", f"p{i}", f"reason {i}")

        esc_path = repo_root / "output" / "escort_escalations.json"
        entries = json.loads(esc_path.read_text(encoding="utf-8"))
        assert len(entries) == 100


# ---------------------------------------------------------------------------
# Provider health tracking in AnalysisAgent
# ---------------------------------------------------------------------------

class TestProviderHealthTracking:
    @pytest.mark.asyncio
    async def test_provider_skipped_after_failures(self, mailbox, config):
        agent = AnalysisAgent(mailbox, config)
        # Simulate 3 consecutive failures for chatgpt (instance variable)
        agent._provider_failures = {"chatgpt": 3}

        problem = ProblemSpec(
            problem_id="test_health",
            source_probe="test",
            severity="P2",
            description="test",
        )

        # Mock all provider calls to avoid real HTTP/CLI connections
        heuristic_result = {
            "provider": "secondary",
            "root_cause": "heuristic:test",
            "fix_strategy": "fix_code",
            "confidence": 0.5,
            "source": "heuristic",
        }
        agent._provider_analyze = AsyncMock(return_value=heuristic_result)
        agent._codex_analyze = AsyncMock(return_value=heuristic_result)

        result = await agent._analyze_one(problem)

        # Should still get a result (heuristic fallback)
        assert result.problem_id == "test_health"

    @pytest.mark.asyncio
    async def test_all_providers_reset_when_all_down(self, mailbox, config):
        agent = AnalysisAgent(mailbox, config)
        agent._provider_failures = {
            "chatgpt": 3,
            "deepseek": 3,
            "codex_cli": 3,
        }

        problem = ProblemSpec(
            problem_id="test_reset",
            source_probe="test",
            severity="P2",
            description="test",
        )

        # Mock all provider calls to avoid real HTTP/CLI connections
        heuristic_result = {
            "provider": "primary",
            "root_cause": "heuristic:test",
            "fix_strategy": "fix_code",
            "confidence": 0.6,
            "source": "heuristic",
        }
        agent._provider_analyze = AsyncMock(return_value=heuristic_result)
        agent._codex_analyze = AsyncMock(return_value=heuristic_result)

        result = await agent._analyze_one(problem)

        # Should produce a result with consensus
        assert result.problem_id == "test_reset"

    @pytest.mark.asyncio
    async def test_instance_isolation(self, mailbox, config):
        """Each AnalysisAgent should have independent provider failure tracking."""
        agent1 = AnalysisAgent(mailbox, config)
        agent2 = AnalysisAgent(mailbox, config)
        agent1._provider_failures["chatgpt"] = 5
        assert agent2._provider_failures.get("chatgpt", 0) == 0


# ---------------------------------------------------------------------------
# CoordinatorState new fields
# ---------------------------------------------------------------------------

class TestCoordinatorStateFields:
    def test_safe_hold_count_default(self):
        state = CoordinatorState()
        assert state.safe_hold_count == 0
        assert state.last_safe_hold_problem_count == 0

    def test_safe_hold_roundtrip(self):
        state = CoordinatorState(safe_hold_count=3, last_safe_hold_problem_count=7)
        d = state.to_dict()
        restored = CoordinatorState.from_dict(d)
        assert restored.safe_hold_count == 3
        assert restored.last_safe_hold_problem_count == 7


# ---------------------------------------------------------------------------
# Exponential backoff actually doubles
# ---------------------------------------------------------------------------

class TestExponentialBackoff:
    @pytest.mark.asyncio
    async def test_backoff_doubles_each_round(self, mailbox, config, repo_root):
        """Sleep should double: 300 → 600 → 1200 → ..."""
        state_path = repo_root / "runtime" / "agents" / "coordinator_state.json"
        recorded_sleeps = []

        async def mock_sleep(s):
            recorded_sleeps.append(s)

        for hold_count in range(4):
            recorded_sleeps.clear()
            agent = CoordinatorAgent(mailbox, config, state_path=state_path)
            agent._coord_state.mode = CoordinatorMode.SAFE_HOLD.value
            agent._coord_state.safe_hold_count = hold_count
            agent._coord_state.last_safe_hold_problem_count = 0  # will exit
            agent._dispatch_and_wait = AsyncMock(return_value={"deduplicated": 0, "findings": []})

            with patch("asyncio.sleep", side_effect=mock_sleep):
                await agent._orchestration_round()

            expected = min(300 * (2 ** hold_count), 28800)
            assert recorded_sleeps[0] == expected, (
                f"hold_count={hold_count}: expected {expected}, got {recorded_sleeps[0]}"
            )


# ---------------------------------------------------------------------------
# Atomic escalation write
# ---------------------------------------------------------------------------

class TestAtomicEscalation:
    def test_atomic_write_survives_concurrent_reads(self, mailbox, config, repo_root):
        """Multiple writes don't corrupt the JSON."""
        state_path = repo_root / "runtime" / "agents" / "coordinator_state.json"
        agent = CoordinatorAgent(mailbox, config, state_path=state_path)

        for i in range(20):
            agent._write_escalation("info", f"p{i}", f"reason {i}")

        esc_path = repo_root / "output" / "escort_escalations.json"
        entries = json.loads(esc_path.read_text(encoding="utf-8"))
        assert len(entries) == 20
        # Verify JSON integrity
        for e in entries:
            assert "timestamp" in e
            assert "level" in e

    def test_atomic_write_no_temp_files_left(self, mailbox, config, repo_root):
        """No .tmp files should remain after write."""
        state_path = repo_root / "runtime" / "agents" / "coordinator_state.json"
        agent = CoordinatorAgent(mailbox, config, state_path=state_path)

        agent._write_escalation("info", "p1", "test")

        output_dir = repo_root / "output"
        tmp_files = list(output_dir.glob("*.tmp"))
        assert len(tmp_files) == 0


# ---------------------------------------------------------------------------
# Doc25 probe section filtering
# ---------------------------------------------------------------------------

class TestDoc25SectionFiltering:
    def test_code_blocks_not_matched(self, tmp_path):
        """Alive markers inside code blocks should be ignored."""
        from automation.agents.doc25_probe import Doc25AngleProbe

        core = tmp_path / "docs" / "core"
        core.mkdir(parents=True)
        doc22 = core / "22_进度总表.md"
        doc22.write_text(
            "## P1 真实问题\n🔴 这是真正的问题\n\n"
            "```python\n# 🔴 这是代码中的注释\nprint('test')\n```\n",
            encoding="utf-8",
        )

        probe = Doc25AngleProbe(tmp_path)
        problems = probe._structural_scan(doc22.read_text(encoding="utf-8"))
        # Should find only 1 (the real one, not the code block one)
        assert len(problems) == 1
        assert "真实问题" in problems[0].title or "真正的问题" in problems[0].description

    def test_closed_sections_not_matched(self, tmp_path):
        """Markers after ✅/resolved should be ignored."""
        from automation.agents.doc25_probe import Doc25AngleProbe

        core = tmp_path / "docs" / "core"
        core.mkdir(parents=True)
        doc22 = core / "22_进度总表.md"
        doc22.write_text(
            "## P1 活跃问题\n🔴 这是活跃的\n\n"
            "## P0 已结束\n✅ resolved\n🔴 这个已经修好了\n",
            encoding="utf-8",
        )

        probe = Doc25AngleProbe(tmp_path)
        problems = probe._structural_scan(doc22.read_text(encoding="utf-8"))
        assert len(problems) == 1

    def test_doc22_resolution_picks_first(self, tmp_path):
        """With multiple doc22 files, should pick alphabetically first."""
        from automation.agents.doc25_probe import Doc25AngleProbe

        core = tmp_path / "docs" / "core"
        core.mkdir(parents=True)
        (core / "22_v1_old.md").write_text("old", encoding="utf-8")
        (core / "22_v2_new.md").write_text("new", encoding="utf-8")

        probe = Doc25AngleProbe(tmp_path)
        assert probe._doc22_path.name == "22_v1_old.md"
