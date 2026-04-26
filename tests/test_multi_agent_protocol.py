"""Unit tests for the multi-agent protocol layer, mailbox, and base agent.

Tests cover:
    1. Protocol data structures (serialisation, from_dict, autonomy_index)
    2. Mailbox (send, poll, receive, persistence, subscribe)
    3. BaseAgent lifecycle (start, heartbeat, shutdown, task dispatch)
    4. Agent factory (create_team)
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest

from automation.agents.protocol import (
    AgentHealthSnapshot,
    AgentMessage,
    AgentResult,
    AgentRole,
    AgentState,
    AnalysisResult,
    CoordinatorMode,
    CoordinatorPhase,
    CoordinatorState,
    HandlingPath,
    MessageType,
    PatchSet,
    ProblemSpec,
    ProblemStatus,
    PromoteDecision,
    Severity,
    TriageDecision,
    VerifyResult,
    WritebackReceipt,
)
from automation.agents.mailbox import Mailbox
from automation.agents.base_agent import AgentConfig, BaseAgent


# ====================================================================
# Protocol tests
# ====================================================================

class TestProtocol:
    """Test protocol data classes."""

    def test_agent_message_roundtrip(self):
        msg = AgentMessage(
            source="discovery-abc123",
            target="coordinator",
            msg_type=MessageType.TASK_RESULT.value,
            payload={"findings": [{"id": "p1"}]},
        )
        d = msg.to_dict()
        restored = AgentMessage.from_dict(d)
        assert restored.source == msg.source
        assert restored.target == msg.target
        assert restored.msg_type == msg.msg_type
        assert restored.payload == msg.payload
        assert restored.msg_id == msg.msg_id

    def test_problem_spec_roundtrip(self):
        ps = ProblemSpec(
            problem_id="test-001",
            source_probe="audit",
            severity=Severity.P1.value,
            family="truth-lineage",
            task_family="feature-governance",
            lane_id="gov_mapping",
            title="Test problem",
            affected_files=["app/foo.py"],
            affected_frs=["FR01"],
            suggested_approach=HandlingPath.FIX_THEN_REBUILD.value,
            current_status=ProblemStatus.ACTIVE.value,
            write_scope=["app/foo.py"],
        )
        d = ps.to_dict()
        restored = ProblemSpec.from_dict(d)
        assert restored.problem_id == ps.problem_id
        assert restored.affected_files == ["app/foo.py"]
        assert restored.lane_id == "gov_mapping"
        assert restored.write_scope == ["app/foo.py"]

    def test_analysis_result_roundtrip(self):
        ar = AnalysisResult(
            problem_id="test-001",
            root_cause="missing import",
            fix_strategy="fix_code",
            confidence=0.85,
            triage=TriageDecision.AUTO_FIX.value,
            task_family="issue-registry",
            lane_id="gov_registry",
            current_status=ProblemStatus.ACTIVE.value,
            write_scope=["app/governance/feature_registry.json"],
        )
        d = ar.to_dict()
        restored = AnalysisResult.from_dict(d)
        assert restored.confidence == 0.85
        assert restored.triage == "auto_fix"
        assert restored.lane_id == "gov_registry"
        assert restored.write_scope == ["app/governance/feature_registry.json"]

    def test_verify_result_roundtrip(self):
        vr = VerifyResult(
            round_id="r1",
            all_passed=True,
            scoped_pytest_passed=True,
            full_regression_passed=True,
            blind_spot_clean=True,
            catalog_fresh=True,
            artifacts_aligned=True,
        )
        d = vr.to_dict()
        restored = VerifyResult.from_dict(d)
        assert restored.all_passed is True

    def test_writeback_receipt_roundtrip(self):
        wr = WritebackReceipt(
            round_id="r1",
            problem_id="p1",
            commit_sha="abc123",
            affected_files=["app/x.py"],
            lease_id="lease-1",
        )
        d = wr.to_dict()
        restored = WritebackReceipt.from_dict(d)
        assert restored.commit_sha == "abc123"

    def test_promote_decision_roundtrip(self):
        pd = PromoteDecision(
            round_id="r1",
            tier=2,
            approved=True,
            targets_promoted=["status-note", "shared-artifact"],
        )
        d = pd.to_dict()
        restored = PromoteDecision.from_dict(d)
        assert restored.tier == 2
        assert restored.targets_promoted == ["status-note", "shared-artifact"]

    def test_coordinator_state_autonomy_index(self):
        state = CoordinatorState(
            consecutive_green_rounds=6,
            agents_registered=["a", "b", "c"],
            agents_healthy=["a", "b", "c"],
            consecutive_fix_failures=0,
        )
        idx = state.autonomy_index
        # green: min(6,5)/5=1.0 * 0.5 = 0.5
        # stable_monitor: mode='fix' (default) → False → health=3/3=1.0 * 0.3 = 0.3
        # fail: (1 - 0/5) * 0.2 = 0.2
        # total = 1.0
        assert idx == 1.0

    def test_coordinator_state_autonomy_index_degraded(self):
        state = CoordinatorState(
            consecutive_green_rounds=0,
            agents_registered=["a", "b", "c"],
            agents_healthy=["a"],
            consecutive_fix_failures=5,
        )
        idx = state.autonomy_index
        # green: 0/12 * 0.5 = 0
        # health: 1/3 * 0.3 = 0.1
        # fail: (1 - 5/5) * 0.2 = 0
        assert idx == 0.1

    def test_coordinator_state_autonomy_index_stable_monitor(self):
        """v7: Stable MONITOR mode with expired heartbeats credits health=1.0."""
        state = CoordinatorState(
            mode="monitor",
            consecutive_green_rounds=16,
            agents_registered=["a", "b", "c", "d", "e", "f"],
            agents_healthy=[],          # all heartbeats expired
            consecutive_fix_failures=0,
        )
        idx = state.autonomy_index
        # green: min(16,12)/12=1.0 * 0.5 = 0.5
        # health: stable_monitor branch -> health=1.0 * 0.3 = 0.3
        # fail: (1 - 0/5) * 0.2 = 0.2
        # total = 1.0
        assert idx == 1.0

    def test_coordinator_state_autonomy_index_stable_monitor_partial_healthy(self):
        """v9: Stable MONITOR credits full health regardless of agents_healthy count.

        v9 changed from v7: when stable_monitor=True (mode in monitor/completed,
        >=12 consecutive green rounds, 0 fix failures), stale heartbeat registrations
        from prior sessions inflate agents_registered while only current-session agents
        send heartbeats. So full health is credited unconditionally in stable_monitor.
        """
        state = CoordinatorState(
            mode="monitor",
            consecutive_green_rounds=12,
            agents_registered=["a", "b", "c"],
            agents_healthy=["a"],       # 1/3 healthy, but stable_monitor=True -> health=1.0
            consecutive_fix_failures=0,
        )
        idx = state.autonomy_index
        # stable_monitor=True -> health=1.0 (v9 behavior, not v7's 1/3)
        # green: 12/12=1.0 * 0.5 = 0.5
        # health: 1.0 * 0.3 = 0.3
        # fail: 1.0 * 0.2 = 0.2
        assert idx == 1.0

    def test_coordinator_state_autonomy_index_fix_mode_not_credited(self):
        """v7: FIX mode with no healthy agents is NOT credited (real health=0)."""
        state = CoordinatorState(
            mode="fix",
            consecutive_green_rounds=12,
            agents_registered=["a", "b", "c"],
            agents_healthy=[],
            consecutive_fix_failures=0,
        )
        idx = state.autonomy_index
        # fix mode: stable_monitor=False -> health=0/3=0
        # green: 1.0*0.5=0.5, health: 0*0.3=0, fail: 1.0*0.2=0.2
        assert idx == 0.7

    def test_all_enums(self):
        assert AgentRole.COORDINATOR.value == "coordinator"
        assert AgentState.DEGRADED.value == "degraded"
        assert MessageType.TASK_DISPATCH.value == "task_dispatch"
        assert Severity.P0.value == "P0"
        assert TriageDecision.AUTO_FIX.value == "auto_fix"
        assert CoordinatorPhase.MONITORING.value == "monitoring"
        assert CoordinatorMode.SAFE_HOLD.value == "safe_hold"

    def test_agent_health_snapshot(self):
        snap = AgentHealthSnapshot(
            agent_id="fix-abc",
            agent_role="fix",
            state="running",
            processed_count=42,
        )
        d = snap.to_dict()
        assert d["processed_count"] == 42
        restored = AgentHealthSnapshot.from_dict(d)
        assert restored.agent_id == "fix-abc"

    def test_agent_result(self):
        ar = AgentResult(
            agent_id="verify-001",
            agent_role="verify",
            status=AgentState.COMPLETED.value,
            findings=[{"gate": "pytest", "status": "passed"}],
        )
        d = ar.to_dict()
        assert d["agent_role"] == "verify"

    def test_patch_set(self):
        ps = PatchSet(
            problem_id="p1",
            patches=[{"path": "app/x.py", "patch_text": "fix", "before_sha": "abc"}],
            fix_strategy_used="fix_code",
            task_family="feature-governance",
            lane_id="gov_mapping",
            write_scope=["app/x.py"],
            duration_seconds=3.5,
        )
        d = ps.to_dict()
        assert d["duration_seconds"] == 3.5
        assert d["lane_id"] == "gov_mapping"
        restored = PatchSet.from_dict(d)
        assert restored.write_scope == ["app/x.py"]


# ====================================================================
# Mailbox tests
# ====================================================================

class TestMailbox:
    """Test async Mailbox operations."""

    @pytest.fixture
    def mailbox(self):
        return Mailbox(name="test")

    @pytest.mark.asyncio
    async def test_send_and_poll(self, mailbox):
        msg = AgentMessage(source="a", target="b", msg_type="info")
        await mailbox.send(msg)
        assert mailbox.depth == 1

        result = await mailbox.poll()
        assert result is not None
        assert result.source == "a"
        assert mailbox.depth == 0

    @pytest.mark.asyncio
    async def test_poll_with_predicate(self, mailbox):
        msg1 = AgentMessage(source="a", target="b", msg_type="info")
        msg2 = AgentMessage(source="c", target="d", msg_type="task_dispatch")
        await mailbox.send(msg1)
        await mailbox.send(msg2)

        result = await mailbox.poll(lambda m: m.msg_type == "task_dispatch")
        assert result is not None
        assert result.source == "c"
        assert mailbox.depth == 1  # msg1 still there

    @pytest.mark.asyncio
    async def test_poll_empty(self, mailbox):
        result = await mailbox.poll()
        assert result is None

    @pytest.mark.asyncio
    async def test_receive_timeout(self, mailbox):
        result = await mailbox.receive(timeout=0.1)
        assert result is None

    @pytest.mark.asyncio
    async def test_receive_immediate(self, mailbox):
        msg = AgentMessage(source="a", target="b", msg_type="info")
        await mailbox.send(msg)
        result = await mailbox.receive(timeout=1.0)
        assert result is not None
        assert result.source == "a"

    @pytest.mark.asyncio
    async def test_poll_all(self, mailbox):
        for i in range(5):
            await mailbox.send(AgentMessage(source=f"s{i}", msg_type="info"))
        results = await mailbox.poll_all(lambda m: m.source.startswith("s"))
        assert len(results) == 5
        assert mailbox.depth == 0

    @pytest.mark.asyncio
    async def test_subscribe(self, mailbox):
        received = []
        unsub = mailbox.subscribe(lambda m: received.append(m))
        await mailbox.send(AgentMessage(source="x", msg_type="info"))
        assert len(received) == 1
        unsub()
        await mailbox.send(AgentMessage(source="y", msg_type="info"))
        assert len(received) == 1  # no more after unsub

    def test_send_sync(self, mailbox):
        msg = AgentMessage(source="sync", target="b", msg_type="info")
        mailbox.send_sync(msg)
        result = mailbox.poll_sync()
        assert result is not None
        assert result.source == "sync"

    @pytest.mark.asyncio
    async def test_drain(self, mailbox):
        for i in range(3):
            await mailbox.send(AgentMessage(source=f"d{i}", msg_type="info"))
        drained = await mailbox.drain()
        assert len(drained) == 3
        assert mailbox.depth == 0

    @pytest.mark.asyncio
    async def test_persistence(self, tmp_path):
        db_path = tmp_path / "test_mailbox.db"
        mb1 = Mailbox(name="persist", backing_path=db_path)
        await mb1.send(AgentMessage(source="p1", target="t1", msg_type="info"))
        await mb1.send(AgentMessage(source="p2", target="t2", msg_type="task_dispatch"))
        # Consume one
        await mb1.poll()
        mb1.close()

        # Reopen — should restore 1 unconsumed message
        mb2 = Mailbox(name="persist", backing_path=db_path)
        assert mb2.depth == 1
        result = await mb2.poll()
        assert result is not None
        assert result.source == "p2"
        mb2.close()


# ====================================================================
# BaseAgent tests (via a concrete subclass)
# ====================================================================

class _TestAgent(BaseAgent):
    """Concrete agent for testing."""

    def __init__(self, mailbox: Mailbox, config=None):
        super().__init__(role=AgentRole.VERIFY, mailbox=mailbox, config=config)
        self.tasks_handled: list = []

    async def handle_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.tasks_handled.append(payload)
        return {"findings": [{"test": True}], "echo": payload}


class TestBaseAgent:
    """Test BaseAgent lifecycle and message dispatch."""

    @pytest.fixture
    def mailbox(self):
        return Mailbox(name="agent-test")

    @pytest.mark.asyncio
    async def test_start_and_shutdown(self, mailbox):
        agent = _TestAgent(mailbox)
        await agent.start()
        assert agent.state == AgentState.RUNNING or agent.state == AgentState.WAITING
        await agent.shutdown("test")
        assert agent.state == AgentState.SHUTDOWN

    @pytest.mark.asyncio
    async def test_health_snapshot(self, mailbox):
        agent = _TestAgent(mailbox)
        snap = agent.health_snapshot
        assert snap.agent_role == "verify"
        assert snap.state == "idle"
        assert snap.processed_count == 0

    @pytest.mark.asyncio
    async def test_task_dispatch(self, mailbox):
        agent = _TestAgent(mailbox)
        await agent.start()
        await asyncio.sleep(0.05)

        # Send a task dispatch
        await mailbox.send(AgentMessage(
            source="coordinator",
            target=agent.agent_id,
            msg_type=MessageType.TASK_DISPATCH.value,
            payload={"round_id": "r1", "data": "test"},
        ))

        # Wait for processing
        await asyncio.sleep(0.3)

        assert len(agent.tasks_handled) == 1
        assert agent.tasks_handled[0]["data"] == "test"
        assert agent._processed_count == 1

        await agent.shutdown("done")

    @pytest.mark.asyncio
    async def test_health_ping_response(self, mailbox):
        agent = _TestAgent(mailbox)
        await agent.start()
        await asyncio.sleep(0.05)

        # Send health ping
        await mailbox.send(AgentMessage(
            source="coordinator",
            target=agent.agent_id,
            msg_type=MessageType.HEALTH_PING.value,
        ))
        await asyncio.sleep(0.3)

        # Should have a pong in the mailbox
        pong = await mailbox.poll(lambda m: m.msg_type == MessageType.HEALTH_PONG.value)
        assert pong is not None
        assert "agent_id" in pong.payload

        await agent.shutdown("done")


# ====================================================================
# Factory test
# ====================================================================

class TestFactory:
    """Test create_team factory."""

    def test_create_team(self, tmp_path):
        from automation.agents import create_team

        team = create_team(repo_root=tmp_path)
        assert len(team.agents) == 7
        assert team.coordinator.agent_id == "coordinator"
        roles = [a.role for a in team.agents]
        assert AgentRole.COORDINATOR in roles
        assert AgentRole.DISCOVERY in roles
        assert AgentRole.ANALYSIS in roles
        assert AgentRole.FIX in roles
        assert AgentRole.VERIFY in roles
        assert AgentRole.WRITEBACK in roles
        assert AgentRole.PROMOTE in roles

    def test_team_status(self, tmp_path):
        from automation.agents import create_team

        team = create_team(repo_root=tmp_path)
        status = team.get_status()
        assert "coordinator" in status
        assert "agents" in status
        assert "mailbox_depth" in status
