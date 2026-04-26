"""Tests for WorkerPool and PipelineController (v10 Escort Team enhancements)."""

import asyncio
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from automation.agents.worker_pool import WorkerPool, WorkerResult, PoolStats
from automation.agents.pipeline import PipelineController, PipelinePhase


# ---------------------------------------------------------------------------
# WorkerPool tests
# ---------------------------------------------------------------------------

class _StubAgent:
    """Minimal agent stub for pool testing."""
    agent_id: str = "stub"

    async def handle_task(self, payload):
        await asyncio.sleep(0.01)
        return {"findings": [{"problem_id": payload.get("task_id", "x")}]}


class _FailAgent:
    """Agent that always fails."""
    agent_id: str = "fail"

    async def handle_task(self, payload):
        raise RuntimeError("deliberate failure")


class _SlowAgent:
    """Agent that takes too long."""
    agent_id: str = "slow"

    async def handle_task(self, payload):
        await asyncio.sleep(100)
        return {}


@pytest.mark.asyncio
async def test_worker_pool_basic_execution():
    """Pool executes tasks and returns results."""
    pool = WorkerPool(agent_factory=_StubAgent, pool_size=3)
    tasks = [
        {"task_id": f"t{i}", "write_scope": [f"app/file{i}.py"]}
        for i in range(5)
    ]
    results = await pool.execute_batch(tasks)
    assert len(results) == 5
    assert all(r.success for r in results)
    assert pool.stats.completed == 5
    assert pool.stats.failed == 0


@pytest.mark.asyncio
async def test_worker_pool_empty_batch():
    pool = WorkerPool(agent_factory=_StubAgent, pool_size=3)
    results = await pool.execute_batch([])
    assert results == []


@pytest.mark.asyncio
async def test_worker_pool_handles_failures():
    pool = WorkerPool(agent_factory=_FailAgent, pool_size=2)
    tasks = [{"task_id": "t1"}, {"task_id": "t2"}]
    results = await pool.execute_batch(tasks)
    assert len(results) == 2
    assert all(not r.success for r in results)
    assert all("deliberate failure" in r.error for r in results)
    assert pool.stats.failed == 2


@pytest.mark.asyncio
async def test_worker_pool_timeout():
    pool = WorkerPool(agent_factory=_SlowAgent, pool_size=1, task_timeout=0.05)
    results = await pool.execute_batch([{"task_id": "slow1"}])
    assert len(results) == 1
    assert not results[0].success
    assert "timeout" in results[0].error


@pytest.mark.asyncio
async def test_worker_pool_scope_grouping():
    """Tasks with overlapping scopes are serialized, non-overlapping are parallel."""
    pool = WorkerPool(agent_factory=_StubAgent, pool_size=5)
    tasks = [
        {"task_id": "t1", "write_scope": ["app/models.py"]},
        {"task_id": "t2", "write_scope": ["app/models.py"]},  # overlaps with t1
        {"task_id": "t3", "write_scope": ["tests/test_x.py"]},  # independent
    ]
    results = await pool.execute_batch(tasks, group_by_scope=True)
    assert len(results) == 3
    assert all(r.success for r in results)


@pytest.mark.asyncio
async def test_worker_pool_no_scope_grouping():
    pool = WorkerPool(agent_factory=_StubAgent, pool_size=5)
    tasks = [
        {"task_id": "t1", "write_scope": ["app/x.py"]},
        {"task_id": "t2", "write_scope": ["app/x.py"]},
    ]
    results = await pool.execute_batch(tasks, group_by_scope=False)
    assert len(results) == 2
    assert all(r.success for r in results)


@pytest.mark.asyncio
async def test_worker_pool_concurrency_cap():
    """Pool respects pool_size as concurrency limit."""
    call_times = []

    class _TimingAgent:
        agent_id = "timing"
        async def handle_task(self, payload):
            call_times.append(asyncio.get_event_loop().time())
            await asyncio.sleep(0.05)
            return {"ok": True}

    pool = WorkerPool(agent_factory=_TimingAgent, pool_size=2)
    tasks = [{"task_id": f"t{i}", "write_scope": [f"unique{i}/"]} for i in range(4)]
    results = await pool.execute_batch(tasks)
    assert all(r.success for r in results)
    # workers_used = min(pool_size, num_groups)
    assert pool.stats.workers_used <= 2


# ---------------------------------------------------------------------------
# PipelineController tests
# ---------------------------------------------------------------------------

def test_pipeline_start_round():
    pipe = PipelineController(max_depth=2)
    r = pipe.start_round("round-1")
    assert r.round_id == "round-1"
    assert r.phase == PipelinePhase.DISCOVERY
    assert pipe.active_count == 1


def test_pipeline_advance_phase():
    pipe = PipelineController()
    pipe.start_round("r1")
    pipe.advance_phase("r1", PipelinePhase.ANALYSIS)
    status = pipe.get_status()
    assert status["rounds"]["r1"]["phase"] == "analysis"


def test_pipeline_complete_round():
    pipe = PipelineController()
    pipe.start_round("r1")
    pipe.complete_round("r1", success=True)
    assert pipe.active_count == 0


def test_pipeline_max_depth():
    pipe = PipelineController(max_depth=2)
    pipe.start_round("r1")
    pipe.start_round("r2")
    assert not pipe.can_start_new_round  # at max depth


def test_pipeline_can_start_after_advance():
    pipe = PipelineController(max_depth=2)
    pipe.start_round("r1")
    assert not pipe.can_start_new_round  # r1 in DISCOVERY
    pipe.advance_phase("r1", PipelinePhase.FIXING)
    assert pipe.can_start_new_round  # r1 past early phases


@pytest.mark.asyncio
async def test_pipeline_write_lock():
    pipe = PipelineController()
    await pipe.acquire_write_lock()
    # Lock should be held
    assert pipe._write_lock.locked()
    pipe.release_write_lock()
    assert not pipe._write_lock.locked()


def test_pipeline_status():
    pipe = PipelineController(max_depth=3)
    pipe.start_round("r1")
    pipe.start_round("r2")
    status = pipe.get_status()
    assert status["active_rounds"] == 2
    assert "r1" in status["rounds"]
    assert "r2" in status["rounds"]
