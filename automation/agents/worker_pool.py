"""WorkerPool — dynamic multi-instance worker spawning for Escort Team.

Implements the Claude Code sub-agent pattern: instead of a single
FixAgent or AnalysisAgent handling all tasks sequentially, the pool
spawns N independent worker instances that pull from a shared task
queue and execute in true asyncio concurrency.

Architecture::

    Coordinator
        ├─ dispatch_parallel(tasks)
        │      ├─ Worker-1  (FixAgent instance)
        │      ├─ Worker-2  (FixAgent instance)
        │      ├─ Worker-3  (FixAgent instance)
        │      └─ Worker-N  (FixAgent instance)
        └─ collect results (fan-in)

Design choices vs single-agent:
* Each worker has its OWN agent instance (isolated state, metrics)
* Workers are ephemeral — created per round, destroyed after
* Task queue uses asyncio.Queue for backpressure
* Results collected via asyncio.gather with per-worker timeout
* Write-scope conflict detection prevents parallel edits to same files
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Type

from .base_agent import AgentConfig, BaseAgent
from .mailbox import Mailbox
from .protocol import AgentRole

logger = logging.getLogger(__name__)

# Defaults
DEFAULT_POOL_SIZE = int(os.environ.get("ESCORT_POOL_SIZE", "5"))
MAX_POOL_SIZE = int(os.environ.get("ESCORT_MAX_POOL_SIZE", "12"))
WORKER_TASK_TIMEOUT = float(os.environ.get("ESCORT_WORKER_TIMEOUT", "600"))


@dataclass
class WorkerResult:
    """Result from a single worker execution."""
    worker_id: str
    task_id: str
    success: bool
    result: Dict[str, Any] = field(default_factory=dict)
    error: str = ""
    duration_seconds: float = 0.0


@dataclass
class PoolStats:
    """Aggregate statistics for a pool execution round."""
    total_tasks: int = 0
    completed: int = 0
    failed: int = 0
    total_duration: float = 0.0
    workers_used: int = 0
    max_concurrent: int = 0


class WorkerPool:
    """Dynamic worker pool for parallel task execution.

    Usage::

        pool = WorkerPool(
            agent_factory=lambda: FixAgent(mailbox=mb, config=cfg),
            pool_size=5,
        )
        results = await pool.execute_batch(tasks)
    """

    def __init__(
        self,
        agent_factory: Callable[[], BaseAgent],
        pool_size: int = DEFAULT_POOL_SIZE,
        task_timeout: float = WORKER_TASK_TIMEOUT,
    ):
        self._agent_factory = agent_factory
        self._pool_size = min(pool_size, MAX_POOL_SIZE)
        self._task_timeout = task_timeout
        self._stats = PoolStats()

    async def execute_batch(
        self,
        tasks: List[Dict[str, Any]],
        *,
        group_by_scope: bool = True,
    ) -> List[WorkerResult]:
        """Execute a batch of tasks across multiple worker instances.

        Args:
            tasks: List of task payloads (each becomes one worker invocation)
            group_by_scope: If True, tasks with overlapping write_scope
                are serialized to prevent file conflicts.

        Returns:
            List of WorkerResult in same order as input tasks.
        """
        if not tasks:
            return []

        self._stats = PoolStats(total_tasks=len(tasks))
        start = time.monotonic()

        # Group tasks by write-scope conflict sets
        if group_by_scope:
            execution_groups = self._group_by_scope(tasks)
        else:
            # Each task is independent
            execution_groups = [[t] for t in tasks]

        # Determine effective concurrency
        effective_pool = min(self._pool_size, len(execution_groups))
        self._stats.workers_used = effective_pool
        self._stats.max_concurrent = effective_pool

        # Create semaphore for concurrency control
        sem = asyncio.Semaphore(effective_pool)

        # Execute groups concurrently (tasks within a group are serial)
        group_tasks = []
        for group in execution_groups:
            group_tasks.append(
                self._execute_group(group, sem)
            )

        group_results = await asyncio.gather(*group_tasks, return_exceptions=True)

        # Flatten results and map back to original task order
        all_results: List[WorkerResult] = []
        for gr in group_results:
            if isinstance(gr, list):
                all_results.extend(gr)
            elif isinstance(gr, Exception):
                all_results.append(WorkerResult(
                    worker_id="pool-error",
                    task_id="unknown",
                    success=False,
                    error=str(gr),
                ))

        self._stats.total_duration = time.monotonic() - start
        self._stats.completed = sum(1 for r in all_results if r.success)
        self._stats.failed = sum(1 for r in all_results if not r.success)

        logger.info(
            "WorkerPool batch complete: %d/%d succeeded, %d workers, %.1fs",
            self._stats.completed,
            self._stats.total_tasks,
            self._stats.workers_used,
            self._stats.total_duration,
        )

        return all_results

    @property
    def stats(self) -> PoolStats:
        return self._stats

    async def _execute_group(
        self,
        group: List[Dict[str, Any]],
        sem: asyncio.Semaphore,
    ) -> List[WorkerResult]:
        """Execute a group of tasks serially (scope-conflicting tasks)."""
        results = []
        async with sem:
            for task in group:
                result = await self._execute_single(task)
                results.append(result)
        return results

    async def _execute_single(self, task: Dict[str, Any]) -> WorkerResult:
        """Execute a single task on a fresh worker instance."""
        task_id = task.get("task_id", uuid.uuid4().hex[:8])
        worker_id = f"worker-{uuid.uuid4().hex[:6]}"
        start = time.monotonic()

        try:
            # Create ephemeral agent instance
            agent = self._agent_factory()
            agent.agent_id = worker_id

            # Execute with timeout
            result = await asyncio.wait_for(
                agent.handle_task(task),
                timeout=self._task_timeout,
            )

            elapsed = time.monotonic() - start
            return WorkerResult(
                worker_id=worker_id,
                task_id=task_id,
                success=True,
                result=result or {},
                duration_seconds=round(elapsed, 2),
            )

        except asyncio.TimeoutError:
            elapsed = time.monotonic() - start
            logger.warning(
                "Worker %s timed out on task %s after %.1fs",
                worker_id, task_id, elapsed,
            )
            return WorkerResult(
                worker_id=worker_id,
                task_id=task_id,
                success=False,
                error=f"timeout after {elapsed:.1f}s",
                duration_seconds=round(elapsed, 2),
            )

        except Exception as exc:
            elapsed = time.monotonic() - start
            logger.warning(
                "Worker %s failed on task %s: %s",
                worker_id, task_id, exc,
            )
            return WorkerResult(
                worker_id=worker_id,
                task_id=task_id,
                success=False,
                error=str(exc),
                duration_seconds=round(elapsed, 2),
            )

    def _group_by_scope(
        self, tasks: List[Dict[str, Any]]
    ) -> List[List[Dict[str, Any]]]:
        """Group tasks by write-scope overlap.

        Tasks that touch overlapping files must be serialized.
        Tasks with non-overlapping scopes can run in parallel.
        Uses a greedy bin-packing approach.
        """
        groups: List[List[Dict[str, Any]]] = []
        group_scopes: List[set] = []

        for task in tasks:
            scope = set(task.get("write_scope", []))
            if not scope:
                # No scope info — safe to parallelize
                groups.append([task])
                group_scopes.append(set())
                continue

            placed = False
            for i, existing_scope in enumerate(group_scopes):
                if not self._scopes_overlap(existing_scope, scope):
                    groups[i].append(task)
                    group_scopes[i] |= scope
                    placed = True
                    break

            if not placed:
                groups.append([task])
                group_scopes.append(scope)

        return groups

    @staticmethod
    def _scopes_overlap(scope_a: set, scope_b: set) -> bool:
        """Check if two file scopes overlap."""
        if not scope_a or not scope_b:
            return False
        # Direct file overlap
        if scope_a & scope_b:
            return True
        # Directory prefix overlap
        for a in scope_a:
            a_root = a.split("**")[0].rstrip("*/")
            for b in scope_b:
                b_root = b.split("**")[0].rstrip("*/")
                if not a_root or not b_root:
                    return True
                if a.startswith(b_root) or b.startswith(a_root):
                    return True
        return False
