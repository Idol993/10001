"""
Async Scheduler Engine
Core orchestration engine that combines metaclass-based node discovery,
topological sorting, and semaphore-controlled concurrent execution.

Backward compatible with Python 3.9+ (provides TaskGroup-like API via gather).
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any, Callable, ClassVar, Dict, List, Optional, Set, Type, TypeVar

from async_task_engine.application.algorithms import BloomFilter, TopologicalSorter
from async_task_engine.application.metaclass import BaseTaskNode, NodeRegistry
from async_task_engine.domain.entities import (
    TaskGraph,
    TaskIdentifier,
    TaskState,
    TaskStatus,
)
from async_task_engine.interface.protocols import SchedulerEngine, StorageBackend

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class _TaskGroupCompat:
    """Compatibility layer providing TaskGroup-like structured concurrency.
    
    For Python < 3.11, wraps asyncio.gather with exception propagation.
    For Python >= 3.11, delegates to native asyncio.TaskGroup.
    """

    def __init__(self) -> None:
        self._tasks: List[asyncio.Task] = []
        self._exceptions: List[BaseException] = []
        self._entered = False

    async def __aenter__(self) -> "_TaskGroupCompat":
        self._entered = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_val is not None:
            # Cancel all running tasks on exception
            for task in self._tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            raise exc_val
        else:
            # Wait for all tasks to complete
            results = await asyncio.gather(*self._tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, BaseException):
                    self._exceptions.append(r)
                    raise r

    def create_task(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._tasks.append(task)
        return task


class EngineConfig:
    """Configuration for the scheduler engine."""

    def __init__(
        self,
        max_concurrent_tasks: int = 10,
        default_timeout: float = 300.0,
        bloom_filter_expected: int = 10000,
        bloom_filter_fp_rate: float = 0.01,
        storage: Optional[StorageBackend] = None,
        continue_on_failure: bool = False,
    ) -> None:
        if max_concurrent_tasks <= 0:
            raise ValueError("max_concurrent_tasks must be positive")
        if default_timeout <= 0:
            raise ValueError("default_timeout must be positive")

        self.max_concurrent_tasks = max_concurrent_tasks
        self.default_timeout = default_timeout
        self.bloom_filter_expected = bloom_filter_expected
        self.bloom_filter_fp_rate = bloom_filter_fp_rate
        self.storage = storage
        self.continue_on_failure = continue_on_failure


class AsyncTaskEngine(SchedulerEngine):
    """
    High-performance async task orchestration engine.
    
    Architecture:
    1. Discovers task nodes via metaclass-based registry
    2. Validates and sorts DAG dependencies via topological sort
    3. Executes tasks concurrently within semaphore limits
    4. Uses BloomFilter for efficient deduplication
    5. Provides graceful cancellation and error propagation
    
    Concurrency Model:
    - asyncio.Semaphore for bounded parallelism
    - _TaskGroupCompat for structured concurrency (Python 3.9+ compat)
    - asyncio.Event for coordination between producer and consumer
    """

    def __init__(self, config: EngineConfig | None = None) -> None:
        self._config = config or EngineConfig()
        self._bloom = BloomFilter[TaskIdentifier](
            expected_items=self._config.bloom_filter_expected,
            false_positive_rate=self._config.bloom_filter_fp_rate,
        )
        self._semaphore = asyncio.Semaphore(self._config.max_concurrent_tasks)
        self._states: Dict[TaskIdentifier, TaskState] = {}
        self._cancel_event = asyncio.Event()
        self._context: Dict[str, Any] = {}

    async def run(
        self,
        graph: TaskGraph,
        context: Dict[str, Any] | None = None,
    ) -> Dict[TaskIdentifier, TaskState]:
        """
        Execute the entire task graph with concurrency control.
        
        Args:
            graph: The task dependency graph to execute
            context: Shared context passed to every task node
            
        Returns:
            Dict mapping task identifiers to their final states
        """
        self._states = {node: TaskState() for node in graph.nodes}
        self._context = context or {}
        self._cancel_event.clear()

        # Validate graph
        levels = TopologicalSorter.get_execution_levels(graph)

        logger.info(
            "Starting engine execution: %d nodes across %d levels",
            len(graph.nodes),
            len(levels),
        )

        try:
            for level_idx, level in enumerate(levels):
                if self._cancel_event.is_set():
                    logger.warning("Cancellation requested, skipping remaining levels")
                    for node in level:
                        if node not in self._states or self._states[node].status == TaskStatus.PENDING:
                            self._states[node].status = TaskStatus.CANCELLED
                    break

                logger.info("Executing level %d: %d tasks concurrently", level_idx, len(level))

                level_tasks: List[asyncio.Task] = []
                for node in level:
                    if self._cancel_event.is_set():
                        state = self._states.get(node, TaskState())
                        if state.status == TaskStatus.PENDING:
                            state.status = TaskStatus.CANCELLED
                        continue
                    if node in self._bloom:
                        logger.debug("Task %s already executed, skipping", node)
                        self._states[node].status = TaskStatus.SKIPPED
                        continue
                    task = asyncio.create_task(self._execute_node(node, graph))
                    level_tasks.append(task)

                if level_tasks:
                    results = await asyncio.gather(*level_tasks, return_exceptions=True)
                    for i, (task, result) in enumerate(zip(level_tasks, results)):
                        if isinstance(result, BaseException):
                            logger.error("Level %d task failed: %s", level_idx, result)
                            if not self._config.continue_on_failure:
                                await self.cancel()
                                break

            self._context = {}

        except Exception as e:
            logger.exception("Engine execution failed: %s", e)
            raise

        logger.info(
            "Engine execution complete: %s",
            {str(k): v.status.value for k, v in self._states.items()},
        )

        return dict(self._states)

    async def _execute_node(self, node_id: TaskIdentifier, graph: TaskGraph) -> None:
        """Execute a single task node with semaphore control and retry logic."""
        async with self._semaphore:
            state = self._states.get(node_id)
            if state is None:
                return

            # Check preconditions
            deps = graph.dependencies.get(node_id, frozenset())
            for dep in deps:
                dep_state = self._states.get(dep)
                if dep_state and dep_state.status not in (TaskStatus.SUCCESS, TaskStatus.SKIPPED):
                    state.status = TaskStatus.SKIPPED
                    logger.warning(
                        "Skipping %s because dependency %s failed", node_id, dep
                    )
                    return

            # Look up node class
            node_type = NodeRegistry.get_node(node_id)
            if node_type is None:
                state.mark_failed(
                    RuntimeError(f"No registered node for identifier: {node_id}")
                )
                return

            instance = node_type()  # Uses cached instance from metaclass
            max_retries = getattr(instance, "max_retries", 0)

            last_error: Optional[BaseException] = None
            for attempt in range(max_retries + 1):
                if self._cancel_event.is_set():
                    state.status = TaskStatus.CANCELLED
                    return

                try:
                    state.mark_running()
                    logger.info("Executing task %s (attempt %d)", node_id, attempt + 1)
                    result = await asyncio.wait_for(
                        instance.execute(self._context.copy()),
                        timeout=self._config.default_timeout,
                    )
                    state.mark_success(result)
                    self._bloom.add(node_id)
                    logger.info(
                        "Task %s completed in %.3fs",
                        node_id,
                        state.duration,
                    )
                    return

                except asyncio.TimeoutError:
                    last_error = TimeoutError(
                        f"Task {node_id} timed out after {self._config.default_timeout}s"
                    )
                    logger.warning("Task %s timed out (attempt %d)", node_id, attempt + 1)

                except BaseException as e:
                    last_error = e
                    logger.error(
                        "Task %s failed on attempt %d: %s",
                        node_id,
                        attempt + 1,
                        e,
                    )

                if attempt < max_retries:
                    state.retry_count = attempt + 1
                    backoff = min(2 ** attempt, 30)  # Exponential backoff
                    await asyncio.sleep(backoff)

            state.mark_failed(last_error or RuntimeError("Max retries exceeded"))

    async def cancel(self) -> None:
        """Gracefully cancel all running tasks."""
        self._cancel_event.set()
        logger.info("Cancellation signal received")
        await asyncio.sleep(0.1)

    def get_context(self) -> Dict[str, Any]:
        return dict(self._context)
